/*******************************************************************

                           circlemaker.cpp

                           Colin Sullivan 2016
                           All rights reserved

********************************************************************/

#include <iostream>
#include <iterator>
#include <algorithm>
#include <map>
#include <ctime>
#include <fstream>
#include <string>
#include "circlemaker.h" 
#include "visitor.h"

pthread_mutex_t CircleMaker::sql_con_mtx = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t CircleMaker::grand_tot_mtx = PTHREAD_MUTEX_INITIALIZER;

int CircleMaker::grand_total_circles = 0;

CircleMaker::CircleMaker(MYSQL* _con) {
    local_circles_found = 0;
    edges_at_start = 0;
    edges_removed = 0;
    edges_at_end = 0;

    _prem_first   = true;         
    _min_value    = 0;
    _max_value    = 1000 * 1000000;
    _in_edge_min  = 1;
    _out_edge_min = 1;
    _num_records  = 500;

    con = _con; 
    e_vec.clear();
    visitor = new Visitor(con);
    visitor->local_circles_ptr = &local_circles_found;
    visitor->e_vec_ptr         = &e_vec;
    visitor->log_vec_ptr       = &log_vec;
}


CircleMaker::~CircleMaker() {
    //delete visitor;
}

/******************   SET QUERY OPTIONS   ************************/ 

void CircleMaker::SetQueryOptions(bool prem_first, long min_value, long max_value, int in_edge_min, int out_edge_min, int num_records){
    _prem_first   = prem_first;
    _min_value    = min_value * 1000000l;
    _max_value    = max_value * 1000000l;
    _in_edge_min  = in_edge_min;
    _out_edge_min = out_edge_min;
    _num_records  = num_records;
  }

void CircleMaker::SetQueryOptions(long min_value, long max_value, int in_edge_min, int out_edge_min, int num_records){
    _prem_first   = true;       
    _min_value    = min_value * 1000000l;
    _max_value    = max_value * 1000000l;
    _in_edge_min  = in_edge_min;
    _out_edge_min = out_edge_min;
    _num_records  = num_records;
  }

/******************      GENERATE A GRAPH         ******************/

void CircleMaker::GenerateGraph() {
    clock_t clock_begin, clock_end;
    clock_begin = clock();
    char q[1024];
    //Create the SQL statement and save it to q
    if(_prem_first == true) {
      sprintf(q, PREMIUM_SQL_STMT, _min_value, _max_value, _in_edge_min, _out_edge_min, _num_records);
    } else if(_prem_first == false) {
      sprintf(q, NO_PREM_SQL_STMT, _min_value, _max_value, _in_edge_min, _out_edge_min, _num_records);
   
    //sprintf(q, NO_OPTIONS_SQL_STMT, _in_edge_min, _out_edge_min, _num_records);
    }

    //Make the query
        if (mysql_query(con, q)) {
            FILE_LOG(logERROR) <<  mysql_error(con);
        }
        MYSQL_RES *result = mysql_store_result(con);
        if (result == NULL) {
            FILE_LOG(logERROR) <<  mysql_error(con);
        }
  
    //Declare some things
    MYSQL_ROW row;
    typedef std::map<int, Graph::vertex_descriptor> MyMap;
    MyMap myMap;
    typedef std::pair<int, Graph::vertex_descriptor> MyPair;

    FILE_LOG(logDEBUG1) << "Connected to " << mysql_get_host_info(con);
    FILE_LOG(logDEBUG1) << "SQL query     : " << q;
    FILE_LOG(logDEBUG1) << "Number of rows returned : " << mysql_num_rows(result);

    //Populate the graph
    while ((row = mysql_fetch_row(result)))
    {
        //FILE_LOG(logINFO) << "fetching rows";
        long item_id = atol(row[1]);
        //Check the Map to see if the item has already been added to graph
        //If not, execute the if statement
        if(myMap.find(item_id) == myMap.end()){
           try{
                Graph::vertex_descriptor vertex = g.add_vertex();
                g[vertex].ItemId = item_id;
                myMap.insert(MyPair(item_id, vertex));
            }
            catch(...){
                FILE_LOG(logERROR) << "  vertex creation failed for item: " << item_id;
            }
        }
        //Now, the vertex either existed before or we just made it
        //Check if the Desired Item is in the graph already
        int itemsef_id = atoi(row[2]);
        //If not, create the vertex
        if(myMap.find(itemsef_id) == myMap.end()) {
            try{
                Graph::vertex_descriptor vertex = g.add_vertex();
                g[vertex].ItemId = itemsef_id;
                myMap.insert(MyPair(itemsef_id, vertex));
            }
            catch(...){
                FILE_LOG(logERROR) << "  vertex creation failed for item: " << itemsef_id;
            }
       }
        //Both vertices are in the graph, so make the edge
        try{
            std::pair<Graph::edge_descriptor, bool> edge = boost::add_edge(myMap[item_id], myMap[itemsef_id], g);
            long offer_id = atol(row[0]);
            g[edge.first].OfferId = offer_id;
            FILE_LOG(logDEBUG1) << "offer: " << offer_id << "  " << item_id << " --> " << itemsef_id;
        }
        catch(...){
            FILE_LOG(logERROR) << "  edge creation failed for offer: " << item_id << ", " << itemsef_id;
        }
    }
    clock_end = clock();
    gen_graph_time = (double)(clock_end - clock_begin) / CLOCKS_PER_SEC * 1000;
    edges_at_start = boost::num_edges(g);
    mysql_free_result(result);
}

/***********************  Find the Circuits  **************************/

void CircleMaker::FindCircles() {
    clock_t clock_begin, clock_end;
    clock_begin = clock();
    int circles_delta = 0;
    int circles_last_itr = 0;
    EdgePredicate edge_property_accessor;
    boost::filtered_graph<Graph, EdgePredicate, VertexPredicate> fg(g, EdgePredicate(g), VertexPredicate(g));
    //Call the main algorithm
    do {
        boost::hawick_unique_circuits(fg, *visitor);
        for(EdgeVec::iterator e_itr = e_vec.begin(); e_itr != e_vec.end(); ++e_itr) {
            edge_property_accessor.removeCandidate(*e_itr);
            edges_removed++;
        }
        e_vec.clear();
        circles_delta = local_circles_found - circles_last_itr;
        circles_last_itr = local_circles_found;
        grand_total_circles += circles_delta;
        //std::cout << "circles delta " << circles_delta <<  std::endl;
        //std::cout << "grand total circles " << grand_total_circles <<  std::endl;
        
    } while (circles_delta != 0);
    edges_at_end = edges_at_start - edges_removed;
    FILE_LOG(logDEBUG1) << "edges at start: " << edges_at_start;
    FILE_LOG(logDEBUG1) << "edges removed: " << edges_removed;
    FILE_LOG(logDEBUG1) << "edges at end: " << edges_at_end;
    FILE_LOG(logDEBUG1) << "circles found in thread: " << local_circles_found;
    clock_end = clock();
    algo_run_time = (double)(clock_end - clock_begin) / CLOCKS_PER_SEC;
}

void CircleMaker::PrintStats(){
        FILE_LOG(logINFO) << local_circles_found << " circles found in $" << _min_value/1000000 << " to $" << _max_value/1000000 << " range";
        FILE_LOG(logINFO) << "graph generated in " << gen_graph_time << " sec";
        FILE_LOG(logINFO) << "algorithm ran in " << algo_run_time << " sec";
        //FILE_LOG(logINFO) << visitor->log_vec.size();
        if(log_vec.size() > 0) {
            for(std::vector<std::string>::iterator it = log_vec.begin(); it != log_vec.end(); ++it) {
                //std::string log_str = *it;
                FILE_LOG(logINFO) << *it;
            }
        }
}

void CircleMaker::GenerateTestGraph() {
}
