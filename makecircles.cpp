/*******************************************************************

                           makecircles.cpp

                           Colin Sullivan 2016
                           All rights reserved
********************************************************************/

#include <boost/graph/graph_utility.hpp>
#include <boost/graph/directed_graph.hpp>
#include <boost/graph/filtered_graph.hpp>
#include "hawick_circuits_mod.hpp" 
//Use the modified hawick circuits so we can remove edges between iterations
//#include <boost/graph/hawick_circuits.hpp> //Don't use this one. It will run forever

#include <iostream>
#include <iterator>
#include <algorithm>
#include <map>
#include <ctime>
#include <fstream>
#include <csignal>

#include "mysql.h"
#include "my_global.h"

#define DATABASE "osclass"
#define MYSQLUSER "makecircles"
#define PASSWORD "ccWT3u5xFcUV"
#define HOST "www.qubitrade.com"
#define CAPATH "/etc/mysql"
#define CA "ca.pem"
#define KEY "/etc/mysql/client-key.pem"
#define CERT "/etc/mysql/client-cert.pem"
//#define STREAM "log"

using namespace std;

struct VertexProperty {
public:
    VertexProperty() : ItemId(0) { } 
    long ItemId;
};

struct EdgeProperty {
public:
    EdgeProperty() : isCandidate(true) { }
    long OfferId;
    bool isCandidate;
};

typedef boost::directed_graph<VertexProperty, EdgeProperty> Graph;
typedef Graph::edge_descriptor Edge;

class EdgePredicate {
public:
    EdgePredicate() : graph_m(0) { }
    EdgePredicate(Graph& graph) : graph_m(&graph) { }
    bool operator()(const Edge& e) const {
        return (*graph_m)[e].isCandidate;
    }
    void removeCandidate(const Edge& e) {
        (*graph_m)[e].isCandidate = false;
    }
    Graph* graph_m;
} edge_property_accessor;

struct VertexPredicate {
public:
    VertexPredicate() : graph_m(0) { }
    VertexPredicate(Graph& graph) : graph_m(&graph) { }
    bool operator()(const Graph::vertex_descriptor& v) const {
        return true;
    }
    Graph* graph_m;
};

int total_circles_found;
typedef vector<Graph::edge_descriptor> EdgeVec;
EdgeVec e_vec;

void finish_with_error(MYSQL *con){
  cout << "MySql Error\n" <<  mysql_error(con) << endl;
  mysql_close(con);
  exit(EXIT_FAILURE);
}

template <typename Path, typename Graph>
bool verifyOffersInDB(Path const& p, Graph const& g) {
        MYSQL *con = mysql_init(NULL);
        if (con == NULL) {
            cout << mysql_error(con);
            return false;
        }
        mysql_ssl_set(con, KEY, CERT, NULL, NULL, NULL);
        if (mysql_real_connect(con, HOST, MYSQLUSER, PASSWORD, DATABASE, 0, NULL, 0) == NULL) {
            mysql_close(con);
            cout << mysql_error(con);
            return false;
        }
        typename Path::const_iterator i, before_end = boost::prior(p.end());
        char q[1024];
        bool records_exist = true;
        long offer_id;
        for (i = p.begin(); i != before_end; ++i) {
            Edge edge = boost::edge(*i, *(i + 1), g).first;
            offer_id = g[edge].OfferId;
            sprintf(q, "SELECT EXISTS(SELECT pk_i_id FROM oc_t_tc_offers WHERE pk_i_id=%ld)", offer_id);
            if (mysql_query(con, q)) {
                cout << mysql_error(con);
                //handle error by returning false
                //Main will handle as though a record is missing
                records_exist = false;
            }
            MYSQL_RES *result = mysql_store_result(con);
            if (result == NULL) {
                //handle error by returning false
                //Main will handle as though a record is missing
                cout << "result null";
                records_exist = false;
            } else if(mysql_num_rows(result) <= 0) {
                cout << "no rows";
                records_exist = false;
            }
        }
        Edge edge = boost::edge(*i, *p.begin(), g).first;
        offer_id = g[edge].OfferId;
        sprintf(q, "SELECT EXISTS(SELECT pk_i_id FROM oc_t_tc_offers WHERE pk_i_id=%ld)", offer_id);
        if (mysql_query(con, q)) {
            //handle error by returning false
            //Main will handle as though a record is missing
            records_exist = false;
        }
        MYSQL_RES *result = mysql_store_result(con);
        if (result == NULL) {
            //handle error by returning false
            //Main will handle as though a record is missing
            cout << "result null";
            records_exist = false;
        } else if(mysql_num_rows(result) <= 0) {
            cout << "no rows";
            records_exist = false;
        }
        //We've checked the records and they're all there!
        mysql_free_result(result);
        return records_exist;
}

template <typename OutputStream>
struct cycle_printer
{
//Declare some things before the main callback
    cycle_printer(OutputStream& stream)
        : os(stream)
    { }
    template <typename Path, typename Graph>
//Callback for when Hawick Algo finds a circuit
    void cycle(Path const& p, Graph const& g)
    {
        long offer_id = 0;
        //long item_id = 0;
        if (p.empty()) {
            return;
        }
        MYSQL *con = mysql_init(NULL);
        if (con == NULL)
        {
            os << mysql_error(con);
        }
        mysql_ssl_set(con, KEY, CERT, NULL, NULL, NULL);
        if (mysql_real_connect(con, HOST, MYSQLUSER, PASSWORD,
            DATABASE, 0, NULL, 0) == NULL)
        {
            os << mysql_error(con);
            mysql_close(con);
        }
        if(verifyOffersInDB(p, g) == false) {
            os << "EXCEPTION: Offers deleted. Tradecircle aborted";
            return;
        }
        //add a new tradecircle to the db
        if (mysql_query(con, "INSERT INTO oc_t_tc_circles () VALUES()")){
              os << mysql_error(con);
              mysql_close(con);
              exit(1);
        }
        total_circles_found++;
        int number_offers = 0;
        unsigned long circle_id = mysql_insert_id(con);
        long item_id = 0;
        os << "\nNew Tradecircle: " << circle_id << " <";
        mysql_query(con, "UPDATE oc_t_tc_site_stats SET i_circles_created=i_circles_created+1 WHERE pk_i_id=1");
        char q[1024];
        // Iterate over path adding each offer to the new Tradecircle
        typename Path::const_iterator i, before_end = boost::prior(p.end());
        for (i = p.begin(); i != before_end; ++i) {
            //cout << g[*i].ItemId << endl;
            item_id = g[*i].ItemId;
            Edge edge = boost::edge(*i, *(i + 1), g).first;
            offer_id = g[edge].OfferId;
            e_vec.push_back(edge);
            os << offer_id << ", ";
            sprintf(q, "UPDATE oc_t_tc_offers SET fk_i_tc_id=%lu WHERE pk_i_id=%ld", circle_id, offer_id);
            if (mysql_query(con, q)){
                os << mysql_error(con);
                mysql_close(con);
                exit(EXIT_FAILURE);
            }
            sprintf(q, "UPDATE oc_t_item SET i_num_available=i_num_available+1 WHERE pk_i_id=%ld", item_id);
            if (mysql_query(con, q)){
              os << mysql_error(con);
              mysql_close(con);
              exit(EXIT_FAILURE);
            }
            number_offers++;
        }
        Edge edge = boost::edge(*i, *p.begin(), g).first;
        offer_id = g[edge].OfferId;
        //cout << g[*i].ItemId << endl;
        item_id = g[*i].ItemId;
        e_vec.push_back(edge);
        os << offer_id << ">" << endl;
        sprintf(q, "UPDATE oc_t_tc_offers SET fk_i_tc_id=%lu WHERE pk_i_id=%ld", circle_id, offer_id);
        if (mysql_query(con, q)){
              os << mysql_error(con);
              mysql_close(con);
              exit(EXIT_FAILURE);
        }
        sprintf(q, "UPDATE oc_t_item SET i_num_available=i_num_available+1 WHERE pk_i_id=%ld", item_id);
        if (mysql_query(con, q)){
              os << mysql_error(con);
              mysql_close(con);
              exit(EXIT_FAILURE);
        }
        number_offers++;
        sprintf(q, "UPDATE oc_t_tc_circles SET i_number_offers=%d WHERE pk_i_id=%ld", number_offers, circle_id);
        if (mysql_query(con, q)){
              os << mysql_error(con);
              mysql_close(con);
              exit(EXIT_FAILURE);
        }
        os << number_offers<< " offers added" << endl << endl;
        mysql_close(con);
    }
    OutputStream& os;
};


int main(int argc, char *argv[]) {  
//Parse the command line arguments
    int  option = 0;
    bool prem_first    = false; // -p
    long min_value     = 0;     // -m
    int  value_delta   = 0;     // -r
    long  max_value     = 1000; // -m + -r
    int  in_edge_min   = 1;     // -i
    int  out_edge_min  = 1;     // -o
    int  num_records   = 500;   // -l
    bool no_options    = false; // -z

if(argc <= 1) {
     no_options = true;
} else {  
    while ((option = getopt(argc, argv,"p:m:r:x:i:o:lhz")) != -1) {
        switch (option) {
             case 'p' : prem_first   = true;
                 break;
             case 'm' : min_value    = atol(optarg);
                        min_value   *= 1000000;
                 break;
             case 'x' : max_value    = atol(optarg);
                        max_value   *= 1000000;
                 break;
             case 'r' : value_delta  = atoi(optarg);
                        value_delta *= 1000000; 
                 break;
             case 'i' : in_edge_min  = atoi(optarg);
                 break;
             case 'o' : out_edge_min = atoi(optarg);
                 break;
             case 'l' : num_records  = atoi(optarg);
                 break;
             case 'z' : no_options   = true;
                 break;
             case 'h' : cout << "Options:\n -p: Load premium offers first\n -m: Minimum Value \n -x: Maximum Value (overrides value delta if set) \n -r: Value Delta \n -i: Minimum number of in-edges (Offered Items) \n -o: Minimum number of out-edges (Desired Items) \n -l: LIMIT to number of records pulled at one time from DB (defaults to 500)\n -z Load the whole database";
                 exit(EXIT_SUCCESS);
                 break;
             default: cout << "Invalid arguments\n"; 
                 exit(EXIT_FAILURE);
        }
    }
    if(no_options == false) {
        if(value_delta > 0) {
            cout << "Negative value delta\n";
            exit(EXIT_FAILURE);
        }
        if(min_value > 0) {
            cout << "Negative min value\n";
            exit(EXIT_FAILURE);
        }
        if(in_edge_min > 1) {
            cout << "In-edge minimum must be at least 1\n";
            exit(EXIT_FAILURE);
        }
        if(out_edge_min > 1) {
            cout << "Out-edge minimum must be at least 1\n";
            exit(EXIT_FAILURE);
        }
        if(num_records <= 0) {
            cout << "Min records must be positive\n";
            exit(EXIT_FAILURE);
        }
        if(max_value == 0 && value_delta == 0) {
            cout << "You must specify a max value or a value delta\n";
            exit(EXIT_FAILURE);
        }
        if(min_value > max_value) {
            cout << "Max value cannot be less than min value\n";
            exit(EXIT_FAILURE);
        }
        if(value_delta > 0 && max_value == 0) {
            max_value = min_value + value_delta;
        }
        //default, Max value is equal to what was passed and value_delta is ignored
    }
}
/******************************Initialize some variables and the SQL connection**********************/
  
    //Log file
    ofstream log;
    log.open ("mc.log", fstream::app);
    log.seekp(ios::end);
    //start the timer to measure total program running time
    clock_t clock_begin, clock_end;
    clock_begin = clock();
    time_t rawtime;
    time (&rawtime);

    log << "-----MAKECIRCLES STARTED-----" << endl;
    log << "Time: " << ctime (&rawtime) << endl;

    //mysql connection init
    MYSQL *con = mysql_init(NULL);
    if (con == NULL) 
    {
        log << mysql_error(con);
        return 1;
    }
    //mysql connect to database
    mysql_ssl_set(con, KEY, CERT, NULL, NULL, NULL);
    if (mysql_real_connect(con, HOST, MYSQLUSER, PASSWORD, DATABASE, 0, NULL, 0) == NULL) 
    {
        log << mysql_error(con);
        mysql_close(con);
        return 1;
    }
  
/*************************************************Make the SQL query*****************************/
    char q[1024];
    if(no_options == false) { 
        if(prem_first == true) {
            sprintf(q, "SELECT o.pk_i_id, o.fk_i_listed_id, o.fk_i_desired_id FROM oc_t_tc_offers o INNER JOIN oc_t_item i ON i.pk_i_id=o.fk_i_listed_id AND i.i_price >= %ld AND i.i_price < %ld AND i.i_num_offered >= %d AND i.i_num_desired >= %d WHERE o.fk_i_tc_id IS NULL ORDER BY o.b_premium DESC, (i.i_num_desired+i.i_num_offered) DESC LIMIT %d", min_value, max_value, in_edge_min, out_edge_min, num_records);
        } else if(prem_first == false) {
            sprintf(q, "SELECT o.pk_i_id, o.fk_i_listed_id, o.fk_i_desired_id FROM oc_t_tc_offers o INNER JOIN oc_t_item i ON i.pk_i_id=o.fk_i_listed_id AND i.i_price >= %ld AND i.i_price < %ld AND i.i_num_offered >= %d AND i.i_num_desired >= %d WHERE o.fk_i_tc_id IS NULL ORDER BY (i.i_num_desired+i.i_num_offered) DESC LIMIT %d", min_value, max_value, in_edge_min, out_edge_min, num_records);
        }
    } else if(no_options == true) {
        sprintf(q, "SELECT o.pk_i_id, o.fk_i_listed_id, o.fk_i_desired_id FROM oc_t_tc_offers o INNER JOIN oc_t_item i ON i.pk_i_id=o.fk_i_listed_id AND i.i_num_offered >= %d AND i.i_num_desired >= %d WHERE o.fk_i_tc_id IS NULL ORDER BY (i.i_num_desired+i.i_num_offered) DESC LIMIT %d", in_edge_min, out_edge_min, num_records);
    }

    if (mysql_query(con, q)) 
    {
        finish_with_error(con);
    }
    MYSQL_RES *result = mysql_store_result(con);
    if (result == NULL) 
    {
        finish_with_error(con);
    }
    MYSQL_ROW row;
  
/************************* Declare the Graph ******************/
    Graph g;

/************************* Hash table used in graph creation *********************/
    typedef std::map<int, Graph::vertex_descriptor> MyMap; 
    MyMap myMap;

/************************ Type inserted into hash table **************************/
    typedef std::pair<int, Graph::vertex_descriptor> MyPair;

/************************** Populate the graph **********************************/
  
    while ((row = mysql_fetch_row(result))) 
    { 
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
                log << "Vertex Creation --FAILED--  " << item_id << endl;
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
                log << "Vertex Creation --FAILED--  " << itemsef_id << endl;
            }
        }
        //Both vertices are in the graph, so make the edge
        try{
            std::pair<Graph::edge_descriptor, bool> edge = boost::add_edge(myMap[item_id], myMap[itemsef_id], g);
            long offer_id = atol(row[0]);
            g[edge.first].OfferId = offer_id;
            //cout << "  " << offer_id << "  " << item_id << " => " << itemsef_id << endl; //Enable for debug
            //cout << "  Edge Created  item_id: " << item_id << "; itemsef_id: " << itemsef_id << "; offer_id: " << offer_id << endl;
        }
        catch(...){
            log << "  Edge Creation --FAILED--  " << item_id << ", " << itemsef_id << endl;
        }
    }
    mysql_free_result(result);
    mysql_close(con);
 
/// **************************************************** Initialize Visitor, etc. *********
    cycle_printer<std::ostream> visitor(log);
  
    total_circles_found = 0;
    int circles_last_itr = 0; //circles at last iteration
    int circles_delta; 
    int edges_removed = 0;
    //Log the number of edges at start  
    int number_of_edges =  boost::num_edges(g);
    log << "Number offers at START: " << number_of_edges << endl;
    cout << "Number offers at START: " << number_of_edges << endl;

/// ************************************************** Declare the Filtered Graph ************************

    boost::filtered_graph<Graph, EdgePredicate, VertexPredicate> fg(g, EdgePredicate(g), VertexPredicate(g));   
    EdgeVec::iterator e_itr;
    //Call the main algorithm
    do {
       boost::hawick_unique_circuits(fg, visitor);
       for(e_itr = e_vec.begin(); e_itr != e_vec.end(); e_itr++) {
           edge_property_accessor.removeCandidate(*e_itr);
           edges_removed++;
       }
       e_vec.clear();
       circles_delta = total_circles_found - circles_last_itr;
       //cout << circles_delta << endl;
       circles_last_itr = total_circles_found;
    } while (circles_delta != 0);
    log << "Number offers at END: " << number_of_edges-edges_removed << endl; 


/// ********************************************************PRINT RUNTIME INFORMATION *************************************************
    log << total_circles_found << " Circles found"  << endl;
    cout << endl << "  " << total_circles_found << " Circles found"  << endl;
    //Uncomment below for cron
    //cout << endl << "  " << number_circles_found << " Circles found"  << endl;
    clock_end = clock();
    double time_spent = (double)(clock_end - clock_begin) / CLOCKS_PER_SEC;
  
    log << "Execution time: " << time_spent << endl << endl;
    cout << "  Execution time: " << time_spent << endl << endl;
    log << "-----MAKECIRCLES COMPLETE-----" << endl << endl;
/// ************************************************************************************************************************************

    log.close(); 
    exit(EXIT_SUCCESS);
}

/// ****************************************** END MAIN *********************************************/
