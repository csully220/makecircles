/********************************************************************

                          circlemaker.h

                          Colin Sullivan 2016
                          All rights reserved

********************************************************************/
#ifndef CIRCLEMAKER_H
#define CIRCLEMAKER_H
#include <string>
#include <boost/graph/graph_utility.hpp>
#include <boost/graph/directed_graph.hpp>
#include <boost/graph/filtered_graph.hpp>
#include <vector>

#include "mysql.h"
#include "my_global.h"
#include "log.h"
#include "hawick_circuits_mod.hpp"
#include "visitor.h"

#define PREMIUM_SQL_STMT "SELECT o.pk_i_id, o.fk_i_listed_id, o.fk_i_desired_id FROM oc_t_tc_offers o INNER JOIN oc_t_item i ON i.pk_i_id=o.fk_i_listed_id AND i.i_price >= %ld AND i.i_price < %ld AND i.i_num_offered >= %d AND i.i_num_desired >= %d WHERE o.fk_i_tc_id IS NULL ORDER BY o.b_premium DESC, (i.i_num_desired+i.i_num_offered) DESC LIMIT %d"

#define NO_PREM_SQL_STMT "SELECT o.pk_i_id, o.fk_i_listed_id, o.fk_i_desired_id FROM oc_t_tc_offers o INNER JOIN oc_t_item i ON i.pk_i_id=o.fk_i_listed_id AND i.i_price >= %ld AND i.i_price < %ld AND i.i_num_offered >= %d AND i.i_num_desired >= %d WHERE o.fk_i_tc_id IS NULL ORDER BY (i.i_num_desired+i.i_num_offered) DESC LIMIT %d"

#define NO_OPTIONS_SQL_STMT "SELECT o.pk_i_id, o.fk_i_listed_id, o.fk_i_desired_id FROM oc_t_tc_offers o INNER JOIN oc_t_item i ON i.pk_i_id=o.fk_i_listed_id AND i.i_num_offered >= %d AND i.i_num_desired >= %d WHERE o.fk_i_tc_id IS NULL ORDER BY (i.i_num_desired+i.i_num_offered) DESC LIMIT %d"

class CircleMaker {
public:
//functions
    CircleMaker(MYSQL* con);
    ~CircleMaker();
    void SetQueryOptions(bool, long, long, int, int, int);
    void SetQueryOptions(long, long, int, int, int);
    void GenerateGraph();
    void GenerateTestGraph();
    void FindCircles();
    void PrintStats();
//data
    Graph g;
    int local_circles_found, edges_at_start, edges_removed, edges_at_end;
    long _min_value, _max_value;
    double gen_graph_time, algo_run_time;
    MYSQL* con;
    EdgeVec e_vec;
    Visitor* visitor;
    static int grand_total_circles;
    std::vector<std::string> log_vec;
private:
    static pthread_mutex_t sql_con_mtx;
    static pthread_mutex_t grand_tot_mtx;
    const char *p_database, *p_mysqluser, *p_password, *p_host, *p_key, *p_cert;
    int _in_edge_min, _out_edge_min, _num_records;
    bool _prem_first;
};

#endif
