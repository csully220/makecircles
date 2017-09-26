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
#include "visitor.h" 

pthread_mutex_t Visitor::sql_con_mtx = PTHREAD_MUTEX_INITIALIZER;

/**************************   VISITOR  ********************************/  

Visitor::Visitor(MYSQL* _con) {                                           
      con = _con;
}

void Visitor::cycle(Path const& p, FilteredGraph const& g) {
  if(con) {
      std::ostringstream log_str; 
      long offer_id = 0;
      long item_id = 0;
      if (p.empty()) {
          return;
      }
      pthread_mutex_lock(&sql_con_mtx);
      if(VerifyOffers(p, g, con) == false) {
          FILE_LOG(logWARNING) << "Offers deleted. Tradecircle aborted";
          return;
      }
      //add a new tradecircle to the db
      if (mysql_query(con, "INSERT INTO oc_t_tc_circles () VALUES()")){
            FILE_LOG(logERROR) << mysql_error(con);
            mysql_close(con);
            exit(1);
      }
      (*local_circles_ptr)++;
      int number_offers = 0;
      unsigned long circle_id = mysql_insert_id(con);
      log_str << "New TC: " << circle_id << " --- ";
      log_str << "items <offers> : ";
      mysql_query(con, "UPDATE oc_t_tc_site_stats SET i_circles_created=i_circles_created+1 WHERE pk_i_id=1");
      char q[1024];
      // Iterate over path adding each offer to the new Tradecircle
      typename Path::const_iterator i, before_end = boost::prior(p.end());
      for (i = p.begin(); i != before_end; ++i) {
          log_str << g[*i].ItemId;
          item_id = g[*i].ItemId;
          Edge edge = boost::edge(*i, *(i + 1), g).first;
          offer_id = g[edge].OfferId;
          e_vec_ptr->push_back(edge);
          log_str << " <" << offer_id << "> ";
          sprintf(q, "UPDATE oc_t_tc_offers SET fk_i_tc_id=%lu WHERE pk_i_id=%ld", circle_id, offer_id);
          if (mysql_query(con, q)){
                FILE_LOG(logERROR) << mysql_error(con);
              mysql_close(con);
              exit(EXIT_FAILURE);
          }
          sprintf(q, "UPDATE oc_t_item SET i_num_available=i_num_available+1 WHERE pk_i_id=%ld", item_id);
          if (mysql_query(con, q)){
            FILE_LOG(logERROR) << mysql_error(con);
            mysql_close(con);
            exit(EXIT_FAILURE);
          }
          number_offers++;
      }
      Edge edge = boost::edge(*i, *p.begin(), g).first;
      offer_id = g[edge].OfferId;
      log_str << g[*i].ItemId;     
      item_id = g[*i].ItemId;
      e_vec_ptr->push_back(edge);
      log_str << " <" << offer_id << "> ";
      sprintf(q, "UPDATE oc_t_tc_offers SET fk_i_tc_id=%lu WHERE pk_i_id=%ld", circle_id, offer_id);
      if (mysql_query(con, q)){
            FILE_LOG(logERROR) << mysql_error(con);
            mysql_close(con);
            exit(EXIT_FAILURE);
      }
      sprintf(q, "UPDATE oc_t_item SET i_num_available=i_num_available+1 WHERE pk_i_id=%ld", item_id);
      if (mysql_query(con, q)){
            FILE_LOG(logERROR) << mysql_error(con);
            mysql_close(con);
            exit(EXIT_FAILURE);
      }
      number_offers++;
      sprintf(q, "UPDATE oc_t_tc_circles SET i_number_offers=%d WHERE pk_i_id=%ld", number_offers, circle_id);
      if (mysql_query(con, q)){
           FILE_LOG(logERROR) << mysql_error(con);
           mysql_close(con);
           exit(EXIT_FAILURE);
      }

      pthread_mutex_unlock(&sql_con_mtx);

      //FILE_LOG(logINFO) << log_str.str();
      log_vec_ptr->push_back(log_str.str());
//      FILE_LOG(logINFO) << log_vec.size();
    }
}

/************************** VERIFY OFFERS *****************************/
 
bool Visitor::VerifyOffers(Path p, FilteredGraph g, MYSQL* con) {
        typename Path::const_iterator i, before_end = boost::prior(p.end());
        char q[1024];
        bool records_exist = true;
        long offer_id;
        for (i = p.begin(); i != before_end; ++i) {
            Edge edge = boost::edge(*i, *(i + 1), g).first;
            offer_id = g[edge].OfferId;
            sprintf(q, "SELECT EXISTS(SELECT pk_i_id FROM oc_t_tc_offers WHERE pk_i_id=%ld)", offer_id);
            if (mysql_query(con, q)) {
                std::cout << mysql_error(con);
                //handle error by returning false
                //Main will handle as though a record is missing
                records_exist = false;
            }
            MYSQL_RES *result = mysql_store_result(con);
            if (result == NULL) {
                //handle error by returning false
                //Main will handle as though a record is missing   
                  std::cout << "result null";
                records_exist = false;
            } else if(mysql_num_rows(result) <= 0) {
                std::cout << "no rows";
                records_exist = false;
            }
        }
        Edge edge = boost::edge(*i, *p.begin(), g).first;
        offer_id = g[edge].OfferId;
        sprintf(q, "SELECT EXISTS(SELECT pk_i_id FROM oc_t_tc_offers WHERE pk_i_id=%ld)", offer_id);
//        pthread_mutex_lock(&sql_con_mtx);
        if (mysql_query(con, q)) {
            //handle error by returning false
            //Main will handle as though a record is missing
            records_exist = false;
        }
        MYSQL_RES *result = mysql_store_result(con);
//        pthread_mutex_unlock(&sql_con_mtx);
        if (result == NULL) {
            //handle error by returning false
            //Main will handle as though a record is missing
            std::cout << "result null";
            records_exist = false;
        } else if(mysql_num_rows(result) <= 0) {
            std::cout << "no rows";
            records_exist = false;
        }
        //We've checked the records and they're all there!
        mysql_free_result(result);
        return records_exist;
}

