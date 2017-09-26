/****************************************************************

                       main.cpp

                       Colin Sullivan 2016
                       All rights reserved

*****************************************************************/
#include <iostream>
#include <csignal>
#include <cstdlib>
#include <unistd.h>
#include <vector>
#include <string>
#include <algorithm>
#include <pthread.h>
#include <boost/tokenizer.hpp>
#include <boost/lexical_cast.hpp>

#include "circlemaker.h"
#include "cfg_parser.h"
#include "log.h"

#define HELP "    options:\n\t -p: load premium offers first (default true)\n\t -m: minimum Value \n\t -x: maximum value (overrides value delta if set) \n\t -r: value delta \n\t -i: minimum number of in-edges (offered items) \n\t -o: minimum number of out-edges (desired items) \n\t -l: LIMIT to number of records pulled at one time from DB (default 500)\n\t -f: CSV file to read intervals from (default is config/intervals.csv)\n\t -z: load the whole database (default)\n\n"

#define LOG_LEVEL logDEBUG

#define LOG(level) \
if (level > Log::ReportingLevel()) ; \
else Log().Get(level)

using namespace std;

void *process_subgraph(void*);
bool validate_options();

int  option        = 0;
bool prem_first    = true;    // -p
long min_value     = 0l;      // -m
long value_delta   = 0l;      // -r
long max_value     = 1000l;   // -m + -r
int  in_edge_min   = 1;       // -i
int  out_edge_min  = 1;       // -o
int  num_records   = 500;     // -l
bool no_options    = false;   // -z
string intrvl_file = "" ; // -f

int main(int argc, char *argv[]) {

clock_t clock_begin, clock_end;
clock_begin = clock();

//Parse the command line arguments
    if(argc <= 1) {
        no_options = true;
    } else {
        while ((option = getopt(argc, argv,"f:m:r:x:i:o:l:hzp")) != -1) {
            switch (option) {
                case 'p' : prem_first   = true;
                break;
                case 'f' : intrvl_file  = optarg;
                break;
                case 'm' : min_value    = atol(optarg);
                break;
                case 'x' : max_value    = atol(optarg);
                break;
                case 'r' : value_delta  = atol(optarg);
                break;
                case 'i' : in_edge_min  = atoi(optarg);
                break;
                case 'o' : out_edge_min = atoi(optarg);
                break;
                case 'l' : num_records  = atoi(optarg);
                break;
                case 'z' : no_options   = true;
                break;
                case 'h' : cout << HELP; 
                exit(EXIT_SUCCESS);
                break;
                default: cout << "Invalid arguments\n";
                exit(EXIT_FAILURE);
            }
        }
        if(validate_options() == false) {
            cout << "Makecircles Failed: Invalid Options" << endl;
            exit(EXIT_FAILURE);
        }
    }

// Get the current date for the logging
    time_t timer;
    char buffer[26];
    struct tm* tm_info;
    time(&timer);
    tm_info = localtime(&timer);

//  Initialize the log file
    strftime(buffer, 26, "./logs/mc.log.%Y.%m.%d", tm_info);
    
    //FILE* pFile = fopen("./logs/mc.log", "a");
    FILE* pFile = fopen(buffer, "a");
    Output2FILE::Stream() = pFile;
    FILELog::ReportingLevel() = LOG_LEVEL;
    FILE_LOG(logINFO) << "****************************************************\n" <<
    "                     *************        START        ******************\n" <<
    "                     ****************************************************";

//  Read in the SQL credentials
    ConfigFile cfg("./config/mysql.creds");
    string mysql_creds[6];
    mysql_creds[0] = cfg.getValueOfKey<string>("DATABASE");
    mysql_creds[1] = cfg.getValueOfKey<string>("MYSQLUSER");
    mysql_creds[2] = cfg.getValueOfKey<string>("PASSWORD");
    mysql_creds[3] = cfg.getValueOfKey<string>("HOST");
    mysql_creds[4] = cfg.getValueOfKey<string>("KEY");
    mysql_creds[5] = cfg.getValueOfKey<string>("CERT");

    const char *p_database  = mysql_creds[0].c_str();
    const char *p_mysqluser = mysql_creds[1].c_str();
    const char *p_password  = mysql_creds[2].c_str();
    const char *p_host      = mysql_creds[3].c_str();
    const char *p_key       = mysql_creds[4].c_str();
    const char *p_cert      = mysql_creds[5].c_str();

//  Open the SQL connection
    MYSQL *con = mysql_init(NULL);
    if (con == NULL) {
        FILE_LOG(logERROR) <<  mysql_error(con);
        cout << "Makecircles Failed: " << mysql_error(con) << endl; 
        mysql_close(con);
        exit(EXIT_FAILURE);
    }
    mysql_ssl_set(con, p_key, p_cert, NULL, NULL, NULL);
    if (mysql_real_connect(con, p_host, p_mysqluser, p_password, p_database, 0, NULL, 0) == NULL) {
        FILE_LOG(logERROR) <<  mysql_error(con);
        cout << "Makecircles Failed: " <<  mysql_error(con) << endl; 
        mysql_close(con);
        exit(EXIT_FAILURE);
    }

//  Read in the value intervals
    if(intrvl_file.empty()) {
        intrvl_file = "./config/intervals.csv";
    }
    string data(intrvl_file);
    ifstream in(data.c_str());
    if (!in.is_open()) {
        FILE_LOG(logERROR) << "Failed to read interval file";
        cout << "Makecircles Failed: Could not read interval file";
        exit(EXIT_FAILURE);
    }
    typedef boost::tokenizer< boost::escaped_list_separator<char> > Tokenizer;
    vector< string > str_vec;
    vector< long > long_vec;
    string line;
    getline(in,line);
    Tokenizer tok(line);
    str_vec.assign(tok.begin(),tok.end());
    string intrvl_str = "intervals: ";
    try {
        for(vector<string>::iterator it = str_vec.begin(); it != str_vec.end(); ++it) {
            intrvl_str.append(*it);
            intrvl_str.append(", ");
            long_vec.push_back(boost::lexical_cast< long >(*it));
        }
    } catch(...) {
        FILE_LOG(logERROR) << "Failed to convert intervals to <longs>";
        cout << "Makecircles Failed: Could not convert intervals to <longs>";
        exit(EXIT_FAILURE);
    }
    intrvl_str.pop_back();
    intrvl_str.pop_back();
    FILE_LOG(logINFO) << intrvl_str;

//  Make CircleMaker objects               
    vector< CircleMaker* > cm_vec;
 
    for(vector< long >::iterator it = long_vec.begin(); it != long_vec.end()-1; ++it) {
        //cout << *it << ", " << *(it+1) << endl;
        CircleMaker* cm = new CircleMaker(con);
        cm->SetQueryOptions(prem_first, *it, *(it+1), in_edge_min, out_edge_min, num_records);
        cm->GenerateGraph();
        cm_vec.push_back(cm);
    }

    CircleMaker::grand_total_circles = 0;
    pthread_t threads[(int)cm_vec.size()];

    int i = 0;

    void *status;
    for(vector< CircleMaker* >::iterator it = cm_vec.begin(); it != cm_vec.end(); ++it) {
        //cout << (*it)->_min_value/1000000 << endl; 
        pthread_create(&threads[i], NULL, process_subgraph, *it);
        FILE_LOG(logDEBUG1) << "thread " << i << " created";
        i++;
    }

    for(i=0; i<(int)cm_vec.size(); i++) {
        pthread_join(threads[i], &status);
        FILE_LOG(logDEBUG1) << "thread " << i << " joined, status: " << (long)status;
    }

    clock_end = clock();
    float total_run_time = (double)(clock_end - clock_begin) / CLOCKS_PER_SEC;
    FILE_LOG(logINFO) << CircleMaker::grand_total_circles << " total circles found in " << total_run_time << " seconds";
    cout << endl << CircleMaker::grand_total_circles << " total circles found in " << total_run_time << " seconds\n\n";
    exit(EXIT_SUCCESS);
}

void *process_subgraph(void* cm_ptr) {
    CircleMaker* cm = (CircleMaker*) cm_ptr;
    cm->FindCircles();
    if(cm->local_circles_found > 0) {
        cm->PrintStats();
    }
    mysql_thread_end(); 
    pthread_exit(NULL);
}

bool validate_options() {
            bool validated = true;
            if(value_delta < 0) {
                FILE_LOG(logERROR) << "Negative value delta\n";
                validated = false; 
            }
            if(min_value < 0) {
                FILE_LOG(logERROR) << "Negative min value\n";
                validated = false; 
            }
            if(in_edge_min < 1) {
                FILE_LOG(logERROR) << "In-edge minimum must be at least 1\n";
                validated = false; 
            }
            if(out_edge_min < 1) {
                FILE_LOG(logERROR) << "Out-edge minimum must be at least 1\n";
                validated = false; 
            }
            if(num_records <= 0) {
                FILE_LOG(logERROR) << "Min records must be positive\n";
                validated = false; 
            }
            if(max_value == 0 && value_delta == 0) {
                FILE_LOG(logERROR) << "You must specify a max value or a value delta\n";
                validated = false; 
            }
            if(min_value > max_value) {
                FILE_LOG(logERROR) << "Max value cannot be less than min value\n";
                validated = false; 
            }
            if(min_value > max_value) {
                FILE_LOG(logERROR) << "Max value cannot be less than min value\n";
                validated = false; 
            }
            if(value_delta > 0 && max_value == 0) {
                max_value = min_value + value_delta;
                if(max_value > 1000) {
                    FILE_LOG(logERROR) << "Value delta is too great - max value greater than $1000";
                validated = false;     
                }
            }   
            return validated; 
}
