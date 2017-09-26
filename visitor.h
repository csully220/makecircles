/********************************************************************

                          circlemaker.h

                          Colin Sullivan 2016
                          All rights reserved

********************************************************************/
#ifndef VISITOR_H
#define VISITOR_H
#include <string>
#include <boost/graph/graph_utility.hpp>
#include <boost/graph/directed_graph.hpp>
#include <boost/graph/filtered_graph.hpp>
#include <vector>

#include "mysql.h"
#include "my_global.h"
#include "log.h"

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

typedef typename boost::directed_graph<VertexProperty, EdgeProperty> Graph;
typedef typename std::vector<Graph::edge_descriptor> EdgeVec;
typedef typename Graph::edge_descriptor Edge;

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
};

struct VertexPredicate {
  public:
    VertexPredicate() : graph_m(0) { }
    VertexPredicate(Graph& graph) : graph_m(&graph) { }
    bool operator()(const Graph::vertex_descriptor& v) const {
      return true;
    }
    Graph* graph_m;
};

typedef typename boost::filtered_graph<Graph, EdgePredicate, VertexPredicate> FilteredGraph;

struct Visitor {
  MYSQL* con;
  int* local_circles_ptr;
  std::vector<std::string>* log_vec_ptr;
  Visitor(MYSQL*);
  static pthread_mutex_t sql_con_mtx;
  typedef typename std::vector<Graph::vertex_descriptor> Path;
  //static int total_circles_found;
  //static EdgeVec e_vec;
  EdgeVec* e_vec_ptr;
  void cycle(Path const&, FilteredGraph const&);
  bool VerifyOffers(Path, FilteredGraph, MYSQL*);
};

#endif
