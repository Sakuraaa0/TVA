//
// Created by lwh on 25-1-2.
//

#ifndef CTGRAPH_HPP
#define CTGRAPH_HPP

#include <iostream>
#include <vector>
#include "edge.hpp"
#include "hopscotch/hopscotchhash.hpp"
#include "types.hpp"

namespace ctgraph
{
    class CtGraph
    {
    public:
        static void open(const std::string& dir, const size_t max_vertex_num, CtGraph** db);


        virtual Status del_edge(VertexId_t src, VertexId_t dis) = 0;

        virtual Status del_edge_sep(VertexId_t src, VertexId_t dis) = 0;

        virtual VertexId_t get_max_vertex_num() = 0;

        virtual VertexId_t new_vertex(bool use_recycled_vertex = false) = 0;

        virtual void put_vertex(VertexId_t vertex_id, std::string_view data) = 0;

        /**
         * Insert/Update the edge.
         * No return values for simplicity.
         */
        virtual void put_edge(VertexId_t src, VertexId_t dst, EdgeProperty_t& s) = 0;

        /**
         * Returns the edge.
         * An empty string indicates not found.
         */
        virtual Status get_edge(VertexId_t src, VertexId_t dis, EdgeProperty_t*& property) = 0;

        virtual Status get_edge_withTs(VertexId_t src, VertexId_t dst, EdgeProperty_t*& property,
                                       SequenceNumber_t ts) = 0;


        /**
         * Returns the all adj edge of vertex src.
         * An empty vector indicates not found.
         */
        virtual bool get_edges(VertexId_t src, std::vector<Edge>& edges) = 0;

        /**
         * Obtain all edges of a specific vertex that are consistent with a given time interval
         */
        virtual bool get_edges_withTs(VertexId_t src, std::vector<Edge>& edges, SequenceNumber_t ts) = 0;

        virtual ~CtGraph() {};

        virtual SequenceNumber_t get_sequence() const = 0;

        virtual void debug() = 0;

        void breakdown(const std::string& label);
    };

} // namespace ctgraph


#endif // CTGRAPH_HPP
