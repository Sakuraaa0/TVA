//
// Created by lwh on 25-1-3.
//
#include "MemTable.hpp"
#include <cassert>
#include "types.hpp"


namespace ctgraph
{
    void MemTable::load_vertex(VertexId_t src, size_t id)
    {
        if (vertex_adjs_[src] == NULLPOINTER)
        {
            NeighBors* tempEdge_ = edge_arena_.GetAnElementById(id);
            remain_capacity_--;
            vertex_adjs_[src] = reinterpret_cast<uintptr_t>(tempEdge_);
        }
    }

    void MemTable::put_edge(VertexId_t src, VertexId_t dst, EdgeProperty_t& s, Marker_t marker, SequenceNumber_t seq,
                            size_t id)
    {
        //  vertex_futexes_[src].lock();
        assert(vertex_adjs_ != nullptr);
        if (vertex_adjs_[src] == NULLPOINTER)
        {
            NeighBors* tempEdge_ = edge_arena_.GetAnElementById(id);
            remain_capacity_--;
            Edge edge = Edge(dst, seq, marker, &s);
            tempEdge_->put_edge(edge);
            vertex_adjs_[src] = reinterpret_cast<uintptr_t>(tempEdge_);
            // vertex_futexes_[src].unlock();
        }
        else
        {
            Edge edge = Edge(dst, seq, marker, &s);
            (reinterpret_cast<NeighBors*>(vertex_adjs_[src]))->put_edge(edge);
            // vertex_futexes_[src].unlock();
        }
        // __sync_fetch_and_add(&listLength_, 1);
    }

    Status MemTable::get(VertexId_t src_, VertexId_t dst_, EdgeProperty_t*& property)
    {
        auto prt = vertex_adjs_[src_];
        if (prt == NULLPOINTER)
        {
            return Status::kNotFound;
        }
        else
        {
            Status temp = (reinterpret_cast<NeighBors*>(prt))->get(dst_, property);
            return temp;
        }
    }

    Status MemTable::get_withTs(VertexId_t src_, VertexId_t dst_, EdgeProperty_t*& property, SequenceNumber_t ts)
    {
        auto prt = vertex_adjs_[src_];
        if (prt == NULLPOINTER)
        {
            return Status::kNotFound;
        }
        else
        {
            Status temp = (reinterpret_cast<NeighBors*>(prt))->get_withTs(dst_, property, ts);
            return temp;
        }
    }

    bool MemTable::get_edges(VertexId_t src, std::vector<Edge>& edges)
    {
        if (src >= max_vertex_num_)
        {
            return false;
        }

        if (vertex_adjs_[src] == NULLPOINTER)
        {
            return false;
        }
        NeighBors* tempEdge_ = reinterpret_cast<NeighBors*>(vertex_adjs_[src]);
        return tempEdge_->get_edges(edges);
    }

    bool MemTable::get_edges_withTs(VertexId_t src, std::vector<Edge>& edges, SequenceNumber_t ts)
    {
        if (src >= max_vertex_num_)
        {
            return false;
        }

        if (vertex_adjs_[src] == NULLPOINTER)
        {
            return false;
        }
        NeighBors* tempEdge_ = reinterpret_cast<NeighBors*>(vertex_adjs_[src]);
        return tempEdge_->get_edges_withTs(edges, ts);
    }


} // namespace ctgraph
