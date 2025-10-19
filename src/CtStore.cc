//
// Created by lwh on 25-1-3.
//
#include "CtStore.hpp"

namespace ctgraph
{
    void CtStore::put_edge(VertexId_t src, VertexId_t dst, EdgeProperty_t& s)
    {
        SequenceNumber_t seq = 0;
        Marker_t marker = false;
        // Edge edge(dst, seq, marker);
        // if (hophashtables_.find(src) == hophashtables_.end()) {
        //
        //     hophashtables_[src] = new hophash::HopscotchHashTable();
        // }
        // hophashtables_[src]->insertWithTs(edge);

        // This line atomically loads the `MemTable` pointer from the `memTable_` atomic variable
        // with `memory_order_acquire` semantics, and assigns it to `old_mem`.
        // `memory_order_acquire` ensures that all subsequent read and write operations in the current thread
        // are not reordered before this load operation. This guarantees that after loading `old_mem`,
        // the current thread can see all modifications made by other threads before storing `old_mem`.
        MemTable* mem = memTable_.load(std::memory_order_acquire);
        auto id = mem->GetMaxEdgeNum() - mem->remain_capacity_;
        mem->put_edge(src, dst, s, marker, seq, id);
        memTable_.store(mem, std::memory_order_release);
    }

    void CtStore::put_edge_withTs(VertexId_t src, VertexId_t dst, EdgeProperty_t& s, SequenceNumber_t ts)
    {
        Marker_t marker = false;
        // Edge edge(dst, seq, marker);
        // if (hophashtables_.find(src) == hophashtables_.end()) {
        //
        //     hophashtables_[src] = new hophash::HopscotchHashTable();
        // }
        // hophashtables_[src]->insertWithTs(edge);

        // This line atomically loads the `MemTable` pointer from the `memTable_` atomic variable
        // with `memory_order_acquire` semantics, and assigns it to `old_mem`.
        // `memory_order_acquire` ensures that all subsequent read and write operations in the current thread
        // are not reordered before this load operation. This guarantees that after loading `old_mem`,
        // the current thread can see all modifications made by other threads before storing `old_mem`.
        MemTable* mem = memTable_.load(std::memory_order_acquire);
        auto id = mem->GetMaxEdgeNum() - mem->remain_capacity_;
        mem->put_edge(src, dst, s, marker, ts, id);
        memTable_.store(mem, std::memory_order_release);
    }

    Status CtStore::get_edge(VertexId_t src, VertexId_t dst, EdgeProperty_t*& property)
    {
        Status rs = kNotFound;
        MemTable* mem = memTable_.load(std::memory_order_acquire);
        rs = mem->get(src, dst, property);
        memTable_.store(mem, std::memory_order_release);
        return rs;
    }

    Status CtStore::get_edge_withTs(VertexId_t src, VertexId_t dst, EdgeProperty_t*& property, SequenceNumber_t ts)
    {
        Status rs = kNotFound;
        MemTable* mem = memTable_.load(std::memory_order_acquire);
        rs = mem->get_withTs(src, dst, property, ts);
        memTable_.store(mem, std::memory_order_release);
        return rs;
    }

    bool CtStore::get_edges(VertexId_t src, std::vector<Edge>& edges)
    {
        MemTable* mem = memTable_.load(std::memory_order_acquire);
        auto rs = mem->get_edges(src, edges);
        memTable_.store(mem, std::memory_order_release);
        return rs;
    }

} // namespace ctgraph
