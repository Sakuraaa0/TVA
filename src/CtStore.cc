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

        MemTable* mem = memTable_.load(std::memory_order_acquire);
        auto id = mem->GetMaxEdgeNum() - mem->remain_capacity_;
        mem->put_edge(src, dst, s, marker, seq, id);
        memTable_.store(mem, std::memory_order_release);

        // 持久化
        if (persist_mgr_ && persist_mgr_->isEnabled()) {
            persist_mgr_->wal().logEdgeInsert(seq, src, dst, s.data() ? s.data() : "");
#ifdef HAS_ROCKSDB
            if (persist_mgr_->isFullMode()) {
                persist_mgr_->rocksDBStore().putFriend(src, dst, seq, true);
            }
#endif
        }
    }

    void CtStore::put_edge_withTs(VertexId_t src, VertexId_t dst, EdgeProperty_t& s, SequenceNumber_t ts)
    {
        Marker_t marker = false;

        MemTable* mem = memTable_.load(std::memory_order_acquire);
        auto id = mem->GetMaxEdgeNum() - mem->remain_capacity_;
        mem->put_edge(src, dst, s, marker, ts, id);
        memTable_.store(mem, std::memory_order_release);

        // 持久化
        if (persist_mgr_ && persist_mgr_->isEnabled()) {
            persist_mgr_->wal().logEdgeInsert(ts, src, dst, s.data() ? s.data() : "");
#ifdef HAS_ROCKSDB
            if (persist_mgr_->isFullMode()) {
                persist_mgr_->rocksDBStore().putFriend(src, dst, ts, true);
            }
#endif
        }
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

}
