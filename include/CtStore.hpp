//
// Created by lwh on 25-1-2.
//

#ifndef CTSTORE_HPP
#define CTSTORE_HPP
#include <atomic>

#include "CtGraph.hpp"
#include "MemTable.hpp"
#include "leveldb/port/mutexlock.h"
#include "leveldb/port/port_stdcxx.h"
#include "livegraph/allocator.hpp"
#include "livegraph/futex.hpp"
#include "persistence/PersistManager.hpp"

namespace ctgraph
{
    class CtStore : public CtGraph
    {
    private:
        ankerl::unordered_dense::map<VertexId_t, hophash::HopscotchHashTable*> hophashtables_;
        std::atomic<MemTable*> memTable_;
        const size_t max_vertex_num_; // max vertex_id supported by the system
        std::atomic<VertexId_t> vertex_id_; // max vertex_id actually used

        leveldb::port::Mutex mu_;
        livegraph::Futex* vertex_futexes_;
        // level file index
        livegraph::SparseArrayAllocator<void> array_allocator;

        // 持久化管理器指针（可选）
        persistence::PersistManager* persist_mgr_ = nullptr;

    public:
        CtStore(const size_t max_vertex_num) : max_vertex_num_(max_vertex_num), vertex_id_(0)
        {
            // init lock
            auto futex_allocater =
                std::allocator_traits<decltype(array_allocator)>::rebind_alloc<Futex>(array_allocator);
            vertex_futexes_ = futex_allocater.allocate(max_vertex_num_);
            memTable_ = new MemTable(vertex_futexes_, max_vertex_num_);
        }

        ~CtStore() override {}

        // 设置持久化管理器
        void setPersistManager(persistence::PersistManager* mgr) { persist_mgr_ = mgr; }

        void put_edge(VertexId_t src, VertexId_t dst, EdgeProperty_t& s) override;

        /**
         * 仅测试使用传入时间戳方便debug
         * @param ts 测试所传入的时间戳
         */
        void put_edge_withTs(VertexId_t src, VertexId_t dst, EdgeProperty_t& s, SequenceNumber_t ts);

        static uint64_t getCurrentTimeInMicroseconds()
        {
            // 获取当前时间点
            const auto now = std::chrono::system_clock::now();
            // 转换为自1970-01-01 00:00:00以来的时间
            const auto duration = now.time_since_epoch();
            // 转换为微秒
            const auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
            // 返回uint64_t类型的微秒数
            return static_cast<uint64_t>(microseconds);
        }


        Status del_edge(VertexId_t src, VertexId_t dis) override { return Status::kOk; }

        Status del_edge_sep(VertexId_t src, VertexId_t dis) override { return Status::kOk; }

        VertexId_t get_max_vertex_num() override { return 0; }

        VertexId_t new_vertex(bool use_recycled_vertex = false) override { return 0; }

        void put_vertex(VertexId_t vertex_id, std::string_view data) override {}
        void load_vertex(VertexId_t vertex_id)
        {
            MemTable* mem = memTable_.load(std::memory_order_acquire);
            auto id = mem->GetMaxEdgeNum() - mem->remain_capacity_;
            mem->load_vertex(vertex_id, id);
            memTable_.store(mem, std::memory_order_release);
        }

        /**
         * Returns the edge.
         * An empty string indicates not found.
         */
        Status get_edge(VertexId_t src, VertexId_t dst, EdgeProperty_t*& property) override;

        Status get_edge_withTs(VertexId_t src, VertexId_t dst, EdgeProperty_t*& property, SequenceNumber_t ts) override;


        /**
         * Returns the all adj edge of vertex src.
         * An empty vector indicates not found.
         */
        bool get_edges(VertexId_t src, std::vector<Edge>& edges) override;

        bool get_edges_withTs(VertexId_t src, std::vector<Edge>& edges, SequenceNumber_t ts) override
        {
            MemTable* mem = memTable_.load(std::memory_order_acquire);
            auto rs = mem->get_edges_withTs(src, edges, ts);
            memTable_.store(mem, std::memory_order_release);
            return rs;
        }

        /**
         * 获取顶点的所有邻居
         * @param vertex 顶点ID
         * @param neighbors 存储邻居的向量
         */
        void getNeighbors(VertexId_t vertex, std::vector<VertexId_t>& neighbors)
        {
            std::vector<Edge> edges;
            if (get_edges(vertex, edges))
            {
                for (const auto& edge : edges)
                {
                    neighbors.push_back(edge.destination());
                }
            }
        }

        [[nodiscard]] SequenceNumber_t get_sequence() const override { return 0; }

        void debug() override {}
    };
} // namespace ctgraph

#endif // CTSTORE_HPP
