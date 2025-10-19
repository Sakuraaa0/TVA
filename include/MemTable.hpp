//
// Created by lwh on 25-1-3.
//

#ifndef MEMTABLE_HPP
#define MEMTABLE_HPP

#include <ankerl/unordered_dense.h>
#include "Neighbors.hpp"
#include "arena.h"
#include "hopscotch/hopscotchhash.hpp"
#include "livegraph/allocator.hpp"
#include "livegraph/futex.hpp"
#include "types.hpp"

using Futex = livegraph::Futex;
namespace hophash
{
    class HopscotchHashTable;
}

namespace ctgraph
{
    class MemTable
    {
    private:
        uint64_t listLength_; // the number of edges
        Arena<NewEdgeAH> edge_arena_;
        Futex* vertex_futexes_;
        const size_t max_vertex_num_;
        livegraph::SparseArrayAllocator<void> array_allocator_;

        uintptr_t* vertex_adjs_;

    public:
        int64_t remain_capacity_;

        MemTable(Futex* vertex_futexes, size_t _max_vertex_num = 1024 << 3) :
            vertex_futexes_(vertex_futexes), listLength_(0), edge_arena_(_max_vertex_num + 1),
            max_vertex_num_(_max_vertex_num)
        {
            remain_capacity_ = max_vertex_num_;
            auto pointer_allocater =
                std::allocator_traits<decltype(array_allocator_)>::rebind_alloc<uintptr_t>(array_allocator_);
            vertex_adjs_ = pointer_allocater.allocate(max_vertex_num_);
        }
        ~MemTable() = default;
        void load_vertex(VertexId_t src, size_t id);
        void put_edge(VertexId_t src, VertexId_t dst, EdgeProperty_t& s, Marker_t marker, SequenceNumber_t seq,
                      size_t id);

        Status get(VertexId_t src_, VertexId_t dst_, EdgeProperty_t*& property);

        Status get_withTs(VertexId_t src_, VertexId_t dst_, EdgeProperty_t*& property, SequenceNumber_t ts);

        [[nodiscard]] uint64_t GetMaxEdgeNum() const { return max_vertex_num_; }

        bool get_edges(VertexId_t src, std::vector<Edge>& edges);
        bool get_edges_withTs(VertexId_t src, std::vector<Edge>& edges, SequenceNumber_t ts);
    };
} // namespace ctgraph

#endif // MEMTABLE_HPP
