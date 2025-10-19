//
// Created by lwh on 25-2-24.
//

#ifndef COLUMNTYPES_HPP
#define COLUMNTYPES_HPP
#include <types.hpp>
#include "livegraph/allocator.hpp"
#include "livegraph/futex.hpp"

namespace column
{
#define bitmap_t uint16_t
#define MAX_VERSION_NUM 64 // max num of version for every column
#define BITMAP_NUM 16

    using SequenceNumber_t = uint64_t; // timestamp
    using VertexId_t = uint64_t;
    using offset_t = uint64_t; // Offset type.
    using next_offset_t = uint64_t;
    using Futex = livegraph::Futex;
    using lock_t = uint64_t; // Lock type, used in remote locking
    using version_count_t = uint64_t;

    const VertexId_t INVALID_VERTEX_ID = ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX;
    const next_offset_t INVALD_NEXT_OFFSET = UINT8_MAX;
    const VertexId_t INVALID_OFFSET = ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX;

    class ColumnVE
    {
    public:
        ColumnVE(VertexId_t dst = INVALID_VERTEX_ID) : dst_(dst) {}
        // Copy assignment operator
        ColumnVE& operator=(const ColumnVE& other)
        {
            if (this != &other)
            {
                dst_ = other.dst_;
            }
            return *this;
        }

        // Move assignment operator
        ColumnVE& operator=(ColumnVE&& other)
        {
            if (this != &other)
            {
                dst_ = std::move(other.dst_);
            }
            return *this;
        }

        // Equality operator
        bool operator==(const ColumnVE& other) const { return dst_ == other.dst_; }

        // Inequality operator
        bool operator!=(const ColumnVE& other) const { return !(*this == other); }

        // Used for sorting in the SkipList
        bool operator<(const ColumnVE& rhs)
        {
            if (dst_ < rhs.dst_)
            {
                return true;
            }
            return false;
        }
        friend bool operator<(const ColumnVE& rhs1, const ColumnVE& rhs2)
        {
            if (rhs1.dst_ < rhs2.dst_)
            {
                return true;
            }
            return false;
        }
        // Used for comparison in the merge phase
        bool operator>(const ColumnVE& rhs)
        {
            if (dst_ > rhs.dst_)
            {
                return true;
            }
            return false;
        }

        bool Equal(const ColumnVE& rhs) const { return dst_ == rhs.dst_; }
        // Used for comparison in lookups
        bool LessThan(const ColumnVE& rhs)
        {
            if (dst_ < rhs.dst_)
            {
                return true;
            }
            return false;
        }

        VertexId_t destination() const { return dst_; }
        void set_destination(VertexId_t dst) { dst_ = dst; }

    private:
        ctgraph::VertexId_t dst_;
    };
} // namespace column

#endif // COLUMNTYPES_HPP
