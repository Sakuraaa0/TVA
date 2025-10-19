//
// Created by lwh on 25-1-2.
//

#ifndef TYPES_HPP
#define TYPES_HPP
#include <fixstring.hpp>

namespace ctgraph
{

#define CACHE_LINE_SIZE 64

    using VertexId_t = uint64_t;
    using SequenceNumber_t = uint64_t; // timestamp
    using EdgePropertyOffset_t = uint32_t; // edge property address offset_
    using Marker_t = bool;
    using EdgeProperty_t = FixString; // edge property// marker bit

    using ControlType = uint8_t;


    const VertexId_t INVALID_VERTEX_ID = ((VertexId_t)UINT16_MAX << 32) + UINT32_MAX;
    constexpr static uintptr_t NULLPOINTER = 0;

    // hopscotch control_
    constexpr static ControlType HOPEMPTY = 0x80;
    constexpr static ControlType HOPDELETE = 0xFE;

    // Control the maximum ratio of shift switching methods in hopscotch
    constexpr static int HOPSHIFTNUM = 2;

    constexpr static int32_t BACKEMPTY = INT32_MAX;

    const SequenceNumber_t INVALID_TIMESTAMP = 0;

    enum Status : unsigned char
    {
        kOk = 0,
        kDelete = 1,
        kNotFound = 2
    };
} // namespace ctgraph

#endif // TYPES_HPP
