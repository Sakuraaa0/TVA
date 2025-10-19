//
// Created by lwh on 25-1-13.
//

#ifndef PREFETCH_HPP
#define PREFETCH_HPP

#include "types.hpp"

namespace ctgraph
{

    inline void prefetch(const void* ptr)
    {
        typedef struct
        {
            char x[CACHE_LINE_SIZE];
        } cacheline_t;
        asm volatile("prefetcht0 %0" : : "m"(*(const cacheline_t*)ptr));
        //__builtin_prefetch(*(const cacheline_t*)ptr));
    }

} // namespace ctgraph

#endif // PREFETCH_HPP
