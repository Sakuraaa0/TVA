//
// Created by lwh on 25-1-10.
//

#ifndef FLAGS_HPP
#define FLAGS_HPP

#include <gflags/gflags_declare.h>

DECLARE_bool(test);
DECLARE_bool(simd);
DECLARE_uint32(reserve_node);
DECLARE_uint32(num_op);
DECLARE_bool(prefetch);
DECLARE_uint32(thread_num);
DECLARE_uint32(ldbc_line_num);
DECLARE_string(dataset);

#endif // FLAGS_HPP
