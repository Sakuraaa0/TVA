//
// Created by lwh on 25-1-10.
//
#include "flags.hpp"
#include <gflags/gflags.h>

DEFINE_bool(test, true, "test flag");
DEFINE_bool(simd, true, "simd flag");
DEFINE_bool(prefetch, false, "if use prefetch");
DEFINE_uint32(reserve_node, 16, "reserve node num of hashtable");
DEFINE_uint32(num_op, 1000000, "number of operations to perform");
DEFINE_uint32(thread_num, 5, "app thread num");
DEFINE_uint32(ldbc_line_num, 150000, "ldbc line num");
DEFINE_string(dataset, "ca-IMDB-sf.el",
              "dataset name, default is ca-IMDB-sf.el");
