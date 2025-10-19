#!/bin/bash

# 检查是否提供了程序名称作为参数
if [ -z "$1" ]; then
    echo "Usage: $0 <program_to_profile>"
    exit 1
fi

PROGRAM=$1

# 收集性能数据，只记录ctgraph::CtStore::put_edge函数
echo "Running perf record for $PROGRAM..."
 sudo perf record -F 9999 -g ./cmake-build-debug/$PROGRAM

# 生成 perf 数据脚本
echo "Generating perf script output..."
perf script > out.perf

# 折叠调用栈
echo "Collapsing stack with FlameGraph tool..."
./thirdparty/FlameGraph/stackcollapse-perf.pl out.perf > out.folded

# 生成火焰图
echo "Generating flamegraph..."
./thirdparty/FlameGraph/flamegraph.pl out.folded > flamegraph.svg

# 删除中间文件
echo "Cleaning up intermediate files..."
rm -f perf.data out.perf out.folded perf.data.old

echo "Flamegraph generated: flamegraph.svg"

echo "Flamegraph generated: flamegraph.svg"
