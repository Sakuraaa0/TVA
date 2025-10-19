# TVA

## Description

**TVA** is a high-performance, version-aware temporal graph storage engine built for real-time analytics on dynamic graphs. TVA introduces a hybrid storage architecture, combining a versioned columnar store for vertex properties and an optimized hopscotch hash table for dynamic topological data, specifically tailored for the diverse and evolving nature of temporal graphs.

Core innovations include:

- **Temporal Table**: Efficiently manages version metadata separated from data values, enabling fast version lookups and minimizing version chain traversal.
- **Hopscotch-based Hash Structure**: Organizes edge version metadata to support efficient, bounded-time neighborhood temporal scans, using hopscotch-inspired algorithms for high update and query throughput.
- **Version-skipping**: ccelerate temporal queries that involves multiple neighborhood scans in the same timestamp.
- **SIMD-Accelerated Queries**: Leverages SIMD (Single Instruction, Multiple Data) parallelism for high-performance batch version filtering and scans.

TVA achieves up to *9.9×* lower temporal query latency and *2.2×* smaller storage overhead compared to state-of-the-art engines, making it a leading choice for efficient, large-scale, and up-to-date temporal graph processing.

For AE reviewers, please directly refer to [Evaluate the Artifact](#for-reviewers-evaluate-the-artifact).

## Getting Started

### Configurations

- CPU:  SIMD (e.g., AVX2, AVX512) should be supported.
- DRAM: ~40GB.
- OS: We have tested in Ubuntu 20.04.
- Compiler: `c++20` should be supported.

### Software Dependencies

This project depends on: `cmake`, `g++`, `gflags_stable`, `unordered_dense`, `clang-format`, and `libboost-all-dev`.

### Build & Installation

```
# Build
mkdir build && cd build
cmake ..
make -j$(nproc)
```

### Run

TVA is usually run via command line. Example:

```
./CtGraph_demo 
```

## For Reviewers: Evaluate the Artifact

If you want to see the Appendix,please see [Appendix](Appendix.pdf)

Our original datasets for each Temporal query workload are as follows.

- [mgBench](https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_small_import.cypher) : vertices 10,000, edges 121,716

- LDBC:
  - [LDBC-sf1](https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/ldbc_interactive_sf1.cypher.gz): vertices 3,181,724, edges 17,256,038
  - [LDBC-sf3](https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/interactive/ldbc_interactive_sf3.cypher.gz): vertices 9,281,922, edges 52,695,735

### Generate temporal data

Upon on original datasets, we provide generation tools for each workload to automaticlly generate graph operation query statements. The generation tool can be found in create_graph_op_queries.py in each workload directory. Those operation query statements can be used to generate temporal/historical data.

```
cd $workloadname
python create_graph_op_quries.py --arg $arg_value
```

You need to specify optional arguments to generate corresponding graph operation query statements. The common arguments are listed below. You can also check specific arguments in each workload.

```
python create_graph_op_quries.py --help
```

| Flag           | Description                                 |
| -------------- | ------------------------------------------- |
| --size         | Dataset size                                |
| --num-op       | The number of graph operation queries       |
| --update-ratio | The update ratio of graph operation queries |
| --delete-ratio | The delete ratio of graph operation queries |
| --write-path   | The write path of results                   |

### Generate temporal query statements

The generation tool can be found in create_temporal_quries.py in each workload directory. Those temporal query statements can be used to test temporal database performance. We can generate temporal query statements using following commonds.


    cd $workloadname
    python create_temporal_quries.py --arg $arg_value

You need to specify optional arguments to generate corresponding temporal query statements. The common arguments are listed below. You can also check specific arguments in each workload.

    python create_temporal_quries.py --help

| Flag             | Description                           |
| ---------------- | ------------------------------------- |
| --size           | Dataset size                          |
| --num-op         | The number of graph operation queries |
| --min-time       | Min time of temporal databas's life   |
| --max-time       | Max time of temporal databas's life   |
| --write-path     | The write path of results             |
| --interval       | Interval of time-slice queries        |
| --frequency-type | Frequency type of temporal queries    |

### Generate Currnet Data

We use datasets from the [Network Repository](https://networkrepository.com/ca.php) and [Stanford SNAP](https://snap.stanford.edu/data/index.html).

### Experiment on Temporal Graph

For Figure 8(a), 8(b), 9(a), 9(b), 9(c), where `$num_op` indicates the number of graph operations. You can artificially modify the query statement to retrieve data from vertices with varying levels of popularity.:

```
  cd experiments
  ./Mybench_experiment FLAGS_num_op
```

For Figure 8(c), 8(d), 9(d), 9(e), 9(f), by altering the original dataset used to generate the test statements, it is possible to conduct tests on LDBC datasets of varying sizes:

```
  cd experiments
  ./LDBC_experiment
```

###  Experiment on Current Graph

If you would like to understand the characteristics of each dataset, you can execute:

```
 cd experiments
  ./degree_count FLAGS_dataset
```

For Figure 10(a), 10(b), where `$dataset` indicates the currnet dataset:ca-IMDB-sf.el, com-dblp-sf.ungraph.el, com-youtube-sf.ungraph.el, and soc-Epinions1-sf.el:

```
  cd experiments
  ./randomInsert_experiment FLAGS_dataset
```

For Figure 11(a), 11(b), 11(c), 11(d), where `$dataset` indicates the currnet dataset:ca-IMDB-sf.el, com-dblp-sf.ungraph.el, com-youtube-sf.ungraph.el, and soc-Epinions1-sf.el:

```
 cd experiments
  ./Graphalytics_experiment FLAGS_dataset
```

### Ablation Study on TVA

For Figure 12(a), 12(b), You can adjust the value of `$version_num` to modify the number of different versions.

```
 cd experiments
  ./VersionChain_test
```

For Figure 12(c), You can adjust the value of `$version_num` to modify the number of different versions.

```
 cd experiments
  ./TemporalChain_test
```

For Figure 12(d),`$simd` can control whether we enable SIMD functionality:

```
 cd experiments
  ./TemporalChain_test FLAG_simd
```

## Appendix

### Code Structure

We mainly introduce the `src` and  `include`  folder, which contains the main code.

```
include
├── CMakeLists.txt
├── Column
│   ├── ColumbTable.hpp
│   ├── ColumnTypes.hpp
│   └── TemporalData.hpp
├── CtGraph.hpp
├── CtStore.hpp
├── MemTable.hpp
├── Neighbors.hpp
├── VersionChain.hpp
├── edge.hpp
├── flags.hpp
├── hopscotch
│   ├── hopscotchhash.hpp
│   └── id_types.hpp
├── prefetch.hpp
└── types.hpp
src
├── CMakeLists.txt
├── ColumbTable.cpp
├── CtGraph.cc
├── CtStore.cc
├── MemTable.cc
├── VersionChain.cpp
└── flags.cc
```

