#include <filesystem>
#include <fstream>
#include <gflags/gflags.h>
#include <omp.h>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "CtStore.hpp"
#include "VersionChain.hpp"
#include "flags.hpp"
#include "hopscotch/hopscotchhash.hpp"
#include "types.hpp"

static uint64_t getCurrentTimeInMicroseconds()
{
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    return static_cast<uint64_t>(microseconds);
}

/**
 * Supported operations:
 * 1. Insert an edge with a timestamp: hashTable.insertWithTs(edge);
 * 2. Get the latest version of an edge: hashTable.latest_value_ptr();
 * 3. Get the edge at a specified timestamp: hashTable.getVersionInRange(value, timestamp, edge_property);
 * 4. Get edges within a specified timestamp range: hashTable.getVersionInRange(value, timestamp, edge_property);
 * 5. Get all latest edges of a vertex: hashTable.getLatestEdges(vertex_id);
 */

int get(int version_num)
{

    ctgraph::VertexId_t vertex_id = 1000;
    std::atomic<ctgraph::VertexId_t> ver = vertex_id;
    ctgraph::CtStore ct_store(ver);
    hophash::HopscotchHashTable hashTable(8192 << 1, 16);
    std::unordered_set<hophash::HashNode, hophash::HashNodeHash> cpTable;
    std::vector<ankerl::unordered_dense::set<hophash::HashNode, hophash::HashNodeHash>> cpTable2;
    std::vector<std::set<hophash::HashNode>> cpSets;
    cpSets.resize(10000);
    cpTable2.resize(10000);

    std::unordered_map<int, ctgraph::VersionChain> version_chains;

    std::vector<int> random_values;
    std::vector<ctgraph::SequenceNumber_t> random_values_seq;
    random_values.reserve(100000);

    for (int i = 0; i < 10000; i++)
    {
        int del = 10000 / version_num;
        int value = rand() % del;

        random_values.push_back(value);
        random_values_seq.push_back(getCurrentTimeInMicroseconds());
    }

    int count = 0;
    auto start_time_insert = std::chrono::high_resolution_clock::now();
    std::unique_ptr<FixString> s = std::make_unique<FixString>("hello");
    for (int value : random_values)
    {
        version_chains[value].insert(random_values_seq[count], value, std::make_shared<FixString>(s->data()));
        count++;
    }
    auto end_time_insert = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration_insert = end_time_insert - start_time_insert;
    std::cout << "Time taken for version chain insert: " << duration_insert.count() << " seconds" << std::endl;

    count = 0;
    start_time_insert = std::chrono::high_resolution_clock::now();
    s = std::make_unique<FixString>("hello");
    for (int value : random_values)
    {
        ctgraph::Edge edge(value, random_values_seq[count], false, s.get());
        hashTable.insertWithTs(edge);
        count++;
    }
    end_time_insert = std::chrono::high_resolution_clock::now();
    duration_insert = end_time_insert - start_time_insert;
    std::cout << "Time taken for hophashTable.insert: " << duration_insert.count() << " seconds" << std::endl;
    // std::cout << "Time for hopbehind " << hashTable.execution_time_ << "
    // seconds"
    //           << std::endl;

    count = 0;
    start_time_insert = std::chrono::high_resolution_clock::now();
    for (int value : random_values)
    {
        auto version = version_chains[value].get_version(random_values_seq[count]);
        if (version)
        {
            assert(version->value == value);
            assert(version->timestamp == random_values_seq[count]);
        }
        count++;
    }
    end_time_insert = std::chrono::high_resolution_clock::now();
    duration_insert = end_time_insert - start_time_insert;
    std::cout << "Time taken for version chain lookup: " << duration_insert.count() << " seconds" << std::endl;

    // count = 0;
    // start_time_insert = std::chrono::high_resolution_clock::now();
    // for (int value : random_values) {
    //   for (int i = 0; i < 10000; i++) {
    //     if (cpSets[i].count(hophash::HashNode(value)) == 0) {
    //       cpSets[i].insert(hophash::HashNode(value));
    //       auto time = std::chrono::system_clock::now();
    //       break;
    //     }
    //   }
    // }
    // end_time_insert = std::chrono::high_resolution_clock::now();
    // duration_insert = end_time_insert - start_time_insert;
    // std::cout << "Time taken for std::set.insert: " << duration_insert.count()
    //           << " seconds" << std::endl;

    // count = 0;
    // start_time_insert = std::chrono::high_resolution_clock::now();
    // for (int value : random_values) {
    //   for (int i = 0; i < 10000; i++) {
    //     if (cpTable2[i].count(value) == 0) {
    //       cpTable2[i].insert(hophash::HashNode(value));
    //       auto time = std::chrono::system_clock::now();
    //       break;
    //     }
    //   }
    // }
    // end_time_insert = std::chrono::high_resolution_clock::now();
    // duration_insert = end_time_insert - start_time_insert;
    // std::cout << "Time taken for :unordered_dense::set.insert: "
    //           << duration_insert.count() << " seconds" << std::endl;

    // count = 0;
    // start_time_insert = std::chrono::high_resolution_clock::now();
    // for (int value : random_values) {
    //   // ct_store.put_edge(1, value, *s.get());
    //   ct_store.put_edge_withTs(1, value, *s.get(), random_values_seq[count++]);
    // }
    // end_time_insert = std::chrono::high_resolution_clock::now();
    // duration_insert = end_time_insert - start_time_insert;
    // std::cout << "Time taken for ct_store.insert: " << duration_insert.count()
    //           << " seconds" << std::endl;

    // int value_count = 0;
    // start_time_insert = std::chrono::high_resolution_clock::now();
    // for (auto vlaue : hashTable.latest_value_ptr()) {
    //   value_count++;
    // }
    // end_time_insert = std::chrono::high_resolution_clock::now();
    // duration_insert = end_time_insert - start_time_insert;
    // std::cout << "Time taken for hop.it: " << duration_insert.count()
    //           << " seconds" << std::endl;
    // std::cout << value_count << std::endl;

    count = 0;
    start_time_insert = std::chrono::high_resolution_clock::now();

    // #pragma omp parallel for num_threads(FLAGS_thread_num) reduction(+:count)
    ctgraph::EdgeProperty_t* edge_property;
    for (int i = 0; i < random_values.size(); i++)
    {
        auto hashnode = hashTable.getVersionInRange(random_values[i], random_values_seq[i], edge_property);
        assert(hashnode != ctgraph::kNotFound);
        // ctgraph::EdgeProperty_t *edge_property;
        // auto it = ct_store.get_edge(1, random_values[i], edge_property);
        // auto it2 = ct_store.get_edge_withTs(1, random_values[i], edge_property,
        //                                     random_values_seq[i]);
        // assert(it == ctgraph::kOk);
        // assert(it2 == ctgraph::kOk);
        count++;
    }

    end_time_insert = std::chrono::high_resolution_clock::now();
    duration_insert = end_time_insert - start_time_insert;
    std::cout << "Time taken for hop.find_all: " << duration_insert.count() << " seconds" << std::endl;
    std::cout << count << std::endl;

    // count = 0;
    // start_time_insert = std::chrono::high_resolution_clock::now();
    // for (auto vlaue : cpSets) {
    //   for (auto set : vlaue) {
    //     count++;
    //   }
    // }
    // end_time_insert = std::chrono::high_resolution_clock::now();
    // duration_insert = end_time_insert - start_time_insert;
    // std::cout << "Time taken for set.find_all: " << duration_insert.count()
    //           << " seconds" << std::endl;
    // std::cout << count << std::endl;

    // start_time_insert = std::chrono::high_resolution_clock::now();
    // std::vector<ctgraph::Edge> edges;
    // ct_store.get_edges(1, edges);
    // assert(edges.size() == value_count);
    // end_time_insert = std::chrono::high_resolution_clock::now();
    // duration_insert = end_time_insert - start_time_insert;
    // std::cout << "Time taken for hop_get_latest_edgs: " <<
    // duration_insert.count()
    //           << " seconds" << std::endl;
    // std::cout << count << std::endl;

    return 0;
}

// int main(int argc, char **argv) {
//   // 解析命令行标志 vscode传入参数去.vscode/settings.json
//   gflags::ParseCommandLineFlags(&argc, &argv, true);

//   // 创建输出目录
//   std::filesystem::path output_dir("../experiment/xiaorong");
//   if (!std::filesystem::exists(output_dir)) {
//     std::filesystem::create_directories(output_dir);
//   }
//   // 创建输出文件
//   std::string output_file = (output_dir / "output.txt").string();
//   std::ofstream output_stream(output_file);
//   // 检查文件是否成功打开
//   if (!output_stream.is_open()) {
//     std::cerr << "无法打开输出文件: " << output_file << std::endl;
//     return 1;
//   }
//   // 重定向cout到文件
//   std::streambuf *cout_buf = std::cout.rdbuf();
//   std::cout.rdbuf(output_stream.rdbuf());

//   std::cout << "simd: " << FLAGS_simd << std::endl;

//   // for (int i = 0; i < 5; i++) {
//   //   get(16);
//   //   std::this_thread::sleep_for(std::chrono::milliseconds(1000));
//   // }
//   for (int i = 1; i < 51; i++) {
//     std::cout << "version_num:" << i * 4 << std::endl;
//     for (int j = 0; j < 1; j++) {
//       get(i * 4);
//       std::this_thread::sleep_for(std::chrono::milliseconds(100));
//     }
//   }

//   // 恢复标准输出
//   std::cout.rdbuf(cout_buf);
//   output_stream.close();
// }

int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::cout << "simd: " << FLAGS_simd << std::endl;
    for (int j = 0; j < 5; j++)
    {
        get(200);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}
