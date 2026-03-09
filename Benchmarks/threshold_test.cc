#include "CtStore.hpp"
#include "flags.hpp"
#include "types.hpp"
#include <chrono>
#include <fstream>
#include <gflags/gflags.h>
#include <iostream>
#include <random>
#include <sys/resource.h>
#include <vector>

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::ofstream result_file("/workspace/CtGraph/experiment/threshold_results.txt",
                            std::ios::out | std::ios::trunc);
  if (!result_file.is_open()) {
    std::cerr << "无法打开结果文件" << std::endl;
    return 1;
  }

  result_file << "Comparing Array vs HopscotchHashTable Performance\n";
  result_file << "=================================================\n\n";

  std::vector<int> data_sizes = {10, 100, 1000, 10000};

  for (int num_edges : data_sizes) {
    result_file << "Data size: " << num_edges << " edges\n";
    result_file << "----------------------------------------\n";
    std::cout << "\nTesting with " << num_edges << " edges:" << std::endl;

    long array_insert, array_query, hash_insert, hash_query;
    std::unique_ptr<FixString> s = std::make_unique<FixString>("");

    // Test 1: Array storage (large threshold)
    {
      FLAGS_reserve_node = 15000;
      ctgraph::CtStore ct_store_array(1);
      ct_store_array.load_vertex(0);

      auto start_time = std::chrono::high_resolution_clock::now();
      if(num_edges == 0){
        for(int j = 0; j < 10; j++){
          for (int dst = 0; dst < num_edges; dst++) {
            ct_store_array.put_edge_withTs(0, dst, *s.get(), j);
          }
        }
      } else {
        for (int dst = 0; dst < num_edges; dst++) {
            ct_store_array.put_edge_withTs(0, dst, *s.get(), 0);
          }
      }
      auto end_time = std::chrono::high_resolution_clock::now();

      auto query_start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < 1000; i++) {
          std::vector<ctgraph::Edge> edges;
          ct_store_array.get_edges(0, edges);
        }
      auto query_end = std::chrono::high_resolution_clock::now();

      array_insert = std::chrono::duration_cast<std::chrono::microseconds>(
          end_time - start_time).count();
      array_query = std::chrono::duration_cast<std::chrono::microseconds>(
          query_end - query_start).count();
    }

    // Test 2: HopscotchHashTable storage (small threshold)
    {
      FLAGS_reserve_node = 1;
      ctgraph::CtStore ct_store_hash(1);
      ct_store_hash.load_vertex(0);
       ct_store_hash.put_edge_withTs(0, 10002, *s.get(), 0);
       ct_store_hash.put_edge_withTs(0, 10003, *s.get(), 0);
       ct_store_hash.put_edge_withTs(0, 10004, *s.get(), 0);

      auto start_time = std::chrono::high_resolution_clock::now();
      if(num_edges == 0){
        for(int j = 0; j < 10; j++){
          for (int dst = 0; dst < num_edges; dst++) {
            ct_store_hash.put_edge_withTs(0, dst, *s.get(), j);
          }
        }
      } else {
        for (int dst = 0; dst < num_edges; dst++) {
            ct_store_hash.put_edge_withTs(0, dst, *s.get(), 0);
          }
      }
      auto end_time = std::chrono::high_resolution_clock::now();

      auto query_start = std::chrono::high_resolution_clock::now();
      for (int i = 0; i < 1000; i++) {
        std::vector<ctgraph::Edge> edges;
        ct_store_hash.get_edges(0, edges);
      }
      auto query_end = std::chrono::high_resolution_clock::now();

      hash_insert = std::chrono::duration_cast<std::chrono::microseconds>(
          end_time - start_time).count();
      hash_query = std::chrono::duration_cast<std::chrono::microseconds>(
          query_end - query_start).count();
    }

    double insert_ratio = (double)array_insert / hash_insert;
    double query_ratio = (double)array_query / hash_query;

    result_file << "  Array - Insert: " << array_insert << " us, Query: "
                << array_query << " us\n";
    result_file << "  Hash  - Insert: " << hash_insert << " us, Query: "
                << hash_query << " us\n";
    result_file << "  Array/Hash - Insert: " << insert_ratio
                << "x, Query: " << query_ratio << "x\n\n";

    std::cout << "  Array: Insert=" << array_insert << "us, Query="
              << array_query << "us" << std::endl;
    std::cout << "  Hash:  Insert=" << hash_insert << "us, Query="
              << hash_query << "us" << std::endl;
    std::cout << "  Array/Hash: Insert=" << insert_ratio
              << "x, Query=" << query_ratio << "x" << std::endl;
  }

  result_file << "============================================\n";
  result_file << "Test completed\n";
  result_file.close();

  std::cout << "Results written to threshold_results.txt" << std::endl;
  return 0;
}
