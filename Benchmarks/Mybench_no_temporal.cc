// Created by lwh on 25-2-24.
// No temporal version - only loads initial data, all timestamps set to 0

#include "Column/ColumbTable.hpp"
#include "CtStore.hpp"
#include "flags.hpp"
#include "hopscotch/hopscotchhash.hpp"
#include "types.hpp"
#include <chrono>
#include <fstream>
#include <gflags/gflags.h>
#include <omp.h>
#include <set>
#include <sstream>
#include <sys/resource.h>
#include <unordered_set>
#include <vector>

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << "reserve node num of hashtable" << FLAGS_reserve_node
            << std::endl;

  std::string num = "32";

  std::ofstream result_file(
      "/workspace/CtGraph/experiment/mybench/mybench_result_no_temporal_" + num +
          ".txt",
      std::ios::out | std::ios::trunc);
  if (!result_file.is_open()) {
    std::cerr << "无法打开结果文件" << std::endl;
    return 1;
  }

  ctgraph::VertexId_t vertex_id;
  if (num == "8") {
    vertex_id = 15000;
  } else if (num == "16") {
    vertex_id = 18000;
  } else if (num == "24") {
    vertex_id = 22000;
  } else if (num == "32") {
    vertex_id = 26000;
  } else if (num == "40") {
    vertex_id = 30000;
  }

  ctgraph::CtStore ct_store(vertex_id);
  column::GraphDB db;
  std::unique_ptr<FixString> s = std::make_unique<FixString>("");
  for (int i = 0; i < vertex_id; i++) {
    ct_store.load_vertex(i);
  }

  auto start_time = std::chrono::high_resolution_clock::now();
  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  long start_memory = usage.ru_maxrss;

  // 只导入初始数据
  std::ifstream file("/workspace/dataset/mybench/small.cypher");
  std::string line;
  while (std::getline(file, line)) {
    if (line.find("CREATE (:User") != std::string::npos) {
      size_t id_pos = line.find("id: ") + 4;
      size_t id_end = line.find(",", id_pos);
      int id = std::stoi(line.substr(id_pos, id_end - id_pos));

      size_t completion_pos = line.find("completion_percentage: ") + 22;
      size_t completion_end = line.find(",", completion_pos);
      int completion = std::stoi(
          line.substr(completion_pos, completion_end - completion_pos));

      size_t gender_pos = line.find("gender: \"") + 9;
      size_t gender_end = line.find("\"", gender_pos);
      std::string gender = line.substr(gender_pos, gender_end - gender_pos);
      int gender_val = (gender == "man") ? 1 : 0;

      size_t age_pos = line.find(", age: ") + 7;
      size_t age_end = line.find("}", age_pos);
      int age = std::stoi(line.substr(age_pos, age_end - age_pos));

      db.LoadTable(0, id,
                   {{"completion_percentage", completion},
                    {"gender", gender_val},
                    {"age", age}},
                   {}, {});
    } else if (line.find("MATCH") != std::string::npos &&
               line.find("Friend") != std::string::npos) {
      size_t src_pos = line.find("id: ") + 4;
      size_t src_end = line.find("}", src_pos);
      int src = std::stoi(line.substr(src_pos, src_end - src_pos));

      size_t dst_pos = line.find("id: ", src_end) + 4;
      size_t dst_end = line.find("}", dst_pos);
      int dst = std::stoi(line.substr(dst_pos, dst_end - dst_pos));

      ct_store.put_edge_withTs(src, dst, *s.get(), 0);
      ct_store.put_edge_withTs(dst, src, *s.get(), 0);
    }
  }
  file.close();

  auto end_time = std::chrono::high_resolution_clock::now();
  getrusage(RUSAGE_SELF, &usage);
  long end_memory = usage.ru_maxrss;

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
  result_file << "执行时间: " << duration.count() << " 毫秒" << std::endl;
  result_file << "内存消耗: " << (end_memory - start_memory) / 1024.0 << " MB"
              << std::endl;

  // Q1查询 - 时间戳设为0
  int add = 1;
  result_file << "\n开始执行Q1查询..." << std::endl;
  auto start_q1 = std::chrono::high_resolution_clock::now();

  std::string file3_name =
      "/workspace/dataset/mybench/" + num + "/cypher_Q1.txt";
  std::ifstream file3(file3_name);
  std::string line3;
  while (std::getline(file3, line3)) {
    if (line3.find("MATCH (r:User{id : ") != std::string::npos &&
        line3.find("TT AS ") != std::string::npos) {
      size_t id_pos = line3.find("id : ") + 5;
      size_t id_end = line3.find("}", id_pos);
      int id = std::stoi(line3.substr(id_pos, id_end - id_pos));

      std::unordered_map<std::string, std::any> vps;
      std::unordered_map<std::string, std::any> eps;
      std::unordered_map<std::string, std::any> ves;
      vps["completion_percentage"] = std::any();
      vps["age"] = std::any();
      vps["gender"] = std::any();
      db.getVersionInRange(0, id + add, vps, eps, ves);
      add = add ^ 1;
    }
  }
  file3.close();
  auto end_q1 = std::chrono::high_resolution_clock::now();
  auto duration_q1 =
      std::chrono::duration_cast<std::chrono::microseconds>(end_q1 - start_q1);
  result_file << "Q1查询执行时间: " << duration_q1.count() << " 微秒"
              << std::endl;

  // Q2查询 - 时间戳设为0
  result_file << "\n开始执行Q2查询..." << std::endl;
  auto start_q2 = std::chrono::high_resolution_clock::now();

  std::string fileq2_name =
      "/workspace/dataset/mybench/" + num + "/cypher_Q2.txt";
  std::ifstream fileq2(fileq2_name);
  std::string lineq2;
  while (std::getline(fileq2, lineq2)) {
    if (lineq2.find("MATCH (r:User{id : ") != std::string::npos &&
        lineq2.find("TT FROM ") != std::string::npos) {
      size_t id_pos = lineq2.find("id : ") + 5;
      size_t id_end = lineq2.find("}", id_pos);
      int id = std::stoi(lineq2.substr(id_pos, id_end - id_pos));

      std::unordered_map<std::string, std::any> vps;
      std::unordered_map<std::string, std::any> eps;
      std::unordered_map<std::string, std::any> ves;
      vps["completion_percentage"] = std::any();
      vps["age"] = std::any();
      vps["gender"] = std::any();

      db.getVersionInRange(0, id + add, vps, eps, ves);
      db.getVersionInRange(0, id + add, vps, eps, ves);
      add = add ^ 1;
    }
  }
  fileq2.close();
  auto end_q2 = std::chrono::high_resolution_clock::now();
  auto duration_q2 =
      std::chrono::duration_cast<std::chrono::microseconds>(end_q2 - start_q2);
  result_file << "Q2查询执行时间: " << duration_q2.count() << " 微秒"
              << std::endl;

  // Q3查询 - 时间戳设为0
  result_file << "\n开始执行Q3查询..." << std::endl;
  auto start_q3 = std::chrono::high_resolution_clock::now();
  std::string file4_name =
      "/workspace/dataset/mybench/" + num + "/cypher_Q3.txt";
  std::ifstream file4(file4_name);
  std::string line4;
  while (std::getline(file4, line4)) {
    if (line4.find("MATCH (n:User{id : ") != std::string::npos &&
        line4.find("TT AS ") != std::string::npos) {
      size_t id_pos = line4.find("id : ") + 5;
      size_t id_end = line4.find("}", id_pos);
      int id = std::stoi(line4.substr(id_pos, id_end - id_pos));

      std::vector<ctgraph::Edge> edges;
      ct_store.get_edges_withTs(1870, edges, 0);
      std::unordered_map<std::string, std::any> vps;
      std::unordered_map<std::string, std::any> eps;
      std::unordered_map<std::string, std::any> ves;
      vps["completion_percentage"] = std::any();
      vps["age"] = std::any();
      vps["gender"] = std::any();
      db.getVersionInRange(0, 1870, vps, eps, ves);
      for (auto edge : edges) {
        db.getVersionInRange(0, edge.destination(), vps, eps, ves);
      }
    }
  }
  file4.close();
  auto end_q3 = std::chrono::high_resolution_clock::now();
  auto duration_q3 =
      std::chrono::duration_cast<std::chrono::microseconds>(end_q3 - start_q3);
  result_file << "Q3查询执行时间: " << duration_q3.count() << " 微秒"
              << std::endl;

  // Q4查询 - 时间戳设为0
  result_file << "\n开始执行Q4查询..." << std::endl;
  auto start_q4 = std::chrono::high_resolution_clock::now();
  std::string fileq4_name =
      "/workspace/dataset/mybench/" + num + "/cypher_Q4.txt";
  std::ifstream fileq4(fileq4_name);
  std::string lineq4;
  while (std::getline(fileq4, lineq4)) {
    if (lineq4.find("MATCH (n:User{id : ") != std::string::npos &&
        lineq4.find("TT FROM ") != std::string::npos) {
      size_t id_pos = lineq4.find("id : ") + 5;
      size_t id_end = lineq4.find("}", id_pos);
      int id = std::stoi(lineq4.substr(id_pos, id_end - id_pos));

      std::vector<ctgraph::Edge> edges;
      std::unordered_map<std::string, std::any> vps;
      std::unordered_map<std::string, std::any> eps;
      std::unordered_map<std::string, std::any> ves;
      vps["completion_percentage"] = std::any();
      vps["age"] = std::any();
      vps["gender"] = std::any();

      ct_store.get_edges_withTs(id, edges, 0);
      db.getVersionInRange(0, id, vps, eps, ves);
      db.getVersionInRange(0, id, vps, eps, ves);
      for (auto edge : edges) {
        db.getVersionInRange(0, edge.destination(), vps, eps, ves);
      }
      edges.clear();
      ct_store.get_edges_withTs(id, edges, 0);
      for (auto edge : edges) {
        db.getVersionInRange(0, edge.destination(), vps, eps, ves);
      }
    }
  }
  fileq4.close();
  auto end_q4 = std::chrono::high_resolution_clock::now();
  auto duration_q4 =
      std::chrono::duration_cast<std::chrono::microseconds>(end_q4 - start_q4);
  result_file << "Q4查询执行时间: " << duration_q4.count() << " 微秒"
              << std::endl;

  auto total_duration = duration_q1 + duration_q2 + duration_q3 + duration_q4;
  result_file << "\n总执行时间: " << total_duration.count() << " 微秒"
              << std::endl;

  result_file.close();

  return 0;
}
