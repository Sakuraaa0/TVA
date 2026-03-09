// Mybench with persistence enabled
#include "Column/ColumbTable.hpp"
#include "CtStore.hpp"
#include "flags.hpp"
#include "persistence/PersistManager.hpp"
#include "types.hpp"
#include <chrono>
#include <fstream>
#include <gflags/gflags.h>
#include <sys/resource.h>
#include <vector>

// 持久化模式选项
DEFINE_int32(persist_mode, 0, "0=memory_only, 1=pointer_mode, 2=full_mode");
DEFINE_bool(wal_enabled, true, "Enable WAL logging");
DEFINE_string(data_dir, "./persist_data", "Persistence data directory");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string num = "32";
  std::ofstream result_file(
      "/workspace/CtGraph/experiment/mybench/mybench_result_persist" + num + ".txt",
      std::ios::out | std::ios::trunc);
  if (!result_file.is_open()) {
    std::cerr << "无法打开结果文件" << std::endl;
    return 1;
  }

  ctgraph::VertexId_t vertex_id = 26000; // num=32

  // 配置持久化
  ctgraph::persistence::PersistConfig config;
  config.mode = static_cast<ctgraph::persistence::PersistMode>(FLAGS_persist_mode);
  config.wal_enabled = FLAGS_wal_enabled;
  config.wal_dir = FLAGS_data_dir + "/wal";
  config.data_dir = FLAGS_data_dir + "/data";
  config.rocksdb_dir = FLAGS_data_dir + "/rocksdb";

  ctgraph::persistence::PersistManager persist_mgr(config);
  if (!persist_mgr.init()) {
    std::cerr << "持久化初始化失败" << std::endl;
    return 1;
  }

  result_file << "持久化模式: " << FLAGS_persist_mode
              << " (0=内存, 1=指针, 2=RocksDB)" << std::endl;
  result_file << "WAL: " << (FLAGS_wal_enabled ? "开启" : "关闭") << std::endl;

  // 创建存储并设置持久化管理器
  ctgraph::CtStore ct_store(vertex_id);
  column::GraphDB db;

  if (config.mode != ctgraph::persistence::PersistMode::MEMORY_ONLY) {
    ct_store.setPersistManager(&persist_mgr);
    db.setPersistManager(&persist_mgr);
  }

  std::unique_ptr<FixString> s = std::make_unique<FixString>("");
  for (int i = 0; i < vertex_id; i++) {
    ct_store.load_vertex(i);
  }

  // 导入初始数据
  auto load_start = std::chrono::high_resolution_clock::now();
  std::ifstream file("/workspace/dataset/mybench/small.cypher");
  std::string line;
  while (std::getline(file, line)) {
    if (line.find("CREATE (:User") != std::string::npos) {
      size_t id_pos = line.find("id: ") + 4;
      size_t id_end = line.find(",", id_pos);
      int id = std::stoi(line.substr(id_pos, id_end - id_pos));

      size_t completion_pos = line.find("completion_percentage: ") + 22;
      size_t completion_end = line.find(",", completion_pos);
      int completion = std::stoi(line.substr(completion_pos, completion_end - completion_pos));

      size_t gender_pos = line.find("gender: \"") + 9;
      size_t gender_end = line.find("\"", gender_pos);
      std::string gender = line.substr(gender_pos, gender_end - gender_pos);
      int gender_val = (gender == "man") ? 1 : 0;

      size_t age_pos = line.find(", age: ") + 7;
      size_t age_end = line.find("}", age_pos);
      int age = std::stoi(line.substr(age_pos, age_end - age_pos));

      db.LoadTable(0, id,
                   {{"completion_percentage", completion}, {"gender", gender_val}, {"age", age}},
                   {}, {});
    } else if (line.find("MATCH") != std::string::npos && line.find("Friend") != std::string::npos) {
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

  // 导入操作数据
  std::string file2_name = "/workspace/dataset/mybench/" + num + "/cypher.txt";
  std::ifstream file2(file2_name);
  std::string line2;
  int line_number = 0;

  struct rusage usage;
  getrusage(RUSAGE_SELF, &usage);
  long start_memory = usage.ru_maxrss;

  while (std::getline(file2, line2)) {
    line_number++;
    if (line2.find("MATCH (n:User") != std::string::npos &&
        line2.find("SET n.completion_percentage") != std::string::npos) {
      size_t id_pos = line2.find("id:") + 3;
      size_t id_end = line2.find("}", id_pos);
      int id = std::stoi(line2.substr(id_pos, id_end - id_pos));

      size_t completion_pos = line2.find("= ") + 2;
      size_t completion_end = line2.find(";", completion_pos);
      int completion = std::stoi(line2.substr(completion_pos, completion_end - completion_pos));

      db.update(line_number, id, {{"completion_percentage", completion}}, {}, {});
    } else if (line2.find("CREATE (n:User") != std::string::npos &&
               line2.find("set n.completion_percentage") != std::string::npos) {
      size_t id_pos = line2.find("id : ") + 5;
      size_t id_end = line2.find("}", id_pos);
      int id = std::stoi(line2.substr(id_pos, id_end - id_pos));

      size_t completion_pos = line2.find("= ") + 2;
      size_t completion_end = line2.find(" RETURN", completion_pos);
      int completion = std::stoi(line2.substr(completion_pos, completion_end - completion_pos));

      db.LoadTable(line_number, id,
                   {{"completion_percentage", completion}, {"gender", 2}, {"age", 0}}, {}, {});
    } else if (line2.find("MATCH (a:User") != std::string::npos &&
               line2.find("CREATE (a)-[:Friend]->(b)") != std::string::npos) {
      size_t src_pos = line2.find("id:") + 3;
      size_t src_end = line2.find("}", src_pos);
      int src = std::stoi(line2.substr(src_pos, src_end - src_pos));

      size_t dst_pos = line2.find("id: ", src_end) + 4;
      size_t dst_end = line2.find("}", dst_pos);
      int dst = std::stoi(line2.substr(dst_pos, dst_end - dst_pos));

      ct_store.put_edge_withTs(src, dst, *s.get(), line_number);
      ct_store.put_edge_withTs(dst, src, *s.get(), line_number);
    } else if (line2.find("MATCH (n:User") != std::string::npos &&
               line2.find("DELETE e") != std::string::npos) {
      size_t src_pos = line2.find("id:") + 3;
      size_t src_end = line2.find("}", src_pos);
      int src = std::stoi(line2.substr(src_pos, src_end - src_pos));

      size_t dst_pos = line2.find("id:", src_end) + 3;
      size_t dst_end = line2.find("}", dst_pos);
      int dst = std::stoi(line2.substr(dst_pos, dst_end - dst_pos));

      ct_store.put_edge_withTs(src, dst, *s.get(), line_number);
      ct_store.put_edge_withTs(dst, src, *s.get(), line_number);
    }
  }
  file2.close();

  // 刷新WAL
  if (config.wal_enabled) {
    persist_mgr.wal().flush();
  }

  auto load_end = std::chrono::high_resolution_clock::now();
  getrusage(RUSAGE_SELF, &usage);
  long end_memory = usage.ru_maxrss;

  auto load_duration = std::chrono::duration_cast<std::chrono::milliseconds>(load_end - load_start);
  result_file << "数据加载时间: " << load_duration.count() << " 毫秒" << std::endl;
  result_file << "内存消耗: " << (end_memory - start_memory) / 1024.0 << " MB" << std::endl;

  // Q1查询
  result_file << "\n开始执行Q1查询..." << std::endl;
  auto start_q1 = std::chrono::high_resolution_clock::now();
  std::string file3_name = "/workspace/dataset/mybench/" + num + "/cypher_Q1.txt";
  std::ifstream file3(file3_name);
  std::string line3;
  int add = 1;
  while (std::getline(file3, line3)) {
    if (line3.find("MATCH (r:User{id : ") != std::string::npos &&
        line3.find("TT AS ") != std::string::npos) {
      size_t id_pos = line3.find("id : ") + 5;
      size_t id_end = line3.find("}", id_pos);
      int id = std::stoi(line3.substr(id_pos, id_end - id_pos));

      size_t ts_pos = line3.find("TT AS ") + 6;
      size_t ts_end = line3.find(" ", ts_pos);
      int timestamp = std::stoi(line3.substr(ts_pos, ts_end - ts_pos));

      std::unordered_map<std::string, std::any> vps, eps, ves;
      vps["completion_percentage"] = std::any();
      vps["age"] = std::any();
      vps["gender"] = std::any();
      db.getVersionInRange(timestamp, id + add, vps, eps, ves);
      add = add ^ 1;
    }
  }
  file3.close();
  auto end_q1 = std::chrono::high_resolution_clock::now();
  auto duration_q1 = std::chrono::duration_cast<std::chrono::microseconds>(end_q1 - start_q1);
  result_file << "Q1查询执行时间: " << duration_q1.count() << " 微秒" << std::endl;

  // Q2查询
  result_file << "\n开始执行Q2查询..." << std::endl;
  auto start_q2 = std::chrono::high_resolution_clock::now();
  std::string fileq2_name = "/workspace/dataset/mybench/" + num + "/cypher_Q2.txt";
  std::ifstream fileq2(fileq2_name);
  std::string lineq2;
  while (std::getline(fileq2, lineq2)) {
    if (lineq2.find("MATCH (r:User{id : ") != std::string::npos &&
        lineq2.find("TT FROM ") != std::string::npos) {
      size_t id_pos = lineq2.find("id : ") + 5;
      size_t id_end = lineq2.find("}", id_pos);
      int id = std::stoi(lineq2.substr(id_pos, id_end - id_pos));

      size_t from_pos = lineq2.find("TT FROM ") + 8;
      size_t from_end = lineq2.find(" TO ", from_pos);
      int from_ts = std::stoi(lineq2.substr(from_pos, from_end - from_pos));

      size_t to_pos = lineq2.find(" TO ") + 4;
      size_t to_end = lineq2.find(" ", to_pos);
      int to_ts = std::stoi(lineq2.substr(to_pos, to_end - to_pos));

      std::unordered_map<std::string, std::any> vps, eps, ves;
      vps["completion_percentage"] = std::any();
      vps["age"] = std::any();
      vps["gender"] = std::any();
      db.getVersionInRange(from_ts, id + add, vps, eps, ves);
      db.getVersionInRange(to_ts, id + add, vps, eps, ves);
      add = add ^ 1;
    }
  }
  fileq2.close();
  auto end_q2 = std::chrono::high_resolution_clock::now();
  auto duration_q2 = std::chrono::duration_cast<std::chrono::microseconds>(end_q2 - start_q2);
  result_file << "Q2查询执行时间: " << duration_q2.count() << " 微秒" << std::endl;

  // Q3查询
  result_file << "\n开始执行Q3查询..." << std::endl;
  auto start_q3 = std::chrono::high_resolution_clock::now();
  std::string file4_name = "/workspace/dataset/mybench/" + num + "/cypher_Q3.txt";
  std::ifstream file4(file4_name);
  std::string line4;
  while (std::getline(file4, line4)) {
    if (line4.find("MATCH (n:User{id : ") != std::string::npos &&
        line4.find("TT AS ") != std::string::npos) {
      size_t id_pos = line4.find("id : ") + 5;
      size_t id_end = line4.find("}", id_pos);
      int id = std::stoi(line4.substr(id_pos, id_end - id_pos));

      size_t ts_pos = line4.find("TT AS ") + 6;
      size_t ts_end = line4.find(" ", ts_pos);
      uint64_t timestamp = std::stoull(line4.substr(ts_pos, ts_end - ts_pos));

      std::vector<ctgraph::Edge> edges;
      ct_store.get_edges_withTs(1870, edges, timestamp);
      std::unordered_map<std::string, std::any> vps, eps, ves;
      vps["completion_percentage"] = std::any();
      vps["age"] = std::any();
      vps["gender"] = std::any();
      db.getVersionInRange(timestamp, 1870, vps, eps, ves);
      for (auto edge : edges) {
        db.getVersionInRange(timestamp, edge.destination(), vps, eps, ves);
      }
    }
  }
  file4.close();
  auto end_q3 = std::chrono::high_resolution_clock::now();
  auto duration_q3 = std::chrono::duration_cast<std::chrono::microseconds>(end_q3 - start_q3);
  result_file << "Q3查询执行时间: " << duration_q3.count() << " 微秒" << std::endl;

  // Q4查询
  result_file << "\n开始执行Q4查询..." << std::endl;
  auto start_q4 = std::chrono::high_resolution_clock::now();
  std::string fileq4_name = "/workspace/dataset/mybench/" + num + "/cypher_Q4.txt";
  std::ifstream fileq4(fileq4_name);
  std::string lineq4;
  while (std::getline(fileq4, lineq4)) {
    if (lineq4.find("MATCH (n:User{id : ") != std::string::npos &&
        lineq4.find("TT FROM ") != std::string::npos) {
      size_t id_pos = lineq4.find("id : ") + 5;
      size_t id_end = lineq4.find("}", id_pos);
      int id = std::stoi(lineq4.substr(id_pos, id_end - id_pos));

      size_t from_pos = lineq4.find("TT FROM ") + 8;
      size_t from_end = lineq4.find(" TO ", from_pos);
      int from_ts = std::stoi(lineq4.substr(from_pos, from_end - from_pos));

      size_t to_pos = lineq4.find(" TO ") + 4;
      size_t to_end = lineq4.find(" ", to_pos);
      int to_ts = std::stoi(lineq4.substr(to_pos, to_end - to_pos));

      std::vector<ctgraph::Edge> edges;
      std::unordered_map<std::string, std::any> vps, eps, ves;
      vps["completion_percentage"] = std::any();
      vps["age"] = std::any();
      vps["gender"] = std::any();

      ct_store.get_edges_withTs(id, edges, from_ts);
      db.getVersionInRange(from_ts, id, vps, eps, ves);
      db.getVersionInRange(to_ts, id, vps, eps, ves);
      for (auto edge : edges) {
        db.getVersionInRange(from_ts, edge.destination(), vps, eps, ves);
      }
      edges.clear();
      ct_store.get_edges_withTs(id, edges, to_ts);
      for (auto edge : edges) {
        db.getVersionInRange(to_ts, edge.destination(), vps, eps, ves);
      }
    }
  }
  fileq4.close();
  auto end_q4 = std::chrono::high_resolution_clock::now();
  auto duration_q4 = std::chrono::duration_cast<std::chrono::microseconds>(end_q4 - start_q4);
  result_file << "Q4查询执行时间: " << duration_q4.count() << " 微秒" << std::endl;

  auto total_duration = duration_q1 + duration_q2 + duration_q3 + duration_q4;
  result_file << "\n总查询时间: " << total_duration.count() << " 微秒" << std::endl;

  persist_mgr.close();
  result_file.close();
  std::cout << "测试完成，结果已写入文件" << std::endl;

  return 0;
}
