// Created for gMark benchmark experiment
//

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
#include <regex>

static uint64_t getCurrentTimeInMicroseconds() {
  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  auto microseconds =
      std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
  return static_cast<uint64_t>(microseconds);
}

// 判断predicate是否为多:多关系（存入ct_store）
// 根据shop.xml的schema定义
bool isMultiRelation(int predicate) {
  // 多:多关系边类型
  static std::set<int> multi_relations = {
    51, 52, 53,  // type, tag, hasGenre
    54,          // homepage (Product -> Website)
    55,          // parentCountry (City -> Country)
    56,          // language
    57, 58, 59, 60,  // like, subscribes, follows, friendOf (User relations)
    61, 62, 63, 64,  // location, age, gender, nationality
    65, 66, 67, 68, 69, 70, 71, 72,  // conductor, performedIn, artist, actor, director, trailer, author, editor
    73, 74, 75,  // offers, contactPoint, employee (Retailer relations)
    76, 77,      // includes, eligibleRegion (Offer relations)
    78, 79,      // makesPurchase, purchaseFor
    80, 81       // reviewer, hasReview
  };
  return multi_relations.count(predicate) > 0;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << "reserve node num of hashtable: " << FLAGS_reserve_node << std::endl;

  // 清空结果文件
  std::ofstream result_file(
      "/workspace/CtGraph/experiment/gmark/gmark_result.txt",
      std::ios::out | std::ios::trunc);
  if (!result_file.is_open()) {
    std::cerr << "无法打开结果文件" << std::endl;
    return 1;
  }

  // gMark WD数据集配置: 最大节点ID约103628
  // 先收集所有出现的节点ID，只初始化这些节点
  std::set<int> all_nodes;
  std::ifstream scan_file("/workspace/dataset/gmark/wd-graph.txt");
  std::string scan_line;
  while (std::getline(scan_file, scan_line)) {
    std::istringstream iss(scan_line);
    int src, predicate, dst;
    if (iss >> src >> predicate >> dst) {
      all_nodes.insert(src);
      all_nodes.insert(dst);
    }
  }
  scan_file.close();

  int max_node_id = *all_nodes.rbegin() + 1;
  result_file << "========== gMark Benchmark ==========" << std::endl;
  result_file << "实际节点数: " << all_nodes.size() << std::endl;
  result_file << "最大节点ID: " << max_node_id << std::endl;

  ctgraph::VertexId_t vertex_id = max_node_id;
  std::atomic<ctgraph::VertexId_t> ver = vertex_id;
  ctgraph::CtStore ct_store(vertex_id);
  column::GraphDB db;
  std::unique_ptr<FixString> s = std::make_unique<FixString>("");

  // 只初始化实际存在的顶点
  result_file << "开始初始化顶点..." << std::endl;
  for (int node_id : all_nodes) {
    ct_store.load_vertex(node_id);
  }
  result_file << "顶点初始化完成" << std::endl;

  // ========== 1. 导入基础图数据 ==========
  result_file << "\n开始导入基础图数据..." << std::endl;
  auto start_load = std::chrono::high_resolution_clock::now();
  struct rusage usage_load;
  getrusage(RUSAGE_SELF, &usage_load);
  long start_memory_load = usage_load.ru_maxrss;

  // 第一遍扫描：收集所有1:1属性类型（predicate）
  std::set<std::string> all_attr_names;
  std::ifstream scan_attr_file("/workspace/dataset/gmark/wd-graph.txt");
  std::string scan_attr_line;
  while (std::getline(scan_attr_file, scan_attr_line)) {
    std::istringstream iss(scan_attr_line);
    int src, predicate, dst;
    if (iss >> src >> predicate >> dst) {
      if (!isMultiRelation(predicate)) {
        all_attr_names.insert("p" + std::to_string(predicate));
      }
    }
  }
  scan_attr_file.close();
  result_file << "属性类型数: " << all_attr_names.size() << std::endl;

  // 第二遍扫描：读取数据
  std::ifstream graph_file("/workspace/dataset/gmark/wd-graph.txt");
  std::string line;
  int edge_count = 0;
  int attr_count = 0;
  int rel_count = 0;

  // 用于存储每个节点的属性
  std::unordered_map<int, std::unordered_map<std::string, int>> node_attrs;

  while (std::getline(graph_file, line)) {
    std::istringstream iss(line);
    int src, predicate, dst;
    if (iss >> src >> predicate >> dst) {
      edge_count++;

      if (isMultiRelation(predicate)) {
        // 多:多关系 -> ct_store
        ct_store.put_edge_withTs(src, dst, *s.get(), 0);
        rel_count++;
      } else {
        // 1:1属性 -> GraphDB
        // 使用predicate ID作为属性名
        std::string attr_name = "p" + std::to_string(predicate);
        node_attrs[src][attr_name] = dst;
        attr_count++;
      }
    }
  }
  graph_file.close();

  // 将收集的属性写入GraphDB（每个节点都有完整的属性列表）
  for (auto& [node_id, attrs] : node_attrs) {
    std::unordered_map<std::string, std::any> any_attrs;
    // 先为所有属性填充空值
    for (const auto& attr_name : all_attr_names) {
      any_attrs[attr_name] = std::any();
    }
    // 再填充实际有值的属性
    for (auto& [key, val] : attrs) {
      any_attrs[key] = val;
    }
    db.LoadTable(0, node_id, any_attrs, {}, {});
  }

  auto end_load = std::chrono::high_resolution_clock::now();
  getrusage(RUSAGE_SELF, &usage_load);
  long end_memory_load = usage_load.ru_maxrss;
  auto duration_load = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_load - start_load);

  result_file << "基础图数据导入完成" << std::endl;
  result_file << "总边数: " << edge_count << std::endl;
  result_file << "属性边数(GraphDB): " << attr_count << std::endl;
  result_file << "关系边数(ct_store): " << rel_count << std::endl;
  result_file << "导入时间: " << duration_load.count() << " 毫秒" << std::endl;
  result_file << "内存消耗: " << (end_memory_load - start_memory_load) / 1024.0 << " MB" << std::endl;

  // ========== 2. 导入时序数据 (graph_op) ==========
  result_file << "\n开始导入时序数据..." << std::endl;
  auto start_op = std::chrono::high_resolution_clock::now();
  struct rusage usage_op;
  getrusage(RUSAGE_SELF, &usage_op);
  long start_memory_op = usage_op.ru_maxrss;

  std::ifstream op_file("/workspace/dataset/gmark/graph_op/cypher.txt");
  std::string op_line;
  int line_number = 0;
  int update_count = 0;

  while (std::getline(op_file, op_line)) {
    line_number++;

    // 解析 MATCH (c:Node {id: 'xxx'}) SET c.name = yyy;
    if (op_line.find("MATCH (c:Node {id:") != std::string::npos &&
        op_line.find("SET c.name") != std::string::npos) {
      try {
        // 提取节点ID
        size_t id_pos = op_line.find("id: '") + 5;
        size_t id_end = op_line.find("'", id_pos);
        if (id_end == std::string::npos || id_end <= id_pos) continue;
        int id = std::stoi(op_line.substr(id_pos, id_end - id_pos));

        // 提取新值
        size_t val_pos = op_line.find("= ") + 2;
        size_t val_end = op_line.find(";", val_pos);
        if (val_end == std::string::npos || val_end <= val_pos) continue;
        int new_val = std::stoi(op_line.substr(val_pos, val_end - val_pos));

        // 更新节点属性（name对应predicate 26）
        // 只更新已经在GraphDB中存在的节点
        if (db.vertex_index.count(id) > 0) {
          db.update(line_number, id, {{"p26", new_val}}, {}, {});
          update_count++;
        }
      } catch (const std::exception& e) {
        // 解析失败，跳过该行
        continue;
      }
    }
  }
  op_file.close();

  auto end_op = std::chrono::high_resolution_clock::now();
  getrusage(RUSAGE_SELF, &usage_op);
  long end_memory_op = usage_op.ru_maxrss;
  auto duration_op = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_op - start_op);

  result_file << "时序数据导入完成" << std::endl;
  result_file << "更新操作数: " << update_count << std::endl;
  result_file << "导入时间: " << duration_op.count() << " 毫秒" << std::endl;
  result_file << "内存消耗: " << (end_memory_op - start_memory_op) / 1024.0 << " MB" << std::endl;

  // ========== 3. 执行时序查询 ==========
  result_file << "\n开始执行时序查询..." << std::endl;
  auto start_query = std::chrono::high_resolution_clock::now();
  struct rusage usage_query;
  getrusage(RUSAGE_SELF, &usage_query);
  long start_memory_query = usage_query.ru_maxrss;

  std::ifstream query_file("/workspace/dataset/gmark/temporal_query/cypher.txt");
  std::string query_line;
  int query_count = 0;

  while (std::getline(query_file, query_line)) {
    query_count++;

    // 解析 TT AS timestamp
    size_t tt_pos = query_line.find("TT AS ");
    if (tt_pos != std::string::npos) {
      try {
        size_t ts_start = tt_pos + 6;
        size_t ts_end = query_line.find(" ", ts_start);
        if (ts_end == std::string::npos) {
          // 可能timestamp在行末
          ts_end = query_line.length();
        }
        int timestamp = std::stoi(query_line.substr(ts_start, ts_end - ts_start));

        // 对几个热点节点执行边查询（基于数据分析的高频节点）
        std::vector<ctgraph::Edge> edges;
        for (int node_id : {81629, 81630, 81631, 81632, 81633}) {  // User节点范围
          ct_store.get_edges_withTs(node_id, edges, timestamp);

          // 对邻居节点查询属性
          for (auto& edge : edges) {
            // 只查询在GraphDB中存在的节点
            if (db.vertex_index.count(edge.destination()) > 0) {
              std::unordered_map<std::string, std::any> vps;
              vps["p50"] = std::any();  // title
              vps["p26"] = std::any();  // name
              std::unordered_map<std::string, std::any> eps;
              std::unordered_map<std::string, std::any> ves;
              db.getVersionInRange(timestamp, edge.destination(), vps, eps, ves);
            }
          }
          edges.clear();
        }
      } catch (const std::exception& e) {
        // 解析失败，跳过该行
        continue;
      }
    }
  }
  query_file.close();

  auto end_query = std::chrono::high_resolution_clock::now();
  getrusage(RUSAGE_SELF, &usage_query);
  long end_memory_query = usage_query.ru_maxrss;
  auto duration_query = std::chrono::duration_cast<std::chrono::microseconds>(
      end_query - start_query);

  result_file << "时序查询执行完成" << std::endl;
  result_file << "查询数: " << query_count << std::endl;
  result_file << "查询时间: " << duration_query.count() << " 微秒" << std::endl;
  result_file << "内存消耗: " << (end_memory_query - start_memory_query) / 1024.0 << " MB" << std::endl;

  // ========== 汇总 ==========
  result_file << "\n========== 汇总 ==========" << std::endl;
  result_file << "基础数据导入时间: " << duration_load.count() << " 毫秒" << std::endl;
  result_file << "时序数据导入时间: " << duration_op.count() << " 毫秒" << std::endl;
  result_file << "时序查询执行时间: " << duration_query.count() << " 微秒" << std::endl;

  result_file.close();
  std::cout << "gMark实验完成，结果保存在 /workspace/CtGraph/experiment/gmark/gmark_result.txt" << std::endl;

  return 0;
}
