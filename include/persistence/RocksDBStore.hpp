#pragma once
#include "PersistConfig.hpp"
#include <any>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

// 前向声明，避免直接依赖rocksdb头文件
namespace rocksdb {
class DB;
class Options;
class Iterator;
} // namespace rocksdb

namespace ctgraph {
namespace persistence {

class RocksDBStore {
public:
  RocksDBStore(const PersistConfig &config) : config_(config), db_(nullptr) {}
  ~RocksDBStore();

  bool open();
  void close();

  // Key格式化
  static std::string userKey(uint64_t uid, uint64_t ts) {
    char buf[64];
    snprintf(buf, sizeof(buf), "user:%08lu:ts:%010lu", uid, ts);
    return buf;
  }

  static std::string friendKey(uint64_t src, uint64_t dst, uint64_t ts) {
    char buf[80];
    snprintf(buf, sizeof(buf), "friend:%08lu:%08lu:ts:%010lu", src, dst, ts);
    return buf;
  }

  // 写入用户数据
  bool putUser(uint64_t uid, uint64_t ts,
               const std::unordered_map<std::string, std::any> &props);

  // 写入好友关系
  bool putFriend(uint64_t src, uint64_t dst, uint64_t ts, bool exists);

  // 查询指定时间点的用户数据
  bool getUser(uint64_t uid, uint64_t ts,
               std::unordered_map<std::string, std::any> &props);

  // 查询指定时间点的好友列表
  bool getFriends(uint64_t uid, uint64_t ts, std::vector<uint64_t> &friends);

  // 查询时间范围内的用户数据
  bool getUserInRange(uint64_t uid, uint64_t from_ts, uint64_t to_ts,
                      std::vector<std::unordered_map<std::string, std::any>> &results);

private:
  std::string serializeProps(const std::unordered_map<std::string, std::any> &props);
  void deserializeProps(const std::string &data,
                        std::unordered_map<std::string, std::any> &props);

  PersistConfig config_;
  rocksdb::DB *db_;
};

} // namespace persistence
} // namespace ctgraph
