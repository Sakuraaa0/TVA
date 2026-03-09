#pragma once
#include <string>

namespace ctgraph {
namespace persistence {

enum class PersistMode {
  MEMORY_ONLY = 0,  // 纯内存，不落盘
  POINTER_MODE = 1, // 指针模式：内存结构+属性值落盘
  FULL_MODE = 2     // 全量模式：全部存入RocksDB
};

struct PersistConfig {
  PersistMode mode = PersistMode::MEMORY_ONLY;
  bool wal_enabled = false;
  std::string wal_dir = "./wal";
  std::string data_dir = "./data";
  std::string rocksdb_dir = "./rocksdb";
  size_t wal_buffer_size = 4 * 1024 * 1024; // 4MB
  size_t sync_interval_ms = 1000;           // 1秒同步一次
};

} // namespace persistence
} // namespace ctgraph
