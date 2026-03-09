#pragma once
#include "DiskStore.hpp"
#include "PersistConfig.hpp"
#include "RocksDBStore.hpp"
#include "WAL.hpp"
#include <memory>

namespace ctgraph {
namespace persistence {

// 统一持久化管理器
class PersistManager {
public:
  PersistManager(const PersistConfig &config)
      : config_(config), wal_(config), disk_store_(config),
        rocksdb_store_(config) {}

  bool init() {
    if (config_.wal_enabled && !wal_.open())
      return false;
    if (config_.mode == PersistMode::POINTER_MODE && !disk_store_.open())
      return false;
    if (config_.mode == PersistMode::FULL_MODE && !rocksdb_store_.open())
      return false;
    return true;
  }

  void close() {
    wal_.close();
    disk_store_.close();
    rocksdb_store_.close();
  }

  // 获取各组件
  WAL &wal() { return wal_; }
  DiskStore &diskStore() { return disk_store_; }
  RocksDBStore &rocksDBStore() { return rocksdb_store_; }
  const PersistConfig &config() const { return config_; }

  bool isEnabled() const { return config_.mode != PersistMode::MEMORY_ONLY; }
  bool isPointerMode() const {
    return config_.mode == PersistMode::POINTER_MODE;
  }
  bool isFullMode() const { return config_.mode == PersistMode::FULL_MODE; }

private:
  PersistConfig config_;
  WAL wal_;
  DiskStore disk_store_;
  RocksDBStore rocksdb_store_;
};

} // namespace persistence
} // namespace ctgraph
