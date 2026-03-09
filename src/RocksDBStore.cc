#include "persistence/RocksDBStore.hpp"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>
#include <sstream>

namespace ctgraph {
namespace persistence {

RocksDBStore::~RocksDBStore() { close(); }

bool RocksDBStore::open() {
  if (config_.mode != PersistMode::FULL_MODE)
    return true;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  rocksdb::Status s = rocksdb::DB::Open(opts, config_.rocksdb_dir, &db_);
  return s.ok();
}

void RocksDBStore::close() {
  if (db_) {
    delete db_;
    db_ = nullptr;
  }
}

std::string RocksDBStore::serializeProps(
    const std::unordered_map<std::string, std::any> &props) {
  std::ostringstream oss;
  oss << "{";
  bool first = true;
  for (const auto &[k, v] : props) {
    if (!first) oss << ",";
    first = false;
    oss << "\"" << k << "\":";
    if (v.type() == typeid(int)) {
      oss << std::any_cast<int>(v);
    } else if (v.type() == typeid(std::string)) {
      oss << "\"" << std::any_cast<std::string>(v) << "\"";
    } else {
      oss << "null";
    }
  }
  oss << "}";
  return oss.str();
}

void RocksDBStore::deserializeProps(
    const std::string &data,
    std::unordered_map<std::string, std::any> &props) {
  // 简单JSON解析，仅支持int和string
  size_t pos = 1; // skip '{'
  while (pos < data.size() && data[pos] != '}') {
    if (data[pos] == '"') {
      size_t key_start = pos + 1;
      size_t key_end = data.find('"', key_start);
      std::string key = data.substr(key_start, key_end - key_start);
      pos = data.find(':', key_end) + 1;
      while (pos < data.size() && data[pos] == ' ') pos++;
      if (data[pos] == '"') {
        size_t val_start = pos + 1;
        size_t val_end = data.find('"', val_start);
        props[key] = data.substr(val_start, val_end - val_start);
        pos = val_end + 1;
      } else if (data[pos] != 'n') {
        size_t val_end = data.find_first_of(",}", pos);
        props[key] = std::stoi(data.substr(pos, val_end - pos));
        pos = val_end;
      } else {
        pos = data.find_first_of(",}", pos);
      }
    }
    if (pos < data.size() && data[pos] == ',') pos++;
  }
}

bool RocksDBStore::putUser(uint64_t uid, uint64_t ts,
                           const std::unordered_map<std::string, std::any> &props) {
  if (!db_) return false;
  std::string key = userKey(uid, ts);
  std::string val = serializeProps(props);
  return db_->Put(rocksdb::WriteOptions(), key, val).ok();
}

bool RocksDBStore::putFriend(uint64_t src, uint64_t dst, uint64_t ts, bool exists) {
  if (!db_) return false;
  std::string key = friendKey(src, dst, ts);
  return db_->Put(rocksdb::WriteOptions(), key, exists ? "1" : "0").ok();
}

bool RocksDBStore::getUser(uint64_t uid, uint64_t ts,
                           std::unordered_map<std::string, std::any> &props) {
  if (!db_) return false;
  std::string prefix = "user:" + std::to_string(uid) + ":ts:";
  std::string target = userKey(uid, ts);

  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
  it->Seek(target);

  std::string found_key, found_val;
  if (it->Valid() && it->key().ToString().substr(0, prefix.size()) == prefix) {
    found_key = it->key().ToString();
    found_val = it->value().ToString();
  } else {
    it->Prev();
    if (it->Valid() && it->key().ToString().substr(0, prefix.size()) == prefix) {
      found_key = it->key().ToString();
      found_val = it->value().ToString();
    } else {
      return false;
    }
  }

  deserializeProps(found_val, props);
  return true;
}

bool RocksDBStore::getFriends(uint64_t uid, uint64_t ts,
                              std::vector<uint64_t> &friends) {
  if (!db_) return false;
  char buf[32];
  snprintf(buf, sizeof(buf), "friend:%08lu:", uid);
  std::string prefix(buf);

  std::unordered_map<uint64_t, bool> friend_status;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));

  for (it->Seek(prefix); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    if (key.substr(0, prefix.size()) != prefix) break;

    // 解析 friend:{src}:{dst}:ts:{timestamp}
    size_t p1 = key.find(':', 7);
    size_t p2 = key.find(':', p1 + 1);
    size_t p3 = key.find(':', p2 + 4);
    uint64_t dst = std::stoull(key.substr(p1 + 1, p2 - p1 - 1));
    uint64_t key_ts = std::stoull(key.substr(p3 + 1));

    if (key_ts <= ts) {
      friend_status[dst] = (it->value().ToString() == "1");
    }
  }

  for (const auto &[dst, exists] : friend_status) {
    if (exists) friends.push_back(dst);
  }
  return true;
}

bool RocksDBStore::getUserInRange(
    uint64_t uid, uint64_t from_ts, uint64_t to_ts,
    std::vector<std::unordered_map<std::string, std::any>> &results) {
  if (!db_) return false;
  std::string start_key = userKey(uid, from_ts);
  std::string end_key = userKey(uid, to_ts);
  std::string prefix = "user:" + std::to_string(uid) + ":ts:";

  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
  for (it->Seek(start_key); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    if (key.substr(0, prefix.size()) != prefix || key > end_key) break;
    std::unordered_map<std::string, std::any> props;
    deserializeProps(it->value().ToString(), props);
    results.push_back(props);
  }
  return true;
}

} // namespace persistence
} // namespace ctgraph
