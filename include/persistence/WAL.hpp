#pragma once
#include "PersistConfig.hpp"
#include "../types.hpp"
#include <any>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace ctgraph {
namespace persistence {

constexpr uint32_t WAL_MAGIC = 0x57414C47; // "WALG"
constexpr uint16_t WAL_VERSION = 1;

enum class WALType : uint8_t {
  VP_INSERT = 0x01,
  VP_UPDATE = 0x02,
  EDGE_INSERT = 0x03,
  EDGE_DELETE = 0x04,
  CHECKPOINT = 0x05
};

#pragma pack(push, 1)
struct WALHeader {
  uint32_t magic;
  uint16_t version;
  uint8_t type;
  uint8_t flags;
  uint64_t timestamp;
  uint32_t payload_len;
};
#pragma pack(pop)

// VP操作的payload
struct VPPayload {
  uint64_t vertex_id;
  // 后跟: col_count(4B) + [col_name_len(2B) + col_name + value_type(1B) + value]...
};

// Edge操作的payload
struct EdgePayload {
  uint64_t src;
  uint64_t dst;
  uint64_t timestamp;
  // 后跟: property_len(4B) + property_data
};

class WAL {
public:
  WAL(const PersistConfig &config) : config_(config), fd_(-1), buffer_pos_(0) {
    buffer_.resize(config.wal_buffer_size);
  }

  ~WAL() { close(); }

  bool open() {
    if (!config_.wal_enabled)
      return true;
    mkdir(config_.wal_dir.c_str(), 0755);
    std::string path = config_.wal_dir + "/wal.log";
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    return fd_ >= 0;
  }

  void close() {
    if (fd_ >= 0) {
      flush();
      ::close(fd_);
      fd_ = -1;
    }
  }

  // 记录VP插入
  bool logVPInsert(SequenceNumber_t ts, uint64_t vertex_id,
                   const std::unordered_map<std::string, std::any> &props) {
    if (!config_.wal_enabled)
      return true;
    return writeVPLog(WALType::VP_INSERT, ts, vertex_id, props);
  }

  // 记录VP更新
  bool logVPUpdate(SequenceNumber_t ts, uint64_t vertex_id,
                   const std::unordered_map<std::string, std::any> &props) {
    if (!config_.wal_enabled)
      return true;
    return writeVPLog(WALType::VP_UPDATE, ts, vertex_id, props);
  }

  // 记录边插入
  bool logEdgeInsert(SequenceNumber_t ts, uint64_t src, uint64_t dst,
                     const std::string &property) {
    if (!config_.wal_enabled)
      return true;
    return writeEdgeLog(WALType::EDGE_INSERT, ts, src, dst, property);
  }

  // 记录边删除
  bool logEdgeDelete(SequenceNumber_t ts, uint64_t src, uint64_t dst) {
    if (!config_.wal_enabled)
      return true;
    return writeEdgeLog(WALType::EDGE_DELETE, ts, src, dst, "");
  }

  bool flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (buffer_pos_ > 0 && fd_ >= 0) {
      ssize_t written = ::write(fd_, buffer_.data(), buffer_pos_);
      if (written != static_cast<ssize_t>(buffer_pos_))
        return false;
      ::fsync(fd_);
      buffer_pos_ = 0;
    }
    return true;
  }

private:
  bool writeVPLog(WALType type, SequenceNumber_t ts, uint64_t vertex_id,
                  const std::unordered_map<std::string, std::any> &props) {
    std::vector<char> payload;
    // vertex_id
    payload.resize(sizeof(uint64_t) + sizeof(uint32_t));
    memcpy(payload.data(), &vertex_id, sizeof(uint64_t));
    uint32_t col_count = props.size();
    memcpy(payload.data() + sizeof(uint64_t), &col_count, sizeof(uint32_t));

    for (const auto &[name, val] : props) {
      uint16_t name_len = name.size();
      size_t pos = payload.size();
      payload.resize(pos + sizeof(uint16_t) + name_len + 1);
      memcpy(payload.data() + pos, &name_len, sizeof(uint16_t));
      memcpy(payload.data() + pos + sizeof(uint16_t), name.data(), name_len);

      uint8_t vtype = 0;
      if (val.type() == typeid(int)) {
        vtype = 1;
        int v = std::any_cast<int>(val);
        payload[pos + sizeof(uint16_t) + name_len] = vtype;
        size_t vpos = payload.size();
        payload.resize(vpos + sizeof(int));
        memcpy(payload.data() + vpos, &v, sizeof(int));
      } else if (val.type() == typeid(std::string)) {
        vtype = 2;
        std::string v = std::any_cast<std::string>(val);
        payload[pos + sizeof(uint16_t) + name_len] = vtype;
        uint32_t slen = v.size();
        size_t vpos = payload.size();
        payload.resize(vpos + sizeof(uint32_t) + slen);
        memcpy(payload.data() + vpos, &slen, sizeof(uint32_t));
        memcpy(payload.data() + vpos + sizeof(uint32_t), v.data(), slen);
      } else {
        payload[pos + sizeof(uint16_t) + name_len] = 0; // null
      }
    }
    return writeRecord(type, ts, payload);
  }

  bool writeEdgeLog(WALType type, SequenceNumber_t ts, uint64_t src,
                    uint64_t dst, const std::string &property) {
    std::vector<char> payload(sizeof(uint64_t) * 3 + sizeof(uint32_t) +
                              property.size());
    size_t pos = 0;
    memcpy(payload.data() + pos, &src, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(payload.data() + pos, &dst, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(payload.data() + pos, &ts, sizeof(uint64_t));
    pos += sizeof(uint64_t);
    uint32_t plen = property.size();
    memcpy(payload.data() + pos, &plen, sizeof(uint32_t));
    pos += sizeof(uint32_t);
    if (plen > 0)
      memcpy(payload.data() + pos, property.data(), plen);
    return writeRecord(type, ts, payload);
  }

  bool writeRecord(WALType type, SequenceNumber_t ts,
                   const std::vector<char> &payload) {
    std::lock_guard<std::mutex> lock(mutex_);
    WALHeader header;
    header.magic = WAL_MAGIC;
    header.version = WAL_VERSION;
    header.type = static_cast<uint8_t>(type);
    header.flags = 0;
    header.timestamp = ts;
    header.payload_len = payload.size();

    uint32_t crc = crc32(payload.data(), payload.size());
    size_t total = sizeof(WALHeader) + payload.size() + sizeof(uint32_t);

    if (buffer_pos_ + total > buffer_.size()) {
      if (fd_ >= 0) {
        ::write(fd_, buffer_.data(), buffer_pos_);
        buffer_pos_ = 0;
      }
    }

    memcpy(buffer_.data() + buffer_pos_, &header, sizeof(WALHeader));
    buffer_pos_ += sizeof(WALHeader);
    memcpy(buffer_.data() + buffer_pos_, payload.data(), payload.size());
    buffer_pos_ += payload.size();
    memcpy(buffer_.data() + buffer_pos_, &crc, sizeof(uint32_t));
    buffer_pos_ += sizeof(uint32_t);
    return true;
  }

  uint32_t crc32(const char *data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
      crc ^= static_cast<uint8_t>(data[i]);
      for (int j = 0; j < 8; j++)
        crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
    }
    return ~crc;
  }

  PersistConfig config_;
  int fd_;
  std::vector<char> buffer_;
  size_t buffer_pos_;
  std::mutex mutex_;
};

} // namespace persistence
} // namespace ctgraph
