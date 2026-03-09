#pragma once
#include "PersistConfig.hpp"
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace ctgraph {
namespace persistence {

// 磁盘偏移类型
using DiskOffset = uint64_t;
constexpr DiskOffset INVALID_DISK_OFFSET = UINT64_MAX;

// 指针模式：属性值存磁盘，内存保留结构和指针
class DiskStore {
public:
  DiskStore(const PersistConfig &config) : config_(config), fd_(-1), offset_(0) {}

  ~DiskStore() { close(); }

  bool open() {
    if (config_.mode != PersistMode::POINTER_MODE)
      return true;
    mkdir(config_.data_dir.c_str(), 0755);
    std::string path = config_.data_dir + "/props.dat";
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0)
      return false;
    // 获取当前文件大小作为偏移
    struct stat st;
    if (fstat(fd_, &st) == 0)
      offset_ = st.st_size;
    return true;
  }

  void close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  // 写入int值，返回磁盘偏移
  DiskOffset writeInt(int value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0)
      return INVALID_DISK_OFFSET;
    DiskOffset pos = offset_;
    uint8_t type = 1; // int type
    ::write(fd_, &type, 1);
    ::write(fd_, &value, sizeof(int));
    offset_ += 1 + sizeof(int);
    return pos;
  }

  // 写入string值，返回磁盘偏移
  DiskOffset writeString(const std::string &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0)
      return INVALID_DISK_OFFSET;
    DiskOffset pos = offset_;
    uint8_t type = 2; // string type
    uint32_t len = value.size();
    ::write(fd_, &type, 1);
    ::write(fd_, &len, sizeof(uint32_t));
    ::write(fd_, value.data(), len);
    offset_ += 1 + sizeof(uint32_t) + len;
    return pos;
  }

  // 读取int值
  bool readInt(DiskOffset offset, int &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0)
      return false;
    lseek(fd_, offset, SEEK_SET);
    uint8_t type;
    if (::read(fd_, &type, 1) != 1 || type != 1)
      return false;
    return ::read(fd_, &value, sizeof(int)) == sizeof(int);
  }

  // 读取string值
  bool readString(DiskOffset offset, std::string &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0)
      return false;
    lseek(fd_, offset, SEEK_SET);
    uint8_t type;
    if (::read(fd_, &type, 1) != 1 || type != 2)
      return false;
    uint32_t len;
    if (::read(fd_, &len, sizeof(uint32_t)) != sizeof(uint32_t))
      return false;
    value.resize(len);
    return ::read(fd_, value.data(), len) == static_cast<ssize_t>(len);
  }

  // 写入边属性，返回磁盘偏移
  DiskOffset writeEdgeProperty(const char *data, size_t len) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0)
      return INVALID_DISK_OFFSET;
    DiskOffset pos = offset_;
    uint32_t size = len;
    ::write(fd_, &size, sizeof(uint32_t));
    ::write(fd_, static_cast<const void*>(data), len);
    offset_ += sizeof(uint32_t) + len;
    return pos;
  }

  // 读取边属性
  bool readEdgeProperty(DiskOffset offset, std::string &data) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fd_ < 0)
      return false;
    lseek(fd_, offset, SEEK_SET);
    uint32_t len;
    if (::read(fd_, &len, sizeof(uint32_t)) != sizeof(uint32_t))
      return false;
    data.resize(len);
    return ::read(fd_, data.data(), len) == static_cast<ssize_t>(len);
  }

private:
  PersistConfig config_;
  int fd_;
  DiskOffset offset_;
  std::mutex mutex_;
};

} // namespace persistence
} // namespace ctgraph
