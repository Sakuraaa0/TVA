//
// Created by lwh on 25-1-2.
//

#ifndef FIXSTRING_HPP
#define FIXSTRING_HPP

#include <cstring>
#include <iostream>
#include <string>
#include <string_view>

class FixString {
private:
  char *data_;
  int length_;

public:
  FixString() : data_(nullptr), length_(0) {}

  FixString(const std::string &str) {
    if (str.size() > 0) {
      data_ = new char[str.length() + 1];
      strcpy(data_, str.c_str());
      length_ = str.length();
    } else {
      data_ = nullptr;
      length_ = 0;
    }
  }

  ~FixString() {
    if (data_ != nullptr) {
      delete[] data_;
    }
  }

  int size() const { return length_; }

  const char *data() const { return data_; }

  char &operator[](int index) { return data_[index]; }

  const char &operator[](int index) const { return data_[index]; }

  bool operator==(const FixString &other) const {
    if (length_ != other.length_) {
      return false;
    }
    return strcmp(data_, other.data_) == 0;
  }

  bool operator!=(const FixString &other) const { return !(*this == other); }

  FixString &operator=(const FixString &other) {
    if (this != &other) {
      if (data_ != nullptr) {
        delete[] data_;
      }
      data_ = new char[other.length_ + 1];
      strcpy(data_, other.data_);
      length_ = other.length_;
    }
    return *this;
  }

  FixString &operator=(const std::string &str) {
    if (data_ != nullptr) {
      delete[] data_;
    }
    data_ = new char[str.length() + 1];
    strcpy(data_, str.c_str());
    length_ = str.length();
    return *this;
  }

  FixString &operator+=(const FixString &other) {
    char *temp = new char[length_ + other.length_ + 1];
    strcpy(temp, data_);
    strcat(temp, other.data_);
    if (data_ != nullptr) {
      delete[] data_;
    }
    data_ = temp;
    length_ += other.length_;
    return *this;
  }

  FixString &operator+=(const std::string &str) {
    char *temp = new char[length_ + str.length() + 1];
    strcpy(temp, data_);
    strcat(temp, str.c_str());
    if (data_ != nullptr) {
      delete[] data_;
    }
    data_ = temp;
    length_ += str.length();
    return *this;
  }

  FixString operator+(const FixString &other) const {
    FixString result(*this);
    result += other;
    return result;
  }

  FixString operator+(const std::string &str) const {
    FixString result(*this);
    result += str;
    return result;
  }

  operator std::string() const { return std::string(data_); }

  operator std::string_view() const { return std::string_view(data_, length_); }

  friend std::ostream &operator<<(std::ostream &os, const FixString &str) {
    if (str.data_ != nullptr) {
      os << str.data_;
    }
    return os;
  }
};
#endif // FIXSTRING_HPP
