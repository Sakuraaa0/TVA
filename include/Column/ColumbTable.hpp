#pragma once
#include <any>
#include <cassert>
#include <cstddef>
#include <gflags/gflags.h>
#include <iostream>
#include <omp.h>
#include <string>
#include <thread>
#include <typeinfo>
#include <unordered_map>
#include <vector>

#include "ColumnTypes.hpp"
#include "TemporalData.hpp"
#include "types.hpp"
#include "../persistence/PersistManager.hpp"
#include <any>
#include <string>
#include <unordered_map>
#include <vector>

namespace column {
// 辅助函数：比较两个 std::any 是否相等，仅支持 int、std::string 和 double 类型
bool anyEqual(const std::any &a, const std::any &b) {
  if (!a.has_value() && !b.has_value())
    return true;
  if (a.type() != b.type())
    return false;
  if (a.type() == typeid(int))
    return std::any_cast<int>(a) == std::any_cast<int>(b);
  else if (a.type() == typeid(std::string))
    return std::any_cast<std::string>(a) == std::any_cast<std::string>(b);
  else if (a.type() == typeid(double))
    return std::any_cast<double>(a) == std::any_cast<double>(b);
  return false;
}

// 辅助函数：打印 std::any 的值
void printAny(const std::any &a) {
  if (!a.has_value()) {
    std::cout << "null";
  } else if (a.type() == typeid(int)) {
    std::cout << std::any_cast<int>(a);
  } else if (a.type() == typeid(std::string)) {
    std::cout << std::any_cast<std::string>(a);
  } else if (a.type() == typeid(double)) {
    std::cout << std::any_cast<double>(a);
  } else {
    std::cout << "unknown";
  }
}

// ColumnTable：列存储表，内部使用 unordered_map
// 存储列名到列数据（vector<std::any>）
class ColumnTable {
public:
  std::unordered_map<std::string, std::vector<std::any>> columns;
  std::unordered_map<std::string, std::vector<std::any>>
      modify_columns; // 记录历史数据
  size_t num_rows = 0;

  // 添加一行数据，row 中的键值对表示列名与对应值
  void addRow(const std::unordered_map<std::string, std::any> &row) {
    // 拷贝一份传入的行数据
    std::unordered_map<std::string, std::any> complete_row = row;
    // 对于表中已有的列，若当前行没有对应的属性，则填充空值
    for (const auto &col : columns) {
      if (complete_row.find(col.first) == complete_row.end()) {
        complete_row[col.first] = std::any();
      }
    }
    // 对于当前行中新出现的列，在表中创建新列，并为之前的所有行填充空值
    for (const auto &p : complete_row) {
      if (columns.find(p.first) == columns.end()) {
        columns[p.first] = std::vector<std::any>(num_rows, std::any());
      }
    }
    // 为所有列追加当前行的值
    for (auto &col : columns) {
      // auto it = complete_row.find(col.first);
      // if (it != complete_row.end()) {
      //     col.second.push_back(it->second);
      // } else {
      //     col.second.push_back(std::any());
      // }
      col.second.push_back(complete_row[col.first]);
    }
    num_rows++;
  }

  /**
   * 更新数据，并将对应每行在modify_columns中的偏移位置进行返回
   * @return 进行修改的行为其对应偏移位置，未进行修改的行为INVALID_OFFSET
   */
  std::vector<offset_t>
  update(offset_t offset,
         const std::unordered_map<std::string, std::any> &rows) {
    std::vector<offset_t> rt_offset;
    // for (const auto &row: rows)
    // {
    //     std::any value_old = columns[row.first][offset];
    //     size_t index = modify_columns[row.first].size();
    //     modify_columns[row.first].emplace_back(value_old);
    //     columns[row.first][offset] = row.second;
    //     rt_offset.emplace_back(index);
    // }

    for (auto &col : columns) {
      if (rows.find(col.first) == rows.end()) {
        rt_offset.emplace_back(INVALID_OFFSET);
      } else {
        std::any value_old = col.second[offset];
        size_t index = modify_columns[col.first].size();
        modify_columns[col.first].emplace_back(value_old);
        col.second[offset] = rows.find(col.first)->second;
        rt_offset.emplace_back(index);
      }
    }
    return rt_offset;
  }
  // 仅实验导入数据使用
  void just_update(offset_t offset,
                   const std::unordered_map<std::string, std::any> &rows) {
    // for (const auto &row: rows)
    // {
    //     std::any value_old = columns[row.first][offset];
    //     size_t index = modify_columns[row.first].size();
    //     modify_columns[row.first].emplace_back(value_old);
    //     columns[row.first][offset] = row.second;
    //     rt_offset.emplace_back(index);
    // }
    for (auto &col : columns) {
      if (rows.find(col.first) != rows.end()) {
        col.second[offset] = rows.find(col.first)->second;
      }
    }
  }

  // 根据行号返回该行所要求的几行数据
  void getRow4Index(size_t index, std::vector<std::string> cols,
                    std::unordered_map<std::string, std::any> &res) const {
    for (const auto &col : cols) {
      if (columns.find(col) != columns.end()) {
        res[col] = columns.at(col)[index];
      }
    }
  }
  // 根据行号返回该行所有最新数据
  std::unordered_map<std::string, std::any> get_row(size_t index) const {
    std::unordered_map<std::string, std::any> result;
    for (const auto &p : columns) {
      if (index < p.second.size())
        result[p.first] = p.second[index];
    }
    return result;
  }

  // 简单查询：返回指定列中等于给定值的所有行号
  std::vector<size_t> query(const std::string &col,
                            const std::any &value) const {
    std::vector<size_t> indices;
    if (columns.find(col) == columns.end())
      return indices;
    const auto &vec = columns.at(col);
    for (size_t i = 0; i < vec.size(); i++) {
      if (anyEqual(vec[i], value)) {
        indices.push_back(i);
      }
    }
    return indices;
  }
};

// GraphDB：基于 ColumnTable 构造的内存图数据库，分顶点和边两个表存储
enum ValueType { VP, EP, VE };

class GraphDB {
public:
  ColumnTable VP;
  ColumnTable EP;
  ColumnTable VE;

  uint8_t vp_count;
  uint8_t ep_count;
  uint8_t ve_count;

  std::vector<TemporalData> temporal_datas_;

  // 通过 id 快速查找顶点和边的行号
  std::unordered_map<size_t, size_t> vertex_index;
  // 通过行号快速查找顶点id
  std::unordered_map<size_t, size_t> column4vertex;

  // 持久化管理器指针（可选）
  ctgraph::persistence::PersistManager *persist_mgr_ = nullptr;

  // 设置持久化管理器
  void setPersistManager(ctgraph::persistence::PersistManager *mgr) {
    persist_mgr_ = mgr;
  }

  /**
   * 初始化载入数据 用作例子VP，EP，VE各一个共三个元素 当前仅插入点
   * @param seq 版本号
   * @param vertex_id 顶点id
   * @param properties 顶点属性
   */
  void loadTableVertex(
      SequenceNumber_t seq, const size_t vertex_id,
      const std::unordered_map<std::string, std::any> &properties = {}) {
    TemporalData new_temporal_data;
    this->vp_count = 1;
    this->ep_count = 1;
    this->ve_count = 1;

    new_temporal_data.header_.lock_ = 0;
    // 此处需要根据实际存放元素进行修改，初始VP为1，没有边等于初始化顶点
    new_temporal_data.header_.exist_bitmap_ = 0x1;
    // new_temporal_data.seq_[0] = seq;
    new_temporal_data.set_seq(0, seq);
    // new_temporal_data.modify_bitmap_[0] = 0x1;
    new_temporal_data.set_modify_bitmap(0, 0x1);
    if (temporal_datas_.size() == 0) // 版本链链接其上一个元素的最新版本
    {
      // new_temporal_data.next_offset_[0] = INVALD_NEXT_OFFSET;
      new_temporal_data.set_next_offset(0, INVALD_NEXT_OFFSET);
    } else {
      // new_temporal_data.next_offset_[0] =
      // temporal_datas_.rbegin()->version_count_ - 1;
      new_temporal_data.set_next_offset(
          0, temporal_datas_.rbegin()->version_count_ - 1);
    }
    std::vector<offset_t> tmp_values;
    for (int i = 0; i < vp_count + ep_count + ve_count; i++) {
      tmp_values.emplace_back(INVALID_OFFSET);
    }
    new_temporal_data.value_offset.emplace_back(tmp_values);
    new_temporal_data.version_count_ = 1;

    temporal_datas_.emplace_back(new_temporal_data);
    // 插入实际数据,就算是空的数据也要填空
    std::unordered_map<std::string, std::any> props = properties;
    std::unordered_map<std::string, std::any> ep_props;
    std::unordered_map<std::string, std::any> ve_props;
    ep_props["relationship"] = std::any();
    ve_props["relationship"] = std::any();
    VP.addRow(props);
    EP.addRow(ep_props);
    VE.addRow(ve_props);

    vertex_index[vertex_id] = VP.num_rows - 1;
    column4vertex[VP.num_rows - 1] = vertex_id;
  }

  /**
   * 初始化载入数据
   * @param seq 当前时间
   * @param vertex_id 顶点id
   * @param vps 顶点属性
   * @param eps 边属性
   * @param ves 边拓扑
   */
  void LoadTable(SequenceNumber_t seq, const size_t vertex_id,
                 const std::unordered_map<std::string, std::any> &vps = {},
                 const std::unordered_map<std::string, std::any> &eps = {},
                 const std::unordered_map<std::string, std::any> &ves = {}) {
    // 统计该表vp，ep，ve个数
    this->vp_count = vps.size();
    this->ep_count = eps.size();
    this->ve_count = ves.size();

    // 初始化版本链
    TemporalData new_temporal_data;
    // for (int i = 0; i < MAX_VERSION_NUM; i++)
    // {
    //     // new_temporal_data.modify_bitmap_[i] = 0;
    //     new_temporal_data.set_modify_bitmap(i, 0);
    // }
    new_temporal_data.header_.lock_ = 0;
    // todo 修改exist_bitmap_ modify_bitmap_

    // new_temporal_data.seq_[0] = seq;
    new_temporal_data.set_seq(0, seq);
    new_temporal_data.set_modify_bitmap(0, 0);

    if (temporal_datas_.size() == 0) // 版本链链接其上一个元素的最新版本
    {
      // new_temporal_data.next_offset_[0] = INVALD_NEXT_OFFSET;
      new_temporal_data.set_next_offset(0, INVALD_NEXT_OFFSET);
    } else {
      // new_temporal_data.next_offset_[0] =
      // temporal_datas_.rbegin()->version_count_ - 1;
      new_temporal_data.set_next_offset(
          0, temporal_datas_.rbegin()->version_count_ - 1);
    }

    std::vector<offset_t> tmp_values(vp_count + ep_count + ve_count,
                                     INVALID_OFFSET);
    new_temporal_data.value_offset.emplace_back(tmp_values);
    new_temporal_data.version_count_ = 1;

    // 插入实际数据
    VP.addRow(vps);
    EP.addRow(eps);
    VE.addRow(ves);
    int count = 0;
    for (auto &col : VP.columns) {
      if (vps.find(col.first)->second.has_value()) {
        // modify_bitmap_ 的第count位置为1
        // new_temporal_data.modify_bitmap_[0] |= (1 << count);
        new_temporal_data.set_modify_bitmap(
            0, new_temporal_data.get_modify_bitmap(0) | (1 << count));
      }
      count++;
    }
    count = 0;
    for (auto &col : EP.columns) {
      if (eps.find(col.first)->second.has_value()) {
        // new_temporal_data.modify_bitmap_[0] |= (1 << (count + vp_count));
        new_temporal_data.set_modify_bitmap(
            0,
            new_temporal_data.get_modify_bitmap(0) | (1 << (count + vp_count)));
      }
      count++;
    }
    count = 0;
    for (auto &col : VE.columns) {
      if (ves.find(col.first)->second.has_value()) {
        // new_temporal_data.modify_bitmap_[0] |= (1 << (count + vp_count +
        // ep_count));
        new_temporal_data.set_modify_bitmap(
            0, new_temporal_data.get_modify_bitmap(0) |
                   (1 << (count + vp_count + ep_count)));
      }
      count++;
    }
    // new_temporal_data.header_.exist_bitmap_ =
    // new_temporal_data.modify_bitmap_[0];
    new_temporal_data.header_.exist_bitmap_ =
        new_temporal_data.get_modify_bitmap(0);
    temporal_datas_.emplace_back(new_temporal_data);
    vertex_index[vertex_id] = VP.num_rows - 1;
    column4vertex[VP.num_rows - 1] = vertex_id;

    // 持久化：WAL + RocksDB全量模式
    if (persist_mgr_ && persist_mgr_->isEnabled()) {
      persist_mgr_->wal().logVPInsert(seq, vertex_id, vps);
      if (persist_mgr_->isFullMode()) {
        persist_mgr_->rocksDBStore().putUser(vertex_id, seq, vps);
      }
    }
  }

  void update(SequenceNumber_t seq, const size_t vertex_id,
              const std::unordered_map<std::string, std::any> &vps = {},
              const std::unordered_map<std::string, std::any> &eps = {},
              const std::unordered_map<std::string, std::any> &ves = {}) {
    offset_t node_offset = vertex_index[vertex_id];
    auto &header = temporal_datas_[node_offset].header_;
    while (header.lock_ == 1) {
    }
    header.lock_ = 1;
    auto &temporal_data = temporal_datas_[node_offset];
    version_count_t version_count = temporal_data.version_count_;
    // temporal_data.seq_[version_count] = seq;
    temporal_data.set_seq(version_count, seq);

    // 查询此次修改具体哪些行
    int i = 0;
    bitmap_t vp_tmp = 0;
    bitmap_t ep_tmp = 0;
    bitmap_t ve_tmp = 0;
#pragma omp parallel for num_threads(16)
    for (auto it = VP.columns.begin(); it != VP.columns.end(); ++it) {
      size_t count = std::distance(VP.columns.begin(), it);
      if (vps.find(it->first) != vps.end()) {
        vp_tmp |= (1 << count);
      }
    }
#pragma omp parallel for num_threads(16)
    for (auto it = EP.columns.begin(); it != EP.columns.end(); ++it) {
      size_t count = std::distance(EP.columns.begin(), it);
      if (eps.find(it->first) != eps.end()) {
        ep_tmp |= (1 << count);
      }
    }
#pragma omp parallel for num_threads(16)
    for (auto it = VE.columns.begin(); it != VE.columns.end(); ++it) {
      size_t count = std::distance(VE.columns.begin(), it);
      if (ves.find(it->first) != ves.end()) {
        ve_tmp |= (1 << count);
      }
    }
    bitmap_t modify_tmp =
        vp_tmp | (ep_tmp << this->vp_count) | (ve_tmp << (ep_count + vp_count));
    header.exist_bitmap_ |= modify_tmp; // 此次更新可能添加最新数据
    // temporal_data.modify_bitmap_[version_count] = modify_tmp;
    temporal_data.set_modify_bitmap(version_count, modify_tmp);
    if (node_offset != 0) {
      // temporal_data.next_offset_[version_count] = temporal_datas_[node_offset
      // - 1].version_count_ - 1;
      temporal_data.set_next_offset(
          version_count, temporal_datas_[node_offset - 1].version_count_ - 1);
    } else {
      // temporal_data.next_offset_[version_count] = INVALD_NEXT_OFFSET;
      temporal_data.set_next_offset(version_count, INVALD_NEXT_OFFSET);
    }
    auto offsets = VP.update(node_offset, vps);
    std::vector<offset_t> ep_offsets = EP.update(node_offset, eps);
    std::vector<offset_t> ve_offsets = VE.update(node_offset, ves);
    offsets.insert(offsets.end(), ep_offsets.begin(), ep_offsets.end());
    offsets.insert(offsets.end(), ve_offsets.begin(), ve_offsets.end());
    temporal_data.value_offset.emplace_back(offsets);
    temporal_data.version_count_++;
    header.lock_ = 0;

    // 持久化：WAL + RocksDB全量模式
    if (persist_mgr_ && persist_mgr_->isEnabled()) {
      persist_mgr_->wal().logVPUpdate(seq, vertex_id, vps);
      if (persist_mgr_->isFullMode()) {
        persist_mgr_->rocksDBStore().putUser(vertex_id, seq, vps);
      }
    }
  }

  //仅实验导入数据使用
  void just_update(const size_t vertex_id,
                   const std::unordered_map<std::string, std::any> &vps) {
    offset_t node_offset = vertex_index[vertex_id];
    bitmap_t vp_tmp = 0;
    for (auto it = VP.columns.begin(); it != VP.columns.end(); ++it) {
      size_t count = std::distance(VP.columns.begin(), it);
      if (vps.find(it->first) != vps.end()) {
        vp_tmp |= (1 << count);
      }
    }
    temporal_datas_[node_offset].header_.exist_bitmap_ |= vp_tmp;
    temporal_datas_[node_offset].set_modify_bitmap(
        0, temporal_datas_[node_offset].header_.exist_bitmap_);
    VP.just_update(node_offset, vps);
  }
  /**
   * 获取指定版本范围内的数据
   * @param seq 所需要查询的版本号
   * @param vertex_id 顶点id
   * @param vps 所需要查询的顶点属性
   * @param eps 所需要查询的边属性
   * @param ves 所需要查询的边属性
   * @return 状态
   */
  ctgraph::Status
  getVersionInRange(SequenceNumber_t seq, const size_t vertex_id,
                    std::unordered_map<std::string, std::any> &vps,
                    std::unordered_map<std::string, std::any> &eps,
                    std::unordered_map<std::string, std::any> &ves) {
    offset_t node_offset = vertex_index[vertex_id];
    auto &header = temporal_datas_[node_offset].header_;
    while (header.lock_ == 1) {
    }
    header.lock_ = 2;
    const auto &temporal_data = temporal_datas_[node_offset];

    // 二分查找对应元素
    offset_t start_idx = 0;
    offset_t end_idx = temporal_data.version_count_ - 1;
    auto lateset_offset = vertex_index.find(vertex_id);
    while (start_idx <= end_idx) {
      offset_t mid_idx = (start_idx + end_idx) / 2;
      // 若 mid 不是最后一个元素，则检查区间
      if (mid_idx < temporal_data.version_count_ - 1) {
        if (temporal_data.get_seq(mid_idx) <= seq &&
            temporal_data.get_seq(mid_idx + 1) > seq) {
          // 找到对应元素
#pragma omp parallel for
          for (auto it = VP.columns.begin(); it != VP.columns.end(); ++it) {
            size_t count = std::distance(VP.columns.begin(), it);
            if (vps.find(it->first) != vps.end()) {
              // 找到temporal_data.value_offset[count]从mid_idx +
              // 1开始往后第一个不为INVALID_OFFSET的值
              bool is_temporal_data_offset = false;
              for (int i = mid_idx + 1; i < temporal_data.version_count_; i++) {
                if (temporal_data.value_offset[i][count] != INVALID_OFFSET) {
                  vps.find(it->first)->second =
                      VP.modify_columns[it->first]
                                       [temporal_data.value_offset[i][count]];
                  is_temporal_data_offset = true;
                  break;
                }
              }
              if (!is_temporal_data_offset) {
                vps.find(it->first)->second =
                    VP.columns[it->first][node_offset];
              }
            }
          }
#pragma omp parallel for
          for (auto it = EP.columns.begin(); it != EP.columns.end(); ++it) {
            size_t count = std::distance(EP.columns.begin(), it);
            if (eps.find(it->first) != eps.end()) {
              bool is_temporal_data_offset = false;
              for (int i = mid_idx + 1; i < temporal_data.version_count_; i++) {
                if (temporal_data.value_offset[i][count + vp_count] !=
                    INVALID_OFFSET) {
                  eps.find(it->first)->second =
                      EP.modify_columns[it->first]
                                       [temporal_data
                                            .value_offset[i][count + vp_count]];
                  is_temporal_data_offset = true;
                  break;
                }
              }
              if (!is_temporal_data_offset) {
                eps.find(it->first)->second =
                    EP.columns[it->first][node_offset];
              }
            }
          }

#pragma omp parallel for
          for (auto it = VE.columns.begin(); it != VE.columns.end(); ++it) {
            size_t count = std::distance(VE.columns.begin(), it);
            if (ves.find(it->first) != ves.end()) {
              bool is_temporal_data_offset = false;
              for (int i = mid_idx + 1; i < temporal_data.version_count_; i++) {
                if (temporal_data.value_offset[i][count + vp_count +
                                                  ep_count] != INVALID_OFFSET) {
                  ves.find(it->first)->second =
                      VE.modify_columns[it->first]
                                       [temporal_data.value_offset
                                            [i][count + vp_count + ep_count]];
                  is_temporal_data_offset = true;
                  break;
                }
              }
              if (!is_temporal_data_offset) {
                ves.find(it->first)->second =
                    VE.columns[it->first][node_offset];
              }
            }
          }
          header.lock_ = 0;
          return ctgraph::kOk;
        }
        // 如果当前元素 > seq，则目标在左侧
        if (temporal_data.get_seq(mid_idx) > seq)
          end_idx = mid_idx - 1;
        else // temporal_data.seq_[mid] < seq，但 temporal_data.seq_[mid+1] 也
             // <= seq
          start_idx = mid_idx + 1;
      } else { // mid 是最后一个元素则代表是最新的
        if (temporal_data.get_seq(mid_idx) <= seq) {
          for (auto &col : vps) {
            col.second = VP.columns[col.first][node_offset];
          }
          for (auto &col : eps) {
            col.second = EP.columns[col.first][node_offset];
          }
          for (auto &col : ves) {
            col.second = VE.columns[col.first][node_offset];
          }
          header.lock_ = 0;
          return ctgraph::kOk;
        } else {
          header.lock_ = 0;
          return ctgraph::kNotFound;
        }
      }
    }
    header.lock_ = 0;
    return ctgraph::kNotFound;
  }
  /**
   * 从开头找到对应系列
   * @param seq 所需要查询的版本号
   * @param row_index 所需要查询的行号
   * @param begin_offset 所需要查询的开始偏移量
   * @param value_type 所需要查询的值类型
   * @param res 所需要查询的值
   * @return 上一个数据的初始偏移
   */
  offset_t getDatafromThisBegin(SequenceNumber_t seq, size_t row_index,
                                offset_t begin_offset, ValueType value_type,
                                std::pair<std::string, std::any> &res) {
    auto &temporal_data = temporal_datas_[row_index];
    offset_t start_offset = begin_offset;
    offset_t end_offset = temporal_data.version_count_ - 1;
    switch (value_type) {
    case ValueType::VP: {
      auto count =
          std::distance(VP.columns.begin(), VP.columns.find(res.first));
      for (int i = start_offset; i < end_offset; i++) {
        if (temporal_data.get_seq(i) <= seq &&
            temporal_data.get_seq(i + 1) > seq) { // 如果符合条件
          bool is_temporal_data_offset = false;
          for (int j = i + 1; j < temporal_data.version_count_; j++) {
            if (temporal_data.value_offset[j][count] != INVALID_OFFSET) {
              res.second =
                  VP.modify_columns[res.first]
                                   [temporal_data.value_offset[j][count]];
              is_temporal_data_offset = true;
              return temporal_data.get_next_offset(i);
            }
          }
          if (!is_temporal_data_offset) { // 说明该值之后没有被修改过，存储在最新位置
            res.second = VP.columns[res.first][row_index];
            return temporal_data.get_next_offset(i);
          }
        }
      }
      // 查看是否是最后一个
      if (temporal_data.get_seq(end_offset) <= seq) {
        res.second = VP.columns[res.first][row_index];
        return temporal_data.get_next_offset(end_offset);
      }
    } break;
    case ValueType::EP: {
      auto count =
          std::distance(EP.columns.begin(), EP.columns.find(res.first));
      for (int i = start_offset; i < end_offset; i++) {
        if (temporal_data.get_seq(i) <= seq &&
            temporal_data.get_seq(i + 1) > seq) { // 如果符合条件
          bool is_temporal_data_offset = false;
          for (int j = i + 1; j < temporal_data.version_count_; j++) {
            if (temporal_data.value_offset[j][count + vp_count] !=
                INVALID_OFFSET) {
              res.second =
                  EP.modify_columns[res.first]
                                   [temporal_data
                                        .value_offset[j][count + vp_count]];
              is_temporal_data_offset = true;
              return temporal_data.get_next_offset(i);
            }
          }
          if (!is_temporal_data_offset) { // 说明该值之后没有被修改过，存储在最新位置
            res.second = EP.columns[res.first][row_index];
            return temporal_data.get_next_offset(i);
          }
        }
      }
      // 查看是否是最后一个
      if (temporal_data.get_seq(end_offset) <= seq) {
        res.second = EP.columns[res.first][row_index];
        return temporal_data.get_next_offset(end_offset);
      }
    } break;
    case ValueType::VE: {
      auto count =
          std::distance(VE.columns.begin(), VE.columns.find(res.first));
      for (int i = start_offset; i < end_offset; i++) {
        if (temporal_data.get_seq(i) <= seq &&
            temporal_data.get_seq(i + 1) > seq) { // 如果符合条件
          bool is_temporal_data_offset = false;
          for (int j = i + 1; j < temporal_data.version_count_; j++) {
            if (temporal_data.value_offset[j][count + vp_count + ep_count] !=
                INVALID_OFFSET) {
              res.second =
                  VE.modify_columns[res.first]
                                   [temporal_data.value_offset
                                        [j][count + vp_count + ep_count]];
              is_temporal_data_offset = true;
              return temporal_data.get_next_offset(i);
            }
          }
          if (!is_temporal_data_offset) { // 说明该值之后没有被修改过，存储在最新位置
            res.second = VE.columns[res.first][row_index];
            return temporal_data.get_next_offset(i);
          }
        }
      }
      // 查看是否是最后一个
      if (temporal_data.get_seq(end_offset) <= seq) {
        res.second = VE.columns[res.first][row_index];
        return temporal_data.get_next_offset(end_offset);
      }
    } break;
    }
    return 0;
  }

  /**
   * 获取指定版本范围内的所有数据
   * @param seq 所需要查询的版本号
   * @param vps 所需要查询的顶点属性
   * @param eps 所需要查询的边属性
   * @param ves 所需要查询的边属性
   * @param vp_res 所需要查询的顶点属性结果<列名，【顶点id，列值】>
   * @param ep_res 所需要查询的边属性结果<列名，【顶点id，列值】>
   * @param ve_res 所需要查询的边属性结果<列名，【顶点id，列值】>
   * @return 状态
   */
  ctgraph::Status getAllInRange(
      SequenceNumber_t seq, std::vector<std::string> &vps,
      std::vector<std::string> &eps, std::vector<std::string> &ves,
      std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>>
          &vp_res,
      std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>>
          &ep_res,
      std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>>
          &ve_res) {
#pragma omp parallel for
    for (auto &col : vps) {
      // 找到最后一个元素符合的版本
      next_offset_t next_offset = INVALD_NEXT_OFFSET;
      size_t vertex_index = this->vertex_index.size() - 1;
      auto &temporal_data = temporal_datas_[vertex_index];
      offset_t start_idx = 0;
      offset_t end_idx = temporal_data.version_count_ - 1;
      bool is_find = false;
      while (start_idx <= end_idx && !is_find) {
        offset_t mid_idx = (start_idx + end_idx) / 2;
        if (mid_idx < temporal_data.version_count_ - 1) {
          if (temporal_data.get_seq(mid_idx) <= seq &&
              temporal_data.get_seq(mid_idx + 1) > seq) {
            size_t count =
                std::distance(VP.columns.begin(), VP.columns.find(col));
            bool is_temporal_data_offset = false;
            for (int j = mid_idx + 1; j < temporal_data.version_count_; j++) {
              if (temporal_data.value_offset[j][count] != INVALID_OFFSET) {
                is_temporal_data_offset = true;
                vp_res[col].push_back(std::make_pair(
                    column4vertex[vertex_index],
                    VP.modify_columns[col]
                                     [temporal_data.value_offset[j][count]]));
                next_offset = temporal_data.get_next_offset(mid_idx);
                is_find = true;
                break;
              }
            }
            if (!is_temporal_data_offset) {
              vp_res[col].push_back(std::make_pair(
                  column4vertex[vertex_index], VP.columns[col][vertex_index]));
              next_offset = temporal_data.get_next_offset(mid_idx);
              is_find = true;
            }
          }
          // 如果当前元素 > seq，则目标在左侧
          if (temporal_data.get_seq(mid_idx) > seq)
            end_idx = mid_idx - 1;
          else // temporal_data.seq_[mid] < seq，但 temporal_data.seq_[mid+1] 也
               // <= seq
            start_idx = mid_idx + 1;
        } else { // mid 是最后一个元素则代表是最新的
          if (temporal_data.get_seq(mid_idx) >= seq) {
            vp_res[col].push_back(std::make_pair(
                column4vertex[vertex_index], VP.columns[col][vertex_index]));
            next_offset = temporal_data.get_next_offset(mid_idx);
            is_find = true;
          } else {
            next_offset = INVALD_NEXT_OFFSET;
            is_find = true;
          }
        }
      }
      for (int i = this->vertex_index.size() - 2; i >= 0; i--) {
        std::pair<std::string, std::any> res = std::make_pair(col, std::any());
        next_offset =
            getDatafromThisBegin(seq, i, next_offset, ValueType::VP, res);
        vp_res[col].push_back(std::make_pair(column4vertex[i], res.second));
      }
    }
#pragma omp parallel for
    for (auto &col : eps) {
      // 找到最后一个元素符合的版本
      next_offset_t next_offset = INVALD_NEXT_OFFSET;
      size_t vertex_index = this->vertex_index.size() - 1;
      auto &temporal_data = temporal_datas_[vertex_index];
      offset_t start_idx = 0;
      offset_t end_idx = temporal_data.version_count_ - 1;
      bool is_find = false;
      while (start_idx <= end_idx && !is_find) {
        offset_t mid_idx = (start_idx + end_idx) / 2;
        if (mid_idx < temporal_data.version_count_ - 1) {
          if (temporal_data.get_seq(mid_idx) <= seq &&
              temporal_data.get_seq(mid_idx + 1) > seq) {
            size_t count =
                std::distance(EP.columns.begin(), EP.columns.find(col));
            bool is_temporal_data_offset = false;
            for (int j = mid_idx + 1; j < temporal_data.version_count_; j++) {
              if (temporal_data.value_offset[j][count + vp_count] !=
                  INVALID_OFFSET) {
                is_temporal_data_offset = true;
                ep_res[col].push_back(std::make_pair(
                    column4vertex[vertex_index],
                    EP.modify_columns[col]
                                     [temporal_data
                                          .value_offset[j][count + vp_count]]));
                next_offset = temporal_data.get_next_offset(mid_idx);
                is_find = true;
                break;
              }
            }
            if (!is_temporal_data_offset) {
              ep_res[col].push_back(std::make_pair(
                  column4vertex[vertex_index], EP.columns[col][vertex_index]));
              next_offset = temporal_data.get_next_offset(mid_idx);
              is_find = true;
            }
          }
          // 如果当前元素 > seq，则目标在左侧
          if (temporal_data.get_seq(mid_idx) > seq)
            end_idx = mid_idx - 1;
          else // temporal_data.seq_[mid] < seq，但 temporal_data.seq_[mid+1] 也
               // <= seq
            start_idx = mid_idx + 1;
        } else { // mid 是最后一个元素则代表是最新的
          if (temporal_data.get_seq(mid_idx) >= seq) {
            ep_res[col].push_back(std::make_pair(
                column4vertex[vertex_index], EP.columns[col][vertex_index]));
            next_offset = temporal_data.get_next_offset(mid_idx);
            is_find = true;
          } else {
            next_offset = INVALD_NEXT_OFFSET;
            is_find = true;
          }
        }
      }
      for (int i = this->vertex_index.size() - 2; i >= 0; i--) {
        std::pair<std::string, std::any> res = std::make_pair(col, std::any());
        next_offset =
            getDatafromThisBegin(seq, i, next_offset, ValueType::EP, res);
        ep_res[col].emplace_back(std::make_pair(column4vertex[i], res.second));
      }
    }
#pragma omp parallel for
    for (auto &col : ves) {
      // 找到最后一个元素符合的版本
      next_offset_t next_offset = INVALD_NEXT_OFFSET;
      size_t vertex_index = this->vertex_index.size() - 1;
      auto &temporal_data = temporal_datas_[vertex_index];
      offset_t start_idx = 0;
      offset_t end_idx = temporal_data.version_count_ - 1;
      bool is_find = false;
      while (start_idx <= end_idx && !is_find) {
        offset_t mid_idx = (start_idx + end_idx) / 2;
        if (mid_idx < temporal_data.version_count_ - 1) {
          if (temporal_data.get_seq(mid_idx) <= seq &&
              temporal_data.get_seq(mid_idx + 1) > seq) {
            size_t count =
                std::distance(VE.columns.begin(), VE.columns.find(col));
            bool is_temporal_data_offset = false;
            for (int j = mid_idx + 1; j < temporal_data.version_count_; j++) {
              if (temporal_data.value_offset[j][count + vp_count + ep_count] !=
                  INVALID_OFFSET) {
                is_temporal_data_offset = true;
                ve_res[col].push_back(std::make_pair(
                    column4vertex[vertex_index],
                    VE.modify_columns[col]
                                     [temporal_data.value_offset
                                          [j][count + vp_count + ep_count]]));
                next_offset = temporal_data.get_next_offset(mid_idx);
                is_find = true;
                break;
              }
            }
            if (!is_temporal_data_offset) {
              ve_res[col].push_back(std::make_pair(
                  column4vertex[vertex_index], VE.columns[col][vertex_index]));
              next_offset = temporal_data.get_next_offset(mid_idx);
              is_find = true;
            }
          }
          // 如果当前元素 > seq，则目标在左侧
          if (temporal_data.get_seq(mid_idx) > seq)
            end_idx = mid_idx - 1;
          else // temporal_data.seq_[mid] < seq，但 temporal_data.seq_[mid+1] 也
               // <= seq
            start_idx = mid_idx + 1;
        } else { // mid 是最后一个元素则代表是最新的
          if (temporal_data.get_seq(mid_idx) >= seq) {
            ve_res[col].push_back(std::make_pair(
                column4vertex[vertex_index], VE.columns[col][vertex_index]));
            next_offset = temporal_data.get_next_offset(mid_idx);
            is_find = true;
          } else {
            next_offset = INVALD_NEXT_OFFSET;
            is_find = true;
          }
        }
      }
      for (int i = this->vertex_index.size() - 2; i >= 0; i--) {
        std::pair<std::string, std::any> res = std::make_pair(col, std::any());
        next_offset =
            getDatafromThisBegin(seq, i, next_offset, ValueType::VE, res);
        ve_res[col].push_back(std::make_pair(column4vertex[i], res.second));
      }
    }
    return ctgraph::kOk;
  }

  std::unordered_map<std::string, std::any>
  getLatestVP(const size_t vertex_id, std::vector<std::string> cols) const {
    auto it = vertex_index.find(vertex_id);
    if (it != vertex_index.end()) {
      std::unordered_map<std::string, std::any> res;
      VP.getRow4Index(it->second, cols, res);
      return res;
    }
    return {};
  }

  ctgraph::Status getLatestVP(const size_t vertex_id,
                              std::vector<std::string> cols,
                              std::unordered_map<std::string, std::any> &res) {
    auto it = vertex_index.find(vertex_id);
    if (it != vertex_index.end()) {
      VP.getRow4Index(it->second, cols, res);
      return ctgraph::kOk;
    }
    return ctgraph::kNotFound;
  }

  std::unordered_map<std::string, std::any>
  getLatestEP(const size_t edge_id, std::vector<std::string> cols) const {
    auto it = vertex_index.find(edge_id);
    if (it != vertex_index.end()) {
      std::unordered_map<std::string, std::any> res;
      EP.getRow4Index(it->second, cols, res);
      return res;
    }
    return {};
  }

  ctgraph::Status getLatestEP(const size_t edge_id,
                              std::vector<std::string> cols,
                              std::unordered_map<std::string, std::any> &res) {
    auto it = vertex_index.find(edge_id);
    if (it != vertex_index.end()) {
      EP.getRow4Index(it->second, cols, res);
      return ctgraph::kOk;
    }
    return ctgraph::kNotFound;
  }

  std::unordered_map<std::string, std::any>
  getLatestVE(const size_t edge_id, std::vector<std::string> cols) const {
    auto it = vertex_index.find(edge_id);
    if (it != vertex_index.end()) {
      std::unordered_map<std::string, std::any> res;
      VE.getRow4Index(it->second, cols, res);
      return res;
    }
    return {};
  }

  ctgraph::Status getLatestVE(const size_t edge_id,
                              std::vector<std::string> cols,
                              std::unordered_map<std::string, std::any> &res) {
    auto it = vertex_index.find(edge_id);
    if (it != vertex_index.end()) {
      VE.getRow4Index(it->second, cols, res);
      return ctgraph::kOk;
    }
    return ctgraph::kNotFound;
  }
};

// 辅助函数：打印一行数据
void printRow(const std::unordered_map<std::string, std::any> &row) {
  std::cout << "{ ";
  for (const auto &p : row) {
    std::cout << p.first << ": ";
    printAny(p.second);
    std::cout << ", ";
  }
  std::cout << "}" << std::endl;
}
} // namespace column
