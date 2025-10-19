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

#include <any>
#include <string>
#include <unordered_map>
#include <vector>
#include "ColumnTypes.hpp"
#include "TemporalData.hpp"
#include "types.hpp"

namespace column
{
    // Auxiliary function: Compares two std::any objects for equality, supporting only int, std::string, and double type
    bool anyEqual(const std::any& a, const std::any& b)
    {
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

    // Auxiliary Function: Print the Value of std::any
    void printAny(const std::any& a)
    {
        if (!a.has_value())
        {
            std::cout << "null";
        }
        else if (a.type() == typeid(int))
        {
            std::cout << std::any_cast<int>(a);
        }
        else if (a.type() == typeid(std::string))
        {
            std::cout << std::any_cast<std::string>(a);
        }
        else if (a.type() == typeid(double))
        {
            std::cout << std::any_cast<double>(a);
        }
        else
        {
            std::cout << "unknown";
        }
    }

    class ColumnTable
    {
    public:
        std::unordered_map<std::string, std::vector<std::any>> columns;
        std::unordered_map<std::string, std::vector<std::any>> modify_columns;
        size_t num_rows = 0;

        // Add a row of data, where the key-value pairs in the row represent the column names and their corresponding
        // value
        void addRow(const std::unordered_map<std::string, std::any>& row)
        {
            std::unordered_map<std::string, std::any> complete_row = row;
            // For the columns that already exist in the table,
            // if the current row does not have a corresponding attribute, it should be filled with a null value
            for (const auto& col : columns)
            {
                if (complete_row.find(col.first) == complete_row.end())
                {
                    complete_row[col.first] = std::any();
                }
            }
            // For the newly emerged columns in the current row,
            // create new columns in the table and populate all previous rows with null values
            for (const auto& p : complete_row)
            {
                if (columns.find(p.first) == columns.end())
                {
                    columns[p.first] = std::vector<std::any>(num_rows, std::any());
                }
            }
            // Append the value of the current row to all columns
            for (auto& col : columns)
            {
                col.second.push_back(complete_row[col.first]);
            }
            num_rows++;
        }

        /**
         * Update the data and return the corresponding offset positions for each row in the modify_columns
         * @return The behavior of making modifications corresponds to the offset position,
         * while the behavior of not making modifications is referred to as INVALID_OFFSET
         */
        std::vector<offset_t> update(offset_t offset, const std::unordered_map<std::string, std::any>& rows)
        {
            std::vector<offset_t> rt_offset;
            // for (const auto &row: rows)
            // {
            //     std::any value_old = columns[row.first][offset];
            //     size_t index = modify_columns[row.first].size();
            //     modify_columns[row.first].emplace_back(value_old);
            //     columns[row.first][offset] = row.second;
            //     rt_offset.emplace_back(index);
            // }

            for (auto& col : columns)
            {
                if (rows.find(col.first) == rows.end())
                {
                    rt_offset.emplace_back(INVALID_OFFSET);
                }
                else
                {
                    std::any value_old = col.second[offset];
                    size_t index = modify_columns[col.first].size();
                    modify_columns[col.first].emplace_back(value_old);
                    col.second[offset] = rows.find(col.first)->second;
                    rt_offset.emplace_back(index);
                }
            }
            return rt_offset;
        }
        // Data used solely for experimental purposes
        void just_update(offset_t offset, const std::unordered_map<std::string, std::any>& rows)
        {
            // for (const auto &row: rows)
            // {
            //     std::any value_old = columns[row.first][offset];
            //     size_t index = modify_columns[row.first].size();
            //     modify_columns[row.first].emplace_back(value_old);
            //     columns[row.first][offset] = row.second;
            //     rt_offset.emplace_back(index);
            // }
            for (auto& col : columns)
            {
                if (rows.find(col.first) != rows.end())
                {
                    col.second[offset] = rows.find(col.first)->second;
                }
            }
        }

        // Return the specified number of rows of data for the given line number
        void getRow4Index(size_t index, std::vector<std::string> cols,
                          std::unordered_map<std::string, std::any>& res) const
        {
            for (const auto& col : cols)
            {
                if (columns.find(col) != columns.end())
                {
                    res[col] = columns.at(col)[index];
                }
            }
        }
        // Return all the latest data for the specified line number
        std::unordered_map<std::string, std::any> get_row(size_t index) const
        {
            std::unordered_map<std::string, std::any> result;
            for (const auto& p : columns)
            {
                if (index < p.second.size())
                    result[p.first] = p.second[index];
            }
            return result;
        }

        // Simple Query: Returns all row numbers in the specified column that are equal to the given value
        std::vector<size_t> query(const std::string& col, const std::any& value) const
        {
            std::vector<size_t> indices;
            if (columns.find(col) == columns.end())
                return indices;
            const auto& vec = columns.at(col);
            for (size_t i = 0; i < vec.size(); i++)
            {
                if (anyEqual(vec[i], value))
                {
                    indices.push_back(i);
                }
            }
            return indices;
        }
    };

    enum ValueType
    {
        VP,
        EP,
        VE
    };

    class GraphDB
    {
    public:
        ColumnTable VP;
        ColumnTable EP;
        ColumnTable VE;

        uint8_t vp_count;
        uint8_t ep_count;
        uint8_t ve_count;

        std::vector<TemporalData> temporal_datas_;

        std::unordered_map<size_t, size_t> vertex_index;
        std::unordered_map<size_t, size_t> column4vertex;

        /**
         * Initialization of loading data serves as an example with three elements:
         * VP, EP, and VE, currently only at the insertion point
         * @param seq version num
         * @param vertex_id v_id
         * @param properties v_properties
         */
        void loadTableVertex(SequenceNumber_t seq, const size_t vertex_id,
                             const std::unordered_map<std::string, std::any>& properties = {})
        {
            TemporalData new_temporal_data;
            this->vp_count = 1;
            this->ep_count = 1;
            this->ve_count = 1;

            new_temporal_data.header_.lock_ = 0;
            new_temporal_data.header_.exist_bitmap_ = 0x1;
            // new_temporal_data.seq_[0] = seq;
            new_temporal_data.set_seq(0, seq);
            // new_temporal_data.modify_bitmap_[0] = 0x1;
            new_temporal_data.set_modify_bitmap(0, 0x1);
            if (temporal_datas_.size() == 0) // The version chain links the latest version of its preceding element
            {
                // new_temporal_data.next_offset_[0] = INVALD_NEXT_OFFSET;
                new_temporal_data.set_next_offset(0, INVALD_NEXT_OFFSET);
            }
            else
            {
                // new_temporal_data.next_offset_[0] =
                // temporal_datas_.rbegin()->version_count_ - 1;
                new_temporal_data.set_next_offset(0, temporal_datas_.rbegin()->version_count_ - 1);
            }
            std::vector<offset_t> tmp_values;
            for (int i = 0; i < vp_count + ep_count + ve_count; i++)
            {
                tmp_values.emplace_back(INVALID_OFFSET);
            }
            new_temporal_data.value_offset.emplace_back(tmp_values);
            new_temporal_data.version_count_ = 1;

            temporal_datas_.emplace_back(new_temporal_data);
            // Insert actual data; even if the data is empty, it should be filled in
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
         * Initialization of loading data
         * @param seq current time
         * @param vertex_id v_id
         * @param vps v_p
         * @param eps e_p
         * @param ves e_topo
         */
        void LoadTable(SequenceNumber_t seq, const size_t vertex_id,
                       const std::unordered_map<std::string, std::any>& vps = {},
                       const std::unordered_map<std::string, std::any>& eps = {},
                       const std::unordered_map<std::string, std::any>& ves = {})
        {
            this->vp_count = vps.size();
            this->ep_count = eps.size();
            this->ve_count = ves.size();

            TemporalData new_temporal_data;
            new_temporal_data.header_.lock_ = 0;
            new_temporal_data.set_seq(0, seq);
            new_temporal_data.set_modify_bitmap(0, 0);

            if (temporal_datas_.size() == 0)
            {
                new_temporal_data.set_next_offset(0, INVALD_NEXT_OFFSET);
            }
            else
            {
                new_temporal_data.set_next_offset(0, temporal_datas_.rbegin()->version_count_ - 1);
            }

            std::vector<offset_t> tmp_values(vp_count + ep_count + ve_count, INVALID_OFFSET);
            new_temporal_data.value_offset.emplace_back(tmp_values);
            new_temporal_data.version_count_ = 1;

            VP.addRow(vps);
            EP.addRow(eps);
            VE.addRow(ves);
            int count = 0;
            for (auto& col : VP.columns)
            {
                if (vps.find(col.first)->second.has_value())
                {
                    // new_temporal_data.modify_bitmap_[0] |= (1 << count);
                    new_temporal_data.set_modify_bitmap(0, new_temporal_data.get_modify_bitmap(0) | (1 << count));
                }
                count++;
            }
            count = 0;
            for (auto& col : EP.columns)
            {
                if (eps.find(col.first)->second.has_value())
                {
                    // new_temporal_data.modify_bitmap_[0] |= (1 << (count + vp_count));
                    new_temporal_data.set_modify_bitmap(
                        0, new_temporal_data.get_modify_bitmap(0) | (1 << (count + vp_count)));
                }
                count++;
            }
            count = 0;
            for (auto& col : VE.columns)
            {
                if (ves.find(col.first)->second.has_value())
                {
                    // new_temporal_data.modify_bitmap_[0] |= (1 << (count + vp_count +
                    // ep_count));
                    new_temporal_data.set_modify_bitmap(
                        0, new_temporal_data.get_modify_bitmap(0) | (1 << (count + vp_count + ep_count)));
                }
                count++;
            }
            // new_temporal_data.header_.exist_bitmap_ =
            // new_temporal_data.modify_bitmap_[0];
            new_temporal_data.header_.exist_bitmap_ = new_temporal_data.get_modify_bitmap(0);
            temporal_datas_.emplace_back(new_temporal_data);
            vertex_index[vertex_id] = VP.num_rows - 1;
            column4vertex[VP.num_rows - 1] = vertex_id;
        }

        void update(SequenceNumber_t seq, const size_t vertex_id,
                    const std::unordered_map<std::string, std::any>& vps = {},
                    const std::unordered_map<std::string, std::any>& eps = {},
                    const std::unordered_map<std::string, std::any>& ves = {})
        {
            offset_t node_offset = vertex_index[vertex_id];
            auto& header = temporal_datas_[node_offset].header_;
            while (header.lock_ == 1)
            {
            }
            header.lock_ = 1;
            auto& temporal_data = temporal_datas_[node_offset];
            version_count_t version_count = temporal_data.version_count_;
            // temporal_data.seq_[version_count] = seq;
            temporal_data.set_seq(version_count, seq);

            int i = 0;
            bitmap_t vp_tmp = 0;
            bitmap_t ep_tmp = 0;
            bitmap_t ve_tmp = 0;
#pragma omp parallel for
            for (auto it = VP.columns.begin(); it != VP.columns.end(); ++it)
            {
                size_t count = std::distance(VP.columns.begin(), it);
                if (vps.find(it->first) != vps.end())
                {
                    vp_tmp |= (1 << count);
                }
            }
#pragma omp parallel for
            for (auto it = EP.columns.begin(); it != EP.columns.end(); ++it)
            {
                size_t count = std::distance(EP.columns.begin(), it);
                if (eps.find(it->first) != eps.end())
                {
                    ep_tmp |= (1 << count);
                }
            }
#pragma omp parallel for
            for (auto it = VE.columns.begin(); it != VE.columns.end(); ++it)
            {
                size_t count = std::distance(VE.columns.begin(), it);
                if (ves.find(it->first) != ves.end())
                {
                    ve_tmp |= (1 << count);
                }
            }
            bitmap_t modify_tmp = vp_tmp | (ep_tmp << this->vp_count) | (ve_tmp << (ep_count + vp_count));
            header.exist_bitmap_ |= modify_tmp;
            temporal_data.set_modify_bitmap(version_count, modify_tmp);
            if (node_offset != 0)
            {
                temporal_data.set_next_offset(version_count, temporal_datas_[node_offset - 1].version_count_ - 1);
            }
            else
            {
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
        }

        void just_update(const size_t vertex_id, const std::unordered_map<std::string, std::any>& vps)
        {
            offset_t node_offset = vertex_index[vertex_id];
            bitmap_t vp_tmp = 0;
            for (auto it = VP.columns.begin(); it != VP.columns.end(); ++it)
            {
                size_t count = std::distance(VP.columns.begin(), it);
                if (vps.find(it->first) != vps.end())
                {
                    vp_tmp |= (1 << count);
                }
            }
            temporal_datas_[node_offset].header_.exist_bitmap_ |= vp_tmp;
            temporal_datas_[node_offset].set_modify_bitmap(0, temporal_datas_[node_offset].header_.exist_bitmap_);
            VP.just_update(node_offset, vps);
        }
        /**
         * Obtain data within the specified version range
         * @param seq The version number that needs to be queried.
         * @param vertex_id v_id
         * @param vps v_p
         * @param eps e_p
         * @param ves e_topo
         * @return status
         */
        ctgraph::Status getVersionInRange(SequenceNumber_t seq, const size_t vertex_id,
                                          std::unordered_map<std::string, std::any>& vps,
                                          std::unordered_map<std::string, std::any>& eps,
                                          std::unordered_map<std::string, std::any>& ves)
        {
            offset_t node_offset = vertex_index[vertex_id];
            auto& header = temporal_datas_[node_offset].header_;
            while (header.lock_ == 1)
            {
            }
            header.lock_ = 2;
            const auto& temporal_data = temporal_datas_[node_offset];

            offset_t start_idx = 0;
            offset_t end_idx = temporal_data.version_count_ - 1;
            auto lateset_offset = vertex_index.find(vertex_id);
            while (start_idx <= end_idx)
            {
                offset_t mid_idx = (start_idx + end_idx) / 2;
                if (mid_idx < temporal_data.version_count_ - 1)
                {
                    if (temporal_data.get_seq(mid_idx) <= seq && temporal_data.get_seq(mid_idx + 1) > seq)
                    {
#pragma omp parallel for
                        for (auto it = VP.columns.begin(); it != VP.columns.end(); ++it)
                        {
                            size_t count = std::distance(VP.columns.begin(), it);
                            if (vps.find(it->first) != vps.end())
                            {
                                bool is_temporal_data_offset = false;
                                for (int i = mid_idx + 1; i < temporal_data.version_count_; i++)
                                {
                                    if (temporal_data.value_offset[i][count] != INVALID_OFFSET)
                                    {
                                        vps.find(it->first)->second =
                                            VP.modify_columns[it->first][temporal_data.value_offset[i][count]];
                                        is_temporal_data_offset = true;
                                        break;
                                    }
                                }
                                if (!is_temporal_data_offset)
                                {
                                    vps.find(it->first)->second = VP.columns[it->first][node_offset];
                                }
                            }
                        }
#pragma omp parallel for
                        for (auto it = EP.columns.begin(); it != EP.columns.end(); ++it)
                        {
                            size_t count = std::distance(EP.columns.begin(), it);
                            if (eps.find(it->first) != eps.end())
                            {
                                bool is_temporal_data_offset = false;
                                for (int i = mid_idx + 1; i < temporal_data.version_count_; i++)
                                {
                                    if (temporal_data.value_offset[i][count + vp_count] != INVALID_OFFSET)
                                    {
                                        eps.find(it->first)->second =
                                            EP.modify_columns[it->first]
                                                             [temporal_data.value_offset[i][count + vp_count]];
                                        is_temporal_data_offset = true;
                                        break;
                                    }
                                }
                                if (!is_temporal_data_offset)
                                {
                                    eps.find(it->first)->second = EP.columns[it->first][node_offset];
                                }
                            }
                        }

#pragma omp parallel for
                        for (auto it = VE.columns.begin(); it != VE.columns.end(); ++it)
                        {
                            size_t count = std::distance(VE.columns.begin(), it);
                            if (ves.find(it->first) != ves.end())
                            {
                                bool is_temporal_data_offset = false;
                                for (int i = mid_idx + 1; i < temporal_data.version_count_; i++)
                                {
                                    if (temporal_data.value_offset[i][count + vp_count + ep_count] != INVALID_OFFSET)
                                    {
                                        ves.find(it->first)->second =
                                            VE.modify_columns
                                                [it->first][temporal_data.value_offset[i][count + vp_count + ep_count]];
                                        is_temporal_data_offset = true;
                                        break;
                                    }
                                }
                                if (!is_temporal_data_offset)
                                {
                                    ves.find(it->first)->second = VE.columns[it->first][node_offset];
                                }
                            }
                        }
                        header.lock_ = 0;
                        return ctgraph::kOk;
                    }
                    if (temporal_data.get_seq(mid_idx) > seq)
                        end_idx = mid_idx - 1;
                    else
                        start_idx = mid_idx + 1;
                }
                else
                {
                    if (temporal_data.get_seq(mid_idx) <= seq)
                    {
                        for (auto& col : vps)
                        {
                            col.second = VP.columns[col.first][node_offset];
                        }
                        for (auto& col : eps)
                        {
                            col.second = EP.columns[col.first][node_offset];
                        }
                        for (auto& col : ves)
                        {
                            col.second = VE.columns[col.first][node_offset];
                        }
                        header.lock_ = 0;
                        return ctgraph::kOk;
                    }
                    header.lock_ = 0;
                    return ctgraph::kNotFound;
                }
            }
            header.lock_ = 0;
            return ctgraph::kNotFound;
        }
        /**
         * Identify the corresponding series from the beginning
         * @param seq The version number that needs to be queried
         * @param row_index query row index
         * @param begin_offset query offset
         * @param value_type query type
         * @param res query result
         * @return next offset
         */
        offset_t getDatafromThisBegin(SequenceNumber_t seq, size_t row_index, offset_t begin_offset,
                                      ValueType value_type, std::pair<std::string, std::any>& res)
        {
            auto& temporal_data = temporal_datas_[row_index];
            offset_t start_offset = begin_offset;
            offset_t end_offset = temporal_data.version_count_ - 1;
            switch (value_type)
            {
            case ValueType::VP:
                {
                    auto count = std::distance(VP.columns.begin(), VP.columns.find(res.first));
                    for (int i = start_offset; i < end_offset; i++)
                    {
                        if (temporal_data.get_seq(i) <= seq && temporal_data.get_seq(i + 1) > seq)
                        {
                            bool is_temporal_data_offset = false;
                            for (int j = i + 1; j < temporal_data.version_count_; j++)
                            {
                                if (temporal_data.value_offset[j][count] != INVALID_OFFSET)
                                {
                                    res.second = VP.modify_columns[res.first][temporal_data.value_offset[j][count]];
                                    is_temporal_data_offset = true;
                                    return temporal_data.get_next_offset(i);
                                }
                            }
                            if (!is_temporal_data_offset)
                            {
                                res.second = VP.columns[res.first][row_index];
                                return temporal_data.get_next_offset(i);
                            }
                        }
                    }
                    if (temporal_data.get_seq(end_offset) <= seq)
                    {
                        res.second = VP.columns[res.first][row_index];
                        return temporal_data.get_next_offset(end_offset);
                    }
                }
                break;
            case ValueType::EP:
                {
                    auto count = std::distance(EP.columns.begin(), EP.columns.find(res.first));
                    for (int i = start_offset; i < end_offset; i++)
                    {
                        if (temporal_data.get_seq(i) <= seq && temporal_data.get_seq(i + 1) > seq)
                        {
                            bool is_temporal_data_offset = false;
                            for (int j = i + 1; j < temporal_data.version_count_; j++)
                            {
                                if (temporal_data.value_offset[j][count + vp_count] != INVALID_OFFSET)
                                {
                                    res.second =
                                        EP.modify_columns[res.first][temporal_data.value_offset[j][count + vp_count]];
                                    is_temporal_data_offset = true;
                                    return temporal_data.get_next_offset(i);
                                }
                            }
                            if (!is_temporal_data_offset)
                            {
                                res.second = EP.columns[res.first][row_index];
                                return temporal_data.get_next_offset(i);
                            }
                        }
                    }
                    if (temporal_data.get_seq(end_offset) <= seq)
                    {
                        res.second = EP.columns[res.first][row_index];
                        return temporal_data.get_next_offset(end_offset);
                    }
                }
                break;
            case ValueType::VE:
                {
                    auto count = std::distance(VE.columns.begin(), VE.columns.find(res.first));
                    for (int i = start_offset; i < end_offset; i++)
                    {
                        if (temporal_data.get_seq(i) <= seq && temporal_data.get_seq(i + 1) > seq)
                        {
                            bool is_temporal_data_offset = false;
                            for (int j = i + 1; j < temporal_data.version_count_; j++)
                            {
                                if (temporal_data.value_offset[j][count + vp_count + ep_count] != INVALID_OFFSET)
                                {
                                    res.second =
                                        VE.modify_columns[res.first]
                                                         [temporal_data.value_offset[j][count + vp_count + ep_count]];
                                    is_temporal_data_offset = true;
                                    return temporal_data.get_next_offset(i);
                                }
                            }
                            if (!is_temporal_data_offset)
                            {
                                res.second = VE.columns[res.first][row_index];
                                return temporal_data.get_next_offset(i);
                            }
                        }
                    }
                    if (temporal_data.get_seq(end_offset) <= seq)
                    {
                        res.second = VE.columns[res.first][row_index];
                        return temporal_data.get_next_offset(end_offset);
                    }
                }
                break;
            }
            return 0;
        }

        /**
         * Retrieve all data within a specified version range
         * @param seq        The version number to query
         * @param vps        The vertex properties to query
         * @param eps        The edge properties to query
         * @param ves        The edge properties to query
         * @param vp_res     The result for the queried vertex properties <column name, [vertex id, column value]>
         * @param ep_res     The result for the queried edge properties <column name, [vertex id, column value]>
         * @param ve_res     The result for the queried edge properties <column name, [vertex id, column value]>
         * @return           Status
         */
        ctgraph::Status getAllInRange(SequenceNumber_t seq, std::vector<std::string>& vps,
                                      std::vector<std::string>& eps, std::vector<std::string>& ves,
                                      std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>>& vp_res,
                                      std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>>& ep_res,
                                      std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>>& ve_res)
        {
#pragma omp parallel for
            for (auto& col : vps)
            {
                next_offset_t next_offset = INVALD_NEXT_OFFSET;
                size_t vertex_index = this->vertex_index.size() - 1;
                auto& temporal_data = temporal_datas_[vertex_index];
                offset_t start_idx = 0;
                offset_t end_idx = temporal_data.version_count_ - 1;
                bool is_find = false;
                while (start_idx <= end_idx && !is_find)
                {
                    offset_t mid_idx = (start_idx + end_idx) / 2;
                    if (mid_idx < temporal_data.version_count_ - 1)
                    {
                        if (temporal_data.get_seq(mid_idx) <= seq && temporal_data.get_seq(mid_idx + 1) > seq)
                        {
                            size_t count = std::distance(VP.columns.begin(), VP.columns.find(col));
                            bool is_temporal_data_offset = false;
                            for (int j = mid_idx + 1; j < temporal_data.version_count_; j++)
                            {
                                if (temporal_data.value_offset[j][count] != INVALID_OFFSET)
                                {
                                    is_temporal_data_offset = true;
                                    vp_res[col].push_back(
                                        std::make_pair(column4vertex[vertex_index],
                                                       VP.modify_columns[col][temporal_data.value_offset[j][count]]));
                                    next_offset = temporal_data.get_next_offset(mid_idx);
                                    is_find = true;
                                    break;
                                }
                            }
                            if (!is_temporal_data_offset)
                            {
                                vp_res[col].push_back(
                                    std::make_pair(column4vertex[vertex_index], VP.columns[col][vertex_index]));
                                next_offset = temporal_data.get_next_offset(mid_idx);
                                is_find = true;
                            }
                        }
                        if (temporal_data.get_seq(mid_idx) > seq)
                            end_idx = mid_idx - 1;
                        else
                            start_idx = mid_idx + 1;
                    }
                    else
                    {
                        if (temporal_data.get_seq(mid_idx) >= seq)
                        {
                            vp_res[col].push_back(
                                std::make_pair(column4vertex[vertex_index], VP.columns[col][vertex_index]));
                            next_offset = temporal_data.get_next_offset(mid_idx);
                            is_find = true;
                        }
                        else
                        {
                            next_offset = INVALD_NEXT_OFFSET;
                            is_find = true;
                        }
                    }
                }
                for (int i = this->vertex_index.size() - 2; i >= 0; i--)
                {
                    std::pair<std::string, std::any> res = std::make_pair(col, std::any());
                    next_offset = getDatafromThisBegin(seq, i, next_offset, ValueType::VP, res);
                    vp_res[col].push_back(std::make_pair(column4vertex[i], res.second));
                }
            }
#pragma omp parallel for
            for (auto& col : eps)
            {
                next_offset_t next_offset = INVALD_NEXT_OFFSET;
                size_t vertex_index = this->vertex_index.size() - 1;
                auto& temporal_data = temporal_datas_[vertex_index];
                offset_t start_idx = 0;
                offset_t end_idx = temporal_data.version_count_ - 1;
                bool is_find = false;
                while (start_idx <= end_idx && !is_find)
                {
                    offset_t mid_idx = (start_idx + end_idx) / 2;
                    if (mid_idx < temporal_data.version_count_ - 1)
                    {
                        if (temporal_data.get_seq(mid_idx) <= seq && temporal_data.get_seq(mid_idx + 1) > seq)
                        {
                            size_t count = std::distance(EP.columns.begin(), EP.columns.find(col));
                            bool is_temporal_data_offset = false;
                            for (int j = mid_idx + 1; j < temporal_data.version_count_; j++)
                            {
                                if (temporal_data.value_offset[j][count + vp_count] != INVALID_OFFSET)
                                {
                                    is_temporal_data_offset = true;
                                    ep_res[col].push_back(std::make_pair(
                                        column4vertex[vertex_index],
                                        EP.modify_columns[col][temporal_data.value_offset[j][count + vp_count]]));
                                    next_offset = temporal_data.get_next_offset(mid_idx);
                                    is_find = true;
                                    break;
                                }
                            }
                            if (!is_temporal_data_offset)
                            {
                                ep_res[col].push_back(
                                    std::make_pair(column4vertex[vertex_index], EP.columns[col][vertex_index]));
                                next_offset = temporal_data.get_next_offset(mid_idx);
                                is_find = true;
                            }
                        }
                        //
                        if (temporal_data.get_seq(mid_idx) > seq)
                            end_idx = mid_idx - 1;
                        else
                            start_idx = mid_idx + 1;
                    }
                    else
                    {
                        if (temporal_data.get_seq(mid_idx) >= seq)
                        {
                            ep_res[col].push_back(
                                std::make_pair(column4vertex[vertex_index], EP.columns[col][vertex_index]));
                            next_offset = temporal_data.get_next_offset(mid_idx);
                            is_find = true;
                        }
                        else
                        {
                            next_offset = INVALD_NEXT_OFFSET;
                            is_find = true;
                        }
                    }
                }
                for (int i = this->vertex_index.size() - 2; i >= 0; i--)
                {
                    std::pair<std::string, std::any> res = std::make_pair(col, std::any());
                    next_offset = getDatafromThisBegin(seq, i, next_offset, ValueType::EP, res);
                    ep_res[col].emplace_back(std::make_pair(column4vertex[i], res.second));
                }
            }
#pragma omp parallel for
            for (auto& col : ves)
            {
                next_offset_t next_offset = INVALD_NEXT_OFFSET;
                size_t vertex_index = this->vertex_index.size() - 1;
                auto& temporal_data = temporal_datas_[vertex_index];
                offset_t start_idx = 0;
                offset_t end_idx = temporal_data.version_count_ - 1;
                bool is_find = false;
                while (start_idx <= end_idx && !is_find)
                {
                    offset_t mid_idx = (start_idx + end_idx) / 2;
                    if (mid_idx < temporal_data.version_count_ - 1)
                    {
                        if (temporal_data.get_seq(mid_idx) <= seq && temporal_data.get_seq(mid_idx + 1) > seq)
                        {
                            size_t count = std::distance(VE.columns.begin(), VE.columns.find(col));
                            bool is_temporal_data_offset = false;
                            for (int j = mid_idx + 1; j < temporal_data.version_count_; j++)
                            {
                                if (temporal_data.value_offset[j][count + vp_count + ep_count] != INVALID_OFFSET)
                                {
                                    is_temporal_data_offset = true;
                                    ve_res[col].push_back(std::make_pair(
                                        column4vertex[vertex_index],
                                        VE.modify_columns[col]
                                                         [temporal_data.value_offset[j][count + vp_count + ep_count]]));
                                    next_offset = temporal_data.get_next_offset(mid_idx);
                                    is_find = true;
                                    break;
                                }
                            }
                            if (!is_temporal_data_offset)
                            {
                                ve_res[col].push_back(
                                    std::make_pair(column4vertex[vertex_index], VE.columns[col][vertex_index]));
                                next_offset = temporal_data.get_next_offset(mid_idx);
                                is_find = true;
                            }
                        }
                        if (temporal_data.get_seq(mid_idx) > seq)
                            end_idx = mid_idx - 1;
                        else
                            start_idx = mid_idx + 1;
                    }
                    else
                    {
                        if (temporal_data.get_seq(mid_idx) >= seq)
                        {
                            ve_res[col].push_back(
                                std::make_pair(column4vertex[vertex_index], VE.columns[col][vertex_index]));
                            next_offset = temporal_data.get_next_offset(mid_idx);
                            is_find = true;
                        }
                        else
                        {
                            next_offset = INVALD_NEXT_OFFSET;
                            is_find = true;
                        }
                    }
                }
                for (int i = this->vertex_index.size() - 2; i >= 0; i--)
                {
                    std::pair<std::string, std::any> res = std::make_pair(col, std::any());
                    next_offset = getDatafromThisBegin(seq, i, next_offset, ValueType::VE, res);
                    ve_res[col].push_back(std::make_pair(column4vertex[i], res.second));
                }
            }
            return ctgraph::kOk;
        }


        std::unordered_map<std::string, std::any> getLatestVP(const size_t vertex_id,
                                                              std::vector<std::string> cols) const
        {
            auto it = vertex_index.find(vertex_id);
            if (it != vertex_index.end())
            {
                std::unordered_map<std::string, std::any> res;
                VP.getRow4Index(it->second, cols, res);
                return res;
            }
            return {};
        }

        ctgraph::Status getLatestVP(const size_t vertex_id, std::vector<std::string> cols,
                                    std::unordered_map<std::string, std::any>& res)
        {
            auto it = vertex_index.find(vertex_id);
            if (it != vertex_index.end())
            {
                VP.getRow4Index(it->second, cols, res);
                return ctgraph::kOk;
            }
            return ctgraph::kNotFound;
        }

        std::unordered_map<std::string, std::any> getLatestEP(const size_t edge_id, std::vector<std::string> cols) const
        {
            auto it = vertex_index.find(edge_id);
            if (it != vertex_index.end())
            {
                std::unordered_map<std::string, std::any> res;
                EP.getRow4Index(it->second, cols, res);
                return res;
            }
            return {};
        }

        ctgraph::Status getLatestEP(const size_t edge_id, std::vector<std::string> cols,
                                    std::unordered_map<std::string, std::any>& res)
        {
            auto it = vertex_index.find(edge_id);
            if (it != vertex_index.end())
            {
                EP.getRow4Index(it->second, cols, res);
                return ctgraph::kOk;
            }
            return ctgraph::kNotFound;
        }

        std::unordered_map<std::string, std::any> getLatestVE(const size_t edge_id, std::vector<std::string> cols) const
        {
            auto it = vertex_index.find(edge_id);
            if (it != vertex_index.end())
            {
                std::unordered_map<std::string, std::any> res;
                VE.getRow4Index(it->second, cols, res);
                return res;
            }
            return {};
        }

        ctgraph::Status getLatestVE(const size_t edge_id, std::vector<std::string> cols,
                                    std::unordered_map<std::string, std::any>& res)
        {
            auto it = vertex_index.find(edge_id);
            if (it != vertex_index.end())
            {
                VE.getRow4Index(it->second, cols, res);
                return ctgraph::kOk;
            }
            return ctgraph::kNotFound;
        }
    };

    void printRow(const std::unordered_map<std::string, std::any>& row)
    {
        std::cout << "{ ";
        for (const auto& p : row)
        {
            std::cout << p.first << ": ";
            printAny(p.second);
            std::cout << ", ";
        }
        std::cout << "}" << std::endl;
    }
} // namespace column
