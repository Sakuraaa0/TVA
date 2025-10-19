// Created by lwh on 25-2-24.
//

#include "Column/ColumbTable.hpp"
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
#include "Column/ColumnTypes.hpp"
#include "Column/TemporalData.hpp"
#include "VersionChain.hpp"
#include "flags.hpp"
#include "types.hpp"

static uint64_t getCurrentTimeInMicroseconds()
{
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    return static_cast<uint64_t>(microseconds);
}

/*
 Supported operations include:
 - Inserting edges with timestamps
 - Retrieving the latest edge
 - Retrieving the edge at a specified timestamp
 - Retrieving all edges within a specified time range
 */


void get(int version_num)
{
    std::cout << "version_num:" << version_num << std::endl;
    column::GraphDB db;
    std::vector<int> random_values;
    std::vector<ctgraph::SequenceNumber_t> random_values_seq;
    int count_value = 1000;
    int repeat_query = 10;
    int count_all = version_num * count_value;
    for (int i = 0; i < count_all; i++)
    {
        int value = i % count_value;
        random_values.push_back(value);
        random_values_seq.push_back(i / count_value + 1);
    }
    for (int i = 0; i < count_value; i++)
    {
        db.LoadTable(0, i, {{"name", 0}}, {}, {});
    }
    for (int i = 0; i < random_values.size(); i++)
    {
        db.update(random_values_seq[i], random_values[i], {{"name", random_values_seq[i] + random_values[i]}}, {}, {});
    }

    std::unordered_map<int, ctgraph::VersionChain> version_chains;
    for (int i = 0; i < random_values_seq.size(); i++)
    {
        version_chains[random_values_seq[i]] = ctgraph::VersionChain();
    }
    for (int i = 0; i < random_values_seq.size(); i++)
    {
        version_chains[random_values[i]].insert(random_values_seq[i], random_values[i],
                                                std::make_shared<FixString>("name"));
    }

    auto start_time_insert = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < repeat_query; i++)
    {
        std::vector<std::string> vps;
        std::vector<std::string> eps;
        std::vector<std::string> ves;
        std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>> vp_res;
        std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>> ep_res;
        std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>> ve_res;
        vps.push_back("name");
        db.getAllInRange((count_all - 1) / count_value + 1, vps, eps, ves, vp_res, ep_res, ve_res);
        int size = vp_res["name"].size();
    }
    auto end_time_insert = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration_insert = end_time_insert - start_time_insert;
    std::cout << "Time taken for temporal table chain query: " << duration_insert.count() << " seconds" << std::endl;

    start_time_insert = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < repeat_query; i++)
    {
        std::unordered_map<std::string, std::any> vps;
        std::unordered_map<std::string, std::any> eps;
        std::unordered_map<std::string, std::any> ves;
        std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>> vp_res;
        std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>> ep_res;
        std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>> ve_res;
        vps["name"] = std::any();
        std::unordered_map<std::string, std::vector<std::pair<size_t, std::any>>> res2;
        for (int j = 0; j < count_value; j++)
        {
            // db.getVersionInRange((count_all - 1) / count_value + 1,
            // random_values[j],
            //                      vps, eps, ves);
            //    vp_res["name"].push_back(std::make_pair(random_values[j],
            //    vps["name"]));
            std::pair<std::string, std::any> res = std::make_pair("name", std::any());
            db.getDatafromThisBegin((count_all - 1) / count_value + 1, random_values[j], 0, column::ValueType::VP, res);
            res2["name"].push_back(std::make_pair(j, res.second));
            // auto version = version_chains[random_values[j]].get_version(1000);
        }
        int a = 0;
    }
    end_time_insert = std::chrono::high_resolution_clock::now();
    duration_insert = end_time_insert - start_time_insert;
    std::cout << "Time taken for version chain query: " << duration_insert.count() << " seconds" << std::endl;

    std::cout << "query number: " << repeat_query * count_value << std::endl;
}

int main(int argc, char** argv)
{
    for (int i = 1; i < 21; i++)
    {
        get(i * 10);
    }
    // for (int i = 0; i < 5; i++) {
    //   get(80);
    // }
}
