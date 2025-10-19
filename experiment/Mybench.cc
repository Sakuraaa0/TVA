// Created by lwh on 25-2-24.
//

#include <chrono>
#include <fstream>
#include <gflags/gflags.h>
#include <omp.h>
#include <set>
#include <sstream>
#include <sys/resource.h>
#include <unordered_set>
#include <vector>
#include "Column/ColumbTable.hpp"
#include "CtStore.hpp"
#include "flags.hpp"
#include "hopscotch/hopscotchhash.hpp"
#include "types.hpp"

static uint64_t getCurrentTimeInMicroseconds()
{
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto microseconds = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    return static_cast<uint64_t>(microseconds);
}


int main(int argc, char** argv)
{

    // 8  - 15000
    // 16 - 18000
    // 24 - 22000
    // 32 - 26000
    // 40 - 30000
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::cout << "reserve node num of hashtable" << FLAGS_reserve_node << std::endl;

    // Establish the correspondence between num and vertex_id
    std::string num = "32"; // default

    std::ofstream result_file("/workspace/CtGraph/experiment/mybench/mybench_result_cold" + num + ".txt",
                              std::ios::out | std::ios::trunc);
    if (!result_file.is_open())
    {
        std::cerr << "cannot open file" << std::endl;
        return 1;
    }

    int end_ts = std::stoi(num) * 10000 - 1000;
    ctgraph::VertexId_t vertex_id;

    // set vertex_id by num
    if (num == "8")
    {
        vertex_id = 15000;
    }
    else if (num == "16")
    {
        vertex_id = 18000;
    }
    else if (num == "24")
    {
        vertex_id = 22000;
    }
    else if (num == "32")
    {
        vertex_id = 26000;
    }
    else if (num == "40")
    {
        vertex_id = 30000;
    }

    std::atomic<ctgraph::VertexId_t> ver = vertex_id;
    std::string file2_name = "/workspace/dataset/mybench/" + num + "/cypher.txt";
    std::ifstream file2(file2_name);
    ctgraph::CtStore ct_store(vertex_id);
    column::GraphDB db;
    std::unique_ptr<FixString> s = std::make_unique<FixString>("");
    for (int i = 0; i < vertex_id; i++)
    {
        ct_store.load_vertex(i);
    }
    std::ifstream file("/workspace/dataset/mybench/small.cypher");
    std::string line;
    while (std::getline(file, line))
    {
        if (line.find("CREATE (:User") != std::string::npos)
        {
            size_t id_pos = line.find("id: ") + 4;
            size_t id_end = line.find(",", id_pos);
            int id = std::stoi(line.substr(id_pos, id_end - id_pos));
            // completion_percentage
            size_t completion_pos = line.find("completion_percentage: ") + 22;
            size_t completion_end = line.find(",", completion_pos);
            int completion = std::stoi(line.substr(completion_pos, completion_end - completion_pos));

            size_t gender_pos = line.find("gender: \"") + 9;
            size_t gender_end = line.find("\"", gender_pos);
            std::string gender = line.substr(gender_pos, gender_end - gender_pos);
            int gender_val = (gender == "man") ? 1 : 0;

            size_t age_pos = line.find(", age: ") + 7;
            size_t age_end = line.find("}", age_pos);
            int age = std::stoi(line.substr(age_pos, age_end - age_pos));

            db.LoadTable(0, id, {{"completion_percentage", completion}, {"gender", gender_val}, {"age", age}}, {}, {});
        }
        else if (line.find("MATCH") != std::string::npos && line.find("Friend") != std::string::npos)
        {
            size_t src_pos = line.find("id: ") + 4;
            size_t src_end = line.find("}", src_pos);
            int src = std::stoi(line.substr(src_pos, src_end - src_pos));

            size_t dst_pos = line.find("id: ", src_end) + 4;
            size_t dst_end = line.find("}", dst_pos);
            int dst = std::stoi(line.substr(dst_pos, dst_end - dst_pos));

            ct_store.put_edge_withTs(src, dst, *s.get(), 0);
            ct_store.put_edge_withTs(dst, src, *s.get(), 0);
        }
    }
    file.close();

    std::string line2;
    int line_number = 0;
    auto start_time = std::chrono::high_resolution_clock::now();
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    long start_memory = usage.ru_maxrss;
    while (std::getline(file2, line2))
    {
        line_number++;
        if (line2.find("MATCH (n:User") != std::string::npos &&
            line2.find("SET n.completion_percentage") != std::string::npos)
        {
            size_t id_pos = line2.find("id:") + 3;
            size_t id_end = line2.find("}", id_pos);
            int id = std::stoi(line2.substr(id_pos, id_end - id_pos));

            size_t completion_pos = line2.find("= ") + 2;
            size_t completion_end = line2.find(";", completion_pos);
            int completion = std::stoi(line2.substr(completion_pos, completion_end - completion_pos));

            db.update(line_number, id, {{"completion_percentage", completion}}, {}, {});
        }
        else if (line2.find("CREATE (n:User") != std::string::npos &&
                 line2.find("set n.completion_percentage") != std::string::npos)
        {
            size_t id_pos = line2.find("id : ") + 5;
            size_t id_end = line2.find("}", id_pos);
            int id = std::stoi(line2.substr(id_pos, id_end - id_pos));

            size_t completion_pos = line2.find("= ") + 2;
            size_t completion_end = line2.find(" RETURN", completion_pos);
            int completion = std::stoi(line2.substr(completion_pos, completion_end - completion_pos));

            db.LoadTable(line_number, id, {{"completion_percentage", completion}, {"gender", 2}, {"age", 0}}, {}, {});
        }
        else if (line2.find("MATCH (a:User") != std::string::npos &&
                 line2.find("CREATE (a)-[:Friend]->(b)") != std::string::npos)
        {
            size_t src_pos = line2.find("id:") + 3;
            size_t src_end = line2.find("}", src_pos);
            int src = std::stoi(line2.substr(src_pos, src_end - src_pos));

            size_t dst_pos = line2.find("id: ", src_end) + 4;
            size_t dst_end = line2.find("}", dst_pos);
            int dst = std::stoi(line2.substr(dst_pos, dst_end - dst_pos));

            ct_store.put_edge_withTs(src, dst, *s.get(), line_number);
            ct_store.put_edge_withTs(dst, src, *s.get(), line_number);
        }
        else if (line2.find("MATCH (n:User") != std::string::npos && line2.find("DELETE e") != std::string::npos)
        {
            size_t src_pos = line2.find("id:") + 3;
            size_t src_end = line2.find("}", src_pos);
            int src = std::stoi(line2.substr(src_pos, src_end - src_pos));

            size_t dst_pos = line2.find("id:", src_end) + 3;
            size_t dst_end = line2.find("}", dst_pos);
            int dst = std::stoi(line2.substr(dst_pos, dst_end - dst_pos));

            ct_store.put_edge_withTs(src, dst, *s.get(), line_number);
            ct_store.put_edge_withTs(dst, src, *s.get(), line_number);
        }
    }
    file2.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    getrusage(RUSAGE_SELF, &usage);
    long end_memory = usage.ru_maxrss;

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    result_file << "exec time: " << duration.count() << " 毫秒" << std::endl;
    result_file << "memory cost: " << (end_memory - start_memory) / 1024.0 << " MB" << std::endl;

    // Q1
    int add = 1;
    result_file << "\nQ1..." << std::endl;
    auto start_q1 = std::chrono::high_resolution_clock::now();
    struct rusage usage_q1;
    getrusage(RUSAGE_SELF, &usage_q1);
    long start_memory_q1 = usage_q1.ru_maxrss;

    std::string file3_name = "/workspace/dataset/mybench/" + num + "/cypher_Q1.txt";
    std::ifstream file3(file3_name);
    std::string line3;
    while (std::getline(file3, line3))
    {
        if (line3.find("MATCH (r:User{id : ") != std::string::npos && line3.find("TT AS ") != std::string::npos)
        {
            size_t id_pos = line3.find("id : ") + 5;
            size_t id_end = line3.find("}", id_pos);
            int id = std::stoi(line3.substr(id_pos, id_end - id_pos));

            size_t ts_pos = line3.find("TT AS ") + 6;
            size_t ts_end = line3.find(" ", ts_pos);
            int timestamp = std::stoi(line3.substr(ts_pos, ts_end - ts_pos));

            std::unordered_map<std::string, std::any> vps;
            std::unordered_map<std::string, std::any> eps;
            std::unordered_map<std::string, std::any> ves;
            vps["completion_percentage"] = std::any();
            vps["age"] = std::any();
            vps["gender"] = std::any();
            db.getVersionInRange(timestamp, id + add, vps, eps, ves);
            add = add ^ 1;
        }
    }
    file3.close();
    auto end_q1 = std::chrono::high_resolution_clock::now();
    getrusage(RUSAGE_SELF, &usage_q1);
    long end_memory_q1 = usage_q1.ru_maxrss;
    auto duration_q1 = std::chrono::duration_cast<std::chrono::microseconds>(end_q1 - start_q1);
    result_file << "Q1 exec time: " << duration_q1.count() << " us" << std::endl;

    // Q2查询
    result_file << "\nQ2..." << std::endl;
    auto start_q2 = std::chrono::high_resolution_clock::now();
    struct rusage usage_q2;
    getrusage(RUSAGE_SELF, &usage_q2);
    long start_memory_q2 = usage_q2.ru_maxrss;

    std::string fileq2_name = "/workspace/dataset/mybench/" + num + "/cypher_Q2.txt";
    std::ifstream fileq2(fileq2_name);
    std::string lineq2;
    while (std::getline(fileq2, lineq2))
    {
        if (lineq2.find("MATCH (r:User{id : ") != std::string::npos && lineq2.find("TT FROM ") != std::string::npos)
        {
            size_t id_pos = lineq2.find("id : ") + 5;
            size_t id_end = lineq2.find("}", id_pos);
            int id = std::stoi(lineq2.substr(id_pos, id_end - id_pos));
            size_t from_pos = lineq2.find("TT FROM ") + 8;
            size_t from_end = lineq2.find(" TO ", from_pos);
            int from_ts = std::stoi(lineq2.substr(from_pos, from_end - from_pos));
            size_t to_pos = lineq2.find(" TO ") + 4;
            size_t to_end = lineq2.find(" ", to_pos);
            int to_ts = std::stoi(lineq2.substr(to_pos, to_end - to_pos));
            std::unordered_map<std::string, std::any> vps;
            std::unordered_map<std::string, std::any> eps;
            std::unordered_map<std::string, std::any> ves;
            vps["completion_percentage"] = std::any();
            vps["age"] = std::any();
            vps["gender"] = std::any();

            db.getVersionInRange(from_ts, id + add, vps, eps, ves);

            db.getVersionInRange(to_ts, id + add, vps, eps, ves);
            add = add ^ 1;
        }
    }
    fileq2.close();
    auto end_q2 = std::chrono::high_resolution_clock::now();
    getrusage(RUSAGE_SELF, &usage_q2);
    long end_memory_q2 = usage_q2.ru_maxrss;
    auto duration_q2 = std::chrono::duration_cast<std::chrono::microseconds>(end_q2 - start_q2);
    result_file << "Q2exec time: " << duration_q2.count() << " us" << std::endl;

    // Q3查询
    result_file << "\nQ3..." << std::endl;
    auto start_q3 = std::chrono::high_resolution_clock::now();
    struct rusage usage_q3;
    getrusage(RUSAGE_SELF, &usage_q3);
    long start_memory_q3 = usage_q3.ru_maxrss;
    std::string file4_name = "/workspace/dataset/mybench/" + num + "/cypher_Q3.txt";
    std::ifstream file4(file4_name);
    std::string line4;
    while (std::getline(file4, line4))
    {
        if (line4.find("MATCH (n:User{id : ") != std::string::npos && line4.find("TT AS ") != std::string::npos)
        {
            size_t id_pos = line4.find("id : ") + 5;
            size_t id_end = line4.find("}", id_pos);
            int id = std::stoi(line4.substr(id_pos, id_end - id_pos));
            size_t ts_pos = line4.find("TT AS ") + 6;
            size_t ts_end = line4.find(" ", ts_pos);
            uint64_t timestamp = std::stoull(line4.substr(ts_pos, ts_end - ts_pos));
            std::vector<ctgraph::Edge> edges;
            ct_store.get_edges_withTs(1870, edges, timestamp);
            std::unordered_map<std::string, std::any> vps;
            std::unordered_map<std::string, std::any> eps;
            std::unordered_map<std::string, std::any> ves;
            vps["completion_percentage"] = std::any();
            vps["age"] = std::any();
            vps["gender"] = std::any();
            db.getVersionInRange(timestamp, 1870, vps, eps, ves);
            for (auto edge : edges)
            {
                db.getVersionInRange(timestamp, edge.destination(), vps, eps, ves);
            }
        }
    }
    file4.close();
    auto end_q3 = std::chrono::high_resolution_clock::now();
    getrusage(RUSAGE_SELF, &usage_q3);
    long end_memory_q3 = usage_q3.ru_maxrss;
    auto duration_q3 = std::chrono::duration_cast<std::chrono::microseconds>(end_q3 - start_q3);
    result_file << "Q3exec time: " << duration_q3.count() << " us" << std::endl;

    // Q4查询
    result_file << "\nQ4..." << std::endl;
    auto start_q4 = std::chrono::high_resolution_clock::now();
    struct rusage usage_q4;
    getrusage(RUSAGE_SELF, &usage_q4);
    long start_memory_q4 = usage_q4.ru_maxrss;
    std::string fileq4_name = "/workspace/dataset/mybench/" + num + "/cypher_Q4.txt";
    std::ifstream fileq4(fileq4_name);
    std::string lineq4;
    while (std::getline(fileq4, lineq4))
    {
        if (lineq4.find("MATCH (n:User{id : ") != std::string::npos && lineq4.find("TT FROM ") != std::string::npos)
        {
            size_t id_pos = lineq4.find("id : ") + 5;
            size_t id_end = lineq4.find("}", id_pos);
            int id = std::stoi(lineq4.substr(id_pos, id_end - id_pos));
            size_t from_pos = lineq4.find("TT FROM ") + 8;
            size_t from_end = lineq4.find(" TO ", from_pos);
            int from_ts = std::stoi(lineq4.substr(from_pos, from_end - from_pos));
            size_t to_pos = lineq4.find(" TO ") + 4;
            size_t to_end = lineq4.find(" ", to_pos);
            int to_ts = std::stoi(lineq4.substr(to_pos, to_end - to_pos));
            std::vector<ctgraph::Edge> edges;
            std::unordered_map<std::string, std::any> vps;
            std::unordered_map<std::string, std::any> eps;
            std::unordered_map<std::string, std::any> ves;
            vps["completion_percentage"] = std::any();
            vps["age"] = std::any();
            vps["gender"] = std::any();
            ct_store.get_edges_withTs(id, edges, from_ts);
            db.getVersionInRange(from_ts, id, vps, eps, ves);
            db.getVersionInRange(to_ts, id, vps, eps, ves);
            for (auto edge : edges)
            {
                db.getVersionInRange(from_ts, edge.destination(), vps, eps, ves);
            }
            edges.clear();
            ct_store.get_edges_withTs(id, edges, to_ts);
            for (auto edge : edges)
            {
                db.getVersionInRange(to_ts, edge.destination(), vps, eps, ves);
            }
        }
    }
    fileq4.close();
    auto end_q4 = std::chrono::high_resolution_clock::now();
    getrusage(RUSAGE_SELF, &usage_q4);
    long end_memory_q4 = usage_q4.ru_maxrss;
    auto duration_q4 = std::chrono::duration_cast<std::chrono::microseconds>(end_q4 - start_q4);
    result_file << "Q4exec time: " << duration_q4.count() << " us" << std::endl;

    auto total_duration = duration_q1 + duration_q2 + duration_q3 + duration_q4;
    long total_memory = (end_memory_q4 - start_memory_q1) / 1024.0;
    result_file << "\nexec time: " << total_duration.count() << " us" << std::endl;
    result_file.close();

    return 0;
}
