#include <fstream>
#include <gflags/gflags.h>
#include <iostream>
#include <omp.h>
#include <random>
#include <set>
#include <sstream>
#include <sys/resource.h>
#include <unordered_set>
#include <vector>
#include "CtStore.hpp"
#include "flags.hpp"
#include "hopscotch/hopscotchhash.hpp"
#include "types.hpp"
#include "unordered_set"


namespace std
{
    template <>
    struct hash<std::pair<ctgraph::VertexId_t, ctgraph::VertexId_t>>
    {
        std::size_t operator()(const std::pair<ctgraph::VertexId_t, ctgraph::VertexId_t>& p) const
        {
            return std::hash<ctgraph::VertexId_t>()(p.first) ^ (std::hash<ctgraph::VertexId_t>()(p.second) << 1);
        }
    };
} // namespace std


int main(int argc, char** argv)
{
    // 896310 ca-IMDB-sf.el
    // 425960  com-dblp-sf.ungraph.el
    // 1157830  com-youtube-sf.ungraph.el
    // 75890 soc-Epinions1-sf.el
    ctgraph::VertexId_t vertex_id = 896310;
    std::atomic ver = vertex_id;
    ctgraph::CtStore ct_store(ver);
    std::vector<std::pair<ctgraph::VertexId_t, ctgraph::VertexId_t>> edges;
    // hophash::HopscotchHashTable hashTable(200000, 16);
    std::ifstream file("/workspace/dataset/shuffle/ca-IMDB-sf.el");
    std::string line;
    while (std::getline(file, line))
    {
        std::istringstream iss(line);
        int src, dst;
        iss >> src >> dst;
        edges.push_back(std::make_pair(src, dst));
    }
    file.close();
    std::random_device rd;
    std::mt19937 g(rd());

    // Calculate the average degree and the maximum degree
    std::unordered_map<ctgraph::VertexId_t, int> degree_count;
    for (const auto& edge : edges)
    {
        degree_count[edge.first]++;
        degree_count[edge.second]++;
    }
    std::vector<int> degrees;
    for (const auto& pair : degree_count)
    {
        degrees.push_back(pair.second);
    }
    std::sort(degrees.begin(), degrees.end());

    double median_degree;
    size_t n = degrees.size();
    if (n % 2 == 1)
    {
        median_degree = degrees[n / 2];
    }
    else
    {
        median_degree = (degrees[n / 2 - 1] + degrees[n / 2]) / 2.0;
    }
    int total_degree = 0;
    int max_degree = 0;
    for (const auto& pair : degree_count)
    {
        total_degree += pair.second;
        max_degree = std::max(max_degree, pair.second);
    }
    double average_degree = static_cast<double>(total_degree) / degree_count.size();
    std::cout << "All Edges num: " << edges.size() << std::endl;
    std::cout << "Average Degree: " << average_degree << std::endl;
    std::cout << "Max Degree: " << max_degree << std::endl;
    std::cout << "Median Degree: " << median_degree << std::endl;
    std::cout << "90% Degree: " << degrees[static_cast<int>(0.99 * degrees.size())] << std::endl;
    struct rusage usage_before;
    getrusage(RUSAGE_SELF, &usage_before);
    int cc = 0;
    for (auto edge : edges)
    {
        ct_store.load_vertex(edge.first);
        cc++;
    }


    std::unique_ptr<FixString> s = std::make_unique<FixString>("");
    auto start_time = std::chrono::high_resolution_clock::now();

    for (auto edge : edges)
    {
        ct_store.put_edge(edge.first, edge.second, *s.get());
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end_time - start_time;
    std::cout << "Time taken for ct_store.put_edge: " << duration.count() << " seconds" << std::endl;

    struct rusage usage_after;
    getrusage(RUSAGE_SELF, &usage_after);
    long memory_used = (usage_after.ru_maxrss - usage_before.ru_maxrss);
    std::cout << "Memory usage: " << memory_used << " KB" << std::endl;
}
