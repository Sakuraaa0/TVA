#include <fstream>
#include <gflags/gflags.h>
#include <iostream>
#include <omp.h>
#include <queue>
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
    ctgraph::VertexId_t vertex_id = 425960;
    std::atomic ver = vertex_id;
    ctgraph::CtStore ct_store(ver);
    std::vector<std::pair<ctgraph::VertexId_t, ctgraph::VertexId_t>> edges;
    // hophash::HopscotchHashTable hashTable(200000, 16);
    // where each line contains a "1 2" that signifies an edge between vertex 1 and vertex 2.
    std::ifstream file("/workspace/dataset/shuffle/com-dblp-sf.ungraph.el");
    std::string line;
    while (std::getline(file, line))
    {
        std::istringstream iss(line);
        int src, dst;
        iss >> src >> dst;
        edges.push_back(std::make_pair(src, dst));
    }
    file.close();
    // Insert all edges from the edges set into the hashTable, but in a random order
    std::random_device rd;
    std::mt19937 g(rd());


    // Calculate the average degree and maximum degree
    // Use unordered_map to count the degree of each node
    std::unordered_map<ctgraph::VertexId_t, int> degree_count;
    for (const auto& edge : edges)
    {
        degree_count[edge.first]++;
        degree_count[edge.second]++;
    }

    // Store the degrees in a vector
    std::vector<int> degrees;
    for (const auto& pair : degree_count)
    {
        degrees.push_back(pair.second);
    }
    std::sort(degrees.begin(), degrees.end());

    // Calculating the median
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
    // Calculate the total degree and the maximum degree
    int total_degree = 0;
    int max_degree = 0;
    for (const auto& pair : degree_count)
    {
        total_degree += pair.second;
        max_degree = std::max(max_degree, pair.second);
    }
    // Calculating the average degree
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

    std::cout << "begin bfs:" << std::endl;
    //--------------------------------------------------bfs-----------------------------------------------------------------
    std::queue<uint64_t> bfs_queue;
    std::unordered_set<uint64_t> visited;
    uint64_t start_vertex = 1;
    struct rusage bfs_usage_before;
    getrusage(RUSAGE_SELF, &bfs_usage_before);
    auto bfs_start_time = std::chrono::high_resolution_clock::now();
    bfs_queue.push(start_vertex);
    visited.insert(start_vertex);
    int level = 0;
    while (!bfs_queue.empty())
    {
        size_t level_size = bfs_queue.size();
        for (size_t i = 0; i < level_size; ++i)
        {
            uint64_t current = bfs_queue.front();
            bfs_queue.pop();
            // std::vector<uint64_t> neighbors;
            std::vector<ctgraph::Edge> neighbors;
            ct_store.get_edges(current, neighbors);
            // ct_store.getNeighbors(current, neighbors);
            for (auto& neighbor : neighbors)
            {
                if (visited.find(neighbor.destination()) == visited.end())
                {
                    visited.insert(neighbor.destination());
                    bfs_queue.push(neighbor.destination());
                }
            }
        }
        level++;
    }
    auto bfs_end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> bfs_duration = bfs_end_time - bfs_start_time;
    struct rusage bfs_usage_after;
    getrusage(RUSAGE_SELF, &bfs_usage_after);
    long bfs_memory_used = (bfs_usage_after.ru_maxrss - bfs_usage_before.ru_maxrss);
    std::cout << "BFS starting from vertex " << start_vertex << ":" << std::endl;
    std::cout << "Total levels: " << level << std::endl;
    std::cout << "Total vertices visited: " << visited.size() << std::endl;
    std::cout << "BFS execution time: " << bfs_duration.count() << " seconds" << std::endl;
    std::cout << "BFS memory usage: " << bfs_memory_used << " KB" << std::endl;


    //-------------------------------------------------sssp-----------------------------------------------------------------
    std::cout << "begin sssp:" << std::endl;
    struct rusage sssp_usage_before;
    getrusage(RUSAGE_SELF, &sssp_usage_before);
    auto sssp_start_time = std::chrono::high_resolution_clock::now();
    std::vector<uint64_t> distances(vertex_id + 1, UINT64_MAX);
    std::vector<bool> sssp_visited(vertex_id + 1, false);
    distances[start_vertex] = 0;
    using Vertex = std::pair<uint64_t, uint64_t>;
    std::priority_queue<Vertex, std::vector<Vertex>, std::greater<Vertex>> pq;
    pq.push({0, start_vertex});
    while (!pq.empty())
    {
        uint64_t current_dist = pq.top().first;
        uint64_t current_vertex = pq.top().second;
        pq.pop();
        if (sssp_visited[current_vertex])
        {
            continue;
        }
        sssp_visited[current_vertex] = true;
        std::vector<ctgraph::Edge> neighbors;
        ct_store.get_edges(current_vertex, neighbors);
        for (const auto& edge : neighbors)
        {
            uint64_t neighbor = edge.destination();
            uint64_t weight = 1;
            if (current_dist + weight < distances[neighbor])
            {
                distances[neighbor] = current_dist + weight;
                pq.push({distances[neighbor], neighbor});
            }
        }
    }
    auto sssp_end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> sssp_duration = sssp_end_time - sssp_start_time;
    struct rusage sssp_usage_after;
    getrusage(RUSAGE_SELF, &sssp_usage_after);
    long sssp_memory_used = (sssp_usage_after.ru_maxrss - sssp_usage_before.ru_maxrss);
    uint64_t reachable_vertices = 0;
    uint64_t max_distance = 0;
    for (uint64_t i = 1; i <= vertex_id; ++i)
    {
        if (distances[i] != UINT64_MAX)
        {
            reachable_vertices++;
            max_distance = std::max(max_distance, distances[i]);
        }
    }
    std::cout << "SSSP starting from vertex " << start_vertex << ":" << std::endl;
    std::cout << "Reachable vertices: " << reachable_vertices << std::endl;
    std::cout << "Maximum distance: " << max_distance << std::endl;
    std::cout << "SSSP execution time: " << sssp_duration.count() << " seconds" << std::endl;
    std::cout << "SSSP memory usage: " << sssp_memory_used << " KB" << std::endl;

    //------------------------------------------wcc-----------------------------------------------------------------
    std::cout << "begin wcc:" << std::endl;
    struct rusage wcc_usage_before;
    getrusage(RUSAGE_SELF, &wcc_usage_before);
    auto wcc_start_time = std::chrono::high_resolution_clock::now();
    std::vector<uint64_t> parent(vertex_id + 1);
    std::vector<uint64_t> rank(vertex_id + 1, 0);
    for (uint64_t i = 1; i <= vertex_id; ++i)
    {
        parent[i] = i;
    }
    std::function<uint64_t(uint64_t)> find = [&parent, &find](uint64_t x) -> uint64_t
    {
        if (parent[x] != x)
        {
            parent[x] = find(parent[x]);
        }
        return parent[x];
    };
    auto unite = [&parent, &rank, &find](uint64_t x, uint64_t y)
    {
        uint64_t root_x = find(x);
        uint64_t root_y = find(y);

        if (root_x != root_y)
        {
            if (rank[root_x] < rank[root_y])
            {
                std::swap(root_x, root_y);
            }
            parent[root_y] = root_x;
            if (rank[root_x] == rank[root_y])
            {
                rank[root_x]++;
            }
        }
    };
    for (const auto& edge : edges)
    {
        unite(edge.first, edge.second);
    }
    std::unordered_map<uint64_t, uint64_t> component_sizes;
    for (uint64_t i = 1; i <= vertex_id; ++i)
    {
        uint64_t root = find(i);
        component_sizes[root]++;
    }
    auto wcc_end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> wcc_duration = wcc_end_time - wcc_start_time;
    struct rusage wcc_usage_after;
    getrusage(RUSAGE_SELF, &wcc_usage_after);
    long wcc_memory_used = (wcc_usage_after.ru_maxrss - wcc_usage_before.ru_maxrss);
    uint64_t max_component_size = 0;
    for (const auto& pair : component_sizes)
    {
        max_component_size = std::max(max_component_size, pair.second);
    }
    std::cout << "WCC results:" << std::endl;
    std::cout << "Number of components: " << component_sizes.size() << std::endl;
    std::cout << "Size of largest component: " << max_component_size << std::endl;
    std::cout << "WCC execution time: " << wcc_duration.count() << " seconds" << std::endl;
    std::cout << "WCC memory usage: " << wcc_memory_used << " KB" << std::endl;

    //------------------------------------------pr-----------------------------------------------------------------
    std::cout << "begin pr:" << std::endl;
    struct rusage pr_usage_before;
    getrusage(RUSAGE_SELF, &pr_usage_before);
    auto pr_start_time = std::chrono::high_resolution_clock::now();
    const double damping_factor = 0.85;
    const int num_iterations = 20;
    std::vector<double> pr_values(vertex_id + 1, 1.0 / vertex_id);
    std::vector<double> new_pr_values(vertex_id + 1, 0.0);
    std::vector<uint64_t> out_degrees(vertex_id + 1, 0);
    for (const auto& edge : edges)
    {
        out_degrees[edge.first]++;
    }
    for (int iter = 0; iter < num_iterations; ++iter)
    {
        std::fill(new_pr_values.begin(), new_pr_values.end(), 0.0);
#pragma omp parallel for schedule(dynamic)
        for (uint64_t i = 1; i <= vertex_id; ++i)
        {
            std::vector<ctgraph::Edge> neighbors;
            ct_store.get_edges(i, neighbors);
            double contribution = pr_values[i] / out_degrees[i];
            for (const auto& edge : neighbors)
            {
                uint64_t neighbor = edge.destination();
#pragma omp atomic
                new_pr_values[neighbor] += contribution;
            }
        }
        double sum = 0.0;
#pragma omp parallel for reduction(+ : sum)
        for (uint64_t i = 1; i <= vertex_id; ++i)
        {
            new_pr_values[i] = (1.0 - damping_factor) / vertex_id + damping_factor * new_pr_values[i];
            sum += new_pr_values[i];
        }
        double scale = 1.0 / sum;
#pragma omp parallel for
        for (uint64_t i = 1; i <= vertex_id; ++i)
        {
            new_pr_values[i] *= scale;
        }
        std::swap(pr_values, new_pr_values);
    }

    auto pr_end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> pr_duration = pr_end_time - pr_start_time;
    struct rusage pr_usage_after;
    getrusage(RUSAGE_SELF, &pr_usage_after);
    long pr_memory_used = (pr_usage_after.ru_maxrss - pr_usage_before.ru_maxrss);
    double max_pr = 0.0;
    double min_pr = 1.0;
    uint64_t max_pr_vertex = 0;
    uint64_t min_pr_vertex = 0;

    for (uint64_t i = 1; i <= vertex_id; ++i)
    {
        if (pr_values[i] > max_pr)
        {
            max_pr = pr_values[i];
            max_pr_vertex = i;
        }
        if (pr_values[i] < min_pr)
        {
            min_pr = pr_values[i];
            min_pr_vertex = i;
        }
    }
    std::cout << "PageRank results:" << std::endl;
    std::cout << "Damping factor: " << damping_factor << std::endl;
    std::cout << "Number of iterations: " << num_iterations << std::endl;
    std::cout << "Vertex with highest PR: " << max_pr_vertex << " (PR = " << max_pr << ")" << std::endl;
    std::cout << "Vertex with lowest PR: " << min_pr_vertex << " (PR = " << min_pr << ")" << std::endl;
    std::cout << "PR execution time: " << pr_duration.count() << " seconds" << std::endl;
    std::cout << "PR memory usage: " << pr_memory_used << " KB" << std::endl;

    return 0;
}
