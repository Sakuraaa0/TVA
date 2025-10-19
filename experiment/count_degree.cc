#include <algorithm>
#include <climits>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>
// #include <execution>

/**
 *
 * Statistical analysis of the out-degree distribution of vertices in the dataset
 *
 */

using vertex_t = int64_t;

// Definition of a Structure for Degree Intervals
struct DegreeInterval
{
    vertex_t start;
    vertex_t end;
};

// Statistical Frequency Distribution
void countDegreeDistribution(vertex_t* data, size_t edge_num, DegreeInterval* intervals, int numIntervals)
{
    vertex_t max_v_id = 0;
    std::vector<int> degree_analysis(numIntervals, 0);

    // Traverse the data and calculate the maximum vertex ID.
    for (size_t i = 0; i < edge_num * 2; i += 2)
    {
        vertex_t src = data[i];
        vertex_t dst = data[i + 1];
        max_v_id = std::max(src, max_v_id);
        max_v_id = std::max(dst, max_v_id);
    }
    std::cout << " vertex_num=" << max_v_id << std::endl;

    std::vector<vertex_t> degrees(max_v_id + 1, 0);

    for (size_t i = 0; i < edge_num * 2; i += 2)
    {
        vertex_t src = data[i];
        degrees[src]++;
    }

    vertex_t max_degree = 0;
    vertex_t sum_degree = 0;
    vertex_t degree_great_0 = 0;
    size_t greater_10000 = 0;
    size_t greater_100000 = 0;
    size_t greater_1000000 = 0;
    for (auto d : degrees)
    {
        max_degree = std::max(d, max_degree);
        sum_degree += d;
        if (d > 0)
        {
            degree_great_0++;
        }

        if (d > 1000000)
        {
            greater_1000000++;
        }
        else if (d > 100000)
        {
            greater_100000++;
        }
        else if (d > 10000)
        {
            greater_10000++;
        }

        // Determining the interval to which the degree belongs.
        bool find = 0;
        for (int j = 0; j < numIntervals; j++)
        {
            if (d >= intervals[j].start && d <= intervals[j].end)
            {
                degree_analysis[j] += 1;
                find = 1;
                break;
            }
        }
    }
    std::cout << " max_degree=" << max_degree << std::endl;
    std::cout << " greater_10000=" << greater_10000 << std::endl;
    std::cout << " greater_100000=" << greater_100000 << std::endl;
    std::cout << " greater_1000000=" << greater_1000000 << std::endl;

    // Output statistical results.
    vertex_t v_sum = 0;
    for (int i = 0; i < numIntervals; i++)
    {
        v_sum += degree_analysis[i];
        std::cout << "[" << intervals[i].start << ", " << intervals[i].end << "]:\t" << degree_analysis[i]
                  << " \trate=\t" << (degree_analysis[i] * 1.0 / max_v_id) << " \t>0 rate=\t"
                  << (degree_analysis[i] * 1.0 / degree_great_0) << std::endl;
    }
    std::cout << " v_num: " << v_sum << std::endl;
    std::cout << " sum_degree: " << sum_degree << std::endl;
    std::cout << " v_num/max_v_id=: " << v_sum * 1.0 / max_v_id << std::endl;
    std::cout << " sum_degree/max_v_id=: " << sum_degree * 1.0 / max_v_id << std::endl;
    std::cout << " degree_great_0: " << degree_great_0 << std::endl;

    std::cout << " --- static: " << std::endl;
    // std::sort(std::execution::par, degrees.begin(), degrees.end(), std::greater<int>());
    std::sort(degrees.begin(), degrees.end(), std::greater<int>());

    // The degrees of the top proportion with the highest statistical frequency.
    std::vector<float> p_rate{0,   0.00001, 0.0001, 0.001, 0.01, 0.05, 0.1,  0.2,  0.3,
                              0.4, 0.5,     0.6,    0.7,   0.8,  0.9,  0.95, 0.99, 1};
    for (auto p : p_rate)
    {
        int index = p * degrees.size();
        if (index >= degrees.size())
        {
            index = degrees.size() - 1;
        }
        std::cout << "      p=" << p << " index=" << index << " value=" << degrees[index] << std::endl;
    }

    // The number of statistical degrees exceeding 1,000,000:
    int count = std::count_if(degrees.begin(), degrees.end(), [](int num) { return num > 400000; });
    std::cout << "  degree > 400000: " << count << std::endl;
}

// cmd: g++ count_degree.cc && ./a.out /data_old/dataset/google/google.e.b.random.b
int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        std::cerr << "Usage: " << argv[0] << " <file_path>" << std::endl;
        return 1;
    }

    const char* filename = argv[1];

    // Use the system function 'open' to access the file.
    int fd = open(filename, O_RDONLY);
    if (fd == -1)
    {
        std::cerr << "Failed to open file." << std::endl;
        return 1;
    }
    off_t size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    size_t edge_num = size / (2 * sizeof(vertex_t));
    std::cout << " edge_num=" << edge_num << std::endl;

    vertex_t* data = static_cast<vertex_t*>(mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED)
    {
        std::cerr << "Failed to create memory mapping." << std::endl;
        return 1;
    }

    // Definition of Degree Interval
    DegreeInterval intervals[] = {{0, 0},   {1, 2},    {3, 4},     {5, 8},     {9, 16},     {17, 32},
                                  {33, 64}, {65, 128}, {129, 256}, {257, 512}, {513, 1024}, {1024, INT_MAX}};
    int numIntervals = sizeof(intervals) / sizeof(DegreeInterval);
    // Statistical Frequency Distribution
    countDegreeDistribution(data, edge_num, intervals, numIntervals);
    munmap(data, size);
    close(fd);
    return 0;
}
