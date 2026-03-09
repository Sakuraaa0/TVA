#include "CtStore.hpp"
#include "VersionChain.hpp"
#include "flags.hpp"
#include "hopscotch/hopscotchhash.hpp"
#include "types.hpp"
#include <filesystem>
#include <fstream>
#include <gflags/gflags.h>
#include <iostream>
#include <omp.h>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <iomanip>
#include <map>

#include <cmath>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

static uint64_t getCurrentTimeInMicroseconds() {
  // 获取当前时间点
  auto now = std::chrono::system_clock::now();
  // 转换为自1970-01-01 00:00:00以来的时间
  auto duration = now.time_since_epoch();
  // 转换为微秒
  auto microseconds =
      std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
  // 返回uint64_t类型的微秒数
  return static_cast<uint64_t>(microseconds);
}

class AccurateZipfGenerator {
private:
  std::mt19937 gen;
  std::uniform_real_distribution<double> uniform_dist;
  int N;        // 元素总数
  double alpha; // 形状参数
  std::vector<double> probabilities;
  std::vector<double> cumulative_probs;

public:
  AccurateZipfGenerator(int N, double alpha,
                        unsigned seed = std::random_device{}())
      : gen(seed), uniform_dist(0.0, 1.0), N(N), alpha(alpha) {
    calculateExactProbabilities();
  }
  void calculateExactProbabilities() {
    probabilities.resize(N + 1);
    cumulative_probs.resize(N + 1);

    // 计算广义调和数 H(N, α)
    double harmonic_sum = 0.0;
    for (int k = 1; k <= N; k++) {
      harmonic_sum += 1.0 / std::pow(k, alpha);
    }

    // 计算精确的概率分布
    double cumulative = 0.0;
    for (int k = 1; k <= N; k++) {
      // P(X = k) = k^(-α) / H(N,α)
      probabilities[k] = (1.0 / std::pow(k, alpha)) / harmonic_sum;
      cumulative += probabilities[k];
      cumulative_probs[k] = cumulative;
    }

    // // 输出分布信息用于验证
    // std::cout << "Zipf Distribution Parameters:\n";
    // std::cout << "N = " << N << ", α = " << alpha << "\n";
    // std::cout << "H(N,α) = " << harmonic_sum << "\n\n";

    // std::cout << "Top 10 probabilities:\n";
    // std::cout << "k\tP(X=k)\t\tCumulative\n";
    // std::cout << "---\t------\t\t----------\n";
    // for (int k = 1; k <= std::min(10, N); k++) {
    //   std::cout << k << "\t" << std::fixed << std::setprecision(6)
    //             << probabilities[k] << "\t" << cumulative_probs[k] << "\n";
    // }
    // std::cout << "\n";
  }
  int generate() {
    double u = uniform_dist(gen);

    // 使用累积分布函数的逆变换方法
    for (int k = 1; k <= N; k++) {
      if (u <= cumulative_probs[k]) {
        return k;
      }
    }
    return N; // 理论上不应该到达这里
  }
  std::vector<int> generate(int count) {
    std::vector<int> result;
    result.reserve(count);
    for (int i = 0; i < count; i++) {
      result.push_back(generate());
    }
    return result;
  }

  // 获取理论概率（用于验证）
  double getTheoreticalProbability(int k) const {
    if (k < 1 || k > N)
      return 0.0;
    return probabilities[k];
  }

  // 获取参数
  int getN() const { return N; }
  double getAlpha() const { return alpha; }
};

void validateZipfDistribution(AccurateZipfGenerator &generator,
                              int sample_size) {
  auto samples = generator.generate(sample_size);

  // 统计频率
  std::map<int, int> frequency;
  for (int val : samples) {
    frequency[val]++;
  }

  std::cout << "=== Distribution Validation ===\n";
  std::cout << "Sample size: " << sample_size << "\n";
  std::cout << "Parameters: N=" << generator.getN()
            << ", α=" << generator.getAlpha() << "\n\n";

  std::cout << "k\tObserved\tExpected\tObs_Prob\tExp_Prob\tError\n";
  std::cout << "---\t--------\t--------\t--------\t--------\t-----\n";

  double total_error = 0.0;
  int display_count = std::min(15, generator.getN());

  for (int k = 1; k <= display_count; k++) {
    int observed = frequency[k];
    double obs_prob = static_cast<double>(observed) / sample_size;
    double exp_prob = generator.getTheoreticalProbability(k);
    double expected = exp_prob * sample_size;
    double error = std::abs(obs_prob - exp_prob);
    total_error += error;

    std::cout << k << "\t" << observed << "\t\t" << std::fixed
              << std::setprecision(1) << expected << "\t\t"
              << std::setprecision(6) << obs_prob << "\t" << exp_prob << "\t"
              << std::setprecision(4) << error << "\n";
  }

  std::cout << "\nTotal absolute error: " << std::setprecision(6) << total_error
            << "\n";
  std::cout << "Average error per element: " << total_error / display_count
            << "\n\n";
}

void demonstrateDifferentParameters() {
  std::cout << "=== Different Zipf Distribution Parameters ===\n\n";

  struct TestCase {
    int N;
    double alpha;
    std::string description;
  };

  std::vector<TestCase> test_cases = {
      {100, 0.5, "Mild skew (α=0.5)"},
      {100, 1.0, "Classic Zipf (α=1.0)"},
      {100, 1.5, "Strong skew (α=1.5)"},
      {100, 2.0, "Very strong skew (α=2.0)"},
      {1000, 1.2, "Large N with moderate skew"},
  };

  for (const auto &test : test_cases) {
    std::cout << "--- " << test.description << " ---\n";
    AccurateZipfGenerator generator(test.N, test.alpha);
    validateZipfDistribution(generator, 50000);
    std::cout << "\n";
  }
}

void verifyProbabilitySum(int N, double alpha) {
  std::cout << "=== Probability Sum Verification ===\n";
  std::cout << "N=" << N << ", α=" << alpha << "\n";

  // 计算广义调和数
  double harmonic_sum = 0.0;
  for (int k = 1; k <= N; k++) {
    harmonic_sum += 1.0 / std::pow(k, alpha);
  }

  // 验证概率和
  double prob_sum = 0.0;
  for (int k = 1; k <= N; k++) {
    double prob = (1.0 / std::pow(k, alpha)) / harmonic_sum;
    prob_sum += prob;
  }

  std::cout << "Harmonic sum H(" << N << "," << alpha
            << ") = " << std::setprecision(10) << harmonic_sum << "\n";
  std::cout << "Sum of all probabilities = " << prob_sum << "\n";
  std::cout << "Error from 1.0 = " << std::abs(1.0 - prob_sum) << "\n\n";
}

// int main() {
//   // 验证概率和
//   verifyProbabilitySum(100, 1.0);
//   verifyProbabilitySum(1000, 1.5);

//   // 测试标准Zipf分布
//   std::cout << "=== Standard Zipf Distribution Test ===\n";
//   AccurateZipfGenerator zipf(100, 1.0);
//   validateZipfDistribution(zipf, 100000);

//   // 测试不同参数
//   demonstrateDifferentParameters();

//   // 经典的80-20规则验证（α≈1.16时）
//   std::cout << "=== 80-20 Rule Verification ===\n";
//   AccurateZipfGenerator zipf_8020(100, 1.16);
//   auto samples = zipf_8020.generate(100000);

//   std::map<int, int> frequency;
//   for (int val : samples) {
//     frequency[val]++;
//   }

//   // 计算前20%元素的访问比例
//   int top_20_percent = 20; // 前20个元素
//   int top_20_accesses = 0;
//   for (int i = 1; i <= top_20_percent; i++) {
//     top_20_accesses += frequency[i];
//   }

//   double ratio = static_cast<double>(top_20_accesses) / samples.size();
//   std::cout << "Top 20% elements account for " << std::setprecision(1)
//             << std::fixed << ratio * 100 << "% of accesses\n\n";

//   return 0;
// }

/**
 n-n支持操作有：
    1. 插入带有时间戳的边hashTable.insertWithTs(edge);
    2. 获取最新版本边hashTable.latest_value_ptr();
    3. 获取指定时间戳的某条边hashTable.getVersionInRange(value, timestamp,
 edge_property);
    4. 获取指定时间戳范围内的某条边hashTable.getVersionInRange(value, timestamp,
 edge_property);
    5. 获取一个顶点的所有最新边hashTable.getLatestEdges(vertex_id);
 */

int get(int maxhop, double aplha) {
  // 解析命令行标志 vscode传入参数去.vscode/settings.json
  int total_num = 10000;
  int total = 1000;
  hophash::HopscotchHashTable hashTable(total_num, maxhop);
  std::cout << "maxhop: " << maxhop << " alpha: " << aplha << std::endl;

  AccurateZipfGenerator zipf(100, aplha);
  auto random_values = zipf.generate(total);

  std::map<int, int> frequency;
  for (int val : random_values) {
    frequency[val]++;
  }

  // 计算前20%元素的访问比例
  int top_20_percent = 20; // 前20个元素
  int top_20_accesses = 0;
  for (int i = 1; i <= top_20_percent; i++) {
    top_20_accesses += frequency[i];
  }

  double ratio = static_cast<double>(top_20_accesses) / random_values.size();
  std::cout << "Top 20% elements account for " << std::setprecision(1)
            << std::fixed << ratio * 100 << "% of accesses\n";

  std::vector<ctgraph::SequenceNumber_t> random_values_seq;

  for (int i = 0; i < total; i++) {
    random_values_seq.emplace_back(1 + i);
  }

  int count = 0;
  auto start_time_insert = std::chrono::high_resolution_clock::now();
  std::unique_ptr<FixString> s = std::make_unique<FixString>("hello");
  for (int value : random_values) {
    ctgraph::Edge edge(value, random_values_seq[count], false, s.get());
    hashTable.insertWithTs(edge);
    count++;
  }
  auto end_time_insert = std::chrono::high_resolution_clock::now();

  auto duration_place = std::chrono::duration_cast<std::chrono::microseconds>(
      end_time_insert - start_time_insert);
  std::cout << "Time taken for hophashTable.insert: " << duration_place.count()
            << " μs" << std::endl;

  std::cout << "The num of max hop distance: "
            << hashTable.get_max_hop_distance() << std::endl;

  return 0;
}

int main() {
  std::filesystem::path outFilePath("../ZIPF4UPDATE/result.txt");
  std::filesystem::create_directories(
      outFilePath.parent_path()); // 确保目录存在
  std::ofstream outFile(outFilePath);
  if (!outFile) {
    std::cerr << "Failed to open the output file." << std::endl;
    return 1;
  }
  std::streambuf *coutbuf = std::cout.rdbuf(); // Save old buf
  std::cout.rdbuf(outFile.rdbuf()); // Redirect std::cout to result.txt

  // 外层是aplha循环为0，0.3，0.6，0.9，1.2，1.5，1.8
  float aplha[7] = {0, 0.3, 0.6, 0.9, 1.2, 1.5, 1.8};
  for (int i = 0; i < 7; i++) {
    for (int j = 4; j < 16; j++) {
      get(j, aplha[i]);
    }
  }

  // Reset std::cout to its original buffer
  std::cout.rdbuf(coutbuf);
  outFile.close();
}