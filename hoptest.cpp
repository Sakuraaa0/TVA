#include "CtStore.hpp"
#include "VersionChain.hpp"
#include "flags.hpp"
#include "hopscotch/hopscotchhash.hpp"
#include "types.hpp"
#include <gflags/gflags.h>
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
  auto now = std::chrono::system_clock::now();
  auto duration = now.time_since_epoch();
  auto microseconds =
      std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
  return static_cast<uint64_t>(microseconds);
}

class AccurateZipfGenerator {
private:
  std::mt19937 gen;
  std::uniform_real_distribution<double> uniform_dist;
  int N;
  double alpha;
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

    double harmonic_sum = 0.0;
    for (int k = 1; k <= N; k++) {
      harmonic_sum += 1.0 / std::pow(k, alpha);
    }

    double cumulative = 0.0;
    for (int k = 1; k <= N; k++) {
      // P(X = k) = k^(-α) / H(N,α)
      probabilities[k] = (1.0 / std::pow(k, alpha)) / harmonic_sum;
      cumulative += probabilities[k];
      cumulative_probs[k] = cumulative;
    }

    std::cout << "Zipf Distribution Parameters:\n";
    std::cout << "N = " << N << ", α = " << alpha << "\n";
    std::cout << "H(N,α) = " << harmonic_sum << "\n\n";

    std::cout << "Top 10 probabilities:\n";
    std::cout << "k\tP(X=k)\t\tCumulative\n";
    std::cout << "---\t------\t\t----------\n";
    for (int k = 1; k <= std::min(10, N); k++) {
      std::cout << k << "\t" << std::fixed << std::setprecision(6)
                << probabilities[k] << "\t" << cumulative_probs[k] << "\n";
    }
    std::cout << "\n";
  }
  int generate() {
    double u = uniform_dist(gen);
    for (int k = 1; k <= N; k++) {
      if (u <= cumulative_probs[k]) {
        return k;
      }
    }
    return N;
  }
  std::vector<int> generate(int count) {
    std::vector<int> result;
    result.reserve(count);
    for (int i = 0; i < count; i++) {
      result.push_back(generate());
    }
    return result;
  }

  double getTheoreticalProbability(int k) const {
    if (k < 1 || k > N)
      return 0.0;
    return probabilities[k];
  }

  int getN() const { return N; }
  double getAlpha() const { return alpha; }
};

void validateZipfDistribution(AccurateZipfGenerator &generator,
                              int sample_size) {
  auto samples = generator.generate(sample_size);

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

  double harmonic_sum = 0.0;
  for (int k = 1; k <= N; k++) {
    harmonic_sum += 1.0 / std::pow(k, alpha);
  }

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


/**
 * Supported operations:
 * 1. Insert an edge with a timestamp: hashTable.insertWithTs(edge);
 * 2. Get the latest version of an edge: hashTable.latest_value_ptr();
 * 3. Get the edge at a specified timestamp: hashTable.getVersionInRange(value, timestamp, edge_property);
 * 4. Get edges within a specified timestamp range: hashTable.getVersionInRange(value, timestamp, edge_property);
 * 5. Get all latest edges of a vertex: hashTable.getLatestEdges(vertex_id);
 */


int get(double aplha) {
    // Parse command-line flags passed by VSCode via .vscode/settings.json
  int total_num = 10000;
  hophash::HopscotchHashTable hashTable(total_num, 2);

  AccurateZipfGenerator zipf(100, aplha);
  auto random_values = zipf.generate(total_num);
  std::cout << "=== 80-20 Rule Verification ===\n";

  std::map<int, int> frequency;
  for (int val : random_values) {
    frequency[val]++;
  }
  int top_20_percent = 20;
  int top_20_accesses = 0;
  for (int i = 1; i <= top_20_percent; i++) {
    top_20_accesses += frequency[i];
  }

  double ratio = static_cast<double>(top_20_accesses) / random_values.size();
  std::cout << "Top 20% elements account for " << std::setprecision(1)
            << std::fixed << ratio * 100 << "% of accesses\n\n";

  std::vector<ctgraph::SequenceNumber_t> random_values_seq;

  int total = 9990;
  for (int i = 0; i < total; i++) {
    random_values_seq.push_back(getCurrentTimeInMicroseconds());
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
  std::chrono::duration<double> duration_insert =
      end_time_insert - start_time_insert;
  std::cout << "Time taken for hophashTable.insert: " << duration_insert.count()
            << " seconds" << std::endl;

  std::cout << "The num of max hop distance: "
            << hashTable.get_max_hop_distance() << std::endl;

  return 0;
}

int main() {
  for (int i = 0; i < 19; i++) {
    get(0.1 * i);
  }
}