/*
                         RADaFlow (forked from proteus)
    Proteus -- High-performance query processing on heterogeneous hardware.

                        Copyright (c) 2025
        Data Intensive Applications and Systems Laboratory (DIAS)
                École Polytechnique Fédérale de Lausanne

                            All Rights Reserved.

    Permission to use, copy, modify and distribute this software and
    its documentation is hereby granted, provided that both the
    copyright notice and this permission notice appear in all copies of
    the software, derivative works or modified versions, and any
    portions thereof, and that both notices appear in supporting
    documentation.

    This code is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
    DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
    RESULTING FROM THE USE OF THIS SOFTWARE.
*/

#include <gtest/gtest.h>

#include <olap/operators/relbuilder-factory.hpp>
#include <olap/operators/relbuilder.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <storage/storage-manager.hpp>

class GRouterTest : public testing::Test {
 public:
  static int pip_number;  // used to avoid duplicate pipeline names and the
                          // resulting issues of query results being saved to
                          // the same shared memory file
  static void TearDownTestSuite() { StorageManager::getInstance().unloadAll(); }

 protected:
  void SetUp() override {}

  void TearDown() override {}

  static RelBuilderFactory getRelBuilderFactory() {
    auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    std::string name = std::string(test_info->test_suite_name()) + "_" +
                       test_info->name() + "_pip" + std::to_string(pip_number);
    std::replace(name.begin(), name.end(), '/', '-');
    pip_number += 1;
    return RelBuilderFactory{name};
  }

  /**
   * Parse the result of a query that returns a single tuple with two attributes
   * @return (count, sum)
   */
  static std::pair<size_t, int32_t> parse_single_count_and_sum(
      const QueryResult& res) {
    std::stringstream res_str;
    res_str << res;
    std::string out_tuple;
    int tuple_count = 0;
    size_t count = 0;
    // this could wrap around in query processing, but that should not matter
    // for the test
    int32_t sum = 0;

    while (std::getline(res_str, out_tuple)) {
      std::istringstream iss(out_tuple);
      std::string attr_str;
      int attr_idx = 0;
      while (std::getline(iss, attr_str, ',')) {
        if (attr_idx == 0) {
          count = std::stoull(attr_str);
        }
        if (attr_idx == 1) {
          sum = std::stoi(attr_str);
        }
        attr_idx += 1;
        EXPECT_LE(attr_idx, 2)
            << "Expected only two attribute (count,sum) in the result";
      }
      tuple_count += 1;
      EXPECT_EQ(tuple_count, 1) << "Expected only one tuple in the result";
    }
    EXPECT_EQ(tuple_count, 1) << "Expected only one tuple in the result";
    return {count, sum};
  }

  /**
   * Parse the result of a query that returns a single tuple with n * two
   * attributes
   * @return pair of vectors (counts, sums)
   */
  static std::pair<std::vector<size_t>, std::vector<int32_t>>
  parse_n_count_and_sum(const QueryResult& res, int n) {
    const int expected_attr_count = n * 2;
    std::stringstream res_str;
    res_str << res;
    std::string out_tuple;
    int tuple_count = 0;
    std::vector<size_t> counts;
    // this could wrap around in query processing, but that should not matter
    // for the test
    std::vector<int32_t> sums;

    while (std::getline(res_str, out_tuple)) {
      std::istringstream iss(out_tuple);
      std::string attr_str;
      int attr_idx = 0;
      while (std::getline(iss, attr_str, ',')) {
        if (attr_idx % 2 == 0) {
          counts.emplace_back(std::stoull(attr_str));
        }
        if (attr_idx % 2 == 1) {
          sums.emplace_back(std::stoi(attr_str));
        }
        attr_idx += 1;
        EXPECT_LE(expected_attr_count, expected_attr_count)
            << "Expected " << std::to_string(expected_attr_count)
            << " attributes in the result";
      }
      tuple_count += 1;
      EXPECT_EQ(tuple_count, 1) << "Expected only one tuple in the result";
    }
    EXPECT_EQ(tuple_count, 1) << "Expected only one tuple in the result";
    return {counts, sums};
  }
};

std::vector<double> normalizeCounts(const std::vector<size_t>& counts) {
  size_t total = std::accumulate(counts.begin(), counts.end(), 0ul);

  std::vector<double> normalizedCounts;
  normalizedCounts.reserve(counts.size());
  for (size_t count : counts) {
    normalizedCounts.push_back(static_cast<double>(count) / total);
  }

  return normalizedCounts;
}

int GRouterTest::pip_number = 0;

// =============================================================================
// LEGACY TESTS - COMMENTED OUT FOR V2 MIGRATION
// =============================================================================
// TODO: Migrate these tests to V2 policies after implementing equivalent
// policies Current legacy policies used and their V2 equivalents:
// - SHARED_RANDOM → HASH_BASED (to be implemented)
// - SHARED_LOCAL → LOCALITY_AWARE (to be implemented)
// - DISTINCT_RANDOM_SPLIT_FORCE_DATA_LOCAL → advanced policy (to be
// implemented)
// - DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL → advanced policy (to be
// implemented)
//
// The GeneralizedRouter has been migrated to V2-only policy system.
// These tests use legacy enum values that are no longer supported.
// =============================================================================

// TEST_F(GRouterTest, default_dop_shared_random) {
//   auto rbf_baseline = getRelBuilderFactory();
//   auto baseline_statement =
//       rbf_baseline.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto baseline_res = baseline_statement.execute();
//   auto [baseline_count, baseline_sum] =
//       parse_single_count_and_sum(baseline_res);
//
//   constexpr int gsplit_slack = 16;
//   auto rbf_split = getRelBuilderFactory();
//   auto split_builder =
//       rbf_split.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .gsplit(gsplit_slack, GeneralizedRoutingPolicy::SHARED_RANDOM);
//   auto split_one =
//       split_builder
//           .path(DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM});
//   auto split_two =
//       split_builder
//           .path(DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM});
//   auto union_statement =
//       split_one.unionAll({split_two})
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {arg["cnt"], arg["sum"]};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//
//   auto [union_count, union_sum] =
//       parse_single_count_and_sum(union_statement.execute());
//   EXPECT_EQ(baseline_count, union_count);
//   EXPECT_EQ(baseline_sum, union_sum);
// }
//
// TEST_F(GRouterTest, default_dop_shared_local) {
//   auto rbf_baseline = getRelBuilderFactory();
//   auto baseline_statement =
//       rbf_baseline.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto baseline_res = baseline_statement.execute();
//   auto [baseline_count, baseline_sum] =
//       parse_single_count_and_sum(baseline_res);
//
//   constexpr int gsplit_slack = 16;
//   auto rbf_split = getRelBuilderFactory();
//   auto split_builder =
//       rbf_split.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .gsplit(gsplit_slack, GeneralizedRoutingPolicy::SHARED_LOCAL);
//   auto split_one =
//       split_builder
//           .path(DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM});
//   auto split_two =
//       split_builder
//           .path(DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM});
//   auto union_statement =
//       split_one.unionAll({split_two})
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {arg["cnt"], arg["sum"]};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//
//   auto [union_count, union_sum] =
//       parse_single_count_and_sum(union_statement.execute());
//   EXPECT_EQ(baseline_count, union_count);
//   EXPECT_EQ(baseline_sum, union_sum);
// }
//
// // /**
// //  * Test the generalized router with a varying number of consumers (param
// 0) and
// //  * a varying the dop of each consumer (param 1).
// //  */
// class GRouterTestVaryNumConsumers
//     : public GRouterTest,
//       public ::testing::WithParamInterface<std::tuple<size_t, size_t>> {};
//
// struct GRouterTestVaryNumConsumersPrintToStringParamName {
//   template <class ParamType>
//   std::string operator()(
//       const ::testing::TestParamInfo<ParamType>& info) const {
//     const auto& param = info.param;
//     std::ostringstream oss;
//     oss << "Consumers" << std::get<0>(param) << "DOP" << std::get<1>(param);
//     return oss.str();
//   }
// };
//
// INSTANTIATE_TEST_SUITE_P(
//     GRouterTestVaryNumConsumers, GRouterTestVaryNumConsumers,
//     testing::Values(std::make_tuple(2, 1), std::make_tuple(3, 1),
//                     std::make_tuple(2, 2), std::make_tuple(3, 2),
//                     std::make_tuple(2, 4), std::make_tuple(3, 4),
//                     std::make_tuple(2, 8), std::make_tuple(4, 8),
//                     std::make_tuple(2, 16)),
//     GRouterTestVaryNumConsumersPrintToStringParamName());
//
// TEST_P(GRouterTestVaryNumConsumers, shared_random_policy) {
//   const size_t num_splits = std::get<0>(GetParam());
//   const DegreeOfParallelism split_path_dop =
//       DegreeOfParallelism{std::get<1>(GetParam())};
//   auto rbf_baseline = getRelBuilderFactory();
//   auto baseline_statement =
//       rbf_baseline.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto baseline_res = baseline_statement.execute();
//   auto [baseline_count, baseline_sum] =
//       parse_single_count_and_sum(baseline_res);
//
//   // In the split case, the result is a num_splits*2 tuple
//   std::vector<Monoid> reduction_ops;
//   for (int i = 0; i < num_splits; i++) {
//     reduction_ops.push_back(SUM);
//     reduction_ops.push_back(SUM);
//   }
//
//   constexpr int gsplit_slack = 16;
//   auto rbf_split = getRelBuilderFactory();
//   auto split_builder =
//       rbf_split.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .gsplit(gsplit_slack, GeneralizedRoutingPolicy::SHARED_RANDOM);
//   std::vector<RelBuilder> splits;
//   for (size_t i = 0; i < num_splits; i++) {
//     splits.push_back(split_builder
//                          .path(DeviceType::CPU, split_path_dop,
//                                std::make_unique<CpuNumaNodeAffinitizer>())
//                          .unpack()
//                          .reduce(
//                              [&](const auto& arg) ->
//                              std::vector<expression_t> {
//                                std::vector<expression_t> ret;
//                                for (int j = 0; j < num_splits; j++) {
//                                  if (j == i) {
//                                    ret.push_back(expression_t{int64_t{1}}.as(
//                                        "tmp", "cnt" + std::to_string(j)));
//                                    ret.push_back(arg["lo_suppkey"].as(
//                                        "tmp", "sum" + std::to_string(j)));
//                                  } else {
//                                    ret.push_back(expression_t{int64_t{0}}.as(
//                                        "tmp", "cnt" + std::to_string(j)));
//                                    ret.push_back(expression_t{int32_t{0}}.as(
//                                        "tmp", "sum" + std::to_string(j)));
//                                  }
//                                }
//                                return ret;
//                              },
//                              reduction_ops));
//   }
//
//   auto first_split = splits.front();
//   auto union_statement =
//       first_split.unionAll({splits.begin() + 1, splits.end()})
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 std::vector<expression_t> ret;
//                 for (int j = 0; j < num_splits; j++) {
//                   ret.push_back(arg["cnt" + std::to_string(j)]);
//                   ret.push_back(arg["sum" + std::to_string(j)]);
//                 }
//                 return ret;
//               },
//               reduction_ops)
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto [counts, sums] =
//       parse_n_count_and_sum(union_statement.execute(), num_splits);
//   const size_t total_count = std::accumulate(counts.begin(), counts.end(),
//   0ul); const size_t total_sum = std::accumulate(sums.begin(), sums.end(),
//   0); EXPECT_EQ(baseline_count, total_count); EXPECT_EQ(baseline_sum,
//   total_sum);
//
//   std::vector<double> normalizedCounts = normalizeCounts(counts);
//   for (auto& split_percentage : normalizedCounts) {
//     LOG(INFO) << "Split percentage: " << split_percentage;
//     // Note: looser bound on shared random because it is less deterministic
//     // As there is a shared queue of work, effects like numa locality to the
//     // queue or the order the consumer open can affect the distribution
//     EXPECT_NEAR(split_percentage, 1.0 / num_splits, 0.10)
//         << "expected a roughly equal distribution of work to splits";
//   }
// }
//
// TEST_P(GRouterTestVaryNumConsumers, shared_local_policy) {
//   const size_t num_splits = std::get<0>(GetParam());
//   const DegreeOfParallelism split_path_dop =
//       DegreeOfParallelism{std::get<1>(GetParam())};
//   if (split_path_dop < topology::getInstance().getCpuNumaNodeCount()) {
//     GTEST_SKIP()
//         << "Skipping test with DOP smaller than the number of NUMA nodes";
//   }
//   auto rbf_baseline = getRelBuilderFactory();
//   auto baseline_statement =
//       rbf_baseline.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto baseline_res = baseline_statement.execute();
//   auto [baseline_count, baseline_sum] =
//       parse_single_count_and_sum(baseline_res);
//
//   // In the split case, the result is a num_splits*2 tuple
//   std::vector<Monoid> reduction_ops;
//   for (int i = 0; i < num_splits; i++) {
//     reduction_ops.push_back(SUM);
//     reduction_ops.push_back(SUM);
//   }
//
//   constexpr int gsplit_slack = 16;
//   auto rbf_split = getRelBuilderFactory();
//   auto split_builder =
//       rbf_split.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .gsplit(gsplit_slack, GeneralizedRoutingPolicy::SHARED_LOCAL);
//   std::vector<RelBuilder> splits;
//   for (size_t i = 0; i < num_splits; i++) {
//     splits.push_back(split_builder
//                          .path(DeviceType::CPU, split_path_dop,
//                                std::make_unique<CpuNumaNodeAffinitizer>())
//                          .unpack()
//                          .reduce(
//                              [&](const auto& arg) ->
//                              std::vector<expression_t> {
//                                std::vector<expression_t> ret;
//                                for (int j = 0; j < num_splits; j++) {
//                                  if (j == i) {
//                                    ret.push_back(expression_t{int64_t{1}}.as(
//                                        "tmp", "cnt" + std::to_string(j)));
//                                    ret.push_back(arg["lo_suppkey"].as(
//                                        "tmp", "sum" + std::to_string(j)));
//                                  } else {
//                                    ret.push_back(expression_t{int64_t{0}}.as(
//                                        "tmp", "cnt" + std::to_string(j)));
//                                    ret.push_back(expression_t{int32_t{0}}.as(
//                                        "tmp", "sum" + std::to_string(j)));
//                                  }
//                                }
//                                return ret;
//                              },
//                              reduction_ops));
//   }
//
//   auto first_split = splits.front();
//   auto union_statement =
//       first_split.unionAll({splits.begin() + 1, splits.end()})
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 std::vector<expression_t> ret;
//                 for (int j = 0; j < num_splits; j++) {
//                   ret.push_back(arg["cnt" + std::to_string(j)]);
//                   ret.push_back(arg["sum" + std::to_string(j)]);
//                 }
//                 return ret;
//               },
//               reduction_ops)
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto [counts, sums] =
//       parse_n_count_and_sum(union_statement.execute(), num_splits);
//   const size_t total_count = std::accumulate(counts.begin(), counts.end(),
//   0ul); const size_t total_sum = std::accumulate(sums.begin(), sums.end(),
//   0); EXPECT_EQ(baseline_count, total_count); EXPECT_EQ(baseline_sum,
//   total_sum);
//
//   std::vector<double> normalizedCounts = normalizeCounts(counts);
//   for (auto& split_percentage : normalizedCounts) {
//     LOG(INFO) << "Split percentage: " << split_percentage;
//     EXPECT_NEAR(split_percentage, 1.0 / num_splits, 0.20)
//         << "expected a roughly equal distribution of work to splits";
//   }
// }
//
// TEST_P(GRouterTestVaryNumConsumers, random_consumer_force_data_local_policy)
// {
//   const size_t num_splits = std::get<0>(GetParam());
//   const DegreeOfParallelism split_path_dop =
//       DegreeOfParallelism{std::get<1>(GetParam())};
//   if (split_path_dop < topology::getInstance().getCpuNumaNodeCount()) {
//     GTEST_SKIP()
//         << "Skipping test with DOP smaller than the number of NUMA nodes";
//   }
//   auto rbf_baseline = getRelBuilderFactory();
//   auto baseline_statement =
//       rbf_baseline.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto baseline_res = baseline_statement.execute();
//   auto [baseline_count, baseline_sum] =
//       parse_single_count_and_sum(baseline_res);
//
//   // In the split case, the result is a num_splits*2 tuple
//   std::vector<Monoid> reduction_ops;
//   for (int i = 0; i < num_splits; i++) {
//     reduction_ops.push_back(SUM);
//     reduction_ops.push_back(SUM);
//   }
//
//   constexpr int gsplit_slack = 16;
//   auto rbf_split = getRelBuilderFactory();
//   auto split_builder =
//       rbf_split.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .gsplit(
//               gsplit_slack,
//               GeneralizedRoutingPolicy::DISTINCT_RANDOM_SPLIT_FORCE_DATA_LOCAL);
//   std::vector<RelBuilder> splits;
//   for (size_t i = 0; i < num_splits; i++) {
//     splits.push_back(split_builder
//                          .path(DeviceType::CPU, split_path_dop,
//                                std::make_unique<CpuNumaNodeAffinitizer>())
//                          .unpack()
//                          .reduce(
//                              [&](const auto& arg) ->
//                              std::vector<expression_t> {
//                                std::vector<expression_t> ret;
//                                for (int j = 0; j < num_splits; j++) {
//                                  if (j == i) {
//                                    ret.push_back(expression_t{int64_t{1}}.as(
//                                        "tmp", "cnt" + std::to_string(j)));
//                                    ret.push_back(arg["lo_suppkey"].as(
//                                        "tmp", "sum" + std::to_string(j)));
//                                  } else {
//                                    ret.push_back(expression_t{int64_t{0}}.as(
//                                        "tmp", "cnt" + std::to_string(j)));
//                                    ret.push_back(expression_t{int32_t{0}}.as(
//                                        "tmp", "sum" + std::to_string(j)));
//                                  }
//                                }
//                                return ret;
//                              },
//                              reduction_ops));
//   }
//
//   auto first_split = splits.front();
//   auto union_statement =
//       first_split.unionAll({splits.begin() + 1, splits.end()})
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 std::vector<expression_t> ret;
//                 for (int j = 0; j < num_splits; j++) {
//                   ret.push_back(arg["cnt" + std::to_string(j)]);
//                   ret.push_back(arg["sum" + std::to_string(j)]);
//                 }
//                 return ret;
//               },
//               reduction_ops)
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto [counts, sums] =
//       parse_n_count_and_sum(union_statement.execute(), num_splits);
//   const size_t total_count = std::accumulate(counts.begin(), counts.end(),
//   0ul); const size_t total_sum = std::accumulate(sums.begin(), sums.end(),
//   0); EXPECT_EQ(baseline_count, total_count); EXPECT_EQ(baseline_sum,
//   total_sum);
//
//   std::vector<double> normalizedCounts = normalizeCounts(counts);
//   for (auto& split_percentage : normalizedCounts) {
//     LOG(INFO) << "Split percentage: " << split_percentage;
//     EXPECT_NEAR(split_percentage, 1.0 / num_splits, 0.05)
//         << "expected a roughly equal distribution of work to splits";
//   }
// }
//
// TEST_P(GRouterTestVaryNumConsumers, random_consumer_prefer_data_local_policy)
// {
//   const size_t num_splits = std::get<0>(GetParam());
//   const DegreeOfParallelism split_path_dop =
//       DegreeOfParallelism{std::get<1>(GetParam())};
//   if (split_path_dop < topology::getInstance().getCpuNumaNodeCount()) {
//     GTEST_SKIP()
//         << "Skipping test with DOP smaller than the number of NUMA nodes";
//   }
//   auto rbf_baseline = getRelBuilderFactory();
//   auto baseline_statement =
//       rbf_baseline.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto baseline_res = baseline_statement.execute();
//   auto [baseline_count, baseline_sum] =
//       parse_single_count_and_sum(baseline_res);
//
//   // In the split case, the result is a num_splits*2 tuple
//   std::vector<Monoid> reduction_ops;
//   for (int i = 0; i < num_splits; i++) {
//     reduction_ops.push_back(SUM);
//     reduction_ops.push_back(SUM);
//   }
//
//   constexpr int gsplit_slack = 16;
//   auto rbf_split = getRelBuilderFactory();
//   auto split_builder =
//       rbf_split.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .gsplit(gsplit_slack, GeneralizedRoutingPolicy::
//                                     DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL);
//   std::vector<RelBuilder> splits;
//   for (size_t i = 0; i < num_splits; i++) {
//     splits.push_back(split_builder
//                          .path(DeviceType::CPU, split_path_dop,
//                                std::make_unique<CpuNumaNodeAffinitizer>())
//                          .unpack()
//                          .reduce(
//                              [&](const auto& arg) ->
//                              std::vector<expression_t> {
//                                std::vector<expression_t> ret;
//                                for (int j = 0; j < num_splits; j++) {
//                                  if (j == i) {
//                                    ret.push_back(expression_t{int64_t{1}}.as(
//                                        "tmp", "cnt" + std::to_string(j)));
//                                    ret.push_back(arg["lo_suppkey"].as(
//                                        "tmp", "sum" + std::to_string(j)));
//                                  } else {
//                                    ret.push_back(expression_t{int64_t{0}}.as(
//                                        "tmp", "cnt" + std::to_string(j)));
//                                    ret.push_back(expression_t{int32_t{0}}.as(
//                                        "tmp", "sum" + std::to_string(j)));
//                                  }
//                                }
//                                return ret;
//                              },
//                              reduction_ops));
//   }
//
//   auto first_split = splits.front();
//   auto union_statement =
//       first_split.unionAll({splits.begin() + 1, splits.end()})
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 std::vector<expression_t> ret;
//                 for (int j = 0; j < num_splits; j++) {
//                   ret.push_back(arg["cnt" + std::to_string(j)]);
//                   ret.push_back(arg["sum" + std::to_string(j)]);
//                 }
//                 return ret;
//               },
//               reduction_ops)
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto [counts, sums] =
//       parse_n_count_and_sum(union_statement.execute(), num_splits);
//   const size_t total_count = std::accumulate(counts.begin(), counts.end(),
//   0ul); const size_t total_sum = std::accumulate(sums.begin(), sums.end(),
//   0); EXPECT_EQ(baseline_count, total_count); EXPECT_EQ(baseline_sum,
//   total_sum);
//
//   std::vector<double> normalizedCounts = normalizeCounts(counts);
//   for (auto& split_percentage : normalizedCounts) {
//     LOG(INFO) << "Split percentage: " << split_percentage;
//     EXPECT_NEAR(split_percentage, 1.0 / num_splits, 0.05)
//         << "expected a roughly equal distribution of work to splits";
//   }
// }
//
// // /**
// //  * Test the generalized router with a varying number of consumers (param
// 0),
// //  * a varying the dop of each consumer (param 1) and varying number of CPU
// NUMA
// //  * nodes to use for each consumer (param 2).
// //  * Uses the DISTINCT_RANDOM_SPLIT_FORCE_DATA_LOCAL policy
// //  */
// class GRouterRandomConsumerForceLocalVaryNodes
//     : public GRouterTest,
//       public ::testing::WithParamInterface<std::tuple<size_t, size_t,
//       size_t>> {
// };
// // TODO add a test that loads all data to one numa node
//
// struct GRouterRandomLocalVaryNodesPrintToStringParamName {
//   template <class ParamType>
//   std::string operator()(
//       const ::testing::TestParamInfo<ParamType>& info) const {
//     const auto& param = info.param;
//     std::ostringstream oss;
//     oss << "Consumers" << std::get<0>(param) << "DOP" << std::get<1>(param)
//         << "NUMAPerConsumer" << std::get<2>(param);
//     return oss.str();
//   }
// };
//
// INSTANTIATE_TEST_SUITE_P(
//     GRouterRandomLocalVaryNodes, GRouterRandomConsumerForceLocalVaryNodes,
//     testing::Values(std::make_tuple(2, 1, 1), std::make_tuple(2, 4, 1),
//                     std::make_tuple(2, 4, 2), std::make_tuple(2, 4, 4),
//                     std::make_tuple(4, 4, 1)),
//     GRouterRandomLocalVaryNodesPrintToStringParamName());
//
// TEST_P(GRouterRandomConsumerForceLocalVaryNodes,
//        random_consumer_force_data_local_policy_specific_numa_aff) {
//   const size_t num_splits = std::get<0>(GetParam());
//   const DegreeOfParallelism split_path_dop =
//       DegreeOfParallelism{std::get<1>(GetParam())};
//   const size_t num_numa_per_consumer = std::get<2>(GetParam());
//
//   const auto& topo = topology::getInstance();
//
//   if (split_path_dop < num_numa_per_consumer) {
//     GTEST_SKIP()
//         << "Skipping test with DOP smaller than the number of NUMA nodes";
//   }
//   if (topo.getCpuNumaNodeCount() < 2) {
//     GTEST_SKIP() << "Skipping test with less than 2 NUMA nodes";
//   }
//   auto rbf_baseline = getRelBuilderFactory();
//   auto baseline_statement =
//       rbf_baseline.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto baseline_res = baseline_statement.execute();
//   auto [baseline_count, baseline_sum] =
//       parse_single_count_and_sum(baseline_res);
//
//   // In the split case, the result is a num_splits*2 tuple
//   std::vector<Monoid> reduction_ops;
//   for (int i = 0; i < num_splits; i++) {
//     reduction_ops.push_back(SUM);
//     reduction_ops.push_back(SUM);
//   }
//
//   constexpr int gsplit_slack = 16;
//   auto rbf_split = getRelBuilderFactory();
//   auto split_builder =
//       rbf_split.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .gsplit(
//               gsplit_slack,
//               GeneralizedRoutingPolicy::DISTINCT_RANDOM_SPLIT_FORCE_DATA_LOCAL);
//   std::vector<RelBuilder> splits;
//   for (size_t i = 0; i < num_splits; i++) {
//     std::vector<uint32_t> numa_ids;
//     for (size_t k = 0; k < num_numa_per_consumer; k++) {
//       numa_ids.push_back(
//           topo.getCpuNumaNodes()[(k + 1) % topo.getCpuNumaNodeCount()]
//               .getLocalCpuId());
//     }
//     splits.push_back(
//         split_builder
//             .path(DeviceType::CPU, split_path_dop,
//                   std::make_unique<SpecificCpuNumaNodeAffinitizer>(numa_ids))
//             .unpack()
//             .reduce(
//                 [&](const auto& arg) -> std::vector<expression_t> {
//                   std::vector<expression_t> ret;
//                   for (int j = 0; j < num_splits; j++) {
//                     if (j == i) {
//                       ret.push_back(expression_t{int64_t{1}}.as(
//                           "tmp", "cnt" + std::to_string(j)));
//                       ret.push_back(arg["lo_suppkey"].as(
//                           "tmp", "sum" + std::to_string(j)));
//                     } else {
//                       ret.push_back(expression_t{int64_t{0}}.as(
//                           "tmp", "cnt" + std::to_string(j)));
//                       ret.push_back(expression_t{int32_t{0}}.as(
//                           "tmp", "sum" + std::to_string(j)));
//                     }
//                   }
//                   return ret;
//                 },
//                 reduction_ops));
//   }
//
//   auto first_split = splits.front();
//   auto union_statement =
//       first_split.unionAll({splits.begin() + 1, splits.end()})
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 std::vector<expression_t> ret;
//                 for (int j = 0; j < num_splits; j++) {
//                   ret.push_back(arg["cnt" + std::to_string(j)]);
//                   ret.push_back(arg["sum" + std::to_string(j)]);
//                 }
//                 return ret;
//               },
//               reduction_ops)
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto [counts, sums] =
//       parse_n_count_and_sum(union_statement.execute(), num_splits);
//   const size_t total_count = std::accumulate(counts.begin(), counts.end(),
//   0ul); const size_t total_sum = std::accumulate(sums.begin(), sums.end(),
//   0); EXPECT_EQ(baseline_count, total_count); EXPECT_EQ(baseline_sum,
//   total_sum);
//
//   std::vector<double> normalizedCounts = normalizeCounts(counts);
//   for (auto& split_percentage : normalizedCounts) {
//     LOG(INFO) << "Split percentage: " << split_percentage;
//     EXPECT_NEAR(split_percentage, 1.0 / num_splits, 0.05)
//         << "expected a roughly equal distribution of work to splits";
//   }
// }
//
// // /**
// //  * Test the generalized router with a varying number of consumers (param
// 0),
// //  * a varying the dop of each consumer (param 1) and varying number of CPU
// NUMA
// //  * nodes to use for each consumer (param 2).
// //  * Uses the DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL policy
// //  */
// class GRouterRandomConsumerPreferLocalVaryNodes
//     : public GRouterTest,
//       public ::testing::WithParamInterface<std::tuple<size_t, size_t,
//       size_t>> {
// };
//
// INSTANTIATE_TEST_SUITE_P(
//     GRouterRandomLocalVaryNodes, GRouterRandomConsumerPreferLocalVaryNodes,
//     testing::Values(std::make_tuple(2, 1, 1), std::make_tuple(2, 4, 1),
//                     std::make_tuple(2, 4, 2), std::make_tuple(2, 4, 4),
//                     std::make_tuple(4, 4, 1)),
//     GRouterRandomLocalVaryNodesPrintToStringParamName());
//
// TEST_P(GRouterRandomConsumerPreferLocalVaryNodes,
//        random_consumer_prefer_data_local_policy_specific_numa_aff) {
//   const size_t num_splits = std::get<0>(GetParam());
//   const DegreeOfParallelism split_path_dop =
//       DegreeOfParallelism{std::get<1>(GetParam())};
//   const size_t num_numa_per_consumer = std::get<2>(GetParam());
//
//   const auto& topo = topology::getInstance();
//
//   if (split_path_dop < num_numa_per_consumer) {
//     GTEST_SKIP()
//         << "Skipping test with DOP smaller than the number of NUMA nodes";
//   }
//   if (topo.getCpuNumaNodeCount() < 2) {
//     GTEST_SKIP() << "Skipping test with less than 2 NUMA nodes";
//   }
//   auto rbf_baseline = getRelBuilderFactory();
//   auto baseline_statement =
//       rbf_baseline.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .unpack()
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 return {expression_t{int64_t{1}}.as("tmp", "cnt"),
//                         arg["lo_suppkey"].as("tmp", "sum")};
//               },
//               {SUM, SUM})
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto baseline_res = baseline_statement.execute();
//   auto [baseline_count, baseline_sum] =
//       parse_single_count_and_sum(baseline_res);
//
//   // In the split case, the result is a num_splits*2 tuple
//   std::vector<Monoid> reduction_ops;
//   for (int i = 0; i < num_splits; i++) {
//     reduction_ops.push_back(SUM);
//     reduction_ops.push_back(SUM);
//   }
//
//   constexpr int gsplit_slack = 16;
//   auto rbf_split = getRelBuilderFactory();
//   auto split_builder =
//       rbf_split.getBuilder()
//           .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
//                 CatalogParser::getInstance(), pg{"block"})
//           .gsplit(gsplit_slack, GeneralizedRoutingPolicy::
//                                     DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL);
//   std::vector<RelBuilder> splits;
//   for (size_t i = 0; i < num_splits; i++) {
//     std::vector<uint32_t> numa_ids;
//     for (size_t k = 0; k < num_numa_per_consumer; k++) {
//       numa_ids.push_back(
//           topo.getCpuNumaNodes()[(k + 1) % topo.getCpuNumaNodeCount()]
//               .getLocalCpuId());
//     }
//     splits.push_back(
//         split_builder
//             .path(DeviceType::CPU, split_path_dop,
//                   std::make_unique<SpecificCpuNumaNodeAffinitizer>(numa_ids))
//             .unpack()
//             .reduce(
//                 [&](const auto& arg) -> std::vector<expression_t> {
//                   std::vector<expression_t> ret;
//                   for (int j = 0; j < num_splits; j++) {
//                     if (j == i) {
//                       ret.push_back(expression_t{int64_t{1}}.as(
//                           "tmp", "cnt" + std::to_string(j)));
//                       ret.push_back(arg["lo_suppkey"].as(
//                           "tmp", "sum" + std::to_string(j)));
//                     } else {
//                       ret.push_back(expression_t{int64_t{0}}.as(
//                           "tmp", "cnt" + std::to_string(j)));
//                       ret.push_back(expression_t{int32_t{0}}.as(
//                           "tmp", "sum" + std::to_string(j)));
//                     }
//                   }
//                   return ret;
//                 },
//                 reduction_ops));
//   }
//
//   auto first_split = splits.front();
//   auto union_statement =
//       first_split.unionAll({splits.begin() + 1, splits.end()})
//           .reduce(
//               [&](const auto& arg) -> std::vector<expression_t> {
//                 std::vector<expression_t> ret;
//                 for (int j = 0; j < num_splits; j++) {
//                   ret.push_back(arg["cnt" + std::to_string(j)]);
//                   ret.push_back(arg["sum" + std::to_string(j)]);
//                 }
//                 return ret;
//               },
//               reduction_ops)
//           .print(pg{"pm-csv"})
//           .prepare();
//   auto [counts, sums] =
//       parse_n_count_and_sum(union_statement.execute(), num_splits);
//   const size_t total_count = std::accumulate(counts.begin(), counts.end(),
//   0ul); const size_t total_sum = std::accumulate(sums.begin(), sums.end(),
//   0); EXPECT_EQ(baseline_count, total_count); EXPECT_EQ(baseline_sum,
//   total_sum);
//
//   std::vector<double> normalizedCounts = normalizeCounts(counts);
//   for (auto& split_percentage : normalizedCounts) {
//     LOG(INFO) << "Split percentage: " << split_percentage;
//     EXPECT_NEAR(split_percentage, 1.0 / num_splits, 0.05)
//         << "expected a roughly equal distribution of work to splits";
//   }
// }

// =============================================================================
// V2 POLICY TESTS - ROUND-ROBIN IMPLEMENTATION
// =============================================================================

TEST_F(GRouterTest, round_robin_v2_policy) {
  // Baseline: run the query without splitting to get expected results
  const DegreeOfParallelism split_path_dop = DegreeOfParallelism{1};
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] =
      parse_single_count_and_sum(baseline_res);

  // Split case: use V2 round-robin policy with gsplit_v2
  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit_v2(gsplit_slack,
                     proteus::routing::GeneralizedRoutingPolicyV2::ROUND_ROBIN);

  auto split_one = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_two = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});

  auto union_statement =
      split_one.unionAll({split_two})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [union_count, union_sum] =
      parse_single_count_and_sum(union_statement.execute());

  // Verify correctness: total count and sum should match baseline
  EXPECT_EQ(baseline_count, union_count)
      << "V2 round-robin: Total count mismatch";
  EXPECT_EQ(baseline_sum, union_sum) << "V2 round-robin: Total sum mismatch";

  LOG(INFO) << "V2 round-robin test passed: baseline_count=" << baseline_count
            << ", union_count=" << union_count
            << ", baseline_sum=" << baseline_sum << ", union_sum=" << union_sum;
}

// Test round-robin V2 policy with 3 consumers (single-threaded)
TEST_F(GRouterTest, round_robin_v2_3_consumers) {
  // Baseline: run the query without splitting to get expected results
  const DegreeOfParallelism split_path_dop = DegreeOfParallelism{1};
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] =
      parse_single_count_and_sum(baseline_res);

  // Split case: use V2 round-robin policy with gsplit_v2 across 3 consumers
  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit_v2(gsplit_slack,
                     proteus::routing::GeneralizedRoutingPolicyV2::ROUND_ROBIN);

  auto split_one = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_two = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_three =
      split_builder
          .path(DeviceType::CPU, split_path_dop,
                std::make_unique<CpuNumaNodeAffinitizer>())
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM});

  auto union_statement =
      split_one.unionAll({split_two, split_three})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [union_count, union_sum] =
      parse_single_count_and_sum(union_statement.execute());

  // Verify correctness: total count and sum should match baseline
  EXPECT_EQ(baseline_count, union_count)
      << "V2 round-robin 3 consumers: Total count mismatch";
  EXPECT_EQ(baseline_sum, union_sum)
      << "V2 round-robin 3 consumers: Total sum mismatch";

  LOG(INFO) << "V2 round-robin 3 consumers test passed: baseline_count="
            << baseline_count << ", union_count=" << union_count
            << ", baseline_sum=" << baseline_sum << ", union_sum=" << union_sum;
}

// Test round-robin V2 policy with 4 consumers (single-threaded)
TEST_F(GRouterTest, round_robin_v2_4_consumers) {
  // Baseline: run the query without splitting to get expected results
  const DegreeOfParallelism split_path_dop = DegreeOfParallelism{1};
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] =
      parse_single_count_and_sum(baseline_res);

  // Split case: use V2 round-robin policy with gsplit_v2 across 4 consumers
  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit_v2(gsplit_slack,
                     proteus::routing::GeneralizedRoutingPolicyV2::ROUND_ROBIN);

  auto split_one = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_two = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_three =
      split_builder
          .path(DeviceType::CPU, split_path_dop,
                std::make_unique<CpuNumaNodeAffinitizer>())
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM});
  auto split_four = split_builder
                        .path(DeviceType::CPU, split_path_dop,
                              std::make_unique<CpuNumaNodeAffinitizer>())
                        .unpack()
                        .reduce(
                            [&](const auto& arg) -> std::vector<expression_t> {
                              return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                      arg["lo_suppkey"].as("tmp", "sum")};
                            },
                            {SUM, SUM});

  auto union_statement =
      split_one.unionAll({split_two, split_three, split_four})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [union_count, union_sum] =
      parse_single_count_and_sum(union_statement.execute());

  // Verify correctness: total count and sum should match baseline
  EXPECT_EQ(baseline_count, union_count)
      << "V2 round-robin 4 consumers: Total count mismatch";
  EXPECT_EQ(baseline_sum, union_sum)
      << "V2 round-robin 4 consumers: Total sum mismatch";

  LOG(INFO) << "V2 round-robin 4 consumers test passed: baseline_count="
            << baseline_count << ", union_count=" << union_count
            << ", baseline_sum=" << baseline_sum << ", union_sum=" << union_sum;
}

// Test round-robin V2 policy with 3 consumers (multi-threaded)
TEST_F(GRouterTest, round_robin_v2_3_consumers_multithreaded) {
  // Baseline: run the query without splitting to get expected results
  const DegreeOfParallelism split_path_dop =
      DegreeOfParallelism{2};  // Multi-threaded
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] =
      parse_single_count_and_sum(baseline_res);

  // Split case: use V2 round-robin policy with gsplit_v2 across 3 multithreaded
  // consumers
  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit_v2(gsplit_slack,
                     proteus::routing::GeneralizedRoutingPolicyV2::ROUND_ROBIN);

  auto split_one = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_two = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_three =
      split_builder
          .path(DeviceType::CPU, split_path_dop,
                std::make_unique<CpuNumaNodeAffinitizer>())
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM});

  auto union_statement =
      split_one.unionAll({split_two, split_three})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [union_count, union_sum] =
      parse_single_count_and_sum(union_statement.execute());

  // Verify correctness: total count and sum should match baseline
  EXPECT_EQ(baseline_count, union_count)
      << "V2 round-robin 3 consumers multithreaded: Total count mismatch";
  EXPECT_EQ(baseline_sum, union_sum)
      << "V2 round-robin 3 consumers multithreaded: Total sum mismatch";

  LOG(INFO)
      << "V2 round-robin 3 consumers multithreaded test passed: baseline_count="
      << baseline_count << ", union_count=" << union_count
      << ", baseline_sum=" << baseline_sum << ", union_sum=" << union_sum;
}

// Test round-robin V2 policy with 4 consumers (multi-threaded)
TEST_F(GRouterTest, round_robin_v2_4_consumers_multithreaded) {
  // Baseline: run the query without splitting to get expected results
  const DegreeOfParallelism split_path_dop =
      DegreeOfParallelism{2};  // Multi-threaded
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] =
      parse_single_count_and_sum(baseline_res);

  // Split case: use V2 round-robin policy with gsplit_v2 across 4 multithreaded
  // consumers
  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit_v2(gsplit_slack,
                     proteus::routing::GeneralizedRoutingPolicyV2::ROUND_ROBIN);

  auto split_one = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_two = split_builder
                       .path(DeviceType::CPU, split_path_dop,
                             std::make_unique<CpuNumaNodeAffinitizer>())
                       .unpack()
                       .reduce(
                           [&](const auto& arg) -> std::vector<expression_t> {
                             return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                     arg["lo_suppkey"].as("tmp", "sum")};
                           },
                           {SUM, SUM});
  auto split_three =
      split_builder
          .path(DeviceType::CPU, split_path_dop,
                std::make_unique<CpuNumaNodeAffinitizer>())
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM});
  auto split_four = split_builder
                        .path(DeviceType::CPU, split_path_dop,
                              std::make_unique<CpuNumaNodeAffinitizer>())
                        .unpack()
                        .reduce(
                            [&](const auto& arg) -> std::vector<expression_t> {
                              return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                                      arg["lo_suppkey"].as("tmp", "sum")};
                            },
                            {SUM, SUM});

  auto union_statement =
      split_one.unionAll({split_two, split_three, split_four})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [union_count, union_sum] =
      parse_single_count_and_sum(union_statement.execute());

  // Verify correctness: total count and sum should match baseline
  EXPECT_EQ(baseline_count, union_count)
      << "V2 round-robin 4 consumers multithreaded: Total count mismatch";
  EXPECT_EQ(baseline_sum, union_sum)
      << "V2 round-robin 4 consumers multithreaded: Total sum mismatch";

  LOG(INFO)
      << "V2 round-robin 4 consumers multithreaded test passed: baseline_count="
      << baseline_count << ", union_count=" << union_count
      << ", baseline_sum=" << baseline_sum << ", union_sum=" << union_sum;
}

// =============================================================================
// V2 POLICY TESTS - LOCALITY-AWARE IMPLEMENTATION
// =============================================================================

// Mock Affinitizer for deterministic testing
class MockAffinitizer : public Affinitizer {
 private:
  std::unordered_map<void*, size_t> ptr_to_numa_map_;
  std::vector<size_t> accessible_cus_;
  mutable std::unordered_map<void*, size_t> routing_history_;

 public:
  MockAffinitizer(const std::vector<size_t>& accessible_cus)
      : accessible_cus_(accessible_cus) {}

  // Map a data pointer to a specific NUMA node
  void mapPointerToNuma(void* ptr, size_t numa_idx) {
    if (std::find(accessible_cus_.begin(), accessible_cus_.end(), numa_idx) ==
        accessible_cus_.end()) {
      LOG(FATAL) << "NUMA index " << numa_idx << " not in accessible CUs";
    }
    ptr_to_numa_map_[ptr] = numa_idx;
  }

  // Affinitizer interface implementations
  size_t getAvailableCUIndex(size_t i) const override {
    return accessible_cus_[i % accessible_cus_.size()];
  }

  const topology::cu& getAvailableCU(size_t i) const override {
    // Return the NUMA node at the requested index
    size_t cu_idx = getAvailableCUIndex(i);
    return topology::getInstance().getCpuNumaNodes()[cu_idx];
  }

  size_t countAffCUs() const override { return accessible_cus_.size(); }

  std::vector<size_t> getCUIndexDomain() const override {
    return accessible_cus_;
  }

  size_t countAllCUs() const override {
    // Total system NUMA nodes
    return topology::getInstance().getCpuNumaNodeCount();
  }

  size_t getLocalCUIndex(void* ptr) const override {
    auto it = ptr_to_numa_map_.find(ptr);
    size_t actual_numa_node;

    if (it != ptr_to_numa_map_.end()) {
      actual_numa_node = it->second;
      // Track routing for verification
      routing_history_[ptr] = actual_numa_node;

      // Check if this NUMA node is in our accessible_cus_ vector
      auto pos = std::find(accessible_cus_.begin(), accessible_cus_.end(),
                           actual_numa_node);
      if (pos != accessible_cus_.end()) {
        // Return the position index (0 to countAffCUs()-1)
        return std::distance(accessible_cus_.begin(), pos);
      }
    } else {
      // Default: assume data is on NUMA node 0
      actual_numa_node = 0;
    }

    // Data is not on any of our accessible CUs, find the closest one
    // For simplicity in mock, just return index 0 (first accessible CU)
    // In a real implementation, this would use NUMA distance calculations
    return 0;
  }

  // Test helper to verify routing
  std::unordered_map<void*, size_t> getRoutingHistory() const {
    return routing_history_;
  }

  void clearRoutingHistory() { routing_history_.clear(); }
};

// Test 1: Single consumer with single NUMA node
TEST_F(GRouterTest, locality_aware_v2_single_consumer_single_numa) {
  const DegreeOfParallelism split_path_dop = DegreeOfParallelism{1};

  // Create mock affinitizer with single NUMA node (index 0)
  auto mock_aff = std::make_unique<MockAffinitizer>(std::vector<size_t>{0});

  // Map some test pointers to NUMA node 0
  void* test_ptr1 = reinterpret_cast<void*>(0x1000);
  void* test_ptr2 = reinterpret_cast<void*>(0x2000);
  void* test_ptr3 = reinterpret_cast<void*>(0x3000);
  mock_aff->mapPointerToNuma(test_ptr1, 0);
  mock_aff->mapPointerToNuma(test_ptr2, 0);
  mock_aff->mapPointerToNuma(test_ptr3, 0);

  // Since we can't directly test the routing decision (it happens in JIT code),
  // we'll verify the configuration is correct
  auto rbf = getRelBuilderFactory();
  auto builder =
      rbf.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit_v2(
              16, proteus::routing::GeneralizedRoutingPolicyV2::LOCALITY_AWARE);

  auto consumer =
      builder.path(DeviceType::CPU, split_path_dop, std::move(mock_aff))
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto res = consumer.execute();
  auto [count, sum] = parse_single_count_and_sum(res);

  // Verify we got results (actual routing happens in JIT)
  EXPECT_GT(count, 0) << "Expected non-zero count for single consumer";
  LOG(INFO) << "Locality-aware single consumer test: count=" << count
            << ", sum=" << sum;
}

// Test 2: Multiple consumers with different NUMA configurations
TEST_F(GRouterTest, locality_aware_v2_multi_consumer_multi_numa) {
  // need at least 1 worker per NUMA node or else risk hanging
  const DegreeOfParallelism split_path_dop = DegreeOfParallelism{2};
  const size_t num_consumers = 2;

  // Create mock affinitizers
  // Consumer 0: can access NUMA nodes 0 and 1
  auto mock_aff0 = std::make_unique<MockAffinitizer>(std::vector<size_t>{0, 1});
  // Consumer 1: can access NUMA nodes 2 and 3
  auto mock_aff1 = std::make_unique<MockAffinitizer>(std::vector<size_t>{2, 3});

  // Baseline
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto [baseline_count, baseline_sum] =
      parse_single_count_and_sum(baseline_statement.execute());

  // Split with locality-aware routing
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit_v2(
              16, proteus::routing::GeneralizedRoutingPolicyV2::LOCALITY_AWARE);

  // Consumer 0 with NUMA 0,1
  auto split_one =
      split_builder.path(DeviceType::CPU, split_path_dop, std::move(mock_aff0))
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt0"),
                        arg["lo_suppkey"].as("tmp", "sum0"),
                        expression_t{int64_t{0}}.as("tmp", "cnt1"),
                        expression_t{int32_t{0}}.as("tmp", "sum1")};
              },
              {SUM, SUM, SUM, SUM});

  // Consumer 1 with NUMA 2,3
  auto split_two =
      split_builder.path(DeviceType::CPU, split_path_dop, std::move(mock_aff1))
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{0}}.as("tmp", "cnt0"),
                        expression_t{int32_t{0}}.as("tmp", "sum0"),
                        expression_t{int64_t{1}}.as("tmp", "cnt1"),
                        arg["lo_suppkey"].as("tmp", "sum1")};
              },
              {SUM, SUM, SUM, SUM});

  auto union_statement =
      split_one.unionAll({split_two})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt0"], arg["sum0"], arg["cnt1"], arg["sum1"]};
              },
              {SUM, SUM, SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [counts, sums] =
      parse_n_count_and_sum(union_statement.execute(), num_consumers);

  // Verify total correctness
  const size_t total_count = std::accumulate(counts.begin(), counts.end(), 0ul);
  const int32_t total_sum = std::accumulate(sums.begin(), sums.end(), 0);
  EXPECT_EQ(baseline_count, total_count) << "Total count mismatch";
  EXPECT_EQ(baseline_sum, total_sum) << "Total sum mismatch";

  // Check distribution
  std::vector<double> normalizedCounts = normalizeCounts(counts);
  for (size_t i = 0; i < normalizedCounts.size(); i++) {
    LOG(INFO) << "Consumer " << i
              << " split percentage: " << normalizedCounts[i];
  }
}

// =============================================================================
// PARAMETERIZED REAL TOPOLOGY TESTS
// =============================================================================

class GRouterLocalityAwareRealTopology
    : public GRouterTest,
      public ::testing::WithParamInterface<std::tuple<size_t, size_t, size_t>> {
};

// Parameter combinations: (num_consumers, dop_per_consumer,
// numa_nodes_per_consumer)
INSTANTIATE_TEST_SUITE_P(
    LocalityAwareTests, GRouterLocalityAwareRealTopology,
    testing::Values(std::make_tuple(2, 1, 1), std::make_tuple(2, 4, 1),
                    std::make_tuple(2, 4, 2), std::make_tuple(4, 4, 1)),
    [](const testing::TestParamInfo<std::tuple<size_t, size_t, size_t>>& info) {
      size_t num_consumers = std::get<0>(info.param);
      size_t dop = std::get<1>(info.param);
      size_t numa_per_consumer = std::get<2>(info.param);
      return "consumers" + std::to_string(num_consumers) + "_dop" +
             std::to_string(dop) + "_numa" + std::to_string(numa_per_consumer);
    });

TEST_P(GRouterLocalityAwareRealTopology, locality_aware_v2_real_topology) {
  const size_t num_splits = std::get<0>(GetParam());
  const DegreeOfParallelism split_path_dop =
      DegreeOfParallelism{std::get<1>(GetParam())};
  const size_t num_numa_per_consumer = std::get<2>(GetParam());

  const auto& topo = topology::getInstance();

  if (split_path_dop < num_numa_per_consumer) {
    GTEST_SKIP()
        << "Skipping test with DOP smaller than the number of NUMA nodes";
  }
  if (topo.getCpuNumaNodeCount() < 2) {
    GTEST_SKIP() << "Skipping test with less than 2 NUMA nodes";
  }

  // Baseline
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] =
      parse_single_count_and_sum(baseline_res);

  // Build reduction ops for multi-consumer case
  std::vector<Monoid> reduction_ops;
  for (size_t i = 0; i < num_splits; i++) {
    reduction_ops.push_back(SUM);
    reduction_ops.push_back(SUM);
  }

  // Split case with locality-aware routing
  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit_v2(
              gsplit_slack,
              proteus::routing::GeneralizedRoutingPolicyV2::LOCALITY_AWARE);

  std::vector<RelBuilder> splits;
  for (size_t i = 0; i < num_splits; i++) {
    // Assign NUMA nodes to this consumer
    std::vector<uint32_t> numa_ids;
    for (size_t k = 0; k < num_numa_per_consumer; k++) {
      numa_ids.push_back(
          topo.getCpuNumaNodes()[(k + i) % topo.getCpuNumaNodeCount()]
              .getLocalCpuId());
    }

    splits.push_back(
        split_builder
            .path(DeviceType::CPU, split_path_dop,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(numa_ids))
            .unpack()
            .reduce(
                [&](const auto& arg) -> std::vector<expression_t> {
                  std::vector<expression_t> ret;
                  for (size_t j = 0; j < num_splits; j++) {
                    if (j == i) {
                      ret.push_back(expression_t{int64_t{1}}.as(
                          "tmp", "cnt" + std::to_string(j)));
                      ret.push_back(arg["lo_suppkey"].as(
                          "tmp", "sum" + std::to_string(j)));
                    } else {
                      ret.push_back(expression_t{int64_t{0}}.as(
                          "tmp", "cnt" + std::to_string(j)));
                      ret.push_back(expression_t{int32_t{0}}.as(
                          "tmp", "sum" + std::to_string(j)));
                    }
                  }
                  return ret;
                },
                reduction_ops));
  }

  auto first_split = splits.front();
  auto union_statement =
      first_split.unionAll({splits.begin() + 1, splits.end()})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                std::vector<expression_t> ret;
                for (size_t j = 0; j < num_splits; j++) {
                  ret.push_back(arg["cnt" + std::to_string(j)]);
                  ret.push_back(arg["sum" + std::to_string(j)]);
                }
                return ret;
              },
              reduction_ops)
          .print(pg{"pm-csv"})
          .prepare();

  auto [counts, sums] =
      parse_n_count_and_sum(union_statement.execute(), num_splits);

  const size_t total_count = std::accumulate(counts.begin(), counts.end(), 0ul);
  const int32_t total_sum = std::accumulate(sums.begin(), sums.end(), 0);
  EXPECT_EQ(baseline_count, total_count);
  EXPECT_EQ(baseline_sum, total_sum);

  std::vector<double> normalizedCounts = normalizeCounts(counts);
  for (size_t i = 0; i < normalizedCounts.size(); i++) {
    LOG(INFO) << "Locality-aware consumer " << i
              << " split percentage: " << normalizedCounts[i];
    EXPECT_NEAR(normalizedCounts[i], 1.0 / num_splits, 0.05)
        << "expected a roughly equal distribution of work to splits";
  }
}
