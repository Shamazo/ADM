/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2024
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

  static std::pair<size_t, int32_t> parse_count_and_sum(
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
};

int GRouterTest::pip_number = 0;

TEST_F(GRouterTest, default_dop_shared_random) {
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
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit(gsplit_slack, GeneralizedRoutingPolicy::SHARED_RANDOM);
  auto split_one =
      split_builder
          .path(DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM});
  auto split_two =
      split_builder
          .path(DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
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
      parse_count_and_sum(union_statement.execute());
  EXPECT_EQ(baseline_count, union_count);
  EXPECT_EQ(baseline_sum, union_sum);
}

TEST_F(GRouterTest, default_dop_shared_local) {
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
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit(gsplit_slack, GeneralizedRoutingPolicy::SHARED_LOCAL);
  auto split_one =
      split_builder
          .path(DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
          .unpack()
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                        arg["lo_suppkey"].as("tmp", "sum")};
              },
              {SUM, SUM});
  auto split_two =
      split_builder
          .path(DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
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
      parse_count_and_sum(union_statement.execute());
  EXPECT_EQ(baseline_count, union_count);
  EXPECT_EQ(baseline_sum, union_sum);
}

/**
 * Test the generalized router with a varying number of consumers (param 0) and
 * a varying the dop of each consumer (param 1).
 */
class GRouterTestVaryNumConsumers
    : public GRouterTest,
      public ::testing::WithParamInterface<std::tuple<size_t, size_t>> {};

struct PrintToStringParamName {
  template <class ParamType>
  std::string operator()(
      const ::testing::TestParamInfo<ParamType>& info) const {
    const auto& param = info.param;
    std::ostringstream oss;
    oss << "Consumers" << std::get<0>(param) << "DOP" << std::get<1>(param);
    return oss.str();
  }
};

INSTANTIATE_TEST_SUITE_P(
    GRouterTestVaryNumConsumers, GRouterTestVaryNumConsumers,
    testing::Values(std::make_tuple(2, 1), std::make_tuple(3, 1),
                    std::make_tuple(2, 2), std::make_tuple(3, 2),
                    std::make_tuple(2, 4), std::make_tuple(3, 4),
                    std::make_tuple(2, 16)),
    PrintToStringParamName());

TEST_P(GRouterTestVaryNumConsumers,
       n_consumers_varying_dop_shared_random_policy) {
  const size_t num_splits = std::get<0>(GetParam());
  const DegreeOfParallelism split_path_dop =
      DegreeOfParallelism{std::get<1>(GetParam())};
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
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit(gsplit_slack, GeneralizedRoutingPolicy::SHARED_RANDOM);
  std::vector<RelBuilder> splits;
  for (size_t i = 0; i < num_splits; i++) {
    splits.push_back(split_builder
                         .path(DeviceType::CPU, split_path_dop,
                               std::make_unique<CpuNumaNodeAffinitizer>())
                         .unpack()
                         .reduce(
                             [&](const auto& arg) -> std::vector<expression_t> {
                               return {
                                   expression_t{int64_t{1}}.as("tmp", "cnt"),
                                   arg["lo_suppkey"].as("tmp", "sum")};
                             },
                             {SUM, SUM}));
  }

  auto first_split = splits.front();
  auto union_statement =
      first_split.unionAll({splits.begin() + 1, splits.end()})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto [union_count, union_sum] =
      parse_count_and_sum(union_statement.execute());
  EXPECT_EQ(baseline_count, union_count);
  EXPECT_EQ(baseline_sum, union_sum);
}

TEST_P(GRouterTestVaryNumConsumers,
       n_consumers_varying_dop_shared_local_policy) {
  const size_t num_splits = std::get<0>(GetParam());
  const DegreeOfParallelism split_path_dop =
      DegreeOfParallelism{std::get<1>(GetParam())};
  if (split_path_dop < topology::getInstance().getCpuNumaNodeCount()) {
    GTEST_SKIP()
        << "Skipping test with DOP smaller than the number of NUMA nodes";
  }
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
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  constexpr int gsplit_slack = 16;
  auto rbf_split = getRelBuilderFactory();
  auto split_builder =
      rbf_split.getBuilder()
          .scan("inputs/ssbm100/lineorder.csv", {"lo_suppkey"},
                CatalogParser::getInstance(), pg{"block"})
          .gsplit(gsplit_slack, GeneralizedRoutingPolicy::SHARED_LOCAL);
  std::vector<RelBuilder> splits;
  for (size_t i = 0; i < num_splits; i++) {
    splits.push_back(split_builder
                         .path(DeviceType::CPU, split_path_dop,
                               std::make_unique<CpuNumaNodeAffinitizer>())
                         .unpack()
                         .reduce(
                             [&](const auto& arg) -> std::vector<expression_t> {
                               return {
                                   expression_t{int64_t{1}}.as("tmp", "cnt"),
                                   arg["lo_suppkey"].as("tmp", "sum")};
                             },
                             {SUM, SUM}));
  }

  auto first_split = splits.front();
  auto union_statement =
      first_split.unionAll({splits.begin() + 1, splits.end()})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto [union_count, union_sum] =
      parse_count_and_sum(union_statement.execute());
  EXPECT_EQ(baseline_count, union_count);
  EXPECT_EQ(baseline_sum, union_sum);
}
