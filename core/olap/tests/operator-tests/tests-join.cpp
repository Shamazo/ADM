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

#include "olap/routing/affinitization-factory.hpp"

class SelfJoinTest : public testing::Test {
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
        try {
          if (attr_idx == 0) {
            count = std::stoull(attr_str);
          }
          if (attr_idx == 1) {
            sum = std::stoi(attr_str);
          }
        } catch (const std::out_of_range& e) {
          CHECK(false) << "Error parsing tuple: " << out_tuple
                       << " with exception: " << e.what();
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

int SelfJoinTest::pip_number = 0;

TEST_F(SelfJoinTest, self_join_with_build_filter) {
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["c_custkey"].as("tmp", "c_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["c_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  auto rbf_join = getRelBuilderFactory();
  auto build_pipeline =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_build", "b_custkey"),
                    arg["c_nation"].as("t_build", "b_nation")};
          });

  auto probe_side =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe", "p_custkey"),
                    arg["c_nation"].as("t_probe", "p_nation")};
          })
          .join(
              build_pipeline,
              [&](const auto& build_arg) -> expression_t {
                return build_arg["b_custkey"];
              },
              [&](const auto& probe_arg) -> expression_t {
                return probe_arg["p_custkey"];
              })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [join_count, join_sum] = parse_count_and_sum(probe_side.execute());
  EXPECT_EQ(baseline_count, join_count);
  EXPECT_EQ(baseline_sum, join_sum);
}

TEST_F(SelfJoinTest, multi_probe_pipelines_shared_HT) {
  // This test verifies that two probe pipelines can share the same build hash
  // table First, create a baseline result that we'll compare against
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["c_custkey"].as("tmp", "c_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["c_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  // Now create a build pipeline with a HashJoinChained operator
  auto rbf_join = getRelBuilderFactory();
  auto build_pipeline =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_build", "b_custkey"),
                    arg["c_nation"].as("t_build", "b_nation")};
          });

  // Create the first probe pipeline using standard join
  auto probe_input =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe1", "p_custkey"),
                    arg["c_nation"].as("t_probe1", "p_nation")};
          });

  // Store the RelBuilder with the HashJoinChained at the root
  auto build_join = probe_input.join(
      build_pipeline,
      [&](const auto& build_arg) -> expression_t {
        return build_arg["b_custkey"];
      },
      [&](const auto& probe_arg) -> expression_t {
        return probe_arg["p_custkey"];
      });

  // Complete the first probe pipeline
  auto probe_one =
      build_join
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Create the second probe pipeline using probeJoin to share the build hash
  // table
  auto probe_two =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe2", "p_custkey"),
                    arg["c_nation"].as("t_probe2", "p_nation")};
          })
          .probeJoin(
              build_join,  // Use the join with HashJoinChained at the root
              [&](const auto& probe_arg) -> expression_t {
                return probe_arg["p_custkey"];
              })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Combine the results of both probe pipelines
  auto union_statement =
      probe_one.unionAll({probe_two})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [join_count, join_sum] = parse_count_and_sum(union_statement.execute());

  // The count should be twice the baseline since we're using the same data
  // twice
  EXPECT_EQ(baseline_count * 2, join_count);
  // The sum should be twice the baseline as well
  EXPECT_EQ(baseline_sum * 2, join_sum);
}

TEST_F(SelfJoinTest, multi_probe_pipelines_shared_HT_multi_thread_morsel) {
  // This test verifies that two probe pipelines can share the same build hash
  // when using a multi-threaded morsel pipeline.

  // First, create a baseline
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["c_custkey"].as("tmp", "c_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["c_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  // Now create a build pipeline with a HashJoinChained operator
  auto rbf_join = getRelBuilderFactory();
  auto build_pipeline =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .router(DegreeOfParallelism{4}, 8, RoutingPolicy::RANDOM,
                  DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_build", "b_custkey"),
                    arg["c_nation"].as("t_build", "b_nation")};
          });

  // Create the first probe pipeline using standard join
  auto probe_input =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .router(DegreeOfParallelism{4}, 8, RoutingPolicy::RANDOM,
                  DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe1", "p_custkey"),
                    arg["c_nation"].as("t_probe1", "p_nation")};
          });

  // Store the RelBuilder with the HashJoinChained at the root
  auto build_join = probe_input.join(
      build_pipeline,
      [&](const auto& build_arg) -> expression_t {
        return build_arg["b_custkey"];
      },
      [&](const auto& probe_arg) -> expression_t {
        return probe_arg["p_custkey"];
      });

  // Complete the first probe pipeline
  auto probe_one =
      build_join
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Create the second probe pipeline using probeJoin to share the build hash
  // table
  auto probe_two =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .router(DegreeOfParallelism{4}, 8, RoutingPolicy::RANDOM,
                  DeviceType::CPU, std::make_unique<CpuNumaNodeAffinitizer>())
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe2", "p_custkey"),
                    arg["c_nation"].as("t_probe2", "p_nation")};
          })
          .probeJoin(
              build_join,  // Use the join with HashJoinChained at the root
              [&](const auto& probe_arg) -> expression_t {
                return probe_arg["p_custkey"];
              })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Combine the results of both probe pipelines
  auto union_statement =
      probe_one.unionAll({probe_two})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [join_count, join_sum] = parse_count_and_sum(union_statement.execute());

  // The count should be twice the baseline since we're using the same data
  // twice
  EXPECT_EQ(baseline_count * 2, join_count);
  // The sum should be twice the baseline as well
  EXPECT_EQ(baseline_sum * 2, join_sum);
}

TEST_F(SelfJoinTest, multi_probe_pipelines_different_probe_payloads_shared_HT) {
  // This test verifies that multiple probe pipelines can share the same build
  // hash table and they all produce correct results.

  // First, create a baseline result that we'll compare against
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/customer.csv",
                {"c_custkey", "c_nation", "c_mktsegment"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["c_custkey"].as("tmp", "c_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["c_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  // Now create a build pipeline that will be shared by multiple probe pipelines
  auto rbf_join = getRelBuilderFactory();
  auto build_pipeline =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv",
                {"c_custkey", "c_nation", "c_mktsegment"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_build", "b_custkey"),
                    arg["c_nation"].as("t_build", "b_nation"),
                    arg["c_mktsegment"].as("t_build", "b_mktsegment")};
          });

  // Create three different probe pipelines that all use the same hash table

  // pipeline 1: Standard filter condition
  // regular HashJoinChained, creates are base join which holds the state
  auto join_one =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe1", "p_custkey"),
                    arg["c_nation"].as("t_probe1", "p_nation")};
          })
          .join(
              build_pipeline,
              [&](const auto& build_arg) -> expression_t {
                return build_arg["b_custkey"];
              },
              [&](const auto& probe_arg) -> expression_t {
                return probe_arg["p_custkey"];
              });
  auto probe_one =
      join_one
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Probe pipeline 2:  Same probe tuple size different subset of columns
  auto probe_two =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_mktsegment"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe2", "p_custkey"),
                    arg["c_mktsegment"].as("t_probe2", "p_mktsegment")};
          })
          .probeJoin(join_one,
                     [&](const auto& probe_arg) -> expression_t {
                       return probe_arg["p_custkey"];
                     })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Probe pipeline 3: Using different subset of columns and different probe
  // tuple size
  auto probe_three =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv",
                {"c_custkey", "c_phone", "c_address"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe3", "p_custkey"),
                    arg["c_phone"].as("t_probe3", "p_phone"),
                    arg["c_address"].as("t_probe3", "p_address")};
          })
          .probeJoin(join_one,
                     [&](const auto& probe_arg) -> expression_t {
                       return probe_arg["p_custkey"];
                     })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Combine the results of all three probe pipelines
  auto union_statement =
      probe_one.unionAll({probe_two, probe_three})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [join_count, join_sum] = parse_count_and_sum(union_statement.execute());

  // The count should be roughly three times the baseline
  EXPECT_EQ(baseline_count * 3, join_count);
  // The sum should be three times the baseline as well
  EXPECT_EQ(baseline_sum * 3, join_sum);
}

TEST_F(SelfJoinTest, multi_probe_different_conditions_shared_HT) {
  // This test verifies that two probe pipelines can share the same build hash
  // table and probe with different key expressions (using project to change the
  // key)

  // First, create a baseline result that we'll compare against
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["c_custkey"].as("tmp", "c_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["c_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  // Now create a build pipeline with a HashJoinChained operator
  auto rbf_join = getRelBuilderFactory();
  auto build_pipeline =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_build", "b_custkey"),
                    arg["c_nation"].as("t_build", "b_nation")};
          });

  // Create the first probe pipeline using standard join
  auto probe_input =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe1", "p_custkey"),
                    arg["c_nation"].as("t_probe1", "p_nation")};
          });

  // Store the RelBuilder with the HashJoinChained at the root
  auto build_join = probe_input.join(
      build_pipeline,
      [&](const auto& build_arg) -> expression_t {
        return build_arg["b_custkey"];
      },
      [&](const auto& probe_arg) -> expression_t {
        return probe_arg["p_custkey"];
      });

  // Complete the first probe pipeline
  auto probe_one =
      build_join
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Create the second probe pipeline using probeJoin to share the build hash
  // table
  auto probe_two =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {(1 + arg["c_custkey"] % 100).as("t_probe2", "p_custkey"),
                    arg["c_nation"].as("t_probe2", "p_nation")};
          })
          .probeJoin(
              build_join,  // Use the join with HashJoinChained at the root
              [&](const auto& probe_arg) -> expression_t {
                return probe_arg["p_custkey"];
              })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["p_custkey"].as("tmp", "p_custkey")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM});

  // Combine the results of both probe pipelines
  auto union_statement =
      probe_one.unionAll({probe_two})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["p_custkey"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [join_count, join_sum] = parse_count_and_sum(union_statement.execute());

  // The count should be twice the baseline since we're using the same data
  // twice
  EXPECT_EQ(baseline_count * 2, join_count);
  // can't check the sum, because we changed the join condition
}

TEST_F(SelfJoinTest, multi_probe_access_different_build_attrs_shared_HT) {
  // This test verifies that two probe pipelines can share the same build
  // hashtable and access different build attributes. We do this by having one
  // pipeline so a sum on one attribute, and the other on a different attribute

  // table First, create a baseline result that we'll compare against
  auto rbf_baseline = getRelBuilderFactory();
  auto baseline_statement =
      rbf_baseline.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {
                expression_t{int64_t{1}}.as("tmp", "cnt"),
                (arg["c_custkey"] * 2 + arg["c_custkey"] * 5).as("tmp", "sum")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();
  auto baseline_res = baseline_statement.execute();
  auto [baseline_count, baseline_sum] = parse_count_and_sum(baseline_res);

  // Now create a build pipeline with a HashJoinChained operator
  auto rbf_join = getRelBuilderFactory();
  auto build_pipeline =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey", "c_nation"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .filter([&](const auto& arg) -> expression_t {
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {
                arg["c_custkey"].as("t_build", "b_custkey"),
                (arg["c_custkey"] * 2).as("t_build", "b_custkey_times_two"),
                (arg["c_custkey"] * 5).as("t_build", "b_custkey_times_five")};
          });

  // Create the first probe pipeline using standard join
  auto probe_input =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe1", "p_custkey")};
          });

  // Store the RelBuilder with the HashJoinChained at the root
  auto build_join = probe_input.join(
      build_pipeline,
      [&](const auto& build_arg) -> expression_t {
        return build_arg["b_custkey"];
      },
      [&](const auto& probe_arg) -> expression_t {
        return probe_arg["p_custkey"];
      });

  // Complete the first probe pipeline
  auto probe_one =
      build_join
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["b_custkey_times_two"].as("tmp", "sum")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM});

  // Create the second probe pipeline using probeJoin to share the build hash
  // table
  auto probe_two =
      rbf_join.getBuilder()
          .scan("inputs/ssbm100/customer.csv", {"c_custkey"},
                CatalogParser::getInstance(), pg{"block"})
          .hintRowCount(100 * 30000)
          .unpack()
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {arg["c_custkey"].as("t_probe2", "p_custkey")};
          })
          .probeJoin(
              build_join,  // Use the join with HashJoinChained at the root
              [&](const auto& probe_arg) -> expression_t {
                return probe_arg["p_custkey"];
              })
          .project([&](const auto& arg) -> std::vector<expression_t> {
            return {expression_t{int64_t{1}}.as("tmp", "cnt"),
                    arg["b_custkey_times_five"].as("tmp", "sum")};
          })
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM});

  // Combine the results of both probe pipelines
  auto union_statement =
      probe_one.unionAll({probe_two})
          .reduce(
              [&](const auto& arg) -> std::vector<expression_t> {
                return {arg["cnt"], arg["sum"]};
              },
              {SUM, SUM})
          .print(pg{"pm-csv"})
          .prepare();

  auto [join_count, join_sum] = parse_count_and_sum(union_statement.execute());

  // The count should be twice the baseline since we're using the same data
  // twice
  EXPECT_EQ(baseline_count * 2, join_count);
  EXPECT_EQ(baseline_sum, join_sum);
  // can't check the sum, because we changed the join condition
}
