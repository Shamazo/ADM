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

#include <query-shaping/nvme-shapers.hpp>

#include "prepared-queries.hpp"

constexpr auto query = "ssb100_Q4_2";

constexpr int filter_id = 42;

PreparedStatement prepare42_adaptive(QueryArgs args) {
  args.morph->setQueryName(query);

  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  auto scan_build_date =
      args.morph->scan("date", {"d_datekey", "d_year"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(
                eq(arg["d_year"], 1997) | eq(arg["d_year"], 1998),
                expressions::Selectivity{2.0 / 7});
          });

  auto scan_build_cust =
      args.morph->scan("customer", {"c_custkey", "c_region"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["c_region"], "AMERICA"),
                                     expressions::Selectivity{1.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_custkey"]};
          });

  auto scan_build_part =
      args.morph->scan("part", {"p_partkey", "p_mfgr", "p_category"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(
                eq(arg["p_mfgr"], "MFGR#1") | eq(arg["p_mfgr"], "MFGR#2"),
                expressions::Selectivity{2.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["p_partkey"], arg["p_category"]};
          });

  auto scan_build_supp =
      args.morph->scan("supplier", {"s_suppkey", "s_region", "s_nation"})
          .router(
              DegreeOfParallelism{args.do_bloom_filter_build ? 1 : compute_dop},
              args.morph->getSlack(), RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["s_region"], "AMERICA"),
                                     expressions::Selectivity{1.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["s_suppkey"], arg["s_nation"]};
          });

  if (args.do_bloom_filter_build) {
    scan_build_supp =
        scan_build_supp
            .bloomfilter_build(
                [&](const auto &arg) -> expression_t {
                  return arg["s_suppkey"];
                },
                args.bloom_filter_size, filter_id, args.pushdown_numa_nodes)
            .pack()
            .router(DegreeOfParallelism{compute_dop}, 4, RoutingPolicy::LOCAL,
                    DeviceType::CPU,
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
            .unpack();
  }

  auto scan_probe = args.morph->scan(
      "lineorder", {"lo_custkey", "lo_partkey", "lo_suppkey", "lo_orderdate",
                    "lo_revenue", "lo_supplycost"});

  auto probe_split = scan_probe.gsplit(
      args.scan_slack, args.policy, args.num_samples, args.skip_first_samples);
  std::vector<RelBuilder> paths;
  const size_t mm_slack = 96 / compute_dop;
  if (args.do_direct) {
    paths.emplace_back(
        probe_split
            .path(DeviceType::CPU, DegreeOfParallelism{16},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
            .memmove(4, DeviceType::CPU));
  }

  if (args.do_staging) {
    paths.emplace_back(
        probe_split
            .path(DeviceType::CPU, DegreeOfParallelism{16},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
            .memmove(
                4, DeviceType::CPU,
                std::vector<bool>{false, false, false, false, false, false}));
  }

  if (args.do_bloom_filter_pushdown) {
    const size_t mm_slack = std::max(4ul, 32/args.pushdown_dop);
    paths.emplace_back(
        probe_split
            .path(DeviceType::CPU, DegreeOfParallelism{args.pushdown_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.pushdown_numa_nodes))
            .memmove(mm_slack, DeviceType::CPU)
            .unpack()
            .bloomfilter_probe(
                [&](const auto &arg) -> expression_t {
                  return arg["lo_suppkey"];
                },
                args.bloom_filter_size, filter_id)
            .pack());
  }

  CHECK_GT(paths.size(), 0) << "Cannot have a plan with with no paths";

  auto first_path = paths.front();
  return first_path
      .unionAll({paths.begin() + 1, paths.end()},
                DegreeOfParallelism{compute_dop},
                std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                    args.compute_numa_nodes),
                2)
      .unpack()
      .join(
          scan_build_supp,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["s_suppkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_suppkey"];
          })
      .join(
          scan_build_date,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["d_datekey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_orderdate"];
          })
      .join(
          scan_build_cust,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["c_custkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_custkey"];
          })
      .join(
          scan_build_part,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["p_partkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_partkey"];
          })
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_year"].as("tmp", "d_year"),
                    arg["s_nation"].as("tmp", "s_nation"),
                    arg["p_category"].as("tmp", "p_category")};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{
                (arg["lo_revenue"] - arg["lo_supplycost"]).as("tmp", "profit"),
                1, 0, SUM}};
          },
          10, 128)
      .pack()
      .router(DegreeOfParallelism{1}, 128, RoutingPolicy::RANDOM,
              DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
      .unpack()
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_year"], arg["s_nation"], arg["p_category"]};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["profit"], 1, 0, SUM}};
          },
          10, 128)
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_year"], arg["s_nation"], arg["p_category"],
                    arg["profit"]};
          },
          {direction::ASC, direction::ASC, direction::ASC, direction::NONE})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement prepare42_adaptive_shared_ht(QueryArgs args) {
  args.morph->setQueryName(query);

  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  auto scan_build_date =
      args.morph->scan("date", {"d_datekey", "d_year"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(
                eq(arg["d_year"], 1997) | eq(arg["d_year"], 1998),
                expressions::Selectivity{2.0 / 7});
          });

  auto scan_build_cust =
      args.morph->scan("customer", {"c_custkey", "c_region"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["c_region"], "AMERICA"),
                                     expressions::Selectivity{1.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_custkey"]};
          });

  auto scan_build_part =
      args.morph->scan("part", {"p_partkey", "p_mfgr", "p_category"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(
                eq(arg["p_mfgr"], "MFGR#1") | eq(arg["p_mfgr"], "MFGR#2"),
                expressions::Selectivity{2.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["p_partkey"], arg["p_category"]};
          });

  auto scan_build_supp =
      args.morph->scan("supplier", {"s_suppkey", "s_region", "s_nation"})
          .router(
              DegreeOfParallelism{args.do_bloom_filter_build ? 1 : compute_dop},
              args.morph->getSlack(), RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["s_region"], "AMERICA"),
                                     expressions::Selectivity{1.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["s_suppkey"], arg["s_nation"]};
          });

  if (args.do_bloom_filter_build) {
    scan_build_supp =
        scan_build_supp
            .bloomfilter_build(
                [&](const auto &arg) -> expression_t {
                  return arg["s_suppkey"];
                },
                args.bloom_filter_size, filter_id, args.pushdown_numa_nodes)
            .pack()
            .router(DegreeOfParallelism{compute_dop}, 4, RoutingPolicy::LOCAL,
                    DeviceType::CPU,
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
            .unpack();
  }

  auto scan_probe = args.morph->scan(
      "lineorder", {"lo_custkey", "lo_partkey", "lo_suppkey", "lo_orderdate",
                    "lo_revenue", "lo_supplycost"});

  // Optional join builders - will be initialized when the first path is processed
  std::optional<RelBuilder> supp_join = std::nullopt;
  std::optional<RelBuilder> date_join = std::nullopt;
  std::optional<RelBuilder> cust_join = std::nullopt;
  std::optional<RelBuilder> part_join = std::nullopt;

  // Lambda to process joins and groupby
  auto processJoinsAndGroupBy = [&](RelBuilder probe_builder) -> RelBuilder {
    // Case 1: Joins already initialized, use probeJoin
    if (supp_join.has_value()) {
      CHECK(date_join.has_value() && cust_join.has_value() && part_join.has_value());
      return probe_builder
          .probeJoin(supp_join.value(),
                     [&](const auto &probe_arg) -> expression_t {
                       return probe_arg["lo_suppkey"];
                     })
          .probeJoin(date_join.value(),
                     [&](const auto &probe_arg) -> expression_t {
                       return probe_arg["lo_orderdate"];
                     })
          .probeJoin(cust_join.value(),
                     [&](const auto &probe_arg) -> expression_t {
                       return probe_arg["lo_custkey"];
                     })
          .probeJoin(part_join.value(),
                     [&](const auto &probe_arg) -> expression_t {
                       return probe_arg["lo_partkey"];
                     })
          .groupby(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["d_year"].as("tmp", "d_year"),
                        arg["s_nation"].as("tmp", "s_nation"),
                        arg["p_category"].as("tmp", "p_category")};
              },
              [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                return {GpuAggrMatExpr{
                    (arg["lo_revenue"] - arg["lo_supplycost"]).as("tmp", "profit"),
                    1, 0, SUM}};
              },
              10, 128).pack();
    }
    // Case 2: Initialize joins
    else {
      // Initialize supp_join
      supp_join = probe_builder.join(
          scan_build_supp,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["s_suppkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_suppkey"];
          });

      // Initialize date_join
      date_join = supp_join.value().join(
          scan_build_date,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["d_datekey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_orderdate"];
          });

      // Initialize cust_join
      cust_join = date_join.value().join(
          scan_build_cust,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["c_custkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_custkey"];
          });

      // Initialize part_join
      part_join = cust_join.value().join(
          scan_build_part,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["p_partkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_partkey"];
          });

      // Return with groupby applied to part_join
      return part_join.value().groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_year"].as("tmp", "d_year"),
                    arg["s_nation"].as("tmp", "s_nation"),
                    arg["p_category"].as("tmp", "p_category")};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{
                (arg["lo_revenue"] - arg["lo_supplycost"]).as("tmp", "profit"),
                1, 0, SUM}};
          },
          10, 128).pack();
    }
  };

  auto probe_split = scan_probe.gsplit(
      args.scan_slack, args.policy, args.num_samples, args.skip_first_samples);
  std::vector<RelBuilder> paths;

  const size_t mm_slack = 96 / compute_dop;
  if (args.do_direct) {
    auto direct = probe_split
                      .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                            std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                                args.compute_numa_nodes))
                      .memmove(mm_slack, DeviceType::CPU)
                      .unpack();
    paths.emplace_back(processJoinsAndGroupBy(direct));
  }

  if (args.do_staging) {
    auto staging = probe_split
                       .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                             std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                                 args.compute_numa_nodes))
                       .memmove(mm_slack, DeviceType::CPU,
                                std::vector<bool>{false, false, false, false, false, false})
                       .unpack();
    paths.emplace_back(processJoinsAndGroupBy(staging));
  }

  if (args.do_bloom_filter_pushdown) {
    const size_t pd_mm_slack = std::max(4ul, 32 / args.pushdown_dop);
    auto pd = probe_split
                  .path(DeviceType::CPU, DegreeOfParallelism{args.pushdown_dop},
                        std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                            args.pushdown_numa_nodes))
                  .memmove(pd_mm_slack, DeviceType::CPU)
                  .unpack()
                  .bloomfilter_probe(
                      [&](const auto &arg) -> expression_t {
                        return arg["lo_suppkey"];
                      },
                      args.bloom_filter_size, filter_id)
                  .pack()
                  .router(DegreeOfParallelism{compute_dop}, 2,
                          RoutingPolicy::RANDOM, DeviceType::CPU,
                          std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                              args.compute_numa_nodes))
                  .unpack();
    paths.emplace_back(processJoinsAndGroupBy(pd));
  }

  CHECK_GT(paths.size(), 0) << "Cannot have a plan with with no paths";

  auto first_path = paths.front();
  return first_path
      .unionAll({paths.begin() + 1, paths.end()},
                DegreeOfParallelism{1},
                std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                    args.compute_numa_nodes),
                128)
      .unpack()
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_year"], arg["s_nation"], arg["p_category"]};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["profit"], 1, 0, SUM}};
          },
          10, 128)
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_year"], arg["s_nation"], arg["p_category"],
                    arg["profit"]};
          },
          {direction::ASC, direction::ASC, direction::ASC, direction::NONE})
      .print(pg{"pm-csv"})
      .prepare();
}
