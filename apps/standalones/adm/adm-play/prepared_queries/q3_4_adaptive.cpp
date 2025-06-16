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

constexpr auto query = "ssb100_Q3_4";

constexpr int filter_id = 34;

PreparedStatement prepare34_adaptive(QueryArgs args) {
  args.morph->setQueryName(query);

  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  auto scan_build_date =
      args.morph->scan("date", {"d_datekey", "d_year", "d_yearmonth"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["d_yearmonth"], "Dec1997"),
                                     expressions::Selectivity{1.0 / 84});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_datekey"], arg["d_year"]};
          });

  auto scan_build_cust =
      args.morph->scan("customer", {"c_custkey", "c_city", "c_nation"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["c_city"], "UNITED KI1") |
                                         eq(arg["c_city"], "UNITED KI5"),
                                     expressions::Selectivity{1.0 / 125});
          });

  auto scan_build_supp =
      args.morph->scan("supplier", {"s_suppkey", "s_city"})
          .router(
              DegreeOfParallelism{args.do_bloom_filter_build ? 1 : compute_dop},
              args.morph->getSlack(), RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["s_city"], "UNITED KI1") |
                                         eq(arg["s_city"], "UNITED KI5"),
                                     expressions::Selectivity{1.0 / 125});
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
      "lineorder", {"lo_custkey", "lo_suppkey", "lo_orderdate", "lo_revenue"});

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
            .memmove(4, DeviceType::CPU,
                     std::vector<bool>{false, false, false, false}));
  }

  if (args.do_bloom_filter_pushdown) {
    const size_t mm_slack = std::max(4ul, 32 / args.pushdown_dop);
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
          scan_build_cust,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["c_custkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_custkey"];
          })
      .join(
          scan_build_date,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["d_datekey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_orderdate"];
          })
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_city"].as("tmp", "c_city"),
                    arg["s_city"].as("tmp", "s_city"),
                    arg["d_year"].as("tmp", "d_year")};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["lo_revenue"].as("tmp", "lo_revenue"), 1,
                                   0, SUM}};
          },
          10, 16)
      .pack()
      .router(DegreeOfParallelism{1}, 128, RoutingPolicy::RANDOM,
              DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
      .unpack()
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_city"], arg["s_city"], arg["d_year"]};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["lo_revenue"], 1, 0, SUM}};
          },
          10, 16)
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_city"], arg["s_city"], arg["d_year"],
                    arg["lo_revenue"]};
          },
          {direction::NONE, direction::NONE, direction::ASC, direction::DESC})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement prepare34_adaptive_shared_ht(QueryArgs args) {
  args.morph->setQueryName(query);

  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  auto scan_build_date =
      args.morph->scan("date", {"d_datekey", "d_year", "d_yearmonth"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["d_yearmonth"], "Dec1997"),
                                     expressions::Selectivity{1.0 / 84});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_datekey"], arg["d_year"]};
          });

  auto scan_build_cust =
      args.morph->scan("customer", {"c_custkey", "c_city", "c_nation"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["c_city"], "UNITED KI1") |
                                         eq(arg["c_city"], "UNITED KI5"),
                                     expressions::Selectivity{1.0 / 125});
          });

  auto scan_build_supp =
      args.morph->scan("supplier", {"s_suppkey", "s_city"})
          .router(
              DegreeOfParallelism{args.do_bloom_filter_build ? 1 : compute_dop},
              args.morph->getSlack(), RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["s_city"], "UNITED KI1") |
                                         eq(arg["s_city"], "UNITED KI5"),
                                     expressions::Selectivity{1.0 / 125});
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
      "lineorder", {"lo_custkey", "lo_suppkey", "lo_orderdate", "lo_revenue"});

  std::optional<RelBuilder> supp_join = std::nullopt;
  std::optional<RelBuilder> cust_join = std::nullopt;
  std::optional<RelBuilder> date_join = std::nullopt;

  auto processJoinsAndGroupBy = [&](RelBuilder probe_builder) -> RelBuilder {
    // Case 1: Joins already initialized
    if (supp_join.has_value()) {
      CHECK(cust_join.has_value() && date_join.has_value());
      return probe_builder
          .probeJoin(supp_join.value(),
                     [&](const auto &probe_arg) -> expression_t {
                       return probe_arg["lo_suppkey"];
                     })
          .probeJoin(cust_join.value(),
                     [&](const auto &probe_arg) -> expression_t {
                       return probe_arg["lo_custkey"];
                     })
          .probeJoin(date_join.value(),
                     [&](const auto &probe_arg) -> expression_t {
                       return probe_arg["lo_orderdate"];
                     })
          .groupby(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["c_city"].as("tmp", "c_city"),
                        arg["s_city"].as("tmp", "s_city"),
                        arg["d_year"].as("tmp", "d_year")};
              },
              [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                return {GpuAggrMatExpr{
                    arg["lo_revenue"].as("tmp", "lo_revenue"), 1, 0, SUM}};
              },
              10, 16)
          .pack();
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

      // Initialize cust_join
      cust_join = supp_join.value().join(
          scan_build_cust,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["c_custkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_custkey"];
          });

      // Initialize date_join
      date_join = cust_join.value().join(
          scan_build_date,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["d_datekey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_orderdate"];
          });

      // Return with groupby applied to date_join
      return date_join.value()
          .groupby(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["c_city"].as("tmp", "c_city"),
                        arg["s_city"].as("tmp", "s_city"),
                        arg["d_year"].as("tmp", "d_year")};
              },
              [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                return {GpuAggrMatExpr{
                    arg["lo_revenue"].as("tmp", "lo_revenue"), 1, 0, SUM}};
              },
              10, 16)
          .pack();
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
                                std::vector<bool>{false, false, false, false})
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
      .unionAll({paths.begin() + 1, paths.end()}, DegreeOfParallelism{1},
                std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                    args.compute_numa_nodes),
                128)
      .unpack()
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_city"], arg["s_city"], arg["d_year"]};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["lo_revenue"], 1, 0, SUM}};
          },
          10, 16)
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_city"], arg["s_city"], arg["d_year"],
                    arg["lo_revenue"]};
          },
          {direction::NONE, direction::NONE, direction::ASC, direction::DESC})
      .print(pg{"pm-csv"})
      .prepare();
}
