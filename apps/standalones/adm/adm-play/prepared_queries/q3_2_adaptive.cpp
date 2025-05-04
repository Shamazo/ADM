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

constexpr auto query = "ssb100_Q3_2";

constexpr int filter_id = 32;

PreparedStatement prepare32_adaptive(QueryArgs args) {
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
                ge(arg["d_year"], 1992) & le(arg["d_year"], 1997),
                expressions::Selectivity{6.0 / 7});
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
            return expressions::hint(eq(arg["c_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_custkey"], arg["c_city"]};
          });

  auto scan_build_supp =
      args.morph->scan("supplier", {"s_suppkey", "s_city", "s_nation"})
          .router(
              DegreeOfParallelism{args.do_bloom_filter_build ? 1 : compute_dop},
              args.morph->getSlack(), RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["s_nation"], "UNITED STATES"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["s_suppkey"], arg["s_city"]};
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
          10, 1024)
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
          10, 1024)
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_city"], arg["s_city"], arg["d_year"],
                    arg["lo_revenue"]};
          },
          {direction::NONE, direction::NONE, direction::ASC, direction::DESC})
      .print(pg{"pm-csv"})
      .prepare();
}
