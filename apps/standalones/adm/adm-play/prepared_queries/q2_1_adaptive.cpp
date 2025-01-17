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

#include <query-shaping/nvme-shapers.hpp>

#include "prepared-queries.hpp"

constexpr auto query = "ssb100_Q2_1";

constexpr int filter_id = 21;

PreparedStatement prepare21_adaptive(SSBArgs args) {
  args.morph->setQueryName(query);

  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  const auto scan_build_date =
      args.morph->scan("date", {"d_datekey", "d_year"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack();

  const auto scan_build_supp =
      args.morph->scan("supplier", {"s_suppkey", "s_nation", "s_region"})
          .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["s_region"], "AMERICA"),
                                     expressions::Selectivity{1.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["s_suppkey"]};
          });

  auto scan_build_part =
      args.morph->scan("part", {"p_partkey", "p_category", "p_brand1"})
          .router(
              DegreeOfParallelism{args.do_bloom_filter_build ? 1 : compute_dop},
              args.morph->getSlack(), RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["p_category"], "MFGR#12"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["p_partkey"], arg["p_brand1"]};
          });

  if (args.do_bloom_filter_build) {
    scan_build_part =
        scan_build_part
            .bloomfilter_build(
                [&](const auto &arg) -> expression_t {
                  return arg["p_partkey"];
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
      "lineorder", {"lo_partkey", "lo_suppkey", "lo_orderdate", "lo_revenue"});

  auto probe_split = scan_probe.gsplit(
      args.scan_slack, args.policy, args.num_samples, args.skip_first_samples);
  std::vector<RelBuilder> paths;

  if (args.do_staging) {
    paths.emplace_back(
        probe_split
            .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
            .memmove(4, DeviceType::CPU,
                     std::vector<bool>{false, false, false, false}));
  }

  if (args.do_bloom_filter_pushdown) {
    paths.emplace_back(
        probe_split
            .path(DeviceType::CPU, DegreeOfParallelism{args.pushdown_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.pushdown_numa_nodes))
            .memmove(8, DeviceType::CPU)
//            .bloomfilter_repack(
//                [&](const auto &arg) -> expression_t {
//                  return arg["lo_partkey"];
//                },
//                args.bloom_filter_size, filter_id));
            .unpack()
            .bloomfilter_probe(
                [&](const auto &arg) -> expression_t {
                  return arg["lo_partkey"];
                },
                args.bloom_filter_size, filter_id)
            .pack());
  }

  if (args.do_direct) {
    paths.emplace_back(
        probe_split
            .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
            .memmove(4, DeviceType::CPU));
  }

  CHECK_GT(paths.size(), 0) << "Cannot have a plan with with no paths";

  auto first_path = paths.front();
  return first_path
      .unionAll({paths.begin() + 1, paths.end()},
                DegreeOfParallelism{compute_dop},
                std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                    args.compute_numa_nodes),
                4)
      .unpack()
      .join(
          scan_build_part,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["p_partkey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_partkey"];
          })
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
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_year"].as("PelagoProject#11438", "d_year"),
                    arg["p_brand1"].as("PelagoProject#11438", "p_brand1")};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{
                arg["lo_revenue"].as("PelagoProject#11438", "EXPR$0"), 1, 0,
                SUM}};
          },
          10, 512)
      .pack()
      .router(DegreeOfParallelism{1}, 64, RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
      .unpack()
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["d_year"], arg["p_brand1"]};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["EXPR$0"], 1, 0, SUM}};
          },
          10, 512)
      .project([&](const auto &arg) -> std::vector<expression_t> {
        return {arg["EXPR$0"], arg["d_year"], arg["p_brand1"]};
      })
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["EXPR$0"], arg["d_year"], arg["p_brand1"]};
          },
          {direction::NONE, direction::ASC, direction::ASC})
      .print(pg{"pm-csv"})
      .prepare();
}
