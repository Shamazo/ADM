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

constexpr auto query = "ssb_Q1_1";
constexpr int filter_id = 11;

RelBuilder add_direct_path(SplitRelBuilder &split, size_t compute_dop,
                           const std::vector<uint32_t> &compute_numa_nodes) {
  return split
      .path(
          DeviceType::CPU, DegreeOfParallelism{compute_dop},
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(compute_numa_nodes))
      .memmove(2, DeviceType::CPU)
      .unpack()
      .filter([&](const auto &arg) -> expression_t {
        return expressions::hint(ge(arg["lo_discount"], 1) &
                                     le(arg["lo_discount"], 3) &
                                     lt(arg["lo_quantity"], 25),
                                 expressions::Selectivity(0.5 * 3.0 / 11));
      })
      .pack();
}

RelBuilder add_staging_path(SplitRelBuilder &split, size_t compute_dop,
                            const std::vector<uint32_t> &compute_numa_nodes) {
  return split
      .path(
          DeviceType::CPU, DegreeOfParallelism{compute_dop},
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(compute_numa_nodes))
      .memmove(2, DeviceType::CPU,
               std::vector<bool>{false, false, false, false})
      .unpack()
      .filter([&](const auto &arg) -> expression_t {
        return expressions::hint(ge(arg["lo_discount"], 1) &
                                     le(arg["lo_discount"], 3) &
                                     lt(arg["lo_quantity"], 25),
                                 expressions::Selectivity(0.5 * 3.0 / 11));
      })
      .pack();
}

RelBuilder add_pushdown_path(SplitRelBuilder &split, size_t pushdown_dop,
                             const std::vector<uint32_t> &pushdown_numa_nodes,
                             bool do_bloomfilter, size_t bloom_filter_size) {
  auto filter = split
                    .path(DeviceType::CPU, DegreeOfParallelism{pushdown_dop},
                          std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                              pushdown_numa_nodes))
                    .memmove(8, DeviceType::CPU)
                    .unpack();
  if (do_bloomfilter) {
    filter = filter.bloomfilter_probe(
        [&](const auto &arg) -> expression_t { return arg["lo_orderdate"]; },
        bloom_filter_size, filter_id);
  }
  return filter
      .filter([&](const auto &arg) -> expression_t {
        return expressions::hint(ge(arg["lo_discount"], 1) &
                                     le(arg["lo_discount"], 3) &
                                     lt(arg["lo_quantity"], 25),
                                 expressions::Selectivity(0.5 * 3.0 / 11));
      })
      .pack();
}

PreparedStatement prepare11_adaptive(SSBArgs args) {
  args.morph->setQueryName(query);
  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  auto scan_build = args.morph->scan("date", {"d_datekey", "d_year"});

  auto scan_probe = args.morph->scan(
      "lineorder",
      {"lo_orderdate", "lo_quantity", "lo_extendedprice", "lo_discount"});

  auto build_pipeline =
      scan_build
          .router(
              DegreeOfParallelism{args.do_bloom_filter_build ? 1 : compute_dop},
              args.morph->getSlack(), RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["d_year"], 1993),
                                     expressions::Selectivity(1.0 / 7));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {(arg["d_datekey"])};
          });

  if (args.do_bloom_filter_build) {
    build_pipeline =
        build_pipeline
            .bloomfilter_build(
                [&](const auto &arg) -> expression_t {
                  return arg["d_datekey"];
                },
                args.bloom_filter_size, filter_id, args.pushdown_numa_nodes)
            .pack()
            .router(DegreeOfParallelism{compute_dop}, 4, RoutingPolicy::LOCAL,
                    DeviceType::CPU,
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
            .unpack();
  }

  auto probe_split = scan_probe.gsplit(
      args.scan_slack, args.policy, args.num_samples, args.skip_first_samples);

  std::vector<RelBuilder> paths;
  if (args.do_direct) {
    paths.emplace_back(
        add_direct_path(probe_split, compute_dop, args.compute_numa_nodes));
  }

  if (args.do_staging) {
    paths.emplace_back(
        add_staging_path(probe_split, compute_dop, args.compute_numa_nodes));
  }

  if (args.do_filter_pushdown) {
    paths.emplace_back(add_pushdown_path(
        probe_split, args.pushdown_dop, args.pushdown_numa_nodes,
        args.do_bloom_filter_pushdown, args.bloom_filter_size));
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
          build_pipeline,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["d_datekey"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg["lo_orderdate"];
          })
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {(arg["lo_extendedprice"] * arg["lo_discount"])
                        .as("tmp", "revenue")};
          },
          {SUM})
      .router(DegreeOfParallelism{1}, 64, RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["revenue"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}
