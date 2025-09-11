/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2019
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

#include "prepared-queries.hpp"

constexpr auto query = "ssb100_Q2_1_pushdown";

constexpr int filter_id = 4221;

PreparedStatement prepare21_pushdown(
    proteus::GPUOnlyNVMeProbeFilterPushdown &morph, bool move_after_pushdown,
    bool do_pushdown) {
  morph.setQueryName(query);

  auto scan_date = morph.scan("date", {"d_datekey", "d_year"});

  auto scan_supp = morph.scan("supplier", {"s_suppkey", "s_nation", "s_region"});

  auto scan_build_part =
      morph.scan("part", {"p_partkey", "p_category", "p_brand1"})
          .router(DegreeOfParallelism{1}, morph.getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  morph.getPushdownAffinitizer())
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["p_category"], "MFGR#12"),
                                     expressions::Selectivity{1.0 / 25});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["p_partkey"], arg["p_brand1"]};
          })
          .bloomfilter_build(
              [&](const auto &arg) -> expression_t { return arg["p_partkey"]; },
              morph.bloom_filter_size, filter_id, morph.pushdown_numa_nodes)
          .pack()
          .to_gpu()
          .unpack();

  auto rel = morph.scan(
      "lineorder", {"lo_partkey", "lo_suppkey", "lo_orderdate", "lo_revenue"});

  return morph
      .parallel(
          rel, {scan_date, scan_supp},
          [&scan_build_part, &move_after_pushdown, &do_pushdown, &morph](
              RelBuilder probe, std::vector<RelBuilder> build) {
            auto scan_build_date = build.at(0).unpack();

            auto scan_build_supp =
                build.at(1)
                    .unpack()
                    .filter([&](const auto &arg) -> expression_t {
                      return expressions::hint(
                          eq(arg["s_region"], "AMERICA"),
                          expressions::Selectivity{1.0 / 5});
                    })
                    .project([&](const auto &arg) -> std::vector<expression_t> {
                      return {arg["s_suppkey"]};
                    });

            RelBuilder filtered_probe = [do_pushdown, &probe, &morph] {
              if (do_pushdown) {
                return probe.unpack()
                    .bloomfilter_probe(
                        [&](const auto &arg) -> expression_t {
                          return arg["lo_partkey"];
                        },
                        morph.bloom_filter_size, filter_id)
                    .pack()
                    // router to convert DOP to main compute dop, e.g. num GPUs
                    .router(morph.getDOP(), morph.getSlack(),
                            RoutingPolicy::LOCAL, morph.getDevice(),
                            morph.getAffinitizer());
              } else {
                return probe.router(morph.getDOP(), morph.getSlack(),
                                    RoutingPolicy::LOCAL, morph.getDevice(),
                                    morph.getAffinitizer());
              }
            }();

            if (move_after_pushdown) {
              filtered_probe = filtered_probe.memmove(16, DeviceType::GPU);
            }
            filtered_probe = filtered_probe.to_gpu();

            return filtered_probe.unpack()
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
                .pack();
          })
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
