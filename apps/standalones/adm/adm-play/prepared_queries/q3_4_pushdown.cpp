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

constexpr auto query = "ssb100_Q3_4_pushdown";

constexpr int filter_id = 4234;

PreparedStatement prepare34_pushdown(
    proteus::GPUOnlyNVMeProbeFilterPushdown &morph, bool move_after_pushdown,
    bool do_pushdown) {
  morph.setQueryName(query);

  auto rel44853 = morph.scan("date", {"d_datekey", "d_year", "d_yearmonth"});
  auto rel44857 = morph.scan("customer", {"c_custkey", "c_city"});
  
  auto scan_build_supp =
      morph.scan("supplier", {"s_suppkey", "s_city"})
          .router(DegreeOfParallelism{1}, morph.getSlack(),
                  RoutingPolicy::LOCAL, DeviceType::CPU,
                  morph.getPushdownAffinitizer())
          .memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["s_city"], "UNITED KI1") |
                                         eq(arg["s_city"], "UNITED KI5"),
                                     expressions::Selectivity{1.0 / 125});
          })
          .bloomfilter_build(
              [&](const auto &arg) -> expression_t { return arg["s_suppkey"]; },
              morph.bloom_filter_size, filter_id, morph.pushdown_numa_nodes)
          .pack()
          .to_gpu()
          .unpack();
  
  auto rel = morph.scan(
      "lineorder", {"lo_custkey", "lo_suppkey", "lo_orderdate", "lo_revenue"});

  return morph
      .parallel(
          rel, {rel44853, rel44857},
          [&scan_build_supp, &move_after_pushdown, &do_pushdown, &morph](
              RelBuilder probe, std::vector<RelBuilder> build) {
            auto rel44853_d =
                build.at(0)
                    .unpack()
                    .filter([&](const auto &arg) -> expression_t {
                      return expressions::hint(
                          eq(arg["d_yearmonth"], "Dec1997"),
                          expressions::Selectivity{1.0 / 84});
                    })
                    .project([&](const auto &arg) -> std::vector<expression_t> {
                      return {arg["d_datekey"], arg["d_year"]};
                    });

            auto rel44857_d = build.at(1).unpack().filter(
                [&](const auto &arg) -> expression_t {
                  return expressions::hint(eq(arg["c_city"], "UNITED KI1") |
                                               eq(arg["c_city"], "UNITED KI5"),
                                           expressions::Selectivity{1.0 / 125});
                });

            RelBuilder filtered_probe = [do_pushdown, &probe, &morph] {
              if (do_pushdown) {
                return probe.unpack()
                    .bloomfilter_probe(
                        [&](const auto &arg) -> expression_t {
                          return arg["lo_suppkey"];
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
                    scan_build_supp,
                    [&](const auto &build_arg) -> expression_t {
                      return build_arg["s_suppkey"];
                    },
                    [&](const auto &probe_arg) -> expression_t {
                      return probe_arg["lo_suppkey"];
                    })
                .join(
                    rel44857_d,
                    [&](const auto &build_arg) -> expression_t {
                      return build_arg["c_custkey"];
                    },
                    [&](const auto &probe_arg) -> expression_t {
                      return probe_arg["lo_custkey"];
                    })
                .join(
                    rel44853_d,
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
                      return {GpuAggrMatExpr{
                          arg["lo_revenue"].as("tmp", "lo_revenue"), 1, 0,
                          SUM}};
                    },
                    10, 16)
                .pack();
          })
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
