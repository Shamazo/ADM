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

#include "prepared-queries.hpp"

constexpr auto query = "ssb100_Q1_2_pushdown";

PreparedStatement prepare12_pushdown(proteus::QueryShaper &morph,
                                     bool move_after_pushdown) {
  morph.setQueryName(query);

  auto rel4483 = morph.scan("date", {"d_datekey", "d_yearmonthnum"});

  auto rel = morph.scan("lineorder", {"lo_orderdate", "lo_quantity",
                                      "lo_extendedprice", "lo_discount"});

  return morph
      .parallel(
          rel, {rel4483},
          [&morph, &move_after_pushdown](RelBuilder probe,
                                         std::vector<RelBuilder> build) {
            auto rel4483_d =
                build.at(0)
                    .unpack()
                    .filter([&](const auto &arg) -> expression_t {
                      return expressions::hint(
                          eq(arg["d_yearmonthnum"], 199401),
                          expressions::Selectivity{1.0 / 84});
                    })
                    .project([&](const auto &arg) -> std::vector<expression_t> {
                      return {(arg["d_datekey"])};
                    });

            auto filtered_probe =
                probe.unpack()
                    .filter([&](const auto &arg) -> expression_t {
                      return expressions::hint(
                          ge(arg["lo_discount"], 4) &
                              le(arg["lo_discount"], 6) &
                              ge(arg["lo_quantity"], 26) &
                              le(arg["lo_quantity"], 35),
                          expressions::Selectivity{0.2 * 3.0 / 11});
                    })
                    .pack()
                    .router(morph.getDOP(), morph.getSlack(),
                            RoutingPolicy::LOCAL, morph.getDevice(),
                            morph.getAffinitizer());
            if (move_after_pushdown || morph.getDevice() == DeviceType::GPU) {
              filtered_probe =
                  filtered_probe.memmove(morph.getSlack(), morph.getDevice());
            }
            if (morph.getDevice() == DeviceType::GPU) {
              filtered_probe = filtered_probe.to_gpu();
            }

            return filtered_probe.unpack()
                .join(
                    rel4483_d,
                    [&](const auto &build_arg) -> expression_t {
                      return build_arg["d_datekey"];
                    },
                    [&](const auto &probe_arg) -> expression_t {
                      return probe_arg["lo_orderdate"];
                    },
                    6, 32)
                .reduce(
                    [&](const auto &arg) -> std::vector<expression_t> {
                      return {(arg["lo_extendedprice"] * arg["lo_discount"])
                                  .as("tmp", "revenue")};
                    },
                    {SUM});
          })
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["revenue"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}
