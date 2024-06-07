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

#include "prepared-queries.hpp"

PreparedStatement scan_sum_micro_pushdown(proteus::QueryShaper &morph,
                                          double selectivity,
                                          bool move_after_pushdown) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro_pushdown");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "Query upperbound: " << query_upperbound;

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto filter_pip =
      morph.distribute_probe(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .pack()
          .router(morph.getDOP(), morph.getSlack(), RoutingPolicy::LOCAL,
                  morph.getDevice(), morph.getAffinitizer());
  if (move_after_pushdown || morph.getDevice() == DeviceType::GPU) {
    filter_pip = filter_pip.memmove(morph.getSlack(), morph.getDevice());
  }
  if (morph.getDevice() == DeviceType::GPU) {
    filter_pip = filter_pip.to_gpu();
  }

  auto reduce_threads = filter_pip.unpack().reduce(
      [&](const auto &arg) -> std::vector<expression_t> {
        return {arg["col2"]};
      },
      {SUM});

  // final reduction after per thread partial reduction
  return morph.collect(reduce_threads)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_sum_micro(proteus::QueryShaper &morph,
                                 double selectivity) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "query_upperbound: " << query_upperbound;

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto filter_sum_pip =
      morph.distribute_probe(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["col2"]};
              },
              {SUM});

  // final reduction after per thread partial reduction
  return morph.collect(filter_sum_pip)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}
