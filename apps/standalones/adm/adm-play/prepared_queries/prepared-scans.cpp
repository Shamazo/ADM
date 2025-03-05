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

PreparedStatement small_scan(proteus::QueryShaper &morph,
                             const std::string &lo_column) {
  morph.setQueryName("small_scan");

  auto scan = morph.scan("lineorder", {lo_column});
  auto gpu_pip =
      morph.distribute_probe(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg[lo_column], 1),
                                     expressions::Selectivity(0.000001));
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg[lo_column]};
              },
              {SUM});
  return morph.collect(gpu_pip)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg[lo_column]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_two_columns(proteus::QueryShaper &morph,
                                   const std::string &lo_col1,
                                   const std::string &lo_col2) {
  morph.setQueryName("two_col_scan");

  auto scan = morph.scan("lineorder", {lo_col1, lo_col2});
  auto gpu_pip =
      morph.distribute_probe(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg[lo_col1], 1),
                                     expressions::Selectivity(0.000001));
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg[lo_col1]};
              },
              {SUM});
  return morph.collect(gpu_pip)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg[lo_col1]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_six_columns(proteus::QueryShaper &morph,
                                   const std::string &lo_col1,
                                   const std::string &lo_col2,
                                   const std::string &lo_col3,
                                   const std::string &lo_col4,
                                   const std::string &lo_col5,
                                   const std::string &lo_col6) {
  morph.setQueryName("six_col_scan");

  auto scan = morph.scan(
      "lineorder", {lo_col1, lo_col2, lo_col3, lo_col4, lo_col5, lo_col6});
  auto gpu_pip =
      morph.distribute_probe(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(gt(arg[lo_col1], 1),
                                     expressions::Selectivity(0.000001));
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg[lo_col1]};
              },
              {SUM});
  return morph.collect(gpu_pip)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg[lo_col1]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}
