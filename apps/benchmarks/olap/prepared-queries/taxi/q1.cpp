/*
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
#include <taxi/query.hpp>

PreparedStatement taxi::Query::prepare1(proteus::QueryShaper &morph,
                                        double trip_distance_min,
                                        double trip_distance_max) {
  auto scan =
      morph.scan("yellow_tripdata", {"tpep_pickup_datetime", "trip_distance"});

  auto parallel_groupby =
      morph.parallelize(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return (le(arg["trip_distance"], trip_distance_max) &
                    gt(arg["trip_distance"], trip_distance_min));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["tpep_pickup_datetime"]};
          })
          .groupby(
              [&](const auto &arg) -> std::vector<expression_t> {
                // Original BigQuery query used TIMESTAMP_TRUNC(..., MONTH)
                // Proteus doesn't yet have TIMESTAMP_TRUNC, so we use just
                // extract the year and month
                return {expressions::ExtractExpression{
                            arg["tpep_pickup_datetime"],
                            expressions::extract_unit::YEAR}
                            .as("tmp", "year"),
                        expressions::ExtractExpression{
                            arg["tpep_pickup_datetime"],
                            expressions::extract_unit::MONTH}
                            .as("tmp", "month")};
              },
              [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                return {GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 1, 0,
                                       SUM}};
              },
              8, /*maxInputSize 10 years of 12 months*/ 256);
  return morph.collect(parallel_groupby)
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["year"].as("tmp", "year"),
                    arg["month"].as("tmp", "month")};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["count"], 1, 0, SUM}};
          },
          8, /*maxInputSize 10 years of 12 months*/ 256)
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["year"], arg["month"], arg["count"]};
          },
          {direction::ASC, direction::ASC, direction::NONE})
      .print(pg{"pm-csv"})
      .prepare();
}
