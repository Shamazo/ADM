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

PreparedStatement prepare_taxi_1_adaptive(QueryArgs &args,
                                          double trip_distance_min,
                                          double trip_distance_max) {
  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  auto scan = args.morph->scan("yellow_tripdata",
                               {"tpep_pickup_datetime", "trip_distance"});

  auto split = scan.gsplit(args.scan_slack, args.policy, args.num_samples,
                           args.skip_first_samples);
  std::vector<RelBuilder> paths;

  if (args.do_staging) {
    paths.emplace_back(
        split
            .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
            .memmove(2, DeviceType::CPU, std::vector<bool>{false, false})
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return (le(arg["trip_distance"], trip_distance_max) &
                      gt(arg["trip_distance"], trip_distance_min));
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {(arg["tpep_pickup_datetime"])};
            })
            .pack());
  }

  if (args.do_direct) {
    paths.emplace_back(
        split
            .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
            .memmove(2, DeviceType::CPU)
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return (le(arg["trip_distance"], trip_distance_max) &
                      gt(arg["trip_distance"], trip_distance_min));
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {arg["tpep_pickup_datetime"]};
            })
            .pack());
  }

  if (args.do_filter_pushdown) {
    const size_t mm_slack = std::max(4ul, 32 / args.pushdown_dop);
    paths.emplace_back(
        split
            .path(DeviceType::CPU, DegreeOfParallelism{args.pushdown_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.pushdown_numa_nodes))
            .memmove(mm_slack, DeviceType::CPU)
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return (le(arg["trip_distance"], trip_distance_max) &
                      gt(arg["trip_distance"], trip_distance_min));
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {(arg["tpep_pickup_datetime"])};
            })
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
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            // Original BigQuery query used TIMESTAMP_TRUNC(..., MONTH)
            // Proteus doesn't yet have TIMESTAMP_TRUNC, so we use just
            // extract the year and month
            return {
                expressions::ExtractExpression{arg["tpep_pickup_datetime"],
                                               expressions::extract_unit::YEAR}
                    .as("tmp", "year"),
                expressions::ExtractExpression{arg["tpep_pickup_datetime"],
                                               expressions::extract_unit::MONTH}
                    .as("tmp", "month")};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {
                GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 1, 0, SUM}};
          },
          8, /*maxInputSize 10 years of 12 months*/ 256)
      .router(DegreeOfParallelism{1}, 64, RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            // Original BigQuery query used TIMESTAMP_TRUNC(..., MONTH)
            // Proteus doesn't yet have TIMESTAMP_TRUNC, so we use just
            // extract the year and month
            return {arg["year"], arg["month"]};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["count"], 1, 0, SUM}};
          },
          8, /*maxInputSize 14 years of 12 months*/ 256)
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["year"], arg["month"], arg["count"]};
          },
          {direction::ASC, direction::ASC, direction::NONE})
      .print(pg{"pm-csv"})
      .prepare();
}
