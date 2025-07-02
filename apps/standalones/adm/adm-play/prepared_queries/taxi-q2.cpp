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

expression_t time_delta_seconds(const expression_t &lhs,
                                const expression_t &rhs) {
  constexpr int64_t ms_in_s = 1000ll;
  return (rhs - lhs).template as<Int64Type>() / ms_in_s;
}

PreparedStatement prepare_taxi_2_adaptive(QueryArgs &args,
                                          double fare_amount_min,
                                          double fare_amount_max) {
  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  auto scan = args.morph->scan(
      "yellow_tripdata", {"trip_distance", "fare_amount",
                          "tpep_dropoff_datetime", "tpep_pickup_datetime"});

  auto split = scan.gsplit_v2(args.scan_slack, args.policy);
  std::vector<RelBuilder> paths;

  if (args.do_staging) {
    paths.emplace_back(
        split
            .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
            .memmove(2, DeviceType::CPU,
                     std::vector<bool>{false, false, false, false})
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return (le(arg["fare_amount"], fare_amount_max) &
                      ge(arg["fare_amount"], fare_amount_min));
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {arg["trip_distance"], arg["tpep_dropoff_datetime"],
                      arg["tpep_pickup_datetime"]};
            })
            .filter([&](const auto &arg) -> expression_t {
              return lt(arg["tpep_pickup_datetime"],
                        arg["tpep_dropoff_datetime"]);
            })
            .filter([&](const auto &arg) -> expression_t {
              return gt(arg["trip_distance"], 0.0);
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
              return (le(arg["fare_amount"], fare_amount_max) &
                      ge(arg["fare_amount"], fare_amount_min));
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {arg["trip_distance"], arg["tpep_dropoff_datetime"],
                      arg["tpep_pickup_datetime"]};
            })
            .filter([&](const auto &arg) -> expression_t {
              return lt(arg["tpep_pickup_datetime"],
                        arg["tpep_dropoff_datetime"]);
            })
            .filter([&](const auto &arg) -> expression_t {
              return gt(arg["trip_distance"], 0.0);
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
              return (le(arg["fare_amount"], fare_amount_max) &
                      ge(arg["fare_amount"], fare_amount_min));
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {arg["trip_distance"], arg["tpep_dropoff_datetime"],
                      arg["tpep_pickup_datetime"]};
            })
            .filter([&](const auto &arg) -> expression_t {
              return lt(arg["tpep_pickup_datetime"],
                        arg["tpep_dropoff_datetime"]);
            })
            .filter([&](const auto &arg) -> expression_t {
              return gt(arg["trip_distance"], 0.0);
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
            return {expressions::ExtractExpression{
                arg["tpep_pickup_datetime"],
                expressions::extract_unit::DAYOFWEEK}
                        .as("tmp", "dayofweek")};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {
                GpuAggrMatExpr{
                    (arg["trip_distance"] /
                     (time_delta_seconds(arg["tpep_pickup_datetime"],
                                         arg["tpep_dropoff_datetime"])
                          .template as<FloatType>()))
                        .as("tmp", "speed_mps"),
                    1, 0, SUM},
                GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 2, 0, SUM}};
          },
          4, 8)
      .router(DegreeOfParallelism{1}, 64, RoutingPolicy::LOCAL, DeviceType::CPU,
              std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                  args.compute_numa_nodes))
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["dayofweek"]};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {GpuAggrMatExpr{arg["speed_mps"], 1, 0, SUM},
                    GpuAggrMatExpr{arg["count"], 2, 0, SUM}};
          },
          4, 8)
      .project([&](const auto &arg) -> std::vector<expression_t> {
        double seconds_in_hour = 3600;
        return {arg["dayofweek"],
                (arg["speed_mps"] / (arg["count"].template as<FloatType>()) *
                 seconds_in_hour)
                    .as("tmp", "avg_speed_mph")};
      })
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["dayofweek"], arg["avg_speed_mph"]};
          },
          {direction::ASC, direction::NONE})
      .print(pg{"pm-csv"})
      .prepare();
}
