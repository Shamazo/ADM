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

#include <magic_enum.hpp>

#include "prepared-queries.hpp"

PreparedStatement prepare_taxi_3_adaptive(QueryArgs &args,
                                          TaxiQueryType query_variant) {
  auto &topo = topology::getInstance();
  const auto compute_dop =
      args.compute_numa_nodes.size() *
      topo.getCpuNumaNodeById(args.compute_numa_nodes.at(0))
          .local_cores.size() /
      (args.use_hyper_threads ? 1 : 2);

  const std::string DOLocationID = query_variant == TaxiQueryType::Q33_synth_cols ||
                                   query_variant == TaxiQueryType::Q32_synth_cols
                                ? "DOLocationID_synthetic"
                                : "DOLocationID";
  const std::string payment_type = query_variant == TaxiQueryType::Q33_synth_cols ||
                                   query_variant == TaxiQueryType::Q32_synth_cols
                                 ? "payment_type_synthetic"
                                 : "payment_type";

  auto scan =  args.morph->scan(
    "yellow_tripdata",
    {"trip_distance", "fare_amount", "tip_amount", "total_amount", "mta_tax",
    DOLocationID, "tpep_dropoff_datetime", "tpep_pickup_datetime",
    payment_type});

  // "LaGuardia Airport"
  // "Newark Airport"

  // "Brooklyn"
  auto scan_zone = [&]() {
    switch (query_variant) {
      case TaxiQueryType::Q31:
        return args.morph->scan("zone_lookup", {"l_LocationID", "l_Zone"})
            .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                    RoutingPolicy::LOCAL, DeviceType::CPU,
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
            .memmove(4, DeviceType::CPU)
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return expressions::hint(eq(arg["l_Zone"], "\"JFK Airport\""),
                                       expressions::Selectivity{1.0 / 10.0});
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {arg["l_LocationID"]};
            });
      case TaxiQueryType::Q32_synth_cols:
      case TaxiQueryType::Q32:
        return args.morph->scan("zone_lookup", {"l_LocationID", "l_Zone"})
            .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                    RoutingPolicy::LOCAL, DeviceType::CPU,
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
            .memmove(4, DeviceType::CPU)
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return expressions::hint(
                  eq(arg["l_Zone"], "\"JFK Airport\"") |
                      eq(arg["l_Zone"], "\"LaGuardia Airport\"") |
                      eq(arg["l_Zone"], "\"Newark Airport\""),
                  expressions::Selectivity{1.0 / 10.0});
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {arg["l_LocationID"]};
            });
      case TaxiQueryType::Q33_synth_cols:
      case TaxiQueryType::Q33:
        return args.morph->scan("zone_lookup", {"l_LocationID", "l_borough"})
            .router(DegreeOfParallelism{compute_dop}, args.morph->getSlack(),
                    RoutingPolicy::LOCAL, DeviceType::CPU,
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
            .memmove(4, DeviceType::CPU)
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return expressions::hint(eq(arg["l_borough"], "\"Brooklyn\""),
                                       expressions::Selectivity{1.0 / 4.0});
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {arg["l_LocationID"]};
            });

      default:
        LOG(FATAL) << "TaxiQueryType not handled ";
    }
  }();

  auto split = scan.gsplit_v2(args.scan_slack, args.policy);
  std::vector<RelBuilder> paths;

  // int64_t jfk_loc_id = 132;
  // int64_t lga_loc_id = 138;
  const size_t mm_slack = 96 / compute_dop;
  const int64_t payment_type_eq = 1;  // 1 = card, 2 = cash

  std::optional<RelBuilder> zone_join = std::nullopt;

  if (args.do_direct) {
    zone_join =
        split
            .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.compute_numa_nodes))
            .memmove(mm_slack, DeviceType::CPU)
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return eq(arg[payment_type], payment_type_eq);
            })
            .filter([&](const auto &arg) -> expression_t {
              return gt(arg["trip_distance"], 0.0);
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {
                  (arg["trip_distance"]).as("tmp", "trip_distance"),
                  (arg["$1"]).as("tmp", "fare_amount"),
                  (arg["$2"]).as("tmp", "tip_amount"),
                  (arg["$3"]).as("tmp", "total_amount"),
                  (arg["$4"]).as("tmp", "mta_tax"),
                  (arg["$5"]).as("tmp", DOLocationID),
                  (arg["$6"]).as("tmp", "tpep_dropoff_datetime"),
                  (arg["$7"]).as("tmp", "tpep_pickup_datetime"),
                  // 1-5 miles
                  (cond((ge(arg["trip_distance"], 0.0) &
                         lt(arg["trip_distance"], 1.0)),
                        "0-1 miles",
                        (cond(
                            (ge(arg["trip_distance"], 1.0) &
                             lt(arg["trip_distance"], 5.0)),
                            "1-5 miles",
                            (cond(
                                (ge(arg["trip_distance"], 5.0) &
                                 lt(arg["trip_distance"], 10.0)),
                                "5-10 miles",
                                // 10-15 miles
                                (cond((ge(arg["trip_distance"], 10.0) &
                                       lt(arg["trip_distance"], 15.0)),
                                      "10-15 miles",
                                      // 15-20 miles (and fall through to 20+
                                      // miles)
                                      (cond((ge(arg["trip_distance"], 15.0) &
                                             lt(arg["trip_distance"], 20.0)),
                                            "15-20 miles", "20+ miles"))))))))))
                      .as("tmp", "trip_distance_bucket")};
            })
            .join(
                scan_zone,
                [&](const auto &build_arg) -> expression_t {
                  return build_arg["l_LocationID"];
                },
                [&](const auto &probe_arg) -> expression_t {
                  return probe_arg[DOLocationID];
                });
    paths.emplace_back(
        zone_join.value()
            .groupby(
                [&](const auto &arg) -> std::vector<expression_t> {
                  return {arg["trip_distance_bucket"]};
                },
                [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                  return {
                      GpuAggrMatExpr{
                          arg["fare_amount"].as("tmp", "fare_amount"), 1, 0,
                          SUM},

                      GpuAggrMatExpr{arg["tip_amount"].as("tmp", "tip_amount"),
                                     2, 0, SUM},
                      GpuAggrMatExpr{arg["mta_tax"].as("tmp", "mta_tax"), 3, 0,
                                     SUM},
                      GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 4, 0,
                                     SUM},
                      GpuAggrMatExpr{
                          (arg["trip_distance"] /
                           (time_delta_seconds(arg["tpep_pickup_datetime"],
                                               arg["tpep_dropoff_datetime"])
                                .template as<FloatType>()))
                              .as("tmp", "speed_mps"),
                          5, 0, SUM},
                  };
                },
                10, 8)
            .pack());
  }

  if (args.do_staging) {
    if (zone_join.has_value()) {
      paths.emplace_back(
          split
              .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
              .memmove(mm_slack, DeviceType::CPU,
                       std::vector<bool>{false, false, false, false, false,
                                         false, false, false})
              .unpack()
              .filter([&](const auto &arg) -> expression_t {
                return eq(arg[payment_type], payment_type_eq);
              })
              .filter([&](const auto &arg) -> expression_t {
                return gt(arg["trip_distance"], 0.0);
              })
              .project([&](const auto &arg) -> std::vector<expression_t> {
                return {
                    (arg["trip_distance"]).as("tmp", "trip_distance"),
                    (arg["$1"]).as("tmp", "fare_amount"),
                    (arg["$2"]).as("tmp", "tip_amount"),
                    (arg["$3"]).as("tmp", "total_amount"),
                    (arg["$4"]).as("tmp", "mta_tax"),
                    (arg["$5"]).as("tmp", DOLocationID),
                    (arg["$6"]).as("tmp", "tpep_dropoff_datetime"),
                    (arg["$7"]).as("tmp", "tpep_pickup_datetime"),
                    // 1-5 miles
                    (cond(
                         (ge(arg["trip_distance"], 0.0) &
                          lt(arg["trip_distance"], 1.0)),
                         "0-1 miles",
                         (cond(
                             (ge(arg["trip_distance"], 1.0) &
                              lt(arg["trip_distance"], 5.0)),
                             "1-5 miles",
                             (cond((ge(arg["trip_distance"], 5.0) &
                                    lt(arg["trip_distance"], 10.0)),
                                   "5-10 miles",
                                   // 10-15 miles
                                   (cond((ge(arg["trip_distance"], 10.0) &
                                          lt(arg["trip_distance"], 15.0)),
                                         "10-15 miles",
                                         // 15-20 miles (and fall through to 20+
                                         // miles)
                                         (cond((ge(arg["trip_distance"], 15.0) &
                                                lt(arg["trip_distance"], 20.0)),
                                               "15-20 miles",
                                               "20+ miles"))))))))))
                        .as("tmp", "trip_distance_bucket")};
              })
              .probeJoin(zone_join.value(),
                         [&](const auto &probe_arg) -> expression_t {
                           return probe_arg[DOLocationID];
                         })
              .groupby(
                  [&](const auto &arg) -> std::vector<expression_t> {
                    return {arg["trip_distance_bucket"]};
                  },
                  [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                    return {
                        GpuAggrMatExpr{
                            arg["fare_amount"].as("tmp", "fare_amount"), 1, 0,
                            SUM},

                        GpuAggrMatExpr{
                            arg["tip_amount"].as("tmp", "tip_amount"), 2, 0,
                            SUM},
                        GpuAggrMatExpr{arg["mta_tax"].as("tmp", "mta_tax"), 3,
                                       0, SUM},
                        GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 4, 0,
                                       SUM},
                        GpuAggrMatExpr{
                            (arg["trip_distance"] /
                             (time_delta_seconds(arg["tpep_pickup_datetime"],
                                                 arg["tpep_dropoff_datetime"])
                                  .template as<FloatType>()))
                                .as("tmp", "speed_mps"),
                            5, 0, SUM},
                    };
                  },
                  10, 8)
              .pack());
    } else {
      zone_join =
          split
              .path(DeviceType::CPU, DegreeOfParallelism{compute_dop},
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
              .memmove(mm_slack, DeviceType::CPU,
                       std::vector<bool>{false, false, false, false, false,
                                         false, false, false})
              .unpack()
              .filter([&](const auto &arg) -> expression_t {
                return eq(arg[payment_type], payment_type_eq);
              })
              .filter([&](const auto &arg) -> expression_t {
                return gt(arg["trip_distance"], 0.0);
              })
              .project([&](const auto &arg) -> std::vector<expression_t> {
                return {
                    (arg["trip_distance"]).as("tmp", "trip_distance"),
                    (arg["$1"]).as("tmp", "fare_amount"),
                    (arg["$2"]).as("tmp", "tip_amount"),
                    (arg["$3"]).as("tmp", "total_amount"),
                    (arg["$4"]).as("tmp", "mta_tax"),
                    (arg["$5"]).as("tmp", DOLocationID),
                    (arg["$6"]).as("tmp", "tpep_dropoff_datetime"),
                    (arg["$7"]).as("tmp", "tpep_pickup_datetime"),
                    // 1-5 miles
                    (cond(
                         (ge(arg["trip_distance"], 0.0) &
                          lt(arg["trip_distance"], 1.0)),
                         "0-1 miles",
                         (cond(
                             (ge(arg["trip_distance"], 1.0) &
                              lt(arg["trip_distance"], 5.0)),
                             "1-5 miles",
                             (cond((ge(arg["trip_distance"], 5.0) &
                                    lt(arg["trip_distance"], 10.0)),
                                   "5-10 miles",
                                   // 10-15 miles
                                   (cond((ge(arg["trip_distance"], 10.0) &
                                          lt(arg["trip_distance"], 15.0)),
                                         "10-15 miles",
                                         // 15-20 miles (and fall through to 20+
                                         // miles)
                                         (cond((ge(arg["trip_distance"], 15.0) &
                                                lt(arg["trip_distance"], 20.0)),
                                               "15-20 miles",
                                               "20+ miles"))))))))))
                        .as("tmp", "trip_distance_bucket")};
              })
              .join(
                  scan_zone,
                  [&](const auto &build_arg) -> expression_t {
                    return build_arg["l_LocationID"];
                  },
                  [&](const auto &probe_arg) -> expression_t {
                    return probe_arg[DOLocationID];
                  });
      paths.emplace_back(
          zone_join.value()
              .groupby(
                  [&](const auto &arg) -> std::vector<expression_t> {
                    return {arg["trip_distance_bucket"]};
                  },
                  [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                    return {
                        GpuAggrMatExpr{
                            arg["fare_amount"].as("tmp", "fare_amount"), 1, 0,
                            SUM},

                        GpuAggrMatExpr{
                            arg["tip_amount"].as("tmp", "tip_amount"), 2, 0,
                            SUM},
                        GpuAggrMatExpr{arg["mta_tax"].as("tmp", "mta_tax"), 3,
                                       0, SUM},
                        GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 4, 0,
                                       SUM},
                        GpuAggrMatExpr{
                            (arg["trip_distance"] /
                             (time_delta_seconds(arg["tpep_pickup_datetime"],
                                                 arg["tpep_dropoff_datetime"])
                                  .template as<FloatType>()))
                                .as("tmp", "speed_mps"),
                            5, 0, SUM},
                    };
                  },
                  10, 8)
              .pack());
    }
  }

  if (args.do_filter_pushdown) {
    const size_t pd_mm_slack = std::max(4ul, 32 / args.pushdown_dop);
    auto pd_move_to_compute =
        split
            .path(DeviceType::CPU, DegreeOfParallelism{args.pushdown_dop},
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      args.pushdown_numa_nodes))
            .memmove(pd_mm_slack, DeviceType::CPU)
            .unpack()
            .filter([&](const auto &arg) -> expression_t {
              return eq(arg[payment_type], payment_type_eq);
            })
            .filter([&](const auto &arg) -> expression_t {
              return gt(arg["trip_distance"], 0.0);
            })
            .project([&](const auto &arg) -> std::vector<expression_t> {
              return {
                  (arg["trip_distance"]).as("tmp", "trip_distance"),
                  (arg["$1"]).as("tmp", "fare_amount"),
                  (arg["$2"]).as("tmp", "tip_amount"),
                  (arg["$3"]).as("tmp", "total_amount"),
                  (arg["$4"]).as("tmp", "mta_tax"),
                  (arg["$5"]).as("tmp", DOLocationID),
                  (arg["$6"]).as("tmp", "tpep_dropoff_datetime"),
                  (arg["$7"]).as("tmp", "tpep_pickup_datetime"),
                  // 1-5 miles
                  (cond((ge(arg["trip_distance"], 0.0) &
                         lt(arg["trip_distance"], 1.0)),
                        "0-1 miles",
                        (cond(
                            (ge(arg["trip_distance"], 1.0) &
                             lt(arg["trip_distance"], 5.0)),
                            "1-5 miles",
                            (cond(
                                (ge(arg["trip_distance"], 5.0) &
                                 lt(arg["trip_distance"], 10.0)),
                                "5-10 miles",
                                // 10-15 miles
                                (cond((ge(arg["trip_distance"], 10.0) &
                                       lt(arg["trip_distance"], 15.0)),
                                      "10-15 miles",
                                      // 15-20 miles (and fall through to 20+
                                      // miles)
                                      (cond((ge(arg["trip_distance"], 15.0) &
                                             lt(arg["trip_distance"], 20.0)),
                                            "15-20 miles", "20+ miles"))))))))))
                      .as("tmp", "trip_distance_bucket")};
            })

            .pack()
            .router(DegreeOfParallelism{compute_dop}, 2, RoutingPolicy::RANDOM,
                    DeviceType::CPU,
                    std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                        args.compute_numa_nodes))
            .unpack();

    if (zone_join.has_value()) {
      paths.emplace_back(
          pd_move_to_compute
              .probeJoin(zone_join.value(),
                         [&](const auto &probe_arg) -> expression_t {
                           return probe_arg[DOLocationID];
                         })
              .groupby(
                  [&](const auto &arg) -> std::vector<expression_t> {
                    return {arg["trip_distance_bucket"]};
                  },
                  [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                    return {
                        GpuAggrMatExpr{
                            arg["fare_amount"].as("tmp", "fare_amount"), 1, 0,
                            SUM},

                        GpuAggrMatExpr{
                            arg["tip_amount"].as("tmp", "tip_amount"), 2, 0,
                            SUM},
                        GpuAggrMatExpr{arg["mta_tax"].as("tmp", "mta_tax"), 3,
                                       0, SUM},
                        GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 4, 0,
                                       SUM},
                        GpuAggrMatExpr{
                            (arg["trip_distance"] /
                             (time_delta_seconds(arg["tpep_pickup_datetime"],
                                                 arg["tpep_dropoff_datetime"])
                                  .template as<FloatType>()))
                                .as("tmp", "speed_mps"),
                            5, 0, SUM},
                    };
                  },
                  10, 8)
              .pack());
    } else {
      zone_join = pd_move_to_compute.join(
          scan_zone,
          [&](const auto &build_arg) -> expression_t {
            return build_arg["l_LocationID"];
          },
          [&](const auto &probe_arg) -> expression_t {
            return probe_arg[DOLocationID];
          });
      paths.emplace_back(
          zone_join.value()
              .groupby(
                  [&](const auto &arg) -> std::vector<expression_t> {
                    return {arg["trip_distance_bucket"]};
                  },
                  [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                    return {
                        GpuAggrMatExpr{
                            arg["fare_amount"].as("tmp", "fare_amount"), 1, 0,
                            SUM},

                        GpuAggrMatExpr{
                            arg["tip_amount"].as("tmp", "tip_amount"), 2, 0,
                            SUM},
                        GpuAggrMatExpr{arg["mta_tax"].as("tmp", "mta_tax"), 3,
                                       0, SUM},
                        GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 4, 0,
                                       SUM},
                        GpuAggrMatExpr{
                            (arg["trip_distance"] /
                             (time_delta_seconds(arg["tpep_pickup_datetime"],
                                                 arg["tpep_dropoff_datetime"])
                                  .template as<FloatType>()))
                                .as("tmp", "speed_mps"),
                            5, 0, SUM},
                    };
                  },
                  10, 8)
              .pack());
    }
  }

  CHECK_GT(paths.size(), 0) << "Cannot have a plan with with no paths";

  auto first_path = paths.front();
  return first_path
      .unionAll({paths.begin() + 1, paths.end()}, DegreeOfParallelism{1},
                std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                    args.compute_numa_nodes),
                128)
      .unpack()
      .groupby(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["trip_distance_bucket"]};
          },
          [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
            return {
                GpuAggrMatExpr{arg["fare_amount"].as("tmp", "fare_amount"), 1,
                               0, SUM},

                GpuAggrMatExpr{arg["tip_amount"].as("tmp", "tip_amount"), 2, 0,
                               SUM},
                GpuAggrMatExpr{arg["mta_tax"].as("tmp", "mta_tax"), 3, 0, SUM},
                GpuAggrMatExpr{expression_t{1}.as("tmp", "count"), 4, 0, SUM},
                GpuAggrMatExpr{arg["speed_mps"], 5, 0, SUM}};
          },
          10, 8)
      .project([&](const auto &arg) -> std::vector<expression_t> {
        double seconds_in_hour = 3600;
        return {arg["trip_distance_bucket"],
                (arg["tip_amount"] / (arg["count"].template as<FloatType>()))
                    .as("tmp", "avg_tip_amount"),
                (arg["fare_amount"] / (arg["count"].template as<FloatType>()))
                    .as("tmp", "avg_fare_amount"),
                (arg["mta_tax"] / (arg["count"].template as<FloatType>()))
                    .as("tmp", "avg_mta_tax"),
                (arg["speed_mps"] / (arg["count"].template as<FloatType>()) *
                 seconds_in_hour)
                    .as("tmp", "avg_speed_mph"),
                arg["count"]};
      })
      .sort(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["trip_distance_bucket"], arg["avg_tip_amount"],
                    arg["avg_fare_amount"],      arg["avg_mta_tax"],
                    arg["avg_speed_mph"],        arg["count"]};
          },
          {direction::NONE, direction::ASC, direction::NONE, direction::NONE,
           direction::NONE, direction::NONE})
      .print(pg{"pm-csv"})
      .prepare();
}
