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

#include <cli-flags.hpp>
#include <olap-perf-util/ssb-aliases.hpp>
#include <platform/topology/topology.hpp>
#include <query-shaping/experimental-shapers.hpp>
#include <taxi/query.hpp>

PreparedStatement taxi_debug_q1(proteus::QueryShaper &morph,
                                double trip_distance_min,
                                double trip_distance_max) {
  morph.setQueryName("taxi_q2");
  auto scan = morph.scan("yellow_tripdata",
                         {"trip_distance", "fare_amount",
                          "tpep_dropoff_datetime", "tpep_pickup_datetime"});

  auto parallel_groupby =
      morph.parallelize(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return (le(arg["trip_distance"], trip_distance_max) &
                    gt(arg["trip_distance"], trip_distance_min));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {
                expressions::ExtractExpression{arg["tpep_pickup_datetime"],
                                               expressions::extract_unit::YEAR}
                    .as("tmp", "year"),
                expressions::ExtractExpression{arg["tpep_pickup_datetime"],
                                               expressions::extract_unit::MONTH}
                    .as("tmp", "month"),
                arg["tpep_pickup_datetime"].as("tmp", "tpep_pickup_datetime"),
                // arg["fare_amount"].as("tmp", "fare_amount"),
                // arg["trip_distance"].as("tmp", "trip_distance"),
                // expressions::ExtractExpression{
                //       arg["tpep_pickup_datetime"],
                //       expressions::extract_unit::DAYOFWEEK}
                //               .as("tmp", "dayofweek")

            };
          });
  return morph.collect(parallel_groupby).print(pg{"pm-csv"}).prepare();
}

expression_t time_delta_seconds(const expression_t &lhs,
                                const expression_t &rhs) {
  constexpr int64_t ms_in_s = 1000ll;
  return (rhs - lhs).template as<Int64Type>() / ms_in_s;
}

PreparedStatement taxi_debug_q2(proteus::QueryShaper &morph,
                                double fare_amount_min,
                                double fare_amount_max) {
  morph.setQueryName("taxi_q2");
  auto scan = morph.scan("yellow_tripdata",
                         {"trip_distance", "fare_amount",
                          "tpep_dropoff_datetime", "tpep_pickup_datetime"});

  auto parallel_groupby =
      morph.parallelize(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return (le(arg["fare_amount"], fare_amount_max) &
                    ge(arg["fare_amount"], fare_amount_min));
          })
          .filter([&](const auto &arg) -> expression_t {
            return lt(arg["tpep_pickup_datetime"],
                      arg["tpep_dropoff_datetime"]);
          })
          .filter([&](const auto &arg) -> expression_t {
            return gt(arg["trip_distance"], 0.0);
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {
                time_delta_seconds(arg["tpep_pickup_datetime"],
                                   arg["tpep_dropoff_datetime"])
                    .as("tmp", "time_delta"),
                arg["tpep_pickup_datetime"].as("tmp", "tpep_pickup_datetime"),
                arg["tpep_dropoff_datetime"].as("tmp", "tpep_dropoff_datetime"),
                // arg["fare_amount"].as("tmp", "fare_amount"),
                // arg["trip_distance"].as("tmp", "trip_distance"),
                // expressions::ExtractExpression{
                //       arg["tpep_pickup_datetime"],
                //       expressions::extract_unit::DAYOFWEEK}
                //               .as("tmp", "dayofweek")

            };
          });
  return morph.collect(parallel_groupby).print(pg{"pm-csv"}).prepare();
}

PreparedStatement taxi_count_star(proteus::QueryShaper &morph) {
  morph.setQueryName("taxi_scan");

  auto scan = morph.scan("yellow_tripdata", {"trip_distance"});
  int32_t t = 2;
  int64_t t2 = 5;

  auto parallel_reduce =
      morph.parallelize(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return gt(arg["trip_distance"], 0.0);
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {expression_t{1}.as("tmp", "count"),
                        arg["trip_distance"].as("tmp", "min"),
                        arg["trip_distance"].as("tmp", "max"),
                        arg["trip_distance"].as("tmp", "sum")};
              },
              {SUM, MIN, MAX, SUM});
  return morph.collect(parallel_reduce)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["count"], arg["min"], arg["max"], arg["sum"]};
          },
          {SUM, MIN, MAX, SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

std::vector<std::chrono::milliseconds> bench_query(PreparedStatement &query) {
  LOG(INFO) << "warming up";
  query.execute();
  std::vector<std::chrono::milliseconds> times;
  LOG(INFO) << "Executing query";
  for (int i = 0; i < 5; i++) {
    time_block t{[&](const auto &x) { times.emplace_back(x); }};
    query.execute();
  }
  return times;
}

FileRecord loadToCpuSocket0(const std::string &name, size_t type_size) {
  size_t psize = ::getFileSize(name.c_str());
  size_t offset = 0;
  time_block t("Topen (" + name + "): ",
               TimeRegistry::Key{"Data loading (CPUs)"});
  const auto &topo = topology::getInstance();

  size_t factor = type_size / sizeof(int32_t);

  auto devices = topo.getCpuNumaNodesByPackageId(0).size();

  size_t filesize = psize / factor;

  size_t pack_alignment = sysconf(_SC_PAGE_SIZE);  // required by mmap
  // in order to do that without the schema, we have to take the worst case
  // of a file with a single-byte column and a 64bit column and align based
  // on that. Otherwise, the segments may be misaligned
  pack_alignment = std::max(pack_alignment, BlockManager::block_size);

  size_t part_size =
      (((filesize + pack_alignment - 1) / pack_alignment + devices - 1) /
       devices) *
      pack_alignment;  // FIXME: assumes maximum record size of 128Bytes

  decltype(FileRecord::data) partitions;
  partitions.reserve(devices);
  int d = 0;
  for (const auto &cpu : topo.getCpuNumaNodesByPackageId(0)) {
    if (part_size * d < filesize) {
      set_exec_location_on_scope cd(cpu);
      partitions.emplace_back(std::make_unique<mmap_file>(
          name, PINNED, std::min(part_size, filesize - part_size * d) * factor,
          part_size * d * factor + offset));
    }
    ++d;
  }
  return FileRecord{std::move(partitions)};
}

int main(int argc, char **argv) {
  LOG(INFO) << "Running taxi benchmark";

  auto olap = proteus::from_cli::olap("Benchmark taxi", &argc, &argv);
  LOG(INFO) << "Finished initialization";
  LOG(INFO) << "Running in: " << std::filesystem::current_path() << " \n";

  // replace CPUOnlySingleServer with this if you want to run on a single socket
  auto& sm = StorageManager::getInstance();
  // StorageManager::Loader socket_0_loader =
  //     [](StorageManager& sm, const std::string &name, size_t type_size) {
  //       return loadToCpuSocket0(name, type_size);
  // };
  // sm.setDefaultLoader(socket_0_loader);
  //
  // std::shared_ptr<proteus::CPUOnlySingleServerSingleSocket> shaper =
  //     std::make_shared<proteus::CPUOnlySingleServerSingleSocket>(
  //         "inputs/taxi/", taxi::Query::getStats(), false, 16);

  std::shared_ptr<proteus::CPUOnlySingleServer> shaper =
      std::make_shared<proteus::CPUOnlySingleServer>(
          "inputs/taxi/", taxi::Query::getStats(), false, 16);

  std::vector<std::pair<std::string, std::vector<std::chrono::milliseconds>>>
      query_times;
  auto query11 = taxi::Query::prepare11(*shaper);
  query_times.emplace_back(std::make_pair("q11", bench_query(query11)));

  auto query12 = taxi::Query::prepare12(*shaper);
  query_times.emplace_back(std::make_pair("q12", bench_query(query12)));
  auto query13 = taxi::Query::prepare13(*shaper);
  query_times.emplace_back(std::make_pair("q13", bench_query(query13)));
  auto query14 = taxi::Query::prepare14(*shaper);
  query_times.emplace_back(std::make_pair("q14", bench_query(query14)));

  auto query21 = taxi::Query::prepare21(*shaper);
  query_times.emplace_back(std::make_pair("q21", bench_query(query21)));
  auto query22 = taxi::Query::prepare22(*shaper);
  query_times.emplace_back(std::make_pair("q22", bench_query(query22)));
  auto query23 = taxi::Query::prepare23(*shaper);
  query_times.emplace_back(std::make_pair("q23", bench_query(query23)));
  auto query24 = taxi::Query::prepare24(*shaper);
  query_times.emplace_back(std::make_pair("q24", bench_query(query24)));

  std::cout << "query,execution_time_ms" << std::endl;
  for (auto [query, times] : query_times) {
    for (auto time : times) {
      std::cout << query << "," << time.count() << std::endl;
    }
  }

  sm.unloadAll();
  sm.dropAllCustomLoaders();
}
