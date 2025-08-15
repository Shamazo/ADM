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
#include <cli-flags.hpp>
#include <codegen/expressions/expressionTypes.hpp>
#include <fstream>
#include <magic_enum.hpp>
#include <olap/operators/relbuilder-factory.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <olap/routing/routing-policy-types-v2.hpp>
#include <platform/topology/affinity_manager.hpp>
#include <platform/topology/topology.hpp>
#include <platform/util/profiling.hpp>
#include <query-shaping/nvme-shapers.hpp>
#include <vector>

#include "common-flags.hpp"
#include "taxi-benchmarks.hpp"
#include "util.hpp"

DECLARE_string(grouter_policy);
DEFINE_string(grouter_policy, "LOCALITY_AWARE", "grouter policy to use.");

DECLARE_int32(pushdown_dop);
DEFINE_int32(
    pushdown_dop, -1,
    "Number of threads to use for pushed-down operators. Default of -1 is a "
    "thread per core on the socket used for pushdown operators.");

DECLARE_bool(bench_direct_taxi);
DEFINE_bool(bench_direct_taxi, false, "");

DECLARE_bool(bench_staging_taxi);
DEFINE_bool(bench_staging_taxi, false, "");

DECLARE_bool(bench_pushdown_taxi);
DEFINE_bool(bench_pushdown_taxi, false, "");

DECLARE_bool(bench_adaptive_taxi);
DEFINE_bool(bench_adaptive_taxi, false, "");

DECLARE_bool(bench_randomseq_adaptive_taxi);
DEFINE_bool(bench_randomseq_adaptive_taxi, false, "");

DECLARE_bool(bench_randomseq_direct_taxi);
DEFINE_bool(bench_randomseq_direct_taxi, false, "");

DECLARE_bool(bench_randomseq_pushdown_taxi);
DEFINE_bool(bench_randomseq_pushdown_taxi, false, "");

TimeStampLogger* global_timestamp_logger;
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  global_timestamp_logger = new TimeStampLogger(FLAGS_timestamp_file);

  auto ctx = proteus::from_cli::olap("adm-taxi", &argc, &argv);

  std::stringstream ss;
  std::optional<std::ofstream> out = std::nullopt;
  if (!FLAGS_result_file.empty()) {
    if (std::filesystem::exists(FLAGS_result_file)) {
      LOG(INFO) << "Result file " << FLAGS_result_file
                << " already exists. Appending to it.";
      out = std::ofstream(FLAGS_result_file, std::ios::app);
    } else {
      LOG(INFO) << "Result file " << FLAGS_result_file
                << " does not exist. Creating it.";
      out = std::ofstream(FLAGS_result_file);
    }

    CHECK(out->is_open()) << "Could not open result file " << FLAGS_result_file;
  }

  /**
   * Scan vary BW benchmarks
   * ##############################
   */
  if (FLAGS_bench_direct_taxi) {
    LOG(INFO) << "running bench_direct_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_taxi(
        {.query_args =
             QueryArgs{
                 .do_staging = false,
                 .do_bloom_filter_build = false,
                 .do_bloom_filter_pushdown = false,
                 .do_filter_pushdown = false,
                 .do_direct = true,
                 .policy = policy,
                 .scan_slack = 12,
                 .use_hyper_threads = FLAGS_use_hyper_threads,
                 .pushdown_numa_nodes =
                     get_default_pushdown_numa_nodes(FLAGS_server_number),
                 .compute_numa_nodes =
                     get_default_compute_numa_nodes(FLAGS_server_number),
                 .pushdown_dop = DegreeOfParallelism{static_cast<size_t>(
                     FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16)}},
         .server_number = FLAGS_server_number,
         .num_iterations = FLAGS_num_iterations});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_staging_taxi) {
    LOG(INFO) << "running bench_staging_taxi";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_taxi(
        {.query_args =
             QueryArgs{
                 .do_staging = true,
                 .do_bloom_filter_build = false,
                 .do_bloom_filter_pushdown = false,
                 .do_filter_pushdown = false,
                 .do_direct = false,
                 .policy = policy,
                 .scan_slack = 12,
                 .use_hyper_threads = FLAGS_use_hyper_threads,
                 .pushdown_numa_nodes =
                     get_default_pushdown_numa_nodes(FLAGS_server_number),
                 .compute_numa_nodes =
                     get_default_compute_numa_nodes(FLAGS_server_number),
                 .pushdown_dop = DegreeOfParallelism{static_cast<size_t>(
                     FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16)}},
         .server_number = FLAGS_server_number,
         .num_iterations = FLAGS_num_iterations});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_pushdown_taxi) {
    LOG(INFO) << "running bench_pushdown_taxi";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_taxi(
        {.query_args =
             QueryArgs{
                 .do_staging = false,
                 .do_bloom_filter_build = false,
                 .do_bloom_filter_pushdown = false,
                 .do_filter_pushdown = true,
                 .do_direct = false,
                 .policy = policy,
                 .scan_slack = 12,
                 .use_hyper_threads = FLAGS_use_hyper_threads,
                 .pushdown_numa_nodes =
                     get_default_pushdown_numa_nodes(FLAGS_server_number),
                 .compute_numa_nodes =
                     get_default_compute_numa_nodes(FLAGS_server_number),
                 .pushdown_dop = DegreeOfParallelism{static_cast<size_t>(
                     FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16)}},
         .server_number = FLAGS_server_number,
         .num_iterations = FLAGS_num_iterations});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_adaptive_taxi) {
    LOG(INFO) << "running bench_adaptive_taxi";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_taxi(
        {.query_args =
             QueryArgs{
                 .do_staging = true,
                 .do_bloom_filter_build = false,
                 .do_bloom_filter_pushdown = false,
                 .do_filter_pushdown = true,
                 .do_direct = true,
                 .policy = policy,
                 .scan_slack = 12,
                 .use_hyper_threads = FLAGS_use_hyper_threads,
                 .pushdown_numa_nodes =
                     get_default_pushdown_numa_nodes(FLAGS_server_number),
                 .compute_numa_nodes =
                     get_default_compute_numa_nodes(FLAGS_server_number),
                 .pushdown_dop = DegreeOfParallelism{static_cast<size_t>(
                     FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16)}},
         .server_number = FLAGS_server_number,
         .num_iterations = FLAGS_num_iterations});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_randomseq_direct_taxi) {
    LOG(INFO) << " running bench_randomseq_direct_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_taxi_random_sequence(
        {.query_args =
             QueryArgs{
                 .do_staging = false,
                 .do_bloom_filter_build = false,
                 .do_bloom_filter_pushdown = false,
                 .do_filter_pushdown = false,
                 .do_direct = true,
                 .policy = policy,
                 .scan_slack = 12,
                 .use_hyper_threads = FLAGS_use_hyper_threads,
                 .pushdown_numa_nodes =
                     get_default_pushdown_numa_nodes(FLAGS_server_number),
                 .compute_numa_nodes =
                     get_default_compute_numa_nodes(FLAGS_server_number),
                 .pushdown_dop = DegreeOfParallelism{static_cast<size_t>(
                     FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16)}},
         .server_number = FLAGS_server_number,
         .num_iterations = FLAGS_num_iterations});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_randomseq_adaptive_taxi) {
    LOG(INFO) << " running bench_randomseq_direct_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_taxi_random_sequence(
        {.query_args =
             QueryArgs{
                 .do_staging = true,
                 .do_bloom_filter_build = true,
                 .do_bloom_filter_pushdown = true,
                 .do_filter_pushdown = true,
                 .do_direct = true,
                 .policy = policy,
                 .scan_slack = 12,
                 .use_hyper_threads = FLAGS_use_hyper_threads,
                 .pushdown_numa_nodes =
                     get_default_pushdown_numa_nodes(FLAGS_server_number),
                 .compute_numa_nodes =
                     get_default_compute_numa_nodes(FLAGS_server_number),
                 .pushdown_dop = DegreeOfParallelism{static_cast<size_t>(
                     FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16)}},
         .server_number = FLAGS_server_number,
         .num_iterations = FLAGS_num_iterations});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_randomseq_pushdown_taxi) {
    LOG(INFO) << " running bench_randomseq_direct_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_taxi_random_sequence(
        {.query_args =
             QueryArgs{
                 .do_staging = false,
                 .do_bloom_filter_build = true,
                 .do_bloom_filter_pushdown = true,
                 .do_filter_pushdown = true,
                 .do_direct = false,
                 .policy = policy,
                 .scan_slack = 12,
                 .use_hyper_threads = FLAGS_use_hyper_threads,
                 .pushdown_numa_nodes =
                     get_default_pushdown_numa_nodes(FLAGS_server_number),
                 .compute_numa_nodes =
                     get_default_compute_numa_nodes(FLAGS_server_number),
                 .pushdown_dop = DegreeOfParallelism{static_cast<size_t>(
                     FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16)}},
         .server_number = FLAGS_server_number,
         .num_iterations = FLAGS_num_iterations});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  std::cout << ss.str();
  auto& sm = StorageManager::getInstance();
  sm.unloadAll();
  return 0;
}
