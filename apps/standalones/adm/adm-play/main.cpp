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
#include <platform/topology/affinity_manager.hpp>
#include <platform/topology/topology.hpp>
#include <platform/util/profiling.hpp>
#include <query-shaping/nvme-shapers.hpp>
#include <ssb/query.hpp>
#include <vector>
#include <olap/routing/routing-policy-types-v2.hpp>

#include "common-flags.hpp"
#include "microbenchmarks.hpp"
#include "ssb-adaptive-benchmarks.hpp"
#include "ssb-benchmarks.hpp"
#include "util.hpp"

// shouldn't really need the DECLAREs, but it silences a warning
DECLARE_bool(bench_varybw_cpu);
DEFINE_bool(bench_varybw_cpu, false,
            "CPU only vary number of NVMes used with uncompressed data with a "
            "scan & sum query");

DECLARE_bool(bench_varybw_cpu_compressed);
DEFINE_bool(bench_varybw_cpu_compressed, false,
            "CPU only vary number of NVMes used with compressed data with a "
            "scan & sum query");

DECLARE_bool(bench_varybw_gpu);
DEFINE_bool(bench_varybw_gpu, false,
            "GPU only vary number of NVMes used with uncompressed data with a "
            "scan & sum query");

DECLARE_bool(bench_varybw_gpu_compressed);
DEFINE_bool(bench_varybw_gpu_compressed, false,
            "GPU only vary number of NVMes used with compressed data with a "
            "scan & sum query");

DECLARE_bool(bench_varybw_cpu_ssb);
DEFINE_bool(bench_varybw_cpu_ssb, false,
            "CPU only vary number of NVMes used with uncompressed data using "
            "--scale_factor and varying the number of drives used");

DECLARE_bool(bench_varybw_cpu_ssb_compressed);
DEFINE_bool(bench_varybw_cpu_ssb_compressed, false,
            "CPU only vary number of NVMes used with compressed data using "
            "--scale_factor and varying the number of drives used");

DECLARE_bool(bench_varybw_gpu_ssb);
DEFINE_bool(bench_varybw_gpu_ssb, false,
            "GPU only vary number of NVMes used with uncompressed data using "
            "--scale_factor and varying the number of drives used");

DECLARE_bool(bench_varybw_gpu_ssb_compressed);
DEFINE_bool(bench_varybw_gpu_ssb_compressed, false,
            "GPU only vary number of NVMes used with compressed data using "
            "--scale_factor and varying the number of drives used");

DECLARE_bool(bench_ssb_cpu_socket_pushdown);
DEFINE_bool(bench_ssb_cpu_socket_pushdown, false,
            "CPU only vary number of NVMes used for SSB Q1.x with the probe "
            "filter pushed down to CPU socket 0 and the rest of the "
            "query processing on socket 1. All data is on NVMe drives on "
            "socket 0.  Degree of parallelism for the pushed-down filter is "
            "set by --pushdown_dop");

DECLARE_bool(bench_ssb_cpu_socket_pushdown_baseline);
DEFINE_bool(bench_ssb_cpu_socket_pushdown_baseline, false,
            "CPU only vary number of NVMes used for SSB Q1.x using the CPU "
            "NUMA nodes of socket 1 and data on NVMe drives on socket 0.");

DECLARE_bool(bench_adaptive_ssb);
DEFINE_bool(bench_adaptive_ssb, false,
            "SSB using adaptive data movement. Using the CPU NUMA nodes of "
            "socket 1 and data on NVMe drives on socket 0.");

DECLARE_bool(bench_grouter_direct_ssb);
DEFINE_bool(bench_grouter_direct_ssb, false,
            "SSB with direct data movement. Using the CPU NUMA nodes of socket "
            "1 and data on NVMe drives on socket 0.");

DECLARE_bool(bench_grouter_staging_ssb);
DEFINE_bool(bench_grouter_staging_ssb, false,
            "SSB with staged data movement. using the CPU NUMA nodes of socket "
            "1 and data on NVMe drives on socket 0.");

DECLARE_bool(bench_grouter_pushdown_ssb);
DEFINE_bool(bench_grouter_pushdown_ssb, false,
            "SSB with pushdown. Using the CPU NUMA nodes of socket 1 and data "
            "on NVMe drives on socket 0.");

DECLARE_bool(bench_randomseq_direct_ssb);
DEFINE_bool(bench_randomseq_direct_ssb, false, "");

DECLARE_bool(bench_randomseq_adaptive_ssb);
DEFINE_bool(bench_randomseq_adaptive_ssb, false, "");

DECLARE_bool(bench_randomseq_pushdown_ssb);
DEFINE_bool(bench_randomseq_pushdown_ssb, false, "");

DECLARE_string(grouter_policy);
DEFINE_string(grouter_policy, "LOCALITY_AWARE",
              "grouter policy to use.");

DECLARE_int32(pushdown_dop);
DEFINE_int32(
    pushdown_dop, -1,
    "Number of threads to use for pushed-down operators. Default of -1 is a "
    "thread per core on the socket used for pushdown operators.");

DECLARE_int32(scale_factor);
DEFINE_int32(scale_factor, 100, "SSB scale factor");


TimeStampLogger* global_timestamp_logger;
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  global_timestamp_logger = new TimeStampLogger(FLAGS_timestamp_file);

  auto ctx = proteus::from_cli::olap("adm-play", &argc, &argv);

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
  if (FLAGS_bench_varybw_cpu) {
    LOG(INFO) << "running bench_varybw_cpu";
    auto res = bench_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                  Shaper::NVMECPU, FLAGS_num_iterations);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_cpu_compressed) {
    LOG(INFO) << "running bench_varybw_cpu_compressed";
    auto res =
        bench_nvme_vary_bw_compressed(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMECPU, FLAGS_num_iterations);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu) {
    LOG(INFO) << "running bench_varybw_gpu";
    auto res = bench_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                  Shaper::NVMEGPU, FLAGS_num_iterations, 2, 16);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu_compressed) {
    LOG(INFO) << "running bench_varybw_gpu_compressed";
    auto res = bench_nvme_vary_bw_compressed(
        FLAGS_scale_factor, FLAGS_server_number, Shaper::NVMEGPU,
        FLAGS_num_iterations, 2, 8);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  /**
   * SSB vary BW benchmarks
   * ##############################
   */

  if (FLAGS_bench_varybw_cpu_ssb) {
    LOG(INFO) << "running bench_varybw_cpu_ssb";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMECPU, FLAGS_num_iterations, 4,
                                      4, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_cpu_ssb_compressed) {
    LOG(INFO) << "running bench_varybw_cpu_ssb_compressed";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMECPU, FLAGS_num_iterations, 16, 4, true);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu_ssb) {
    LOG(INFO) << "running bench_varybw_gpu_ssb";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMEGPU, FLAGS_num_iterations, 16, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu_ssb_compressed) {
    LOG(INFO) << "running bench_varybw_gpu_ssb_compressed";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMEGPU, 2, 4, true);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_adaptive_ssb) {
    LOG(INFO) << " running bench_adaptive_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(FLAGS_grouter_policy)
            .value();



    auto res = bench_adaptive_ssb(
        {.ssb_query_args =
             QueryArgs{.do_staging = true,
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

  if (FLAGS_bench_grouter_direct_ssb) {
    LOG(INFO) << " running bench_grouter_direct_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_ssb(
        {.ssb_query_args =
             QueryArgs{.do_staging = false,
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

  if (FLAGS_bench_grouter_pushdown_ssb) {
    LOG(INFO) << " running bench_grouter_pushdown_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_ssb(
        {.ssb_query_args =
             QueryArgs{.do_staging = false,
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

  if (FLAGS_bench_grouter_staging_ssb) {
    LOG(INFO) << " running bench_grouter_staging_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_ssb(
        {.ssb_query_args =
             QueryArgs{.do_staging = true,
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

  if (FLAGS_bench_randomseq_direct_ssb) {
    LOG(INFO) << " running bench_randomseq_direct_ssb";
    auto res = bench_adaptive_ssb_random_sequence(
        {.ssb_query_args =
             QueryArgs{
                 .do_staging = false,
                 .do_bloom_filter_build = false,
                 .do_bloom_filter_pushdown = false,
                 .do_filter_pushdown = false,
                 .do_direct = true,
                 .policy = proteus::routing::GeneralizedRoutingPolicyV2::
                     LOCALITY_AWARE,
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

  if (FLAGS_bench_randomseq_adaptive_ssb) {
    LOG(INFO) << " running bench_randomseq_direct_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_ssb_random_sequence(
        {.ssb_query_args =
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

  if (FLAGS_bench_randomseq_pushdown_ssb) {
    LOG(INFO) << " running bench_randomseq_direct_ssb";
    auto policy =
        magic_enum::enum_cast<proteus::routing::GeneralizedRoutingPolicyV2>(
            FLAGS_grouter_policy)
            .value();
    auto res = bench_adaptive_ssb_random_sequence(
        {.ssb_query_args =
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
