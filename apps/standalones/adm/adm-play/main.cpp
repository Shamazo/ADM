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

#include "microbenchmarks.hpp"
#include "prepared-queries.hpp"
#include "selectivity-micros.hpp"
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

DECLARE_bool(bench_ssb_gpu_pushdown);
DEFINE_bool(bench_ssb_gpu_pushdown, false,
            "GPU only vary number of NVMes used for SSB Q1.x with the probe "
            "filter pushed down to the CPU"
            "--scale_factor and varying the number of drives used");

DECLARE_bool(bench_ssb_gpu_pushdown_compressed);
DEFINE_bool(bench_ssb_gpu_pushdown_compressed, false,
            "GPU only vary number of NVMes used for SSB Q1.x with the probe "
            "filter pushed down to the CPU. Use compressed data"
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

DECLARE_bool(bench_micro_cpu_socket_pushdown_baseline);
DEFINE_bool(
    bench_micro_cpu_socket_pushdown_baseline, false,
    "CPU only vary the selectivity of a 2 col scan->filter->sum query."
    "Compute uses the NUMA nodes of socket 1 and the data is on NVMe "
    "drives on socket 0. Data is moved directly from NVMes to socket 1.");

DECLARE_bool(bench_micro_cpu_socket_stage_both);
DEFINE_bool(bench_micro_cpu_socket_stage_both, false,
            "CPU only vary the selectivity of a 2 col scan->filter->sum query."
            "Compute uses the NUMA nodes of socket 1 and the data is on NVMe "
            "drives on socket 0. Data is moved from NVMes to socket 0 then "
            "accessed over the interconnect.");

DECLARE_bool(bench_micro_cpu_socket_stage_one);
DEFINE_bool(bench_micro_cpu_socket_stage_one, false,
            "CPU only vary the selectivity of a 2 col scan->filter->sum query."
            "Compute uses the NUMA nodes of socket 1 and the data is on NVMe "
            "drives on socket 0. Data for the first column is moved directly "
            "from NVMes to socket 1. Data for the second column is moved from "
            "NVMes to socket 0 then accessed over the interconnect.");

DECLARE_bool(bench_micro_cpu_socket_pushdown_filter);
DEFINE_bool(bench_micro_cpu_socket_pushdown_filter, false,
            "CPU only vary the selectivity of a 2 col scan->filter->sum query "
            "with filter pushdown. Compute (the sum) uses the NUMA nodes of "
            "socket 1. The data is on NVMe drives on socket 0. The filter runs "
            "on socket 0, the filtered values are written to memory on socket "
            "1 and accessed over the interconnect by the sum operator. The "
            "filter parallelism of the filter is set by --pushdown_dop");

DECLARE_bool(bench_micro_cpu_socket_pushdown_filter_memmove);
DEFINE_bool(bench_micro_cpu_socket_pushdown_filter_memmove, false,
            "Identical to bench_micro_cpu_socket_pushdown_filter, but the "
            "filtered data is explicitly mem-moved to socket 1");

DECLARE_int32(pushdown_dop);
DEFINE_int32(
    pushdown_dop, -1,
    "Number of threads to use for pushed-down operators. Default of -1 is a "
    "thread per core on the socket used for pushdown operators.");

DECLARE_int32(scale_factor);
DEFINE_int32(scale_factor, 100, "SSB scale factor");

DECLARE_int32(server_number);
DEFINE_int32(server_number, 46, "server number (DIAS internal)");

DECLARE_int32(num_iterations);
DEFINE_int32(num_iterations, 5, "Number of types to run each query");

DECLARE_string(result_file);
DEFINE_string(result_file, "",
              "[optional] output file for results [default: stdout]");

DECLARE_string(timestamp_file);
DEFINE_string(timestamp_file, "adm-play-timestamps.csv",
              "[optional] output file for TimeStampLogger logs  [default: "
              "adm-play-timestamps.csv]");

TimeStampLogger* global_timestamp_logger;
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  global_timestamp_logger = new TimeStampLogger(FLAGS_timestamp_file);

  auto ctx = proteus::from_cli::olap("adm-play", &argc, &argv);

  std::stringstream ss;
  std::optional<std::ofstream> out = std::nullopt;
  if (!FLAGS_result_file.empty()) {
    out = std::ofstream(FLAGS_result_file);
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
                                      Shaper::NVMECPU, 4, 4, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_cpu_ssb_compressed) {
    LOG(INFO) << "running bench_varybw_cpu_ssb_compressed";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMECPU, 2, 16, true);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu_ssb) {
    LOG(INFO) << "running bench_varybw_gpu_ssb";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMEGPU, 2, 16, false);
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

  if (FLAGS_bench_ssb_gpu_pushdown) {
    LOG(INFO) << "running bench_ssb_gpu_pushdown";
    auto res = bench_ssb_q1_gpu_pushdown_vary_bw(
        FLAGS_scale_factor, FLAGS_server_number, 2, 4, 4, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_ssb_gpu_pushdown_compressed) {
    LOG(INFO) << "running bench_ssb_gpu_pushdown";
    auto res = bench_ssb_q1_gpu_pushdown_vary_bw(
        FLAGS_scale_factor, FLAGS_server_number, 2, 4, 4, true);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_ssb_cpu_socket_pushdown) {
    std::optional<size_t> pushdown_dop =
        (FLAGS_pushdown_dop == -1) ? std::nullopt
                                   : std::make_optional(FLAGS_pushdown_dop);
    LOG(INFO) << "running bench_ssb_cpu_socket_pushdown";
    auto res = bench_ssb_q1_cpu_socket_pushdown_vary_bw(
        FLAGS_scale_factor, FLAGS_server_number, 2, 4, 4, false, pushdown_dop);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  /**
   * Selectivity microbenchmarks
   * ##############################
   */

  if (FLAGS_bench_micro_cpu_socket_pushdown_baseline) {
    LOG(INFO) << "running bench_micro_cpu_socket_pushdown_baseline";
    auto res = bench_micro_cpu_socket_pushdown_baseline_vary_sel(
        FLAGS_server_number, FLAGS_num_iterations, 4, 8, {true, true}, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_stage_both) {
    LOG(INFO) << "running bench_micro_cpu_socket_stage_both";
    auto res = bench_micro_cpu_socket_pushdown_baseline_vary_sel(
        FLAGS_server_number, FLAGS_num_iterations, 4, 8, {false, false}, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_stage_one) {
    LOG(INFO) << "running bench_micro_cpu_socket_stage_one";
    auto res = bench_micro_cpu_socket_pushdown_baseline_vary_sel(
        FLAGS_server_number, FLAGS_num_iterations, 4, 8, {true, false}, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_pushdown_filter) {
    LOG(INFO) << "running bench_micro_cpu_socket_pushdown_filter";
    DCHECK_NE(FLAGS_pushdown_dop, 0) << "cannot have a pushdown DOP of 0. This "
                                        "is not the equivalent of no pushdown";
    std::optional<size_t> pushdown_dop =
        (FLAGS_pushdown_dop == -1) ? std::nullopt
                                   : std::make_optional(FLAGS_pushdown_dop);
    auto res = bench_micro_cpu_socket_pushdown_filter_vary_sel(
        FLAGS_server_number, FLAGS_num_iterations, 4, 8, false, pushdown_dop);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_pushdown_filter_memmove) {
    LOG(INFO) << "running bench_micro_cpu_socket_pushdown_filter_memmove";
    DCHECK_NE(FLAGS_pushdown_dop, 0) << "cannot have a pushdown DOP of 0. This "
                                        "is not the equivalent of no pushdown";
    std::optional<size_t> pushdown_dop =
        (FLAGS_pushdown_dop == -1) ? std::nullopt
                                   : std::make_optional(FLAGS_pushdown_dop);
    auto res = bench_micro_cpu_socket_pushdown_filter_memmove_vary_sel(
        FLAGS_server_number, FLAGS_num_iterations, 4, 8, false, pushdown_dop);
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
