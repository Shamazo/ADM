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
#include <vector>

#include "common-flags.hpp"
#include "selectivity-micros.hpp"
#include "util.hpp"

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

DECLARE_bool(bench_micro_cpu_socket_adaptive);
DEFINE_bool(bench_micro_cpu_socket_adaptive, false, "");

DECLARE_bool(bench_micro_cpu_socket_samesocket_baseline);
DEFINE_bool(bench_micro_cpu_socket_samesocket_baseline, false, "");

DECLARE_bool(bench_micro_cpu_socket_grouter_pd);
DEFINE_bool(bench_micro_cpu_socket_grouter_pd, false, "");

DECLARE_bool(bench_micro_cpu_socket_grouter_stage_both);
DEFINE_bool(bench_micro_cpu_socket_grouter_stage_both, false, "");

DECLARE_bool(bench_micro_cpu_socket_grouter_direct);
DEFINE_bool(bench_micro_cpu_socket_grouter_direct, false, "");

DECLARE_bool(bench_micro_cpu_socket_grouter_stage_both_partial_sum);
DEFINE_bool(bench_micro_cpu_socket_grouter_stage_both_partial_sum, false, "");

DECLARE_string(selectivities);
DEFINE_string(selectivities, "", "Comma-separated list of doubles in [0,1]");

DECLARE_string(grouter_policy);
DEFINE_string(grouter_policy, "DISTINCT_THROUGHPUT_SPLIT_PREFER_DATA_LOCAL",
              "Comma-separated list of doubles in [0,1]");

DECLARE_bool(exit_loop);
DEFINE_bool(exit_loop, false,
            "Enter an infinite loop after benchmarks have completed, awaiting "
            "a SIGKILL");

DECLARE_int32(pushdown_dop);
DEFINE_int32(
    pushdown_dop, -1,
    "Number of threads to use for pushed-down operators. Default of -1 is a "
    "thread per core on the socket used for pushdown operators.");

std::vector<double> parseDoubles(const std::string& str) {
  std::vector<double> result;
  std::stringstream ss(str);
  std::string item;
  while (std::getline(ss, item, ',')) {
    result.push_back(std::stod(item));
  }
  return result;
}

TimeStampLogger* global_timestamp_logger;
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  global_timestamp_logger = new TimeStampLogger(FLAGS_timestamp_file);

  auto ctx = proteus::from_cli::olap("selectivity-micros", &argc, &argv);

  std::stringstream ss;
  std::optional<std::ofstream> out_file = std::nullopt;
  if (!FLAGS_result_file.empty()) {
    if (std::filesystem::exists(FLAGS_result_file)) {
      LOG(INFO) << "Result file " << FLAGS_result_file
                << " already exists. Appending to it.";
      out_file = std::ofstream(FLAGS_result_file, std::ios::app);
    } else {
      LOG(INFO) << "Result file " << FLAGS_result_file
                << " does not exist. Creating it.";
      out_file = std::ofstream(FLAGS_result_file);
    }

    CHECK(out_file->is_open())
        << "Could not open result file " << FLAGS_result_file;
  }

  /**
   * Selectivity microbenchmarks
   * ##############################
   */

  if (FLAGS_bench_micro_cpu_socket_samesocket_baseline) {
    /**
     * Fixed BW (12 drives on one socket on dias49). Compute on socket 0, data
     * movement is NVMe->socket0
     */
    LOG(INFO) << "running bench_micro_cpu_socket_samesocket_baseline";
    CHECK_EQ(FLAGS_server_number, 49) << "only setup for dias49 at the moment";
    VarySelMicroArgs args = {
        .server_number = FLAGS_server_number,
        .prep_query_function = {scan_sum_micro,
                                "random_ints_scan_sum_same_socket"},
        .shaper_type = Shaper::NVMECPU,
        .num_iterations = FLAGS_num_iterations,
        .scan_router_slack = 4,
        .scan_memmove_slack = 8,
        .compressed = false,
        .compute_numa_nodes = {0, 1, 2, 3},
        .pushdown_dop = std::nullopt,
        .do_transfer = {true, true}};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    auto res = bench_nvme_vary_sel_micro(args);

    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_pushdown_baseline) {
    /**
     * Fixed BW (12 drives on one socket on dias49). Compute on socket 1, data
     * movement is NVMe->socket1 directly without going through socket 1 memory
     */
    LOG(INFO) << "running bench_micro_cpu_socket_pushdown_baseline";
    CHECK_EQ(FLAGS_server_number, 49) << "only setup for dias49 at the moment";

    VarySelMicroArgs args = {
        .server_number = FLAGS_server_number,
        .prep_query_function = {scan_sum_micro, "random_ints_scan_sum_direct"},
        .shaper_type = Shaper::NVMECPU,
        .num_iterations = FLAGS_num_iterations,
        .scan_router_slack = 4,
        .scan_memmove_slack = 8,
        .compressed = false,
        .pushdown_dop = std::nullopt,
        .do_transfer = {true, true}};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    auto res = bench_nvme_vary_sel_micro(args);

    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_stage_both) {
    /**
     * Fixed BW (12 drives on one socket on dias49). Compute on socket 1, data
     * movement is NVMe->socket0, then accessed by socket 1 CPU
     */
    LOG(INFO) << "running bench_micro_cpu_socket_stage_both";
    CHECK_EQ(FLAGS_server_number, 49) << "only setup for dias49 at the moment";

    VarySelMicroArgs args = {
        .server_number = FLAGS_server_number,
        .prep_query_function = {scan_sum_micro,
                                "random_ints_scan_sum_stage_both"},
        .shaper_type = Shaper::NVMECPU,
        .num_iterations = FLAGS_num_iterations,
        .scan_router_slack = 4,
        .scan_memmove_slack = 8,
        .compressed = false,
        .pushdown_dop = std::nullopt,
        .do_transfer = {false, false}};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    auto res = bench_nvme_vary_sel_micro(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_stage_one) {
    /**
     * Fixed BW (12 drives on one socket on dias49). Compute on socket 1, data
     * movement is NVMe->socket0 for the second column, then accessed by socket
     * 1 CPU. The first column is NVMe->socket1 directly.
     */
    LOG(INFO) << "running bench_micro_cpu_socket_stage_one";
    VarySelMicroArgs args = {
        .server_number = FLAGS_server_number,
        .prep_query_function = {scan_sum_micro,
                                "random_ints_scan_sum_stage_one"},
        .shaper_type = Shaper::NVMECPU,
        .num_iterations = FLAGS_num_iterations,
        .scan_router_slack = 4,
        .scan_memmove_slack = 8,
        .compressed = false,
        .pushdown_dop = std::nullopt,
        .do_transfer = {true, false}};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    auto res = bench_nvme_vary_sel_micro(args);

    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_pushdown_filter) {
    /**
     * Fixed BW (12 drives on one socket on dias49). Compute (the sum operator)
     * runs on socket 1. The data is on NVMe drives on socket 0. The filter runs
     * on socket 0. The filtered data is written back to memory in socket 0 and
     * then accessed directly by the sum operator on socket 1.
     */
    LOG(INFO) << "running bench_micro_cpu_socket_pushdown_filter";
    DCHECK_NE(FLAGS_pushdown_dop, 0) << "cannot have a pushdown DOP of 0. This "
                                        "is not the equivalent of no pushdown";
    CHECK_EQ(FLAGS_server_number, 49) << "only setup for dias49 at the moment";
    std::optional<size_t> pushdown_dop =
        (FLAGS_pushdown_dop == -1) ? std::nullopt
                                   : std::make_optional(FLAGS_pushdown_dop);
    auto ts = global_timestamp_logger->log_time_range(
        "bench_pushdown_filter_" + std::to_string(pushdown_dop.value_or(0)));
    VarySelMicroArgs args = {
        .server_number = FLAGS_server_number,
        .prep_query_function = {[](proteus::QueryShaper& morph, double sel) {
                                  return scan_sum_micro_pushdown(morph, sel,
                                                                 false);
                                },
                                "random_ints_scan_sum_pushdown"},
        .shaper_type = Shaper::NVMESOCKETPUSHDOWN,
        .num_iterations = FLAGS_num_iterations,
        .scan_router_slack = 4,
        .scan_memmove_slack = 8,
        .do_transfer = {false, false},
        .compressed = false,
        .pushdown_dop = pushdown_dop};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    auto res = bench_nvme_vary_sel_micro(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_pushdown_filter_memmove) {
    /**
     * Identical to bench_micro_cpu_socket_pushdown_filter, but the
     * filtered data is first written back to memory in socket 0 and then
     * explicitly moved in blocks to socket 1.
     */
    LOG(INFO) << "running bench_micro_cpu_socket_pushdown_filter_memmove";
    DCHECK_NE(FLAGS_pushdown_dop, 0) << "cannot have a pushdown DOP of 0. This "
                                        "is not the equivalent of no pushdown";
    std::optional<size_t> pushdown_dop =
        (FLAGS_pushdown_dop == -1) ? std::nullopt
                                   : std::make_optional(FLAGS_pushdown_dop);
    VarySelMicroArgs args = {
        .server_number = FLAGS_server_number,
        .prep_query_function = {[](proteus::QueryShaper& morph, double sel) {
                                  return scan_sum_micro_pushdown(morph, sel,
                                                                 false);
                                },
                                "random_ints_scan_sum_pushdown"},
        .shaper_type = Shaper::NVMESOCKETPUSHDOWN,
        .num_iterations = FLAGS_num_iterations,
        .scan_router_slack = 4,
        .scan_memmove_slack = 8,
        .do_transfer = {true, true},
        .compressed = false,
        .pushdown_dop = pushdown_dop};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    auto res = bench_nvme_vary_sel_micro(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_adaptive) {
    LOG(INFO) << "bench_micro_cpu_socket_adaptive";
    auto policy =
        magic_enum::enum_cast<GeneralizedRoutingPolicy>(FLAGS_grouter_policy)
            .value();

    VarySelMicroAdaptiveArgs args = {
        .server_number = FLAGS_server_number,
        .num_iterations = FLAGS_num_iterations,
        .pushdown_dop = FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16,
        .scan_slack = 12,
        .policy = policy};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    auto res = bench_micro_cpu_adaptive_vary_sel(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_grouter_pd) {
    LOG(INFO) << "FLAGS_bench_micro_cpu_socket_grouter_pd";
    auto policy =
        magic_enum::enum_cast<GeneralizedRoutingPolicy>(FLAGS_grouter_policy)
            .value();
    VarySelMicroAdaptiveArgs args = {
        .server_number = FLAGS_server_number,
        .num_iterations = FLAGS_num_iterations,
        .pushdown_dop = FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16,
        .scan_slack = 12,
        .policy = policy};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    auto res = bench_micro_cpu_grouter_pd_vary_sel(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_micro_cpu_socket_grouter_stage_both) {
    auto policy =
        magic_enum::enum_cast<GeneralizedRoutingPolicy>(FLAGS_grouter_policy)
            .value();

    VarySelMicroAdaptiveArgs args = {
        .server_number = FLAGS_server_number,
        .num_iterations = FLAGS_num_iterations,
        .pushdown_dop = FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16,
        .scan_slack = 12,
        .policy = policy};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    LOG(INFO) << "FLAGS_bench_micro_cpu_socket_grouter_stage_both";
    auto res = bench_micro_cpu_grouter_staging_vary_sel(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }


  if (FLAGS_bench_micro_cpu_socket_grouter_direct) {
    auto policy =
        magic_enum::enum_cast<GeneralizedRoutingPolicy>(FLAGS_grouter_policy)
            .value();

    VarySelMicroAdaptiveArgs args = {
        .server_number = FLAGS_server_number,
        .num_iterations = FLAGS_num_iterations,
        .pushdown_dop = FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16,
        .scan_slack = 12,
        .policy = policy};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    LOG(INFO) << "FLAGS_bench_micro_cpu_socket_grouter_direct";
    auto res = bench_micro_cpu_grouter_direct_vary_sel(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }


  if (FLAGS_bench_micro_cpu_socket_grouter_stage_both_partial_sum) {
    auto policy =
        magic_enum::enum_cast<GeneralizedRoutingPolicy>(FLAGS_grouter_policy)
            .value();

    VarySelMicroAdaptiveArgs args = {
        .server_number = FLAGS_server_number,
        .num_iterations = FLAGS_num_iterations,
        .pushdown_dop = FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16,
        .scan_slack = 24,
        .policy = policy};
    if (!FLAGS_selectivities.empty()) {
      args.selectivities = parseDoubles(FLAGS_selectivities);
    }
    LOG(INFO) << "FLAGS_bench_micro_cpu_socket_grouter_stage_both";
    auto res = bench_micro_cpu_grouter_staging_partial_sum_vary_sel(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  std::cout << ss.str();
  auto& sm = StorageManager::getInstance();
  sm.unloadAll();
  if (FLAGS_exit_loop) {
    LOG(INFO) << " benchmarks complete, entering do while loop";
    while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  //  return 0;
}
