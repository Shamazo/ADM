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

#ifndef PROTEUS_SELECTIVITY_MICROS_HPP
#define PROTEUS_SELECTIVITY_MICROS_HPP

#include <olap/plan/catalog-parser.hpp>
#include <query-shaping/nvme-shapers.hpp>

#include "util.hpp"

std::vector<uint32_t> get_default_pushdown_numa_nodes(int server_number) {
  switch (server_number) {
    case 49:
      return {0, 1, 2, 3};
    case 44:
      return {0};
    default:
      LOG(FATAL) << "unknown server number";
  }
}

std::vector<uint32_t> get_default_compute_numa_nodes(int server_number) {
  switch (server_number) {
    case 49:
      return {4, 5, 6, 7};
    case 46:
      return {1};
    default:
      LOG(FATAL) << "unknown server number";
  }
}

/**
 *

 * @param compressed should always be false for now, until compression support
 * is added in this function.
 * @param pushdown_dop degree of parallelism for the pushed down operators
 * @param selectivity the selectivity of the filter in range [0, 1]
 * @param do_transfer For Shaper::NVMECPU only, whether the memmove
 * NVMe->CPU_memory is to memory local to the current core (true) or memory
 * local to the NVMe drive (false). 1 bool per column
 *  @param pushdown_numa_nodes CPU NUMA Node IDs to affinitize the pushdown ops
 * to. Currently ignored and set manually
 * @param compute_numa_nodes CPU NUMA Node IDs to affinitize the rest of the
 * compute to. Only applicable to shapers that use CPU (i.e.
 * CPUOnlyNvmeProbeFilterPushdown). Currently ignored and set manually
 */
std::string bench_pushdown_micro_nvme_vary_sel(
    int server_number,
    std::pair<std::function<decltype(scan_sum_micro)>, std::string>
        prep_query_function = {scan_sum_micro, "random_ints_scan_sum"},
    Shaper shaper_type = Shaper::NVMECPU, int num_iterations = 5,
    int scan_router_slack = 2, int scan_memmove_slack = 4,
    bool compressed = false, std::optional<size_t> pushdown_dop = std::nullopt,
    double selectivity = 0.1, std::vector<bool> do_transfer = {false, false},
    std::vector<uint32_t> pushdown_numa_nodes = {0, 1, 2, 3},
    std::vector<uint32_t> compute_numa_nodes = {4, 5, 6, 7}) {
  pushdown_numa_nodes = get_default_pushdown_numa_nodes(server_number);
  compute_numa_nodes = get_default_compute_numa_nodes(server_number);
  CHECK_EQ(compressed, false)
      << "operation over compressed data not implemented yet for this function";

  std::stringstream result_string;
  result_string << "query,"
                << "is_compressed,"
                << "num_drives,"
                << "diascld,"
                << "time_ms,"
                << "date,"
                << "scan_router_slack,"
                << "scan_memmove_slack,"
                << "shaper,"
                << "pushdown_dop,"
                << "selectivity,"
                << "do_transfers,"
                << "query_output," << std::endl;

  const auto md_dirs =
      get_ran_ints_input_dirs_socket_zero_12_drives(server_number);

  // row count. This is currently hardcoded to the row count of 100GiB of ints
  // and also is not currently used in mem-move for selectivity estimates
  std::map<std::string, std::function<double(proteus::InputPrefixQueryShaper&)>>
      sel_micro_stats = {
          {"random_ints_100GB_10000", [](auto&) { return 26843545600.0; }}};
  //    for (const double& sel : {0.0001, 0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4,
  //    0.5,
  //                              0.6, 0.7, 0.8, 0.9, 1.0}) {
  //  for (const double& sel : {0.0001, 0.1, 1.0}) {
  for (const double& sel : {selectivity}) {
    /// important, because the relations are all the same from the point of
    /// view of the catalog we need to drop the catalog to ensure we use the
    /// right plugin instance for each configurations of md files
    CatalogParser::getInstance().clear();
    std::unique_ptr<proteus::InputPrefixQueryShaper> shaper;
    // assuming all numa nodes have the same core count
    DegreeOfParallelism pushdown_dop_value =
        DegreeOfParallelism{pushdown_dop.value_or(topology::getInstance()
                                                      .getCpuNumaNodes()
                                                      .front()
                                                      .local_cores.size() *
                                                  pushdown_numa_nodes.size())};
    std::string do_transfers_str = "N/A";

    switch (shaper_type) {
      case Shaper::NVMEGPUPUSHDOWN:
        shaper = std::make_unique<proteus::GPUOnlyNVMeProbeFilterPushdown>(
            md_dirs, "inputs/random_ints", sel_micro_stats, true,
            scan_memmove_slack, scan_router_slack, 16, pushdown_dop_value,
            pushdown_numa_nodes);
        break;
      case Shaper::NVMESOCKETPUSHDOWN:
        shaper = std::make_unique<proteus::CPUOnlyNvmeProbeFilterPushdown>(
            md_dirs, "inputs/random_ints", sel_micro_stats, true,
            scan_memmove_slack, scan_router_slack, 16, pushdown_dop_value,
            pushdown_numa_nodes, compute_numa_nodes);
        break;
      case Shaper::NVMECPU:
        shaper = std::make_unique<proteus::CPUOnlyNVMeMorsel>(
            md_dirs, "inputs/random_ints", sel_micro_stats, true,
            scan_memmove_slack, scan_router_slack, 16, do_transfer,
            compute_numa_nodes);
        do_transfers_str = "";
        for (const bool transfer : do_transfer) {
          do_transfers_str += transfer ? "true-" : "false-";
        }
        break;
      default:
        LOG(FATAL) << "unknown shaper type";
    }

    auto prep_query = prep_query_function.first(*shaper, sel);
    auto ts =
        global_timestamp_logger->log_time_range("sel_" + std::to_string(sel));
    auto bench_res =
        benchmark_query(prep_query_function.second, prep_query, num_iterations);
    result_string << bench_res.label << "," << (compressed ? "true," : "false,")
                  << md_dirs.size() << "," << server_number << ","
                  << bench_res.average_query_time.count() << ","
                  << get_current_date_str() << "," << scan_router_slack << ","
                  << scan_memmove_slack << ","
                  << magic_enum::enum_name(shaper_type) << ","
                  << pushdown_dop_value << "," << sel << "," << do_transfers_str
                  << "," << bench_res.query_result << std::endl;
  }
  return result_string.str();
}

/**
 * Fixed BW (12 drives on one socket on dias49). Compute on socket 1, data
 * movement is NVMe->socket1 directly without going through socket 1 memory
 */
std::string bench_micro_cpu_socket_pushdown_baseline_vary_sel(
    int server_number, int num_iterations = 5, int scan_router_slack = 2,
    int scan_memmove_slack = 4, std::vector<bool> do_transfer = {true, true},
    bool compressed = false, double selectivity = 0.1) {
  LOG(INFO) << "selectivity " << selectivity;
  CHECK_EQ(server_number, 49) << "only setup for dias49 at the moment";
  CHECK_EQ(do_transfer.size(), 2);
  std::string post_fix = "";
  if (do_transfer[0] && do_transfer[1]) {
    post_fix = "_direct";
  } else if (do_transfer[0] && !do_transfer[1]) {
    post_fix = "_stage_one";
  } else {
    post_fix = "_stage_both";
  }

  auto ts =
      global_timestamp_logger->log_time_range("bench_baseline" + post_fix);

  return bench_pushdown_micro_nvme_vary_sel(
      server_number, {scan_sum_micro, "random_ints_scan_sum" + post_fix},
      Shaper::NVMECPU, num_iterations, 32, 32, compressed, std::nullopt,
      selectivity, do_transfer);
}

/**
 * Fixed BW (12 drives on one socket on dias49). Compute (the sum operator) runs
 * on socket 1. The data is on NVMe drives on socket 0. The filter runs on
 * socket 0. The filtered data is written back to memory in socket 0 and then
 * accessed directly by the sum operator on socket 1.
 * @param pushdown_dop degree of parallelism for the pushed down operators on
 * socket 0
 */
std::string bench_micro_cpu_socket_pushdown_filter_vary_sel(
    int server_number, int num_iterations = 5, int scan_router_slack = 2,
    int scan_memmove_slack = 4, bool compressed = false,
    std::optional<int> pushdown_dop = std::nullopt, double selectivity = 0.1) {
  CHECK_EQ(server_number, 49) << "only setup for dias49 at the moment";
  auto ts = global_timestamp_logger->log_time_range(
      "bench_pushdown_filter_" + std::to_string(pushdown_dop.value_or(0)));

  return bench_pushdown_micro_nvme_vary_sel(
      server_number,
      {[](proteus::QueryShaper& morph, double sel) {
         return scan_sum_micro_pushdown(morph, sel, false);
       },
       "random_ints_scan_sum"},
      Shaper::NVMESOCKETPUSHDOWN, num_iterations, 32, 32, compressed,
      pushdown_dop, selectivity);
}

/**
 * Identical to bench_micro_cpu_socket_pushdown_filter_vary_sel, but the
 * filtered data is first written back to memory in socket 0 and then explicitly
 * moved in blocks to socket 1.
 */
std::string bench_micro_cpu_socket_pushdown_filter_memmove_vary_sel(
    int server_number, int num_iterations = 5, int scan_router_slack = 2,
    int scan_memmove_slack = 4, bool compressed = false,
    std::optional<int> pushdown_dop = std::nullopt, double selectivity = 0.1) {
  CHECK(server_number == 49 || server_number == 46)
      << "not set up for this server: " << server_number;

  auto ts = global_timestamp_logger->log_time_range(
      "bench_pushdown_filter_memmove_" +
      std::to_string(pushdown_dop.value_or(0)));

  return bench_pushdown_micro_nvme_vary_sel(
      server_number,
      {[](proteus::QueryShaper& morph, double sel) {
         return scan_sum_micro_pushdown(morph, sel, true);
       },
       "random_ints_scan_sum"},
      Shaper::NVMESOCKETPUSHDOWN, num_iterations, scan_router_slack,
      scan_memmove_slack, compressed, pushdown_dop, selectivity);
}

#endif  // PROTEUS_SELECTIVITY_MICROS_HPP
