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

#include <magic_enum.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <query-shaping/nvme-shapers.hpp>

#include "prepared_queries/prepared-queries.hpp"
#include "util.hpp"


/**
 * Arguments for the bench_nvme_vary_sel_micro function
 */
struct VarySelMicroArgs {
  int server_number;
  std::pair<std::function<decltype(scan_sum_micro)>, std::string>
      prep_query_function = {scan_sum_micro, "random_ints_scan_sum"};
  Shaper shaper_type = Shaper::NVMECPU;
  int num_iterations = 5;
  int scan_router_slack = 2;
  int scan_memmove_slack = 4;
  /// should always be false for now, until compression support  is added in
  /// this function.
  bool compressed = false;
  /// degree of parallelism for the pushed down operators
  std::optional<size_t> pushdown_dop = std::nullopt;
  /// For Shaper::NVMECPU only, whether the memmove NVMe->CPU_memory is to
  /// memory local to the current core (true) or memory local to the NVMe drive
  /// (false). 1 bool per column
  std::vector<bool> do_transfer = {false, false};
  /// CPU NUMA Node IDs to affinitize the pushdown ops to.
  std::vector<uint32_t> pushdown_numa_nodes =
      get_default_pushdown_numa_nodes(server_number);
  /// CPU NUMA Node IDs to affinitize the rest of the compute to. Only
  /// applicable to shapers that use CPU (i.e. CPUOnlyNvmeProbeFilterPushdown).
  std::vector<uint32_t> compute_numa_nodes =
      get_default_compute_numa_nodes(server_number);
  std::vector<double> selectivities = {0.0001, 0.001, 0.005, 0.01, 0.02, 0.05,
                                       0.1,    0.2,   0.3,   0.4,  0.5,  0.6,
                                       0.7,    0.8,   0.9,   1.0};
};

std::string bench_nvme_vary_sel_micro(VarySelMicroArgs args) {
  //  pushdown_numa_nodes = get_default_pushdown_numa_nodes(server_number);
  //  compute_numa_nodes = get_default_compute_numa_nodes(server_number);
  CHECK_EQ(args.compressed, false)
      << "operation over compressed data not implemented yet for this function";
  std::stringstream result_string;
  result_string << "query,"
                << "num_drives,"
                << "time_ms,"
                << "date,"
                << "query_output,"
                << "selectivity,"
                << "diascld,"
                << "shaper,"
                << "is_compressed,"
                << "pushdown_dop,"
                << "scan_router_slack,"
                << "scan_memmove_slack,"
                << "do_transfers," << std::endl;

  const auto md_dirs =
      get_ran_ints_input_dirs_socket_zero_12_drives(args.server_number);

  // row count. This is currently hardcoded to the row count of 100GiB of ints
  // and also is not currently used in mem-move for selectivity estimates
  std::map<std::string, std::function<double(proteus::InputPrefixQueryShaper&)>>
      sel_micro_stats = {
          {"random_ints_100GB_10000", [](auto&) { return 26843545600.0; }}};
  for (const double& sel : args.selectivities) {
    /// important, because the relations are all the same from the point of
    /// view of the catalog we need to drop the catalog to ensure we use the
    /// right plugin instance for each configurations of md files
    CatalogParser::getInstance().clear();
    std::unique_ptr<proteus::InputPrefixQueryShaper> shaper;
    // assuming all numa nodes have the same core count
    DegreeOfParallelism pushdown_dop_value = DegreeOfParallelism{
        args.pushdown_dop.value_or(topology::getInstance()
                                       .getCpuNumaNodes()
                                       .front()
                                       .local_cores.size() *
                                   args.pushdown_numa_nodes.size())};
    std::string do_transfers_str = "N/A";

    switch (args.shaper_type) {
      case Shaper::NVMEGPUPUSHDOWN:
        shaper = std::make_unique<proteus::GPUOnlyNVMeProbeFilterPushdown>(
            md_dirs, "inputs/random_ints", sel_micro_stats, true,
            args.scan_memmove_slack, args.scan_router_slack, 16,
            pushdown_dop_value, args.pushdown_numa_nodes);
        break;
      case Shaper::NVMESOCKETPUSHDOWN:
        shaper = std::make_unique<proteus::CPUOnlyNvmeProbeFilterPushdown>(
            md_dirs, "inputs/random_ints", sel_micro_stats, true,
            args.scan_memmove_slack, args.scan_router_slack, 16,
            pushdown_dop_value, args.pushdown_numa_nodes,
            args.compute_numa_nodes);
        break;
      case Shaper::NVMECPU:
        shaper = std::make_unique<proteus::CPUOnlyNVMeMorsel>(
            md_dirs, "inputs/random_ints", sel_micro_stats, true,
            args.scan_memmove_slack, args.scan_router_slack, 16,
            args.do_transfer, args.compute_numa_nodes);
        do_transfers_str = "";
        for (const bool transfer : args.do_transfer) {
          do_transfers_str += transfer ? "true-" : "false-";
        }
        break;
      default:
        LOG(FATAL) << "unknown shaper type";
    }

    auto prep_query = args.prep_query_function.first(*shaper, sel);
    auto ts = global_timestamp_logger->log_time_range(
        "benchmark", R"("{""selectivity"": )" + std::to_string(sel) +
                         R"(, ""query"": "")" +
                         args.prep_query_function.second + "\"\"}\"");
    auto bench_res = benchmark_query(args.prep_query_function.second,
                                     prep_query, args.num_iterations);
    result_string << bench_res.label << "," << md_dirs.size() << ","
                  << bench_res.average_query_time.count() << ","
                  << get_current_date_str() << "," << bench_res.query_result
                  << "," << sel << "," << args.server_number << ","
                  << magic_enum::enum_name(args.shaper_type) << ","
                  << (args.compressed ? "true," : "false,")
                  << pushdown_dop_value << "," << args.scan_router_slack << ","
                  << args.scan_memmove_slack << "," << do_transfers_str
                  << std::endl;
  }
  return result_string.str();
}

/**
 * Arguments for the bench_micro_cpu_adaptive_vary_sel function
 */
struct VarySelMicroAdaptiveArgs {
  int server_number;
  std::pair<std::function<decltype(scan_sum_micro)>, std::string>
      prep_query_function = {scan_sum_micro, "random_ints_scan_sum"};
  Shaper shaper_type = Shaper::NVMECPU;
  int num_iterations = 5;
  /// should always be false for now, until compression support  is added in
  /// this function.
  bool compressed = false;
  /// degree of parallelism for the pushed down operators
  std::optional<size_t> pushdown_dop = std::nullopt;
  int scan_slack = 24;
  /// CPU NUMA Node IDs to affinitize the pushdown ops to.
  std::vector<uint32_t> pushdown_numa_nodes =
      get_default_pushdown_numa_nodes(server_number);
  /// CPU NUMA Node IDs to affinitize the rest of the compute to. Only
  /// applicable to shapers that use CPU (i.e. CPUOnlyNvmeProbeFilterPushdown).
  std::vector<uint32_t> compute_numa_nodes =
      get_default_compute_numa_nodes(server_number);
  std::vector<double> selectivities = {0.0001, 0.001, 0.005, 0.01, 0.02, 0.05,
                                       0.1,    0.2,   0.3,   0.4,  0.5,  0.6,
                                       0.7,    0.8,   0.9,   1.0};
  GeneralizedRoutingPolicy policy =
      GeneralizedRoutingPolicy::DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL;

  std::string header() {
    return "server_number,shaper,compressed,pushdown_dop,scan_slack,policy";
  }
};

std::ostream& operator<<(std::ostream& os,
                         const VarySelMicroAdaptiveArgs& args) {
  os << args.server_number << "," << magic_enum::enum_name(args.shaper_type)
     << "," << (args.compressed ? "true," : "false,")
     << args.pushdown_dop.value_or(0) << "," << args.scan_slack << ","
     << magic_enum::enum_name(args.policy);
  return os;
}

std::string bench_micro_cpu_adaptive_vary_sel(VarySelMicroAdaptiveArgs args) {
  CHECK(args.server_number == 49)
      << "not set up for this server: " << args.server_number;

  std::stringstream result_string;

  result_string << "query,"
                << "num_drives,"
                << "time_ms,"
                << "date,"
                << "query_output,"
                << "selectivity," << args.header() << std::endl;

  const auto md_dirs =
      get_ran_ints_input_dirs_socket_zero_12_drives(args.server_number);

  // row count. This is currently hardcoded to the row count of 100GiB of ints
  // and also is not currently used in mem-move for selectivity estimates
  std::map<std::string, std::function<double(proteus::InputPrefixQueryShaper&)>>
      sel_micro_stats = {
          {"random_ints_100GB_10000", [](auto&) { return 26843545600.0; }}};

  // arguments apart from MD_dirs, input prefix, and stats are not used
  std::unique_ptr<proteus::QueryShaper> shaper =
      std::make_unique<proteus::CPUOnlyNVMeMorsel>(
          md_dirs, "inputs/random_ints", sel_micro_stats, true, 0, 0, 16);

  for (const double& sel : args.selectivities) {
    auto query = scan_sum_micro_adaptive(
        *shaper, sel, DegreeOfParallelism{args.pushdown_dop.value_or(24)},
        args.scan_slack, args.policy);
    auto ts = global_timestamp_logger->log_time_range(
        "benchmark", R"("{""selectivity"": )" + std::to_string(sel) +
                         R"(, ""query"": "")" +
                         args.prep_query_function.second + "\"\"}\"");

    auto bench_res = benchmark_query("adaptive", query, args.num_iterations);

    result_string << bench_res.label << "," << md_dirs.size() << ","
                  << bench_res.average_query_time.count() << ","
                  << get_current_date_str() << "," << bench_res.query_result
                  << "," << sel << "," << args << std::endl;
  }

  return result_string.str();
}

std::string bench_micro_cpu_grouter_pd_vary_sel(VarySelMicroAdaptiveArgs args) {
  CHECK(args.server_number == 49)
      << "not set up for this server: " << args.server_number;

  std::stringstream result_string;

  result_string << "query,"
                << "num_drives,"
                << "time_ms,"
                << "date,"
                << "query_output,"
                << "selectivity," << args.header() << std::endl;

  const auto md_dirs =
      get_ran_ints_input_dirs_socket_zero_12_drives(args.server_number);

  // row count. This is currently hardcoded to the row count of 100GiB of ints
  // and also is not currently used in mem-move for selectivity estimates
  std::map<std::string, std::function<double(proteus::InputPrefixQueryShaper&)>>
      sel_micro_stats = {
          {"random_ints_100GB_10000", [](auto&) { return 26843545600.0; }}};

  // arguments apart from MD_dirs, input prefix, and stats are not used
  std::unique_ptr<proteus::QueryShaper> shaper =
      std::make_unique<proteus::CPUOnlyNVMeMorsel>(
          md_dirs, "inputs/random_ints", sel_micro_stats, true, 0, 0, 16);

  for (const double& sel : args.selectivities) {
    auto query = scan_sum_micro_grouter_pushdown(
        *shaper, sel, DegreeOfParallelism{args.pushdown_dop.value_or(24)},
        args.scan_slack, args.policy);
    auto ts = global_timestamp_logger->log_time_range(
        "benchmark", R"("{""selectivity"": )" + std::to_string(sel) +
                         R"(, ""query"": "")" +
                         args.prep_query_function.second + "\"\"}\"");

    auto bench_res = benchmark_query("grouter_pd", query, args.num_iterations);

    result_string << bench_res.label << "," << md_dirs.size() << ","
                  << bench_res.average_query_time.count() << ","
                  << get_current_date_str() << "," << bench_res.query_result
                  << "," << sel << "," << args << std::endl;
  }

  return result_string.str();
}

std::string bench_micro_cpu_grouter_staging_vary_sel(
    VarySelMicroAdaptiveArgs args) {
  CHECK(args.server_number == 49)
      << "not set up for this server: " << args.server_number;

  std::stringstream result_string;

  result_string << "query,"
                << "num_drives,"
                << "time_ms,"
                << "date,"
                << "query_output,"
                << "selectivity," << args.header() << std::endl;

  const auto md_dirs =
      get_ran_ints_input_dirs_socket_zero_12_drives(args.server_number);

  // row count. This is currently hardcoded to the row count of 100GiB of ints
  // and also is not currently used in mem-move for selectivity estimates
  std::map<std::string, std::function<double(proteus::InputPrefixQueryShaper&)>>
      sel_micro_stats = {
          {"random_ints_100GB_10000", [](auto&) { return 26843545600.0; }}};

  // arguments apart from MD_dirs, input prefix, and stats are not used
  std::unique_ptr<proteus::QueryShaper> shaper =
      std::make_unique<proteus::CPUOnlyNVMeMorsel>(
          md_dirs, "inputs/random_ints", sel_micro_stats, true, 0, 0, 16);

  for (const double& sel : args.selectivities) {
    auto query = scan_sum_micro_grouter_staging(
        *shaper, sel, DegreeOfParallelism{args.pushdown_dop.value_or(24)},
        args.scan_slack, args.policy);
    auto ts = global_timestamp_logger->log_time_range(
        "benchmark", R"("{""selectivity"": )" + std::to_string(sel) +
                         R"(, ""query"": "")" +
                         args.prep_query_function.second + "\"\"}\"");

    auto bench_res =
        benchmark_query("grouter_staging", query, args.num_iterations);

    result_string << bench_res.label << "," << md_dirs.size() << ","
                  << bench_res.average_query_time.count() << ","
                  << get_current_date_str() << "," << bench_res.query_result
                  << "," << sel << "," << args << std::endl;
  }

  return result_string.str();
}

std::string bench_micro_cpu_grouter_staging_partial_sum_vary_sel(
    VarySelMicroAdaptiveArgs args) {
  CHECK(args.server_number == 49)
      << "not set up for this server: " << args.server_number;

  std::stringstream result_string;

  result_string << "query,"
                << "num_drives,"
                << "time_ms,"
                << "date,"
                << "query_output,"
                << "selectivity," << args.header() << std::endl;

  const auto md_dirs =
      get_ran_ints_input_dirs_socket_zero_12_drives(args.server_number);

  // row count. This is currently hardcoded to the row count of 100GiB of ints
  // and also is not currently used in mem-move for selectivity estimates
  std::map<std::string, std::function<double(proteus::InputPrefixQueryShaper&)>>
      sel_micro_stats = {
          {"random_ints_100GB_10000", [](auto&) { return 26843545600.0; }}};

  // arguments apart from MD_dirs, input prefix, and stats are not used
  std::unique_ptr<proteus::QueryShaper> shaper =
      std::make_unique<proteus::CPUOnlyNVMeMorsel>(
          md_dirs, "inputs/random_ints", sel_micro_stats, true, 0, 0, 16);

  for (const double& sel : args.selectivities) {
    auto query = scan_sum_micro_grouter_staging_partial_reduction(
        *shaper, sel, DegreeOfParallelism{args.pushdown_dop.value_or(24)},
        args.scan_slack, args.policy);
    auto ts = global_timestamp_logger->log_time_range(
        "benchmark", R"("{""selectivity"": )" + std::to_string(sel) +
                         R"(, ""query"": "")" +
                         args.prep_query_function.second + "\"\"}\"");

    auto bench_res = benchmark_query("grouter_staging_partial_sums", query,
                                     args.num_iterations);

    result_string << bench_res.label << "," << md_dirs.size() << ","
                  << bench_res.average_query_time.count() << ","
                  << get_current_date_str() << "," << bench_res.query_result
                  << "," << sel << "," << args << std::endl;
  }

  return result_string.str();
}

#endif  // PROTEUS_SELECTIVITY_MICROS_HPP
