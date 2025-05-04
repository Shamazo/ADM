//
// Created by Hamish Nicholson on 11.01.25.
//

#ifndef PROTEUS_ADAPTIVITY_MICROS_HPP
#define PROTEUS_ADAPTIVITY_MICROS_HPP

#include <string.h>

#include <magic_enum.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <optional>
#include <query-shaping/nvme-shapers.hpp>
#include <ssb/query.hpp>
#include <vector>

#include "prepared_queries/prepared-queries.hpp"
#include "util.hpp"

struct AdaptiveMicroArgs {
  int server_number;
  Shaper shaper_type = Shaper::NVMECPU;
  int num_iterations = 5;
  /// should always be false for now, until compression support  is added in
  /// this function.
  bool compressed = false;
  /// degree of parallelism for the pushed down operators
  std::optional<size_t> pushdown_dop = std::nullopt;
  int scan_slack = 12;
  int skip_samples = 0;
  /// CPU NUMA Node IDs to affinitize the pushdown ops to.
  std::vector<uint32_t> pushdown_numa_nodes =
      get_default_pushdown_numa_nodes(server_number);
  /// CPU NUMA Node IDs to affinitize the rest of the compute to. Only
  /// applicable to shapers that use CPU (i.e. CPUOnlyNvmeProbeFilterPushdown).
  std::vector<uint32_t> compute_numa_nodes =
      get_default_compute_numa_nodes(server_number);
  GeneralizedRoutingPolicy policy =
      GeneralizedRoutingPolicy::DISTINCT_THROUGHPUT_SPLIT_PREFER_DATA_LOCAL;

  std::string header() {
    return "server_number,shaper,compressed,pushdown_dop,scan_slack,policy";
  }
};

std::ostream& operator<<(std::ostream& os, const AdaptiveMicroArgs& args) {
  os << args.server_number << "," << magic_enum::enum_name(args.shaper_type)
     << "," << (args.compressed ? "true," : "false,")
     << args.pushdown_dop.value_or(0) << "," << args.scan_slack << ","
     << magic_enum::enum_name(args.policy);
  return os;
}

std::string bench_adaptive_micro(AdaptiveMicroArgs args) {
  CHECK(args.server_number == 49)
      << "not set up for this server: " << args.server_number;

  std::stringstream result_string;

  result_string << "query,"
                << "num_drives,"
                << "time_ms,"
                << "date,"
                << "query_output,"
                << "num_samples," << args.header() << std::endl;

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

  //  baselines

  {
    auto query = scan_sum_micro_grouter_staging(
        *shaper, 0.5, DegreeOfParallelism{args.pushdown_dop.value_or(24)},
        args.scan_slack, args.policy);

    auto bench_res =
        benchmark_query("staging_scan_sum_sel_0-5", query, args.num_iterations);
    for (const auto& per_query_time : bench_res.per_query_times) {
      result_string << bench_res.label << "," << md_dirs.size() << ","
                    << per_query_time.count() << "," << get_current_date_str()
                    << "," << bench_res.query_result << "," << "n/a" << ","
                    << args << std::endl;
    }
  }

  {
    auto query = scan_sum_micro_grouter_pushdown(
        *shaper, 0.5, DegreeOfParallelism{args.pushdown_dop.value_or(24)},
        args.scan_slack, args.policy);

    auto bench_res = benchmark_query("pushdown_scan_sum_sel_0-5", query,
                                     args.num_iterations);
    for (const auto& per_query_time : bench_res.per_query_times) {
      result_string << bench_res.label << "," << md_dirs.size() << ","
                    << per_query_time.count() << "," << get_current_date_str()
                    << "," << bench_res.query_result << "," << "n/a" << ","
                    << args << std::endl;
    }
  }

  {
    auto query = scan_sum_micro_grouter_direct(
        *shaper, 0.5, DegreeOfParallelism{args.pushdown_dop.value_or(24)},
        args.scan_slack, args.policy);

    auto bench_res =
        benchmark_query("direct_scan_sum_sel_0-5", query, args.num_iterations);
    for (const auto& per_query_time : bench_res.per_query_times) {
      result_string << bench_res.label << "," << md_dirs.size() << ","
                    << per_query_time.count() << "," << get_current_date_str()
                    << "," << bench_res.query_result << "," << "n/a" << ","
                    << args << std::endl;
    }
  }

  for (uint64_t samples : {10, 20, 40, 80, 100, 200, 400, 600, 800, 1000, 2000,
                           4000, 8000, 10000}) {
    if (samples <= args.skip_samples){
      LOG(INFO) << "skipping samples " << samples;
      continue;
    }
    auto query = scan_sum_micro_adaptive(
        *shaper, 0.5, DegreeOfParallelism{args.pushdown_dop.value_or(24)},
        args.scan_slack, args.policy, samples);

    auto bench_res = benchmark_query("adaptive_scan_sum_sel_0-5", query,
                                     args.num_iterations);
    for (const auto& per_query_time : bench_res.per_query_times) {
      result_string << bench_res.label << "," << md_dirs.size() << ","
                    << per_query_time.count() << "," << get_current_date_str()
                    << "," << bench_res.query_result << "," << samples << ","
                    << args << std::endl;
    }
  }

  return result_string.str();
}

struct AdaptiveSSBArgs {
  QueryArgs ssb_query_args;
  int server_number;
  std::pair<std::function<decltype(scan_sum_micro)>, std::string>
      prep_query_function = {scan_sum_micro, "random_ints_scan_sum"};
  Shaper shaper_type = Shaper::NVMECPU;
  int num_iterations = 5;
  /// should always be false for now, until compression support  is added in
  /// this function.
  bool compressed = false;
  int scale_factor = 1000;

  std::string header() {
    return "server_number,shaper,compressed,pushdown_dop,scan_slack,bloom_"
           "filter_size,policy,"
           "scale_factor";
  }
};

std::ostream& operator<<(std::ostream& os, const AdaptiveSSBArgs& args) {
  os << args.server_number << "," << magic_enum::enum_name(args.shaper_type)
     << "," << (args.compressed ? "true," : "false,")
     << args.ssb_query_args.pushdown_dop << ","
     << args.ssb_query_args.scan_slack << ","
     << args.ssb_query_args.bloom_filter_size << ","
     << magic_enum::enum_name(args.ssb_query_args.policy) << ","
     << args.scale_factor;
  return os;
}

std::string bench_adaptive_ssb31(AdaptiveSSBArgs args) {
  CHECK(args.server_number == 49)
      << "not set up for this server: " << args.server_number;

  std::stringstream result_string;

  result_string << "query,"
                << "num_drives,"
                << "time_ms,"
                << "using_hyperthreading,"
                << "samples,"
                << "skip_samples,"
                << "date,"
                << args.header() << std::endl;

  const auto md_dirs =
      get_input_dirs_socket_one(args.scale_factor, args.server_number);

  // arguments apart from MD_dirs, input prefix, and stats are not used
  std::shared_ptr<proteus::CPUOnlyNVMeMorsel> shaper =
      std::make_shared<proteus::CPUOnlyNVMeMorsel>(
          md_dirs.at(0), "inputs/ssbm100",
          ssb::Query::getStats(args.scale_factor), true, 0, 0, 16);

//  for (uint64_t samples : {10, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 225, 250, 275, 300, 350, 400, 500, 600, 800, 1000, 1500, 2000}) {
    for (uint64_t samples : {20, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 350, 400, 500, 600,700,800,900,1000}) {

//    for (uint64_t samples : {20, 225, 1500, 2000, 2500, 3000}) {
    if (samples <= args.ssb_query_args.skip_first_samples){
      LOG(INFO) << "skipping samples " << samples;
      continue;
    }
//  for (uint64_t samples : {250, 300, 400, 500}) {
    QueryArgs ssb_args = args.ssb_query_args;
    ssb_args.morph = shaper;
    ssb_args.num_samples = samples;
    ssb_args.check();

    auto query = prepare31_adaptive(ssb_args);

    auto bench_res =
        benchmark_query("SSB_Q31_adaptive", query, args.num_iterations);
    for (const auto& per_query_time : bench_res.per_query_times) {
      result_string << bench_res.label << "," << md_dirs.size() << ","
                    << per_query_time.count() << ","  << std::boolalpha
                    << args.ssb_query_args.use_hyper_threads << "," << samples << "," <<
          ssb_args.skip_first_samples << "," <<
          get_current_date_str()
                    << "," << args << std::endl;
    }
  }

//  for (uint64_t samples : {20, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 350, 400, 500}) {
////  for (uint64_t samples : {20, 225, 1500, 2000, 2500, 3000}) {
//    if (samples <= args.ssb_query_args.skip_first_samples){
//      LOG(INFO) << "skipping samples " << samples;
//      continue;
//    }
////  for (uint64_t samples : {250, 300, 400, 500}) {
//    SSBArgs ssb_args = args.ssb_query_args;
//    ssb_args.morph = shaper;
//    ssb_args.num_samples = samples;
//    ssb_args.check();
//
//    auto query = prepare34_adaptive(ssb_args);
//
//    auto bench_res =
//        benchmark_query("SSB_Q34_adaptive", query, args.num_iterations);
//    for (const auto& per_query_time : bench_res.per_query_times) {
//      result_string << bench_res.label << "," << md_dirs.size() << ","
//                    << per_query_time.count() << ","  << std::boolalpha
//                    << args.ssb_query_args.use_hyper_threads << "," << samples << "," <<
//          ssb_args.skip_first_samples << "," <<
//          get_current_date_str()
//                    << "," << args << std::endl;
//    }
//  }

  for (uint64_t samples : {20, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 350, 400, 500}) {
    //  for (uint64_t samples : {20, 225, 1500, 2000, 2500, 3000}) {
    if (samples <= args.ssb_query_args.skip_first_samples){
      LOG(INFO) << "skipping samples " << samples;
      continue;
    }
    //  for (uint64_t samples : {250, 300, 400, 500}) {
    QueryArgs ssb_args = args.ssb_query_args;
    ssb_args.morph = shaper;
    ssb_args.num_samples = samples;
    ssb_args.check();

    auto query = prepare11_adaptive(ssb_args);

    auto bench_res =
        benchmark_query("SSB_Q11_adaptive", query, args.num_iterations);
    for (const auto& per_query_time : bench_res.per_query_times) {
      result_string << bench_res.label << "," << md_dirs.size() << ","
                    << per_query_time.count() << ","  << std::boolalpha
                    << args.ssb_query_args.use_hyper_threads << "," << samples << "," <<
          ssb_args.skip_first_samples << "," <<
          get_current_date_str()
                    << "," << args << std::endl;
    }
  }

  return result_string.str();
}
#endif  // PROTEUS_ADAPTIVITY_MICROS_HPP
