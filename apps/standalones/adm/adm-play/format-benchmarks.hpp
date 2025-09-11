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

#ifndef FORMAT_BENCHMARKS_HPP
#define FORMAT_BENCHMARKS_HPP

#include <magic_enum.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <query-shaping/nvme-shapers.hpp>

#include "prepared_queries/prepared-queries.hpp"
#include "util.hpp"

std::vector<std::pair<decltype(&prepare32_pushdown), std::string>>
gpu_pd_ssb_queries() {
  return {
      {prepare11_pushdown, "ssb_Q1.1_pushdown"},
      {prepare12_pushdown, "ssb_Q1.2_pushdown"},
      {prepare21_pushdown, "ssb_Q2.1_pushdown"},
      {prepare22_pushdown, "ssb_Q2.2_pushdown"},
      {prepare23_pushdown, "ssb_Q2.3_pushdown"},
      {prepare31_pushdown, "ssb_Q3.1_pushdown"},
      {prepare32_pushdown, "ssb_Q3.2_pushdown"},
      {prepare33_pushdown, "ssb_Q3.3_pushdown"},
      {prepare34_pushdown, "ssb_Q3.4_pushdown"},
      {prepare41_pushdown, "ssb_Q4.1_pushdown"},
      {prepare42_pushdown, "ssb_Q4.2_pushdown"},
      {prepare43_pushdown, "ssb_Q4.3_pushdown"},
  };
}

std::vector<std::pair<decltype(&ssb::Query::prepare11), std::string>>
standard_ssb_queries() {
  return {
      {ssb::Query::prepare11, "ssb_Q1.1"}, {ssb::Query::prepare12, "ssb_Q1.2"},
      {ssb::Query::prepare13, "ssb_Q1.3"}, {ssb::Query::prepare21, "ssb_Q2.1"},
      {ssb::Query::prepare22, "ssb_Q2.2"}, {ssb::Query::prepare23, "ssb_Q2.3"},
      {ssb::Query::prepare31, "ssb_Q3.1"}, {ssb::Query::prepare32, "ssb_Q3.2"},
      {ssb::Query::prepare33, "ssb_Q3.3"}, {ssb::Query::prepare34, "ssb_Q3.4"},
      {ssb::Query::prepare41, "ssb_Q4.1"}, {ssb::Query::prepare42, "ssb_Q4.2"},
      {ssb::Query::prepare43, "ssb_Q4.3"}};
}

struct FormatBenchArgs {
  int server_number;
  Shaper shaper_type = Shaper::NVMECPU;
  int num_iterations = 5;
  NvmePlugin::CompressionFormat_t compression_type =
      NvmePlugin::CompressionFormat_t::UNCOMPRESSED;
  int scale_factor = 1000;
  int scan_router_slack = 4;
  int scan_memmove_slack = 4;
  int num_drives = 8;
  uint32_t pushdown_dop = 24;
  bool do_pushdown =
      true;  // If false, then with GPUOnlyNVMeProbeFilterPushdown, this
             // effectively becomes staging. If true, then it does probe filter
             // pushdown.
  bool move_after_pushdown =
      true;  // @see prepare31_pushdown. If true, then after the filter
             // pushdown, an explicit memmove is done to move the packed output
             // blocks to the compute socket/device.
  std::vector<uint32_t> pushdown_numa_nodes =
      {};                            // todo rename is both pushdown and compute
                                     // numa nodes
  size_t bloom_filter_size = 256_K;  // in bits

  std::string header() {
    return "server_number,shaper,format,num_drives,pushdown_dop,do_pushdown,"
           "move_after_pushdown,scan_slack,"
           "scan_mm_slack,bloom_filter_size,"
           "scale_factor";
  }
};

std::ostream& operator<<(std::ostream& os, const FormatBenchArgs& args) {
  os << args.server_number << "," << magic_enum::enum_name(args.shaper_type)
     << "," << magic_enum::enum_name(args.compression_type) << ","
     << args.num_drives << "," << args.pushdown_dop << "," << args.do_pushdown
     << "," << args.move_after_pushdown << "," << args.scan_router_slack << ","
     << args.scan_memmove_slack << "," << args.bloom_filter_size
     << ","
     // << magic_enum::enum_name(args.ssb_query_args.policy) << ","
     << args.scale_factor;
  return os;
}

std::string bench_pushdown_ssb(FormatBenchArgs args) {
  CHECK(args.shaper_type == Shaper::NVMEGPUPUSHDOWN)
      << "shaper_type must be NVMEGPUPUSHDOWN. Is "
      << magic_enum::enum_name(args.shaper_type);
  std::stringstream result_string;
  result_string << "query,"
                << "time_ms,"
                << "date," << args.header() << std::endl;

  const auto all_md_dirs =
      get_input_dir_socket_one(args.scale_factor, args.server_number,
                               args.num_drives, args.compression_type);

  for (auto [query, query_name] : gpu_pd_ssb_queries()) {
    for (const auto& md_dirs : all_md_dirs) {
      /// important, because the relations are all the same from the point of
      /// view of the catalog we need to drop the catalog to ensure we use the
      /// right plugin instance for each configurations of md files
      CatalogParser::getInstance().clear();

      // assuming all numa nodes have the same core count
      DegreeOfParallelism pushdown_dop_value =
          DegreeOfParallelism{args.pushdown_dop};

      // Shaper is only used for the plan and to provide the SSB stats
      // no slacks or affinitizers are currently used from the shaper
      std::shared_ptr<proteus::GPUOnlyNVMeProbeFilterPushdown> shaper =
          std::make_unique<proteus::GPUOnlyNVMeProbeFilterPushdown>(
              md_dirs, "inputs/ssbm100",
              ssb::Query::getStats(args.scale_factor), true,
              args.scan_memmove_slack, args.scan_router_slack, 16,
              pushdown_dop_value, args.pushdown_numa_nodes,
              args.bloom_filter_size);

      auto prep_query =
          query(*shaper, args.move_after_pushdown, args.do_pushdown);

      auto bench_res =
          benchmark_query(query_name, prep_query, args.num_iterations);
      for (auto& query_time : bench_res.per_query_times) {
        result_string << bench_res.label << "," << query_time.count() << ","
                      << get_current_date_str() << "," << args << std::endl;
      }
      LOG(INFO) << bench_res.label
                << " average time: " << bench_res.average_query_time.count();
    }
  }

  return result_string.str();
}

std::string bench_ssb(FormatBenchArgs args) {
  std::stringstream result_string;
  result_string << "query,"
                << "time_ms,"
                << "date," << args.header() << std::endl;

  const auto all_md_dirs =
      get_input_dir_socket_one(args.scale_factor, args.server_number,
                               args.num_drives, args.compression_type);

  for (const auto& md_dirs : all_md_dirs) {
    for (auto [query_prep_func, query_name] : standard_ssb_queries()) {
      /// important, because the relations are all the same from the point of
      /// view of the catalog we need to drop the catalog to ensure we use the
      /// right plugin instance for each configurations of md files
      CatalogParser::getInstance().clear();
      std::unique_ptr<proteus::InputPrefixQueryShaper> shaper;
      switch (args.shaper_type) {
        case Shaper::NVMECPU:
          shaper = std::make_unique<proteus::CPUOnlyNVMeMorsel>(
              md_dirs, "inputs/ssbm100",
              ssb::Query::getStats(args.scale_factor), true,
              args.scan_memmove_slack, args.scan_router_slack, 16, std::nullopt,
              args.pushdown_numa_nodes);
          break;
        case Shaper::NVMEGPU:
          shaper = std::make_unique<proteus::GPUOnlyNVMe>(
              md_dirs, "inputs/ssbm100",
              ssb::Query::getStats(args.scale_factor), true,
              args.scan_memmove_slack, args.scan_router_slack, 16);
          break;
        default:
          LOG(FATAL) << "invalid shaper: "
                     << magic_enum::enum_name(args.shaper_type);
      }

      auto prep_query = query_prep_func(*shaper);
      auto bench_res =
          benchmark_query(query_name, prep_query, args.num_iterations);
      for (auto& query_time : bench_res.per_query_times) {
        result_string << bench_res.label << "," << query_time.count() << ","
                      << get_current_date_str() << "," << args << std::endl;
      }
      LOG(INFO) << bench_res.label
                << " average time: " << bench_res.average_query_time.count();
    }
  }
  return result_string.str();
}

#endif  // FORMAT_BENCHMARKS_HPP
