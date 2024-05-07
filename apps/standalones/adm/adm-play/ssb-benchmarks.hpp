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
#ifndef PROTEUS_ADM_SSB_BENCHMARKS_HPP
#define PROTEUS_ADM_SSB_BENCHMARKS_HPP

#include <olap/plan/catalog-parser.hpp>
#include <query-shaping/nvme-shapers.hpp>

#include "util.hpp"

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

std::string bench_queries_nvme_vary_bw(
    int sf, int server_number,
    std::vector<std::pair<decltype(&ssb::Query::prepare11), std::string>>
        queries,
    Shaper shaper_type = Shaper::NVMECPU, int num_iterations = 5,
    int scan_router_slack = 2, int scan_memmove_slack = 4,
    bool compressed = false) {
  std::stringstream result_string;
  result_string << "query,"
                << "is_compressed,"
                << "num_drives,"
                << "diascld,"
                << "time_ms,"
                << "date,"
                << "scan_router_slack,"
                << "scan_memmove_slack,"
                << "shaper" << std::endl;

  const auto all_md_dirs = compressed
                               ? get_input_dirs_compressed(sf, server_number)
                               : get_input_dirs(sf, server_number);

  for (const auto& md_dirs : all_md_dirs) {
    for (auto [query_prep_func, query_name] : queries) {
      /// important, because the relations are all the same from the point of
      /// view of the catalog we need to drop the catalog to ensure we use the
      /// right plugin instance for each configurations of md files
      CatalogParser::getInstance().clear();
      std::unique_ptr<proteus::InputPrefixQueryShaper> shaper;
      switch (shaper_type) {
        case Shaper::NVMECPU:
          shaper = std::make_unique<proteus::CPUOnlyNVMeMorsel>(
              md_dirs, "inputs/ssbm100", ssb::Query::getStats(sf), true,
              scan_memmove_slack, scan_router_slack, 16);
          break;
        case Shaper::NVMEGPU:
          shaper = std::make_unique<proteus::GPUOnlyNVMe>(
              md_dirs, "inputs/ssbm100", ssb::Query::getStats(sf), true,
              scan_memmove_slack, scan_router_slack, 16);
          break;
        case Shaper::NVMEGPUPUSHDOWN:
          shaper = std::make_unique<proteus::GPUOnlyNVMeProbeFilterPushdown>(
              md_dirs, "inputs/ssbm100", ssb::Query::getStats(sf), true,
              scan_memmove_slack, scan_router_slack, 16);
          break;
      }
      //      LOG(INFO) << "magic_enum::enum_name(shaper_type) " <<
      //      magic_enum::enum_name(shaper_type);
      auto prep_query = query_prep_func(*shaper);
      auto bench_res = benchmark_query(query_name, prep_query, num_iterations);
      result_string << bench_res.label << ","
                    << (compressed ? "true," : "false,") << md_dirs.size()
                    << "," << server_number << ","
                    << bench_res.average_query_time.count() << ","
                    << get_current_date_str() << "," << scan_router_slack << ","
                    << scan_memmove_slack << ","
                    << magic_enum::enum_name(shaper_type) << std::endl;
    }
  }
  return result_string.str();
}

std::string bench_ssb_nvme_vary_bw(int sf, int server_number,
                                   Shaper shaper_type = Shaper::NVMECPU,
                                   int num_iterations = 5,
                                   int scan_router_slack = 2,
                                   int scan_memmove_slack = 4,
                                   bool compressed = false) {
  CHECK(sf == 100 || sf == 1000) << "sf is not 100 or 1000";
  return bench_queries_nvme_vary_bw(
      sf, server_number, standard_ssb_queries(), shaper_type, num_iterations,
      scan_router_slack, scan_memmove_slack, compressed);
}

std::string bench_ssb_q1_gpu_pushdown_vary_bw(int sf, int server_number,
                                              int num_iterations = 5,
                                              int scan_router_slack = 2,
                                              int scan_memmove_slack = 4,
                                              bool compressed = false) {
  CHECK(sf == 100 || sf == 1000) << "sf is not 100 or 1000";
  std::vector<std::pair<decltype(&ssb::Query::prepare11), std::string>>
      q1_queries = {{prepare11_pushdown, "ssb_Q1.1_pushdown"},
                    {prepare12_pushdown, "ssb_Q1.2_pushdown"},
                    {prepare13_pushdown, "ssb_Q1.3_pushdown"}};

  return bench_queries_nvme_vary_bw(
      sf, server_number, q1_queries, Shaper::NVMEGPUPUSHDOWN, num_iterations,
      scan_router_slack, scan_memmove_slack, compressed);
}

#endif  // PROTEUS_ADM_SSB_BENCHMARKS_HPP
