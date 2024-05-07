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

#ifndef PROTEUS_ADM_MICROBENCHMARKS_HPP
#define PROTEUS_ADM_MICROBENCHMARKS_HPP

#include <olap/plan/catalog-parser.hpp>
#include <query-shaping/nvme-shapers.hpp>

#include "prepared-queries.hpp"
#include "util.hpp"

std::string bench_nvme_vary_bw_compressed(int sf, int server_number,
                                          Shaper shaper_type = Shaper::NVMECPU,
                                          int num_iterations = 5,
                                          int scan_router_slack = 2,
                                          int scan_memmove_slack = 4) {
  CHECK(sf == 100 || sf == 1000) << "sf is not 100 or 1000";
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

  const auto all_md_dirs = get_input_dirs_compressed(sf, server_number);
  for (const auto& md_dirs : all_md_dirs) {
    /// important, because the relations are all the same from the point of view
    /// of the catalog we need to drop the catalog to ensure we use the right
    /// plugin instance for each configurations of md files
    CatalogParser::getInstance().clear();
    LOG(INFO) << "running with " << md_dirs.size() << " md directories";

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
    }

    auto scan_query = small_scan(*shaper, "lo_commitdate");
    auto bench_res =
        benchmark_query("scan_commitdate", scan_query, num_iterations);
    result_string << bench_res.label << ","
                  << "true," << md_dirs.size() << "," << server_number << ","
                  << bench_res.average_query_time.count() << ","
                  << get_current_date_str() << "," << scan_router_slack << ","
                  << scan_memmove_slack << ","
                  << magic_enum::enum_name(shaper_type) << std::endl;
  }
  return result_string.str();
}

std::string bench_nvme_vary_bw(int sf, int server_number,
                               Shaper shaper_type = Shaper::NVMECPU,
                               int num_iterations = 5,
                               int scan_router_slack = 2,
                               int scan_memmove_slack = 4) {
  CHECK(sf == 100 || sf == 1000) << "sf is not 100 or 1000";
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

  const auto all_md_dirs = get_input_dirs(sf, server_number);
  for (const auto& md_dirs : all_md_dirs) {
    /// important, because the relations are all the same from the point of view
    /// of the catalog we need to drop the catalog to ensure we use the right
    /// plugin instance for each configurations of md files
    CatalogParser::getInstance().clear();
    LOG(INFO) << "running with " << md_dirs.size() << " md directories";
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
    }
    const auto prt = profiling::ProfileRegionType(
        std::string("bench_nvme_vary_bw::") +
        std::string(magic_enum::enum_name(shaper_type)));
    auto prof_reg = profiling::ProfileRegion(prt);
    auto scan_query = small_scan(*shaper, "lo_commitdate");
    auto bench_res =
        benchmark_query("scan_commitdate", scan_query, num_iterations);
    result_string << bench_res.label << ","
                  << "false," << md_dirs.size() << "," << server_number << ","
                  << bench_res.average_query_time.count() << ","
                  << get_current_date_str() << "," << scan_router_slack << ","
                  << scan_memmove_slack << ","
                  << magic_enum::enum_name(shaper_type) << std::endl;
  }
  return result_string.str();
}

#endif  // PROTEUS_ADM_MICROBENCHMARKS_HPP
