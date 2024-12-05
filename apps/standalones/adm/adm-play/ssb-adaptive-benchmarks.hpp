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
#ifndef PROTEUS_ADM_SSB_ADAPTIVE_BENCHMARKS_HPP
#define PROTEUS_ADM_SSB_ADAPTIVE_BENCHMARKS_HPP

#include <magic_enum.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <query-shaping/nvme-shapers.hpp>

#include "prepared_queries/prepared-queries.hpp"
#include "util.hpp"

std::vector<std::pair<decltype(&prepare11_adaptive), std::string>>
grouter_ssb_queries() {
  return {
      {prepare11_adaptive, "grouter_ssb_Q1.1"},
      //      {prepare12_adaptive, "grouter_ssb_Q1.2"},
      //      {prepare13_adaptive, "grouter_ssb_Q1.3"},
      //      {prepare21_adaptive, "grouter_ssb_Q2.1"},
      //      {prepare22_adaptive, "grouter_ssb_Q2.2"},
      //      {prepare23_adaptive, "grouter_ssb_Q2.3"},
      //            {prepare31_adaptive, "grouter_ssb_Q3.1"},
      //      {ssb::Query::prepare32, "ssb_Q3.2"},
      //      {ssb::Query::prepare33, "ssb_Q3.3"}, {ssb::Query::prepare34,
      //      "ssb_Q3.4"}, {ssb::Query::prepare41, "ssb_Q4.1"},
      //      {ssb::Query::prepare42, "ssb_Q4.2"}, {ssb::Query::prepare43,
      //      "ssb_Q4.3"}
  };
}

struct SSBAdaptiveArgs {
  SSBArgs ssb_query_args;
  int server_number;
  std::pair<std::function<decltype(scan_sum_micro)>, std::string>
      prep_query_function = {scan_sum_micro, "random_ints_scan_sum"};
  Shaper shaper_type = Shaper::NVMECPU;
  int num_iterations = 5;
  /// should always be false for now, until compression support  is added in
  /// this function.
  bool compressed = false;
  //  /// degree of parallelism for the pushed down operators
  //  size_t pushdown_dop = 16;
  //  int scan_slack = 24;
  //  /// CPU NUMA Node IDs to affinitize the pushdown ops to.
  //  std::vector<uint32_t> pushdown_numa_nodes =
  //      get_default_pushdown_numa_nodes(server_number);
  //  /// CPU NUMA Node IDs to affinitize the rest of the compute to. Only
  //  /// applicable to shapers that use CPU (i.e.
  //  CPUOnlyNvmeProbeFilterPushdown). std::vector<uint32_t> compute_numa_nodes
  //  =
  //      get_default_compute_numa_nodes(server_number);
  //  GeneralizedRoutingPolicy policy =
  //      GeneralizedRoutingPolicy::DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL;
  int scale_factor = 1000;

  std::string header() {
    return "server_number,shaper,compressed,pushdown_dop,scan_slack,policy,"
           "scale_factor";
  }
};

std::ostream& operator<<(std::ostream& os, const SSBAdaptiveArgs& args) {
  os << args.server_number << "," << magic_enum::enum_name(args.shaper_type)
     << "," << (args.compressed ? "true," : "false,")
     << args.ssb_query_args.pushdown_dop << ","
     << args.ssb_query_args.scan_slack << "," << "bf_size,"
     << args.ssb_query_args.bloom_filter_size << ","
     << magic_enum::enum_name(args.ssb_query_args.policy) << ","
     << args.scale_factor;
  return os;
}

std::string bench_adaptive_ssb(SSBAdaptiveArgs args) {
  std::stringstream result_string;
  result_string << "query,"
                << "paths,"
                << "num_drives,"
                << "time_ms,"
                << "date," << args.header() << std::endl;

  CHECK(!args.compressed) << "todo";
  const auto all_md_dirs =
      get_input_dirs_socket_zero(args.scale_factor, args.server_number);

  for (auto [query, query_name] : grouter_ssb_queries()) {
    for (const auto& md_dirs : all_md_dirs) {
      /// important, because the relations are all the same from the point of
      /// view of the catalog we need to drop the catalog to ensure we use the
      /// right plugin instance for each configurations of md files
      CatalogParser::getInstance().clear();

      // assuming all numa nodes have the same core count
      DegreeOfParallelism pushdown_dop_value =
          DegreeOfParallelism{args.ssb_query_args.pushdown_dop};

      // Shaper is only used for the plan and to provide the SSB stats
      // no slacks or affinitizers are currently used from the shaper
      std::shared_ptr<proteus::CPUOnlyNVMeMorsel> shaper =
          std::make_shared<proteus::CPUOnlyNVMeMorsel>(
              md_dirs, "inputs/ssbm100",
              ssb::Query::getStats(args.scale_factor), true, 4, 24, 16);

      SSBArgs ssb_args = args.ssb_query_args;
      ssb_args.morph = shaper;
      ssb_args.check();

      std::string paths = "";
      auto prep_query = query(ssb_args);
      if (ssb_args.do_bloom_filter_build) {
        paths += "_bf_build";
      }
      if (ssb_args.do_bloom_filter_pushdown) {
        paths += "_bf_pushdown";
      }
      if (ssb_args.do_filter_pushdown) {
        paths += "_filter_pushdown";
      }
      if (ssb_args.do_direct) {
        paths += "_direct";
      }
      if (ssb_args.do_staging) {
        paths += "_staging";
      }

      auto bench_res =
          benchmark_query(query_name, prep_query, args.num_iterations);
      result_string << bench_res.label << "," << paths << "," << md_dirs.size()
                    << "," << bench_res.average_query_time.count() << ","
                    << get_current_date_str() << "," << args << std::endl;
    }
  }

  return result_string.str();
}

// std::string bench_ssb_adaptive(int sf, int server_number,
//                                int num_iterations = 5,
//                                bool compressed = false) {
//   CHECK(sf == 100 || sf == 1000) << "sf is not 100 or 1000";
//   constexpr int num_sockets = 1;
//   return bench_adaptive_ssb(sf, server_number, num_iterations, compressed);
// }

#endif  // PROTEUS_ADM_SSB_ADAPTIVE_BENCHMARKS_HPP
