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

#ifndef TAXI_BENCHMARKS_HPP
#define TAXI_BENCHMARKS_HPP

#include <magic_enum.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <olap/routing/routing-policy-types-v2.hpp>
#include <query-shaping/nvme-shapers.hpp>
#include <taxi/query.hpp>

#include "prepared_queries/prepared-queries.hpp"
#include "util.hpp"

std::vector<std::pair<decltype(&prepare11_adaptive), std::string>>
grouter_ssb_queries() {
  return {
    {prepare_taxi_11_adaptive, "taxi_Q1.1"},
          {prepare_taxi_12_adaptive, "taxi_Q1.2"},
          {prepare_taxi_13_adaptive, "taxi_Q1.3"},
          {prepare_taxi_14_adaptive, "taxi_Q1.4"},
          {prepare_taxi_21_adaptive, "taxi_Q2.1"},
          {prepare_taxi_22_adaptive, "taxi_Q2.2"},
          {prepare_taxi_23_adaptive, "taxi_Q2.3"},
          {prepare_taxi_24_adaptive, "taxi_Q2.4"}};
}

struct TaxiAdaptiveArgs {
  QueryArgs query_args;
  int server_number;
  std::pair<std::function<decltype(scan_sum_micro)>, std::string>
      prep_query_function = {scan_sum_micro, "random_ints_scan_sum"};
  Shaper shaper_type = Shaper::NVMECPU;
  int num_iterations = 5;
  /// should always be false for now, until compression support  is added in
  /// this function.
  bool compressed = false;

  std::string header() {
    return "server_number,shaper,compressed,pushdown_dop,scan_slack,bloom_"
           "filter_size,policy";
  }
};

std::ostream& operator<<(std::ostream& os, const TaxiAdaptiveArgs& args) {
  os << args.server_number << "," << magic_enum::enum_name(args.shaper_type)
     << "," << (args.compressed ? "true," : "false,")
     << args.query_args.pushdown_dop << "," << args.query_args.scan_slack << ","
     << args.query_args.bloom_filter_size << ","
     << magic_enum::enum_name(args.query_args.policy);
  return os;
}

std::string bench_adaptive_taxi(TaxiAdaptiveArgs args) {
  std::stringstream result_string;
  result_string << "query,"
                << "paths,"
                << "num_drives,"
                << "time_ms,"
                << "using_hyperthreading,"
                << "samples,"
                << "skip_samples,"
                << "date," << args.header() << std::endl;

  CHECK(!args.compressed) << "todo";
  const auto all_md_dirs = get_taxi_input_dirs_socket_one(args.server_number);

  for (auto [query, query_name] : grouter_ssb_queries()) {
    for (const auto& md_dirs : all_md_dirs) {
      /// important, because the relations are all the same from the point of
      /// view of the catalog we need to drop the catalog to ensure we use the
      /// right plugin instance for each configurations of md files
      CatalogParser::getInstance().clear();

      // assuming all numa nodes have the same core count
      DegreeOfParallelism pushdown_dop_value =
          DegreeOfParallelism{args.query_args.pushdown_dop};

      // Shaper is only used for the plan and to provide the SSB stats
      // no slacks or affinitizers are currently used from the shaper
      std::shared_ptr<proteus::CPUOnlyNVMeMorsel> shaper =
          std::make_shared<proteus::CPUOnlyNVMeMorsel>(
              md_dirs, "inputs/taxi", taxi::Query::getStats(), true, 4, 24, 16);

      QueryArgs ssb_args = args.query_args;
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
      for (auto& query_time : bench_res.per_query_times) {
        result_string
            << bench_res.label << "," << paths << "," << md_dirs.size() << ","
            << query_time.count() << "," << std::boolalpha
            << args.query_args.use_hyper_threads << ","
            << (args.query_args.policy ==
                        proteus::routing::GeneralizedRoutingPolicyV2::
                            THROUGHPUT_BASED
                    ? 0  // Sampling parameters removed in V2
                    : 0)
            << ","
            << (args.query_args.policy ==
                        proteus::routing::GeneralizedRoutingPolicyV2::
                            THROUGHPUT_BASED
                    ? 0  // Sampling parameters removed in V2
                    : 0)
            << "," << get_current_date_str() << "," << args << std::endl;
      }
      LOG(INFO) << bench_res.label
                << " average time: " << bench_res.average_query_time.count();
    }
  }

  return result_string.str();
}

#endif  // TAXI_BENCHMARKS_HPP
