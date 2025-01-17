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

#ifndef PROTEUS_ADM_PREPARED_QUERIES_HPP
#define PROTEUS_ADM_PREPARED_QUERIES_HPP

#include <olap/plan/prepared-statement.hpp>
#include <olap/routing/routing-policy-types.hpp>
#include <query-shaping/nvme-shapers.hpp>
#include <query-shaping/query-shaper.hpp>

#include "../util.hpp"

PreparedStatement small_scan(proteus::QueryShaper &morph,
                             const std::string &lo_column);

/**
 * Filter on column 1, sum the values of column 2 that pass the filter
 * Operates on data generated with adm-data-gen
 * @param selectivity selectivity of the filter, value in [0, 1]
 * @param move_after_pushdown for CPU execution if true, perform an explicit
 * move after the filter push down. i.e. move the packed output blocks after the
 * filter from the pushdown CPU socket to the compute CPU socket.
 */
PreparedStatement scan_sum_micro_pushdown(proteus::QueryShaper &morph,
                                          double selectivity,
                                          bool move_after_pushdown = false);

/**
 *
 * @param morph Is only used for morph.scan to handle metadata stuff
 * @param selectivity selectivity of the filter, value in [0, 1]
 */
PreparedStatement scan_sum_micro_adaptive(proteus::QueryShaper &morph,
                                          double selectivity,
                                          DegreeOfParallelism pushdown_dop,
                                          int scan_slack,
                                          GeneralizedRoutingPolicy policy);

PreparedStatement scan_sum_micro_grouter_staging(
    proteus::QueryShaper &morph, double selectivity,
    DegreeOfParallelism pushdown_dop, int scan_slack,
    GeneralizedRoutingPolicy policy);

/**
 * Partial reduction before the union all in the same compiled pipeline as the
 * filter
 */
PreparedStatement scan_sum_micro_grouter_staging_partial_reduction(
    proteus::QueryShaper &morph, double selectivity,
    DegreeOfParallelism pushdown_dop, int scan_slack,
    GeneralizedRoutingPolicy policy);

PreparedStatement scan_sum_micro_grouter_pushdown(
    proteus::QueryShaper &morph, double selectivity,
    DegreeOfParallelism pushdown_dop, int scan_slack,
    GeneralizedRoutingPolicy policy);

PreparedStatement scan_sum_micro_grouter_direct(
    proteus::QueryShaper &morph, double selectivity,
    DegreeOfParallelism pushdown_dop, int scan_slack,
    GeneralizedRoutingPolicy policy);

PreparedStatement scan_sum_micro_adaptivev2(proteus::QueryShaper &morph,
                                            double selectivity,
                                            DegreeOfParallelism pushdown_dop,
                                            int scan_slack,
                                            GeneralizedRoutingPolicy policy);

/**
 * The same query as scan_sum_micro_pushdown, but with no pushdown. The filter
 * and summation are in the same pipeline
 * @param selectivity selectivity of the filter, value in [0, 1]
 */
PreparedStatement scan_sum_micro(proteus::QueryShaper &morph,
                                 double selectivity);

PreparedStatement scan_two_columns(proteus::QueryShaper &morph,
                                   const std::string &lo_col1,
                                   const std::string &lo_col2);

PreparedStatement scan_six_columns(proteus::QueryShaper &morph,
                                   const std::string &lo_col1,
                                   const std::string &lo_col2,
                                   const std::string &lo_col3,
                                   const std::string &lo_col4,
                                   const std::string &lo_col5,
                                   const std::string &lo_col6);

/**
 * @param move_after_pushdown for CPU execution if true, perform an explicit
 * move after the filter push down. i.e. move the packed output blocks after the
 * filter from the pushdown CPU socket to the comput CPU socket.
 */
PreparedStatement prepare11_pushdown(proteus::QueryShaper &morph,
                                     bool move_after_pushdown = false);
PreparedStatement prepare12_pushdown(proteus::QueryShaper &morph,
                                     bool move_after_pushdown = false);
PreparedStatement prepare13_pushdown(proteus::QueryShaper &morph,
                                     bool move_after_pushdown = false);

struct SSBArgs {
  std::shared_ptr<proteus::CPUOnlyNVMeMorsel> morph = nullptr;
  DegreeOfParallelism pushdown_dop = DegreeOfParallelism{4};
  int scan_slack = 24;
  GeneralizedRoutingPolicy policy =
      GeneralizedRoutingPolicy::DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL;
  std::vector<uint32_t> pushdown_numa_nodes = {};
  std::vector<uint32_t> compute_numa_nodes = {};
  bool do_direct = true;
  bool do_staging = true;
  bool do_filter_pushdown = false;
  bool do_bloom_filter_build = false;
  bool do_bloom_filter_pushdown = false;
  size_t bloom_filter_size = 1_M;     // in bits
  uint64_t num_samples = 350;         // for throughput policy only
  uint32_t skip_first_samples = 250;  // for throughput policy only
  bool use_hyper_threads = false;
  inline void check() {
    CHECK_NE(morph, nullptr);
    if (do_filter_pushdown) {
      CHECK_GT(pushdown_dop, 0);
      CHECK_GT(pushdown_numa_nodes.size(), 0);
    }
    CHECK_GE(compute_numa_nodes.size(), 0);
    CHECK_EQ(do_bloom_filter_pushdown, do_bloom_filter_build);
    CHECK_GT(scan_slack, 0);
    CHECK_GT(num_samples, skip_first_samples);
    CHECK(!compute_numa_nodes.empty());
  }
};

PreparedStatement prepare11_adaptive(SSBArgs);
PreparedStatement prepare12_adaptive(SSBArgs);
PreparedStatement prepare13_adaptive(SSBArgs);
PreparedStatement prepare21_adaptive(SSBArgs);
PreparedStatement prepare22_adaptive(SSBArgs);
PreparedStatement prepare23_adaptive(SSBArgs);
PreparedStatement prepare31_adaptive(SSBArgs);
PreparedStatement prepare32_adaptive(SSBArgs);
PreparedStatement prepare33_adaptive(SSBArgs);
PreparedStatement prepare34_adaptive(SSBArgs);
PreparedStatement prepare41_adaptive(SSBArgs);
PreparedStatement prepare42_adaptive(SSBArgs);
PreparedStatement prepare43_adaptive(SSBArgs);

#endif  // PROTEUS_ADM_PREPARED_QUERIES_HPP
