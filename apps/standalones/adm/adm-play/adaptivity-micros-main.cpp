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

#include "adaptivity-micros.hpp"
#include "common-flags.hpp"
#include "util.hpp"

DECLARE_int32(pushdown_dop);
DEFINE_int32(
    pushdown_dop, -1,
    "Number of threads to use for pushed-down operators. Default of -1 is a "
    "thread per core on the socket used for pushdown operators.");

DECLARE_int32(scan_slack);
DEFINE_int32(scan_slack, 12, "Slack of the split/grouter");

DECLARE_int32(skip_samples);
DEFINE_int32(skip_samples, 0, "");

DECLARE_bool(bench_vary_samples_scansum);
DEFINE_bool(bench_vary_samples_scansum, false, "");

DECLARE_bool(bench_vary_samples_ssb_q31);
DEFINE_bool(bench_vary_samples_ssb_q31, false, "");

TimeStampLogger* global_timestamp_logger;
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  global_timestamp_logger = new TimeStampLogger(FLAGS_timestamp_file);

  auto ctx = proteus::from_cli::olap("adaptivity-micros", &argc, &argv);

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

  if (FLAGS_bench_vary_samples_scansum) {
    LOG(INFO) << "bench_vary_samples_scansum";
    int skip_samples = FLAGS_skip_samples;
    if (skip_samples != 0){
        skip_samples = 150;
    }
    AdaptiveMicroArgs args = {
        .server_number = FLAGS_server_number,
        .num_iterations = FLAGS_num_iterations,
        .pushdown_dop = FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16,
        .scan_slack = FLAGS_scan_slack,
    .skip_samples = skip_samples};
    auto res = bench_adaptive_micro(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  if (FLAGS_bench_vary_samples_ssb_q31) {
    uint32_t skip_samples = FLAGS_skip_samples;
    if (skip_samples != 0){
      if (FLAGS_use_hyper_threads){
        skip_samples = 225;
      } else {
        skip_samples = 175;
      }
    }

    LOG(INFO) << " skip samples " << skip_samples;
    LOG(INFO) << "bench_vary_samples_ssb_q31";
    AdaptiveSSBArgs args = {
        .ssb_query_args =
            SSBArgs{.do_staging = true,
                    .do_bloom_filter_build = true,
                    .do_bloom_filter_pushdown = true,
                    .do_filter_pushdown = true,
                    .do_direct = true,
                    .policy = GeneralizedRoutingPolicy::
                        DISTINCT_THROUGHPUT_SPLIT_PREFER_DATA_LOCAL,
                    .scan_slack = FLAGS_scan_slack,
                    .skip_first_samples = skip_samples,
                    .use_hyper_threads = FLAGS_use_hyper_threads,
                    .pushdown_numa_nodes =
                        get_default_pushdown_numa_nodes(FLAGS_server_number),
                    .compute_numa_nodes =
                        get_default_compute_numa_nodes(FLAGS_server_number),
                    .pushdown_dop = DegreeOfParallelism{static_cast<size_t>(
                        FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16)}},
        .server_number = FLAGS_server_number,
        .num_iterations = FLAGS_num_iterations};
    auto res = bench_adaptive_ssb31(args);
    ss << res;
    if (out_file.has_value()) {
      *out_file << res << std::endl;
    }
  }

  std::cout << ss.str();
  auto& sm = StorageManager::getInstance();
  sm.unloadAll();
}
