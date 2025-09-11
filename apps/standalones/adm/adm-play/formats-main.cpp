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
#include <codegen/expressions/expressionTypes.hpp>
#include <fstream>
#include <magic_enum.hpp>
#include <olap/operators/relbuilder-factory.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <olap/routing/routing-policy-types-v2.hpp>
#include <platform/topology/affinity_manager.hpp>
#include <platform/topology/topology.hpp>
#include <platform/util/profiling.hpp>
#include <query-shaping/nvme-shapers.hpp>
#include <ssb/query.hpp>
#include <vector>

#include "common-flags.hpp"
#include "format-benchmarks.hpp"
#include "microbenchmarks.hpp"
#include "util.hpp"

DECLARE_bool(bench_ssb_gpu_pushdown);
DEFINE_bool(bench_ssb_gpu_pushdown, false,
            "GPU SSB query execution with filter/bloomfilter pushdown to the "
            "CPU and then eagerly moved to the GPU");

DECLARE_bool(bench_ssb_gpu_staging);
DEFINE_bool(bench_ssb_gpu_staging, false,
            "GPU SSB query execution with probe data moved (and decompressed "
            "if relevent) to/on the CPU and then lazily accessed by the GPU");

DECLARE_bool(bench_ssb_gpu);
DEFINE_bool(bench_ssb_gpu, false,
            "GPU SSB query execution with direct NVMe to GPU data movement");

DECLARE_bool(bench_ssb_cpu);
DEFINE_bool(bench_ssb_cpu, false,
            "GPU SSB query execution with direct NVMe to CPU data movement");

DECLARE_int32(pushdown_dop);
DEFINE_int32(pushdown_dop, 24,
             "Number of threads to use for pushed-down operators.");

DECLARE_int32(num_drives);
DEFINE_int32(num_drives, 1, "Number of drives to read from");

DECLARE_string(compression_type);
DEFINE_string(
    compression_type, "UNCOMPRESSED",
    "compression type to use. See NvmePlugin::CompressionFormat_t enum.");

DECLARE_int32(scale_factor);
DEFINE_int32(scale_factor, 1000, "SSB scale factor");

TimeStampLogger* global_timestamp_logger;
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  global_timestamp_logger = new TimeStampLogger(FLAGS_timestamp_file);

  auto ctx = proteus::from_cli::olap("adm-play", &argc, &argv);

  NvmePlugin::CompressionFormat_t compression_type = [&]() {
    auto opt = magic_enum::enum_cast<NvmePlugin::CompressionFormat_t>(
        FLAGS_compression_type, magic_enum::case_insensitive);
    CHECK(opt.has_value()) << "Could not parse compression type "
                           << FLAGS_compression_type
                           << ". Supported types are: " <<
        []() {
          std::string result;
          for (const auto& name :
               magic_enum::enum_names<NvmePlugin::CompressionFormat_t>()) {
            if (!result.empty()) result += ", ";
            result += name;
          }
          return result;
        }();
    return opt.value();
  }();

  std::stringstream ss;
  std::optional<std::ofstream> out = std::nullopt;
  if (!FLAGS_result_file.empty()) {
    if (std::filesystem::exists(FLAGS_result_file)) {
      LOG(INFO) << "Result file " << FLAGS_result_file
                << " already exists. Appending to it.";
      out = std::ofstream(FLAGS_result_file, std::ios::app);
    } else {
      LOG(INFO) << "Result file " << FLAGS_result_file
                << " does not exist. Creating it.";
      out = std::ofstream(FLAGS_result_file);
    }

    CHECK(out->is_open()) << "Could not open result file " << FLAGS_result_file;
  }

  const auto pushdown_dop =
      static_cast<uint32_t>(FLAGS_pushdown_dop != -1 ? FLAGS_pushdown_dop : 16);

  if (FLAGS_bench_ssb_gpu_pushdown) {
    auto res = bench_pushdown_ssb(
        {.server_number = FLAGS_server_number,
         .shaper_type = Shaper::NVMEGPUPUSHDOWN,
         .pushdown_numa_nodes =
             get_default_pushdown_numa_nodes(FLAGS_server_number),
         .bloom_filter_size = 1_M,
         .compression_type = compression_type,
         .pushdown_dop = pushdown_dop,
         .num_iterations = FLAGS_num_iterations,
         .num_drives = FLAGS_num_drives,
         .move_after_pushdown = true,
         .do_pushdown = true});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_ssb_gpu_staging) {
    auto res = bench_pushdown_ssb(
        {.server_number = FLAGS_server_number,
         .shaper_type = Shaper::NVMEGPUPUSHDOWN,
         .pushdown_numa_nodes =
             get_default_pushdown_numa_nodes(FLAGS_server_number),
         .bloom_filter_size = 1_M,
         .compression_type = compression_type,
         .pushdown_dop = pushdown_dop,
         .num_iterations = FLAGS_num_iterations,
         .num_drives = FLAGS_num_drives,
         .move_after_pushdown = false,
         .do_pushdown = false});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_ssb_gpu) {
    auto res = bench_ssb(
        {.server_number = FLAGS_server_number,
         .shaper_type = Shaper::NVMEGPU,
         .pushdown_numa_nodes =
             get_default_pushdown_numa_nodes(FLAGS_server_number),
         .compression_type = compression_type,
         .num_iterations = FLAGS_num_iterations,
         .num_drives = FLAGS_num_drives});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_ssb_cpu) {
    auto res = bench_ssb(
        {.server_number = FLAGS_server_number,
         .shaper_type = Shaper::NVMECPU,
         .pushdown_numa_nodes =
             get_default_pushdown_numa_nodes(FLAGS_server_number),
         .compression_type = compression_type,
         .num_iterations = FLAGS_num_iterations,
         .num_drives = FLAGS_num_drives});
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  std::cout << ss.str();
  auto& sm = StorageManager::getInstance();
  sm.unloadAll();
  return 0;
}
