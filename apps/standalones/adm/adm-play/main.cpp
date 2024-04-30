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
#include <cli-flags.hpp>
#include <codegen/expressions/expressionTypes.hpp>
#include <magic_enum.hpp>
#include <olap/operators/relbuilder-factory.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <platform/topology/affinity_manager.hpp>
#include <platform/topology/topology.hpp>
#include <platform/util/profiling.hpp>
#include <query-shaping/nvme-shapers.hpp>
#include <ssb/query.hpp>
#include <vector>

#include "util.hpp"

struct QueryBenchResult {
  std::vector<std::chrono::milliseconds> pipeline_times;
  std::chrono::milliseconds average_query_time;
  std::string label;
};

QueryBenchResult benchmark_query(const std::string& label,
                                 PreparedStatement& statement,
                                 size_t num_iterations) {
  std::vector<std::vector<std::chrono::milliseconds>> pipeline_times(
      num_iterations);
  // warmup
  statement.execute();
  profiling::ProfileRegionType pr_type = profiling::ProfileRegionType(label);
  for (int i = 0; i < num_iterations; i++) {
    profiling::resume();
    profiling::ProfileRegion pr(pr_type);

    auto res = statement.execute(pipeline_times[i]);
  }
  profiling::pause();

  const size_t num_pipelines = pipeline_times[0].size();
  std::vector<std::chrono::milliseconds> sum_of_pipeline_times(num_pipelines);
  for (auto per_it_pipeline_times : pipeline_times) {
    for (size_t j = 0; j < per_it_pipeline_times.size(); j++) {
      sum_of_pipeline_times.at(j) += per_it_pipeline_times.at(j);
    }
  }

  std::vector<std::chrono::milliseconds> mean_pipeline_times(num_pipelines);
  for (size_t j = 0; j < num_pipelines; j++) {
    mean_pipeline_times[j] =
        std::chrono::milliseconds(sum_of_pipeline_times.at(j) / num_iterations);
  }

  QueryBenchResult result;
  result.label = label;
  result.pipeline_times = mean_pipeline_times;
  result.average_query_time = std::chrono::milliseconds(
      std::accumulate(mean_pipeline_times.begin(), mean_pipeline_times.end(),
                      std::chrono::milliseconds(0)));
  return result;
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
        shaper = std::make_unique<proteus::GPUOnlyNVMeMorsel>(
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
        shaper = std::make_unique<proteus::GPUOnlyNVMeMorsel>(
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

std::string bench_ssb_nvme_vary_bw(int sf, int server_number,
                                   Shaper shaper_type = Shaper::NVMECPU,
                                   int num_iterations = 5,
                                   int scan_router_slack = 2,
                                   int scan_memmove_slack = 4,
                                   bool compressed = false) {
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

  const auto all_md_dirs = compressed
                               ? get_input_dirs_compressed(sf, server_number)
                               : get_input_dirs(sf, server_number);

  for (const auto& md_dirs : all_md_dirs) {
    for (auto [query_prep_func, query_name] :
         std::vector<std::pair<decltype(&ssb::Query::prepare11), std::string>>{
             {ssb::Query::prepare11, "ssb_Q1.1"},
             {ssb::Query::prepare12, "ssb_Q1.2"},
             {ssb::Query::prepare13, "ssb_Q1.3"},
             {ssb::Query::prepare21, "ssb_Q2.1"},
             {ssb::Query::prepare22, "ssb_Q2.2"},
             {ssb::Query::prepare23, "ssb_Q2.3"},
             {ssb::Query::prepare31, "ssb_Q3.1"},
             {ssb::Query::prepare32, "ssb_Q3.2"},
             {ssb::Query::prepare33, "ssb_Q3.3"},
             {ssb::Query::prepare34, "ssb_Q3.4"},
             {ssb::Query::prepare41, "ssb_Q4.1"},
             {ssb::Query::prepare42, "ssb_Q4.2"},
             {ssb::Query::prepare43, "ssb_Q4.3"}}) {
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
          shaper = std::make_unique<proteus::GPUOnlyNVMeMorsel>(
              md_dirs, "inputs/ssbm100", ssb::Query::getStats(sf), true,
              scan_memmove_slack, scan_router_slack, 16);
          break;
      }
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

// shouldn't really need the DECLAREs, but it silences a warning
DECLARE_bool(bench_varybw_cpu);
DEFINE_bool(bench_varybw_cpu, false,
            "CPU only vary number of NVMes used with uncompressed data with a "
            "scan & sum query");

DECLARE_bool(bench_varybw_cpu_compressed);
DEFINE_bool(bench_varybw_cpu_compressed, false,
            "CPU only vary number of NVMes used with compressed data with a "
            "scan & sum query");

DECLARE_bool(bench_varybw_gpu);
DEFINE_bool(bench_varybw_gpu, false,
            "GPU only vary number of NVMes used with uncompressed data with a "
            "scan & sum query");

DECLARE_bool(bench_varybw_gpu_compressed);
DEFINE_bool(bench_varybw_gpu_compressed, false,
            "GPU only vary number of NVMes used with compressed data with a "
            "scan & sum query");

DECLARE_bool(bench_varybw_cpu_ssb);
DEFINE_bool(bench_varybw_cpu_ssb, false,
            "CPU only vary number of NVMes used with uncompressed data using "
            "--scale_factor and varying the number of drives used");

DECLARE_bool(bench_varybw_cpu_ssb_compressed);
DEFINE_bool(bench_varybw_cpu_ssb_compressed, false,
            "CPU only vary number of NVMes used with compressed data using "
            "--scale_factor and varying the number of drives used");

DECLARE_bool(bench_varybw_gpu_ssb);
DEFINE_bool(bench_varybw_gpu_ssb, false,
            "GPU only vary number of NVMes used with uncompressed data using "
            "--scale_factor and varying the number of drives used");

DECLARE_bool(bench_varybw_gpu_ssb_compressed);
DEFINE_bool(bench_varybw_gpu_ssb_compressed, false,
            "GPU only vary number of NVMes used with compressed data using "
            "--scale_factor and varying the number of drives used");

DECLARE_int32(scale_factor);
DEFINE_int32(scale_factor, 100, "SSB scale factor");

DECLARE_int32(server_number);
DEFINE_int32(server_number, 46, "server number (DIAS internal)");

DECLARE_int32(num_iterations);
DEFINE_int32(num_iterations, 5, "Number of types to run each query");

DECLARE_string(result_file);
DEFINE_string(result_file, "",
              "[optional] output file for results [default: stdout]");

int main(int argc, char* argv[]) {
  auto ctx = proteus::from_cli::olap("adm-play", &argc, &argv);

  std::stringstream ss;
  std::optional<std::ofstream> out = std::nullopt;
  if (!FLAGS_result_file.empty()) {
    out = std::ofstream(FLAGS_result_file);
    CHECK(out->is_open()) << "Could not open result file " << FLAGS_result_file;
  }

  if (FLAGS_bench_varybw_cpu) {
    LOG(INFO) << "running bench_varybw_cpu";
    auto res = bench_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                  Shaper::NVMECPU, FLAGS_num_iterations);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_cpu_compressed) {
    LOG(INFO) << "running bench_varybw_cpu_compressed";
    auto res =
        bench_nvme_vary_bw_compressed(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMECPU, FLAGS_num_iterations);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu) {
    LOG(INFO) << "running bench_varybw_gpu";
    auto res = bench_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                  Shaper::NVMEGPU, FLAGS_num_iterations, 8, 8);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu_compressed) {
    LOG(INFO) << "running bench_varybw_gpu_compressed";
    auto res = bench_nvme_vary_bw_compressed(
        FLAGS_scale_factor, FLAGS_server_number, Shaper::NVMEGPU,
        FLAGS_num_iterations, 2, 8);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_cpu_ssb) {
    LOG(INFO) << "running bench_varybw_cpu_ssb";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMECPU, 2, 4, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_cpu_ssb_compressed) {
    LOG(INFO) << "running bench_varybw_cpu_ssb_compressed";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMECPU, 2, 4, true);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu_ssb) {
    LOG(INFO) << "running bench_varybw_gpu_ssb";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMEGPU, 2, 4, false);
    ss << res;
    ss << std::endl;
    if (out.has_value()) {
      *out << res << std::endl;
    }
  }

  if (FLAGS_bench_varybw_gpu_ssb_compressed) {
    LOG(INFO) << "running bench_varybw_gpu_ssb_compressed";
    auto res = bench_ssb_nvme_vary_bw(FLAGS_scale_factor, FLAGS_server_number,
                                      Shaper::NVMEGPU, 2, 4, true);
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
