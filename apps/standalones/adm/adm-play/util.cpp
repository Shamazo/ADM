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

#include "util.hpp"

#include <numeric>
#include <platform/util/profiling.hpp>
#include <utility>

std::string get_current_date_str() {
  auto now = std::chrono::system_clock::now();
  std::time_t now_time_t = std::chrono::system_clock::to_time_t(now);
  std::tm* now_tm = std::localtime(&now_time_t);
  char buffer[50];
  std::strftime(buffer, sizeof(buffer), "%Y %B %e %H:%M", now_tm);
  return {buffer};
}

LogTimeRange::LogTimeRange(std::string label, TimeStampLogger* logger,
                           std::string extra)
    : m_label(std::move(label)),
      m_logger(logger),
      m_extra(std::move(extra)),
      m_start_time(std::chrono::high_resolution_clock::now()) {}

LogTimeRange::~LogTimeRange() {
  m_logger->log_end(m_start_time, m_label, m_extra);
}

std::string timepoint_to_string(
    const std::chrono::high_resolution_clock::time_point& tp) {
  // Convert to time_t
  auto duration = tp.time_since_epoch();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
  std::time_t t = std::chrono::system_clock::to_time_t(
      std::chrono::system_clock::time_point(seconds));

  // Convert to struct tm in UTC time
  std::tm* tm = std::gmtime(&t);

  // Format time as "HH:MM:SS"
  std::stringstream ss;
  ss << std::put_time(tm, "%H:%M:%S");

  // Calculate milliseconds
  auto millis =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() %
      1000;

  // Append milliseconds to the formatted time
  ss << ':' << std::setfill('0') << std::setw(3) << millis;

  return ss.str();
}

QueryBenchResult benchmark_query(const std::string& label,
                                 PreparedStatement& statement,
                                 size_t num_iterations) {
  std::vector<std::vector<std::chrono::milliseconds>> pipeline_times(
      num_iterations);
  // warmup
  std::string warmup_query_output;
  {
    LOG(INFO) << "warmup_begin";
    auto ts = global_timestamp_logger->log_time_range(
        "query_warmup", R"("{""query"": "")" + label + "\"\"}\"");
    auto res = statement.execute();
    LOG(INFO) << "warmup_end";
    std::stringstream ss;
    ss << res;
    warmup_query_output = ss.str();
  }
  std::this_thread::sleep_for(std::chrono::seconds(10));
  profiling::ProfileRegionType pr_type = profiling::ProfileRegionType(label);
  for (int i = 0; i < num_iterations; i++) {
    std::this_thread::sleep_for(std::chrono::seconds(4));
    LOG(INFO) << "begin_run_iteration " << i << "/" << num_iterations << " for "
              << label;
    auto ts = global_timestamp_logger->log_time_range(
        "bench_query_execute", R"("{""query"": "")" + label +
                                   R"("", ""iteration"": )" +
                                   std::to_string(i) + "}\"");

    profiling::ProfileRegion pr(pr_type);

    auto res = statement.execute(pipeline_times[i]);
    LOG(INFO) << "end_run_iteration " << i << "/" << num_iterations << " for "
              << label;
    std::stringstream ss;
    ss << res;
    CHECK_EQ(ss.str(), warmup_query_output);
  }

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

  std::vector<std::chrono::milliseconds> mean_per_query_times(num_iterations);
  for (int i = 0; i < num_iterations; i++) {
    mean_per_query_times[i] = std::chrono::milliseconds(
        std::accumulate(pipeline_times[i].begin(), pipeline_times[i].end(),
                        std::chrono::milliseconds(0)));
  }

  QueryBenchResult result;
  result.per_query_times = mean_per_query_times;
  result.label = label;
  result.pipeline_times = mean_pipeline_times;
  result.average_query_time = std::chrono::milliseconds(
      std::accumulate(mean_pipeline_times.begin(), mean_pipeline_times.end(),
                      std::chrono::milliseconds(0)));
  result.query_result = warmup_query_output;
  return result;
}

void check_vector_paths(const std::vector<std::string>& paths) {
  {
    for (const auto& path : paths) {
      CHECK(std::filesystem::exists(path)) << "path not found " << path;
    }
  }
}

std::vector<std::vector<std::string>> get_input_dirs_compressed(
    const int sf, const int server_number) {
  CHECK(sf == 100 || sf == 1000) << "sf is not 100 or 1000";
  if (server_number == 44) {
    if (sf == 100) {
      std::vector<std::string> one_drive = {
          "/scratch2/nicholso/data/compressed_ssbm100"};
      std::vector<std::string> two_drives = {
          "/scratch/nicholso/data/compressed_ssbm100_0_2",
          "/scratch2/nicholso/data/compressed_ssbm100_1_2"};

      check_vector_paths(one_drive);
      check_vector_paths(two_drives);
      return {one_drive, two_drives};
    }
    if (sf == 1000) {
      std::vector<std::string> one_drive = {
          "/scratch2/nicholso/data/compressed_ssbm1000"};
      std::vector<std::string> two_drives = {
          "/scratch/nicholso/data/compressed_ssbm1000_0_2",
          "/scratch2/nicholso/data/compressed_ssbm1000_1_2"};

      check_vector_paths(one_drive);
      check_vector_paths(two_drives);
      return {one_drive, two_drives};
    }
  }

  if (server_number == 46) {
    if (sf == 100) {
      std::vector<std::string> one_drive = {
          "/scratch/nicholso/data/compressed_ssbm100"};
      std::vector<std::string> two_drives = {
          "/scratch/nicholso/data/compressed_ssbm100_0_2",
          "/scratch3/nicholso/data/compressed_ssbm100_1_2"};

      std::vector<std::string> four_drives = {
          "/scratch/nicholso/data/compressed_ssbm100_0_4",
          "/scratch2/nicholso/data/compressed_ssbm100_1_4",
          "/scratch3/nicholso/data/compressed_ssbm100_2_4",
          "/scratch4/nicholso/data/compressed_ssbm100_3_4"};
      check_vector_paths(one_drive);
      check_vector_paths(two_drives);
      check_vector_paths(four_drives);
      return {one_drive, two_drives, four_drives};
    }

    if (sf == 1000) {
      std::vector<std::string> one_drive = {
          "/scratch4/nicholso/data/compressed_ssbm1000"};

      std::vector<std::string> two_drives = {
          "/scratch/nicholso/data/compressed_ssbm1000_0_2",
          "/scratch3/nicholso/data/compressed_ssbm1000_1_2"};

      std::vector<std::string> four_drives = {
          "/scratch/nicholso/data/compressed_ssbm1000_0_4",
          "/scratch2/nicholso/data/compressed_ssbm1000_1_4",
          "/scratch3/nicholso/data/compressed_ssbm1000_2_4",
          "/scratch4/nicholso/data/compressed_ssbm1000_3_4"};
      check_vector_paths(one_drive);
      check_vector_paths(two_drives);
      check_vector_paths(four_drives);
      return {one_drive, two_drives, four_drives};
    }
  }

  if (server_number == 49) {
    if (sf == 100) {
      LOG(FATAL) << "not sf100 on diascld49";
    }

    if (sf == 1000) {
      std::vector<std::string> one_drive = {
          "/nvme14/nicholso/data/compressed_ssbm1000"};

      std::vector<std::string> two_drives = {
          "/nvme12/nicholso/data/compressed_ssbm1000_0_2",
          "/nvme22/nicholso/data/compressed_ssbm1000_1_2"};

      std::vector<std::string> four_drives = {
          "/nvme0/nicholso/data/compressed_ssbm1000_0_4",
          "/nvme13/nicholso/data/compressed_ssbm1000_1_4",
          "/nvme28/nicholso/data/compressed_ssbm1000_2_4",
          "/nvme22/nicholso/data/compressed_ssbm1000_3_4"};

      std::vector<std::string> six_drives = {
          "/nvme0/nicholso/data/compressed_ssbm1000_0_6",
          "/nvme13/nicholso/data/compressed_ssbm1000_1_6",
          "/nvme6/nicholso/data/compressed_ssbm1000_2_6",
          "/nvme28/nicholso/data/compressed_ssbm1000_3_6",
          "/nvme20/nicholso/data/compressed_ssbm1000_4_6",
          "/nvme24/nicholso/data/compressed_ssbm1000_5_6"};

      std::vector<std::string> eight_drives = {
          "/nvme0/nicholso/data/compressed_ssbm1000_0_8",
          "/nvme13/nicholso/data/compressed_ssbm1000_1_8",
          "/nvme14/nicholso/data/compressed_ssbm1000_2_8",
          "/nvme6/nicholso/data/compressed_ssbm1000_3_8",
          "/nvme28/nicholso/data/compressed_ssbm1000_4_8",
          "/nvme20/nicholso/data/compressed_ssbm1000_5_8",
          "/nvme21/nicholso/data/compressed_ssbm1000_6_8",
          "/nvme24/nicholso/data/compressed_ssbm1000_7_8"};

      std::vector<std::string> ten_drives = {
          "/nvme0/nicholso/data/compressed_ssbm1000_0_10",
          "/nvme7/nicholso/data/compressed_ssbm1000_1_10",
          "/nvme13/nicholso/data/compressed_ssbm1000_2_10",
          "/nvme14/nicholso/data/compressed_ssbm1000_3_10",
          "/nvme6/nicholso/data/compressed_ssbm1000_4_10",
          "/nvme28/nicholso/data/compressed_ssbm1000_5_10",
          "/nvme29/nicholso/data/compressed_ssbm1000_6_10",
          "/nvme20/nicholso/data/compressed_ssbm1000_7_10",
          "/nvme21/nicholso/data/compressed_ssbm1000_8_10",
          "/nvme24/nicholso/data/compressed_ssbm1000_9_10"};

      std::vector<std::string> twelve_drives = {
          "/nvme0/nicholso/data/compressed_ssbm1000_0_12",
          "/nvme7/nicholso/data/compressed_ssbm1000_1_12",
          "/nvme13/nicholso/data/compressed_ssbm1000_2_12",
          "/nvme14/nicholso/data/compressed_ssbm1000_3_12",
          "/nvme6/nicholso/data/compressed_ssbm1000_4_12",
          "/nvme9/nicholso/data/compressed_ssbm1000_5_12",
          "/nvme28/nicholso/data/compressed_ssbm1000_6_12",
          "/nvme29/nicholso/data/compressed_ssbm1000_7_12",
          "/nvme20/nicholso/data/compressed_ssbm1000_8_12",
          "/nvme21/nicholso/data/compressed_ssbm1000_9_12",
          "/nvme24/nicholso/data/compressed_ssbm1000_10_12",
          "/nvme25/nicholso/data/compressed_ssbm1000_11_12"};

      std::vector<std::string> sixteen_drives = {
          "/nvme0/nicholso/data/compressed_ssbm1000_0_18",
          "/nvme7/nicholso/data/compressed_ssbm1000_1_18",
          "/nvme2/nicholso/data/compressed_ssbm1000_2_18",
          "/nvme13/nicholso/data/compressed_ssbm1000_3_18",
          "/nvme14/nicholso/data/compressed_ssbm1000_4_18",
          "/nvme15/nicholso/data/compressed_ssbm1000_5_18",
          "/nvme6/nicholso/data/compressed_ssbm1000_6_18",
          "/nvme9/nicholso/data/compressed_ssbm1000_7_18",
          "/nvme10/nicholso/data/compressed_ssbm1000_8_18",
          "/nvme28/nicholso/data/compressed_ssbm1000_9_18",
          "/nvme29/nicholso/data/compressed_ssbm1000_10_18",
          "/nvme30/nicholso/data/compressed_ssbm1000_11_18",
          "/nvme20/nicholso/data/compressed_ssbm1000_12_18",
          "/nvme21/nicholso/data/compressed_ssbm1000_13_18",
          "/nvme22/nicholso/data/compressed_ssbm1000_14_18",
          "/nvme24/nicholso/data/compressed_ssbm1000_15_18",
          "/nvme25/nicholso/data/compressed_ssbm1000_16_18",
          "/nvme26/nicholso/data/compressed_ssbm1000_17_18"};

      std::vector<std::string> twentyfour_drives = {
          "/nvme0/nicholso/data/compressed_ssbm1000_0_24",
          "/nvme7/nicholso/data/compressed_ssbm1000_1_24",
          "/nvme2/nicholso/data/compressed_ssbm1000_2_24",
          "/nvme3/nicholso/data/compressed_ssbm1000_3_24",
          "/nvme12/nicholso/data/compressed_ssbm1000_4_24",
          "/nvme13/nicholso/data/compressed_ssbm1000_5_24",
          "/nvme14/nicholso/data/compressed_ssbm1000_6_24",
          "/nvme15/nicholso/data/compressed_ssbm1000_7_24",
          "/nvme6/nicholso/data/compressed_ssbm1000_8_24",
          "/nvme9/nicholso/data/compressed_ssbm1000_9_24",
          "/nvme10/nicholso/data/compressed_ssbm1000_10_24",
          "/nvme11/nicholso/data/compressed_ssbm1000_11_24",
          "/nvme28/nicholso/data/compressed_ssbm1000_12_24",
          "/nvme29/nicholso/data/compressed_ssbm1000_13_24",
          "/nvme30/nicholso/data/compressed_ssbm1000_14_24",
          "/nvme31/nicholso/data/compressed_ssbm1000_15_24",
          "/nvme20/nicholso/data/compressed_ssbm1000_16_24",
          "/nvme21/nicholso/data/compressed_ssbm1000_17_24",
          "/nvme22/nicholso/data/compressed_ssbm1000_18_24",
          "/nvme23/nicholso/data/compressed_ssbm1000_19_24",
          "/nvme24/nicholso/data/compressed_ssbm1000_20_24",
          "/nvme25/nicholso/data/compressed_ssbm1000_21_24",
          "/nvme26/nicholso/data/compressed_ssbm1000_22_24",
          "/nvme27/nicholso/data/compressed_ssbm1000_23_24"};

      check_vector_paths(one_drive);
      check_vector_paths(two_drives);
      check_vector_paths(four_drives);
      check_vector_paths(six_drives);
      check_vector_paths(eight_drives);
      check_vector_paths(ten_drives);
      check_vector_paths(twelve_drives);
      check_vector_paths(sixteen_drives);
      check_vector_paths(twentyfour_drives);
      //      return {twentyfour_drives};
      return {one_drive,     two_drives,     four_drives,
              six_drives,    eight_drives,   ten_drives,
              twelve_drives, sixteen_drives, twentyfour_drives};
    }
  }

  LOG(FATAL) << "not set up for this server: " << server_number;
}

std::vector<std::string> get_ran_ints_input_dirs_socket_zero_12_drives(
    int server_number) {
  CHECK_EQ(server_number, 49);
  if (server_number == 49) {
    std::vector<std::string> twelve_drives = {
        "/nvme1/nicholso/data/random_ints_0_12",   // node 0
        "/nvme7/nicholso/data/random_ints_1_12",   // node 1
        "/nvme13/nicholso/data/random_ints_2_12",  // node 2
//        "/nvme14/nicholso/data/random_ints_3_12",  // node 2,  problem child
//        "/nvme5/nicholso/data/random_ints_3_12",  // node 1
        "/nvme8/nicholso/data/random_ints_3_12",  // node 3
        "/nvme6/nicholso/data/random_ints_4_12",   // node 1
        "/nvme9/nicholso/data/random_ints_5_12",   // node 3
        "/nvme3/nicholso/data/random_ints_6_12",   // node 0
        "/nvme4/nicholso/data/random_ints_7_12",   // node 1
        "/nvme12/nicholso/data/random_ints_8_12",  // node 2
        "/nvme10/nicholso/data/random_ints_9_12",  // node 3
        "/nvme2/nicholso/data/random_ints_10_12",  // node 0
        "/nvme15/nicholso/data/random_ints_11_12"  // node 2
    };

    check_vector_paths(twelve_drives);

    return twelve_drives;
  }

  LOG(FATAL) << "not set up for this server: " << server_number;
}

std::vector<std::vector<std::string>> get_ran_ints_input_dirs_socket_zero(
    int server_number) {
  if (server_number == 49) {
    std::vector<std::string> two_drives = {
        "/nvme4/nicholso/data/random_ints_0_2",  // node 2 move to nvme 4 on
                                                 // node 1
        "/nvme13/nicholso/data/random_ints_1_2"  // node 3
    };

    std::vector<std::string> four_drives = {
        "/nvme2/nicholso/data/random_ints_0_4",   // node 0
        "/nvme12/nicholso/data/random_ints_1_4",  // node 2
        "/nvme7/nicholso/data/random_ints_2_4",   // node 1
        "/nvme10/nicholso/data/random_ints_3_4"   // node 3
    };

    std::vector<std::string> six_drives = {
        "/nvme0/nicholso/data/random_ints_0_6",   // node 0
        "/nvme13/nicholso/data/random_ints_1_6",  // node 2
        "/nvme6/nicholso/data/random_ints_2_6",   // node 1
        "/nvme9/nicholso/data/random_ints_3_6",   // node 3
        "/nvme3/nicholso/data/random_ints_4_6",   // node 0
        "/nvme7/nicholso/data/random_ints_5_6",   // node 1
    };

    std::vector<std::string> eight_drives = {
        "/nvme2/nicholso/data/random_ints_0_8",   // node 0
        "/nvme13/nicholso/data/random_ints_1_8",  // node 2
        "/nvme12/nicholso/data/random_ints_2_8",  // node 2
        "/nvme6/nicholso/data/random_ints_3_8",   // node 1
        "/nvme3/nicholso/data/random_ints_4_8",   // node 0
        "/nvme10/nicholso/data/random_ints_5_8",  // node 3
        "/nvme9/nicholso/data/random_ints_6_8",   // node 3
        "/nvme4/nicholso/data/random_ints_7_8",   // node 1
    };

    std::vector<std::string> ten_drives = {
        "/nvme2/nicholso/data/random_ints_0_10",   // node 0
        "/nvme7/nicholso/data/random_ints_1_10",   // node 1
        "/nvme13/nicholso/data/random_ints_2_10",  // node 2
        "/nvme12/nicholso/data/random_ints_3_10",  // node 2
        "/nvme6/nicholso/data/random_ints_4_10",   // node 1
        "/nvme3/nicholso/data/random_ints_5_10",   // node 0
        "/nvme4/nicholso/data/random_ints_6_10",   // node 1
        "/nvme11/nicholso/data/random_ints_7_10",  // node 3
        "/nvme9/nicholso/data/random_ints_8_10",   // node 3
        "/nvme5/nicholso/data/random_ints_9_10"    // node 1
    };

    std::vector<std::string> twelve_drives =
        get_ran_ints_input_dirs_socket_zero_12_drives(server_number);

    //    check_vector_paths(one_drive);
    check_vector_paths(two_drives);
    check_vector_paths(four_drives);
    check_vector_paths(six_drives);
    check_vector_paths(eight_drives);
    check_vector_paths(ten_drives);
    check_vector_paths(twelve_drives);
    //      return {twelve_drives}
    return {two_drives,   four_drives, six_drives,
            eight_drives, ten_drives,  twelve_drives};
  }

  LOG(FATAL) << "not set up for this server: " << server_number;
}

std::vector<std::vector<std::string>> get_input_dirs_socket_zero(
    int sf, int server_number) {
  CHECK(sf == 100 || sf == 1000) << "sf is not 100 or 1000";
  if (server_number == 49) {
    if (sf == 100) {
      LOG(FATAL) << "no sf100 on diascld49 yet";
    }

    if (sf == 1000) {
      std::vector<std::string> one_drive = {
          "/nvme11/nicholso/data/sbm1000"  // node 3
      };

      std::vector<std::string> two_drives = {
          "/nvme12/nicholso/data/sbm1000_0_2",  // node 2
          "/nvme11/nicholso/data/sbm1000_1_2"   // node 3
      };

      std::vector<std::string> four_drives = {
          "/nvme0/nicholso/data/sbm1000_0_4",   // node 0
          "/nvme12/nicholso/data/sbm1000_1_4",  // node 2
          "/nvme7/nicholso/data/sbm1000_2_4",   // node 1
          "/nvme10/nicholso/data/sbm1000_3_4"   // node 3
      };

      std::vector<std::string> six_drives = {
          "/nvme0/nicholso/data/sbm1000_0_6",   // node 0
          "/nvme13/nicholso/data/sbm1000_1_6",  // node 2
          "/nvme6/nicholso/data/sbm1000_2_6",   // node 1
          "/nvme9/nicholso/data/sbm1000_3_6",   // node 3
          "/nvme3/nicholso/data/sbm1000_4_6",   // node 0
          "/nvme7/nicholso/data/sbm1000_5_6",   // node 1
      };

      std::vector<std::string> eight_drives = {
          "/nvme0/nicholso/data/sbm1000_0_8",   // node 0
          "/nvme13/nicholso/data/sbm1000_1_8",  // node 2
          "/nvme14/nicholso/data/sbm1000_2_8",  // node 2
          "/nvme6/nicholso/data/sbm1000_3_8",   // node 1
          "/nvme3/nicholso/data/sbm1000_4_8",   // node 0
          "/nvme10/nicholso/data/sbm1000_5_8",  // node 3
          "/nvme9/nicholso/data/sbm1000_6_8",   // node 3
          "/nvme4/nicholso/data/sbm1000_7_8",   // node 1
      };

      std::vector<std::string> ten_drives = {
          "/nvme0/nicholso/data/sbm1000_0_10",   // node 0
          "/nvme7/nicholso/data/sbm1000_1_10",   // node 1
          "/nvme13/nicholso/data/sbm1000_2_10",  // node 2
          "/nvme14/nicholso/data/sbm1000_3_10",  // node 2
          "/nvme6/nicholso/data/sbm1000_4_10",   // node 1
          "/nvme3/nicholso/data/sbm1000_5_10",   // node 0
          "/nvme4/nicholso/data/sbm1000_6_10",   // node 1
          "/nvme11/nicholso/data/sbm1000_7_10",  // node 3
          "/nvme9/nicholso/data/sbm1000_8_10",   // node 3
          "/nvme5/nicholso/data/sbm1000_9_10"    // node 1
      };

      std::vector<std::string> twelve_drives = {
          "/nvme1/nicholso/data/sbm1000_0_12",   // node 0
          "/nvme7/nicholso/data/sbm1000_1_12",   // node 1
          "/nvme13/nicholso/data/sbm1000_2_12",  // node 2
          "/nvme11/nicholso/data/sbm1000_3_12",   // node 3
          "/nvme6/nicholso/data/sbm1000_4_12",   // node 1
          "/nvme9/nicholso/data/sbm1000_5_12",   // node 3
          "/nvme3/nicholso/data/sbm1000_6_12",   // node 0
          "/nvme4/nicholso/data/sbm1000_7_12",   // node 1
          "/nvme12/nicholso/data/sbm1000_8_12",  // node 2
          "/nvme10/nicholso/data/sbm1000_9_12",  // node 3
          "/nvme2/nicholso/data/sbm1000_10_12",  // node 0
          "/nvme15/nicholso/data/sbm1000_11_12"  // node 2
      };
      //      check_vector_paths(one_drive);
      //      check_vector_paths(two_drives);
      //      check_vector_paths(four_drives);
      //      check_vector_paths(six_drives);
      //      check_vector_paths(eight_drives);
      //      check_vector_paths(ten_drives);
      check_vector_paths(twelve_drives);
      return {twelve_drives};
      //      return {one_drive,    two_drives, four_drives,  six_drives,
      //              eight_drives, ten_drives, twelve_drives};
    }
  }
  LOG(FATAL) << "not set up for this server: " << server_number;
}

std::vector<std::vector<std::string>> get_input_dirs(int sf,
                                                     int server_number) {
  CHECK(sf == 100 || sf == 1000) << "sf is not 100 or 1000";
  if (server_number == 44) {
    if (sf == 100) {
      std::vector<std::string> one_drive = {"/scratch/data/ssbm100"};
      std::vector<std::string> two_drives = {
          "/scratch/nicholso/data/ssbm100_0_2",
          "/scratch2/nicholso/data/ssbm100_1_2"};

      check_vector_paths(one_drive);
      check_vector_paths(two_drives);
      return {one_drive, two_drives};
    }
    if (sf == 1000) {
      std::vector<std::string> one_drive = {"/scratch/data/ssbm1000"};
      //      std::vector<std::string> two_drives = {
      //          "/scratch/nicholso/data/ssbm1000_0_2",
      //          "/scratch2/nicholso/data/ssbm1000_1_2"};

      check_vector_paths(one_drive);
      //      check_vector_paths(two_drives);
      return {one_drive};
    }
  }

  if (server_number == 46) {
    if (sf == 100) {
      //      std::vector<std::string> one_drive =
      //      {"/scratch/nicholso/data/ssbm100"};
      //
      //      std::vector<std::string> two_drives = {
      //          "/scratch/nicholso/data/ssbm100_0_2",
      //          "/scratch3/nicholso/data/ssbm100_1_2"};

      std::vector<std::string> four_drives = {
          "/scratch/nicholso/data/ssbm100_0_4",
          "/scratch2/nicholso/data/ssbm100_1_4",
          "/scratch3/nicholso/data/ssbm100_2_4",
          "/scratch4/nicholso/data/ssbm100_3_4"};
      return {four_drives};
      //      check_vector_paths(one_drive);
      //      check_vector_paths(two_drives);
      //      check_vector_paths(four_drives);
      //      return {one_drive, two_drives, four_drives};
    }

    if (sf == 1000) {
      std::vector<std::string> one_drive = {"/scratch/nicholso/data/ssbm1000"};

      std::vector<std::string> two_drives = {
          "/scratch/nicholso/data/ssbm1000_0_2",
          "/scratch3/nicholso/data/ssbm1000_1_2"};

      std::vector<std::string> four_drives = {
          "/scratch/nicholso/data/ssbm1000_0_4",
          "/scratch2/nicholso/data/ssbm1000_1_4",
          "/scratch3/nicholso/data/ssbm1000_2_4",
          "/scratch4/nicholso/data/ssbm1000_3_4"};

      check_vector_paths(one_drive);
      check_vector_paths(two_drives);
      check_vector_paths(four_drives);
      return {one_drive, two_drives, four_drives};
    }
  }

  if (server_number == 49) {
    if (sf == 100) {
      LOG(FATAL) << "not sf100 on diascld49";
    }

    if (sf == 1000) {
      std::vector<std::string> one_drive = {"/nvme11/nicholso/data/sbm1000"};

      std::vector<std::string> two_drives = {
          "/nvme12/nicholso/data/sbm1000_0_2",
          "/nvme23/nicholso/data/sbm1000_1_2"};

      std::vector<std::string> four_drives = {
          "/nvme0/nicholso/data/sbm1000_0_4",
          "/nvme12/nicholso/data/sbm1000_1_4",
          "/nvme28/nicholso/data/sbm1000_2_4",
          "/nvme21/nicholso/data/sbm1000_3_4"};

      std::vector<std::string> six_drives = {
          "/nvme0/nicholso/data/sbm1000_0_6",
          "/nvme13/nicholso/data/sbm1000_1_6",
          "/nvme6/nicholso/data/sbm1000_2_6",
          "/nvme28/nicholso/data/sbm1000_3_6",
          "/nvme20/nicholso/data/sbm1000_4_6",
          "/nvme24/nicholso/data/sbm1000_5_6"};

      std::vector<std::string> eight_drives = {
          "/nvme0/nicholso/data/sbm1000_0_8",
          "/nvme13/nicholso/data/sbm1000_1_8",
          "/nvme14/nicholso/data/sbm1000_2_8",
          "/nvme6/nicholso/data/sbm1000_3_8",
          "/nvme28/nicholso/data/sbm1000_4_8",
          "/nvme20/nicholso/data/sbm1000_5_8",
          "/nvme21/nicholso/data/sbm1000_6_8",
          "/nvme24/nicholso/data/sbm1000_7_8"};

      std::vector<std::string> ten_drives = {
          "/nvme0/nicholso/data/sbm1000_0_10",
          "/nvme7/nicholso/data/sbm1000_1_10",
          "/nvme13/nicholso/data/sbm1000_2_10",
          "/nvme14/nicholso/data/sbm1000_3_10",
          "/nvme6/nicholso/data/sbm1000_4_10",
          "/nvme28/nicholso/data/sbm1000_5_10",
          "/nvme29/nicholso/data/sbm1000_6_10",
          "/nvme20/nicholso/data/sbm1000_7_10",
          "/nvme21/nicholso/data/sbm1000_8_10",
          "/nvme24/nicholso/data/sbm1000_9_10"};

      std::vector<std::string> twelve_drives = {
          "/nvme0/nicholso/data/sbm1000_0_12",
          "/nvme7/nicholso/data/sbm1000_1_12",
          "/nvme13/nicholso/data/sbm1000_2_12",
          "/nvme14/nicholso/data/sbm1000_3_12",
          "/nvme6/nicholso/data/sbm1000_4_12",
          "/nvme9/nicholso/data/sbm1000_5_12",
          "/nvme28/nicholso/data/sbm1000_6_12",
          "/nvme29/nicholso/data/sbm1000_7_12",
          "/nvme20/nicholso/data/sbm1000_8_12",
          "/nvme21/nicholso/data/sbm1000_9_12",
          "/nvme24/nicholso/data/sbm1000_10_12",
          "/nvme25/nicholso/data/sbm1000_11_12"};

      std::vector<std::string> sixteen_drives = {
          "/nvme0/nicholso/data/sbm1000_0_18",
          "/nvme7/nicholso/data/sbm1000_1_18",
          "/nvme2/nicholso/data/sbm1000_2_18",
          "/nvme13/nicholso/data/sbm1000_3_18",
          "/nvme14/nicholso/data/sbm1000_4_18",
          "/nvme15/nicholso/data/sbm1000_5_18",
          "/nvme6/nicholso/data/sbm1000_6_18",
          "/nvme9/nicholso/data/sbm1000_7_18",
          "/nvme10/nicholso/data/sbm1000_8_18",
          "/nvme28/nicholso/data/sbm1000_9_18",
          "/nvme29/nicholso/data/sbm1000_10_18",
          "/nvme30/nicholso/data/sbm1000_11_18",
          "/nvme20/nicholso/data/sbm1000_12_18",
          "/nvme21/nicholso/data/sbm1000_13_18",
          "/nvme22/nicholso/data/sbm1000_14_18",
          "/nvme24/nicholso/data/sbm1000_15_18",
          "/nvme25/nicholso/data/sbm1000_16_18",
          "/nvme26/nicholso/data/sbm1000_17_18"};

      std::vector<std::string> twentyfour_drives = {
          "/nvme0/nicholso/data/sbm1000_0_24",
          "/nvme7/nicholso/data/sbm1000_1_24",
          "/nvme2/nicholso/data/sbm1000_2_24",
          "/nvme3/nicholso/data/sbm1000_3_24",
          "/nvme12/nicholso/data/sbm1000_4_24",
          "/nvme13/nicholso/data/sbm1000_5_24",
          "/nvme14/nicholso/data/sbm1000_6_24",
          "/nvme15/nicholso/data/sbm1000_7_24",
          "/nvme6/nicholso/data/sbm1000_8_24",
          "/nvme9/nicholso/data/sbm1000_9_24",
          "/nvme10/nicholso/data/sbm1000_10_24",
          "/nvme11/nicholso/data/sbm1000_11_24",
          "/nvme28/nicholso/data/sbm1000_12_24",
          "/nvme29/nicholso/data/sbm1000_13_24",
          "/nvme30/nicholso/data/sbm1000_14_24",
          "/nvme31/nicholso/data/sbm1000_15_24",
          "/nvme20/nicholso/data/sbm1000_16_24",
          "/nvme21/nicholso/data/sbm1000_17_24",
          "/nvme22/nicholso/data/sbm1000_18_24",
          "/nvme23/nicholso/data/sbm1000_19_24",
          "/nvme24/nicholso/data/sbm1000_20_24",
          "/nvme25/nicholso/data/sbm1000_21_24",
          "/nvme26/nicholso/data/sbm1000_22_24",
          "/nvme27/nicholso/data/sbm1000_23_24"};

      check_vector_paths(one_drive);
      check_vector_paths(two_drives);
      check_vector_paths(four_drives);
      check_vector_paths(six_drives);
      check_vector_paths(eight_drives);
      check_vector_paths(ten_drives);
      check_vector_paths(twelve_drives);
      check_vector_paths(sixteen_drives);
      check_vector_paths(twentyfour_drives);
      //      return {twentyfour_drives}
      return {one_drive,     two_drives,     four_drives,
              six_drives,    eight_drives,   ten_drives,
              twelve_drives, sixteen_drives, twentyfour_drives};
    }
  }
  LOG(FATAL) << "not set up for this server: " << server_number;
}

std::vector<uint32_t> get_default_pushdown_numa_nodes(int server_number) {
  switch (server_number) {
    case 49:
      return {0, 1, 2, 3};
    case 44:
      return {0};
    default:
      LOG(FATAL) << "unknown server number";
  }
}

std::vector<uint32_t> get_default_compute_numa_nodes(int server_number) {
  switch (server_number) {
    case 49:
      return {4, 5, 6, 7};
    case 46:
      return {1};
    default:
      LOG(FATAL) << "unknown server number";
  }
}
