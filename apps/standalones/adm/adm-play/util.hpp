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

#ifndef PROTEUS_ADM_BENCH_UTIL_HPP
#define PROTEUS_ADM_BENCH_UTIL_HPP
#include <filesystem>
#include <fstream>
#include <olap/plan/prepared-statement.hpp>
#include <platform/util/glog.hpp>
#include <query-shaping/query-shaper.hpp>
#include <string>
#include <vector>

enum class Shaper { NVMECPU, NVMEGPU, NVMEGPUPUSHDOWN, NVMESOCKETPUSHDOWN };

struct QueryBenchResult {
  std::vector<std::chrono::milliseconds> pipeline_times;
  std::chrono::milliseconds average_query_time;
  std::string label;
  std::string query_result;
};

/**
 * Run a query num_iteration times, after one warm up, and return the average
 * time
 * @param label string to store in the returned QueryBenchResult
 * @param statement query to execute
 * @param num_iterations number of times to execute the query after warming up
 * @return a QueryBenchResult with the average query time and average time per
 * pipeline in the query
 */
QueryBenchResult benchmark_query(const std::string& label,
                                 PreparedStatement& statement,
                                 size_t num_iterations);

/*
 * For all combinations of drives. Outer vector is per drive num.
 * Return vector of hardcoded filepaths for the given server and scale factor
 */
std::vector<std::vector<std::string>> get_input_dirs_compressed(
    const int sf, const int server_number);

std::vector<std::vector<std::string>> get_input_dirs(int sf, int server_number);

std::vector<std::vector<std::string>> get_input_dirs_socket_zero(
    int sf, int server_number);

std::vector<std::string> get_ran_ints_input_dirs_socket_zero_12_drives(
    int server_number);

std::string get_current_date_str();

class TimeStampLogger;

/**
 * Time point to string convertor in format HH:MM:SS:MMM
 * (hours:minutes:seconds:milliseconds)
 */
std::string timepoint_to_string(
    const std::chrono::high_resolution_clock::time_point& tp);

/**
 * Duration to string convertor in format HH:MM:SS:MMM
 * (hours:minutes:seconds:milliseconds)
 */
template <typename Duration>
std::string duration_to_string(const Duration& duration) {
  // Convert the duration to milliseconds
  auto as_milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration);

  // Extract hours, minutes, seconds, and milliseconds
  auto hours = std::chrono::duration_cast<std::chrono::hours>(as_milliseconds);
  auto mins = std::chrono::duration_cast<std::chrono::minutes>(
      as_milliseconds % std::chrono::hours(1));
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(
      as_milliseconds % std::chrono::minutes(1));
  auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(
      as_milliseconds % std::chrono::seconds(1));

  // Format as a string
  std::ostringstream oss;
  oss << std::setw(2) << std::setfill('0') << hours.count() << ':'
      << std::setw(2) << std::setfill('0') << mins.count() << ':'
      << std::setw(2) << std::setfill('0') << secs.count() << ':'
      << std::setw(3) << std::setfill('0') << millis.count();

  return oss.str();
}
class LogTimeRange {
 public:
  LogTimeRange(std::string label, TimeStampLogger* logger,
               std::string extra = "");

  ~LogTimeRange();

 private:
  const std::string m_label;
  const std::string m_extra;
  TimeStampLogger* m_logger;
  const std::chrono::time_point<std::chrono::high_resolution_clock>
      m_start_time;
};

class TimeStampLogger {
 public:
  /**
   * Create a new TimeStampLogger and open the output file. Output file will be
   * created or truncated. Logged times are relative to TimeStampLogger creation
   * @note TimeStampLogger is not thread safe
   * @param output_file_path path to write timestamps to
   */
  TimeStampLogger(const std::filesystem::path& output_file_path)
      : start_time(std::chrono::high_resolution_clock::now()) {
    output_file =
        std::fstream(output_file_path, std::ios::out | std::ios::trunc);
    LOG(INFO) << "opening output file " << output_file_path;
    PCHECK(output_file.is_open()) << "Could not open" << output_file_path;
    output_file << "name,timestamp_start,timestamp_end,extra" << std::endl;
    auto start_epoch = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    log_time_range(
        "start_adm_timestamp",
        R"("{""system_clock_start"": )" + std::to_string(start_epoch) + "}\"");
  }

  LogTimeRange log_time_range(const std::string& label,
                              const std::string& extra = "") {
    return {label, this, extra};
  }

  void log_end(
      std::chrono::time_point<std::chrono::high_resolution_clock> start,
      const std::string& label, const std::string& extra) {
    auto end = std::chrono::high_resolution_clock::now();
    auto start_relative = std::chrono::duration_cast<std::chrono::nanoseconds>(
        start - start_time);
    auto end_relative =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start_time);
    output_file << label << "," << start_relative.count() << ","
                << end_relative.count() << "," << extra << std::endl;
  }

 private:
  std::chrono::time_point<std::chrono::high_resolution_clock> start_time;

  friend class LogTimeRange;

  std::fstream output_file;
};

extern TimeStampLogger* global_timestamp_logger;

std::vector<uint32_t> get_default_pushdown_numa_nodes(int server_number);

std::vector<uint32_t> get_default_compute_numa_nodes(int server_number);

#endif  // PROTEUS_ADM_BENCH_UTIL_HPP
