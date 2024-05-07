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
#include <olap/plan/prepared-statement.hpp>
#include <platform/util/glog.hpp>
#include <query-shaping/query-shaper.hpp>
#include <string>
#include <vector>

enum class Shaper { NVMECPU, NVMEGPU, NVMEGPUPUSHDOWN };

struct QueryBenchResult {
  std::vector<std::chrono::milliseconds> pipeline_times;
  std::chrono::milliseconds average_query_time;
  std::string label;
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
QueryBenchResult benchmark_query(const std::string &label,
                                 PreparedStatement &statement,
                                 size_t num_iterations);

/*
 * For all combinations of drives. Outer vector is per drive num.
 * Return vector of hardcoded filepaths for the given server and scale factor
 */
std::vector<std::vector<std::string>> get_input_dirs_compressed(
    const int sf, const int server_number);

std::vector<std::vector<std::string>> get_input_dirs(int sf, int server_number);

std::string get_current_date_str();

#endif  // PROTEUS_ADM_BENCH_UTIL_HPP
