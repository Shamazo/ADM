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

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <fstream>
#include <future>
#include <random>
#include <thread>
#include <vector>

DECLARE_string(output_file);
DEFINE_string(output_file, "", "output file for results");

/**
 * generate 100GiB of random integers and write them to a file
 * @param filename output file
 * @param lower_bound inclusive lower bound
 * @param upper_bound inclusive upper bound
 */
[[clang::optnone]] void generateRandomInts(const std::string& filename,
                                           int lower_bound, int upper_bound) {
  const int seed = 42;
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> dis(lower_bound, upper_bound);

  std::ofstream file(filename, std::ios::binary);

  PCHECK(file.is_open()) << "Unable to open file for writing";

  constexpr size_t total_size = 100ul * 1024 * 1024 * 1024;  // 100 GiB
  constexpr size_t num_ints = total_size / sizeof(int);

  constexpr size_t buffer_size = 16 * 1024 * 1024;  // 16 MiB
  constexpr size_t buffer_int_count = buffer_size / sizeof(int);
  constexpr size_t num_buffers = num_ints / buffer_int_count;

  constexpr int concurrent_bufs = 2;
  std::vector<std::vector<int>> double_buffers(concurrent_bufs);
  for (int i = 0; i < double_buffers.size(); i++) {
    double_buffers[i].resize(buffer_int_count);
  }

  auto generate_buffer = [buffs_ptr = &double_buffers, dis_ptr = &dis,
                          gen_ptr = &gen](size_t k) {
    CHECK_LT(k, concurrent_bufs);
    CHECK_EQ((*buffs_ptr)[k].size(), buffer_int_count);
    for (size_t i = 0; i < buffer_int_count; ++i) {
      (*buffs_ptr)[k][i] = (*dis_ptr)(*gen_ptr);
    }
  };

  size_t curr_buffer = 0;
  std::future<void> future =
      std::async(std::launch::async, generate_buffer, curr_buffer);
  for (int i = 0; i < num_buffers; ++i) {
    LOG_EVERY_N(INFO, 10) << "generating " << i << "/" << num_buffers;
    future.wait();
    const size_t prev_gen_buffer = curr_buffer;
    curr_buffer = (curr_buffer + 1) % concurrent_bufs;
    // check if last buffer
    if (i + 1 != num_buffers) {
      future = std::async(std::launch::async, generate_buffer, curr_buffer);
    }
    file.write(reinterpret_cast<char*>(double_buffers[prev_gen_buffer].data()),
               buffer_size);
  }

  file.close();
}

int main(int argc, char* argv[]) {
  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_colorlogtostderr = true;
  google::LogToStderr();
  CHECK_GT(FLAGS_output_file.size(), 0) << "Output file not specified";
  LOG(INFO) << "will write to " << FLAGS_output_file;

  generateRandomInts(FLAGS_output_file, 1, 10000);
  LOG(INFO) << "done";

  return 0;
}
