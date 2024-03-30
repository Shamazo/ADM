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
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <filesystem>
#include <fstream>
#include <platform/util/logging.hpp>
#include <vector>
namespace fs = std::filesystem;

// Define a command line flag called 'path'.
DEFINE_string(input_directory, "", "Path to the input dataset");
DEFINE_string(
    output_directory, "",
    "Path to the output directory to write the split dataset and metadata");
DECLARE_bool(
    compress_data);  // shouldn't really need this, but it silences a warning
DEFINE_bool(compress_data, false, "Compress data with lz4");

std::vector<std::vector<char>> splitFileIntoChunks(const fs::path& filePath,
                                                   const size_t chunkSize) {
  std::ifstream file(filePath, std::ios::binary | std::ios::ate);
  std::streamsize fileSize = file.tellg();
  file.seekg(0, std::ios::beg);

  std::vector<std::vector<char>> chunks;
  chunks.reserve((fileSize + chunkSize - 1) / chunkSize);

  while (fileSize > chunkSize) {
    std::vector<char> chunk(chunkSize);
    if (!file.read(chunk.data(), chunkSize)) {
      LOG(FATAL) << "Failed to read chunk from file";
    }
    chunks.push_back(std::move(chunk));
    fileSize -= chunkSize;
  }

  if (fileSize > 0) {
    std::vector<char> chunk(fileSize);
    if (file.read(chunk.data(), fileSize)) {
      chunks.push_back(std::move(chunk));
    } else {
      LOG(FATAL) << "Failed to read last chunk from file";
    }
  }

  size_t total_size_bytes = 0;
  for (const auto& chunk : chunks) {
    total_size_bytes += chunk.size();
  }
  CHECK_EQ(total_size_bytes, fs::file_size(filePath));

  return chunks;
}

/**
 *
 * @param chunks vector of chunks to write to files
 * @param numFiles number of files to write the chunks into
 * @param baseFilePath base output file path, not an actual file. e.g.
 * `supplier.csv.s_address` will result in `supplier.csv.s_address1_2`,
 * `supplier.csv.s_address2_2`, etc.
 */
void writeChunksToFiles(const std::vector<std::vector<char>>& chunks,
                        int numFiles, const std::string& baseFilePath) {
  // ceiling division
  const int chunksPerFile = (chunks.size() + numFiles - 1) / numFiles;

  for (int i = 0; i < numFiles; ++i) {
    const fs::path filePath =
        baseFilePath + "_" + std::to_string(i) + "_" + std::to_string(numFiles);
    std::ofstream file(filePath, std::ios::binary);

    rapidjson::Document metadata;
    metadata.SetObject();
    rapidjson::Document::AllocatorType& allocator = metadata.GetAllocator();

    metadata.AddMember("data_format", "", allocator);
    metadata.AddMember("_comment", "generated with adm-partition-ssb",
                       allocator);
    metadata.AddMember("data_file",
                       rapidjson::Value(filePath.filename().c_str(), allocator),
                       allocator);
    metadata.AddMember("num_blocks", 0, allocator);

    rapidjson::Value block_sizes(rapidjson::kArrayType);
    rapidjson::Value block_offsets(rapidjson::kArrayType);

    size_t offset = 0;
    for (int j = 0; j < chunksPerFile && i * chunksPerFile + j < chunks.size();
         ++j) {
      const auto& chunk = chunks[i * chunksPerFile + j];
      file.write(chunk.data(), chunk.size());

      metadata["num_blocks"] = j + 1;
      block_sizes.PushBack(rapidjson::Value().SetInt64(chunk.size()),
                           allocator);
      block_offsets.PushBack(rapidjson::Value().SetInt64(offset), allocator);

      // write padding to align to 4096
      if (chunk.size() % 4096 != 0) {
        for (i = 0; i < 4096 - (chunk.size() % 4096); ++i) {
          file.write("\0", 1);
          offset += 1;
        }
      }

      offset += chunk.size();
    }

    metadata.AddMember("block_sizes", block_sizes, allocator);
    metadata.AddMember("block_offsets", block_offsets, allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    metadata.Accept(writer);

    std::ofstream metaFile(filePath.string() + ".metadata.json");
    metaFile << strbuf.GetString();
  }
}

int main(int argc, char* argv[]) {
  // Parse the command line arguments
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging((argv)[0]);
  FLAGS_colorlogtostderr = true;
  google::LogToStderr();

  CHECK(!FLAGS_input_directory.empty()) << "Input directory not provided";
  CHECK(!FLAGS_output_directory.empty()) << "Output directory not provided";

  fs::path input_dir_path(FLAGS_input_directory);
  fs::path output_dir_path(FLAGS_output_directory);
  CHECK(fs::exists(input_dir_path)) << "Input directory does not exist";
  CHECK(fs::is_directory(input_dir_path))
      << "Input directory is not a directory";
  CHECK(fs::exists(output_dir_path)) << "Output directory does not exist";
  CHECK(fs::is_directory(output_dir_path))
      << "Output directory is not a directory";

  LOG(INFO) << "input directory: " << FLAGS_input_directory;
  LOG(INFO) << "output directory: " << FLAGS_output_directory;
  LOG(INFO) << "compress data: " << FLAGS_compress_data;

  std::vector<fs::path> data_file_paths;
  for (const auto& entry : fs::directory_iterator(input_dir_path)) {
    if (entry.is_regular_file() &&
        entry.path().string().find("dict") == std::string::npos) {
      data_file_paths.push_back(entry.path());
    }
  }
  const size_t chunkSize = 2 * 1024 * 1024;  // 2MiB

  if (FLAGS_compress_data == true) {
    LOG(FATAL) << "Compression not implemented";
  } else {
    for (const auto& data_file_path : data_file_paths) {
      LOG(INFO) << "Splitting file: " << data_file_path;
      auto chunks = splitFileIntoChunks(data_file_path, chunkSize);
      writeChunksToFiles(
          chunks, 2,
          FLAGS_output_directory + "/" + data_file_path.filename().string());
      fs::path dict_path = data_file_path.string() + ".dict";
      if (fs::exists(dict_path)) {
        fs::copy(dict_path,
                 FLAGS_output_directory + "/" + dict_path.filename().string());
      }
    }
  }

  return 0;
}
