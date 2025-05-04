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

#include <gflags/gflags.h>
#include <lz4.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <platform/threadpool/threadpool.hpp>
#include <platform/topology/topology.hpp>
#include <platform/util/glog.hpp>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"  // For better error messages

/**
 * @brief Parses a JSON string with a specific structure to extract attribute
 * names and their types for all relations
 *
 * Assumes the JSON structure under the target key follows the pattern:
 * {
 * "relation1": {
 * "type": {
 * "inner": {
 * "attributes": [
 * { "attrName": "name1", "relName": "inputs/taxi/relation1.csv", "type": {
 * "type": "type1" } }, { "attrName": "name2", "relName":
 * "inputs/taxi/relation1.csv", "type": { "type": "type2" } },
 * ...
 * ]
 * }
 * }
 * }
 * }
 * It extracts the mapping from "attrName" to the inner "type" string.
 *
 * @param json_string A C-style string containing the JSON document.
 * @return std::unordered_map<std::string, std::string> Mapping attribute names
 * to their types. Attribute names are prepended with the relName, stripping
 * anything before the last '/'. e.g. in the above example relation1.csv.name1 :
 * type1
 */
std::unordered_map<std::string, std::string> parseAttributeTypes(
    const char* json_string) {
  std::unordered_map<std::string, std::string> attribute_map;
  rapidjson::Document document;

  // 1. Parse the JSON string
  rapidjson::ParseResult parseResult = document.Parse(json_string);
  if (!parseResult) {
    std::cerr << "JSON parse error: "
              << rapidjson::GetParseError_En(parseResult.Code())
              << " at offset " << parseResult.Offset() << std::endl;
    return attribute_map;  // Return empty map on parse error
  }

  // 2. Validate root is an object
  if (!document.IsObject()) {
    LOG(FATAL) << "Error: JSON root is not an object." << std::endl;
  }

  // 4. iterate over relations, assume all are objects
  for (auto const& [key, top_level_value] : document.GetObject()) {
    // 5. Check each value is an object
    if (!top_level_value.IsObject()) {
      LOG(FATAL) << "Error: Value associated with key '" << key.GetString()
                 << "' is not an object." << std::endl;
    }

    // --- Start navigating the structure under the target key ---

    // 6. Access the nested "type" object
    if (!top_level_value.HasMember("type") ||
        !top_level_value["type"].IsObject()) {
      LOG(FATAL) << "Error: Key '" << key.GetString()
                 << "' does not contain a valid 'type' object." << std::endl;
    }
    const rapidjson::Value& type_object = top_level_value["type"];

    // 7. Access the nested "inner" object
    if (!type_object.HasMember("inner") || !type_object["inner"].IsObject()) {
      LOG(FATAL) << "Error: Key '" << key.GetString()
                 << "' does not contain a valid 'type.inner' object."
                 << std::endl;
    }
    const rapidjson::Value& inner_object = type_object["inner"];

    // 8. Access the "attributes" array
    if (!inner_object.HasMember("attributes") ||
        !inner_object["attributes"].IsArray()) {
      LOG(FATAL) << "Error: Key '" << key.GetString()
                 << "' does not contain a valid 'type.inner.attributes' array."
                 << std::endl;
    }
    const rapidjson::Value& attributes_array = inner_object["attributes"];

    // 9. Iterate through the attribute objects in the array
    for (const auto& attribute_obj : attributes_array.GetArray()) {
      if (!attribute_obj.IsObject()) {
        LOG(FATAL) << "Warning: Found non-object element within 'attributes' "
                      "array for key '"
                   << key.GetString() << "'." << std::endl;
      }

      // 10. Extract "attrName" and "relName"
      std::string attr_name;
      if (attribute_obj.HasMember("attrName") &&
          attribute_obj["attrName"].IsString()) {
        attr_name = attribute_obj["attrName"].GetString();
      } else {
        LOG(FATAL) << "Warning: Found attribute object without a valid string "
                      "'attrName' under key '"
                   << key.GetString() << "'." << std::endl;
      }

      std::string rel_name;
      if (attribute_obj.HasMember("relName") &&
          attribute_obj["relName"].IsString()) {
        std::string full_rel_name = attribute_obj["relName"].GetString();
        size_t last_slash_pos = full_rel_name.find_last_of('/');
        if (last_slash_pos != std::string::npos) {
          rel_name = full_rel_name.substr(last_slash_pos + 1);
        }

      } else {
        LOG(FATAL) << "Warning: Found attribute object without a valid string "
                      "'relName' under key '"
                   << key.GetString() << "'." << std::endl;
      }

      // 11. Extract the inner "type" string
      std::string inner_type_str;
      if (attribute_obj.HasMember("type") && attribute_obj["type"].IsObject()) {
        const rapidjson::Value& attr_type_obj = attribute_obj["type"];
        if (attr_type_obj.HasMember("type") &&
            attr_type_obj["type"].IsString()) {
          inner_type_str = attr_type_obj["type"].GetString();
        }
      }

      if (inner_type_str.empty()) {
        LOG(FATAL)
            << "Warning: Could not find inner type string for attribute '"
            << attr_name << "' under key '" << key.GetString() << "'."
            << std::endl;
      }

      // 12. Add to the map
      attribute_map[rel_name + "." + attr_name] = inner_type_str;
    }
  }

  return attribute_map;
}

size_t toProteusTypeSize(const std::string& proteus_type) {
  if (proteus_type == "float") {
    return 8;
  } else if (proteus_type == "dstring") {
    return 4;
  } else if (proteus_type == "int") {
    return 4;
  } else if (proteus_type == "int64") {
    return 4;
  } else if (proteus_type == "datetime") {
    return 8;
  } else {
    LOG(FATAL) << "unknown or unimplemented type: " << proteus_type;
  }
}

namespace fs = std::filesystem;
[[nodiscard]] int decompress_block(const std::vector<uint32_t>& chunk_sizes,
                                   std::span<char> compressed_buffer,
                                   std::span<char> output_buffer,
                                   int max_decomp_chunk_size) {
  int comp_idx = 0;
  int decomp_idx = 0;
  for (const auto& chunk_size : chunk_sizes) {
    auto decompressed_size = LZ4_decompress_safe(
        &compressed_buffer[comp_idx], &output_buffer[decomp_idx], chunk_size,
        max_decomp_chunk_size);
    if (decompressed_size < 0) [[unlikely]] {
      LOG(ERROR) << "failed to decompress block. LZ4 return value: "
                 << decompressed_size << " for chunk size: " << chunk_size
                 << " at comp index: " << comp_idx
                 << " decomp index: " << decomp_idx;
      return decompressed_size;
    }

    comp_idx += chunk_size;
    decomp_idx += decompressed_size;
  }
  DCHECK_LE(decomp_idx, output_buffer.size());
  return 0;
}

DEFINE_string(input_directory, "", "Path to the input dataset");
DECLARE_int32(
    num_files);  // shouldn't really need this, but it silences a warning
DEFINE_int32(num_files, 2, "Number of files to split the dataset into");
DECLARE_bool(
    compress_data);  // shouldn't really need this, but it silences a warning
DEFINE_bool(compress_data, false, "Compress data with lz4");

DEFINE_string(output_directories, "",
              "Comma delimited list of output directories");

// Align to 4KiB for GDS
// https://docs.nvidia.com/gpudirect-storage/best-practices-guide/index.html#cufile-bufregister-fileread-filewrite
// 512 is sufficient of IO_DIRECT
static constexpr size_t kAlignment = 4 * 1024;

std::vector<std::vector<char>> splitFileIntoBlocks(fs::path filePath,
                                                   const size_t blockSize) {
  CHECK_GT(blockSize, 0) << "block size must be greater than 0. blockSize: "
                         << blockSize;
  LOG(INFO) << "Splitting file: " << filePath.string()
            << " into blocks of size: " << blockSize;
  std::ifstream file(filePath, std::ios::binary | std::ios::ate);
  PCHECK(file.is_open()) << "Failed to open file: " << filePath.string();
  std::streamsize fileSize = file.tellg();
  CHECK_GE(fileSize, 0) << "error getting file size for " << filePath.string();
  file.seekg(0, std::ios::beg);

  std::vector<std::vector<char>> blocks;
  blocks.reserve((fileSize + blockSize - 1) / blockSize);

  while (fileSize > blockSize) {
    std::vector<char> block(blockSize);
    if (!file.read(block.data(), blockSize)) {
      LOG(FATAL) << "Failed to read block from file: " << filePath.string()
                 << ". blockSize: " << blockSize << " fileSize: " << fileSize;
    }
    blocks.push_back(std::move(block));
    fileSize -= blockSize;
  }

  if (fileSize > 0) {
    // CHECK_EQ(fileSize % 4, 0)
    //     << "sub block size remainder is not a multiple of 4 bytes: " <<
    //     fileSize
    //     << "for file: " << filePath;
    std::vector<char> block(fileSize);
    if (file.read(block.data(), fileSize)) {
      blocks.push_back(std::move(block));
    } else {
      LOG(FATAL) << "Failed to read last block from file. " << filePath;
    }
  }

  size_t total_size_bytes = 0;
  for (const auto& block : blocks) {
    total_size_bytes += block.size();
  }
  CHECK_EQ(total_size_bytes, fs::file_size(filePath));

  return blocks;
}

/**
 * @param blocks vector of uncompressed blocks to write to files
 * @param output_directories A vector of directories. The file is split into
 * size() parts and one part is written to each directory
 * @param base_file_Name The base file name to write to. The file name will be
 * appended with _i_numFiles where i is the index of the file and numFiles is
 * the total number of files
 */
void writeUncompressedChunksToFiles(
    const std::vector<std::vector<char>>& blocks,
    const std::vector<fs::path> output_directories,
    const std::string& base_file_Name, size_t type_size) {
  // ceiling division
  const int numFiles = output_directories.size();
  const int blocksPerFile = (blocks.size() + numFiles - 1) / numFiles;

  for (int i = 0; i < numFiles; ++i) {
    const fs::path filePath =
        output_directories[i] / (base_file_Name + "_" + std::to_string(i) +
                                 "_" + std::to_string(numFiles));
    std::ofstream file(filePath, std::ios::binary);

    rapidjson::Document metadata;
    metadata.SetObject();
    rapidjson::Document::AllocatorType& allocator = metadata.GetAllocator();

    metadata.AddMember("data_format", "UNCOMPRESSED", allocator);
    metadata.AddMember("_comment", "generated with adm-partition-ssb",
                       allocator);
    metadata.AddMember("data_file",
                       rapidjson::Value(filePath.filename().c_str(), allocator),
                       allocator);
    metadata.AddMember("num_blocks", 0, allocator);

    rapidjson::Value block_sizes(rapidjson::kArrayType);
    rapidjson::Value block_offsets(rapidjson::kArrayType);
    rapidjson::Value value_counts(rapidjson::kArrayType);

    size_t offset = 0;
    for (int j = 0; j < blocksPerFile && i * blocksPerFile + j < blocks.size();
         ++j) {
      const auto& block = blocks[i * blocksPerFile + j];
      file.write(block.data(), block.size());

      metadata["num_blocks"] = j + 1;
      block_sizes.PushBack(rapidjson::Value().SetUint(block.size()), allocator);
      block_offsets.PushBack(rapidjson::Value().SetUint64(offset), allocator);
      CHECK_EQ(block.size() % type_size, 0)
          << "block size is not a multiple of " << type_size
          << " bytes: " << block.size();
      value_counts.PushBack(
          rapidjson::Value().SetUint(block.size() / type_size), allocator);

      // write padding to align to 4096
      if (block.size() % kAlignment != 0) {
        for (int k = 0; k < kAlignment - (block.size() % kAlignment); ++k) {
          file.write("\0", 1);
          offset += 1;
        }
      }

      offset += block.size();
    }

    metadata.AddMember("block_sizes", block_sizes, allocator);
    metadata.AddMember("block_offsets", block_offsets, allocator);
    metadata.AddMember("value_counts", value_counts, allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    metadata.Accept(writer);

    std::ofstream metaFile(filePath.string() + ".metadata.json");
    metaFile << strbuf.GetString();
    metaFile.flush();
  }
}

struct CompressedBlock {
  std::vector<std::vector<char>> compressed_chunks;
  size_t num_values;
};

/**
 * @param chunk uncompressed data
 * @return vector of compressed bytes
 */
std::vector<char> compressChunk(std::span<const char>& chunk) {
  auto max_compressed_chunk_size = LZ4_compressBound(chunk.size());
  std::vector<char> compressed_chunk(max_compressed_chunk_size);

  const int compressed_size =
      LZ4_compress_default(chunk.data(), compressed_chunk.data(),
                           chunk.size(),  // size of the input
                           max_compressed_chunk_size);
  CHECK_GT(compressed_size, 0)
      << "Failed to compress chunk with size " << chunk.size();
  compressed_chunk.resize(compressed_size);

  return compressed_chunk;
}

/**
 * @param block uncompressed data block
 * @param chunk_size size in bytes of each uncompressed chunk
 * @return vector of compressed chunks
 */
CompressedBlock compressBlock(const std::vector<char>& block,
                              size_t chunk_size) {
  size_t num_chunks =
      (block.size() + chunk_size - 1) / chunk_size;  // ceiling division
  std::vector<std::vector<char>> compressed_chunks;
  compressed_chunks.reserve(num_chunks);

  for (size_t i = 0; i < block.size(); i += chunk_size) {
    size_t end = std::min(i + chunk_size, block.size());
    auto chunk = std::span(block.data() + i, end - i);
    CHECK_LE(chunk.size(), chunk_size);
    compressed_chunks.emplace_back(compressChunk(chunk));
#ifndef NDEBUG
    // validate
    char* decompressed_chunk = new char[chunk.size()];
    auto res =
        LZ4_decompress_safe(compressed_chunks.back().data(), decompressed_chunk,
                            compressed_chunks.back().size(), chunk.size());
    CHECK_EQ(res, chunk.size()) << "failed to decompress chunk";
    auto cmp_res = memcmp(chunk.data(), decompressed_chunk, chunk.size());
    CHECK_EQ(cmp_res, 0) << "decompressed chunk does not match original chunk";
    delete[] decompressed_chunk;
#endif
  }
  CHECK_EQ(compressed_chunks.size(), num_chunks);
  // fixme: assuming 4 byte types everywhere
  CHECK_EQ(block.size() % 4, 0)
      << "fixme only 4 byte types. block size: " << block.size();
  return {compressed_chunks, block.size() / 4};
}

/**
 *
 * @return a vector of compressed blocks, each of which is a vector of
 * compressed chunks
 */
std::vector<CompressedBlock> compressBlocks(
    ThreadPool& tp, std::vector<std::vector<char>>& uncompressed_blocks,
    size_t chunk_size) {
  // important, maintain the order of the blocks as we compress each column
  // seperately
  std::vector<std::future<CompressedBlock>> compressed_block_futures;
  compressed_block_futures.reserve(uncompressed_blocks.size());
  for (const auto& block : uncompressed_blocks) {
    compressed_block_futures.emplace_back(
        tp.enqueue(compressBlock, block, chunk_size));
  }

  std::vector<CompressedBlock> compressed_blocks;
  compressed_blocks.reserve(uncompressed_blocks.size());
  for (int i = 0; i < compressed_block_futures.size(); ++i) {
    compressed_blocks.emplace_back(compressed_block_futures[i].get());
    uncompressed_blocks[i].resize(0);
  }
  CHECK_EQ(uncompressed_blocks.size(), compressed_blocks.size());

  return compressed_blocks;
}

/**
 * @param blocks vector of compressed blocks to write to files
 * @param output_directories A vector of directories. The file is split into
 * size() parts and one part is written to each directory
 * @param base_file_Name The base file name to write to. The file name will be
 * appended with _i_numFiles where i is the index of the file and numFiles is
 * the total number of files
 * @param chunk_size size in bytes of each uncompressed chunk, previously used
 * to compress the blocks
 */
void writeCompressedBlocksToFiles(
    std::vector<CompressedBlock>& blocks,
    const std::vector<fs::path> output_directories,
    const std::string& base_file_Name, const int chunk_size) {
  // ceiling division
  const int numFiles = output_directories.size();
  const int blocksPerFile = (blocks.size() + numFiles - 1) / numFiles;
  LOG(INFO) << "splitting " << blocks.size() << " blocks into " << numFiles
            << " files with " << blocksPerFile << " blocks per file";

  for (int i = 0; i < numFiles; ++i) {
    const fs::path filePath =
        output_directories[i] / (base_file_Name + "_" + std::to_string(i) +
                                 "_" + std::to_string(numFiles));
    int data_file_fd = open(filePath.c_str(), O_CREAT | O_RDWR, 0644);
    PCHECK(data_file_fd > 0) << "failed to open: " << filePath;

    rapidjson::Document metadata;
    metadata.SetObject();
    rapidjson::Document::AllocatorType& allocator = metadata.GetAllocator();

    metadata.AddMember("data_format", "COMPRESSED", allocator);
    metadata.AddMember("_comment", "generated with adm-partition-ssb",
                       allocator);
    metadata.AddMember("data_file",
                       rapidjson::Value(filePath.filename().c_str(), allocator),
                       allocator);
    metadata.AddMember("num_blocks", 0, allocator);
    metadata.AddMember("decompressed_chunk_size", chunk_size, allocator);

    rapidjson::Value block_sizes(rapidjson::kArrayType);
    rapidjson::Value block_offsets(rapidjson::kArrayType);
    rapidjson::Value value_counts(rapidjson::kArrayType);
    rapidjson::Value all_chunk_sizes(rapidjson::kArrayType);

    size_t offset = 0;
    size_t bytes_written = 0;
    size_t chunks_written = 0;
    for (int j = 0; j < blocksPerFile && i * blocksPerFile + j < blocks.size();
         ++j) {
      auto& block = blocks[i * blocksPerFile + j];

      uint64_t block_size = 0;
      rapidjson::Value block_chunk_sizes(rapidjson::kArrayType);

      CHECK_GE(block.compressed_chunks.size(), 1);
      std::vector<uint32_t> chunk_sizes;
      for (const auto& chunk : block.compressed_chunks) {
        CHECK_GT(chunk.size(), 0);
        auto wrote_bytes = write(data_file_fd, chunk.data(), chunk.size());
        PCHECK(wrote_bytes == chunk.size())
            << "failed to write to file. wrote_bytes: " << wrote_bytes
            << " to file " << filePath;
        bytes_written += chunk.size();
        block_size += chunk.size();
        chunk_sizes.push_back(chunk.size());
        block_chunk_sizes.PushBack(rapidjson::Value().SetUint(chunk.size()),
                                   allocator);
        chunks_written += 1;

#ifndef NDEBUG
        char* decompressed_chunk = new char[16 * 1024];
        auto res = LZ4_decompress_safe(chunk.data(), decompressed_chunk,
                                       chunk.size(), 16 * 1024);
        CHECK_GT(res, 0) << "failed to decompress chunk";
        delete[] decompressed_chunk;
#endif
      }

      // read back and decompress block to verify
#ifndef NDEBUG
      auto curr_position = lseek(data_file_fd, 0, SEEK_CUR);
      PCHECK(lseek(data_file_fd, -1 * block_size, SEEK_CUR) >= 0)
          << "seeking: " << -block_size << " from position " << curr_position;
      char* debug_comp_buffer = new char[block_size];
      char* debug_decomp_buffer = new char[chunk_size * chunk_sizes.size()];
      PCHECK(read(data_file_fd, debug_comp_buffer, block_size) == block_size)
          << "failed to read back compressed block " << i * blocksPerFile + j
          << " of " << filePath;
      CHECK_EQ(
          decompress_block(
              chunk_sizes, std::span(debug_comp_buffer, block_size),
              std::span(debug_decomp_buffer, chunk_size * chunk_sizes.size()),
              chunk_size  // max decomp chunk size
              ),
          0)
          << "failed to read back and decompress block "
          << i * blocksPerFile + j << " of " << filePath << "block size "
          << block_size << " chunks: " << chunk_sizes.size();
      delete[] debug_comp_buffer;
      delete[] debug_decomp_buffer;
#endif

      metadata["num_blocks"] = j + 1;
      block_sizes.PushBack(rapidjson::Value().SetUint(block_size), allocator);
      block_offsets.PushBack(rapidjson::Value().SetUint64(offset), allocator);
      value_counts.PushBack(rapidjson::Value().SetUint(block.num_values),
                            allocator);
      all_chunk_sizes.PushBack(block_chunk_sizes, allocator);

      // write padding to align to 4096
      constexpr char padding_byte = '1';
      if (block_size % kAlignment != 0) {
        for (int k = 0; k < kAlignment - (block_size % kAlignment); ++k) {
          PCHECK(write(data_file_fd, &padding_byte, 1) > 0)
              << "failed to write padding";
          offset += 1;
        }
      }

      offset += block_size;
      //      CHECK_EQ(offset % kAlignment, 0);
    }

    metadata.AddMember("block_sizes", block_sizes, allocator);
    metadata.AddMember("block_offsets", block_offsets, allocator);
    metadata.AddMember("value_counts", value_counts, allocator);
    metadata.AddMember("chunk_sizes", all_chunk_sizes, allocator);

    rapidjson::StringBuffer strbuf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
    metadata.Accept(writer);
    std::ofstream metaFile(filePath.string() + ".metadata.json");
    PCHECK(metaFile) << "failed to open metadata file for writing";
    metaFile << strbuf.GetString();
    PCHECK(metaFile) << "failed to write metadata to file";
    close(data_file_fd);
    LOG(INFO) << "wrote " << bytes_written << " bytes to " << filePath << "in "
              << chunks_written << " chunks and " << blocks.size() << " blocks";
  }
}

std::vector<fs::path> splitOutputDirectories(const std::string& str) {
  std::vector<fs::path> result;
  std::stringstream ss(str);
  std::string token;

  while (std::getline(ss, token, ',')) {
    result.push_back(token);
    if (!fs::exists(result.back())) {
      if (fs::create_directories(result.back())) {
        LOG(INFO) << "Successfully created directories: " << result.back();
      } else {
        LOG(FATAL) << "Failed to create directories or they already exist: "
                   << result.back();
      }
    } else {
      CHECK(fs::is_directory(result.back())) << "output directory is not a "
                                                "directory: "
                                             << result.back();
    }
  }

  return result;
}

int main(int argc, char* argv[]) {
  // Parse the command line arguments
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging((argv)[0]);
  FLAGS_colorlogtostderr = true;
  google::LogToStderr();

  CHECK(!FLAGS_input_directory.empty()) << "Input directory not provided";
  CHECK(!FLAGS_output_directories.empty()) << "Output directories not provided";
  //  CHECK_GT(FLAGS_num_files, 0) << "Number of files must be greater than 0";

  std::vector<fs::path> output_directories =
      splitOutputDirectories(FLAGS_output_directories);

  fs::path input_dir_path(FLAGS_input_directory);
  CHECK(fs::exists(input_dir_path))
      << "Input directory does not exist: " << input_dir_path;
  CHECK(fs::is_directory(input_dir_path))
      << "Input directory is not a directory: " << input_dir_path;

  LOG(INFO) << "input directory: " << FLAGS_input_directory;
  LOG(INFO) << "output directories: " << FLAGS_output_directories;
  LOG(INFO) << "compress data: " << FLAGS_compress_data;
  LOG(INFO) << "Splitting each file into:" << output_directories.size()
            << "output files";
  google::InstallFailureSignalHandler();

  topology::init();
  auto& topo = topology::getInstance();

  std::vector<std::pair<fs::path, size_t>> data_file_paths;
  std::unordered_map<std::string, std::string> attribute_map;
  for (const auto& entry : fs::directory_iterator(input_dir_path)) {
    if (entry.path().string().find("catalog") != std::string::npos) {
      LOG(INFO) << "found the catalog " << entry.path();
      std::ifstream file_stream(entry.path());
      if (!file_stream.is_open()) {
        LOG(FATAL) << "Error: Could not open file: " << entry;
      }
      std::stringstream buffer;
      buffer << file_stream.rdbuf();
      std::string file_content = buffer.str();
      if (file_content.empty()) {
        LOG(FATAL) << "Error: File is empty or could not be read: " << entry;
      }
      attribute_map = parseAttributeTypes(file_content.c_str());
    }
  }
  size_t maxDataTypeSize = 0;
  for (const auto& entry : fs::directory_iterator(input_dir_path)) {
    if (entry.is_regular_file() &&
        entry.path().string().find("dict") == std::string::npos &&
        entry.path().string().find("catalog") == std::string::npos) {
      size_t data_type_size =
          toProteusTypeSize(attribute_map[entry.path().filename().string()]);
      data_file_paths.push_back({entry.path(), data_type_size});
      maxDataTypeSize = std::max(maxDataTypeSize, data_type_size);
    }
  }
  CHECK_GT(maxDataTypeSize, 0) << "max data type size cannot be 0!";
  constexpr size_t kMaxBlockSize = 2 * 1024 * 1024;  // 2MiB for 4 byte types
  constexpr size_t kChunkSize = 16 * 1024;           // 16 KiB

  if (FLAGS_compress_data == true) {
    ThreadPool pool(topo.getCoreCount());
    for (const auto& [data_file_path, data_type_size] : data_file_paths) {
      CHECK_EQ(maxDataTypeSize % data_type_size, 0);
      CHECK_GT(data_type_size, 0)
          << "data type size must be greater than 0. data_file_path "
          << data_file_path;
      const int block_size = kMaxBlockSize / (maxDataTypeSize / data_type_size);
      LOG(INFO) << "Splitting file: " << data_file_path
                << " with data type size " << data_type_size
                << " using block size " << block_size;
      auto blocks = splitFileIntoBlocks(data_file_path, block_size);
      auto compressed_blocks = compressBlocks(pool, blocks, kChunkSize);
      CHECK_EQ(blocks.size(), compressed_blocks.size());
      writeCompressedBlocksToFiles(compressed_blocks, output_directories,
                                   data_file_path.filename().string(),
                                   kChunkSize);
      fs::path dict_path = data_file_path.string() + ".dict";
      if (fs::exists(dict_path)) {
        for (const auto& output_dir : output_directories) {
          fs::copy(dict_path, output_dir / dict_path.filename().string());
        }
      }
    }
  } else {
    ThreadPool pool(true);
    std::vector<std::future<void>> split_column_futures;
    split_column_futures.reserve(data_file_paths.size());
    for (const auto& path_size_pair : data_file_paths) {
      const fs::path data_file_path = path_size_pair.first;
      const size_t data_type_size = path_size_pair.second;
      CHECK_EQ(maxDataTypeSize % data_type_size, 0);
      split_column_futures.emplace_back(pool.enqueue([=]() {
        CHECK_EQ(maxDataTypeSize % data_type_size, 0);
        CHECK_GT(data_type_size, 0)
            << "data type size must be greater than 0. data_file_path "
            << data_file_path;

        const int block_size =
            kMaxBlockSize / (maxDataTypeSize / data_type_size);
        LOG(INFO) << "Splitting file: " << data_file_path
                  << " with data type size " << data_type_size
                  << " using block size " << block_size;
        auto blocks = splitFileIntoBlocks(data_file_path, block_size);
        writeUncompressedChunksToFiles(blocks, output_directories,
                                       data_file_path.filename().string(),
                                       data_type_size);
        fs::path dict_path = data_file_path.string() + ".dict";
        if (fs::exists(dict_path)) {
          for (const auto& output_dir : output_directories) {
            fs::copy(dict_path, output_dir / dict_path.filename().string());
          }
        }
      }));
    }
    LOG(INFO) << "waiting for futures to complete";
    CHECK_EQ(split_column_futures.size(), data_file_paths.size());
    for (int i = 0; i < data_file_paths.size(); i++) {
      LOG(INFO) << "waiting for future " << data_file_paths[i].first;
      split_column_futures[i].get();
    }
    LOG(INFO) << "done";
  }

  return 0;
}
