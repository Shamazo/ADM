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

#ifndef PROTEUS_COMPRESSION_HPP
#define PROTEUS_COMPRESSION_HPP

#include <platform/util/logging.hpp>
#include <span>
#include <vector>

#include "lz4.h"

/**
 * @return a positive integer on success indicating the number of bytes
 * decompressed, a negative value on failure
 */
[[nodiscard]] int decompress_block(
    const std::vector<uint32_t>& chunk_sizes,
    const std::span<const char> compressed_buffer,
    std::span<char> output_buffer, int max_decomp_chunk_size) {
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
    DCHECK_LE(decomp_idx, output_buffer.size());
  }
  return decomp_idx;
}

[[nodiscard]] int decompress_block(const std::vector<uint32_t>& chunk_sizes,
                                   std::span<char> compressed_buffer,
                                   std::span<char> output_buffer,
                                   int max_decomp_chunk_size) {
  return decompress_block(
      chunk_sizes,
      std::span<const char>(compressed_buffer.data(), compressed_buffer.size()),
      output_buffer, max_decomp_chunk_size);
}

#endif  // PROTEUS_COMPRESSION_HPP
