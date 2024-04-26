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

#include <platform/topology/topology.hpp>
#include <platform/util/logging.hpp>
#include <span>
#include <vector>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnewline-eof"
#pragma clang diagnostic ignored "-Wdocumentation-unknown-command"
#pragma clang diagnostic ignored "-Wdocumentation"

#include "lz4.h"
#include "nvcomp.hpp"

#pragma clang diagnostic pop

/**
 * @brief begin asynchronous decompression on the provided stream
 * @note Assumes that compressed_buffer and output_buffer are accessible by the
 * GPU
 * @return a positive integer on success indicating the number of bytes
 * decompressed, a negative value on failure
 */
[[nodiscard]] int decompress_block_gpu(std::vector<uint32_t>& chunk_sizes,
                                       std::span<char> compressed_buffer,
                                       std::span<char> output_buffer,
                                       int max_decomp_chunk_size,
                                       cudaStream_t stream);

/**
 * @brief begin asynchronous decompression on the provided stream
 * The same as @see decompress_block_gpu but for multiple blocks, which can be
 * more efficient
 */
[[nodiscard]] int batch_decompress_block_gpu(
    const std::vector<std::vector<uint32_t>>& block_chunk_sizes,
    const std::vector<std::span<char>>& block_compressed_buffers,
    const std::vector<std::span<char>>& block_output_buffers,
    int max_decomp_chunk_size, cudaStream_t stream);

/**
 * @return a positive integer on success indicating the number of bytes
 * decompressed, a negative value on failure
 */
[[nodiscard]] int decompress_block(const std::vector<uint32_t>& chunk_sizes,
                                   std::span<const char> compressed_buffer,
                                   std::span<char> output_buffer,
                                   int max_decomp_chunk_size);

[[nodiscard]] int decompress_block(const std::vector<uint32_t>& chunk_sizes,
                                   std::span<char> compressed_buffer,
                                   std::span<char> output_buffer,
                                   int max_decomp_chunk_size);

#endif  // PROTEUS_COMPRESSION_HPP
