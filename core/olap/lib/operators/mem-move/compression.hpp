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

#ifndef PROTEUS_COMPRESSION_HPP
#define PROTEUS_COMPRESSION_HPP

#ifdef HAVE_NVCOMP
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnewline-eof"
#pragma clang diagnostic ignored "-Wdocumentation-unknown-command"
#pragma clang diagnostic ignored "-Wdocumentation"
#include <nvcomp/shared_types.h>
#pragma clang diagnostic pop
#else
#define nvcompStatus_t void
#endif

#include <platform/topology/topology.hpp>
#include <span>
#include <vector>

class GpuDecompressor {
 private:
  const size_t m_max_decomp_chunk_size;
  void** m_device_compressed_ptrs;
  void** m_host_compressed_ptrs;
  void** m_device_uncompressed_ptrs;
  void** m_host_uncompressed_ptrs;
  size_t* m_host_compressed_chunk_sizes;
  size_t* m_device_compressed_bytes;
  size_t*
      m_device_uncompressed_bytes;  // "buffer" size for each decompressed chunk
  nvcompStatus_t* m_device_status_ptrs;
  void* m_device_decomp_workspace;
  size_t m_device_decomp_workspace_size;
  size_t* m_device_actual_uncompressed_bytes;
  size_t* m_host_uncompressed_chunk_sizes;
  size_t* m_device_uncompressed_chunk_sizes;

  size_t last_batch_num_chunks;
  int m_gpu_index_in_topo;

 public:
  GpuDecompressor(size_t max_decomp_chunk_size, size_t max_batch_block_count,
                  size_t max_chunks_per_block, int gpu_index_in_topo = -1);
  ~GpuDecompressor();

  /**
   * @brief begin asynchronous decompression on the provided stream. If no
   * stream is provided decompression is synchronous and will block the calling
   * thread.
   * @note only one decompression operation can be active at a time.
   * @note Assumes that compressed_buffer and output_buffer are accessible by
   * the GPU
   * @return Synchonous calls return the number of bytes decompressed on
   * success. Asynchronous calls return 0 on success. Both return a negative int
   * indicating a nvcompStatus_t error on failure.
   */
  [[nodiscard]] int decompress_block_gpu(std::vector<uint32_t>& chunk_sizes,
                                         std::span<char> compressed_buffer,
                                         std::span<char> output_buffer,
                                         cudaStream_t stream = nullptr);

  /**
   * @brief This is the same functionality as @see decompress_block_gpu but for
   * multiple blocks, which can be more efficient
   */
  [[nodiscard]] int batch_decompress_block_gpu(
      const std::vector<std::vector<uint32_t>>& block_chunk_sizes,
      const std::vector<std::span<char>>& block_compressed_buffers,
      const std::vector<std::span<char>>& block_output_buffers,
      cudaStream_t stream = nullptr);

  /**
   * @brief Get the number of bytes decompressed in the last batch
   * If a stream was provided to batch_decompress_block_gpu or
   * decompress_block_gpu, the stream must be synchronized before calling this
   * function. This function must only be called at most once per asynchronous
   * decompression operation.
   * @return a negative int indicating a nvcompStatus_t error on failure.
   * Returns a positive integer on success indicating the number of bytes
   * decompressed.
   */
  [[nodiscard]] int get_last_batch_bytes_decompressed();

 private:
  void** get_device_uncompressed_ptrs(
      const std::vector<std::span<char>>& block_output_buffers,
      const std::vector<std::vector<uint32_t>>& block_chunk_sizes,
      size_t batch_size, cudaStream_t stream);

  size_t* get_device_compressed_bytes(
      const std::vector<std::vector<uint32_t>>& blocks_chunk_sizes,
      size_t batch_size, cudaStream_t stream);

  void** get_device_compressed_ptrs(
      const std::vector<std::span<char>>& compressed_buffers,
      const std::vector<std::vector<uint32_t>>& chunk_sizes, size_t batch_size,
      cudaStream_t stream);

  size_t* get_device_uncompressed_chunk_sizes(size_t batch_size,
                                              cudaStream_t stream);

  int decompress_gpu(const size_t batch_size, void** device_uncompressed_ptrs,
                     void** device_compressed_ptrs,
                     size_t* device_compressed_bytes, size_t output_buffer_size,
                     cudaStream_t stream);
};

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
