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

#include "compression.hpp"

#include "platform/common/gpu/gpu-common.hpp"

/* Allocate space for GPU pointers for uncompressed data chunks. */
[[maybe_unused]] static void **get_device_uncompressed_ptrs(
    std::span<char> output_buffer, int max_decomp_chunk_size, size_t batch_size,
    cudaStream_t stream) {
  void **host_uncompressed_chunk_ptrs = nullptr;
  gpu_run(cudaMallocHost(&host_uncompressed_chunk_ptrs,
                         sizeof(void *) * batch_size));
  for (size_t ix_chunk = 0; ix_chunk < batch_size; ++ix_chunk) {
    host_uncompressed_chunk_ptrs[ix_chunk] =
        output_buffer.data() + ix_chunk * max_decomp_chunk_size;
  }

  void **device_uncompressed_ptrs = nullptr;
  gpu_run(cudaMallocAsync(&device_uncompressed_ptrs,
                          sizeof(void *) * batch_size, stream));
  gpu_run(cudaMemcpyAsync(
      device_uncompressed_ptrs, host_uncompressed_chunk_ptrs,
      sizeof(void *) * batch_size, cudaMemcpyHostToDevice, stream));

  gpu_run(cudaFreeHost(host_uncompressed_chunk_ptrs));

  return device_uncompressed_ptrs;
}

[[maybe_unused]]
/* Batch version of the function above. */
static void **
get_device_uncompressed_ptrs(
    const std::vector<std::span<char>> &block_output_buffers,
    const std::vector<std::vector<uint32_t>> &block_chunk_sizes,
    int max_decomp_chunk_size, size_t batch_size, cudaStream_t stream) {
  void **host_uncompressed_chunk_ptrs = nullptr;
  gpu_run(cudaMallocHost(&host_uncompressed_chunk_ptrs,
                         sizeof(void *) * batch_size));

  size_t ix_chunk = 0;
  for (size_t i = 0; i < block_output_buffers.size(); ++i) {
    for (size_t j = 0; j < block_chunk_sizes[i].size(); ++j) {
      host_uncompressed_chunk_ptrs[ix_chunk] =
          block_output_buffers[i].data() + j * max_decomp_chunk_size;
      ++ix_chunk;
    }
  }
  DCHECK_EQ(ix_chunk, batch_size);

  void **device_uncompressed_ptrs = nullptr;
  gpu_run(cudaMallocAsync(&device_uncompressed_ptrs,
                          sizeof(void *) * batch_size, stream));
  gpu_run(cudaMemcpyAsync(
      device_uncompressed_ptrs, host_uncompressed_chunk_ptrs,
      sizeof(void *) * batch_size, cudaMemcpyHostToDevice, stream));

  gpu_run(cudaFreeHost(host_uncompressed_chunk_ptrs));

  return device_uncompressed_ptrs;
}

/* Allocate space for GPU pointers for compressed data sizes. */
[[maybe_unused]] static size_t *get_device_compressed_bytes(
    const std::vector<uint32_t> &chunk_sizes, cudaStream_t stream) {
  size_t *host_compressed_chunk_sizes = nullptr;
  gpu_run(cudaMallocHost(&host_compressed_chunk_sizes,
                         sizeof(size_t) * chunk_sizes.size()));
  for (size_t i = 0; i < chunk_sizes.size(); ++i) {
    host_compressed_chunk_sizes[i] = chunk_sizes[i];
  }

  size_t *device_compressed_bytes = nullptr;
  gpu_run(cudaMallocAsync(&device_compressed_bytes,
                          sizeof(size_t) * chunk_sizes.size(), stream));
  gpu_run(cudaMemcpyAsync(device_compressed_bytes, host_compressed_chunk_sizes,
                          sizeof(size_t) * chunk_sizes.size(),
                          cudaMemcpyHostToDevice, stream));

  gpu_run(cudaFreeHost(host_compressed_chunk_sizes));

  return device_compressed_bytes;
}

/* Batch version of the function above. */
[[maybe_unused]] static size_t *get_device_compressed_bytes(
    const std::vector<std::vector<uint32_t>> &blocks_chunk_sizes,
    size_t batch_size, cudaStream_t stream) {
  size_t *host_compressed_chunk_sizes = nullptr;
  gpu_run(cudaMallocHost(&host_compressed_chunk_sizes,
                         sizeof(size_t) * batch_size));
  size_t ix_chunk = 0;
  for (const auto &block_chunk_size : blocks_chunk_sizes) {
    for (unsigned int chunk_size : block_chunk_size) {
      host_compressed_chunk_sizes[ix_chunk] = chunk_size;
      ++ix_chunk;
    }
  }
  DCHECK_EQ(ix_chunk, batch_size);

  size_t *device_compressed_bytes = nullptr;
  gpu_run(cudaMallocAsync(&device_compressed_bytes, sizeof(size_t) * batch_size,
                          stream));
  gpu_run(cudaMemcpyAsync(device_compressed_bytes, host_compressed_chunk_sizes,
                          sizeof(size_t) * batch_size, cudaMemcpyHostToDevice,
                          stream));

  gpu_run(cudaFreeHost(host_compressed_chunk_sizes));

  return device_compressed_bytes;
}

/* Allocate space for GPU pointers for compressed data chunks. */
[[maybe_unused]] static void **get_device_compressed_ptrs(
    std::span<char> compressed_buffer, const std::vector<uint32_t> &chunk_sizes,
    cudaStream_t stream) {
  void **host_compressed_chunk_ptrs = nullptr;
  gpu_run(cudaMallocHost(&host_compressed_chunk_ptrs,
                         sizeof(void *) * chunk_sizes.size()));
  size_t current_offset = 0;
  for (size_t ix_chunk = 0; ix_chunk < chunk_sizes.size(); ++ix_chunk) {
    host_compressed_chunk_ptrs[ix_chunk] =
        compressed_buffer.data() + current_offset;
    current_offset += chunk_sizes[ix_chunk];
  }

  void **device_compressed_ptrs = nullptr;
  gpu_run(cudaMallocAsync(&device_compressed_ptrs,
                          sizeof(void *) * chunk_sizes.size(), stream));
  gpu_run(cudaMemcpyAsync(device_compressed_ptrs, host_compressed_chunk_ptrs,
                          sizeof(void *) * chunk_sizes.size(),
                          cudaMemcpyHostToDevice, stream));

  gpu_run(cudaFreeHost(host_compressed_chunk_ptrs));

  return device_compressed_ptrs;
}

/* Batch version of the function above. */
[[maybe_unused]] static void **get_device_compressed_ptrs(
    const std::vector<std::span<char>> &compressed_buffers,
    const std::vector<std::vector<uint32_t>> &chunk_sizes, size_t batch_size,
    cudaStream_t stream) {
  void **host_compressed_chunk_ptrs = nullptr;
  gpu_run(
      cudaMallocHost(&host_compressed_chunk_ptrs, sizeof(void *) * batch_size));
  size_t ix_chunk = 0;
  for (size_t i = 0; i < compressed_buffers.size(); ++i) {
    size_t current_offset = 0;
    for (size_t j = 0; j < chunk_sizes[i].size(); ++j) {
      host_compressed_chunk_ptrs[ix_chunk] =
          compressed_buffers[i].data() + current_offset;
      current_offset += chunk_sizes[i][j];
      ++ix_chunk;
    }
  }
  DCHECK_EQ(ix_chunk, batch_size);

  void **device_compressed_ptrs = nullptr;
  gpu_run(cudaMallocAsync(&device_compressed_ptrs, sizeof(void *) * batch_size,
                          stream));
  gpu_run(cudaMemcpyAsync(device_compressed_ptrs, host_compressed_chunk_ptrs,
                          sizeof(void *) * batch_size, cudaMemcpyHostToDevice,
                          stream));

  gpu_run(cudaFreeHost(host_compressed_chunk_ptrs));

  return device_compressed_ptrs;
}

[[maybe_unused]] static int decompress_gpu(
    const size_t batch_size, void **device_uncompressed_ptrs,
    void **device_compressed_ptrs, size_t *device_compressed_bytes,
    int max_decomp_chunk_size, size_t output_buffer_size, cudaStream_t stream) {
  size_t decomp_temp_bytes;
  nvcompStatus_t status = nvcompBatchedLZ4DecompressGetTempSize(
      batch_size, max_decomp_chunk_size, &decomp_temp_bytes);
  if (status != nvcompSuccess) {
    throw std::runtime_error(
        "nvcompBatchedGzipDecompressGetTempSize() failed.");
  }

  size_t *device_uncompressed_bytes = nullptr;
  gpu_run(cudaMallocAsync(&device_uncompressed_bytes,
                          sizeof(size_t) * batch_size, stream));
  status = nvcompBatchedLZ4GetDecompressSizeAsync(
      device_compressed_ptrs, device_compressed_bytes,
      device_uncompressed_bytes, batch_size, stream);
  if (status != nvcompSuccess) {
    throw std::runtime_error(
        "nvcompBatchedGzipDecompressGetTempSize() failed.");
  }

  void *device_comp_temp = nullptr;
  gpu_run(cudaMallocAsync(&device_comp_temp, decomp_temp_bytes, stream));

  size_t *device_actual_uncompressed_bytes = nullptr;
  gpu_run(cudaMallocAsync(&device_actual_uncompressed_bytes,
                          batch_size * sizeof(size_t), stream));

  nvcompStatus_t *device_status_ptrs = nullptr;
  gpu_run(cudaMallocAsync(&device_status_ptrs,
                          batch_size * sizeof(nvcompStatus_t), stream));

  // Run decompression
  status = nvcompBatchedLZ4DecompressAsync(
      device_compressed_ptrs, device_compressed_bytes,
      device_uncompressed_bytes, device_actual_uncompressed_bytes, batch_size,
      device_comp_temp, decomp_temp_bytes, device_uncompressed_ptrs,
      device_status_ptrs, stream);

  if (status != nvcompSuccess) {
    throw std::runtime_error("GPU decompression failed!");
  }

  gpu_run(cudaStreamSynchronize(stream));

  // Assume device_status_ptrs is an array of nvcompStatus_t on the device
  auto *host_status_ptrs = new nvcompStatus_t[batch_size];

  // Copy the status from device to host
  gpu_run(cudaMemcpy(host_status_ptrs, device_status_ptrs,
                     batch_size * sizeof(nvcompStatus_t),
                     cudaMemcpyDeviceToHost));

  // Check the status of each decompression task
  for (size_t i = 0; i < batch_size; i++) {
    if (host_status_ptrs[i] != nvcompSuccess) {
      std::stringstream ss;
      ss << "GPU decompression failed for chunk " << i << " with status "
         << host_status_ptrs[i];
      throw std::runtime_error(ss.str());
    }
  }

  /* Validation that the decompressed size is the expected one.  */
  auto *host_actual_uncompressed_bytes = new size_t[batch_size];
  gpu_run(cudaMemcpy(host_actual_uncompressed_bytes,
                     device_actual_uncompressed_bytes,
                     batch_size * sizeof(size_t), cudaMemcpyDeviceToHost));
  size_t decompressed_size = 0;
  for (size_t i = 0; i < batch_size; i++) {
    decompressed_size += host_actual_uncompressed_bytes[i];
  }
  DCHECK_LE(decompressed_size, output_buffer_size);

  delete[] host_status_ptrs;
  delete[] host_actual_uncompressed_bytes;
  gpu_run(cudaFree(device_comp_temp));
  gpu_run(cudaFree(device_status_ptrs));
  gpu_run(cudaFree(device_actual_uncompressed_bytes));

  return (int)decompressed_size;
}

/* Decompress a batch of compressed data chunks on the GPU. */
[[nodiscard]] int batch_decompress_block_gpu(
    const std::vector<std::vector<uint32_t>> &block_chunk_sizes,
    const std::vector<std::span<char>> &block_compressed_buffers,
    const std::vector<std::span<char>> &block_output_buffers,
    int max_decomp_chunk_size, cudaStream_t stream) {
  DCHECK_EQ(block_compressed_buffers.size(), block_chunk_sizes.size());
  DCHECK_EQ(block_output_buffers.size(), block_chunk_sizes.size());

  size_t batch_size = 0;
  for (const auto &chunk_sizes : block_chunk_sizes) {
    batch_size += chunk_sizes.size();
  }

  size_t output_buffer_size = 0;
  for (const auto &output_buffer : block_output_buffers) {
    output_buffer_size += output_buffer.size();
  }

  void **device_uncompressed_ptrs =
      get_device_uncompressed_ptrs(block_output_buffers, block_chunk_sizes,
                                   max_decomp_chunk_size, batch_size, stream);

  void **device_compressed_ptrs = get_device_compressed_ptrs(
      block_compressed_buffers, block_chunk_sizes, batch_size, stream);

  size_t *device_compressed_bytes =
      get_device_compressed_bytes(block_chunk_sizes, batch_size, stream);

  return decompress_gpu(batch_size, device_uncompressed_ptrs,
                        device_compressed_ptrs, device_compressed_bytes,
                        max_decomp_chunk_size, output_buffer_size, stream);
}

/* Assuming that compressed_buffer and output_buffer are GPU resident.  */
[[nodiscard]] int decompress_block_gpu(std::vector<uint32_t> &chunk_sizes,
                                       std::span<char> compressed_buffer,
                                       std::span<char> output_buffer,
                                       int max_decomp_chunk_size,
                                       cudaStream_t stream) {
  std::vector<std::vector<uint32_t>> block_chunk_sizes(1);
  block_chunk_sizes[0] = std::move(chunk_sizes);

  std::vector<std::span<char>> block_compressed_buffers(1);
  block_compressed_buffers[0] = compressed_buffer;

  std::vector<std::span<char>> block_output_buffers(1);
  block_output_buffers[0] = output_buffer;

  return batch_decompress_block_gpu(block_chunk_sizes, block_compressed_buffers,
                                    block_output_buffers, max_decomp_chunk_size,
                                    stream);
}

/**
 * @return a positive integer on success indicating the number of bytes
 * decompressed, a negative value on failure
 */
[[nodiscard]] int decompress_block(
    const std::vector<uint32_t> &chunk_sizes,
    const std::span<const char> compressed_buffer,
    std::span<char> output_buffer, int max_decomp_chunk_size) {
  int comp_idx = 0;
  int decomp_idx = 0;
  for (const auto &chunk_size : chunk_sizes) {
    auto decompressed_size = LZ4_decompress_safe(
        &compressed_buffer[comp_idx], &output_buffer[decomp_idx],
        (int)chunk_size, max_decomp_chunk_size);
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

[[nodiscard]] int decompress_block(const std::vector<uint32_t> &chunk_sizes,
                                   std::span<char> compressed_buffer,
                                   std::span<char> output_buffer,
                                   int max_decomp_chunk_size) {
  return decompress_block(
      chunk_sizes,
      std::span<const char>(compressed_buffer.data(), compressed_buffer.size()),
      output_buffer, max_decomp_chunk_size);
}
