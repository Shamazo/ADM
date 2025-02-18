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

#include <lz4.h>

#include <magic_enum.hpp>
#include <platform/common/gpu/gpu-common.hpp>
#include <platform/memory/memory-manager.hpp>
#include <platform/topology/affinity_manager.hpp>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wnewline-eof"
#pragma clang diagnostic ignored "-Wdocumentation-unknown-command"
#pragma clang diagnostic ignored "-Wdocumentation"

#include <nvcomp.hpp>

#pragma clang diagnostic pop

GpuDecompressor::GpuDecompressor(size_t max_decomp_chunk_size,
                                 size_t max_batch_block_count,
                                 size_t max_chunks_per_block,
                                 int gpu_index_in_topo)
    : m_max_decomp_chunk_size(max_decomp_chunk_size), last_batch_num_chunks(0) {
  // ensure that we are allocating memory on the correct GPU
  int gpu_index = gpu_index_in_topo;
  if (gpu_index_in_topo == -1) {
    gpu_index = topology::getInstance().getActiveGpu().index_in_topo;
  }
  m_gpu_index_in_topo = gpu_index;

  auto scope =
      topology::getInstance().getGpus()[m_gpu_index_in_topo].set_on_scope();

  const int max_batch_chunks = max_batch_block_count * max_chunks_per_block;
  m_host_compressed_ptrs = static_cast<void **>(
      MemoryManager::mallocPinned(sizeof(void *) * max_batch_chunks));
  m_device_compressed_ptrs = static_cast<void **>(
      MemoryManager::mallocGpu(sizeof(void *) * max_batch_chunks));
  m_host_uncompressed_ptrs = static_cast<void **>(
      MemoryManager::mallocPinned(sizeof(void *) * max_batch_chunks));
  m_device_uncompressed_ptrs = static_cast<void **>(
      MemoryManager::mallocGpu(sizeof(void *) * max_batch_chunks));
  m_host_compressed_chunk_sizes = static_cast<size_t *>(
      MemoryManager::mallocPinned(sizeof(size_t) * max_batch_chunks));

  m_device_compressed_bytes = static_cast<size_t *>(
      MemoryManager::mallocGpu(sizeof(size_t) * max_batch_chunks));

  m_device_decomp_workspace_size = 0;
  nvcompStatus_t status = nvcompBatchedLZ4DecompressGetTempSize(
      max_batch_chunks, max_decomp_chunk_size, &m_device_decomp_workspace_size);
  if (status != nvcompSuccess) {
    throw std::runtime_error(
        std::string("nvcompBatchedLZ4DecompressGetTempSize() failed with: ") +
        std::string(magic_enum::enum_name(status)));
  }
  m_device_decomp_workspace =
      MemoryManager::mallocGpu(m_device_decomp_workspace_size);
  m_device_actual_uncompressed_bytes = static_cast<size_t *>(
      MemoryManager::mallocGpu(sizeof(size_t) * max_batch_chunks));
  m_device_uncompressed_bytes = static_cast<size_t *>(
      MemoryManager::mallocGpu(sizeof(size_t) * max_batch_chunks));

  m_device_status_ptrs = static_cast<nvcompStatus_t *>(
      MemoryManager::mallocGpu(sizeof(nvcompStatus_t) * max_batch_chunks));

  m_host_uncompressed_chunk_sizes = static_cast<size_t *>(
      MemoryManager::mallocPinned(sizeof(size_t) * max_batch_chunks));
  m_device_uncompressed_chunk_sizes = static_cast<size_t *>(
      MemoryManager::mallocGpu(sizeof(size_t) * max_batch_chunks));
}

GpuDecompressor::~GpuDecompressor() {
  MemoryManager::freePinned(m_host_compressed_ptrs);
  MemoryManager::freeGpu(m_device_compressed_ptrs);
  MemoryManager::freePinned(m_host_uncompressed_ptrs);
  MemoryManager::freeGpu(m_device_uncompressed_ptrs);
  MemoryManager::freePinned(m_host_compressed_chunk_sizes);
  MemoryManager::freeGpu(m_device_compressed_bytes);

  MemoryManager::freeGpu(m_device_decomp_workspace);
  MemoryManager::freeGpu(m_device_actual_uncompressed_bytes);
  MemoryManager::freeGpu(m_device_uncompressed_bytes);
  MemoryManager::freeGpu(m_device_status_ptrs);
  MemoryManager::freePinned(m_host_uncompressed_chunk_sizes);
  MemoryManager::freeGpu(m_device_uncompressed_chunk_sizes);
}

int GpuDecompressor::get_last_batch_bytes_decompressed() {
  DCHECK_GT(last_batch_num_chunks, 0) << "No outstanding asynchronous call.";
  // Assume device_status_ptrs is an array of nvcompStatus_t on the device
  std::vector<nvcompStatus_t> host_status_ptrs(last_batch_num_chunks);

  // Copy the status from device to host
  gpu_run(cudaMemcpy(host_status_ptrs.data(), m_device_status_ptrs,
                     last_batch_num_chunks * sizeof(nvcompStatus_t),
                     cudaMemcpyDeviceToHost));

  // Check the status of each decompression task
  for (int i = 0; i < last_batch_num_chunks; i++) {
    if (host_status_ptrs[i] != nvcompSuccess) {
      LOG(ERROR) << "GPU decompression failed for chunk " << i
                 << " with status "
                 << magic_enum::enum_name(host_status_ptrs[i]);
      last_batch_num_chunks = 0;
      return -(i + 1);
    }
  }

  std::vector<size_t> host_actual_uncompressed_bytes(last_batch_num_chunks);
  gpu_run(cudaMemcpy(
      host_actual_uncompressed_bytes.data(), m_device_actual_uncompressed_bytes,
      last_batch_num_chunks * sizeof(size_t), cudaMemcpyDeviceToHost));
  size_t decompressed_size = 0;
  for (size_t i = 0; i < last_batch_num_chunks; i++) {
    decompressed_size += host_actual_uncompressed_bytes[i];
  }
  last_batch_num_chunks = 0;
  return decompressed_size;
}

void **GpuDecompressor::get_device_uncompressed_ptrs(
    const std::vector<std::span<char>> &block_output_buffers,
    const std::vector<std::vector<uint32_t>> &block_chunk_sizes,
    size_t batch_size, cudaStream_t stream) {
  size_t ix_chunk = 0;
  for (size_t i = 0; i < block_output_buffers.size(); ++i) {
    for (size_t j = 0; j < block_chunk_sizes[i].size(); ++j) {
      m_host_uncompressed_ptrs[ix_chunk] =
          block_output_buffers[i].data() + j * m_max_decomp_chunk_size;
      ++ix_chunk;
    }
  }
  DCHECK_EQ(ix_chunk, batch_size);

  gpu_run(cudaMemcpy(m_device_uncompressed_ptrs, m_host_uncompressed_ptrs,
                     sizeof(void *) * batch_size, cudaMemcpyHostToDevice));

  return m_device_uncompressed_ptrs;
}

size_t *GpuDecompressor::get_device_uncompressed_chunk_sizes(
    size_t batch_size, cudaStream_t stream) {
  // We know that every chunk will be < max_decomp_chunk_size
  // so we can skip a call to nvcompBatchedLZ4GetDecompressSizeAsync

  for (size_t i = 0; i < batch_size; ++i) {
    m_host_uncompressed_chunk_sizes[i] = m_max_decomp_chunk_size;
  }

  gpu_run(cudaMemcpy(m_device_uncompressed_chunk_sizes,
                     m_host_uncompressed_chunk_sizes,
                     sizeof(void *) * batch_size, cudaMemcpyHostToDevice));

  return m_device_uncompressed_chunk_sizes;
}

/* Batch version of the function above. */
size_t *GpuDecompressor::get_device_compressed_bytes(
    const std::vector<std::vector<uint32_t>> &blocks_chunk_sizes,
    size_t batch_size, cudaStream_t stream) {
  size_t ix_chunk = 0;
  for (const auto &block_chunk_size : blocks_chunk_sizes) {
    for (unsigned int chunk_size : block_chunk_size) {
      m_host_compressed_chunk_sizes[ix_chunk] = chunk_size;
      ++ix_chunk;
    }
  }
  DCHECK_EQ(ix_chunk, batch_size);

  gpu_run(cudaMemcpy(m_device_compressed_bytes, m_host_compressed_chunk_sizes,
                     sizeof(size_t) * batch_size, cudaMemcpyHostToDevice));

  return m_device_compressed_bytes;
}

/* Batch version of the function above. */
void **GpuDecompressor::get_device_compressed_ptrs(
    const std::vector<std::span<char>> &compressed_buffers,
    const std::vector<std::vector<uint32_t>> &chunk_sizes, size_t batch_size,
    cudaStream_t stream) {
  size_t ix_chunk = 0;
  for (size_t i = 0; i < compressed_buffers.size(); ++i) {
    size_t current_offset = 0;
    for (size_t j = 0; j < chunk_sizes[i].size(); ++j) {
      m_host_compressed_ptrs[ix_chunk] =
          compressed_buffers[i].data() + current_offset;
      current_offset += chunk_sizes[i][j];
      ++ix_chunk;
    }
  }
  DCHECK_EQ(ix_chunk, batch_size);

  gpu_run(cudaMemcpy(m_device_compressed_ptrs, m_host_compressed_ptrs,
                     sizeof(void *) * batch_size, cudaMemcpyHostToDevice));

  return m_device_compressed_ptrs;
}

int GpuDecompressor::decompress_gpu(const size_t batch_size,
                                    void **device_uncompressed_ptrs,
                                    void **device_compressed_ptrs,
                                    size_t *device_compressed_bytes,
                                    size_t output_buffer_size,
                                    cudaStream_t stream) {
  // to ensure kernels launched on the correct GPU
  set_device_on_scope d(topology::getInstance().getGpus()[m_gpu_index_in_topo]);

  const bool synchronous = (stream == nullptr);
  if (synchronous) {
    stream = createNonBlockingStream();
  }
  DCHECK_GT(batch_size, 0) << "Cannot have an empty batch.";
  last_batch_num_chunks = batch_size;

  nvcompStatus_t status;

  size_t *device_uncompressed_bytes =
      get_device_uncompressed_chunk_sizes(batch_size, stream);

  // Run decompression
  status = nvcompBatchedLZ4DecompressAsync(
      device_compressed_ptrs, device_compressed_bytes,
      device_uncompressed_bytes, m_device_actual_uncompressed_bytes, batch_size,
      m_device_decomp_workspace, m_device_decomp_workspace_size,
      device_uncompressed_ptrs, m_device_status_ptrs, stream);

  if (status != nvcompSuccess) {
    LOG(ERROR)
        << "decompress_gpu failed on nvcompBatchedLZ4DecompressAsync with "
        << magic_enum::enum_name(status);
    return -status;
  }

  if (synchronous) {
    syncAndDestroyStream(stream);
    // Assume device_status_ptrs is an array of nvcompStatus_t on the device
    std::vector<nvcompStatus_t> host_status_ptrs(batch_size);

    // Copy the status from device to host
    gpu_run(cudaMemcpy(host_status_ptrs.data(), m_device_status_ptrs,
                       batch_size * sizeof(nvcompStatus_t),
                       cudaMemcpyDeviceToHost));

    // Check the status of each decompression task
    for (size_t i = 0; i < batch_size; i++) {
      if (host_status_ptrs[i] != nvcompSuccess) {
        LOG(ERROR) << "GPU decompression failed for chunk " << i
                   << " with status "
                   << magic_enum::enum_name(host_status_ptrs[i]);
        return -host_status_ptrs[i];
      }
    }

    std::vector<size_t> host_actual_uncompressed_bytes(batch_size);
    gpu_run(cudaMemcpy(host_actual_uncompressed_bytes.data(),
                       m_device_actual_uncompressed_bytes,
                       batch_size * sizeof(size_t), cudaMemcpyDeviceToHost));
    size_t decompressed_size = 0;
    for (size_t i = 0; i < batch_size; i++) {
      decompressed_size += host_actual_uncompressed_bytes[i];
    }
    DCHECK_LE(decompressed_size, output_buffer_size);
    last_batch_num_chunks = 0;  // we use last_batch_num_chunks to check if we
                                // have an outstanding asynchronous call
    return decompressed_size;
  }

  return (int)0;
}

/* Decompress a batch of compressed data chunks on the GPU. */
[[nodiscard]] int GpuDecompressor::batch_decompress_block_gpu(
    const std::vector<std::vector<uint32_t>> &block_chunk_sizes,
    const std::vector<std::span<char>> &block_compressed_buffers,
    const std::vector<std::span<char>> &block_output_buffers,
    cudaStream_t stream) {
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

  void **device_uncompressed_ptrs = get_device_uncompressed_ptrs(
      block_output_buffers, block_chunk_sizes, batch_size, stream);

  void **device_compressed_ptrs = get_device_compressed_ptrs(
      block_compressed_buffers, block_chunk_sizes, batch_size, stream);

  size_t *device_compressed_bytes =
      get_device_compressed_bytes(block_chunk_sizes, batch_size, stream);

  return decompress_gpu(batch_size, device_uncompressed_ptrs,
                        device_compressed_ptrs, device_compressed_bytes,
                        output_buffer_size, stream);
}

/* Assuming that compressed_buffer and output_buffer are GPU resident.  */
[[nodiscard]] int GpuDecompressor::decompress_block_gpu(
    std::vector<uint32_t> &chunk_sizes, std::span<char> compressed_buffer,
    std::span<char> output_buffer, cudaStream_t stream) {
  DCHECK_GT(chunk_sizes.size(), 0) << "Cannot have an empty batch.";
  std::vector<std::vector<uint32_t>> block_chunk_sizes(1);
  block_chunk_sizes[0] = std::move(chunk_sizes);

  std::vector<std::span<char>> block_compressed_buffers(1);
  block_compressed_buffers[0] = compressed_buffer;

  std::vector<std::span<char>> block_output_buffers(1);
  block_output_buffers[0] = output_buffer;

  return batch_decompress_block_gpu(block_chunk_sizes, block_compressed_buffers,
                                    block_output_buffers, stream);
}

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
