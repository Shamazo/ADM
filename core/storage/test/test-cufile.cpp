/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2022
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

#include <cuda_runtime.h>
// Nvidia wrote a bad doxygen string, but that is not our problem
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
// #include <cufile.h>
#include <cufile_181/cufile.h>
#pragma clang diagnostic pop
#include <gtest/gtest.h>

#include <platform/memory/block-manager.hpp>
#include <platform/util/glog.hpp>

#include "storage-test.hpp"

using loadBlockFunc = std::function<proteus::managed_ptr(int)>;

loadBlockFunc getLoadCustKeyBlockfunc() {
  return [](int targetDevice) {
    if (targetDevice >= 0) {
      LOG(FATAL) << "This function can only load to CPU";
    }
    auto block = BlockManager::get_buffer();

    std::string fileName = "inputs/ssbm100/customer.csv.c_custkey";

    auto fd = open(fileName.c_str(), O_DIRECT | O_RDONLY);
    if (fd == -1) {
      LOG(ERROR) << "Failed to open file for reading: " << fileName.c_str();
      LOG(ERROR) << "Errno: " << errno;
      assert(fd != -1);
    }
    auto bytes_read = read(fd, block.get(), 2097152ul);
    if (bytes_read != 2097152) {
      assert(bytes_read == 2097152);
    }
    close(fd);

    return block;
  };
}

loadBlockFunc gpuDirectGetLoadCustKeyBlockfunc() {
  return [](int targetDevice) {
    // load to CPU
    if (targetDevice < 0) {
      return getLoadCustKeyBlockfunc()(-1);
    }
    // load to GPU
    auto& topo = topology::getInstance();
    set_exec_location_on_scope d{topo.getGpus().at(targetDevice)};

    size_t readSize = 2097152ul;
    ssize_t ret;
    CUfileError_t status;

    std::string fileName = "inputs/ssbm100/customer.csv.c_custkey";

    auto fd = open(fileName.c_str(), O_DIRECT | O_RDONLY);
    if (fd == -1) {
      LOG(ERROR) << "Failed to open file for reading: " << fileName.c_str();
      LOG(ERROR) << "Errno: " << errno;
      assert(fd != -1);
    }

    status = cuFileDriverOpen();
    if (status.err != CU_FILE_SUCCESS) {
      LOG(ERROR) << " cuFile driver failed to open: " << status.err << ", "
                 << cufileop_status_error(status.err);
      close(fd);
      return proteus::managed_ptr(nullptr);
    }

    // register a file handle
    CUfileDescr_t cf_descr;
    CUfileHandle_t cf_handle;
    memset((void*)&cf_descr, 0, sizeof(CUfileDescr_t));
    cf_descr.handle.fd = fd;
    cf_descr.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
    status = cuFileHandleRegister(&cf_handle, &cf_descr);
    if (status.err != CU_FILE_SUCCESS) {
      LOG(ERROR) << "cuFileHandleRegister fd " << fd << " status "
                 << status.err;
      close(fd);
      return proteus::managed_ptr(nullptr);
    }

    // allocate memory on the gpu to load the 2MiB to
    auto destinationMemory = BlockManager::h_get_buffer(0);

    // cuFileBufRegister should be called for device buffers targeted by cufile
    //    but this is a performance consideration not correctness
    //    status = cuFileBufRegister(destinationMemory, readSize, 0);
    //    if (status.err != CU_FILE_SUCCESS) {
    //      LOG(ERROR) << "buffer registration failed " << status.err;
    //      cuFileHandleDeregister(cf_handle);
    //      close(fd);
    //      MemoryManager::freeGpu(destinationMemory);
    //      return proteus::managed_ptr(nullptr);
    //    }

    // finally, do our read
    // last two arguments are file offset, and buffer offset, both 0 here
    // because we just want to read the first 2MiB of the file
    ret = cuFileRead(cf_handle, destinationMemory.get(), readSize, 0, 0);

    if (ret < 0 || ret != readSize) {
      LOG(ERROR) << "cuFileRead failed " << ret;
    }

    // if we pinned, release the GPU memory pinning
    //    LOG(INFO) << "Unpinning cuFile buffer." << std::endl;
    //    status = cuFileBufDeregister(destinationMemory);
    //    if (status.err != CU_FILE_SUCCESS) {
    //      LOG(ERROR) << "buffer deregister failed";
    //      BlockManager::release_buffer(std::move(destinationMemory));
    //      cuFileHandleDeregister(cf_handle);
    //      close(fd);
    //      return proteus::managed_ptr(nullptr);
    //    }

    return destinationMemory;
  };
}

TEST(cuFile, smokeTestCuFile) {
  LOG(INFO) << "running cuFile smoke test";
  auto& topo = topology::getInstance();
  if (topo.getGpus().size() < 1) {
    LOG(WARNING) << "No GPUS, skipping test;";
    GTEST_SKIP();
  }
  auto& currentGpu = topo.getActiveGpu();
  auto gpuBlock = gpuDirectGetLoadCustKeyBlockfunc()(currentGpu.id);
  ASSERT_NE(gpuBlock.get(), nullptr)
      << "Failed to load data to gpu with cufile";
  auto* gpuDataLoadedTo = topo.getGpuAddressed(gpuBlock.get());
  ASSERT_EQ(currentGpu.id, gpuDataLoadedTo->id) << "Data loaded to wrong GPU";

  BlockManager::release_buffer(std::move(gpuBlock));
}

TEST(cuFile, streamingCuFileInterface) {
  auto& topo = topology::getInstance();
  if (topo.getGpus().size() < 1) {
    LOG(WARNING) << "No GPUS, skipping test;";
    GTEST_SKIP();
  }

  set_exec_location_on_scope d{topo.getGpus().at(0)};

  size_t readSize = 2097152ul;
  ssize_t ret;
  CUfileError_t status;

  std::string fileName = "inputs/ssbm100/customer.csv.c_custkey";

  auto fd = open(fileName.c_str(), O_DIRECT | O_RDONLY);
  ASSERT_NE(fd, -1) << "Failed to open file for reading: " << fileName.c_str();

  status = cuFileDriverOpen();
  ASSERT_EQ(status.err, CU_FILE_SUCCESS)
      << " cuFile driver failed to open: " << status.err << ", "
      << cufileop_status_error(status.err);

  // register a file handle
  CUfileDescr_t cf_descr;
  CUfileHandle_t cf_handle;
  memset((void*)&cf_descr, 0, sizeof(CUfileDescr_t));
  cf_descr.handle.fd = fd;
  cf_descr.type = CU_FILE_HANDLE_TYPE_OPAQUE_FD;
  status = cuFileHandleRegister(&cf_handle, &cf_descr);
  ASSERT_EQ(status.err, CU_FILE_SUCCESS)
      << "cuFileHandleRegister fd " << fd << " status " << status.err;

  // allocate memory on the gpu to load the 2MiB to
  auto destinationMemory = BlockManager::h_get_buffer(0);
  ssize_t bytes_read = 0;
  auto strm = createNonBlockingStream();
  status = cuFileStreamRegister(strm, CU_FILE_STREAM_PAGE_ALIGNED_INPUTS |
                                          CU_FILE_STREAM_FIXED_BUF_OFFSET |
                                          CU_FILE_STREAM_FIXED_FILE_OFFSET |
                                          CU_FILE_STREAM_FIXED_FILE_SIZE);
  ASSERT_EQ(status.err, CU_FILE_SUCCESS)
      << "cuFileHandleRegister fd " << fd << " status " << status.err;
  off_t offset = 0;
  status = cuFileReadAsync(cf_handle, destinationMemory.get(), &readSize,
                           &offset, &offset, &bytes_read, strm);
  ASSERT_EQ(status.err, CU_FILE_SUCCESS)
      << "Failed to cuFileReadAsync" << status.err;

  auto copied_to_cpu_buffer = BlockManager::get_buffer();
  gpu_run(cudaMemcpy(copied_to_cpu_buffer.get(), destinationMemory.get(),
                     readSize, cudaMemcpyKind::cudaMemcpyDefault));

  gpu_run(cuStreamSynchronize(strm));
  status = cuFileStreamDeregister(strm);
  ASSERT_EQ(status.err, CU_FILE_SUCCESS)
      << "failed to cuFileStreamDeregister status: " << status.err;
  syncAndDestroyStream(strm);

  auto cpu_loaded = getLoadCustKeyBlockfunc()(-1);
  EXPECT_EQ(memcmp(copied_to_cpu_buffer.get(), cpu_loaded.get(), readSize), 0);
  EXPECT_EQ(readSize, bytes_read);

  BlockManager::release_buffer(std::move(copied_to_cpu_buffer));
  BlockManager::release_buffer(std::move(destinationMemory));
  BlockManager::release_buffer(std::move(cpu_loaded));
}
