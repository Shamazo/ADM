/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2023
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

#include "mem-move-device.hpp"

#include <lz4.h>

#include <atomic>
#include <codegen/jit/pipeline.hpp>
#include <platform/memory/block-manager.hpp>
#include <platform/memory/memory-manager.hpp>
#include <platform/threadpool/threadpool.hpp>
#include <platform/util/timing.hpp>
#include <platform/util/tracing.hpp>

#include "compression.hpp"
#include "lib/util/catalog.hpp"
#include "olap/plugins/binary-block-nvme-plugin.hpp"

buff_pair buff_pair::not_moved(proteus::managed_ptr buff) {
  return {std::move(buff), nullptr};
}

bool buff_pair::moved() const { return !old_buff; }

proteus::managed_ptr MemMoveDevice::MemMoveConf::force_push(
    const proteus::managed_ptr &src, size_t bytes, int target_device,
    uint64_t srcServer, cudaStream_t movestrm) {
  // FIXME: buffer manager should be able to provide blocks of arbitrary size
  assert(bytes <= BlockManager::block_size);
  auto buff = BlockManager::h_get_buffer(target_device);

  if (bytes > 0) {
    BlockManager::overwrite_bytes(buff.get(), src.get(), bytes, movestrm,
                                  false);
  }

  return buff;
}

proteus::managed_ptr MemMoveDevice::MemMoveConf::force_push_from_nvme(
    const proteus::managed_ptr &src, int target_device, cudaStream_t movestrm,
    workunit *wu) {
  auto page_id = NvmePlugin::PageId_t::from_ptr(src.get());
  DCHECK_NE(nvme_plugin, nullptr);
  const auto page_io_info = nvme_plugin->getPageIoInfo(page_id);

  auto buff = BlockManager::h_get_buffer(target_device);

  if (page_io_info.is_compressed) {
    // allocate temporary storage for IO target
    // decompress into the target buffer
    char *compressed_buff =
        static_cast<char *>(std::aligned_alloc(4096, page_io_info.size));
    auto decompressed_buff = static_cast<char *>(buff.get());
    wu->complete += 1;

    // decompressed buff will have a lifetime exceeding the callback, as the
    // catcher spins on wu->complete.
    // compressed buf ownership is moved into the lambda which frees it
    auto cb = [page_id, wu, compressed_buff, io_size = page_io_info.size,
               max_decomp_chunk_size = page_io_info.decompressed_chunk_size,
               chunk_sizes = std::move(page_io_info.chunk_sizes),
               decompressed_buff = decompressed_buff] {
      auto decomp_span =
          std::span<char>(decompressed_buff, BlockManager::block_size);
      auto comp_span = std::span<char>(compressed_buff, io_size);

      auto decomp_res = decompress_block(chunk_sizes, comp_span, decomp_span,
                                         max_decomp_chunk_size);
      CHECK_GT(decomp_res, 0)
          << "failed to decompress block for page: " << page_id;
      std::free(compressed_buff);
      std::atomic_fetch_add_explicit(&wu->complete, -1,
                                     std::memory_order_relaxed);
      DCHECK_GE(wu->complete, 0);
    };
    auto read_size = page_io_info.size;
    if (read_size % 512 != 0) {
      read_size += 512 - (read_size % 512);  // align to 512 bytes for O_DIRECT
    }
    DCHECK_GE(read_size, 0);
    DCHECK_EQ(read_size % 512, 0);
    DCHECK_GE(page_io_info.offset, 0);
    DCHECK_EQ(page_io_info.offset % 512, 0);

    io_uring->read(page_io_info.fd, compressed_buff, read_size,
                   page_io_info.offset, std::move(cb));

  } else {
    DCHECK_LE(page_io_info.size, BlockManager::block_size);
    wu->complete += 1;

    // TODO really should probably pass the result of IO to the callback
    proteus::storage::IoUringThreadUnsafe::CompletionCallBackSuccess cb = [wu] {
      std::atomic_fetch_add_explicit(&wu->complete, -1,
                                     std::memory_order_relaxed);
    };
    auto read_size = page_io_info.size;
    if (read_size % 512 != 0) {
      read_size += 512 - (read_size % 512);  // align to 512 bytes for O_DIRECT
    }

    io_uring->read(page_io_info.fd, buff.get(), read_size, page_io_info.offset,
                   cb);
  }
  return buff;
}

buff_pair MemMoveDevice::MemMoveConf::push(proteus::managed_ptr src,
                                           size_t bytes, int target_device,
                                           uint64_t srcServer, workunit *wu) {
  DCHECK_EQ(srcServer, 0);
  if (NvmePlugin::PageId_t::isPageIdPtr(src.get())) {
    auto buff = force_push_from_nvme(src, target_device, strm, wu);
    src.release();
    return buff_pair::not_moved(
        std::move(buff));  // src is not a pointer and does not need freeing
  } else {
    // currently only used do NVMe to CPU IO
    // TODO: is pass the fact we are doing IO to the operator constructor
    const auto *d = topology::getInstance().getGpuAddressed(src.get());
    int dev = d ? static_cast<int>(d->id) : -1;

    if (dev == target_device) {
      return buff_pair::not_moved(std::move(src));  // block in correct device
    }

    auto buff = force_push(src, bytes, target_device, srcServer, strm);
    return buff_pair{std::move(buff), std::move(src)};
  }
}

extern "C" {
void make_mem_move_device(char **src_ptrs, size_t *bytes, int target_device,
                          uint64_t srcServer, MemMoveDevice::MemMoveConf *mmc,
                          int num_buffers, pb *pair_buffs,
                          MemMoveDevice::workunit *wu) noexcept {
  wu->complete = 0;
  for (int i = 0; i < num_buffers; i++) {
    auto x = mmc->push(proteus::managed_ptr{src_ptrs[i]}, bytes[i],
                       target_device, srcServer, wu);
    pair_buffs[i] = {x.new_buff.release(), x.old_buff.release()};
  }
  if (NvmePlugin::PageId_t::isPageIdPtr(src_ptrs[0])) {
    mmc->io_uring->submit();
    /// poll until there are (probably) enough buffers for the next iteration
    while (mmc->idle.size_unsafe() < num_buffers) {
      mmc->io_uring->poll();
      std::this_thread::yield();
    }
  }
}

MemMoveDevice::workunit *acquireWorkUnit(
    MemMoveDevice::MemMoveConf *mmc) noexcept {
  return mmc->acquire();
}

void propagateWorkUnit(MemMoveDevice::MemMoveConf *mmc,
                       MemMoveDevice::workunit *buff, bool is_noop) noexcept {
  mmc->propagate(buff, is_noop);
}
}

void MemMoveDevice::genReleaseOldBuffer(OlapParallelContext *context,
                                        llvm::Value *src) const {
  auto charPtrType = llvm::Type::getInt8PtrTy(context->getLLVMContext());
  context->gen_call(release_buffer,
                    {context->getBuilder()->CreateBitCast(src, charPtrType)});
}

void MemMoveDevice::produce_(OlapParallelContext *context) {
  auto &llvmContext = context->getLLVMContext();
  auto int32_type = llvm::Type::getInt32Ty(context->getLLVMContext());
  auto charPtrType = llvm::Type::getInt8PtrTy(context->getLLVMContext());

  auto pg =
      Catalog::getInstance().getPlugin(wantedFields[0]->getRelationName());
  auto oidType = pg->getOIDType()->getLLVMType(llvmContext);

  std::vector<llvm::Type *> tr_types;
  for (auto wantedField : wantedFields) {
    tr_types.push_back(wantedField->getLLVMType(llvmContext));
    tr_types.push_back(
        wantedField->getLLVMType(llvmContext));  // old buffer, to be released
  }
  tr_types.push_back(oidType);  // cnt
  tr_types.push_back(oidType);  // oid

  data_type = llvm::StructType::get(llvmContext, tr_types);

  RecordAttribute tupleCnt =
      RecordAttribute(wantedFields[0]->getRelationName(), "activeCnt",
                      pg->getOIDType());  // FIXME: OID type for blocks ?
  RecordAttribute tupleIdentifier = RecordAttribute(
      wantedFields[0]->getRelationName(), activeLoop, pg->getOIDType());

  // Generate catch code
  int p = context->appendParameter(llvm::PointerType::get(data_type, 0), true,
                                   true);
  context->setGlobalFunction();

  auto Builder = context->getBuilder();
  auto entryBB = Builder->GetInsertBlock();
  auto F = entryBB->getParent();

  auto mainBB = llvm::BasicBlock::Create(llvmContext, "main", F);

  auto endBB = llvm::BasicBlock::Create(llvmContext, "end", F);
  context->setEndingBlock(endBB);

  Builder->SetInsertPoint(entryBB);

  auto params = Builder->CreateLoad(
      context->getArgument(p)->getType()->getPointerElementType(),
      context->getArgument(p));

  map<RecordAttribute, ProteusValueMemory> variableBindings;

  for (size_t i = 0; i < wantedFields.size(); ++i) {
    auto param = Builder->CreateExtractValue(params, 2 * i);

    auto src = Builder->CreateExtractValue(params, 2 * i + 1);

    genReleaseOldBuffer(context, src);

    variableBindings[*(wantedFields[i])] =
        context->toMem(param, context->createFalse());
  }

  auto cnt = Builder->CreateExtractValue(params, 2 * wantedFields.size());

  variableBindings[tupleCnt] = context->toMem(cnt, context->createFalse());

  auto oid = Builder->CreateExtractValue(params, 2 * wantedFields.size() + 1);

  variableBindings[tupleIdentifier] =
      context->toMem(oid, context->createFalse());

  context->setCurrentEntryBlock(Builder->GetInsertBlock());

  Builder->SetInsertPoint(mainBB);

  OperatorState state{*this, variableBindings};
  getParent()->consume(context, state);

  Builder->CreateBr(endBB);

  Builder->SetInsertPoint(context->getCurrentEntryBlock());
  // Insert an explicit fall through from the current (entry) block to the
  // CondBB.
  Builder->CreateBr(mainBB);

  Builder->SetInsertPoint(context->getEndingBlock());

  context->popPipeline();

  catch_pip = context->removeLatestPipeline();

  // push new pipeline for the throw part
  context->pushPipeline();

  device_id_var = context->appendStateVar(int32_type);
  memmvconf_var = context->appendStateVar(charPtrType);

  context->registerOpen(this, [this](Pipeline *pip) { this->open(pip); });
  context->registerClose(this, [this](Pipeline *pip) { this->close(pip); });

  getChild()->produce(context);
}

ProteusValueMemory MemMoveDevice::getServerId(
    OlapParallelContext *context, const OperatorState &childState) const {
  return context->toMem(context->createInt64(0), context->createFalse());
}

void MemMoveDevice::consume(OlapParallelContext *context,
                            const OperatorState &childState) {
  // Prepare
  auto &llvmContext = context->getLLVMContext();
  auto Builder = context->getBuilder();
  auto insBB = Builder->GetInsertBlock();
  // TODO a bit of a hack to not manipulate tuple cnts for non-scan moves
  const bool non_scan_move =
      (wantedFields[0]->getRelationName().find("tmp") != std::string::npos) ||
      (wantedFields[0]->getRelationName().find("Pelago") != std::string::npos);

  auto charPtrType = llvm::Type::getInt8PtrTy(context->getLLVMContext());

  auto workunit_type = llvm::StructType::get(
      llvmContext, std::vector<llvm::Type *>{charPtrType, charPtrType});

  // Find block size
  Plugin *pg =
      Catalog::getInstance().getPlugin(wantedFields[0]->getRelationName());
  RecordAttribute tupleCnt{wantedFields[0]->getRelationName(), "activeCnt",
                           pg->getOIDType()};  // FIXME: OID type for blocks ?

  ProteusValueMemory mem_cntWrapper = childState[tupleCnt];
  ProteusValueMemory mem_srcServer = getServerId(context, childState);
  Builder->SetInsertPoint(context->getCurrentEntryBlock());

  auto device_id = ((OlapParallelContext *)context)->getStateVar(device_id_var);

  // Begin inserting code
  Builder->SetInsertPoint(insBB);
  auto N = Builder->CreateLoad(
      mem_cntWrapper.mem->getType()->getPointerElementType(),
      mem_cntWrapper.mem);
  auto srcS = Builder->CreateLoad(
      mem_srcServer.mem->getType()->getPointerElementType(), mem_srcServer.mem);

  RecordAttribute tupleIdentifier{wantedFields[0]->getRelationName(),
                                  activeLoop, pg->getOIDType()};

  ProteusValueMemory mem_oidWrapper = childState[tupleIdentifier];
  llvm::Value *oid = Builder->CreateLoad(
      mem_oidWrapper.mem->getType()->getPointerElementType(),
      mem_oidWrapper.mem);

  llvm::Value *memmv =
      ((OlapParallelContext *)context)->getStateVar(memmvconf_var);

  std::vector<llvm::Value *> pushed;
  llvm::Value *is_noop = context->createTrue();

  auto *F = context->getGlobalFunction();
  llvm::ArrayType *int64_array_type =
      llvm::ArrayType::get(context->createSizeType(), wantedFields.size());
  llvm::AllocaInst *mv_sizes_bytes = context->CreateEntryBlockAlloca(
      F, "mv_sizes_bytes", int64_array_type, nullptr);

  llvm::ArrayType *charptr_array_type =
      llvm::ArrayType::get(charPtrType, wantedFields.size());
  llvm::AllocaInst *mv_src_ptrs = context->CreateEntryBlockAlloca(
      F, "mv_src_ptrs", charptr_array_type, nullptr);

  // store size in bytes, and src_ptr into in the above declared arrays for each
  // field to be moved
  auto *page_id_mask = llvm::ConstantInt::get(context->createSizeType(),
                                              llvm::APInt(64, 1).shl(63));
  llvm::Value *any_ptr_is_page_id = context->createFalse();

  for (size_t i = 0; i < wantedFields.size(); ++i) {
    RecordAttribute block_attr(*(wantedFields[i]), true);

    ProteusValueMemory mem_valWrapper = childState[block_attr];

    auto mv_src_ptr = Builder->CreateBitCast(
        Builder->CreateLoad(
            mem_valWrapper.mem->getType()->getPointerElementType(),
            mem_valWrapper.mem),
        charPtrType);

    auto mv_block_type = mem_valWrapper.mem->getType()
                             ->getPointerElementType()
                             ->getPointerElementType();

    llvm::Value *size = llvm::ConstantInt::get(
        llvmContext, llvm::APInt(64, context->getSizeOf(mv_block_type)));
    auto Nloc = Builder->CreateZExtOrBitCast(N, size->getType());
    size = Builder->CreateMul(size, Nloc);  /// now in bytes
    llvm::Value *size_as_int64 = Builder->CreateIntCast(
        size, llvm::Type::getInt64Ty(llvmContext), false);

    // same index into both sizes and ptrs arrays
    llvm::Value *gepIndices[] = {
        llvm::ConstantInt::get(llvm::Type::getInt32Ty(llvmContext), 0),
        llvm::ConstantInt::get(llvm::Type::getInt32Ty(llvmContext), i)};

    llvm::Value *size_ptr =
        Builder->CreateGEP(int64_array_type, mv_sizes_bytes, gepIndices);
    Builder->CreateStore(size_as_int64, size_ptr);

    auto *src_ptr =
        Builder->CreateGEP(charptr_array_type, mv_src_ptrs, gepIndices);

    //    check if any src_ptr is a page_id
    auto *intPtr = Builder->CreatePtrToInt(
        mv_src_ptr, llvm::Type::getInt64Ty(context->getLLVMContext()));
    auto *and_res = Builder->CreateAnd(intPtr, page_id_mask);
    any_ptr_is_page_id = Builder->CreateOr(
        any_ptr_is_page_id, Builder->CreateICmpEQ(and_res, page_id_mask));
    Builder->CreateStore(mv_src_ptr, src_ptr);
  }

  {
    if (!non_scan_move) {
      // if we have anyPage Ids we need to update tupleCnt to store a tupleCnt
      // instead of blockCnt, which the NvmePlugin stores in `tupleCnt`
      auto ifAnyPageId = context->gen_if({any_ptr_is_page_id});

      auto thenAnyIsPageId = std::move(ifAnyPageId)([&]() {  // NOLINT
        RecordAttribute actualTupleCnt{wantedFields[0]->getRelationName(),
                                       "tupleCnt", pg->getOIDType()};
        ProteusValueMemory mem_tupleCntWrapper = childState[actualTupleCnt];
        llvm::Value *mem_tupleCnt = Builder->CreateLoad(
            mem_tupleCntWrapper.mem->getType()->getPointerElementType(),
            mem_tupleCntWrapper.mem);
        //      context->log(mem_tupleCnt);
        Builder->CreateStore(mem_tupleCnt, mem_cntWrapper.mem);

      });
    }
    // note, all of this is a bit hacky, we ignore size in the above and below
    // loops for page ids, but we could actually just store the size/fd/offset
    // in the Operator state
  }

  // create an array of `pb` to pass to make_mem_move_device as a return
  // argument
  auto mv_num_bufs = context->createInt32(wantedFields.size());
  auto *llvm_pb_type = context->toLLVM<std::remove_cv_t<pb>>();
  llvm::ArrayType *llvm_pb_array_type =
      llvm::ArrayType::get(llvm_pb_type, wantedFields.size());
  llvm::AllocaInst *pb_array =
      Builder->CreateAlloca(llvm_pb_array_type, nullptr, "pb_array");

  auto workunit_ptr8 = context->gen_call(acquireWorkUnit, {memmv});
  auto workunit_ptr = Builder->CreateBitCast(
      workunit_ptr8, llvm::PointerType::getUnqual(workunit_type));
  // TODO pass selectivities to make_mem_move_device
  context->gen_call(make_mem_move_device,
                    {mv_src_ptrs, mv_sizes_bytes, device_id, srcS, memmv,
                     mv_num_bufs, pb_array, workunit_ptr});

  for (size_t i = 0; i < wantedFields.size(); ++i) {
    RecordAttribute block_attr(*(wantedFields[i]), true);
    ProteusValueMemory mem_valWrapper = childState[block_attr];

    llvm::Value *gep_index_moved[] = {context->createInt32(0),
                                      context->createInt32(i),
                                      context->createInt32(0)};
    llvm::Value *gep_index_to_release[] = {context->createInt32(0),
                                           context->createInt32(i),
                                           context->createInt32(1)};
    llvm::Value *moved_ptr = Builder->CreateGEP(
        pb_array->getType()->getNonOpaquePointerElementType(), pb_array,
        gep_index_moved, "gep_buffpair_structs_moved");
    llvm::Value *to_release_ptr = Builder->CreateGEP(
        pb_array->getType()->getNonOpaquePointerElementType(), pb_array,
        gep_index_to_release, "gep_buffpair_structs_to_release");
    llvm::Value *moved =
        Builder->CreateLoad(charPtrType, moved_ptr, "load_moved");
    llvm::Value *to_release =
        Builder->CreateLoad(charPtrType, to_release_ptr, "load_to_release");

    pushed.push_back(Builder->CreateBitCast(
        moved, mem_valWrapper.mem->getType()->getPointerElementType()));
    pushed.push_back(Builder->CreateBitCast(
        to_release, mem_valWrapper.mem->getType()->getPointerElementType()));
    //    TODO handle not actually a noop for nvme
    is_noop = context->createFalse();
    //        Builder->CreateAnd(is_noop, Builder->CreateICmpEQ(moved,
    //        to_release));
  }

  // This is now definitely number of tuples
  auto *num_tuples = Builder->CreateLoad(
      mem_cntWrapper.mem->getType()->getPointerElementType(),
      mem_cntWrapper.mem);
  pushed.push_back(num_tuples);
  pushed.push_back(oid);

  llvm::Value *d = llvm::UndefValue::get(data_type);
  for (size_t i = 0; i < pushed.size(); ++i) {
    d = Builder->CreateInsertValue(d, pushed[i], i);
  }

  auto workunit_dat = Builder->CreateLoad(
      workunit_ptr->getType()->getPointerElementType(), workunit_ptr);
  auto d_ptr = Builder->CreateExtractValue(workunit_dat, 0);
  d_ptr =
      Builder->CreateBitCast(d_ptr, llvm::PointerType::getUnqual(data_type));
  Builder->CreateStore(d, d_ptr);

  // finally propagate the workunit
  context->gen_call(propagateWorkUnit, {memmv, workunit_ptr8, is_noop});
}

MemMoveDevice::MemMoveConf *MemMoveDevice::createMoveConf() const {
  void *pmmc = MemoryManager::mallocPinned(sizeof(MemMoveConf));
  return new (pmmc) MemMoveConf;
}

void MemMoveDevice::destroyMoveConf(MemMoveDevice::MemMoveConf *mmc) const {
  mmc->~MemMoveConf();
  MemoryManager::freePinned(mmc);
}

void MemMoveDevice::open(Pipeline *pip) {
  auto *wu = (workunit *)MemoryManager::mallocPinned(sizeof(workunit) * slack);
  event_range<range_log_op::MEMMOVE_OPEN> er{id, catch_pip->getUUID(),
                                             pip->getGroup()};

  // nvtxRangePushA("memmove::open");
  cudaStream_t strm = createNonBlockingStream();

  size_t data_size = (pip->getSizeOf(data_type) + 16 - 1) & ~((size_t)0xF);

  MemMoveConf *mmc = createMoveConf();

#ifndef NCUDA
  mmc->strm = strm;
#endif
  mmc->slack = slack;
  mmc->data_buffs = MemoryManager::mallocPinned(data_size * slack);
  auto *pg =
      Catalog::getInstance().getPlugin(wantedFields[0]->getRelationName());
  if (dynamic_cast<NvmePlugin *>(pg)) {
    mmc->nvme_plugin = dynamic_cast<NvmePlugin *>(pg);
    mmc->io_uring = std::make_unique<proteus::storage::IoUringThreadUnsafe>(32);
  }
  char *data_buff = (char *)mmc->data_buffs;
  for (size_t i = 0; i < slack; ++i) {
    wu[i].data = ((void *)(data_buff + i * data_size));
    wu[i].complete = 0;
    mmc->idle.push(wu + i);
  }
  // nvtxRangePushA("memmove::open2");
  for (size_t i = 0; i < slack; ++i) {
    gpu_run(cudaEventCreateWithFlags(
        &(wu[i].event), cudaEventDisableTiming | cudaEventBlockingSync));
  }
  // nvtxRangePop();

  mmc->worker = ThreadPool::getInstance().enqueue(
      &MemMoveDevice::catcher, this, mmc, pip->getGroup(), exec_location{},
      pip->getSession());

  pip->setStateVar<int>(device_id_var, getTargetDevice());

  pip->setStateVar<void *>(memmvconf_var, mmc);
  // nvtxRangePop();
}

int MemMoveDevice::getTargetDevice() const {
  if (to_cpu) return -1;
  return topology::getInstance().getActiveGpu().id;
}

void MemMoveDevice::close(Pipeline *pip) {
  auto *mmc = pip->getStateVar<MemMoveConf *>(memmvconf_var);

  {
    event_range<range_log_op::MEMMOVE_CLOSE> er{id, catch_pip->getUUID(),
                                                pip->getGroup()};
    if (mmc->io_uring) {
      mmc->io_uring->flush();
    }
    mmc->tran.close();

    nvtxRangePop();
    mmc->worker.get();
  }

  event_range<range_log_op::MEMMOVE_CLOSE_CLEAN_UP> er{id, catch_pip->getUUID(),
                                                       pip->getGroup()};
  syncAndDestroyStream(mmc->strm);

  nvtxRangePushA("MemMoveDev_running2");
  nvtxRangePushA("MemMoveDev_running");

  nvtxRangePushA("MemMoveDev_release");
  workunit *start_wu = nullptr;
  for (size_t i = 0; i < slack; ++i) {
    workunit *wu = mmc->idle.pop_unsafe();
    gpu_run(cudaEventDestroy(wu->event));
    if (i == 0 || wu < start_wu) start_wu = wu;
  }
  nvtxRangePop();
  nvtxRangePop();

  MemoryManager::freePinned(mmc->data_buffs);
  MemoryManager::freePinned(start_wu);

  mmc->idle.close();

  destroyMoveConf(mmc);
}

void MemMoveDevice::MemMoveConf::propagate(MemMoveDevice::workunit *buff,
                                           bool is_noop) {
  if (!is_noop) gpu_run(cudaEventRecord(buff->event, strm));

  tran.push(buff);
}

MemMoveDevice::workunit *MemMoveDevice::MemMoveConf::acquire() {
  // time_block t{"pop: "};
  // LOG(INFO) << "pop";
  MemMoveDevice::workunit *ret = nullptr;
#ifndef NDEBUG
  bool popres =
#endif
      idle.pop(ret);
  assert(popres);
  return ret;
}

bool MemMoveDevice::MemMoveConf::getPropagated(MemMoveDevice::workunit **ret) {
  if (!tran.pop(*ret)) return false;
  gpu_run(cudaEventSynchronize((*ret)->event));
  while ((*ret)->complete != 0) {
    std::this_thread::yield();
  }
  return true;
}

void MemMoveDevice::MemMoveConf::release(MemMoveDevice::workunit *buff) {
  // LOG(INFO) << "pushed";
  // time_block t{"pushed: "};
  idle.push(buff);
}

void MemMoveDevice::catcher(MemMoveConf *mmc, int group_id,
                            exec_location target_dev, const void *session) {
  set_exec_location_on_scope d(target_dev);
  std::this_thread::yield();

  nvtxRangePushA("memmove::catch");

  auto pip = catch_pip->getPipeline(group_id);

  nvtxRangePushA("memmove::catch_open");
  pip->open(session);
  nvtxRangePop();

  {
    do {
      MemMoveDevice::workunit *p = nullptr;
      if (!mmc->getPropagated(&p)) break;
      for (size_t i = 0; i < wantedFields.size(); ++i) {
        ((proteus::managed_ptr *)(p->data))[i * 2] =
            mmc->pull(std::move(((proteus::managed_ptr *)(p->data))[i * 2]));
      }
      {
        event_range<range_log_op::MEMMOVE_CONSUME> er{id, catch_pip->getUUID(),
                                                      pip->getGroup()};
        nvtxRangePushA("memmove::catch_cons");
        pip->consume(p->data);
        nvtxRangePop();
      }

      mmc->release(p);
    } while (true);
  }

  event_range<range_log_op::MEMMOVE_CLOSE> er{id, catch_pip->getUUID(),
                                              pip->getGroup()};
  nvtxRangePushA("memmove::catch_close");
  pip->close();
  nvtxRangePop();

  nvtxRangePop();
}
