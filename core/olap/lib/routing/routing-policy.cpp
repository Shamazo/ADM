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

#include "lib/operators/router/routing-policy.hpp"

#include <magic_enum.hpp>
#include <olap/routing/affinitizers.hpp>
#include <olap/util/jit/control-flow/if-statement.hpp>
#include <platform/network/infiniband/infiniband-manager.hpp>
#include <platform/topology/topology.hpp>

#include "lib/expressions/expressions-generator.hpp"
#include "lib/operators/operators.hpp"
using magic_enum::ostream_operators::operator<<;

/// For using an AffinityPolicy from codegen
extern "C" size_t random_local_cu_index(void *ptr, AffinityPolicy *aff) {
  return aff->getIndexOfRandLocalCU(ptr);
}

/// For using an Affinitizer from codegen
extern "C" size_t random_local_cu_index_in_topo(void *ptr, Affinitizer *aff) {
  return aff->getLocalCUIndex(ptr);
}

namespace routing {
::routing_target Random::evaluate(OlapParallelContext *const context,
                                  const OperatorState &childState,
                                  ProteusValueMemory retrycnt) {
  if (fanout == 1) return {context->createInt64(0), context->createFalse()};
  auto *Builder = context->getBuilder();

  // state is initialized with a random number in the entry block
  // then we keep rehashing this random number to get new "random" numbers
  // without having to do a runtime call to rand()
  auto state = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    ExpressionGeneratorVisitor vis{context, childState};
    return context->toMem(expressions::rand().accept(vis));
  }();

  auto target = Builder->CreateLoad(
      state.mem->getType()->getPointerElementType(), state.mem);
  ExpressionGeneratorVisitor vis{context, childState};
  Builder->CreateStore(
      Builder->CreateZExtOrTrunc(
          expressions::HashExpression{
              expressions::ProteusValueExpression{
                  new IntType(), ProteusValue{target, state.isNull}}}
              .accept(vis)
              .value,
          target->getType()),
      state.mem);

  auto fanoutV =
      llvm::ConstantInt::get((llvm::IntegerType *)target->getType(), fanout);
  return {Builder->CreateURem(target, fanoutV), context->createTrue()};
}

::routing_target HashBased::evaluate(OlapParallelContext *const context,
                                     const OperatorState &childState,
                                     ProteusValueMemory retrycnt) {
  auto Builder = context->getBuilder();

  ExpressionGeneratorVisitor exprGenerator{context, childState};
  auto target = e.accept(exprGenerator).value;
  auto fanoutV =
      llvm::ConstantInt::get((llvm::IntegerType *)target->getType(), fanout);
  return {Builder->CreateURem(target, fanoutV), context->createFalse()};
}

::routing_target Local::evaluate(OlapParallelContext *const context,
                                 const OperatorState &childState,
                                 ProteusValueMemory retrycnt) {
  if (fanout == 1) return {context->createInt64(0), context->createFalse()};

  auto *Builder = context->getBuilder();
  auto charPtrType = llvm::Type::getInt8PtrTy(context->getLLVMContext());

  auto ptr = Builder->CreateLoad(
      childState[wantedField].mem->getType()->getPointerElementType(),
      childState[wantedField].mem);
  auto ptr8 = Builder->CreateBitCast(ptr, charPtrType);

  auto this_ptr = Builder->CreateIntToPtr(context->createInt64((uintptr_t)aff),
                                          charPtrType);

  auto target = context->gen_call(random_local_cu_index, {ptr8, this_ptr});

  return {target, context->createTrue()};
}

Local::Local(size_t fanout, const std::vector<RecordAttribute *> &wantedFields,
             const AffinityPolicy *aff)
    : fanout(fanout), wantedField(*wantedFields[0]), aff(aff) {}

RandomSplitForceDataLocal::RandomSplitForceDataLocal(
    const std::vector<RecordAttribute *> &wantedFields,
    std::vector<Affinitizer *> _affs,
    const std::vector<DeviceType> &target_device_types)
    : wantedField(*wantedFields[0]), consumer_affs(std::move(_affs)) {
  CHECK_EQ(consumer_affs.size(), target_device_types.size())
      << "consumer_affs and target_device_types must have the same size";
  const auto &topo = topology::getInstance();
  if (topo.getGpuCount() == 0) {
    for (const auto &d : target_device_types) {
      CHECK_NE(d, DeviceType::GPU) << "No GPU available";
    }
  }
  const auto count_cus = topo.getCpuNumaNodeCount() + topo.getGpuCount();
  for (int i = 0; i < consumer_affs.size(); i++) {
    consumer_offsets.push_back(count_cus * i +
                               (target_device_types[i] == DeviceType::GPU
                                    ? topo.getCpuNumaNodeCount()
                                    : 0));
  }
}

::routing_target RandomSplitForceDataLocal::evaluate(
    OlapParallelContext *const context, const OperatorState &childState,
    ProteusValueMemory retrycnt) {
  auto *Builder = context->getBuilder();

  // state is initialized with a random number in the entry block
  // then we keep rehashing this random number to get new "random" numbers
  // without having to do a runtime call to rand()
  auto rand_state = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    ExpressionGeneratorVisitor vis{context, childState};
    return context->toMem(expressions::rand().accept(vis));
  }();

  /// array holding the base offsets for the queues of each consuming target
  auto llvm_consumer_offsets = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    llvm::Value *arraySize =
        llvm::ConstantInt::get(elementType, consumer_offsets.size());
    llvm::AllocaInst *alloca = Builder->CreateAlloca(
        elementType, arraySize, "consumer_queue_offsets_arr");

    for (int i = 0; i < consumer_offsets.size(); i++) {
      llvm::Value *index = llvm::ConstantInt::get(elementType, i);
      llvm::Value *elementPtr = Builder->CreateGEP(elementType, alloca, index);
      Builder->CreateStore(
          llvm::ConstantInt::get(elementType, consumer_offsets[i]), elementPtr);
    }
    return alloca;
  }();

  /// array holding the aff ptrs (as Int64) for each consumer
  auto llvm_consumer_affs = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    llvm::Value *arraySize =
        llvm::ConstantInt::get(elementType, consumer_affs.size());
    llvm::AllocaInst *alloca =
        Builder->CreateAlloca(elementType, arraySize, "consumer_aff_ptr_arr");

    for (int i = 0; i < consumer_affs.size(); i++) {
      llvm::Value *index = llvm::ConstantInt::get(elementType, i);
      llvm::Value *elementPtr = Builder->CreateGEP(elementType, alloca, index);
      Builder->CreateStore(
          llvm::ConstantInt::get(elementType,
                                 reinterpret_cast<uintptr_t>(consumer_affs[i])),
          elementPtr);
    }
    return alloca;
  }();

  auto rand_int = Builder->CreateLoad(
      rand_state.mem->getType()->getPointerElementType(), rand_state.mem);
  ExpressionGeneratorVisitor vis{context, childState};
  Builder->CreateStore(
      Builder->CreateZExtOrTrunc(
          expressions::HashExpression{
              expressions::ProteusValueExpression{
                  new IntType(), ProteusValue{rand_int, rand_state.isNull}}}
              .accept(vis)
              .value,
          rand_int->getType()),
      rand_state.mem);

  // first we get a random consumer
  auto num_consumers = llvm::ConstantInt::get(
      (llvm::IntegerType *)rand_int->getType(), consumer_offsets.size());
  auto target_consumer = Builder->CreateURem(rand_int, num_consumers);

  // then we use locality to find the queue for the target consumer
  // using the affinity policy and then adding the start queue offset for the
  // target consumer
  auto charPtrType = llvm::Type::getInt8PtrTy(context->getLLVMContext());
  auto ptr = Builder->CreateLoad(
      childState[wantedField].mem->getType()->getPointerElementType(),
      childState[wantedField].mem);
  auto ptr8 = Builder->CreateBitCast(ptr, charPtrType);

  auto aff_int_ptr = Builder->CreateLoad(
      Builder->getInt64Ty(),
      Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_affs,
                         target_consumer),
      "consumer_aff_int_ptr");
  auto consumer_aff_ptr =
      Builder->CreateIntToPtr(aff_int_ptr, charPtrType, "consumer_aff_ptr");
  auto target_numa = context->gen_call(random_local_cu_index_in_topo,
                                       {ptr8, consumer_aff_ptr});
  auto queue_offset_for_consumer = Builder->CreateLoad(
      Builder->getInt64Ty(),
      Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_offsets,
                         target_consumer),
      "load_queue_offset_for_consumer");
  auto target_queue =
      Builder->CreateAdd(target_numa, queue_offset_for_consumer);
  return {target_queue, context->createTrue()};
}

RandomSplitPreferDataLocal::RandomSplitPreferDataLocal(
    const std::vector<RecordAttribute *> &wantedFields,
    std::vector<Affinitizer *> _affs,
    const std::vector<DeviceType> &target_device_types)
    : wantedField(*wantedFields[0]),
      consumer_affs(std::move(_affs)),
      device_types(target_device_types) {
  CHECK_EQ(consumer_affs.size(), target_device_types.size())
      << "consumer_affs and target_device_types must have the same size";
  const auto &topo = topology::getInstance();
  if (topo.getGpuCount() == 0) {
    for (const auto &d : target_device_types) {
      CHECK_NE(d, DeviceType::GPU) << "No GPU available";
    }
  }
  const auto count_cus = topo.getCpuNumaNodeCount() + topo.getGpuCount();
  for (int i = 0; i < consumer_affs.size(); i++) {
    consumer_offsets.push_back(count_cus * i +
                               (target_device_types[i] == DeviceType::GPU
                                    ? topo.getCpuNumaNodeCount()
                                    : 0));
  }
}

::routing_target RandomSplitPreferDataLocal::evaluate(
    OlapParallelContext *const context, const OperatorState &childState,
    ProteusValueMemory retrycnt) {
  auto *Builder = context->getBuilder();

  // state is initialized with a random number in the entry block
  // then we keep rehashing this random number to get new "random" numbers
  // without having to do a runtime call to rand()
  auto rand_state = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    ExpressionGeneratorVisitor vis{context, childState};
    return context->toMem(expressions::rand().accept(vis));
  }();

  /// array holding the base offsets for the queues of each consuming target
  auto llvm_consumer_offsets = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    llvm::Value *arraySize =
        llvm::ConstantInt::get(elementType, consumer_offsets.size());
    llvm::AllocaInst *alloca = Builder->CreateAlloca(
        elementType, arraySize, "consumer_queue_offsets_arr");

    for (int i = 0; i < consumer_offsets.size(); i++) {
      llvm::Value *index = llvm::ConstantInt::get(elementType, i);
      llvm::Value *elementPtr = Builder->CreateGEP(elementType, alloca, index);
      Builder->CreateStore(
          llvm::ConstantInt::get(elementType, consumer_offsets[i]), elementPtr);
    }
    return alloca;
  }();

  /// array holding the aff ptrs (as Int64) for each consumer
  auto llvm_consumer_affs = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    llvm::Value *arraySize =
        llvm::ConstantInt::get(elementType, consumer_affs.size());
    llvm::AllocaInst *alloca =
        Builder->CreateAlloca(elementType, arraySize, "consumer_aff_ptr_arr");

    for (int i = 0; i < consumer_affs.size(); i++) {
      llvm::Value *index = llvm::ConstantInt::get(elementType, i);
      llvm::Value *elementPtr = Builder->CreateGEP(elementType, alloca, index);
      Builder->CreateStore(
          llvm::ConstantInt::get(elementType,
                                 reinterpret_cast<uintptr_t>(consumer_affs[i])),
          elementPtr);
    }
    return alloca;
  }();

  /// array holding the count of queues per consumer
  auto llvm_consumer_queue_counts = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    llvm::Value *arraySize =
        llvm::ConstantInt::get(elementType, consumer_affs.size());
    llvm::AllocaInst *alloca = Builder->CreateAlloca(
        elementType, arraySize, "consumer_count_queues_arr");

    for (int i = 0; i < consumer_affs.size(); i++) {
      llvm::Value *index = llvm::ConstantInt::get(elementType, i);
      llvm::Value *elementPtr = Builder->CreateGEP(elementType, alloca, index);
      Builder->CreateStore(
          llvm::ConstantInt::get(
              elementType,
              reinterpret_cast<uintptr_t>(consumer_affs[i]->countAffCUs())),
          elementPtr);
    }
    return alloca;
  }();

  /// array holding which numa nodes/gpus are used by each consumer
  /// See the header file for how CPUs/GPUs are encoded into a single index
  auto llvm_consumer_numa_nodes = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    auto &topo = topology::getInstance();
    const auto system_count_cus =
        topo.getCpuNumaNodeCount() + topo.getGpuCount();

    // allocate for the maximum number so we can use constant offsets per
    // consumer later
    llvm::Value *arraySize = llvm::ConstantInt::get(
        elementType, system_count_cus * consumer_affs.size());
    llvm::AllocaInst *alloca = Builder->CreateAlloca(
        elementType, arraySize, "consumer_used_queues_arr");

    for (int i = 0; i < consumer_affs.size(); i++) {
      auto *aff = consumer_affs[i];
      auto cus_used = aff->getCUIndexDomain();
      int consumer_start_index = system_count_cus * i;
      for (int j = 0; j < cus_used.size(); j++) {
        const size_t cu_index = cus_used[j];
        const auto adjusted_cu_index =
            cu_index + (device_types[i] == DeviceType::GPU
                            ? topo.getCpuNumaNodeCount()
                            : 0);
        DCHECK_LT(adjusted_cu_index, system_count_cus);
        DCHECK_LT(consumer_start_index + j,
                  system_count_cus * consumer_affs.size());
        llvm::Value *alloca_idx =
            llvm::ConstantInt::get(elementType, consumer_start_index + j);
        auto elementPtr = Builder->CreateGEP(elementType, alloca, alloca_idx);
        Builder->CreateStore(
            llvm::ConstantInt::get(elementType, adjusted_cu_index), elementPtr);
      }
    }
    return alloca;
  }();

  auto rand_int = Builder->CreateLoad(
      rand_state.mem->getType()->getPointerElementType(), rand_state.mem);
  ExpressionGeneratorVisitor vis{context, childState};
  Builder->CreateStore(
      Builder->CreateZExtOrTrunc(
          expressions::HashExpression{
              expressions::ProteusValueExpression{
                  new IntType(), ProteusValue{rand_int, rand_state.isNull}}}
              .accept(vis)
              .value,
          rand_int->getType()),
      rand_state.mem);

  // first we get a random consumer
  auto num_consumers = llvm::ConstantInt::get(
      (llvm::IntegerType *)rand_int->getType(), consumer_offsets.size());
  auto target_consumer = Builder->CreateURem(rand_int, num_consumers);

  llvm::BasicBlock *b_if;
  llvm::BasicBlock *b_else;
  llvm::Value *numa_if;
  llvm::Value *numa_else;
  auto phi_type = llvm::IntegerType::getInt64Ty(context->getLLVMContext());

  // first time we prefer a data local queue of the consumer
  gen_if(lt(
             expressions::ProteusValueExpression{
                 new IntType(),
                 {Builder->CreateLoad(
                      retrycnt.mem->getType()->getPointerElementType(),
                      retrycnt.mem),
                  retrycnt.isNull}},
             1),
         childState, context)([&]() {
    // we use locality to find the queue for the target consumer
    // using the affinity policy. The queue offset is added after the phi
    auto charPtrType = llvm::Type::getInt8PtrTy(context->getLLVMContext());
    auto ptr = Builder->CreateLoad(
        childState[wantedField].mem->getType()->getPointerElementType(),
        childState[wantedField].mem);
    auto ptr8 = Builder->CreateBitCast(ptr, charPtrType);

    auto aff_int_ptr = Builder->CreateLoad(
        Builder->getInt64Ty(),
        Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_affs,
                           target_consumer),
        "consumer_aff_int_ptr");
    auto consumer_aff_ptr =
        Builder->CreateIntToPtr(aff_int_ptr, charPtrType, "consumer_aff_ptr");
    numa_if = context->gen_call(random_local_cu_index_in_topo,
                                {ptr8, consumer_aff_ptr});
    b_if = Builder->GetInsertBlock();
  }).gen_else([&]() {
    // second time we pick a random queue for the consumer
    auto rand_int2 = Builder->CreateLoad(
        rand_state.mem->getType()->getPointerElementType(), rand_state.mem);
    ExpressionGeneratorVisitor vis2{context, childState};
    Builder->CreateStore(
        Builder->CreateZExtOrTrunc(
            expressions::HashExpression{
                expressions::ProteusValueExpression{
                    new IntType(), ProteusValue{rand_int, rand_state.isNull}}}
                .accept(vis)
                .value,
            rand_int->getType()),
        rand_state.mem);
    auto consumer_count_queues = Builder->CreateLoad(
        Builder->getInt64Ty(),
        Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_queue_counts,
                           target_consumer),
        "load_consumer_count_queues");
    auto &topo = topology::getInstance();
    const auto system_count_cus =
        topo.getCpuNumaNodeCount() + topo.getGpuCount();
    // construct index into llvm_consumer_numa_nodes
    auto cons_numa_idx = Builder->CreateAdd(
        Builder->CreateMul(
            Builder->CreateZExtOrTrunc(target_consumer, Builder->getInt64Ty()),
            llvm::ConstantInt::get(Builder->getInt64Ty(), system_count_cus)),
        Builder->CreateURem(
            Builder->CreateZExtOrTrunc(rand_int2, Builder->getInt64Ty()),
            consumer_count_queues));

    numa_else = Builder->CreateLoad(
        Builder->getInt64Ty(),
        Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_numa_nodes,
                           cons_numa_idx),
        "load_consumer_numa_node");
    b_else = Builder->GetInsertBlock();
  });

  // A value in [0, aff->countAffCUs())
  auto target_numa_phi = Builder->CreatePHI(phi_type, 2);
  target_numa_phi->addIncoming(numa_if, b_if);
  target_numa_phi->addIncoming(numa_else, b_else);

  auto queue_offset_for_consumer = Builder->CreateLoad(
      Builder->getInt64Ty(),
      Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_offsets,
                         target_consumer),
      "load_queue_offset_for_consumer");
  auto target_queue =
      Builder->CreateAdd(target_numa_phi, queue_offset_for_consumer);
  return {target_queue, context->createTrue()};
}

ThroughputSplitPreferDataLocal::ThroughputSplitPreferDataLocal(
    const std::vector<RecordAttribute *> &wantedFields,
    std::vector<Affinitizer *> _affs,
    const std::vector<DeviceType> &target_device_types)
    : wantedField(*wantedFields[0]),
      consumer_affs(std::move(_affs)),
      device_types(target_device_types) {
  CHECK_EQ(consumer_affs.size(), target_device_types.size())
      << "consumer_affs and target_device_types must have the same size";
  const auto &topo = topology::getInstance();
  if (topo.getGpuCount() == 0) {
    for (const auto &d : target_device_types) {
      CHECK_NE(d, DeviceType::GPU) << "No GPU available";
    }
  }
  const auto count_cus = topo.getCpuNumaNodeCount() + topo.getGpuCount();
  for (int i = 0; i < consumer_affs.size(); i++) {
    consumer_offsets.push_back(count_cus * i +
                               (target_device_types[i] == DeviceType::GPU
                                    ? topo.getCpuNumaNodeCount()
                                    : 0));
  }
}

extern "C" {
void record_event(void *state, void *event) {
  static_cast<ThroughputSplitPreferDataLocal::RoutingState *>(state)
      ->tracker.notify_event(event, rdtsc());
}

void *createRoutingState(uint64_t num_consumers) {
  return new ThroughputSplitPreferDataLocal::RoutingState(num_consumers);
}
void destroyRoutingState(void *state) {
  delete static_cast<ThroughputSplitPreferDataLocal::RoutingState *>(state);
}
uint64_t get_target(void *state) {
  return static_cast<ThroughputSplitPreferDataLocal::RoutingState *>(state)
      ->get_target();
}
}

void ThroughputSplitPreferDataLocal::generateStateInit(
    OlapParallelContext *context) {
  auto charPtrType = llvm::Type::getInt8PtrTy(context->getLLVMContext());
  routing_state_var = context->appendStateVar(
      charPtrType,
      [=](llvm::Value *pip) -> llvm::Value * {
        return context->gen_call(createRoutingState,
                                 {context->createInt64(consumer_affs.size())});
      },
      [=](llvm::Value *, llvm::Value *s) {
        return context->gen_call(destroyRoutingState, {s});
      });
}
::routing_target ThroughputSplitPreferDataLocal::evaluate(
    OlapParallelContext *const context, const OperatorState &childState,
    ProteusValueMemory retrycnt) {
  auto *Builder = context->getBuilder();
  auto charPtrType = llvm::Type::getInt8PtrTy(context->getLLVMContext());

  // state is initialized with a random number in the entry block
  // then we keep rehashing this random number to get new "random" numbers
  // without having to do a runtime call to rand()
  auto rand_state = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    ExpressionGeneratorVisitor vis{context, childState};
    return context->toMem(expressions::rand().accept(vis));
  }();

  auto count_state = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    ExpressionGeneratorVisitor vis{context, childState};
    auto init_count = expression_t(0l);
    return context->toMem(init_count.accept(vis));
  }();

  /// array holding the base offsets for the queues of each consuming target
  auto llvm_consumer_offsets = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    llvm::Value *arraySize =
        llvm::ConstantInt::get(elementType, consumer_offsets.size());
    llvm::AllocaInst *alloca = Builder->CreateAlloca(
        elementType, arraySize, "consumer_queue_offsets_arr");

    for (int i = 0; i < consumer_offsets.size(); i++) {
      llvm::Value *index = llvm::ConstantInt::get(elementType, i);
      llvm::Value *elementPtr = Builder->CreateGEP(elementType, alloca, index);
      Builder->CreateStore(
          llvm::ConstantInt::get(elementType, consumer_offsets[i]), elementPtr);
    }
    return alloca;
  }();

  /// array holding the aff ptrs (as Int64) for each consumer
  auto llvm_consumer_affs = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    llvm::Value *arraySize =
        llvm::ConstantInt::get(elementType, consumer_affs.size());
    llvm::AllocaInst *alloca =
        Builder->CreateAlloca(elementType, arraySize, "consumer_aff_ptr_arr");

    for (int i = 0; i < consumer_affs.size(); i++) {
      llvm::Value *index = llvm::ConstantInt::get(elementType, i);
      llvm::Value *elementPtr = Builder->CreateGEP(elementType, alloca, index);
      Builder->CreateStore(
          llvm::ConstantInt::get(elementType,
                                 reinterpret_cast<uintptr_t>(consumer_affs[i])),
          elementPtr);
    }
    return alloca;
  }();

  /// array holding the count of queues per consumer
  auto llvm_consumer_queue_counts = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    llvm::Value *arraySize =
        llvm::ConstantInt::get(elementType, consumer_affs.size());
    llvm::AllocaInst *alloca = Builder->CreateAlloca(
        elementType, arraySize, "consumer_count_queues_arr");

    for (int i = 0; i < consumer_affs.size(); i++) {
      llvm::Value *index = llvm::ConstantInt::get(elementType, i);
      llvm::Value *elementPtr = Builder->CreateGEP(elementType, alloca, index);
      Builder->CreateStore(
          llvm::ConstantInt::get(
              elementType,
              reinterpret_cast<uintptr_t>(consumer_affs[i]->countAffCUs())),
          elementPtr);
    }
    return alloca;
  }();

  /// array holding which numa nodes/gpus are used by each consumer
  /// See the header file for how CPUs/GPUs are encoded into a single index
  auto llvm_consumer_numa_nodes = [&] {
    save_current_blocks_and_restore_at_exit_scope e{context};
    Builder->SetInsertPoint(context->getCurrentEntryBlock());
    llvm::Type *elementType = Builder->getInt64Ty();
    auto &topo = topology::getInstance();
    const auto system_count_cus =
        topo.getCpuNumaNodeCount() + topo.getGpuCount();

    // allocate for the maximum number so we can use constant offsets per
    // consumer later
    llvm::Value *arraySize = llvm::ConstantInt::get(
        elementType, system_count_cus * consumer_affs.size());
    llvm::AllocaInst *alloca = Builder->CreateAlloca(
        elementType, arraySize, "consumer_used_queues_arr");

    for (int i = 0; i < consumer_affs.size(); i++) {
      auto *aff = consumer_affs[i];
      auto cus_used = aff->getCUIndexDomain();
      int consumer_start_index = system_count_cus * i;
      for (int j = 0; j < cus_used.size(); j++) {
        const size_t cu_index = cus_used[j];
        const auto adjusted_cu_index =
            cu_index + (device_types[i] == DeviceType::GPU
                            ? topo.getCpuNumaNodeCount()
                            : 0);
        DCHECK_LT(adjusted_cu_index, system_count_cus);
        DCHECK_LT(consumer_start_index + j,
                  system_count_cus * consumer_affs.size());
        llvm::Value *alloca_idx =
            llvm::ConstantInt::get(elementType, consumer_start_index + j);
        auto elementPtr = Builder->CreateGEP(elementType, alloca, alloca_idx);
        Builder->CreateStore(
            llvm::ConstantInt::get(elementType, adjusted_cu_index), elementPtr);
      }
    }
    return alloca;
  }();

  //
  //  auto rand_int = Builder->CreateLoad(
  //      rand_state.mem->getType()->getPointerElementType(), rand_state.mem);
  //  ExpressionGeneratorVisitor vis{context, childState};
  //  Builder->CreateStore(
  //      Builder->CreateZExtOrTrunc(
  //          expressions::HashExpression{
  //              expressions::ProteusValueExpression{
  //                  new IntType(), ProteusValue{rand_int, rand_state.isNull}}}
  //              .accept(vis)
  //              .value,
  //          rand_int->getType()),
  //      rand_state.mem);
  const auto state_ptr = context->getStateVar(routing_state_var);

  auto target_consumer = context->gen_call(get_target, {state_ptr});

  auto rowgroup_ptr = Builder->CreateLoad(
      childState[wantedField].mem->getType()->getPointerElementType(),
      childState[wantedField].mem);
  auto row_group_ptr8 = Builder->CreateBitCast(rowgroup_ptr, charPtrType);
  context->gen_call(record_event, {state_ptr, row_group_ptr8});

  llvm::BasicBlock *b_if;
  llvm::BasicBlock *b_else;
  llvm::Value *numa_if;
  llvm::Value *numa_else;
  auto phi_type = llvm::IntegerType::getInt64Ty(context->getLLVMContext());

  // first time we prefer a data local queue of the consumer
  gen_if(lt(
             expressions::ProteusValueExpression{
                 new IntType(),
                 {Builder->CreateLoad(
                      retrycnt.mem->getType()->getPointerElementType(),
                      retrycnt.mem),
                  retrycnt.isNull}},
             1),
         childState, context)([&]() {
    // we use locality to find the queue for the target consumer
    // using the affinity policy. The queue offset is added after the phi

    auto aff_int_ptr = Builder->CreateLoad(
        Builder->getInt64Ty(),
        Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_affs,
                           target_consumer),
        "consumer_aff_int_ptr");
    auto consumer_aff_ptr =
        Builder->CreateIntToPtr(aff_int_ptr, charPtrType, "consumer_aff_ptr");
    numa_if = context->gen_call(random_local_cu_index_in_topo,
                                {row_group_ptr8, consumer_aff_ptr});
    b_if = Builder->GetInsertBlock();
  }).gen_else([&]() {
    // second time we pick a random queue for the consumer
    auto rand_int = Builder->CreateLoad(
        rand_state.mem->getType()->getPointerElementType(), rand_state.mem);
    ExpressionGeneratorVisitor vis{context, childState};
    Builder->CreateStore(
        Builder->CreateZExtOrTrunc(
            expressions::HashExpression{
                expressions::ProteusValueExpression{
                    new IntType(), ProteusValue{rand_int, rand_state.isNull}}}
                .accept(vis)
                .value,
            rand_int->getType()),
        rand_state.mem);
    auto consumer_count_queues = Builder->CreateLoad(
        Builder->getInt64Ty(),
        Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_queue_counts,
                           target_consumer),
        "load_consumer_count_queues");
    auto &topo = topology::getInstance();
    const auto system_count_cus =
        topo.getCpuNumaNodeCount() + topo.getGpuCount();
    // construct index into llvm_consumer_numa_nodes
    auto cons_numa_idx = Builder->CreateAdd(
        Builder->CreateMul(
            Builder->CreateZExtOrTrunc(target_consumer, Builder->getInt64Ty()),
            llvm::ConstantInt::get(Builder->getInt64Ty(), system_count_cus)),
        Builder->CreateURem(
            Builder->CreateZExtOrTrunc(rand_int, Builder->getInt64Ty()),
            consumer_count_queues));

    numa_else = Builder->CreateLoad(
        Builder->getInt64Ty(),
        Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_numa_nodes,
                           cons_numa_idx),
        "load_consumer_numa_node");
    b_else = Builder->GetInsertBlock();
  });

  // A value in [0, aff->countAffCUs())
  auto target_numa_phi = Builder->CreatePHI(phi_type, 2);
  target_numa_phi->addIncoming(numa_if, b_if);
  target_numa_phi->addIncoming(numa_else, b_else);

  auto queue_offset_for_consumer = Builder->CreateLoad(
      Builder->getInt64Ty(),
      Builder->CreateGEP(Builder->getInt64Ty(), llvm_consumer_offsets,
                         target_consumer),
      "load_queue_offset_for_consumer");
  auto target_queue =
      Builder->CreateAdd(target_numa_phi, queue_offset_for_consumer);
  return {target_queue, context->createTrue()};
}

LocalServer::LocalServer(size_t fanout)
    : HashBased(fanout, (int)InfiniBandManager::server_id()) {}

PreferLocal::PreferLocal(size_t fanout,
                         const std::vector<RecordAttribute *> &wantedFields,
                         const AffinityPolicy *aff)
    : priority(fanout, wantedFields, aff), alternative(fanout) {}

routing_target PreferLocal::evaluate(OlapParallelContext *context,
                                     const OperatorState &childState,
                                     ProteusValueMemory retrycnt) {
  auto *Builder = context->getBuilder();

  llvm::BasicBlock *target_b1;
  llvm::BasicBlock *target_b2;
  llvm::Value *target_p1;
  llvm::Value *target_p2;
  auto target_phi_type =
      llvm::IntegerType::getInt64Ty(context->getLLVMContext());

  gen_if(lt(
             expressions::ProteusValueExpression{
                 new IntType(),
                 {Builder->CreateLoad(
                      retrycnt.mem->getType()->getPointerElementType(),
                      retrycnt.mem),
                  retrycnt.isNull}},
             1),
         childState, context)([&]() {
    target_p1 = Builder->CreateZExt(
        priority.evaluate(context, childState, retrycnt).target,
        target_phi_type);
    target_b1 = Builder->GetInsertBlock();
  }).gen_else([&]() {
    target_p2 = Builder->CreateZExt(
        alternative.evaluate(context, childState, retrycnt).target,
        target_phi_type);
    target_b2 = Builder->GetInsertBlock();
  });

  auto target_phi = Builder->CreatePHI(target_phi_type, 2);
  target_phi->addIncoming(target_p1, target_b1);
  target_phi->addIncoming(target_p2, target_b2);

  llvm::BasicBlock *retry_b1;
  llvm::BasicBlock *retry_b2;
  llvm::Value *retry_p1;
  llvm::Value *retry_p2;

  auto retry_phi_type = context->createBoolType();

  // retrycnt < 3 then force
  gen_if(lt(
             expressions::ProteusValueExpression{
                 new IntType(),
                 {Builder->CreateLoad(
                      retrycnt.mem->getType()->getPointerElementType(),
                      retrycnt.mem),
                  retrycnt.isNull}},
             3),
         childState, context)([&]() {
    retry_p1 = context->createTrue();
    retry_b1 = Builder->GetInsertBlock();
  }).gen_else([&]() {
    retry_p2 = context->createFalse();
    retry_b2 = Builder->GetInsertBlock();
  });

  auto retry_phi = Builder->CreatePHI(retry_phi_type, 2);
  retry_phi->addIncoming(retry_p1, retry_b1);
  retry_phi->addIncoming(retry_p2, retry_b2);

  return {target_phi, retry_phi};
}

PreferLocalServer::PreferLocalServer(size_t fanout)
    : priority(fanout), alternative(fanout) {}

routing_target PreferLocalServer::evaluate(OlapParallelContext *context,
                                           const OperatorState &childState,
                                           ProteusValueMemory retrycnt) {
  auto *Builder = context->getBuilder();

  llvm::BasicBlock *b1;
  llvm::BasicBlock *b2;
  llvm::Value *p1;
  llvm::Value *p2;
  auto phi_type = llvm::IntegerType::getInt64Ty(context->getLLVMContext());

  gen_if(lt(
             expressions::ProteusValueExpression{
                 new IntType(),
                 {Builder->CreateLoad(
                      retrycnt.mem->getType()->getPointerElementType(),
                      retrycnt.mem),
                  retrycnt.isNull}},
             1),
         childState, context)([&]() {
    p1 = Builder->CreateZExt(
        priority.evaluate(context, childState, retrycnt).target, phi_type);
    b1 = Builder->GetInsertBlock();
  }).gen_else([&]() {
    p2 = Builder->CreateZExt(
        alternative.evaluate(context, childState, retrycnt).target, phi_type);
    b2 = Builder->GetInsertBlock();
  });

  auto phi = Builder->CreatePHI(phi_type, 2);
  phi->addIncoming(p1, b1);
  phi->addIncoming(p2, b2);

  return {phi, context->createTrue()};
}

}  // namespace routing
