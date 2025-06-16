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

#include "generalized-router.hpp"

#include <chrono>
#include <codegen/jit/pipeline.hpp>
#include <lib/expressions/expressions-generator.hpp>
#include <lib/util/catalog.hpp>
#include <magic_enum.hpp>
#include <platform/network/infiniband/infiniband-manager.hpp>
#include <platform/util/timing.hpp>

#include "lib/operators/router/routing-policy.hpp"
#include "router.hpp"
#include "routing-policy-factory.hpp"
#include "routing-policy-v2.hpp"

namespace proteus {

int64_t getGroupId(Pipeline *pip) { return pip->getGroup(); }


[[nodiscard]] void *acquireBufferGeneralized(int free_pool_idx,
                                             GeneralizedRouter *xch,
                                             int64_t groupId) {
  //  event_range<range_log_op::GROUTER_ACQUIRE_BUFF> er{{}, {},
  //                                             groupId};
  return xch->acquireBufferGeneralized(free_pool_idx, false, groupId).release();
}

[[nodiscard]] void *try_acquireBufferGeneralized(int free_pool_idx,
                                                 GeneralizedRouter *xch,

                                                 int64_t groupId) {
  // can generate a lot of trace events
  //  event_range<range_log_op::GROUTER_ACQUIRE_BUFF> er{{}, {},
  //                                             groupId};
  //  LOG(INFO) << "Trying to acquire buffer for free_pool_idx " <<
  //  free_pool_idx;
  return xch->acquireBufferGeneralized(free_pool_idx, true, groupId).release();
}

void releaseBufferGeneralized(int target, GeneralizedRouter *xch, void *buff) {
  xch->releaseBufferGeneralized(target, proteus::managed_ptr{buff});
}

/**
 * FFI shim function for V2 routing policies.
 * Called from JIT code to route tuples using C++ policy decisions.
 */
extern "C" void route_and_enqueue_via_cpp(GeneralizedRouter *router,
                                          const void *jit_routing_keys_payload,
                                          size_t jit_routing_keys_size,
                                          const void *jit_full_payload,
                                          size_t full_payload_size,
                                          int64_t group_id) {
  // Get the V2 policy instance
  auto *policy = router->get_policy();
  DCHECK(policy) << "route_and_enqueue_via_cpp: No V2 policy found";

  // Create routing context
  routing::RoutingContext context{
      .keys_payload = jit_routing_keys_payload,
      .keys_payload_size = jit_routing_keys_size,
      .current_timestamp = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count()),
      .tuple_count = 0,   // Policy will manage its own tuple counting
      .queue_depths = {}  // TODO: Populate queue depths
  };

  // Get total consumer count
  int num_consumers = router->getFanoutDOP_for_policy();
  DCHECK_GT(num_consumers, 0)
      << "route_and_enqueue_via_cpp: Invalid consumer count: " << num_consumers;

  // Retry loop implementation
  std::vector<int> failed_channels;
  int retry_count = 0;
  const int max_retries =
      3;  // Maximum retry attempts before falling back to blocking

  while (retry_count <= max_retries) {
    // Make routing decision with current retry count
    auto decision = policy->getTargetChannel(router, context, num_consumers,
                                             retry_count, failed_channels);

    // Validate channel - use total number of queues for validation
    int total_queues = static_cast<int>(router->getNumberOfQueues());
    DCHECK_GE(decision.target_channel, 0)
        << "route_and_enqueue_via_cpp: Invalid target channel "
        << decision.target_channel;
    DCHECK_LT(decision.target_channel, total_queues)
        << "route_and_enqueue_via_cpp: Target channel "
        << decision.target_channel << " >= total_queues " << total_queues;

    // Try to acquire buffer
    void *buffer = nullptr;

    if (decision.supports_retry && retry_count < max_retries) {
      // Use non-blocking try_acquire for policies that support retry
      buffer = try_acquireBufferGeneralized(decision.free_pool_index, router,
                                            group_id);
    } else {
      // Use blocking acquire for policies without retry support or on final
      // attempt
      buffer =
          acquireBufferGeneralized(decision.free_pool_index, router, group_id);
    }

    if (buffer) {
      // Success - copy data to buffer
      size_t buf_size = router->get_cpp_buf_size();
      DCHECK_LE(full_payload_size, buf_size)
          << "route_and_enqueue_via_cpp: Payload size " << full_payload_size
          << " exceeds buffer size " << buf_size;

      memcpy(buffer, jit_full_payload, full_payload_size);

      // Release buffer to target channel
      releaseBufferGeneralized(decision.target_channel, router, buffer);

      // Notify policy of successful routing
      policy->onTupleRouted(decision.target_channel, true);
      return;
    }

    // Buffer acquisition failed
    if (!decision.supports_retry) {
      // Policy doesn't support retry - this should not happen as we used
      // blocking acquire
      LOG(FATAL) << "route_and_enqueue_via_cpp: Buffer acquisition failed for "
                    "non-retry policy";
    }

    // Track failed channel and notify policy
    failed_channels.push_back(decision.target_channel);
    policy->onChannelBackPressure(decision.target_channel);

    retry_count++;

    // // Small delay to avoid busy spinning
    // if (retry_count < max_retries) {
    //   std::this_thread::sleep_for(std::chrono::microseconds(100));
    // }
  }

  // Should not reach here - final blocking acquire should have succeeded
  LOG(FATAL) << "route_and_enqueue_via_cpp: Failed to acquire buffer after all "
                "retries";

  // static std::atomic<uint64_t> call_counter{0};
  // uint64_t current_call = call_counter.fetch_add(1,
  // std::memory_order_relaxed); if (current_call % 100 == 0) {
  //   LOG(INFO) << "route_and_enqueue_via_cpp: Successfully routed to channel "
  //             << decision.target_channel;
  // }
}

GeneralizedRouterConsumer::GeneralizedRouterConsumer(
    std::shared_ptr<GeneralizedRouter> producer, DegreeOfParallelism fanout,
    std::unique_ptr<Affinitizer> aff, DeviceType target_device,
    int consumer_index)
    : experimental::UnaryOperator(std::static_pointer_cast<Operator>(producer)),
      producer(producer.get()),
      fanout(fanout),
      aff(std::move(aff)),
      aff_policy(std::make_unique<AffinityPolicy>(this->aff->countAffCUs(),
                                                  this->aff.get())),
      target_device(target_device),
      consumer_index(consumer_index),
      consumed_count(0) {}

GeneralizedRouter::~GeneralizedRouter() = default;

void GeneralizedRouterConsumer::produce_(OlapParallelContext *context) {
  consume(context, {*this, {}});

  catch_pip = context->operator->();

  producer->produceForConsumer(*this, context);
}

void GeneralizedRouterConsumer::consume(OlapParallelContext *context,
                                        const OperatorState &childState) {
  auto &llvmContext = context->getLLVMContext();

  std::shared_ptr<Plugin> pg = Catalog::getInstance().getPlugin(
      producer->wantedFields[0]->getRelationName());

  auto nvme_plugin = dynamic_cast<NvmePlugin *>(pg.get());
  const bool is_nvme_plugin = nvme_plugin != nullptr;
  const bool non_scan_move = producer->wantedFields[0]->getRelationName().find(
                                 "tmp") != std::string::npos;

  const ExpressionType *ptoid = pg->getOIDType();

  llvm::Type *oidType = ptoid->getLLVMType(llvmContext);

  std::vector<llvm::Type *> param_typelist;
  for (auto field : producer->wantedFields) {
    auto *wtype = field->getLLVMType(llvmContext);
    if (wtype == nullptr)
      wtype = oidType;  // FIXME: dirty hack for JSON inner lists

    param_typelist.push_back(wtype);
    producer->need_cnt =
        producer->need_cnt || (field->getOriginalType()->getTypeID() == BLOCK);
  }

  param_typelist.push_back(oidType);                              // oid
  param_typelist.push_back(llvm::Type::getInt64Ty(llvmContext));  // srcServer
  if (producer->need_cnt) param_typelist.push_back(oidType);      // cnt

  // This currently assumes that if we are routing blocks (need_cnt) and using
  // the nvme plugin, then we need to add tupleCnt as a param. This may not
  // always be true, e.g. pack intermediate results on GPU and route the blocks
  // to the CPU. TBD if this breaks things
  if (is_nvme_plugin)
    param_typelist.push_back(oidType);  // the real tupleCnt. For NvmePlugin cnt
                                        // is the number of blocks

  producer->params_type = llvm::StructType::get(llvmContext, param_typelist);
  producer->buf_size = context->getSizeOf(producer->params_type);

  // blockCnt for NvmePlugin
  RecordAttribute tupleCnt(producer->wantedFields[0]->getRelationName(),
                           "activeCnt",
                           pg->getOIDType());  // FIXME: OID type for blocks ?
  RecordAttribute tupleIdentifier(producer->wantedFields[0]->getRelationName(),
                                  activeLoop, pg->getOIDType());
  RecordAttribute realTupleCnt(producer->wantedFields[0]->getRelationName(),
                               "tupleCnt", pg->getOIDType());
  RecordAttribute srcServer{producer->wantedFields[0]->getRelationName(),
                            "srcServer",
                            new Int64Type()};  // FIXME: OID type for blocks ?

  // Generate catch code
  auto p = context->appendParameter(
      llvm::PointerType::get(producer->params_type, 0), true, true);
  context->setGlobalFunction();

  auto *Builder = context->getBuilder();
  auto *entryBB = Builder->GetInsertBlock();
  auto *F = entryBB->getParent();

  context->setCurrentEntryBlock(entryBB);

  auto *mainBB = llvm::BasicBlock::Create(llvmContext, "main", F);

  auto *endBB = llvm::BasicBlock::Create(llvmContext, "end", F);
  context->setEndingBlock(endBB);

  Builder->SetInsertPoint(entryBB);

  auto *arg = context->getArgument(p);
  auto *params =
      Builder->CreateLoad(arg->getType()->getNonOpaquePointerElementType(),
                          context->getArgument(p));

  map<RecordAttribute, ProteusValueMemory> variableBindings;

  for (size_t i = 0; i < producer->wantedFields.size(); ++i) {
    auto *param = Builder->CreateExtractValue(params, i);

    // FIMXE: should we alse transfer this information ?
    variableBindings[*(producer->wantedFields[i])] =
        context->toMem(param, context->createFalse());
  }
  auto *oid =
      Builder->CreateExtractValue(params, producer->wantedFields.size());
  variableBindings[tupleIdentifier] =
      context->toMem(oid, context->createFalse());

  auto *srv =
      Builder->CreateExtractValue(params, producer->wantedFields.size() + 1);
  variableBindings[srcServer] =
      context->toMem(srv, context->createFalse(), "srcServer");

  if (producer->need_cnt) {
    auto *cnt =
        Builder->CreateExtractValue(params, producer->wantedFields.size() + 2);
    variableBindings[tupleCnt] = context->toMem(cnt, context->createFalse());

    if (is_nvme_plugin && !non_scan_move) {
      llvm::Value *tuple_cnt = Builder->CreateExtractValue(
          params, producer->wantedFields.size() + 3);
      variableBindings[realTupleCnt] =
          context->toMem(tuple_cnt, context->createFalse());
    }
  }

  Builder->SetInsertPoint(mainBB);

  OperatorState state{*this, variableBindings};
  getParent()->consume(context, state);

  Builder->CreateBr(endBB);

  Builder->SetInsertPoint(context->getCurrentEntryBlock());
  // Insert an explicit fall through from the current (entry) block to the
  // CondBB.
  Builder->CreateBr(mainBB);

  Builder->SetInsertPoint(context->getEndingBlock());
}


void GeneralizedRouter::produceForConsumer(
    const GeneralizedRouterConsumer &cons, OlapParallelContext *context) {
  assert(cons.producer == this);
  if (&cons == consumers.back().lock().get()) {
    produce(context);
  }
}

void GeneralizedRouter::produce_(OlapParallelContext *context) {
  context->popPipeline();

  // push new pipeline for the throw part
  context->pushPipeline(nullptr, "grouter_");

  context->registerOpen(this, [this](Pipeline *pip) { this->open(pip); });
  context->registerClose(this, [this](Pipeline *pip) { this->close(pip); });
  /*
   * At this point we are compiling, so no more GeneralizedRouterConsumers
   * should be added. This means we know how many pipelines we are splitting
   * into. So we construct the routing policy here to enable policies that wish
   * to use that the number of split pipelines.
   */
  // Initialize V2 Policy System using variant-based configuration
  auto config = createPolicyConfig();
  routing_policy_v2_ =
      routing::RoutingPolicyFactory::getInstance().createPolicy(policy_type_v2_,
                                                                config);

  DCHECK(routing_policy_v2_)
      << "Failed to create V2 policy: " << static_cast<int>(policy_type_v2_);

  // LOG(INFO) << "GeneralizedRouter: Initialized V2 "
  //           << magic_enum::enum_name(policy_type_v2_)
  //           << " policy with " << consumers.size() << " consumers";

  groupVar = context->appendStateVar(
      llvm::IntegerType::getIntNTy(context->getLLVMContext(),
                                   sizeof(int64_t) * 8),
      [=](llvm::Value *pip) { return context->gen_call(getGroupId, {pip}); },
      [=](llvm::Value *, llvm::Value *s) {});

  getChild()->produce(context);
}

llvm::Value *GeneralizedRouter::createTaskDescription(
    OlapParallelContext *context, const OperatorState &childState) {
  auto *Builder = context->getBuilder();

  llvm::Value *params = llvm::UndefValue::get(params_type);

  std::shared_ptr<Plugin> pg =
      Catalog::getInstance().getPlugin(wantedFields[0]->getRelationName());

  auto nvme_plugin = dynamic_cast<NvmePlugin *>(pg.get());
  const bool is_nvme_plugin = nvme_plugin != nullptr;

  auto rec = childState.getProducer().getRowType();
  ExpressionGeneratorVisitor vis{context, childState};
  for (size_t i = 0; i < wantedFields.size(); ++i) {
    auto v =
        expressions::InputArgument{&rec}[*wantedFields[i]].accept(vis).value;

    auto vi =
        (v->getType()->isPointerTy() &&
         v->getType()->getPointerElementType()->isArrayTy() &&
         v->getType()->getPointerElementType()->getArrayElementType() ==
             params_type->getStructElementType(i)->getPointerElementType())
            ? Builder->CreateInBoundsGEP(
                  v->getType()->getNonOpaquePointerElementType(), v,
                  {context->createInt32(0), context->createInt32(0)})
            : v;  // Is this still relevant?

    params = Builder->CreateInsertValue(params, vi, i);
  }

  RecordAttribute tupleIdentifier(wantedFields[0]->getRelationName(),
                                  activeLoop, pg->getOIDType());

  ProteusValueMemory mem_oidWrapper = childState[tupleIdentifier];
  params = Builder->CreateInsertValue(
      params,
      Builder->CreateLoad(
          mem_oidWrapper.mem->getType()->getPointerElementType(),
          mem_oidWrapper.mem),
      wantedFields.size());

  auto srcServer = [&]() -> llvm::Value * {
    try {
      return Builder->CreateLoad(childState[{wantedFields[0]->getRelationName(),
                                             "srcServer", new Int64Type()}]
                                     .mem->getType()
                                     ->getPointerElementType(),
                                 childState[{wantedFields[0]->getRelationName(),
                                             "srcServer", new Int64Type()}]
                                     .mem);
    } catch (const std::out_of_range &) {
      return context->createInt64(InfiniBandManager::server_id());
    }
  }();

  params =
      Builder->CreateInsertValue(params, srcServer, wantedFields.size() + 1);

  if (need_cnt) {
    RecordAttribute tupleCnt(wantedFields[0]->getRelationName(), "activeCnt",
                             pg->getOIDType());  // FIXME: OID type for blocks ?

    ProteusValueMemory mem_cntWrapper = childState[tupleCnt];
    params = Builder->CreateInsertValue(
        params,
        Builder->CreateLoad(
            mem_cntWrapper.mem->getType()->getPointerElementType(),
            mem_cntWrapper.mem),
        wantedFields.size() + 2);

    if (is_nvme_plugin) {
      RecordAttribute realTupleCnt(wantedFields[0]->getRelationName(),
                                   "tupleCnt", pg->getOIDType());
      bool have_tuple_count = true;
      try {
        childState[{realTupleCnt}];
      } catch (const attribute_not_found_in_state &) {
        have_tuple_count = false;
      }
      if (have_tuple_count) {
        ProteusValueMemory mem_realTupleCntWrapper = childState[realTupleCnt];
        params = Builder->CreateInsertValue(
            params,
            Builder->CreateLoad(
                mem_realTupleCntWrapper.mem->getType()->getPointerElementType(),
                mem_realTupleCntWrapper.mem),
            wantedFields.size() + 3);
      }
    }
  }

  return params;
}

void GeneralizedRouter::consume(OlapParallelContext *context,
                                const OperatorState &childState) {
  // Warmup threads to avoid thread creation overhead
  for (const auto &cons : consumers) {
    auto c_ptr = cons.lock();
    for (int i = 0; i < c_ptr->fanout; i++) {
      firers.emplace_back([]() {});
    }
  }
  for (auto &t : firers) t.get();
  firers.clear();

  auto groupId = context->getStateVar(groupVar);
  auto params = createTaskDescription(context, childState);

  // V2 Policy System: Delegate LLVM code generation to the policy
  CHECK(routing_policy_v2_) << "No routing policy configured";
  routing_policy_v2_->generateConsumeLogic(context, childState, params, groupId,
                                           this);
}

size_t GeneralizedRouter::getNumberOfQueues() const {
  CHECK(routing_policy_v2_) << "No routing policy configured";
  return routing_policy_v2_->getQueueConfiguration(consumers.size())
      .total_queues;
}

DegreeOfParallelism GeneralizedRouter::getDOP() const {
  size_t total = 0;
  for (auto &cons : consumers) {
    auto c_ptr = cons.lock();
    total += c_ptr->getDOP();
  }
  return DegreeOfParallelism{total};
}

void *GeneralizedRouter::allocate_buffers_for_free_pool(size_t free_pool_idx) {
  DCHECK_NE(buf_size, 0);
  void *mem = MemoryManager::mallocPinned(buf_size * slack);
  for (int j = 0; j < slack; ++j) {
    freeBufferGeneralized(free_pool_idx,
                          proteus::managed_ptr{((char *)mem) + j * buf_size});
  }
  const int free_size = free_pool.at(free_pool_idx).size_unsafe();
  counterlogger.log(getUUID(), counter_type::ROUTER_FREE_POOL_SIZE, free_size,
                    free_pool_idx);
  return mem;
}

void GeneralizedRouter::create_queues() {
  CHECK(routing_policy_v2_) << "Cannot create queues without routing policy";

  const auto config =
      routing_policy_v2_->getQueueConfiguration(consumers.size());
  const auto queueCnt = config.total_queues;

  if (free_pool.size() != queueCnt) {
    free_pool.clear();
    ready_fifo.clear();

    // Create queues and pools based on policy configuration
    if (config.shared_free_pools) {
      // Create minimal free pools (e.g., just pool 0 for round-robin)
      free_pool.emplace_back(1);
      for (size_t i = 0; i < queueCnt; ++i) {
        ready_fifo.emplace_back();
      }
    } else {
      // Create pool per queue (e.g., locality-aware)
      for (size_t i = 0; i < queueCnt; ++i) {
        free_pool.emplace_back(1);
        ready_fifo.emplace_back();
      }
    }
  } else {
    for (auto &f2 : free_pool) f2.reset();
    for (auto &f2 : ready_fifo) f2.reset();
  }
  return;
}

void GeneralizedRouter::open(Pipeline *pip) {
  event_range<range_log_op::GROUTER_OPEN> er{m_id, pip->getGeneratorUUID(),
                                             pip->getGroup()};
  std::lock_guard<std::mutex> guard(init_mutex);

  if (firers.empty()) {
    {
      event_range<range_log_op::GROUTER_CREATE_QUEUES> e{
          m_id, pip->getGeneratorUUID()};
      create_queues();
    }

    // Allocate and initialize policy state
    if (routing_policy_v2_) {
      policy_state_size_ = routing_policy_v2_->getStateSize();
      if (policy_state_size_ > 0) {
        // Allocate aligned memory for atomic operations
        policy_state_ =
            std::aligned_alloc(alignof(std::max_align_t), policy_state_size_);
        CHECK(policy_state_) << "Failed to allocate policy state";

        routing_policy_v2_->initializeState(policy_state_);
        routing_policy_v2_->setState(policy_state_);
      }
    }
    remaining_producers = producers;
    DLOG(INFO) << "GeneralizedRouter initialized with " << producers
               << " expected producers";
    std::unordered_set<int> allocated_pools{};
    for (auto &cons : consumers) {
      auto c_ptr = cons.lock();
      event_range<range_log_op::GROUTER_INIT_CONS> e{
          m_id, c_ptr->catch_pip->getUUID()};
      c_ptr->spawnWorker(pip->getSession(), firers, allocated_pools);
    }
  }
}

void GeneralizedRouter::close(Pipeline *pip) {
  time_block t("Tterm_exchange: ");

  int rem = --remaining_producers;
  CHECK_GE(rem, 0);

  if (rem == 0) {
    event_range<range_log_op::GROUTER_CLOSE> er{m_id, pip->getUUID(),
                                                pip->getGroup()};
    for (auto &r : ready_fifo) {
      r.close();
    }

    nvtxRangePushA("Exchange_waiting_to_close");
    for (auto &thread : firers) {
      if (thread.valid()) {
        thread.get();
      }
    }
    nvtxRangePop();
    firers.clear();
    for (auto &r : free_pool) r.close();
    
    // Clean up policy state
    if (routing_policy_v2_ && policy_state_) {
      routing_policy_v2_->cleanupState(policy_state_);
      std::free(policy_state_);
      policy_state_ = nullptr;
      routing_policy_v2_->setState(nullptr);
    }
  }
}

std::shared_ptr<GeneralizedRouterConsumer> GeneralizedRouter::appendConsumer(
    DeviceType target_device, DegreeOfParallelism dop,
    std::unique_ptr<Affinitizer> aff) {
  // TODO: Add V2 policy-specific warnings when locality-aware policies are implemented

  auto new_consumer = std::make_shared<GeneralizedRouterConsumer>(
      getSelfPtr(), dop, std::move(aff), target_device, consumers.size());
  consumers.emplace_back(std::weak_ptr(new_consumer));
  return new_consumer;
}

bool GeneralizedRouterConsumer::isFiltering() const {
  return producer->isFiltering();
}

RecordType GeneralizedRouterConsumer::getRowType() const {
  return producer->getRowType();
}

DegreeOfParallelism GeneralizedRouterConsumer::getDOPServers() const {
  return producer->getDOPServers();
}

DeviceType GeneralizedRouterConsumer::getDeviceType() const {
  return producer->getDeviceType();
}

bool GeneralizedRouterConsumer::isPacked() const {
  return producer->isPacked();
}

proteus::traits::HomReplication GeneralizedRouterConsumer::getHomReplication()
    const {
  return producer->getHomReplication();
}

void GeneralizedRouterConsumer::foreachTaskDo(int target_queue,
                                              int source_free_pool,
                                              Pipeline *pip,
                                              PipelineGen *pipGen,
                                              std::function<void(void *)> f) {
  DCHECK_LE(target_queue, producer->ready_fifo.size());
  producer->ready_fifo.at(target_queue)
      .foreachItemDo(
          [&]() {
            // We don't update the counters on every iteration to reduce log
            // flooding
            static int count;
            count += 1;
            if (count % 5 == 0) {
              const int fifo_size =
                  producer->ready_fifo.at(target_queue).size_unsafe();
              counterlogger.log(producer->getUUID(),
                                counter_type::ROUTER_READY_QUEUE_SIZE,
                                fifo_size, target_queue);
              const int free_size =
                  producer->free_pool.at(source_free_pool).size_unsafe();
              counterlogger.log(producer->getUUID(),
                                counter_type::ROUTER_FREE_POOL_SIZE, free_size,
                                target_queue);
            }
            return event_range<range_log_op::GROUTER_WAITING_FOR_TASK>{
                m_id, pipGen->getUUID(), pip->getGroup()};
          },
          [&](void *ptr) {
            int curr_count =
                consumed_count.fetch_add(1, std::memory_order_relaxed);
            // We don't update the count on every iteration to reduce log
            // flooding
            if (curr_count % 10 == 0) {
              counterlogger.log(producer->getUUID(),
                                counter_type::GROUTER_CONSUME_COUNT, curr_count,
                                consumer_index);
            }
            f(ptr);

            {
              producer->freeBufferGeneralized(source_free_pool,
                                              proteus::managed_ptr{ptr});
              std::this_thread::yield();
            }
          });
}

void GeneralizedRouterConsumer::fire(int target_queue, int local_target,
                                     int source_free_pool, PipelineGen *pipGen,
                                     const void *session,
                                     bool should_allocate_queue_buffs) {
  pthread_setname_np(pthread_self(),
                     (pipGen->getName() + ":" + std::to_string(target_queue) +
                      ":" + std::to_string(target_queue))
                         .c_str());
  auto &cu = aff->getAvailableCU(local_target);

  auto exec_affinity = cu.set_on_scope();
  auto pip = pipGen->getPipeline(local_target);
  event_range<range_log_op::GROUTER_CONS_FIRE> er{m_id, pip->getGeneratorUUID(),
                                                  pip->getGroup()};
  // if we remove that, following opens may allocate memory to wrong socket!
  std::this_thread::yield();
  void *buffer_mem = nullptr;
  if (should_allocate_queue_buffs) {
    event_range<range_log_op::GROUTER_ALLOC_QUEUE_BUFFS> er2{
        m_id, pip->getGeneratorUUID(), pip->getGroup()};
    buffer_mem = producer->allocate_buffers_for_free_pool(source_free_pool);
  }

  pip->open(session);
  {
    foreachTaskDo(target_queue, source_free_pool, pip.get(), pipGen,
                  [&](void *ptr) {
                    event_range<range_log_op::GROUTER_CONSUME> er2{
                        m_id, pip->getGeneratorUUID(), pip->getGroup()};
                    pip->consume((void *)(((uintptr_t)ptr) & ~uintptr_t(1)));
                  });
  }

  pip->close();
  if (buffer_mem != nullptr) {
    for (int j = 0; j < producer->slack; ++j) {
      /* Release and ignore, it will be handled by the following freePinned */
      ((void)(producer
                  ->acquireBufferGeneralized(source_free_pool, false,
                                             pip->getGroup())
                  .release()));
    }
    MemoryManager::freePinned(buffer_mem);
  }
  consumed_count = 0;
}

void GeneralizedRouterConsumer::spawnWorker(
    const void *session, threadvector &firers,
    std::unordered_set<int> &allocated_pools) {
  // Get policy from producer
  auto *policy = producer->routing_policy_v2_.get();
  CHECK(policy);

  // Get queue configuration from policy
  size_t queue_offset = policy->getQueueOffsetForConsumer(consumer_index);
  std::vector<int> local_targets =
      policy->getLocalQueuesForConsumer(consumer_index, aff.get());

  CHECK(!local_targets.empty())
      << "Consumer " << consumer_index << " has no local targets";

  for (size_t i = 0; i < fanout; ++i) {
    int local_target_idx = local_targets[i % local_targets.size()];
    int target_queue = queue_offset + local_target_idx;

    // Get the free pool for this queue from policy
    int source_free_pool = policy->getFreepoolForQueue(target_queue);

    // Determine if we should allocate buffers
    bool alloc_buffers = false;
    if (allocated_pools.find(source_free_pool) == allocated_pools.end()) {
      allocated_pools.insert(source_free_pool);
      alloc_buffers = true;
    }

    // if (alloc_buffers) {
    //   LOG(INFO) << "Will allocate buffer for consumer " << consumer_index
    //             << " target queue " << target_queue
    //             << " source free pool " << source_free_pool
    //             << " i: " << i;
    // }

    firers.emplace_back(&GeneralizedRouterConsumer::fire, this, target_queue, i,
                        source_free_pool, catch_pip, session, alloc_buffers);
  }
}

proteus::managed_ptr GeneralizedRouter::acquireBufferGeneralized(
    int free_pool_idx, bool polling, int64_t groupId) {
  CHECK_GE(free_pool_idx, 0) << "Invalid negative free pool index";
  CHECK_LT(free_pool_idx, free_pool.size())
      << "Free pool index " << free_pool_idx << " out of range";

  if (free_pool.at(free_pool_idx).empty_unsafe() && polling) {
    return nullptr;
  }
  void *buff = nullptr;
  auto x = free_pool.at(free_pool_idx).pop(buff);
  CHECK(x) << "Failed to acquire buffer from pool " << free_pool_idx;

  return proteus::managed_ptr{buff};
}

void GeneralizedRouter::releaseBufferGeneralized(int target,
                                                 proteus::managed_ptr buff) {
  CHECK_GE(target, 0) << "Invalid negative target queue";
  DCHECK_LT(target, ready_fifo.size())
      << "Target queue " << target << " out of range";
  ready_fifo.at(target).push(buff.release());
}

void GeneralizedRouter::freeBufferGeneralized(int free_pool_idx,
                                              proteus::managed_ptr buff) {
  DCHECK_GE(free_pool_idx, 0) << "Invalid negative free pool index";
  DCHECK_LT(free_pool_idx, free_pool.size())
      << "Free pool index " << free_pool_idx << " out of range";
  free_pool.at(free_pool_idx).emplace(buff.release());
}

bool GeneralizedRouter::get_readyGeneralized(int target,
                                             proteus::managed_ptr &buff) {
  assert(target < ready_fifo.size() && target >= 0);

  void *ptr;
  bool r = ready_fifo.at(target).pop2(ptr);
  if (r) buff = proteus::managed_ptr{ptr};
  return r;
}

// V2 Policy System Helper Method Implementations

routing::PolicyDataRequirements GeneralizedRouter::getRoutingDataRequirements() const {
  if (routing_policy_v2_) {
    return routing_policy_v2_->getDataRequirements();
  }
  // Return empty requirements for no policy
  return {{}, 0};
}

size_t GeneralizedRouter::getRoutingKeysSize() const {
  return getRoutingDataRequirements().total_size;
}

void GeneralizedRouter::generateDynamicKeyExtraction(
    OlapParallelContext *context, const OperatorState &childState,
    llvm::Value *&keys_ptr_out, llvm::Value *&keys_size_out) {
  auto requirements = getRoutingDataRequirements();
  llvm::LLVMContext &llvmContext = context->getLLVMContext();
  llvm::IRBuilder<> *Builder = context->getBuilder();

  if (requirements.total_size == 0) {
    // Round-robin case - no keys needed
    keys_ptr_out =
        llvm::ConstantPointerNull::get(llvm::Type::getInt8PtrTy(llvmContext));
    keys_size_out = context->createInt64(0);
    return;
  }

  // Allocate space for keys
  llvm::AllocaInst *keys_alloca = Builder->CreateAlloca(llvm::ArrayType::get(
      llvm::Type::getInt8Ty(llvmContext), requirements.total_size));
  keys_ptr_out = Builder->CreateBitCast(keys_alloca,
                                        llvm::Type::getInt8PtrTy(llvmContext));
  keys_size_out = context->createInt64(requirements.total_size);

  // Extract each required field
  for (const auto &field : requirements.required_keys) {
    llvm::Value *value = nullptr;

    switch (field.source) {
      case routing::PolicyDataRequirements::FieldSource::FIRST_DATA_FIELD: {
        // Extract first field from wantedFields (data pointer)
        CHECK(!wantedFields.empty())
            << "No data fields available for locality routing";

        auto rec = childState.getProducer().getRowType();
        ExpressionGeneratorVisitor vis{context, childState};
        value = expressions::InputArgument{&rec}[*wantedFields[0]]
                    .accept(vis)
                    .value;

        // NUMA detection expects void*, ensure proper type
        CHECK(value->getType()->isPointerTy())
            << "First data field must be a pointer for locality routing";

        break;
      }

      case routing::PolicyDataRequirements::FieldSource::TUPLE_IDENTIFIER: {
        // Extract tuple identifier (OID)
        std::shared_ptr<Plugin> pg = Catalog::getInstance().getPlugin(
            wantedFields[0]->getRelationName());
        RecordAttribute tupleIdentifier(wantedFields[0]->getRelationName(),
                                        activeLoop, pg->getOIDType());
        ProteusValueMemory mem_oidWrapper = childState[tupleIdentifier];
        value = Builder->CreateLoad(
            mem_oidWrapper.mem->getType()->getPointerElementType(),
            mem_oidWrapper.mem);
        break;
      }

      case routing::PolicyDataRequirements::FieldSource::SOURCE_SERVER: {
        // Extract source server ID
        try {
          value = Builder->CreateLoad(
              childState[{wantedFields[0]->getRelationName(), "srcServer",
                          new Int64Type()}]
                  .mem->getType()
                  ->getPointerElementType(),
              childState[{wantedFields[0]->getRelationName(), "srcServer",
                          new Int64Type()}]
                  .mem);
        } catch (const std::out_of_range &) {
          value = context->createInt64(InfiniBandManager::server_id());
        }
        break;
      }

      case routing::PolicyDataRequirements::FieldSource::CUSTOM_HASH: {
        // TODO: Implement custom hash extraction
        CHECK(false) << "CUSTOM_HASH field source not implemented yet";
      }

      default:
        CHECK(false) << "Unknown field source: "
                     << static_cast<int>(field.source);
    }

    // Store the extracted value at the correct offset
    if (value) {
      llvm::Value *field_ptr = Builder->CreateInBoundsGEP(
          keys_alloca->getAllocatedType(), keys_alloca,
          {context->createInt32(0), context->createInt64(field.offset)});

      // Cast to appropriate pointer type and store
      llvm::Type *field_type = value->getType();
      llvm::PointerType *store_ptr_type = llvm::PointerType::get(field_type, 0);
      llvm::Value *typed_ptr =
          Builder->CreateBitCast(field_ptr, store_ptr_type);
      Builder->CreateStore(value, typed_ptr);
    }
  }
}

// Variant-based policy configuration
routing::PolicyConfigVariant GeneralizedRouter::createPolicyConfig() const {
  switch (policy_type_v2_) {
    case routing::GeneralizedRoutingPolicyV2::ROUND_ROBIN:
      return std::monostate{};

    case routing::GeneralizedRoutingPolicyV2::LOCALITY_AWARE: {
      routing::LocalityAwarePolicyConfig config;

      // Extract affinitizers and device types from consumers
      for (const auto &weak_consumer : consumers) {
        auto consumer = weak_consumer.lock();
        CHECK(consumer) << "Consumer expired during config creation";

        // Access protected members (we're a friend class)
        config.consumer_affinitizers.push_back(consumer->aff.get());
        config.consumer_device_types.push_back(consumer->target_device);
      }

      // Validate before returning
      config.validate();
      return config;
    }

    default:
      CHECK(false) << "Unsupported routing policy: "
                   << static_cast<int>(policy_type_v2_);
  }
}

}  // namespace proteus
