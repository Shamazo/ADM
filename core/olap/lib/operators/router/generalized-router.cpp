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

#include "generalized-router.hpp"

#include <codegen/jit/pipeline.hpp>
#include <lib/expressions/expressions-generator.hpp>
#include <lib/util/catalog.hpp>
#include <platform/network/infiniband/infiniband-manager.hpp>
#include <platform/util/demangle.hpp>
#include <platform/util/timing.hpp>

#include "lib/operators/router/routing-policy.hpp"
#include "router.hpp"

namespace proteus {

int64_t getGroupId(Pipeline *pip) { return pip->getGroup(); }

[[nodiscard]] void *acquireBufferGeneralized(int target, GeneralizedRouter *xch,
                                             PipelineGen *pipGen,
                                             int64_t groupId) {
  return xch->acquireBufferGeneralized(target, false, pipGen, groupId)
      .release();
}

[[nodiscard]] void *try_acquireBufferGeneralized(int target,
                                                 GeneralizedRouter *xch,
                                                 PipelineGen *pipGen,
                                                 int64_t groupId) {
  return xch->acquireBufferGeneralized(target, true, pipGen, groupId).release();
}

void releaseBufferGeneralized(int target, GeneralizedRouter *xch, void *buff) {
  xch->releaseBufferGeneralized(target, proteus::managed_ptr{buff});
}

void GeneralizedRouterConsumer::produce_(OlapParallelContext *context) {
  consume(context, {*this, {}});

  catch_pip = context->operator->();

  producer.produceForConsumer(*this, context);
}

void GeneralizedRouterConsumer::consume(OlapParallelContext *context,
                                        const OperatorState &childState)
{
  auto &llvmContext = context->getLLVMContext();

  Plugin *pg = Catalog::getInstance().getPlugin(
      producer.wantedFields[0]->getRelationName());

  const ExpressionType *ptoid = pg->getOIDType();

  llvm::Type *oidType = ptoid->getLLVMType(llvmContext);

  std::vector<llvm::Type *> param_typelist;
  for (auto field : producer.wantedFields) {
    auto *wtype = field->getLLVMType(llvmContext);
    if (wtype == nullptr)
      wtype = oidType;  // FIXME: dirty hack for JSON inner lists

    param_typelist.push_back(wtype);
    producer.need_cnt =
        producer.need_cnt || (field->getOriginalType()->getTypeID() == BLOCK);
  }

  param_typelist.push_back(oidType);                              // oid
  param_typelist.push_back(llvm::Type::getInt64Ty(llvmContext));  // srcServer
  if (producer.need_cnt) param_typelist.push_back(oidType);       // cnt

  producer.params_type = llvm::StructType::get(llvmContext, param_typelist);
  producer.buf_size = context->getSizeOf(producer.params_type);

  RecordAttribute tupleCnt(producer.wantedFields[0]->getRelationName(),
                           "activeCnt",
                           pg->getOIDType());  // FIXME: OID type for blocks ?
  RecordAttribute tupleIdentifier(producer.wantedFields[0]->getRelationName(),
                                  activeLoop, pg->getOIDType());
  RecordAttribute srcServer{producer.wantedFields[0]->getRelationName(),
                            "srcServer",
                            new Int64Type()};  // FIXME: OID type for blocks ?

  // Generate catch code
  auto p = context->appendParameter(
      llvm::PointerType::get(producer.params_type, 0), true, true);
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

  for (size_t i = 0; i < producer.wantedFields.size(); ++i) {
    auto *param = Builder->CreateExtractValue(params, i);

    // FIMXE: should we alse transfer this information ?
    variableBindings[*(producer.wantedFields[i])] =
        context->toMem(param, context->createFalse());
  }
  auto *oid = Builder->CreateExtractValue(params, producer.wantedFields.size());
  variableBindings[tupleIdentifier] =
      context->toMem(oid, context->createFalse());

  auto *srv =
      Builder->CreateExtractValue(params, producer.wantedFields.size() + 1);
  variableBindings[srcServer] =
      context->toMem(srv, context->createFalse(), "srcServer");

  if (producer.need_cnt) {
    auto *cnt =
        Builder->CreateExtractValue(params, producer.wantedFields.size() + 2);
    variableBindings[tupleCnt] = context->toMem(cnt, context->createFalse());
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
  // Builder->CreateRetVoid();
}

std::unique_ptr<routing::RoutingPolicy> GeneralizedRouter::getPolicy(
    RoutingPolicy p, DegreeOfParallelism dop,
    const std::vector<RecordAttribute *> &wantedFields) {
  switch (p) {
      //    case RoutingPolicy::HASH_BASED: {
      //      assert(hashExpr.has_value());
      //      return std::make_unique<routing::HashBased>(fanout,
      //      hashExpr.value());
      //    }
      //      //    case RoutingPolicy::LOCAL: {
      //      //      return std::make_unique<routing::Local>(
      //      //          fanout, wantedFields, new AffinityPolicy(fanout,
      //      //          aff.get()));
      //      //      //      return std::make_unique<routing::PreferLocal>(
      //      //      //          fanout, wantedFields, new
      //      AffinityPolicy(fanout,
      //      //      aff.get()));
      //      //    }
      //      //    case RoutingPolicy::FORCE_LOCAL: {
      //      //      return std::make_unique<routing::Local>(
      //      //          fanout, wantedFields, new AffinityPolicy(fanout,
      //      //          aff.get()));
      //      //    }
    case RoutingPolicy::LOCAL:  // FIXME: add the flexible version
    case RoutingPolicy::FORCE_LOCAL: {
      return std::make_unique<routing::Local>(
          dop, wantedFields,
          new AffinityPolicy(dop, new CpuNumaNodeAffinitizer()));
    }
    case RoutingPolicy::RANDOM: {
      return std::make_unique<routing::Random>(dop);
    }
    default: {
      assert(false && "Unimplemented");  // FIXME: rest of the policies
    }
  }
}

void GeneralizedRouter::produceForConsumer(
    const GeneralizedRouterConsumer &cons, OlapParallelContext *context) {
  assert(cons.producer == *this);
  if (&cons == consumers.back().get()) {
    produce(context);
  }
}

void GeneralizedRouter::produce_(OlapParallelContext *context) {
  context->popPipeline();

  // push new pipeline for the throw part
  context->pushPipeline();

  context->registerOpen(this, [this](Pipeline *pip) { this->open(pip); });
  context->registerClose(this, [this](Pipeline *pip) { this->close(pip); });

  groupVar = context->appendStateVar(
      llvm::IntegerType::getIntNTy(context->getLLVMContext(),
                                   sizeof(int64_t) * 8),
      [=](llvm::Value *pip) {
        return context->gen_call(proteus::getGroupId, {pip});
      },
      [=](llvm::Value *, llvm::Value *s) {});

  getChild()->produce(context);
}

llvm::Value *GeneralizedRouter::createTaskDescription(
    OlapParallelContext *context, const OperatorState &childState) {
  auto &llvmContext = context->getLLVMContext();
  auto *Builder = context->getBuilder();

  llvm::Value *params = llvm::UndefValue::get(params_type);

  Plugin *pg =
      Catalog::getInstance().getPlugin(wantedFields[0]->getRelationName());

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
  }

  return params;
}

void GeneralizedRouter::consume(OlapParallelContext *context,
                                const OperatorState &childState) {
  llvm::LLVMContext &llvmContext = context->getLLVMContext();
  llvm::IRBuilder<> *Builder = context->getBuilder();

  llvm::PointerType *charPtrType = llvm::Type::getInt8PtrTy(llvmContext);
  auto groupId = context->getStateVar(groupVar);

  auto params = createTaskDescription(context, childState);

  llvm::Value *exchangePtr =
      llvm::ConstantInt::get(llvmContext, llvm::APInt(64, ((uint64_t)this)));
  llvm::Value *exchange = Builder->CreateIntToPtr(exchangePtr, charPtrType);

  auto retry_cnt =
      context->toMem(context->createInt32(0), context->createFalse());

  llvm::Value *param_ptr;
  llvm::Value *target;
  context
      ->gen_do([&]() {
        // FIXME: rest of policies
        switch (policy_type) {
          case RoutingPolicy::RANDOM: {
            target = context->createInt32(0);
            param_ptr = context->gen_call(
                proteus::acquireBufferGeneralized,
                {target, exchange,
                 context->CastPtrToLlvmPtr(charPtrType,
                                           context->getCurrentPipeline()),
                 groupId});
            break;
          }
          default: {
            auto srcServer = [&]() -> llvm::Value * {
              try {
                return Builder->CreateLoad(
                    childState[{wantedFields[0]->getRelationName(), "srcServer",
                                new Int64Type()}]
                        .mem->getType()
                        ->getPointerElementType(),
                    childState[{wantedFields[0]->getRelationName(), "srcServer",
                                new Int64Type()}]
                        .mem);
              } catch (const std::out_of_range &) {
                return context->createInt64(InfiniBandManager::server_id());
              }
            }();

            bool may_retry = false;

            auto phi_type =
                llvm::IntegerType::getInt32Ty(context->getLLVMContext());
            llvm::BasicBlock *b1;
            llvm::BasicBlock *b2;
            llvm::Value *p1;
            llvm::Value *p2;
            context
                ->gen_if({Builder->CreateICmpEQ(
                              srcServer, context->createInt64(
                                             InfiniBandManager::server_id())),
                          context->createFalse()})([&]() {
                  auto &x = *routing;
                  LOG(INFO) << demangle(typeid(x).name());
                  auto r = routing->evaluate(context, childState, retry_cnt);

                  r.target->setName("target");
                  p1 = Builder->CreateTruncOrBitCast(
                      r.target, llvm::Type::getInt32Ty(llvmContext));
                  may_retry = r.may_retry;

                  b1 = Builder->GetInsertBlock();
                })
                .gen_else([&]() {
                  p2 = Builder->CreateURem(
                      Builder->CreateTruncOrBitCast(
                          context->gen_call(rand, {}),
                          llvm::Type::getInt32Ty(llvmContext)),
                      context->createInt32(2));
                  may_retry = false;

                  b2 = Builder->GetInsertBlock();
                });

            auto phi = Builder->CreatePHI(phi_type, 2);
            phi->addIncoming(p1, b1);
            phi->addIncoming(p2, b2);
            target = phi;

            assert(!may_retry &&
                   "Unimplemented, needs to take another path above due to the "
                   "mismatch of the two may_retry paths");
            param_ptr = context->gen_call(
                (may_retry)
                    ? (proteus::try_acquireBufferGeneralized /* FIXME */)
                    : (proteus::acquireBufferGeneralized),
                {target, exchange,
                 context->CastPtrToLlvmPtr(charPtrType,
                                           context->getCurrentPipeline()),
                 groupId});

            break;
          }
        }

        Builder->CreateStore(
            Builder->CreateAdd(
                Builder->CreateLoad(
                    retry_cnt.mem->getType()->getPointerElementType(),
                    retry_cnt.mem),
                context->createInt32(1)),
            retry_cnt.mem);
      })
      .gen_while([&]() {
        llvm::Value *null_ptr = llvm::ConstantPointerNull::get(
            ((llvm::PointerType *)param_ptr->getType()));
        llvm::Value *is_null = Builder->CreateICmpEQ(param_ptr, null_ptr);

        return ProteusValue{is_null, context->createFalse()};
      });

  param_ptr = Builder->CreateBitCast(
      param_ptr, llvm::PointerType::getUnqual(params->getType()));

  Builder->CreateStore(params, param_ptr);

  context->gen_call(
      proteus::releaseBufferGeneralized,
      {target, exchange, Builder->CreateBitCast(param_ptr, charPtrType)});
}

size_t GeneralizedRouter::getNumberOfQueues() const {
  switch (policy_type) {
    case RoutingPolicy::LOCAL:  // FIMXE
    case RoutingPolicy::FORCE_LOCAL: {
      auto &topo = topology::getInstance();
      return topo.getCpuNumaNodeCount() + topo.getGpuCount();
    }
    case RoutingPolicy::RANDOM: {
      return 1;
    }
    case RoutingPolicy::HASH_BASED: {
      return getDOP();
    }
  }
}

DegreeOfParallelism GeneralizedRouter::getDOP() const {
  size_t total = 0;
  for (auto &cons : consumers) total += cons->getDOP();
  return DegreeOfParallelism{total};
}

void GeneralizedRouter::open(Pipeline *pip) {
  std::lock_guard<std::mutex> guard(init_mutex);

  if (firers.empty()) {
    auto queueCnt = getNumberOfQueues();

    if (free_pool.size() != queueCnt) {
      free_pool.clear();  // = new threadsafe_set<void *>[fanout];
      ready_fifo.clear();
      //    ready_fifo.reserve(fanout);
      //    ready_fifo = new AsyncQueueMPMC<void *>[fanout];
      //    assert(free_pool);

      auto limit = 1;  // dynamic_cast<Split *>(this) ? 1 : 2;
      for (int i = 0; i < queueCnt; ++i) {
        free_pool.emplace_back(1 /* FIXME: i % limit +
                               (dynamic_cast<Split *>(this) ? 1 : 0) */);
        ready_fifo.emplace_back(1 /* FIXME: i % limit +
                               (dynamic_cast<Split *>(this) ? 1 : 0) */);
        //      ready_fifo[i].reset();
      }
    } else {
      for (auto &f2 : free_pool) f2.reset();
      for (auto &f2 : ready_fifo) f2.reset();
    }

    //    eventlogger.log(this, log_op::EXCHANGE_INIT_CONS_START);
    remaining_producers = producers;
    for (auto &cons : consumers) cons->spawnWorker(pip->getSession(), firers);
    //    eventlogger.log(this, log_op::EXCHANGE_INIT_CONS_END);
  }
}

void GeneralizedRouter::close(Pipeline *pip) {
  time_block t("Tterm_exchange: ");

  int rem = --remaining_producers;
  assert(rem >= 0);

  // for (int i = 0 ; i < fanout ; ++i) ready_pool_cv[i].notify_all();

  if (rem == 0) {
    size_t p = 0;
    for (auto &r : ready_fifo) {
      r.close();
    }

    //    eventlogger.log(this, log_op::EXCHANGE_JOIN_START);
    nvtxRangePushA("Exchange_waiting_to_close");
    for (auto &t : firers) t.get();
    nvtxRangePop();
    //    eventlogger.log(this, log_op::EXCHANGE_JOIN_END);
    firers.clear();

    for (auto &r : free_pool) r.close();
    //    //    delete[] free_pool;
    //    free_pool.clear();
    //    //    delete[] ready_fifo;
    //    ready_fifo.clear();
  }
}
GeneralizedRouterConsumer *GeneralizedRouter::appendConsumer(
    DeviceType target, DegreeOfParallelism dop,
    std::unique_ptr<Affinitizer> aff) {
  consumers.emplace_back(
      // FIXME: propagate target if needed
      std::make_unique<GeneralizedRouterConsumer>(*this, dop, std::move(aff)));
  return consumers.back().get();
}

bool GeneralizedRouterConsumer::isFiltering() const {
  return producer.isFiltering();
}

RecordType GeneralizedRouterConsumer::getRowType() const {
  return producer.getRowType();
}

DegreeOfParallelism GeneralizedRouterConsumer::getDOPServers() const {
  return producer.getDOPServers();
}

DeviceType GeneralizedRouterConsumer::getDeviceType() const {
  return producer.getDeviceType();
}

bool GeneralizedRouterConsumer::isPacked() const { return producer.isPacked(); }

proteus::traits::HomReplication GeneralizedRouterConsumer::getHomReplication()
    const {
  return producer.getHomReplication();
}

void GeneralizedRouterConsumer::foreachTaskDo(int target, Pipeline *pip,
                                              PipelineGen *pipGen,
                                              std::function<void(void *)> f) {
  assert(target < producer.ready_fifo.size());
  producer.ready_fifo.at(target).foreachItemDo(
      [&]() {
        return event_range<range_log_op::ROUTER_WAITING_FOR_TASK>{
            this, pipGen, pip->getGroup()};
      },
      [&](void *ptr) {
        f(ptr);

        {
          producer.freeBufferGeneralized(target, proteus::managed_ptr{ptr});
          std::this_thread::yield();
        }
      });
}

void GeneralizedRouterConsumer::fire(int target, int local_target,
                                     PipelineGen *pipGen, const void *session) {
  pthread_setname_np(pthread_self(),
                     (pipGen->getName() + std::to_string((uintptr_t)this) +
                      "::" + std::to_string(target))
                         .c_str());
  const auto &cu = aff->getAvailableCU(local_target);
  // set_exec_location_on_scope d(cu);
  auto exec_affinity = cu.set_on_scope();
  auto pip = pipGen->getPipeline(local_target);
  std::this_thread::yield();  // if we remove that, following opens may
  // allocate memory to wrong socket!
  void *mem = nullptr;
  auto s = producer.slack;
  auto f = producer.getNumberOfQueues();

  if (target < f) {
    assert(producer.buf_size);
    mem = MemoryManager::mallocPinned(producer.buf_size * s);
    //    LOG_IF(INFO, dynamic_cast<routing::Local *>(getPolicy().get()))
    //    << topology::getInstance().getCpuNumaNodeAddressed(mem)->id;
    for (int j = 0; j < s; ++j) {
      producer.freeBufferGeneralized(
          target, proteus::managed_ptr{((char *)mem) + j * producer.buf_size});
    }
  }

  {
    event_range<range_log_op::EXCHANGE_INIT_CONS> e{this, pipGen,
                                                    pip->getGroup()};
    pip->open(session);
  }

  {
    auto target2 = (producer.policy_type == RoutingPolicy::RANDOM)
                       ? 0
                       : (local_target % 2);

    foreachTaskDo(target2, pip.get(), pipGen, [&](void *ptr) {
      pip->consume((void *)(((uintptr_t)ptr) & ~uintptr_t(1)));
    });
  }

  pip->close();

  if (target < f) {
    for (int j = 0; j < s; ++j) {
      /* Release and ignore, it will be handled by the following freePinned */
      ((void)(producer
                  .acquireBufferGeneralized(target, false, pipGen,
                                            pip->getGroup())
                  .release()));
    }
    MemoryManager::freePinned(mem);
  }
}

void GeneralizedRouterConsumer::spawnWorker(const void *session,
                                            threadvector &firers) {
  auto start = firers.size();
  for (size_t i = 0; i < fanout; ++i) {
    firers.emplace_back(&GeneralizedRouterConsumer::fire, this, start + i, i,
                        catch_pip, session);
  }
}

proteus::managed_ptr GeneralizedRouter::acquireBufferGeneralized(
    int target, bool polling, PipelineGen *pipGen, int64_t groupId) {
  if (free_pool.at(target).empty_unsafe() && polling) {
    LOG(INFO) << free_pool.at(target).size_unsafe();
    nvtxRangePop();
    return nullptr;
  }
  void *buff = nullptr;
  assert(target < ready_fifo.size());
  auto x = free_pool.at(target).pop(buff);
  assert(x);

  return proteus::managed_ptr{buff};
}

void GeneralizedRouter::releaseBufferGeneralized(int target,
                                                 proteus::managed_ptr buff) {
  assert(target < ready_fifo.size());
  ready_fifo.at(target).push(buff.release());
}

void GeneralizedRouter::freeBufferGeneralized(int target,
                                              proteus::managed_ptr buff) {
  assert(target < free_pool.size());
  free_pool.at(target).emplace(buff.release());
}

bool GeneralizedRouter::get_readyGeneralized(int target,
                                             proteus::managed_ptr &buff) {
  assert(target < ready_fifo.size() && target >= 0);

  void *ptr;
  bool r = ready_fifo.at(target).pop2(ptr);
  if (r) buff = proteus::managed_ptr{ptr};
  return r;
}

}  // namespace proteus
