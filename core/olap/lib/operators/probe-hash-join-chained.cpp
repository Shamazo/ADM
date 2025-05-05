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

#include "probe-hash-join-chained.hpp"

#include <codegen/jit/pipeline.hpp>
#include <platform/memory/memory-manager.hpp>
#include <utility>

#include "hash-join-chained-morsel.hpp"
#include "lib/expressions/expressions-hasher.hpp"
#include "olap/util/parallel-context.hpp"

using namespace llvm;

/**
 * Constructs a ProbeHashJoinChained operator that uses a shared hash table from
 * an existing HashJoinChained.
 *
 * @param buildJoin The HashJoinChained operator whose build hash table will be
 * shared. It must have already been constructed, but may not have executed yet.
 *                  Its buildState must be initialized, even if only with
 * placeholder values.
 * @param probe_mat_exprs Materialization expressions for the probe side
 * @param probe_packet_widths Packet widths for the probe side (in bits)
 * @param probe_keyexpr Expression for the probe key to match against build keys
 * @param probe_child Child operator that produces probe tuples
 * @param opLabel Identifier label for this operator
 *
 * Note: build_mat_exprs and build_packet_widths are deliberately NOT
 * initialized in the initializer list. Instead, they will be populated from
 * buildState in probeHashTableFormat() after the build phase has initialized
 * packind values.
 */
ProbeHashJoinChained::ProbeHashJoinChained(
    std::shared_ptr<HashJoinChained> buildJoin,
    std::vector<GpuMatExpr> probe_mat_exprs,
    const std::vector<size_t> &probe_packet_widths, expression_t probe_keyexpr,
    std::shared_ptr<Operator> const probe_child, std::string opLabel)
    : UnaryOperator(probe_child),
      probe_mat_exprs(std::move(probe_mat_exprs)),
      build_keyexpr(buildJoin->build_keyexpr),
      probe_keyexpr(std::move(probe_keyexpr)),
      buildState(buildJoin->getBuildState()),
      hash_bits(buildJoin->hash_bits),
      opLabel(std::move(opLabel)) {
  // Ensure build state is available and increment reference count
  CHECK(buildState) << "Cannot create ProbeHashJoinChained: no build state "
                       "available in the provided HashJoinChained";
  CHECK(buildJoin->getDOP() == 1 || dynamic_cast<HashJoinChainedMorsel*>(buildJoin.get()) !=
        nullptr)
      << "ProbeHashJoinChained only works with HashJoinChainedMorsel or with single threaded HashJoinChained";
  buildState->ref_count += 1;
}

void ProbeHashJoinChained::produce_(OlapParallelContext *context) {
  // First, set up the format for the hash table probe
  probeHashTableFormat(context);

  // Register callbacks for pipeline open/close
  context->registerOpen(this, [this](Pipeline *pip) { this->open_probe(pip); });
  context->registerClose(this,
                         [this](Pipeline *pip) { this->close_probe(pip); });

  // Delegate to child operator to produce tuples
  getChild()->produce(context);
}

void ProbeHashJoinChained::consume(OlapParallelContext *const context,
                                   const OperatorState &childState) {
  generate_probe(context, childState);
}

/**
 * Set up the state variables and LLVM types for the probe phase of the hash
 * join.
 *
 * This method uses the shared build state from the original HashJoinChained to:
 * 1. Retrieve properly initialized materialization expressions (with packind
 * values)
 * 2. Create matching struct types for the hash table record format
 * 3. Set up state variables for accessing the hash table during code generation
 */
void ProbeHashJoinChained::probeHashTableFormat(OlapParallelContext *context) {
  // Verify that we have the shared state
  CHECK(buildState) << "No build state available in "
                       "ProbeHashJoinChained::probeHashTableFormat";

  // Ensure build state has the required data
  if (buildState->build_mat_exprs.empty() ||
      buildState->build_packet_widths.empty()) {
    LOG(WARNING) << "Build state does not have initialized expressions.";
  }

  // Copy expressions from the build state
  // at this point the base HashJoinChained should havbe already generated its
  // code which means the GpuMatExpr->packind should be initialized
  build_mat_exprs = buildState->build_mat_exprs;
  build_packet_widths = buildState->build_packet_widths;

  // Create a pointer to the head of the hash table
  Type *int32_type = Type::getInt32Ty(context->getLLVMContext());
  Type *t_head_ptr = PointerType::getUnqual(int32_type);
  probe_head_param_id = context->appendStateVar(t_head_ptr);

  // Process each packet width to create the same structure types as in
  // HashJoinChained
  size_t i = 0;
  for (size_t p = 0; p < build_packet_widths.size(); ++p) {
    size_t bindex = 0;
    size_t packind = 0;

    std::vector<Type *> body;
    while (i < build_mat_exprs.size() && build_mat_exprs[i].packet == p) {
      if (build_mat_exprs[i].bitoffset != bindex) {
        // Insert space for alignment
        assert(build_mat_exprs[i].bitoffset > bindex);
        body.push_back(
            Type::getIntNTy(context->getLLVMContext(),
                            (build_mat_exprs[i].bitoffset - bindex)));
        ++packind;
      }

      auto out_type = build_mat_exprs[i].expr.getExpressionType();
      Type *llvm_type = out_type->getLLVMType(context->getLLVMContext());

      body.push_back(llvm_type);
      bindex = build_mat_exprs[i].bitoffset + context->getSizeOf(llvm_type) * 8;

      // We don't modify packind here - we use the value from buildState
      ++i;
    }
    CHECK_GE(build_packet_widths[p], bindex);

    if (build_packet_widths[p] > bindex) {
      body.push_back(Type::getIntNTy(context->getLLVMContext(),
                                     (build_packet_widths[p] - bindex)));
    }

    // Critical: Use the exact same naming convention as HashJoinChained
    std::string structName = "hj_chained_struct_" + std::to_string(p);
    Type *t;

    // Try to look up an existing struct type with this name in the LLVM context
    if (StructType *existingType =
            StructType::getTypeByName(context->getLLVMContext(), structName)) {
      t = existingType;
    } else {
      // Otherwise create a new one
      t = StructType::create(body, structName, true);
    }

    Type *t_ptr = PointerType::getUnqual(t);
    in_param_ids.push_back(context->appendStateVar(t_ptr));
  }

  // Sanity checks
  CHECK_EQ(i, build_mat_exprs.size())
      << "Inconsistent build_mat_exprs in "
         "ProbeHashJoinChained::probeHashTableFormat";

  CHECK(!in_param_ids.empty())
      << "No in_param_ids created in probeHashTableFormat";
}

Value *ProbeHashJoinChained::hash(const expression_t &exprs,
                                  Context *const context,
                                  const OperatorState &childState) const {
  ExpressionHasherVisitor hasher{context, childState};
  Value *hash = exprs.accept(hasher).value;
  auto size = ConstantInt::get(hash->getType(), (size_t(1) << hash_bits));
  return context->getBuilder()->CreateURem(hash, size);
}

void ProbeHashJoinChained::generate_probe(OlapParallelContext *context,
                                          const OperatorState &childState) {
  IRBuilder<> *Builder = context->getBuilder();
  LLVMContext &llvmContext = context->getLLVMContext();
  Function *TheFunction = Builder->GetInsertBlock()->getParent();

  BasicBlock *insBB = Builder->GetInsertBlock();

  Builder->SetInsertPoint(context->getCurrentEntryBlock());
  Value *head_ptr = context->getStateVar(probe_head_param_id);
  head_ptr->setName(opLabel + "_head_ptr");

  Builder->SetInsertPoint(insBB);

  ExpressionGeneratorVisitor exprGenerator(context, childState);
  ProteusValue keyWrapper = probe_keyexpr.accept(exprGenerator);
  Value *hash = ProbeHashJoinChained::hash(probe_keyexpr, context, childState);

  // current = head[hash(key)]
  auto currentPtr = Builder->CreateInBoundsGEP(
      head_ptr->getType()->getNonOpaquePointerElementType(), head_ptr, hash);
  Value *current = Builder->CreateLoad(
      currentPtr->getType()->getPointerElementType(), currentPtr);
  current->setName("current");

  AllocaInst *mem_current = context->CreateEntryBlockAlloca(
      TheFunction, "mem_current", current->getType());

  Builder->CreateStore(current, mem_current);

  // while (current != eoc){

  BasicBlock *CondBB =
      BasicBlock::Create(llvmContext, "chainFollowCond", TheFunction);
  BasicBlock *ThenBB =
      BasicBlock::Create(llvmContext, "chainFollow", TheFunction);
  BasicBlock *MergeBB = BasicBlock::Create(llvmContext, "cont", TheFunction);

  Builder->CreateBr(CondBB);

  Builder->SetInsertPoint(CondBB);

  // check end of chain

  Value *condition = Builder->CreateICmpNE(
      Builder->CreateLoad(mem_current->getType()->getPointerElementType(),
                          mem_current),
      ConstantInt::get(current->getType(), ~((size_t)0)));

  Builder->CreateCondBr(condition, ThenBB, MergeBB);
  Builder->SetInsertPoint(ThenBB);

  // check match

  // Check if we have any in_param_ids
  if (in_param_ids.empty()) {
    LOG(ERROR) << "No in_param_ids available for probe - this might indicate a "
                  "problem with probeHashTableFormat";
    throw runtime_error("No in_param_ids available for probe");
  }

  std::vector<Value *> in_ptrs;
  std::vector<Value *> in_vals;
  for (size_t i = 0; i < in_param_ids.size(); ++i) {
    Value *in_ptr = context->getStateVar(in_param_ids[i]);

    // LOG(INFO) << "Processing parameter " << i << " of type "
    //           << (in_ptr && in_ptr->getType() ?
    //           in_ptr->getType()->getTypeID() : -1);

    if (in_param_ids.size() != 1) {
      in_ptr->setName(opLabel + "_data" + std::to_string(i) + "_ptr");
    } else {
      in_ptr->setName(opLabel + "_data_ptr");
    }

    // We're creating code to access a record at the current position in the
    // hash chain
    Value *current_val = Builder->CreateLoad(
        mem_current->getType()->getPointerElementType(), mem_current);

    // GEP instruction to index into the hash table with current_val
    Value *gep = Builder->CreateInBoundsGEP(
        in_ptr->getType()->getNonOpaquePointerElementType(), in_ptr,
        current_val);
    gep->setName(opLabel + "_data" + std::to_string(i) + "_ptr_at_curr");
    in_ptrs.push_back(gep);

    // Load the record from the hash table entry
    Type *elemType = in_ptrs.back()->getType()->getPointerElementType();
    Value *loaded_val = Builder->CreateLoad(elemType, in_ptrs.back());
    loaded_val->setName(opLabel + "_data" + std::to_string(i) + "_val");

    in_vals.push_back(loaded_val);
  }

  // We need at least one value to proceed
  if (in_vals.empty()) {
    LOG(ERROR) << "No values loaded from hash table";
    throw runtime_error("No values loaded from hash table");
  }

  // Examine the type of the loaded value before trying to extract from it
  Value *firstVal = in_vals[0];

  // Check if it's a struct type before extracting values
  if (!firstVal->getType()->isStructTy()) {
    LOG(ERROR) << "Cannot extract values from non-struct type: "
               << firstVal->getType()->getTypeID();
    throw runtime_error("First value is not a struct type");
  }

  std::vector<unsigned> Indices0;
  Indices0.push_back(0);
  Value *next = Builder->CreateExtractValue(firstVal, Indices0);
  next->setName("next_chain_ptr");

  std::vector<unsigned> Indices1;
  Indices1.push_back(1);
  Value *build_key = Builder->CreateExtractValue(firstVal, Indices1);
  build_key->setName("build_key");

  // Store the next pointer for chain traversal
  Builder->CreateStore(next, mem_current);

  // Set up the key comparison
  ExpressionGeneratorVisitor eqGenerator{context, childState};
  expressions::ProteusValueExpression build_expr{
      probe_keyexpr.getExpressionType(),
      ProteusValue{build_key, context->createFalse()}};
  Value *match_condition =
      eq(probe_keyexpr, build_expr).accept(eqGenerator).value;

  BasicBlock *MatchThenBB =
      BasicBlock::Create(llvmContext, "matchChainFollow", TheFunction);

  Builder->CreateCondBr(match_condition, MatchThenBB, CondBB);

  Builder->SetInsertPoint(MatchThenBB);

  // Reconstruct tuples
  std::map<RecordAttribute, ProteusValueMemory> allJoinBindings;

  if (probe_keyexpr.isRegistered()) {
    allJoinBindings[probe_keyexpr.getRegisteredAs()] =
        context->toMem(keyWrapper.value, context->createFalse());
  }

  if (probe_keyexpr.getExpressionType()->getTypeID() == RECORD) {
    auto rc = dynamic_cast<const expressions::RecordConstruction *>(
        probe_keyexpr.getUnderlyingExpression());

    size_t i = 0;
    for (const auto &a : rc->getAtts()) {
      auto e = a.getExpression();
      if (e.isRegistered()) {
        Value *d = Builder->CreateExtractValue(keyWrapper.value, i);

        allJoinBindings[e.getRegisteredAs()] =
            context->toMem(d, context->createFalse());
      }
      ++i;
    }
  }

  if (build_keyexpr.isRegistered()) {
    allJoinBindings[build_keyexpr.getRegisteredAs()] =
        context->toMem(build_key, context->createFalse());
  }

  if (build_keyexpr.getExpressionType()->getTypeID() == RECORD) {
    auto rc = dynamic_cast<const expressions::RecordConstruction *>(
        build_keyexpr.getUnderlyingExpression());

    size_t i = 0;
    for (const auto &a : rc->getAtts()) {
      auto e = a.getExpression();
      if (e.isRegistered()) {
        Value *d = Builder->CreateExtractValue(build_key, i);

        allJoinBindings[e.getRegisteredAs()] =
            context->toMem(d, context->createFalse());
      }
      ++i;
    }
  }

  // from probe side
  for (const GpuMatExpr &mexpr : probe_mat_exprs) {
    if (mexpr.packet == 0 && mexpr.packind == 0) continue;

    // set activeLoop for build rel if not set (may be multiple ones!)
    {  // NOTE: Is there a better way ?
      Catalog &catalog = Catalog::getInstance();
      string probeRel = mexpr.expr.getRegisteredRelName();
      std::shared_ptr<Plugin> pg = catalog.getPlugin(probeRel);
      assert(pg);

      RecordAttribute probe_oid(probeRel, activeLoop, pg->getOIDType());

      if (allJoinBindings.count(probe_oid) == 0) {
        auto pr_oid_type = dynamic_cast<PrimitiveType *>(pg->getOIDType());
        if (!pr_oid_type) {
          string error_msg(
              "[ProbeHashJoinChained: ] Only primitive OIDs are supported.");
          LOG(ERROR) << error_msg;
          throw runtime_error(error_msg);
        }

        llvm::Type *llvm_oid_type = pr_oid_type->getLLVMType(llvmContext);

        allJoinBindings[probe_oid] = context->toMem(
            UndefValue::get(llvm_oid_type), context->createFalse());
      }
    }

    if (mexpr.expr.getTypeID() !=
            expressions::ExpressionId::RECORD_PROJECTION ||
        !(mexpr.expr.getRegisteredAs() ==
          dynamic_cast<const expressions::RecordProjection *>(
              mexpr.expr.getUnderlyingExpression())
              ->getAttribute())) {
      ExpressionGeneratorVisitor exprGenerator(context, childState);
      ProteusValue val = mexpr.expr.accept(exprGenerator);

      allJoinBindings[mexpr.expr.getRegisteredAs()] =
          context->toMem(val.value, val.isNull);
    } else {
      allJoinBindings[mexpr.expr.getRegisteredAs()] =
          childState[mexpr.expr.getRegisteredAs()];
    }
  }

  // from build side
  for (const GpuMatExpr &mexpr : build_mat_exprs) {
    if (mexpr.packet == 0 && mexpr.packind == 0) continue;

    // set activeLoop for build rel if not set (may be multiple ones!)
    {  // NOTE: Is there a better way ?
      Catalog &catalog = Catalog::getInstance();
      string buildRel = mexpr.expr.getRegisteredRelName();
      std::shared_ptr<Plugin> pg = catalog.getPlugin(buildRel);
      assert(pg);
      RecordAttribute build_oid{buildRel, activeLoop, pg->getOIDType()};

      if (allJoinBindings.count(build_oid) == 0) {
        auto pr_oid_type = dynamic_cast<PrimitiveType *>(pg->getOIDType());
        if (!pr_oid_type) {
          string error_msg(
              "[ProbeHashJoinChained: ] Only primitive OIDs are supported.");
          LOG(ERROR) << error_msg;
          throw runtime_error(error_msg);
        }

        llvm::Type *llvm_oid_type = pr_oid_type->getLLVMType(llvmContext);

        allJoinBindings[build_oid] = context->toMem(
            UndefValue::get(llvm_oid_type), context->createFalse());
      }
    }

    Value *val =
        Builder->CreateExtractValue(in_vals[mexpr.packet], mexpr.packind);

    allJoinBindings[mexpr.expr.getRegisteredAs()] =
        context->toMem(val, context->createFalse());
  }
  // Triggering parent
  OperatorState newState{*this, allJoinBindings};
  getParent()->consume(context, newState);

  bool fk = false;
  Builder->CreateBr((fk) ? MergeBB : CondBB);

  Builder->SetInsertPoint(MergeBB);
}

/**
 * Pipeline open handler that sets state variables using the shared build hash
 * table.
 *
 * This method is called at runtime (execution phase) to connect the probe
 * pipeline with the shared hash table memory from the build phase.
 */
void ProbeHashJoinChained::open_probe(Pipeline *pip) {
  // Wait for the build state to be ready if necessary
  if (!buildState->build_complete.load()) {
    while (!buildState->build_complete.load()) {
      // Simple spin wait
      std::this_thread::yield();
    }
  }

  // Verify that the shared state has valid data
  CHECK(buildState->head_array)
      << "Invalid head array pointer in shared build state";
  CHECK_GE(buildState->data_arrays.size(), build_packet_widths.size())
      << "Not enough data arrays in shared build state";

  // Set state variables using values from the shared build state
  pip->setStateVar(probe_head_param_id, buildState->head_array);

  for (size_t i = 0; i < build_packet_widths.size(); ++i) {
    CHECK(i < in_param_ids.size() && i < buildState->data_arrays.size())
        << "Index out of bounds when setting state variables for probe phase";
    pip->setStateVar(in_param_ids[i], buildState->data_arrays[i]);
  }
}

void ProbeHashJoinChained::close_probe(Pipeline *pip) {
  // We don't free any memory here as it's managed by the original
  // HashJoinChained
}
