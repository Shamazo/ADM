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

#include <glog/logging.h>

#include <atomic>
#include <unordered_map>

#include "lib/operators/operators.hpp"
#include "lib/operators/router/generalized-router.hpp"
#include "lib/operators/router/routing-policy-factory.hpp"
#include "lib/operators/router/routing-policy-v2.hpp"

// LLVM includes
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Value.h>

namespace proteus::routing {

// Define the state structure
struct RoundRobinState {
  std::atomic<uint64_t> routing_counter{0};
};

/**
 * Simple round-robin routing policy implementation.
 * Distributes tuples evenly across all consumer channels using an atomic
 * counter. This policy requires no key extraction from tuples.
 */
class RoundRobinPolicy : public RoutingPolicyV2 {
 public:
  // Constructor that accepts variant configuration
  explicit RoundRobinPolicy(const PolicyConfigVariant& config) {
    // Round-robin expects no configuration (std::monostate)
    CHECK(std::holds_alternative<std::monostate>(config))
        << "RoundRobinPolicy expects no configuration";
  }
  size_t getStateSize() const override { return sizeof(RoundRobinState); }

  void initializeState(void* state) override {
    new (state) RoundRobinState();
  }

  void cleanupState(void* state) override {
    // Call destructor explicitly (though for POD types this is a no-op)
    static_cast<RoundRobinState*>(state)->~RoundRobinState();
  }

  PolicyDataRequirements getDataRequirements() const override {
    // Round-robin needs no fields from tuples
    return {{}, 0};
  }

  RoutingDecision getTargetChannel(const GeneralizedRouter* router,
                                   const RoutingContext& context,
      int num_total_consumers, int retry_count,
      const std::vector<int>& failed_channels) override {
    CHECK(state_) << "RoundRobinPolicy: State not initialized";
    CHECK_GT(num_total_consumers, 0)
        << "RoundRobinPolicy: Invalid consumer count: " << num_total_consumers;

    auto* rr_state = static_cast<RoundRobinState*>(state_);

    // Round-robin ignores retry_count - always uses same logic
    // Use policy state for routing
    int channel =
        rr_state->routing_counter.fetch_add(1, std::memory_order_relaxed) %
        num_total_consumers;

    // Log periodically using routing counter
    uint64_t current_counter =
        rr_state->routing_counter.load(std::memory_order_relaxed);

    // No retry support for round-robin - use blocking buffer acquisition
    // All queues use shared free pool 0
    return {channel, false, 0};
  }

  // Queue Management Interface Implementation
  QueueConfiguration getQueueConfiguration(
      size_t num_consumers) const override {
    CHECK_GT(num_consumers, 0) << "Round-robin requires at least one consumer";
    return {
        .total_queues = num_consumers,
        .queues_per_consumer = 1,
        .shared_free_pools = true  // Round-robin shares pool 0
    };
  }

  size_t getFreepoolForQueue(size_t queue_index) const override {
    return 0;  // All queues share free pool 0
  }

  size_t getQueueOffsetForConsumer(size_t consumer_index) const override {
    return consumer_index;  // One queue per consumer
  }

  std::vector<int> getLocalQueuesForConsumer(
      size_t consumer_index, const Affinitizer* consumer_aff) const override {
    CHECK_NOTNULL(consumer_aff);
    return {0};  // Each consumer handles only their single queue
  }

  void generateConsumeLogic(OlapParallelContext* context,
                            const OperatorState& childState,
                            llvm::Value* params, llvm::Value* groupId,
                            GeneralizedRouter* router) override {
    llvm::LLVMContext& llvmContext = context->getLLVMContext();
    llvm::IRBuilder<>* Builder = context->getBuilder();

    // For round-robin, we don't need key extraction - pass null/0
    llvm::Value* keys_ptr =
        llvm::ConstantPointerNull::get(llvm::Type::getInt8PtrTy(llvmContext));
    llvm::Value* keys_size = context->createInt64(0);

    // Create full payload - need to store params struct and get pointer to it
    llvm::AllocaInst* params_alloca = Builder->CreateAlloca(params->getType());
    Builder->CreateStore(params, params_alloca);

    // Cast the pointer to i8* for FFI
    llvm::PointerType* charPtrType = llvm::Type::getInt8PtrTy(llvmContext);
    llvm::Value* full_payload =
        Builder->CreateBitCast(params_alloca, charPtrType);
    llvm::Value* full_payload_size =
        context->createInt64(router->get_cpp_buf_size());

    // Create router pointer for FFI
    llvm::Value* exchangePtr = llvm::ConstantInt::get(
        llvmContext, llvm::APInt(64, reinterpret_cast<uint64_t>(router)));
    llvm::Value* exchange = Builder->CreateIntToPtr(exchangePtr, charPtrType);

    context->gen_call(proteus::route_and_enqueue_via_cpp,
                      {exchange, keys_ptr, keys_size, full_payload,
                       full_payload_size, groupId});
  }

  // Helper methods for FFI access
  RoundRobinState* getState() const {
    return static_cast<RoundRobinState*>(state_);
  }

  void onTupleRouted(int channel, bool success) override {
    if (!success) {
      LOG(WARNING) << "RoundRobinPolicy: Failed to route to channel "
                   << channel;
    }
  }
};

// Register the round-robin policy with the factory
REGISTER_ROUTING_POLICY(RoundRobinPolicy,
                        GeneralizedRoutingPolicyV2::ROUND_ROBIN)

}  // namespace proteus::routing
