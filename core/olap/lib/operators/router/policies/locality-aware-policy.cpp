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

#include "locality-aware-policy.hpp"

#include <glog/logging.h>

#include <algorithm>
#include <iomanip>
#include <lib/operators/operators.hpp>
#include <olap/routing/affinitizers.hpp>
#include <platform/topology/topology.hpp>
#include <random>
#include <sstream>

#include "lib/operators/router/generalized-router.hpp"
#include "lib/operators/router/routing-policy-factory.hpp"
#include "lib/operators/router/routing-policy-v2.hpp"

// LLVM includes
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Value.h>

// External C function for NUMA detection
// Defined in routing-policy.cpp
extern "C" size_t random_local_cu_index_in_topo(void* ptr, Affinitizer* aff);

namespace proteus {
namespace routing {

size_t LocalityAwarePolicy::getStateSize() const {
  return 0;  // No state needed for LocalityAwarePolicy
}

void LocalityAwarePolicy::initializeState(void* state) {
  stats_.reset();
  LOG(INFO) << "LocalityAwarePolicy: Reset statistics for new query";
}

void LocalityAwarePolicy::cleanupState(void* state) {
  LOG(INFO) << "=== Final Routing Stats for Query ===";
  stats_.log_stats();
  stats_.reset();
}

LocalityAwarePolicy::LocalityAwarePolicy(const PolicyConfigVariant& config)
    : random_state_{0} {
  // Locality-aware expects LocalityAwarePolicyConfig
  CHECK(std::holds_alternative<LocalityAwarePolicyConfig>(config))
      << "LocalityAwarePolicy requires LocalityAwarePolicyConfig";

  const auto& locality_config = std::get<LocalityAwarePolicyConfig>(config);
  initializeFromConfig(locality_config);
  initialized_ = true;
}

void LocalityAwarePolicy::initializeFromConfig(
    const LocalityAwarePolicyConfig& config) {
  // Validate configuration
  config.validate();

  // Copy configuration data
  consumer_affs_ = config.consumer_affinitizers;
  device_types_ = config.consumer_device_types;

  // Get topology information
  const auto& topo = topology::getInstance();
  system_cu_count_ = topo.getCpuNumaNodeCount() + topo.getGpuCount();

  // Calculate consumer offsets and domains
  for (size_t i = 0; i < consumer_affs_.size(); i++) {
    // Base offset for this consumer's queues
    size_t offset = system_cu_count_ * i;
    consumer_offsets_.push_back(offset);

    // Get accessible CUs for this consumer
    consumer_cu_domains_.push_back(consumer_affs_[i]->getCUIndexDomain());
  }

  LOG(INFO) << "LocalityAwarePolicy initialized with " << consumer_affs_.size()
            << " consumers, system_cu_count=" << system_cu_count_;
}

PolicyDataRequirements LocalityAwarePolicy::getDataRequirements() const {
  // Need the data pointer for NUMA detection
  return {{{PolicyDataRequirements::FieldSource::FIRST_DATA_FIELD, "void_ptr",
            0, sizeof(void*)}},
          sizeof(void*)};
}

RoutingDecision LocalityAwarePolicy::getTargetChannel(
    const GeneralizedRouter* router, const RoutingContext& context,
    int num_total_consumers, int retry_count,
    const std::vector<int>& failed_channels) {
  // Check initialization
  DCHECK(initialized_) << "LocalityAwarePolicy used before initialization";

  // Validate context
  DCHECK_GE(context.keys_payload_size, sizeof(void*))
      << "Invalid keys payload for locality-aware routing";

  size_t target_consumer;
  size_t absolute_cu_idx;

  // Track total calls and log periodically
  logStatsIfNeeded();

  if (retry_count == 0) {
    // PHASE 1: Locality-aware routing (first attempt)

    // Random consumer selection for load balancing
    uint64_t rand_val = random_state_;
    random_state_ += 1;
    rand_val = hash_combine(rand_val);  // Simple hash for better distribution
    target_consumer = rand_val % consumer_affs_.size();

    // Track initial consumer selection
    trackInitialConsumerSelection(target_consumer);

    // Extract data pointer
    void* data_ptr = *static_cast<void* const*>(context.keys_payload);

    // Get the closest NUMA node for this data
    absolute_cu_idx = random_local_cu_index_in_topo(
        data_ptr, consumer_affs_[target_consumer]);

#ifndef NDEBUG
    // Find this CU in the consumer's accessible domain
    const auto& cu_domain = consumer_cu_domains_[target_consumer];
    auto it = std::find(cu_domain.begin(), cu_domain.end(), absolute_cu_idx);
    DCHECK(it != cu_domain.end()) << "affinitizer returned a CU "
                                      "not in the consumer's accessible domain";
#endif
    // TODO create new random round robin policy without the last case
  } else if (retry_count == 1) {
    // PHASE 2: Random selection from consumer's accessible CUs (retry)

    if (!failed_channels.empty()) {
      // Determine which consumer owns the failed channel
      int failed_queue = failed_channels.back();
      target_consumer = failed_queue / system_cu_count_;
    } else {
      // Fallback: random consumer if no failed channels info
      uint64_t rand_val = random_state_;
      random_state_ += 1;
      rand_val = hash_combine(rand_val);
      target_consumer = rand_val % consumer_affs_.size();
    }

    // Track retry consumer selection
    trackRetryConsumerSelection(target_consumer);

    // Get the accessible CUs for this consumer
    const auto& cu_domain = consumer_cu_domains_[target_consumer];
    CHECK(!cu_domain.empty())
        << "Consumer " << target_consumer << " has no accessible CUs";

    // Random selection within consumer's domain
    uint64_t rand_val2 = random_state_;
    random_state_ += 1;
    rand_val2 = hash_combine(rand_val2);
    size_t random_idx = rand_val2 % cu_domain.size();
    absolute_cu_idx = cu_domain[random_idx];
  } else {
    // PHASE 3: Random selection (retry 2+)
    // Random consumer
    uint64_t rand_val = random_state_;
    random_state_ += 1;
    rand_val = hash_combine(rand_val);
      target_consumer = rand_val % consumer_affs_.size();

    // Track retry consumer selection
    trackRetryConsumerSelection(target_consumer);

    // Get the accessible CUs for this consumer
    const auto& cu_domain = consumer_cu_domains_[target_consumer];
    CHECK(!cu_domain.empty())
        << "Consumer " << target_consumer << " has no accessible CUs";

    // Random selection within consumer's domain
    uint64_t rand_val2 = random_state_;
    random_state_ += 1;
    rand_val2 = hash_combine(rand_val2);
    size_t random_idx = rand_val2 % cu_domain.size();
    absolute_cu_idx = cu_domain[random_idx];
  }

  // Calculate final queue index
  int target_queue = consumer_offsets_[target_consumer] + absolute_cu_idx;

  // Locality routing supports retry for congestion handling
  // Use queue-specific free pool for NUMA locality
  return {target_queue, true, static_cast<size_t>(target_queue)};
}

void LocalityAwarePolicy::onChannelBackPressure(int channel) {
  stats_.queue_failures[channel] += 1;
}

void LocalityAwarePolicy::onTupleRouted(int channel, bool success) {
  if (success) {
    int consumer_idx = channel / system_cu_count_;
    stats_.successful_routes[consumer_idx] += 1;
    stats_.queue_successes[channel] += 1;
  }
}

void LocalityAwarePolicy::trackInitialConsumerSelection(
    size_t consumer_idx) const {
  stats_.initial_consumer_attempts[consumer_idx] += 1;
}

void LocalityAwarePolicy::trackRetryConsumerSelection(
    size_t consumer_idx) const {
  stats_.consumer_selected_on_retry[consumer_idx] += 1;
}

void LocalityAwarePolicy::logStatsIfNeeded() const {
  stats_.total_calls += 1;
  if (stats_.total_calls % 6000 == 0) {
    stats_.log_stats();
  }
}

QueueConfiguration LocalityAwarePolicy::getQueueConfiguration(
    size_t num_consumers) const {
  CHECK(initialized_) << "LocalityAwarePolicy used before initialization";
  CHECK_GT(num_consumers, 0) << "Locality-aware requires at least one consumer";
  CHECK_GT(system_cu_count_, 0) << "Invalid system CU count";

  return {
      .total_queues = num_consumers * system_cu_count_,
      .queues_per_consumer = system_cu_count_,
      .shared_free_pools = false  // Each queue has its own pool
  };
}

size_t LocalityAwarePolicy::getFreepoolForQueue(size_t queue_index) const {
  return queue_index;  // 1:1 mapping for locality
}

size_t LocalityAwarePolicy::getQueueOffsetForConsumer(
    size_t consumer_index) const {
  CHECK_LT(consumer_index, consumer_offsets_.size())
      << "Consumer index " << consumer_index << " out of range";
  return consumer_offsets_[consumer_index];
}

std::vector<int> LocalityAwarePolicy::getLocalQueuesForConsumer(
    size_t consumer_index, const Affinitizer* consumer_aff) const {
  CHECK_NOTNULL(consumer_aff);
  CHECK_LT(consumer_index, consumer_cu_domains_.size())
      << "Consumer index " << consumer_index << " out of range";

  // Return the CU indices this consumer can access
  std::vector<int> local_queues;
  const auto& cu_domain = consumer_cu_domains_[consumer_index];
  CHECK(!cu_domain.empty())
      << "Consumer " << consumer_index << " has no accessible CUs";

  for (size_t cu_idx : cu_domain) {
    local_queues.push_back(static_cast<int>(cu_idx));
  }
  return local_queues;
}

void LocalityAwarePolicy::generateConsumeLogic(OlapParallelContext* context,
                                               const OperatorState& childState,
                                               llvm::Value* params,
                                               llvm::Value* groupId,
                                               GeneralizedRouter* router) {
  llvm::LLVMContext& llvmContext = context->getLLVMContext();
  llvm::IRBuilder<>* Builder = context->getBuilder();

  // Extract keys for locality-aware routing
  llvm::Value* keys_ptr;
  llvm::Value* keys_size;
  router->generateDynamicKeyExtraction(context, childState, keys_ptr,
                                       keys_size);

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

uint64_t LocalityAwarePolicy::hash_combine(uint64_t x) const {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33;
  return x;
}

// Register the locality-aware policy with the factory
REGISTER_ROUTING_POLICY(LocalityAwarePolicy,
                        GeneralizedRoutingPolicyV2::LOCALITY_AWARE)

}  // namespace routing
}  // namespace proteus
