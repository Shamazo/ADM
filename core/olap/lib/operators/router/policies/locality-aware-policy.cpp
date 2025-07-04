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

#include <lib/operators/operators.hpp>
#include <olap/expressions/expressions.hpp>
#include <olap/routing/affinitizers.hpp>
#include <platform/topology/topology.hpp>
#include <random>

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

/**
 * With a single consumer, try to route to the closest NUMA node. On retry, use
 * a random NUMA node from the consumer's accessible domains. With multiple
 * consumers, randomly select a consumer and then route to the closest NUMA node
 * for that consumer. On retry, use the same consumer and a random NUMA node
 * from its accessible domains.
 */
class LocalityAwarePolicy : public RoutingPolicyV2 {
 private:
  std::vector<Affinitizer*> consumer_affs_;  // Affinitizers per consumer
  std::vector<size_t> consumer_offsets_;     // Base queue offset per consumer
  std::vector<DeviceType> device_types_;     // Device type per consumer
  std::vector<std::vector<size_t>>
      consumer_cu_domains_;     // Accessible CUs per consumer
  size_t system_cu_count_ = 0;  // Total CUs in system
  mutable std::atomic<uint64_t> random_state_{0};
  bool initialized_ = false;  // Track initialization state
  static constexpr size_t statsArraySize = 100;
  std::array<uint32_t, statsArraySize>* stats_array =
      nullptr;  // state managed by grouter

 public:
  size_t getStateSize() const override {
    return statsArraySize * sizeof(uint32_t);
  }
  void initializeState(void* state) override {
    std::array<uint32_t, statsArraySize>* stats_array_ =
        reinterpret_cast<std::array<uint32_t, statsArraySize>*>(state);
    stats_array = stats_array_;
    for (size_t i = 0; i < statsArraySize; ++i) {
      (*stats_array_)[i] = 0;  // Initialize all stats to zero
    }
  }
  void cleanupState(void* state) override {
    size_t num_used_queues = system_cu_count_ * consumer_affs_.size();
    // Print indices on the first line
    std::stringstream line1;
    line1 << "idx:    ";
    for (size_t i = 0; i < num_used_queues; ++i) {
      line1 << std::setw(6) << i << ", ";
    }
    LOG(INFO) << line1.str();

    // Print counts on the second line
    std::stringstream line2;
    line2 << "count:  ";
    for (size_t i = 0; i < num_used_queues; ++i) {
      line2 << std::setw(6) << (*stats_array)[i] << ", ";
    }
    LOG(INFO) << line2.str();
    stats_array = nullptr;
  }

  // Constructor that accepts variant configuration
  explicit LocalityAwarePolicy(const PolicyConfigVariant& config)
      : random_state_{0} {
    // Locality-aware expects LocalityAwarePolicyConfig
    CHECK(std::holds_alternative<LocalityAwarePolicyConfig>(config))
        << "LocalityAwarePolicy requires LocalityAwarePolicyConfig";

    const auto& locality_config = std::get<LocalityAwarePolicyConfig>(config);
    initializeFromConfig(locality_config);
    initialized_ = true;
  }

 private:
  void initializeFromConfig(const LocalityAwarePolicyConfig& config) {
    // Validate configuration
    config.validate();

    // Copy configuration data
    consumer_affs_ = config.consumer_affinitizers;
    device_types_ = config.consumer_device_types;

    // Get topology information
    const auto& topo = topology::getInstance();
    system_cu_count_ = topo.getCpuNumaNodeCount() + topo.getGpuCount();

    DCHECK_LT(statsArraySize, system_cu_count_ * consumer_affs_.size());

    // Calculate consumer offsets and domains
    for (size_t i = 0; i < consumer_affs_.size(); i++) {
      // Base offset for this consumer's queues
      size_t offset = system_cu_count_ * i;
      consumer_offsets_.push_back(offset);

      // Get accessible CUs for this consumer
      consumer_cu_domains_.push_back(consumer_affs_[i]->getCUIndexDomain());
    }

    LOG(INFO) << "LocalityAwarePolicy initialized with "
              << consumer_affs_.size()
              << " consumers, system_cu_count=" << system_cu_count_;
  }

  PolicyDataRequirements getDataRequirements() const override {
    // Need the data pointer for NUMA detection
    return {{{PolicyDataRequirements::FieldSource::FIRST_DATA_FIELD, "void_ptr",
              0, sizeof(void*)}},
            sizeof(void*)};
  }

  RoutingDecision getTargetChannel(
      const GeneralizedRouter* router, const RoutingContext& context,
      int num_total_consumers, int retry_count,
      const std::vector<int>& failed_channels) override {
    // Check initialization
    CHECK(initialized_) << "LocalityAwarePolicy used before initialization";

    // Validate context
    CHECK_GE(context.keys_payload_size, sizeof(void*))
        << "Invalid keys payload for locality-aware routing";

    size_t target_consumer;
    size_t absolute_cu_idx;

    if (retry_count == 0) {
      // PHASE 1: Locality-aware routing (first attempt)

      // Random consumer selection for load balancing
      uint64_t rand_val = random_state_.fetch_add(1, std::memory_order_relaxed);
      rand_val = hash_combine(rand_val);  // Simple hash for better distribution
      target_consumer = rand_val % consumer_affs_.size();

      // Extract data pointer
      void* data_ptr = *static_cast<void* const*>(context.keys_payload);

      // Get local NUMA node for this data
      // This returns the actual NUMA node index where the data resides
      size_t detected_cu_idx = random_local_cu_index_in_topo(
          data_ptr, consumer_affs_[target_consumer]);

      // Find this CU in the consumer's accessible domain
      const auto& cu_domain = consumer_cu_domains_[target_consumer];
      auto it = std::find(cu_domain.begin(), cu_domain.end(), detected_cu_idx);

      if (it != cu_domain.end()) {
        // Data is on an accessible CU, use it directly
        absolute_cu_idx = detected_cu_idx;
      } else {
        // Data is not on an accessible CU, use the first accessible CU as
        // fallback In a real implementation, this would find the closest
        // accessible CU
        absolute_cu_idx = cu_domain[0];
      }
    } else {
      // PHASE 2: Random selection from consumer's accessible CUs (retry)

      if (!failed_channels.empty()) {
        // Determine which consumer owns the failed channel
        int failed_queue = failed_channels.back();
        target_consumer = failed_queue / system_cu_count_;
      } else {
        // Fallback: random consumer if no failed channels info
        uint64_t rand_val =
            random_state_.fetch_add(1, std::memory_order_relaxed);
        rand_val = hash_combine(rand_val);
        target_consumer = rand_val % consumer_affs_.size();
      }

      // Get the accessible CUs for this consumer
      const auto& cu_domain = consumer_cu_domains_[target_consumer];
      CHECK(!cu_domain.empty())
          << "Consumer " << target_consumer << " has no accessible CUs";

      // Random selection within consumer's domain
      uint64_t rand_val2 =
          random_state_.fetch_add(1, std::memory_order_relaxed);
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

  void onChannelBackPressure(int channel) override {
    // Future: Track back pressure per NUMA node for adaptive behavior
  }
  void onTupleRouted(int channel, bool success) override {
    (*stats_array)[channel] += 1;
  }

  // Queue Management Interface Implementation
  QueueConfiguration getQueueConfiguration(
      size_t num_consumers) const override {
    CHECK(initialized_) << "LocalityAwarePolicy used before initialization";
    CHECK_GT(num_consumers, 0)
        << "Locality-aware requires at least one consumer";
    CHECK_GT(system_cu_count_, 0) << "Invalid system CU count";

    return {
        .total_queues = num_consumers * system_cu_count_,
        .queues_per_consumer = system_cu_count_,
        .shared_free_pools = false  // Each queue has its own pool
    };
  }

  size_t getFreepoolForQueue(size_t queue_index) const override {
    return queue_index;  // 1:1 mapping for locality
  }

  size_t getQueueOffsetForConsumer(size_t consumer_index) const override {
    CHECK_LT(consumer_index, consumer_offsets_.size())
        << "Consumer index " << consumer_index << " out of range";
    return consumer_offsets_[consumer_index];
  }

  std::vector<int> getLocalQueuesForConsumer(
      size_t consumer_index, const Affinitizer* consumer_aff) const override {
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

  void generateConsumeLogic(OlapParallelContext* context,
                            const OperatorState& childState,
                            llvm::Value* params, llvm::Value* groupId,
                            GeneralizedRouter* router) override {
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

 private:
  // Simple hash combine for better random distribution
  uint64_t hash_combine(uint64_t x) const {
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
  }
};

// Register the locality-aware policy with the factory
REGISTER_ROUTING_POLICY(LocalityAwarePolicy,
                        GeneralizedRoutingPolicyV2::LOCALITY_AWARE)

}  // namespace routing
}  // namespace proteus
