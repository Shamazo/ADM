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

#ifndef PROTEUS_ROUTING_POLICY_V2_HPP
#define PROTEUS_ROUTING_POLICY_V2_HPP

#include <cstdlib>
#include <map>
#include <memory>
#include <olap/util/parallel-context.hpp>
#include <platform/topology/device-types.hpp>
#include <string>
#include <variant>
#include <vector>

// Forward declarations
class Affinitizer;
class OperatorState;

namespace proteus {
class GeneralizedRouter;
}

// LLVM forward declarations
namespace llvm {
class Value;
}

namespace proteus::routing {

/**
 * Minimal data requirements structure for routing policies.
 * Describes what data fields a policy needs extracted from tuples.
 */
struct PolicyDataRequirements {
  enum class FieldSource {
    TUPLE_IDENTIFIER,
    FIRST_DATA_FIELD,
    SOURCE_SERVER,
    CUSTOM_HASH
  };

  struct FieldDescriptor {
    FieldSource source;
    std::string type_name;
    size_t offset;
    size_t size;
  };

  std::vector<FieldDescriptor> required_keys;
  size_t total_size;
};

/**
 * Runtime context passed to routing policies for decision making.
 * Contains extracted key data and current system state.
 */
struct RoutingContext {
  const void* keys_payload;  // Extracted key data based on policy requirements
  size_t keys_payload_size;  // Size of keys_payload in bytes
  uint64_t
      current_timestamp;  // Current system timestamp for time-based policies
  uint64_t tuple_count;   // Total tuples processed by this router instance
  std::map<int, size_t>
      queue_depths;  // Current queue depth per consumer channel
  std::vector<double>
      consumer_throughput;  // Throughput (tuples/sec) per consumer
};

/**
 * Result of a routing policy decision.
 * Specifies target channel and retry behavior.
 */
struct RoutingDecision {
  int target_channel;
  bool supports_retry;  // false = use blocking, true = use polling
  size_t free_pool_index;  // Which free pool to acquire buffer from
};

// Base configuration type for all policies
struct PolicyConfig {
  virtual ~PolicyConfig() = default;
};

/**
 * Queue configuration structure for routing policies.
 * Describes the queue layout and buffer pool strategy for a policy.
 */
struct QueueConfiguration {
  size_t total_queues;         // Total number of queues needed
  size_t queues_per_consumer;  // Queues per consumer (for validation)
  bool shared_free_pools;      // true = consumers share free pools, false =
                               // dedicated pools
};

// Configuration for locality-aware policy
struct LocalityAwarePolicyConfig : PolicyConfig {
  std::vector<Affinitizer*> consumer_affinitizers;
  std::vector<DeviceType> consumer_device_types;

  // Validation in the struct
  void validate() const {
    CHECK(!consumer_affinitizers.empty())
        << "LocalityAwarePolicy requires consumer affinitizers";
    CHECK_EQ(consumer_affinitizers.size(), consumer_device_types.size())
        << "Mismatch between affinitizers and device types count";
  }
};

// Configuration for adaptive throughput based policy
struct AdaptiveThroughputBasedConfig : PolicyConfig {
  std::vector<Affinitizer*> consumer_affinitizers;
  std::vector<DeviceType> consumer_device_types;

  // Configurable parameters with defaults
  uint64_t evaluation_batch_size = 350;
  uint64_t monitoring_batch_size = 500;
  double degradation_threshold = 0.2;
  double switch_improvement_threshold = 0.1;
  uint64_t sub_batch_size = 5;  // Record measurements every N tuples
  double decay_factor =
      0.99;  // Exponential decay factor (higher = slower decay)

  // Constructor that loads from environment variables
  AdaptiveThroughputBasedConfig() {
    // Load from environment variables if available
    if (const char* env_eval_batch =
            std::getenv("ADAPTIVE_THROUGHPUT_EVALUATION_BATCH_SIZE")) {
      evaluation_batch_size = std::stoull(env_eval_batch);
    }
    if (const char* env_mon_batch =
            std::getenv("ADAPTIVE_THROUGHPUT_MONITORING_BATCH_SIZE")) {
      monitoring_batch_size = std::stoull(env_mon_batch);
    }
    if (const char* env_deg_thresh =
            std::getenv("ADAPTIVE_THROUGHPUT_DEGRADATION_THRESHOLD")) {
      degradation_threshold = std::stod(env_deg_thresh);
    }
    if (const char* env_switch_thresh =
            std::getenv("ADAPTIVE_THROUGHPUT_SWITCH_IMPROVEMENT_THRESHOLD")) {
      switch_improvement_threshold = std::stod(env_switch_thresh);
    }
    if (const char* env_sub_batch =
            std::getenv("ADAPTIVE_THROUGHPUT_SUB_BATCH_SIZE")) {
      sub_batch_size = std::stoull(env_sub_batch);
    }
    if (const char* env_decay =
            std::getenv("ADAPTIVE_THROUGHPUT_DECAY_FACTOR")) {
      decay_factor = std::stod(env_decay);
    }
  }

  void validate() const {
    CHECK(!consumer_affinitizers.empty())
        << "AdaptiveThroughputBasedPolicy requires consumer affinitizers";
    CHECK_EQ(consumer_affinitizers.size(), consumer_device_types.size())
        << "Mismatch between affinitizers and device types count";
    CHECK_GT(evaluation_batch_size, 0)
        << "Evaluation batch size must be positive";
    CHECK_GT(monitoring_batch_size, 0)
        << "Monitoring batch size must be positive";
    CHECK_GE(degradation_threshold, 0.0)
        << "Degradation threshold must be non-negative";
    CHECK_LE(degradation_threshold, 1.0)
        << "Degradation threshold must be <= 1.0";
    CHECK_GE(switch_improvement_threshold, 0.0)
        << "Switch improvement threshold must be non-negative";
    CHECK_GT(sub_batch_size, 0) << "Sub-batch size must be positive";
    CHECK_GT(decay_factor, 0.0) << "Decay factor must be positive";
    CHECK_LE(decay_factor, 1.0) << "Decay factor must be <= 1.0";
  }
};

/**
 * Variant-based configuration system for routing policies.
 * Type-safe alternative to std::unordered_map<std::string, std::any>
 */
using PolicyConfigVariant =
    std::variant<std::monostate,  // No config needed (e.g., ROUND_ROBIN)
                 LocalityAwarePolicyConfig,     // LOCALITY_AWARE config
                 AdaptiveThroughputBasedConfig  // ADAPTIVE_THROUGHPUT_BASED
                                                // config
                 >;

/**
 * Base interface for V2 routing policies.
 * Policies implement routing decisions in C++ rather than JIT code.
 * Named V2 to avoid conflicts with legacy RoutingPolicy used by router.cpp/hpp.
 */
class RoutingPolicyV2 {
 public:
  virtual ~RoutingPolicyV2() = default;

  // State management interface
  virtual size_t getStateSize() const { return 0; }
  virtual void initializeState(void* state) {}
  virtual void cleanupState(void* state) {}

  /**
   * Returns the data requirements for this policy.
   * Called during router initialization to determine key extraction needs.
   */
  virtual PolicyDataRequirements getDataRequirements() const = 0;

  /**
   * Makes routing decision for a tuple.
   * Called from FFI shim function during tuple processing.
   * @param router The GeneralizedRouter instance (for accessing runtime state)
   * @param context RoutingContext with tuple keys and runtime metadata
   * @param num_total_consumers Total number of consumer pipelines (not threads)
   * @param retry_count Number of previous routing attempts (0 = first attempt)
   * @param failed_channels List of channels that failed in previous attempts
   * @return RoutingDecision with target channel and retry capability
   */
  virtual RoutingDecision getTargetChannel(
      const GeneralizedRouter* router, const RoutingContext& context,
      int num_total_consumers, int retry_count = 0,
      const std::vector<int>& failed_channels = {}) = 0;

  /**
   * Queue Management Interface
   */

  /**
   * Returns the queue configuration for this policy
   */
  virtual QueueConfiguration getQueueConfiguration(
      size_t num_consumers) const = 0;

  /**
   * Maps a queue index to its corresponding free pool index
   * This allows policies to control buffer pool assignment
   */
  virtual size_t getFreepoolForQueue(size_t queue_index) const = 0;

  /**
   * Returns the queue offset for a specific consumer
   * (where consumer's queues start in the global queue array)
   */
  virtual size_t getQueueOffsetForConsumer(size_t consumer_index) const = 0;

  /**
   * Returns the list of local queue indices (relative to consumer's offset)
   * that a consumer worker thread should handle
   */
  virtual std::vector<int> getLocalQueuesForConsumer(
      size_t consumer_index, const Affinitizer* consumer_aff) const = 0;

  /**
   * Generates LLVM IR code for the consume method.
   * This method is called during JIT compilation to generate policy-specific
   * routing logic within GeneralizedRouter::consume.
   *
   * @param context The OLAP parallel context for LLVM code generation
   * @param childState The operator state from the child operator
   * @param params The params value containing tuple data
   * @param groupId The group ID value for the current tuple
   * @param router The GeneralizedRouter instance (for accessing router state)
   */
  virtual void generateConsumeLogic(OlapParallelContext* context,
                                    const OperatorState& childState,
                                    llvm::Value* params, llvm::Value* groupId,
                                    GeneralizedRouter* router) = 0;

  // Optional lifecycle hooks for advanced policies
  virtual void onTupleRouted(int channel, bool success) {}
  virtual void onChannelBackPressure(int channel) {}

 protected:
  // Policies will cast this to their specific state type
  void* state_ = nullptr;

  // Helper to set state pointer (called by router)
  friend class ::proteus::GeneralizedRouter;
  void setState(void* state) { state_ = state; }
};

}  // namespace proteus::routing

#endif  // PROTEUS_ROUTING_POLICY_V2_HPP
