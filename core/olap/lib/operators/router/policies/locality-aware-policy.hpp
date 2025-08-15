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

#ifndef PROTEUS_LOCALITY_AWARE_POLICY_HPP
#define PROTEUS_LOCALITY_AWARE_POLICY_HPP

#include <glog/logging.h>

#include <array>
#include <atomic>
#include <vector>

#include "lib/operators/router/routing-policy-v2.hpp"

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
 public:
  // Constructor that accepts variant configuration
  explicit LocalityAwarePolicy(const PolicyConfigVariant& config);

  // RoutingPolicyV2 interface
  size_t getStateSize() const override;
  void initializeState(void* state) override;
  void cleanupState(void* state) override;
  PolicyDataRequirements getDataRequirements() const override;
  RoutingDecision getTargetChannel(
      const GeneralizedRouter* router, const RoutingContext& context,
      int num_total_consumers, int retry_count,
      const std::vector<int>& failed_channels) override;
  void onChannelBackPressure(int channel) override;
  void onTupleRouted(int channel, bool success) override;

  // Queue Management Interface Implementation
  QueueConfiguration getQueueConfiguration(size_t num_consumers) const override;
  size_t getFreepoolForQueue(size_t queue_index) const override;
  size_t getQueueOffsetForConsumer(size_t consumer_index) const override;
  std::vector<int> getLocalQueuesForConsumer(
      size_t consumer_index, const Affinitizer* consumer_aff) const override;
  void generateConsumeLogic(OlapParallelContext* context,
                            const OperatorState& childState,
                            llvm::Value* params, llvm::Value* groupId,
                            GeneralizedRouter* router) override;

 protected:
  void initializeFromConfig(const LocalityAwarePolicyConfig& config);
  uint64_t hash_combine(uint64_t x) const;

  // Statistics tracking - shared by all LocalityAware policies
  struct RoutingStats {
    // all hardcoded to 3 consumers and 8 NUMA nodes + 1 GPU
    std::array<uint64_t, 3> initial_consumer_attempts = {0, 0, 0};
    std::array<uint64_t, 3> successful_routes = {0, 0, 0};
    std::array<uint64_t, 27> queue_failures = {0};
    std::array<uint64_t, 27> queue_successes = {0};
    std::array<uint64_t, 3> consumer_selected_on_retry = {0, 0, 0};
    uint64_t total_calls = 0;
    uint64_t last_log_call = 0;

    void reset() {
      for (auto& v : initial_consumer_attempts) v = 0;
      for (auto& v : successful_routes) v = 0;
      for (auto& v : queue_failures) v = 0;
      for (auto& v : queue_successes) v = 0;
      for (auto& v : consumer_selected_on_retry) v = 0;
      total_calls = 0;
      last_log_call = 0;
    }

    void log_stats() const {
      LOG(INFO) << "=== Routing Stats ===";
      LOG(INFO) << "Total routing calls: " << total_calls;
      LOG(INFO) << "Initial attempts - Direct: " << initial_consumer_attempts[0]
                << ", Staging: " << initial_consumer_attempts[1]
                << ", Pushdown: " << initial_consumer_attempts[2];
      LOG(INFO) << "Successful routes - Direct: " << successful_routes[0]
                << ", Staging: " << successful_routes[1]
                << ", Pushdown: " << successful_routes[2];

      LOG(INFO) << "Queue failures:";
      LOG(INFO) << "  Direct(0-7): " << queue_failures[0] << ","
                << queue_failures[1] << "," << queue_failures[2] << ","
                << queue_failures[3] << "," << queue_failures[4] << ","
                << queue_failures[5] << "," << queue_failures[6] << ","
                << queue_failures[7];
      LOG(INFO) << "  Staging(9-12): " << queue_failures[9] << ","
                << queue_failures[10] << "," << queue_failures[11] << ","
                << queue_failures[12] << "," << queue_failures[13];
      LOG(INFO) << "  Pushdown(22-25): " << queue_failures[22] << ","
                << queue_failures[23] << "," << queue_failures[24] << ","
                << queue_failures[25] << "," << queue_failures[26];

      LOG(INFO) << "Queue successes:";
      LOG(INFO) << "  Direct(0-7): " << queue_successes[0] << ","
                << queue_successes[1] << "," << queue_successes[2] << ","
                << queue_successes[3] << "," << queue_successes[4] << ","
                << queue_successes[5] << "," << queue_successes[6] << ","
                << queue_successes[7];
      LOG(INFO) << "  Staging(9-13): " << queue_successes[9] << ","
                << queue_successes[10] << "," << queue_successes[11] << ","
                << queue_successes[12] << "," << queue_successes[13];
      LOG(INFO) << "  Pushdown(22-25): " << queue_successes[22] << ","
                << queue_successes[23] << "," << queue_successes[24] << ","
                << queue_successes[25] << "," << queue_successes[26];

      LOG(INFO) << "Retries selected - Direct: "
                << consumer_selected_on_retry[0]
                << ", Staging: " << consumer_selected_on_retry[1]
                << ", Pushdown: " << consumer_selected_on_retry[2];
    }
  };

  // Helper methods for statistics tracking
  void trackInitialConsumerSelection(size_t consumer_idx) const;
  void trackRetryConsumerSelection(size_t consumer_idx) const;
  void logStatsIfNeeded() const;

  std::vector<Affinitizer*> consumer_affs_;  // Affinitizers per consumer
  std::vector<size_t> consumer_offsets_;     // Base queue offset per consumer
  std::vector<DeviceType> device_types_;     // Device type per consumer
  std::vector<std::vector<size_t>>
      consumer_cu_domains_;     // Accessible CUs per consumer
  size_t system_cu_count_ = 0;  // Total CUs in system
  mutable uint64_t random_state_{0};
  bool initialized_ = false;    // Track initialization state
  mutable RoutingStats stats_;  // Statistics tracking
  // LocalityAwareState* state_ptr =
  //     nullptr;  // state managed by grouter. non-owning ptr
};

}  // namespace routing
}  // namespace proteus

#endif  // PROTEUS_LOCALITY_AWARE_POLICY_HPP
