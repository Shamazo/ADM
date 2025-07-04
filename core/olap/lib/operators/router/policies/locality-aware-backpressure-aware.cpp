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

#include <algorithm>
#include <lib/operators/operators.hpp>
#include <olap/routing/affinitizers.hpp>
#include <olap/routing/routing-policy-types-v2.hpp>
#include <platform/topology/topology.hpp>
#include <unordered_set>

#include "lib/operators/router/generalized-router.hpp"
#include "lib/operators/router/routing-policy-factory.hpp"
#include "lib/operators/router/routing-policy-v2.hpp"
#include "locality-aware-policy.hpp"

// External C function for NUMA detection
// Defined in routing-policy.cpp
extern "C" size_t random_local_cu_index_in_topo(void* ptr, Affinitizer* aff);

namespace proteus {
namespace routing {

/**
 * Locality-aware policy that avoids congested consumers on retry.
 * This policy:
 * - Initially routes randomly for load distribution
 * - On retry, excludes consumers that have backpressured queues (the consumer
 * that failed last attempt)
 * - Falls back to random selection among non-congested consumers
 */
class LocalityAwareBackpressureAwarePolicy : public LocalityAwarePolicy {
 public:
  // Constructor that accepts variant configuration
  explicit LocalityAwareBackpressureAwarePolicy(
      const PolicyConfigVariant& config)
      : LocalityAwarePolicy(config) {}

  RoutingDecision getTargetChannel(
      const GeneralizedRouter* router, const RoutingContext& context,
      int num_total_consumers, int retry_count,
      const std::vector<int>& failed_channels) override {
    // Check initialization
    DCHECK(initialized_)
        << "LocalityAwareBackpressureAwarePolicy used before initialization";

    // Validate context
    DCHECK_GE(context.keys_payload_size, sizeof(void*))
        << "Invalid keys payload for locality-aware routing";

    size_t target_consumer;
    size_t absolute_cu_idx;

    // Track total calls and log periodically
    logStatsIfNeeded();

    if (retry_count == 0) {
      // PHASE 1: Random consumer selection for initial load balancing

      uint64_t rand_val = random_state_.fetch_add(1, std::memory_order_relaxed);
      rand_val = hash_combine(rand_val);
      target_consumer = rand_val % consumer_affs_.size();

      // Track initial consumer selection
      trackInitialConsumerSelection(target_consumer);

      // Extract data pointer
      void* data_ptr = *static_cast<void* const*>(context.keys_payload);

      // Get the closest NUMA node for this data
      absolute_cu_idx = random_local_cu_index_in_topo(
          data_ptr, consumer_affs_[target_consumer]);

    } else {
      // PHASE 2+: Avoid congested consumers on retry

      // Build set of congested consumers from RECENT failed channels only
      std::unordered_set<size_t> congested_consumers;

      // Only consider the most recent failure(s) to avoid marking all consumers
      // as congested due to accumulated history
      if (!failed_channels.empty()) {
        // Get the consumer that owns the most recent failed queue
        int most_recent_failed = failed_channels.back();
        size_t recent_consumer = most_recent_failed / system_cu_count_;
        congested_consumers.insert(recent_consumer);

        // Optional: also consider the previous failure if it's from a different
        // consumer
        if (failed_channels.size() > 1) {
          int prev_failed = failed_channels[failed_channels.size() - 2];
          size_t prev_consumer = prev_failed / system_cu_count_;
          if (prev_consumer != recent_consumer) {
            congested_consumers.insert(prev_consumer);
          }
        }
      }

      // Log congestion state periodically
      static std::atomic<uint64_t> retry_log_counter{0};
      if (retry_log_counter.fetch_add(1) % 500 == 0) {
        std::string congested_str = "Retry " + std::to_string(retry_count) +
                                    " - Congested consumers: [";
        for (size_t c : congested_consumers) {
          congested_str += std::to_string(c) + " ";
        }
        congested_str += "] from recent failures. All failed queues: [";
        for (int q : failed_channels) {
          congested_str += std::to_string(q) + " ";
        }
        congested_str += "]";
        LOG(INFO) << congested_str;
      }

      // Build list of non-congested consumers
      std::vector<size_t> available_consumers;
      for (size_t i = 0; i < consumer_affs_.size(); ++i) {
        if (congested_consumers.find(i) == congested_consumers.end()) {
          available_consumers.push_back(i);
        }
      }

      if (available_consumers.empty()) {
        // All consumers are congested - fall back to random selection
        // This shouldn't happen often if system is properly provisioned
        LOG(WARNING) << "All consumers congested, falling back to random "
                        "selection on retry: "
                     << retry_count;
        uint64_t rand_val =
            random_state_.fetch_add(1, std::memory_order_relaxed);
        rand_val = hash_combine(rand_val);
        target_consumer = rand_val % consumer_affs_.size();
      } else {
        // Select randomly from non-congested consumers
        uint64_t rand_val =
            random_state_.fetch_add(1, std::memory_order_relaxed);
        rand_val = hash_combine(rand_val);
        target_consumer =
            available_consumers[rand_val % available_consumers.size()];
      }

      // Track retry consumer selection
      trackRetryConsumerSelection(target_consumer);

      // For retry, prefer random CU selection within consumer's domain
      // to spread load better
      const auto& cu_domain = consumer_cu_domains_[target_consumer];
      CHECK(!cu_domain.empty())
          << "Consumer " << target_consumer << " has no accessible CUs";

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
};

// Register the backpressure-aware locality policy with the factory
REGISTER_ROUTING_POLICY(
    LocalityAwareBackpressureAwarePolicy,
    GeneralizedRoutingPolicyV2::LOCALITY_AWARE_BACKPRESSURE_AWARE)

}  // namespace routing
}  // namespace proteus
