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
#include <platform/topology/topology.hpp>

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
 * Locality-aware policy variant that uses random consumer selection on retry.
 * Ignores failed_channels information for consumer selection during retries,
 * providing better load balancing when interconnect is not a bottleneck.
 */
class LocalityAwareRetryConsPolicy : public LocalityAwarePolicy {
 public:
  // Constructor that accepts variant configuration
  explicit LocalityAwareRetryConsPolicy(const PolicyConfigVariant& config)
      : LocalityAwarePolicy(config) {}

  RoutingDecision getTargetChannel(
      const GeneralizedRouter* router, const RoutingContext& context,
      int num_total_consumers, int retry_count,
      const std::vector<int>& failed_channels) override {
    // Check initialization
    DCHECK(initialized_)
        << "LocalityAwareRetryConsPolicy used before initialization";

    // Validate context
    DCHECK_GE(context.keys_payload_size, sizeof(void*))
        << "Invalid keys payload for locality-aware routing";

    size_t target_consumer;
    size_t absolute_cu_idx;

    // Track total calls and log periodically
    logStatsIfNeeded();

    if (retry_count == 0) {
      // PHASE 1: Locality-aware routing (first attempt) - same as base class

      // Random consumer selection for load balancing
      uint64_t rand_val = random_state_.fetch_add(1, std::memory_order_relaxed);
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
      auto it = std::find(cu_domain.begin(), cu_domain.end(), detected_cu_idx);
      DCHECK_NE(it, cu_domain.end())
          << "affinitizer returned a CU "
             "not in the consumer's accessible domain";
#endif
    } else if (retry_count == 1) {
      // PHASE 2: Random consumer selection (retry) - DIFFERENT from base class
      // Always use random consumer, completely ignore failed_channels, but try
      // to get a local channel

      uint64_t rand_val = random_state_.fetch_add(1, std::memory_order_relaxed);
      rand_val = hash_combine(rand_val);
      target_consumer = rand_val % consumer_affs_.size();

      // Track retry consumer selection
      trackRetryConsumerSelection(target_consumer);
      void* data_ptr = *static_cast<void* const*>(context.keys_payload);
      // Get the closest NUMA node for this data
      absolute_cu_idx = random_local_cu_index_in_topo(
          data_ptr, consumer_affs_[target_consumer]);

#ifndef NDEBUG
      // Find this CU in the consumer's accessible domain
      const auto& cu_domain = consumer_cu_domains_[target_consumer];
      auto it = std::find(cu_domain.begin(), cu_domain.end(), detected_cu_idx);
      DCHECK_NE(it, cu_domain.end())
          << "affinitizer returned a CU "
             "not in the consumer's accessible domain";
#endif
    } else {
      // PHASE 3: Random consumer selection (retry) - DIFFERENT from base class
      // Always use random consumer, completely ignore failed_channels, use any
      // NUMA node

      uint64_t rand_val = random_state_.fetch_add(1, std::memory_order_relaxed);
      rand_val = hash_combine(rand_val);
      target_consumer = rand_val % consumer_affs_.size();

      // Track retry consumer selection
      trackRetryConsumerSelection(target_consumer);

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
};

// Register the locality-aware retry consumer policy with the factory
REGISTER_ROUTING_POLICY(
    LocalityAwareRetryConsPolicy,
    GeneralizedRoutingPolicyV2::LOCALITY_AWARE_WITH_RANDOM_CONS_RETRY)

}  // namespace routing
}  // namespace proteus
