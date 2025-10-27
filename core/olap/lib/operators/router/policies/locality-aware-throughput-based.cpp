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

#include "locality-aware-throughput-based.hpp"

#include <glog/logging.h>
#include <platform/util/tracing.hpp>
#include <algorithm>
#include <iomanip>
#include <magic_enum.hpp>

#include "lib/operators/router/routing-policy-factory.hpp"
#include "lib/operators/router/routing-policy-v2.hpp"

// External C function for NUMA detection
// Defined in routing-policy.cpp
extern "C" size_t random_local_cu_index_in_topo(void* ptr, Affinitizer* aff);

namespace proteus {
namespace routing {

PolicyConfigVariant extractLocalityConfig(
  const PolicyConfigVariant& config) {
  CHECK(std::holds_alternative<AdaptiveThroughputBasedConfig>(config))
      << "extractLocalityConfig expects AdaptiveThroughputBasedConfig";

  const auto& adaptive_config = std::get<AdaptiveThroughputBasedConfig>(config);

  // Create locality config from adaptive config
  LocalityAwarePolicyConfig locality_config;
  locality_config.consumer_affinitizers = adaptive_config.consumer_affinitizers;
  locality_config.consumer_device_types = adaptive_config.consumer_device_types;

  // Validate the extracted config
  locality_config.validate();

  return locality_config;
}

LocalityAwareThroughputBasedPolicy::LocalityAwareThroughputBasedPolicy(
    const PolicyConfigVariant& config) : LocalityAwarePolicy(extractLocalityConfig(config)) {
  // Extract the adaptive throughput config
  CHECK(std::holds_alternative<AdaptiveThroughputBasedConfig>(config))
      << "LocalityAwareThroughputBasedPolicy requires AdaptiveThroughputBasedConfig";

  const auto& adaptive_config = std::get<AdaptiveThroughputBasedConfig>(config);
  initializeFromConfig(adaptive_config);
  initialized_ = true;
}



void LocalityAwareThroughputBasedPolicy::initializeFromConfig(
    const AdaptiveThroughputBasedConfig& config) {
  // Validate configuration
  config.validate();

  // Store configuration
  config_ = config;

  LOG(INFO) << "LocalityAwareThroughputBasedPolicy initialized with:";
  LOG(INFO) << "  Evaluation batch size: " << config_.evaluation_batch_size;
  LOG(INFO) << "  Monitoring batch size: " << config_.monitoring_batch_size;
  LOG(INFO) << "  Degradation threshold: " << config_.degradation_threshold;
  LOG(INFO) << "  Switch improvement threshold: " << config_.switch_improvement_threshold;
  LOG(INFO) << "  Decay factor: " << config_.decay_factor;
  LOG(INFO) << "  Number of consumers: " << config_.consumer_affinitizers.size();
}

size_t LocalityAwareThroughputBasedPolicy::getStateSize() const {
  return sizeof(AdaptiveThroughputState);
}

void LocalityAwareThroughputBasedPolicy::initializeState(void* state) {
  CHECK(initialized_) << "LocalityAwareThroughputBasedPolicy used before initialization";
  
  AdaptiveThroughputState* adaptive_state = new (state) AdaptiveThroughputState();
  adaptive_state->reset();
  
  // Copy configuration values
  adaptive_state->evaluation_batch_size = config_.evaluation_batch_size;
  adaptive_state->monitoring_batch_size = config_.monitoring_batch_size;
  adaptive_state->degradation_threshold = config_.degradation_threshold;
  adaptive_state->switch_improvement_threshold = config_.switch_improvement_threshold;
  adaptive_state->decay_factor = config_.decay_factor;


  // Reserve space for throughput samples
  uint64_t expected_samples = std::max(adaptive_state->monitoring_batch_size, adaptive_state->evaluation_batch_size);
  adaptive_state->batch_throughput_samples.reserve(expected_samples);
  
  // Start the first batch
  startNewBatch(adaptive_state);
  
  LOG(INFO) << "LocalityAwareThroughputBasedPolicy: State initialized for new query";
  
  // Also initialize the parent locality-aware state
  LocalityAwarePolicy::initializeState(state);
}

void LocalityAwareThroughputBasedPolicy::cleanupState(void* state) {
  auto* adaptive_state = static_cast<AdaptiveThroughputState*>(state);
  LOG(INFO) << "=== Final Adaptive Throughput Policy Stats ===";
  adaptive_state->log_stats();
  
  // Also cleanup the parent locality-aware state
  LocalityAwarePolicy::cleanupState(state);
  adaptive_state->reset();
}

RoutingDecision LocalityAwareThroughputBasedPolicy::getTargetChannel(
    const GeneralizedRouter* router, const RoutingContext& context,
    int num_total_consumers, int retry_count,
    const std::vector<int>& failed_channels) {
  DCHECK(initialized_) << "LocalityAwareThroughputBasedPolicy used before initialization";
  DCHECK(state_);

  auto* adaptive_state = static_cast<AdaptiveThroughputState*>(state_);
  
  // Determine target consumer based on current phase
  int target_consumer = (adaptive_state->current_phase == AdaptiveThroughputState::Phase::InitialEvaluation) 
                        ? adaptive_state->evaluation_target_consumer 
                        : adaptive_state->active_consumer;
  
  // Collect throughput sample for the target consumer
  if (retry_count == 0 && target_consumer < static_cast<int>(context.consumer_throughput.size()) &&
      context.consumer_throughput[target_consumer] > 0.0) {
    adaptive_state->batch_throughput_samples.push_back(context.consumer_throughput[target_consumer]);
    uint64_t target_batch_size = (adaptive_state->current_phase == AdaptiveThroughputState::Phase::InitialEvaluation)
                                 ? adaptive_state->evaluation_batch_size
                                 : adaptive_state->monitoring_batch_size;
    CHECK_LE(adaptive_state->batch_throughput_samples.size(), target_batch_size)
        << "Too many throughput samples collected in a single batch";
  }
  
  // Track statistics and log periodically (inherited from parent)
  // logStatsIfNeeded();
  
  size_t absolute_cu_idx;

  if (retry_count == 0) {
    // PHASE 1: Locality-aware routing for the target consumer (first attempt)
    
    // Track initial consumer selection for our target consumer
    trackInitialConsumerSelection(target_consumer);
    
    // Extract data pointer for NUMA detection
    DCHECK_GE(context.keys_payload_size, sizeof(void*))
        << "Invalid keys payload for locality-aware routing";
    void* data_ptr = *static_cast<void* const*>(context.keys_payload);
    
    // Get the closest NUMA node for this data using the target consumer's affinitizer
    absolute_cu_idx = random_local_cu_index_in_topo(
        data_ptr, consumer_affs_[target_consumer]);

    // const auto& cu_domain = consumer_cu_domains_[target_consumer];
    // DCHECK(!cu_domain.empty())
    //     << "Target consumer " << target_consumer << " has no accessible CUs";
    // // Random selection within target consumer's domain
    // uint64_t rand_val = random_state_.fetch_add(1, std::memory_order_relaxed);
    // rand_val = hash_combine(rand_val);
    // size_t random_idx = rand_val % cu_domain.size();
    // absolute_cu_idx = cu_domain[random_idx];
        
#ifndef NDEBUG
    // Verify this CU is in the target consumer's accessible domain
    const auto& cu_domain = consumer_cu_domains_[target_consumer];
    auto it = std::find(cu_domain.begin(), cu_domain.end(), absolute_cu_idx);
    DCHECK(it != cu_domain.end()) << "affinitizer returned a CU "
                                      "not in the target consumer's accessible domain";
#endif
    
  } else {
    // PHASE 2: Random selection from target consumer's accessible CUs (retry)
    
    // Track retry consumer selection for our target consumer
    trackRetryConsumerSelection(target_consumer);
    
    // Get the accessible CUs for the target consumer
    const auto& cu_domain = consumer_cu_domains_[target_consumer];
    DCHECK(!cu_domain.empty())
        << "Target consumer " << target_consumer << " has no accessible CUs";
    
    // Random selection within target consumer's domain
    uint64_t rand_val = random_state_;
    random_state_ += 1;
    rand_val = hash_combine(rand_val);
    size_t random_idx = rand_val % cu_domain.size();
    absolute_cu_idx = cu_domain[random_idx];
  }
  
  // Calculate final queue index for the target consumer
  size_t target_queue_offset = getQueueOffsetForConsumer(target_consumer);
  int target_queue = target_queue_offset + absolute_cu_idx;
  
  // Return routing decision with queue-specific free pool for NUMA locality
  return {target_queue, true, static_cast<size_t>(target_queue)};
}

void LocalityAwareThroughputBasedPolicy::onTupleRouted(int channel, bool success) {
  auto* adaptive_state = static_cast<AdaptiveThroughputState*>(state_);
  DCHECK(adaptive_state);

  if (success) {
    adaptive_state->total_tuples_processed++;
    adaptive_state->current_batch_tuple_count++;
    
    // Check if we've completed a batch
    uint64_t target_batch_size = (adaptive_state->current_phase == AdaptiveThroughputState::Phase::InitialEvaluation)
                                 ? adaptive_state->evaluation_batch_size
                                 : adaptive_state->monitoring_batch_size;
    
    if (adaptive_state->current_batch_tuple_count >= target_batch_size) {
      handleBatchCompletion(adaptive_state, static_cast<int>(consumer_affs_.size()));
    }
  }
  
  // Also call parent for statistics tracking
  LocalityAwarePolicy::onTupleRouted(channel, success);
}

void LocalityAwareThroughputBasedPolicy::handleBatchCompletion(
    AdaptiveThroughputState* state, int num_consumers) {
  
  // Calculate average throughput from collected samples
  double new_throughput = calculateAverageThroughput(state->batch_throughput_samples);

  
  // Handle phase-specific logic
  if (state->current_phase == AdaptiveThroughputState::Phase::InitialEvaluation) {
    const int completed_consumer = state->evaluation_target_consumer;
    counterlogger.log({}, counter_type::GROUTER_CONS_THROUGHPUT, new_throughput,
                      completed_consumer);

    // Update known throughput
    state->known_throughput_tps[completed_consumer] = new_throughput;
    LOG(INFO) << "Adaptive Throughput Policy: Consumer " << completed_consumer
              << " evaluated at " << std::fixed << std::setprecision(2)
              << new_throughput << " TPS";
    
    // Move to next consumer
    state->evaluation_target_consumer++;
    
    if (state->evaluation_target_consumer >= num_consumers) {
      // Initial evaluation complete - find best consumer
      const int best_consumer = findBestConsumerFromKnownThroughputs(state, num_consumers);
      // const int best_consumer = 1;
      state->active_consumer = best_consumer;
      state->active_consumer_best_throughput_tps = state->known_throughput_tps[best_consumer];
      
      LOG(INFO) << "Adaptive Throughput Policy: evaluation complete. "
                << "Selected consumer " << best_consumer
                << " with " << std::fixed << std::setprecision(2)
                << state->active_consumer_best_throughput_tps << " TPS";
      
      // Switch to monitoring phase
      state->current_phase = AdaptiveThroughputState::Phase::Monitoring;
      state->evaluation_target_consumer = best_consumer;
      state->phase_transitions++;
    } else {
      // Continue evaluation with next consumer
      state->active_consumer = state->evaluation_target_consumer;
    }
    
  } else {  // Monitoring phase
    // Update known throughput
    LOG(INFO) << "Adaptive Throughput Policy: Consumer " << state->active_consumer
                << " observed performance" << std::fixed << std::setprecision(2)
                << new_throughput << " TPS";
    state->known_throughput_tps[state->active_consumer] = std::max(new_throughput, state->known_throughput_tps[state->active_consumer]);
    if (new_throughput > state->active_consumer_best_throughput_tps) {
      // Performance improved
      state->active_consumer_best_throughput_tps = new_throughput;
      // LOG(INFO) << "Adaptive Throughput Policy: Consumer " << state->active_consumer
      //           << " performance improved to " << std::fixed << std::setprecision(2)
      //           << new_throughput << " TPS";
    } else if (new_throughput < state->active_consumer_best_throughput_tps * 
               (1.0 - state->degradation_threshold)) {
      // Performance degraded - trigger re-evaluation
      LOG(INFO) << "Adaptive Throughput Policy: Performance degraded from "
                << std::fixed << std::setprecision(2)
                << state->active_consumer_best_throughput_tps << " to "
                << new_throughput << " TPS for consumer #" << state->active_consumer;

      state->current_phase = AdaptiveThroughputState::Phase::InitialEvaluation;
      state->evaluation_target_consumer = 2;
      state->active_consumer = 2;
      state->re_evaluations++;
      state->phase_transitions++;

      // const int best_consumer = findBestConsumerFromKnownThroughputs(state, num_consumers);
      // if (best_consumer != state->active_consumer) {
      //   LOG(INFO) << "Switching to consumer #" << best_consumer
      //     << " with previous " << std::fixed << std::setprecision(2)
      //     << state->known_throughput_tps[best_consumer] << " TPS";
      //   state->active_consumer = best_consumer;
      //   state->phase_transitions++;
      // }


      // bool exist_better_consumer = false;
      // for (int i = 0; i < num_consumers; i++) {
      //   if (i == state->active_consumer) {
      //     // Skip the active consumer
      //     continue;
      //   }
      //   if (state->known_throughput_tps[i] > new_throughput) {
      //     LOG(INFO) << "Consumer #" << i
      //              << " has better throughput (" << state->known_throughput_tps[i]
      //              << " TPS) than current active consumer #" << state->active_consumer
      //              << " (" << new_throughput << " TPS). Triggering re-evaluation.";
      //     exist_better_consumer = true;
      //   }
      // }
      // // TODO can improve logic here to avoid re-evaluating current consumer
      // if (exist_better_consumer) {
      //   state->current_phase = AdaptiveThroughputState::Phase::InitialEvaluation;
      //   state->evaluation_target_consumer = 0;
      //   state->active_consumer = 0;
      //   state->re_evaluations++;
      //   state->phase_transitions++;
      // }
    }
  }
  
  // Reset for next batch
  startNewBatch(state);
}

int LocalityAwareThroughputBasedPolicy::findBestConsumerFromKnownThroughputs(
    const AdaptiveThroughputState* state, int num_consumers) const {
  int best_consumer = 0;
  double best_throughput = state->known_throughput_tps[0];
  
  for (int i = 0; i < num_consumers; i++) {
    // LOG(INFO) << "Consumer #" << i
    //           << " known throughput: " << std::fixed << std::setprecision(2)
    //           << state->known_throughput_tps[i] << " TPS";
    if (state->known_throughput_tps[i] > best_throughput) {
      best_throughput = state->known_throughput_tps[i];
      best_consumer = i;
    }
  }
  
  return best_consumer;
}

void LocalityAwareThroughputBasedPolicy::startNewBatch(
    AdaptiveThroughputState* state) {
  state->batch_throughput_samples.clear();
  state->current_batch_tuple_count = 0;
}

double LocalityAwareThroughputBasedPolicy::calculateAverageThroughput(
    const std::vector<double>& samples) const {
  if (samples.empty()) {
    return 0.0;
  }
  
  double sum = 0.0;
  int count = 0;
  double weight_sum = 0.0;
  double weight = config_.decay_factor;
  DCHECK_GT(samples.size(), 200)
      << "Too few throughput samples collected, expected at least 200, got "
      << samples.size();
  for (int i = samples.size() - 1; i > 200; i -= 1) {
    DCHECK(!std::isnan(samples[i]) ) << "Invalid sample isnan value at index " << i;
    DCHECK(!std::isinf(samples[i])) << "Invalid sample isinf value at index " << i;
    sum += weight * samples[i];
    weight_sum += weight;
    weight *= weight;
    count++;
  }
  DCHECK_GT(sum, 0.0) << "No valid throughput samples collected. count " << count << " samples.size() " << samples.size();
  DCHECK_GT(count, 0) << "No valid throughput samples collected";
  
  return sum / weight_sum;
}


// Register the policy with the factory
REGISTER_ROUTING_POLICY(LocalityAwareThroughputBasedPolicy,
                        GeneralizedRoutingPolicyV2::ADAPTIVE_THROUGHPUT_BASED)

}  // namespace routing
}  // namespace proteus
