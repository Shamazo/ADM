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

#ifndef PROTEUS_LOCALITY_AWARE_THROUGHPUT_BASED_HPP
#define PROTEUS_LOCALITY_AWARE_THROUGHPUT_BASED_HPP

#include <glog/logging.h>

#include <array>
#include <chrono>
#include <vector>

#include "locality-aware-policy.hpp"
#include "lib/operators/router/routing-policy-v2.hpp"

namespace proteus {
namespace routing {

/**
 * State structure for adaptive throughput based routing policy.
 * Manages performance tracking and consumer selection decisions.
 */
struct AdaptiveThroughputState {
  // --- Consumer Performance Data ---
  static constexpr size_t MAX_CONSUMERS = 8;
  std::array<double, MAX_CONSUMERS> known_throughput_tps;
  double active_consumer_best_throughput_tps{0.0};

  // --- Active Routing State ---
  int active_consumer{0};

  // --- Evaluation and Monitoring State ---
  enum class Phase { InitialEvaluation, Monitoring };
  Phase current_phase{Phase::InitialEvaluation};

  int evaluation_target_consumer{0};
  uint64_t current_batch_tuple_count{0};

  // --- Throughput Sample Collection ---
  std::vector<double> batch_throughput_samples;

  // --- Configuration ---
  uint64_t evaluation_batch_size{400};
  uint64_t monitoring_batch_size{1000};
  double degradation_threshold{0.2};
  double switch_improvement_threshold{0.1};
  double decay_factor{0.95};    // Slow decay factor (higher = slower decay)

  // --- Statistics ---
  uint64_t total_tuples_processed{0};
  uint64_t phase_transitions{0};
  uint64_t re_evaluations{0};
  uint64_t consumer_switches{0};

  void reset() {
    known_throughput_tps.fill(0.0);
    active_consumer_best_throughput_tps = 0.0;
    active_consumer = 0;
    current_phase = Phase::InitialEvaluation;
    evaluation_target_consumer = 0;
    current_batch_tuple_count = 0;
    batch_throughput_samples.clear();
    total_tuples_processed = 0;
    phase_transitions = 0;
    re_evaluations = 0;
    consumer_switches = 0;
  }

  void log_stats() const {
    LOG(INFO) << "=== Adaptive Throughput Policy Stats ===";
    LOG(INFO) << "Total tuples processed: " << total_tuples_processed;
    LOG(INFO) << "Phase transitions: " << phase_transitions;
    LOG(INFO) << "Re-evaluations: " << re_evaluations;
    LOG(INFO) << "Consumer switches: " << consumer_switches;
    LOG(INFO) << "Active consumer: " << active_consumer;
    LOG(INFO) << "Active consumer best TPS: " << active_consumer_best_throughput_tps;
    LOG(INFO) << "Current phase: " << (current_phase == Phase::InitialEvaluation ? "InitialEvaluation" : "Monitoring");
  }
};

/**
 * Adaptive Throughput Based Routing Policy.
 * Routes all tuples to the single best-performing consumer, with locality-aware
 * routing within that consumer's queues. Continuously monitors performance
 * and switches consumers when degradation is detected.
 */
class LocalityAwareThroughputBasedPolicy : public LocalityAwarePolicy {
 public:
  // Constructor that accepts variant configuration
  explicit LocalityAwareThroughputBasedPolicy(const PolicyConfigVariant& config);

  // RoutingPolicyV2 interface overrides
  size_t getStateSize() const override;
  void initializeState(void* state) override;
  void cleanupState(void* state) override;
  RoutingDecision getTargetChannel(
      const GeneralizedRouter* router, const RoutingContext& context,
      int num_total_consumers, int retry_count,
      const std::vector<int>& failed_channels) override;
  void onTupleRouted(int channel, bool success) override;

 private:
  // Internal helper methods
  void initializeFromConfig(const AdaptiveThroughputBasedConfig& config);
  void handleBatchCompletion(AdaptiveThroughputState* state, int num_consumers);
  int findBestConsumerFromKnownThroughputs(const AdaptiveThroughputState* state, int num_consumers) const;
  void startNewBatch(AdaptiveThroughputState* state);
  double calculateAverageThroughput(const std::vector<double>& samples) const;

  // Configuration parameters
  AdaptiveThroughputBasedConfig config_;
  bool initialized_ = false;
};

}  // namespace routing
}  // namespace proteus

#endif  // PROTEUS_LOCALITY_AWARE_THROUGHPUT_BASED_HPP
