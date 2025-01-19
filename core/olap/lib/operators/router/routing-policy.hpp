/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2023
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

#ifndef ROUTING_POLICY_HPP_
#define ROUTING_POLICY_HPP_

#include <codegen/expressions/expressions.hpp>
#include <olap/plugins/plugins.hpp>
#include <olap/routing/affinitizers.hpp>
#include <olap/routing/routing-policy-types.hpp>
#include <olap/util/parallel-context.hpp>
#include <platform/topology/device-types.hpp>
#include <platform/topology/topology.hpp>
#include <platform/util/rdtsc.hpp>
#include <utility>

struct routing_target {
  llvm::Value *target;
  llvm::Value *source_pool;
  llvm::Value *may_retry;
};

namespace routing {
class RoutingPolicy {
 public:
  virtual ~RoutingPolicy() = default;
  virtual routing_target evaluate(OlapParallelContext *context,
                                  const OperatorState &childState,
                                  ProteusValueMemory retrycnt) = 0;
};

class Random : public RoutingPolicy {
  size_t fanout;

 public:
  explicit Random(size_t fanout) : fanout(fanout) {}
  routing_target evaluate(OlapParallelContext *context,
                          const OperatorState &childState,
                          ProteusValueMemory retrycnt) override;
};

class HashBased : public RoutingPolicy {
  size_t fanout;
  expression_t e;

 public:
  HashBased(size_t fanout, expression_t e) : fanout(fanout), e(std::move(e)) {}
  routing_target evaluate(OlapParallelContext *context,
                          const OperatorState &childState,
                          ProteusValueMemory retrycnt) override;
};

class Local : public RoutingPolicy {
  size_t fanout;
  RecordAttribute wantedField;
  const AffinityPolicy *aff;  // use pointer to satisfy lifetime requirements

 public:
  Local(size_t fanout, const std::vector<RecordAttribute *> &wantedFields,
        const AffinityPolicy *aff);
  routing_target evaluate(OlapParallelContext *context,
                          const OperatorState &childState,
                          ProteusValueMemory retrycnt) override;
};

/**
 * A routing policy for the generalized router where the generalized router may
 * have multiple distinct consumer pipelines. This policy assumes there are
 * #consuming_pipelines * (#cpu_numa_nodes + #GPUS) targets. targets [0,
 * (#cpu_numa_nodes + #GPUS)] are for the first consumer, targets
 * [(#cpu_numa_nodes + #GPUS), 2 * (#cpu_numa_nodes + #GPUS)] are for the second
 * consumer, and so on. Nodes are ordered by their index in the topology. With
 * CPUs followed by GPUs. For example, if there are 4 CPU NUMA nodes and 2 GPUs,
 * and two consumers, the targets/queues are as follows:
 * [CONS1_CPU1, CONS1_CPU2, CONS1_CPU3, CONS1_CPU4, CONS1_GPU1, CONS1_GPU2,
 * CONS2_CPU1, CONS2_CPU2, CONS2_CPU3, CONS2_CPU4, CONS2_GPU1, CONS2_GPU2]
 *
 * A routing policy returns a target queue, a source free pool (to use for
 * acquiring a slot), and a boolean indicating if the operation may be retried.
 * Router ignores the source free pool and uses just the target queue.
 * Generalized router uses the source free pool to acquire a slot.
 *
 * Each consumer supplies their own Affinitizer, that returns an index of
 * the target node/GPU in the topology, and target device.
 */
class RandomSplitForceDataLocal : public RoutingPolicy {
  const RecordAttribute wantedField;
  std::vector<Affinitizer *>
      consumer_affs;  // use pointer to satisfy lifetime requirements
  std::vector<size_t> consumer_offsets;

 public:
  RandomSplitForceDataLocal(
      const std::vector<RecordAttribute *> &wantedFields,
      std::vector<Affinitizer *> consumer_affs,
      const std::vector<DeviceType> &consumer_device_types);
  routing_target evaluate(OlapParallelContext *context,
                          const OperatorState &childState,
                          ProteusValueMemory retrycnt) override;
};

/**
 * This is nearly the same as RandomSplitForceDataLocal. On the first attempt,
 * it will select a random consumer and then force a data local queue. On
 * retries it will select a random consumer and a random queue ignoring data
 * locality
 */
class RandomSplitPreferDataLocal : public RoutingPolicy {
  const RecordAttribute wantedField;
  std::vector<Affinitizer *>
      consumer_affs;  // use pointer to satisfy lifetime requirements
  std::vector<size_t> consumer_offsets;
  std::vector<DeviceType> device_types;

 public:
  RandomSplitPreferDataLocal(
      const std::vector<RecordAttribute *> &wantedFields,
      std::vector<Affinitizer *> consumer_affs,
      const std::vector<DeviceType> &consumer_device_types);
  routing_target evaluate(OlapParallelContext *context,
                          const OperatorState &childState,
                          ProteusValueMemory retrycnt) override;
};

class ThroughputTracker {
 private:
  uint64_t completed_events;
  uint64_t total_time;
  // Current event being tracked and its start time
  void *current_event;
  uint64_t current_start_time;
  // Flag to handle first event
  bool first_event;
  const uint32_t count_skip_events;

 public:
  ThroughputTracker(uint32_t _count_skip_events)
      : count_skip_events(_count_skip_events) {
    reset();
  }

  void reset() {
    completed_events = 0;
    total_time = 0;
    current_event = nullptr;
    current_start_time = 0;
    first_event = true;
  }

  uint64_t get_completed_events() const { return completed_events; }

  void notify_event(void *event, uint64_t timestamp) {
    if (first_event) {
      current_event = event;
      current_start_time = timestamp;
      first_event = false;
      return;
    }

    // If we see a different event, complete the current one
    if (event != current_event) {
      uint64_t duration = timestamp - current_start_time;
      completed_events++;
      if (completed_events > count_skip_events) {
        total_time += duration;
      }
      current_event = event;
      current_start_time = timestamp;
    }
  }

  double get_current_throughput() const {
    if (total_time == 0 || completed_events == 0) {
      return 0.0;
    }
    // Return events per unit time
    return static_cast<double>(completed_events - count_skip_events) /
           total_time;
  }
};

/**
 * This routing policy will select a consumer based on the throughput of the
 * consumers. It will sample the throughput of each consumer and select the
 * consumer with the highest throughput. It will switch to the consumer with the
 * highest throughput after a certain number of samples.
 */
class ThroughputSplitPreferDataLocal : public RoutingPolicy {
  const RecordAttribute wantedField;
  std::vector<Affinitizer *>
      consumer_affs;  // use pointer to satisfy lifetime requirements
  std::vector<size_t> consumer_offsets;
  std::vector<DeviceType> device_types;
  StateVar routing_state_var;
  const uint64_t sample_size;
  const uint32_t count_skip_events;

 public:
  class RoutingState {
   public:
    const uint64_t num_consumers;
    ThroughputTracker tracker;
    uint64_t curr_target;
    std::vector<double> throughputs;
    bool exploit;
    const uint64_t sample_size;
    RoutingState(uint64_t _num_consumers, uint64_t _sample_size,
                 uint32_t _count_skip_events)
        : num_consumers(_num_consumers),
          tracker(_count_skip_events),
          curr_target(0),
          throughputs(),
          exploit(false),
          sample_size(_sample_size) {
      throughputs.resize(num_consumers, 0.0);
    }
    uint64_t get_target() {
      //      if (num_consumers == 1) {
      //        return 0;
      //      } else {
      //        return 1;
      //      }

      if (exploit) {
        return curr_target;
      }
      if (tracker.get_completed_events() > sample_size) {
        throughputs[curr_target] = tracker.get_current_throughput();
        LOG(INFO) << " previous throughput of " << curr_target << " is "
                  << tracker.get_current_throughput() << " with " << sample_size
                  << "samples";
        curr_target = (curr_target + 1) % num_consumers;
        LOG(INFO) << "switching to " << curr_target;
        tracker.reset();
        if (curr_target == 0) {
          curr_target = std::distance(
              throughputs.begin(),
              std::max_element(throughputs.begin(), throughputs.end()));
          LOG(INFO) << "exploit time: " << curr_target;
          exploit = true;
        }
      }
      return curr_target;
    }
  };

 public:
  ThroughputSplitPreferDataLocal(
      const std::vector<RecordAttribute *> &wantedFields,
      std::vector<Affinitizer *> consumer_affs,
      const std::vector<DeviceType> &consumer_device_types,
      uint64_t sample_size, uint32_t count_skip_events);
  routing_target evaluate(OlapParallelContext *context,
                          const OperatorState &childState,
                          ProteusValueMemory retrycnt) override;
  void generateStateInit(OlapParallelContext *context);
};

class LocalServer : public HashBased {
 public:
  LocalServer(size_t fanout);
};

class PreferLocal : public RoutingPolicy {
  Local priority;
  Random alternative;

 public:
  PreferLocal(size_t fanout, const std::vector<RecordAttribute *> &wantedFields,
              const AffinityPolicy *aff);
  routing_target evaluate(OlapParallelContext *context,
                          const OperatorState &childState,
                          ProteusValueMemory retrycnt) override;
};

extern "C" {
[[maybe_unused]] void record_event(void *state, void *event);
[[maybe_unused]] void *createRoutingState(uint64_t num_consumers,
                                          uint64_t sample_size,
                                          uint32_t count_skip_samples);
[[maybe_unused]] void destroyRoutingState(void *state);
[[maybe_unused]] uint64_t get_target(void *state);
}

class PreferLocalServer : public RoutingPolicy {
  LocalServer priority;
  Random alternative;

 public:
  PreferLocalServer(size_t fanout);
  routing_target evaluate(OlapParallelContext *context,
                          const OperatorState &childState,
                          ProteusValueMemory retrycnt) override;
};

}  // namespace routing

#endif /* ROUTING_POLICY_HPP_ */
