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

#ifndef PROTEUS_GENERALIZED_ROUTER_HPP
#define PROTEUS_GENERALIZED_ROUTER_HPP

#include <olap/routing/affinitizers.hpp>
#include <olap/routing/routing-policy-types-v2.hpp>
#include <platform/memory/managed-pointer.hpp>
#include <platform/threadpool/threadvector.hpp>
#include <platform/util/datastructures/threadsafe-set.hpp>
#include <unordered_set>
#include <chrono>
#include <deque>
#include <atomic>

#include "lib/operators/operators.hpp"
#include "lib/operators/router/routing-policy.hpp"
#include "routing-policy-v2.hpp"

namespace proteus {

/**
 * @brief Tracks throughput metrics for a single consumer thread
 * 
 * Uses a sliding window approach to calculate throughput based on recent
 * processing times. Thread-safe for concurrent access.
 */
struct ThroughputTracker {
  static constexpr size_t WINDOW_SIZE = 8;  // Number of samples to keep
  
  // Circular buffer for storing processing times in nanoseconds
  std::deque<uint64_t> processing_times;

  // Running statistics
  std::atomic<uint64_t> total_tuples{0};
  std::atomic<uint64_t> total_time_ns{0};

  void reset() {
    processing_times.clear();
    total_tuples.store(0, std::memory_order_relaxed);
    total_time_ns.store(0, std::memory_order_relaxed);
  }
  /**
   * Update throughput tracking with a new measurement
   * @param elapsed_ns Time taken to process the tuple in nanoseconds
   */
  void updateThroughputTracking(uint64_t elapsed_ns) {
    if (elapsed_ns < 200000) {
      // Ignore very small processing times, pipeline not filled yet
      return;
    }

    // Add to circular buffer
    processing_times.push_back(elapsed_ns);
    if (processing_times.size() > WINDOW_SIZE) {
      // Remove oldest entry
      uint64_t old_time = processing_times.front();
      processing_times.pop_front();
      
      // Update running totals
      total_time_ns.fetch_sub(old_time, std::memory_order_relaxed);
    } else {
      // total tuples in window.
      total_tuples.fetch_add(1, std::memory_order_relaxed);
    }

    // thread_local static uint64_t log_counter = 0;
    // log_counter++;
    // if (log_counter % 100 == 0) {
    //   LOG(INFO) << "ThroughputTracker: total_tuples=" << total_tuples
    //             << ", total_time_s=" << ((double) total_time_ns) / 1e9 << " " << processing_times[0] << ", " << processing_times[1] << ", " << processing_times[2] << ", " << processing_times[3] << ", ";
    // }

    total_time_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
  }
  
  /**
   * Get current throughput in tuples per second
   * @return Current throughput based on sliding window
   */
  double getThroughput() const {
    if (total_time_ns == 0) {
      return 0.0;
    }
    // Calculate throughput: tuples/second
    double throughput = (total_tuples* 1e9 ) / (total_time_ns);
    return throughput;
  }
};

/**
 * @brief Primary FFI entry point for V2 routing policies
 *
 * Called from JIT-generated code to route a single tuple through the C++
 * policy system. This function encapsulates the complete routing pipeline:
 * policy decision, buffer acquisition, data copy, and enqueue.
 *
 * @param router Pointer to the GeneralizedRouter instance managing this routing
 * @param jit_routing_keys_payload Extracted routing keys (policy-specific
 * format)
 * @param jit_routing_keys_size Size of the keys payload in bytes
 * @param jit_full_payload Complete tuple data to be routed
 * @param full_payload_size Size of the full payload in bytes
 * @param group_id Pipeline group identifier for tracing and debugging
 *
 * Routing Process:
 * 1. Creates RoutingContext with keys, timestamp, and system state
 * 2. Calls policy->getTargetChannel() for routing decision
 * 3. Acquires buffer from appropriate free pool (policy-determined)
 * 4. Copies payload data to buffer
 * 5. Enqueues buffer to target consumer queue
 * 6. Notifies policy of successful routing
 *
 */
extern "C" void route_and_enqueue_via_cpp(GeneralizedRouter *router,
                                          const void *jit_routing_keys_payload,
                                          size_t jit_routing_keys_size,
                                          const void *jit_full_payload,
                                          size_t full_payload_size,
                                          int64_t group_id);

class GeneralizedRouter;

/**
 * Blocking
 */
[[nodiscard]] void *acquireBufferGeneralized(int target, GeneralizedRouter *xch,
                                             int64_t groupId);
/**
 * non-blocking. Returns nullptr if no buffer is available.
 */
[[nodiscard]] void *try_acquireBufferGeneralized(int target,
                                                 GeneralizedRouter *xch,
                                                 int64_t groupId);

/**
 * @brief Release buffer to consumer's ready queue
 *
 * Enqueues a filled buffer to the specified consumer's ready queue,
 * making it available for consumption. This completes the routing
 * pipeline by delivering the tuple to its target consumer.
 *
 * @param target Target queue index (consumer-specific)
 * @param xch GeneralizedRouter instance managing the queues
 * @param buff Pointer to the filled buffer
 *
 * Buffer Lifecycle:
 * After release, the buffer is owned by the consumer until it
 * calls freeBufferGeneralized() to return it to the free pool.
 */
void releaseBufferGeneralized(int target, GeneralizedRouter *xch, void *buff);

class GeneralizedRouterConsumer final : public experimental::UnaryOperator {
 protected:
  GeneralizedRouter *producer;

  const DegreeOfParallelism fanout;
  PipelineGen *catch_pip;

  std::unique_ptr<Affinitizer> aff;
  std::unique_ptr<AffinityPolicy> aff_policy;
  const DeviceType target_device;
  /// used only for recoding counter statistics
  const int consumer_index;
  /// Count of rowgroups/items that this consumer has consumed
  alignas(64) std::atomic<int> consumed_count;
  
  /// Throughput trackers for each thread
  std::vector<ThroughputTracker> thread_throughput_trackers_;

 public:
  GeneralizedRouterConsumer(std::shared_ptr<GeneralizedRouter> producer,
                            DegreeOfParallelism fanout,
                            std::unique_ptr<Affinitizer> aff,
                            DeviceType target_device, int consumer_index);

  void consume(OlapParallelContext *context,
               const OperatorState &childState) override;

  [[nodiscard]] bool isFiltering() const override;

  [[nodiscard]] RecordType getRowType() const override;

  [[nodiscard]] DegreeOfParallelism getDOP() const override { return fanout; }
  [[nodiscard]] DeviceType getDeviceType() const override;

  [[nodiscard]] DegreeOfParallelism getDOPServers() const override;
  [[nodiscard]] bool isPacked() const override;
  [[nodiscard]] proteus::traits::HomReplication getHomReplication()
      const override;
  
  /**
   * @brief Get aggregated throughput information for this consumer
   * 
   * Returns the average throughput across all threads of this consumer.
   * Used by routing policies to make informed decisions based on actual
   * processing performance.
   * 
   * @return Average throughput in tuples per second
   */
  double getThroughputInfo() const;

 protected:
  void produce_(OlapParallelContext *context) override;
  virtual void spawnWorker(const void *session, threadvector &firers,
                           std::unordered_set<int> &allocated_pools);
  virtual void fire(int target_queue, int local_target, int source_free_pool,
                    PipelineGen *pipGen, const void *session,
                    bool should_allocate_free_pool_buffs);
  virtual void foreachTaskDo(int target, int source_free_pool, Pipeline *pip,
                             PipelineGen *pipGen,
                             std::function<void(void *)> f, int thread_index);

  friend class GeneralizedRouter;
};

class GeneralizedRouter final : public experimental::UnaryOperator {
  // weak_ptr because consumers are parent operators who have an owning
  // shared_ptr to this GeneralizedRouter
public:
  std::vector<std::weak_ptr<GeneralizedRouterConsumer>> consumers;
private:
  threadvector firers;

  const size_t slack;

  const std::vector<RecordAttribute *> wantedFields;
  llvm::Type *params_type;

  StateVar groupVar;
  size_t buf_size;
  bool need_cnt = false;

  std::atomic<int> remaining_producers;
  int producers;

  std::mutex init_mutex;

  //  std::deque<AsyncQueueMPMC<void *>,
  //  proteus::memory::ExplicitSocketPinnedMemoryAllocator<AsyncQueueMPMC<void
  //  *>>>
  //  ready_fifo{proteus::memory::ExplicitSocketPinnedMemoryAllocator<AsyncQueueMPMC<void
  //  *>>{1}};
  std::deque<AsyncQueueMPMCWithSleep<void *>> ready_fifo{};

  //  std::deque<AsyncQueueMPMC<void *>,
  //  proteus::memory::ExplicitSocketPinnedMemoryAllocator<AsyncQueueMPMC<void
  //  *>>>
  //  free_pool{proteus::memory::ExplicitSocketPinnedMemoryAllocator<AsyncQueueMPMC<void
  //  *>>{1}};
  std::deque<AsyncQueueMPMC<void *>> free_pool{};
  //  std::deque<AsyncQueueMPMC<void *>,
  //             proteus::memory::ExplicitSocketPinnedMemoryAllocator<
  //                 AsyncQueueMPMC<void *>>>
  //      ready_fifo{proteus::memory::ExplicitSocketPinnedMemoryAllocator<
  //          AsyncQueueMPMC<void *>>{1}};
  //
  //  std::deque<AsyncQueueMPMC<void *>,
  //             proteus::memory::ExplicitSocketPinnedMemoryAllocator<
  //                 AsyncQueueMPMC<void *>>>
  //      free_pool{proteus::memory::ExplicitSocketPinnedMemoryAllocator<
  //          AsyncQueueMPMC<void *>>{1}};
  //  //  threadsafe_set<void *> *free_pool = nullptr;

  std::weak_ptr<GeneralizedRouter> self_ptr;

  // V2 Policy System Members
  proteus::routing::GeneralizedRoutingPolicyV2 policy_type_v2_;
  std::unique_ptr<proteus::routing::RoutingPolicyV2> routing_policy_v2_;
  // Policy state management
  void *policy_state_ = nullptr;
  size_t policy_state_size_ = 0;

 public:
  // Access for FFI if needed
  void *getPolicyState() const { return policy_state_; }

 protected:
  struct ConstructorGuard {
    explicit ConstructorGuard(int) {}
  };

 public:
  struct Args {
    std::shared_ptr<Operator> child;
    size_t slack;
    std::vector<RecordAttribute *> attrs;
    // V2 Policy System
    proteus::routing::GeneralizedRoutingPolicyV2 policy_type_v2 =
        proteus::routing::GeneralizedRoutingPolicyV2::ROUND_ROBIN;
  };

  static std::shared_ptr<GeneralizedRouter> create(Args args) {
    auto op = std::make_shared<GeneralizedRouter>(ConstructorGuard{0},
                                                  std::move(args));
    op->setSelfPtr(op);
    return op;
  }

  /**
   * GeneralizedRouter can only be constructed on the heap using the create
   * method. This constructor should not be called directly.
   * This is to ensure ownership and lifetimes, as GeneralizedRouterConsumers
   * share ownership of a GeneralizedRouter.
   */
  explicit GeneralizedRouter([[maybe_unused]] ConstructorGuard guard, Args args)
      : experimental::UnaryOperator(std::move(args.child)),
        slack(args.slack),
        producers(0),  // Will be set in constructor body
        wantedFields(std::move(args.attrs)),
        policy_type_v2_(args.policy_type_v2),
        params_type(nullptr),
        buf_size(0) {
    producers = getChild()->getDOP();
  }

  // Custom destructor to handle incomplete type in unique_ptr
  ~GeneralizedRouter() override;

  void consume(OlapParallelContext *context,
               const OperatorState &childState) override;

  [[nodiscard]] bool isFiltering() const override { return false; }

  [[nodiscard]] RecordType getRowType() const override { return wantedFields; }

  [[nodiscard]] DegreeOfParallelism getDOP() const override;

  std::shared_ptr<GeneralizedRouterConsumer> appendConsumer(
      DeviceType target_device, DegreeOfParallelism dop,
      std::unique_ptr<Affinitizer> aff);

  // V2 Policy System Helper Methods
  proteus::routing::PolicyDataRequirements getRoutingDataRequirements() const;
  size_t getRoutingKeysSize() const;
  proteus::routing::RoutingPolicyV2 *get_policy() {
    return routing_policy_v2_.get();
  }
  proteus::routing::GeneralizedRoutingPolicyV2 get_policy_type() const {
    return policy_type_v2_;
  }
  int getFanoutDOP_for_policy() { return static_cast<int>(consumers.size()); }
  size_t get_cpp_buf_size() { return buf_size; }
  // Removed getTupleCount() - access through policy state now
  size_t getQueueDepth(int channel) const {
    if (channel >= 0 && channel < static_cast<int>(ready_fifo.size())) {
      return ready_fifo.at(channel).size_unsafe();
    }
    return 0;
  }

  // Get variant-based policy configuration
  proteus::routing::PolicyConfigVariant createPolicyConfig() const;

 public:
  // Make generateDynamicKeyExtraction accessible to policy classes
  void generateDynamicKeyExtraction(OlapParallelContext *context,
                                    const OperatorState &childState,
                                    llvm::Value *&keys_ptr_out,
                                    llvm::Value *&keys_size_out);

  // Accessor methods for policy classes
  size_t get_cpp_buf_size() const { return buf_size; }

 protected:
  void setSelfPtr(const std::shared_ptr<GeneralizedRouter> &self) {
    self_ptr = self;
  }
  std::shared_ptr<GeneralizedRouter> getSelfPtr() { return self_ptr.lock(); }

  /**
   * Aquire a buffer from a free_pool
   * @param free_pool_idx The free pool to aquire a buffer from
   * @param polling If false, the function will block until a buffer is
   * available. If true, will return nullptr if the target free pool is empty
   * @param groupId unsure why needed
   * @return nullptr or a buffer
   */
  [[nodiscard]] virtual proteus::managed_ptr acquireBufferGeneralized(
      int free_pool_idx, bool polling, int64_t groupId);
  /**
   * Release a buffer to the target ready queue
   * @param target target queue
   * @param buff buffer previously acquired from the target free pool
   */
  virtual void releaseBufferGeneralized(int target, proteus::managed_ptr buff);
  virtual void freeBufferGeneralized(int free_pool_idx,
                                     proteus::managed_ptr buff);
  virtual bool get_readyGeneralized(int target, proteus::managed_ptr &buff);

  friend void *acquireBufferGeneralized(int target, GeneralizedRouter *xch,
                                        int64_t groupId);
  friend void *try_acquireBufferGeneralized(int target, GeneralizedRouter *xch,

                                            int64_t groupId);
  friend void releaseBufferGeneralized(int target, GeneralizedRouter *xch,
                                       void *buff);

  void produce_(OlapParallelContext *context) override;

  void produceForConsumer(const GeneralizedRouterConsumer &cons,
                          OlapParallelContext *context);

  virtual void open(Pipeline *pip);
  /**
   * helper function to allocate slots for a free pool
   * Assumes init_mutex is held when called.
   */
  virtual void create_queues();
  virtual void *allocate_buffers_for_free_pool(size_t free_pool_idx);
  virtual void close(Pipeline *pip);

  virtual llvm::Value *createTaskDescription(OlapParallelContext *context,
                                             const OperatorState &childState);
public:
  // TODO make route_and_enqueue_via_cpp a friend or a method
 [[nodiscard]] virtual size_t getNumberOfQueues() const;


  friend class GeneralizedRouterConsumer;
};

}  // namespace proteus

#endif /* PROTEUS_GENERALIZED_ROUTER_HPP */
