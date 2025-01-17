/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2024
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
#include <platform/memory/managed-pointer.hpp>
#include <platform/threadpool/threadvector.hpp>
#include <platform/util/datastructures/threadsafe-set.hpp>

#include "lib/operators/operators.hpp"
#include "lib/operators/router/routing-policy.hpp"

namespace proteus {

class GeneralizedRouter;

[[nodiscard]] void *acquireBufferGeneralized(int target, GeneralizedRouter *xch,
                                             int64_t groupId);
[[nodiscard]] void *try_acquireBufferGeneralized(int target,
                                                 GeneralizedRouter *xch,
                                                 int64_t groupId);
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

 protected:
  void produce_(OlapParallelContext *context) override;
  virtual void spawnWorker(const void *session, size_t queue_offset,
                           threadvector &firers);
  virtual void fire(int target_queue, int local_target, PipelineGen *pipGen,
                    const void *session, bool should_allocate_queue_buffs);
  virtual void foreachTaskDo(int target, Pipeline *pip, PipelineGen *pipGen,
                             std::function<void(void *)> f);

  friend class GeneralizedRouter;
};

class GeneralizedRouter final : public experimental::UnaryOperator {
  // weak_ptr because consumers are parent operators who have an owning
  // shared_ptr to this GeneralizedRouter
  std::vector<std::weak_ptr<GeneralizedRouterConsumer>> consumers;

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

  const GeneralizedRoutingPolicy policy_type;
  std::unique_ptr<routing::RoutingPolicy> routing;
  std::weak_ptr<GeneralizedRouter> self_ptr;
  /// When using the throughput split policies, the number of rowgroups to test
  /// each consumer with
  const uint64_t sample_size;
  /// When using the throughput split policies, skip the first
  /// count_skip_samples rowgroups when evaluating throughput
  const uint32_t count_skip_samples;

 protected:
  struct ConstructorGuard {
    explicit ConstructorGuard(int) {}
  };

 public:
  struct Args {
    std::shared_ptr<Operator> child;
    size_t slack;
    std::vector<RecordAttribute *> attrs;
    GeneralizedRoutingPolicy policy_type;
    /// When using the throughput split policies, the number of rowgroups to
    /// test each consumer with
    uint64_t sample_size = 350;
    /// When using the throughput split policies, skip the first
    /// count_skip_samples rowgroups when evaluating throughput
    uint32_t count_skip_samples = 250;
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
        producers(getChild()->getDOP()),
        wantedFields(std::move(args.attrs)),
        policy_type(args.policy_type),
        params_type(nullptr),
        buf_size(0),
        sample_size(args.sample_size),
        count_skip_samples(args.count_skip_samples) {}

  void consume(OlapParallelContext *context,
               const OperatorState &childState) override;

  [[nodiscard]] bool isFiltering() const override { return false; }

  [[nodiscard]] RecordType getRowType() const override { return wantedFields; }

  [[nodiscard]] DegreeOfParallelism getDOP() const override;

  std::shared_ptr<GeneralizedRouterConsumer> appendConsumer(
      DeviceType target_device, DegreeOfParallelism dop,
      std::unique_ptr<Affinitizer> aff);

 protected:
  void setSelfPtr(const std::shared_ptr<GeneralizedRouter> &self) {
    self_ptr = self;
  }
  std::shared_ptr<GeneralizedRouter> getSelfPtr() { return self_ptr.lock(); }

  /**
   * Aquire a buffer from a free_pool
   * @param target The free pool to aquire a buffer from
   * @param polling If false, the function will block until a buffer is
   * available. If true, will return nullptr if the target free pool is empty
   * @param groupId unsure why needed
   * @return nullptr or a buffer
   */
  [[nodiscard]] virtual proteus::managed_ptr acquireBufferGeneralized(
      int target, bool polling, int64_t groupId);
  /**
   * Release a buffer to the target ready queue
   * @param target target queue
   * @param buff buffer previously acquired from the target free pool
   */
  virtual void releaseBufferGeneralized(int target, proteus::managed_ptr buff);
  virtual void freeBufferGeneralized(int target, proteus::managed_ptr buff);
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
   * helper function to create queues
   * Assumes init_mutex is held when called.
   */
  virtual void create_queues();
  virtual void *allocate_buffers_for_queue(size_t queue);
  virtual void close(Pipeline *pip);

  virtual llvm::Value *createTaskDescription(OlapParallelContext *context,
                                             const OperatorState &childState);

  [[nodiscard]] virtual size_t getNumberOfQueues() const;

  std::unique_ptr<routing::RoutingPolicy> getPolicy(
      GeneralizedRoutingPolicy p, DegreeOfParallelism dop,
      const std::vector<RecordAttribute *> &wantedFields);

  friend class GeneralizedRouterConsumer;
};

}  // namespace proteus

#endif /* PROTEUS_GENERALIZED_ROUTER_HPP */
