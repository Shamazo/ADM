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

#ifndef HASH_JOIN_CHAINED_HPP_
#define HASH_JOIN_CHAINED_HPP_

#include <atomic>
#include <memory>
#include <unordered_map>

#include "olap/operators/gpu/gpu-materializer-expr.hpp"
#include "olap/util/parallel-context.hpp"
#include "operators.hpp"

/**
 * @class HashJoinBuildState
 *
 * Shared state for hash join operations that need to be shared between
 * different operators (e.g., HashJoinChained and ProbeHashJoinChained).
 *
 * This is used when multiple probe pipelines need to share the same build hash
 * table. The class contains both runtime state (memory pointers, counters) and
 * code generation state (materialization expressions with initialized packind
 * values).
 *
 * This state is initially created by HashJoinChained with placeholder values,
 * then fully populated during the build phase, and finally used by
 * ProbeHashJoinChained instances to access the shared hash table.
 */
class HashJoinBuildState {
 public:
  HashJoinBuildState(uint32_t *head_array, std::vector<void *> data_arrays,
                     std::atomic<size_t> *counter, int hash_bits,
                     size_t max_build_size)
      : head_array(head_array),
        data_arrays(std::move(data_arrays)),
        counter(counter),
        hash_bits(hash_bits),
        max_build_size(max_build_size),
        build_complete(false),
        ref_count(0) {}

  ~HashJoinBuildState() {
    // Memory is freed by the original HashJoinChained operator
    // to avoid double-free issues
  }

  // State data
  uint32_t *head_array;             // Hash table head pointers
  std::vector<void *> data_arrays;  // Build record storage
  std::atomic<size_t> *counter;     // Atomic counter for record allocation

  // Configuration
  int hash_bits;          // Number of hash bits used
  size_t max_build_size;  // Maximum capacity of the build table

  // Code generation state
  std::vector<GpuMatExpr>
      build_mat_exprs;  // Materialization expressions with initialized packind
  std::vector<size_t> build_packet_widths;  // Width of each packet in bits

  // Synchronization
  std::atomic<bool> build_complete;  // Flag to indicate build phase completion
  std::atomic<int> ref_count;        // Number of operators using this state
};

// Convenience typedef for shared pointers to HashJoinBuildState
using HashJoinBuildStatePtr = std::shared_ptr<HashJoinBuildState>;

class HashJoinChained : public BinaryOperator {
 public:
  HashJoinChained(std::vector<GpuMatExpr> build_mat_exprs,
                  std::vector<size_t> build_packet_widths,
                  expression_t build_keyexpr,
                  std::shared_ptr<Operator> const build_child,
                  std::vector<GpuMatExpr> probe_mat_exprs,
                  const std::vector<size_t> &probe_packet_widths,
                  expression_t probe_keyexpr,
                  std::shared_ptr<Operator> const probe_child, int hash_bits,
                  size_t maxBuildInputSize, string opLabel = "hj_chained");
  ~HashJoinChained() override {
    DLOG(INFO) << "Collapsing HashJoinChained operator";
  }

  void produce_(OlapParallelContext *context) override;
  virtual void consume(OlapParallelContext *const context,
                       const OperatorState &childState);
  void consume(Context *const context,
               const OperatorState &childState) override;

  virtual void open_probe(Pipeline *pip);
  virtual void open_build(Pipeline *pip);
  virtual void close_probe(Pipeline *pip);
  virtual void close_build(Pipeline *pip);

  // Get the shared build state for use by other probe operators
  [[nodiscard]] HashJoinBuildStatePtr getBuildState() const {
    return buildState;
  }

  bool isFiltering() const override { return true; }

  RecordType getRowType() const override {
    std::vector<RecordAttribute *> ret;

    for (const GpuMatExpr &mexpr : build_mat_exprs) {
      if (mexpr.packet == 0 && mexpr.packind == 0) continue;
      ret.emplace_back(new RecordAttribute(mexpr.expr.getRegisteredAs()));
    }

    for (const GpuMatExpr &mexpr : probe_mat_exprs) {
      if (mexpr.packet == 0 && mexpr.packind == 0) continue;
      ret.emplace_back(new RecordAttribute(mexpr.expr.getRegisteredAs()));
    }

    if (build_keyexpr.isRegistered()) {
      ret.emplace_back(new RecordAttribute(build_keyexpr.getRegisteredAs()));
    }

    if (build_keyexpr.getExpressionType()->getTypeID() == RECORD) {
      auto rc = dynamic_cast<const expressions::RecordConstruction *>(
          build_keyexpr.getUnderlyingExpression());

      for (const auto &a : rc->getAtts()) {
        auto e = a.getExpression();
        if (e.isRegistered()) {
          ret.emplace_back(new RecordAttribute(e.getRegisteredAs()));
        }
      }
    }

    if (probe_keyexpr.isRegistered()) {
      ret.emplace_back(new RecordAttribute(probe_keyexpr.getRegisteredAs()));
    }

    if (probe_keyexpr.getExpressionType()->getTypeID() == RECORD) {
      auto rc = dynamic_cast<const expressions::RecordConstruction *>(
          probe_keyexpr.getUnderlyingExpression());

      for (const auto &a : rc->getAtts()) {
        auto e = a.getExpression();
        if (e.isRegistered()) {
          ret.emplace_back(new RecordAttribute(e.getRegisteredAs()));
        }
      }
    }

    return ret;
  }

  [[nodiscard]] proteus::traits::HomReplication getHomReplication()
      const override {
    return getRightChild()->getHomReplication();
  }

 protected:
  void generate_build(OlapParallelContext *context,
                      const OperatorState &childState);
  void generate_probe(OlapParallelContext *context,
                      const OperatorState &childState);
  void buildHashTableFormat(OlapParallelContext *context);
  void probeHashTableFormat(OlapParallelContext *context);

  virtual llvm::Value *nextIndex(OlapParallelContext *context);
  virtual llvm::Value *replaceHead(OlapParallelContext *context,
                                   llvm::Value *h_ptr, llvm::Value *index);

  llvm::Value *hash(const expression_t &exprs, Context *const context,
                    const OperatorState &childState) const;

  std::vector<GpuMatExpr> build_mat_exprs;
  std::vector<GpuMatExpr> probe_mat_exprs;
  std::vector<size_t> build_packet_widths;
  expression_t build_keyexpr;

  expression_t probe_keyexpr;

  StateVar head_param_id;
  std::vector<StateVar> out_param_ids;
  std::vector<StateVar> in_param_ids;
  StateVar cnt_param_id;

  StateVar probe_head_param_id;

  int hash_bits;
  size_t maxBuildInputSize;

  string opLabel;

  // Shared build state
  HashJoinBuildStatePtr buildState;

  // std::unordered_map<int32_t, std::vector<void *>> confs;
  std::vector<void *> confs[256];

  // Make ProbeHashJoinChained a friend class to access protected members
  friend class ProbeHashJoinChained;
};

#endif /* HASH_JOIN_CHAINED_HPP_ */
