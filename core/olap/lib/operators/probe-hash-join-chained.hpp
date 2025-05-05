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

#ifndef PROBE_HASH_JOIN_CHAINED_HPP_
#define PROBE_HASH_JOIN_CHAINED_HPP_

#include "hash-join-chained.hpp"

/**
 * @class ProbeHashJoinChained
 *
 * A variant of HashJoinChained that only performs the probe phase and uses
 * a pre-built hash table from another HashJoinChained(Morsel) operator.
 *
 * This allows multiple independent probe pipelines to share the same build hash
 * table, avoiding redundant work when the same hash table is needed in multiple
 * join operations.
 *
 * Usage pattern:
 * 1. Create a HashJoinChained for the build phase
 * 2. Create one or more ProbeHashJoinChained instances, passing the
 * HashJoinChained instance
 * 3. Each ProbeHashJoinChained will use the shared hash table created by
 * HashJoinChained
 *
 * Note: that the original HashJoinChained should be the used for the probe
 * phase. ProbeHashJoinChained should just be used for additional probes
 *
 * Note: ProbeHashJoinChained can only used used with HashJoinChainedMorsel
 * or a single threaded HashJoinChained.
 *
 * The ProbeHashJoinChained operators will wait for the HashJoinChained build
 * phase to complete before proceeding with their probe phase.
 */
class ProbeHashJoinChained : public experimental::UnaryOperator {
 public:
  ProbeHashJoinChained(std::shared_ptr<HashJoinChained> buildJoin,
                       std::vector<GpuMatExpr> probe_mat_exprs,
                       const std::vector<size_t> &probe_packet_widths,
                       expression_t probe_keyexpr,
                       std::shared_ptr<Operator> const probe_child,
                       string opLabel = "probe_hj_chained");
  ~ProbeHashJoinChained() override {
    LOG(INFO) << "Collapsing ProbeHashJoinChained operator";

    // Decrement reference count to the shared state
    if (buildState) {
      buildState->ref_count -= 1;
    }
  }

  void produce_(OlapParallelContext *context) override;
  void consume(OlapParallelContext *const context,
               const OperatorState &childState) override;

  void open_probe(Pipeline *pip);
  void close_probe(Pipeline *pip);

  bool isFiltering() const override { return true; }

  [[nodiscard]] proteus::traits::HomParallelization getHomParallelization()
      const override {
    return getChild()->getHomParallelization();
  }

  RecordType getRowType() const override {
    std::vector<RecordAttribute *> ret;

    for (const GpuMatExpr &mexpr : buildState->build_mat_exprs) {
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

 protected:
  void generate_probe(OlapParallelContext *context,
                      const OperatorState &childState);
  void probeHashTableFormat(OlapParallelContext *context);

  llvm::Value *hash(const expression_t &exprs, Context *const context,
                    const OperatorState &childState) const;

  std::vector<GpuMatExpr> build_mat_exprs;
  std::vector<GpuMatExpr> probe_mat_exprs;
  std::vector<size_t> build_packet_widths;
  expression_t build_keyexpr;
  expression_t probe_keyexpr;

  StateVar probe_head_param_id;
  std::vector<StateVar> in_param_ids;

  HashJoinBuildStatePtr buildState;
  int hash_bits;
  string opLabel;
};

#endif /* PROBE_HASH_JOIN_CHAINED_HPP_ */
