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

#ifndef OPERATORS_HPP_
#define OPERATORS_HPP_

#include <codegen/expressions/expressions.hpp>
#include <olap/util/parallel-context.hpp>
#include <platform/common/common.hpp>
#include <platform/topology/device-types.hpp>
#include <stduuid/uuid.hpp>
#include <utility>

#include "lib/plugins/output/plugins-output.hpp"
#include "llvm/IR/IRBuilder.h"
#include "olap/plugins/plugins.hpp"
#include "olap/routing/degree-of-parallelism.hpp"

// Fwd declaration
class Plugin;
class OperatorState;

namespace proteus::traits {

/**
 * Trait controlled by MemMoves (broadcast)
 */
enum class HomReplication {
  UNIQUE, /**< Each element exists in (exactly) one stream */
  BRDCST, /**< Each element exists in all streams */
};

/**
 * Trait controlled by Router
 */
enum class HomParallelization {
  SINGLE,   /**< There is only a single stream */
  PARALLEL, /**< Multiple streams running in parallel */
};
}  // namespace proteus::traits

class Operator {
 public:
  Operator() : m_parent(nullptr), m_id(uuids::uuid_system_generator{}()) {}
  virtual ~Operator() = default;
  virtual void setParent(Operator *parent) { this->m_parent = parent; }
  /**
   * Callee must ensure that the parent operator is not deallocated before using
   * this ptr
   * @return A non-owning pointer to the parent operator
   */
  Operator *const getParent() const { return m_parent; }
  // Overloaded operator used in checks for children of Join op. More complex
  // cases may require different handling
  bool operator==(
      const Operator &i) const { /*if(this != &i) LOG(INFO) << "NOT EQUAL
                                       OPERATORS"<<this<<" vs "<<&i;*/
    return this == &i;
  }

 protected:
  virtual void produce_(OlapParallelContext *context) = 0;
  const uuids::uuid m_id;

 public:
  virtual void produce(OlapParallelContext *context) final {
    // #ifndef NDEBUG
    //     auto * pip = context->getCurrentPipeline();
    // #endif
    produce_(context);
    //    assert(pip == context->getCurrentPipeline());
  }

  /**
   * Consume is not a const method because Nest does need to keep some state
   * info. Context needs to be passed from the consuming to the producing
   * side to kickstart execution once an HT has been built
   */
  virtual void consume(Context *const context,
                       const OperatorState &childState) = 0;

  [[nodiscard]] uuids::uuid getUUID() const { return m_id; }

  [[nodiscard]] virtual RecordType getRowType() const = 0;
  //  {
  //    // FIXME: throw an exception for now, but as soon as existing classes
  //    // implementat it, we should mark it function abstract
  //    throw runtime_error("unimplemented");
  //  }
  /* Used by caching service. Aim is finding whether data to be cached has been
   * filtered by some of the children operators of the plan */
  [[nodiscard]] virtual bool isFiltering() const = 0;

  // Traits

  [[nodiscard]] virtual DeviceType getDeviceType() const = 0;
  [[nodiscard]] virtual DegreeOfParallelism getDOP() const = 0;
  [[nodiscard]] virtual DegreeOfParallelism getDOPServers() const = 0;
  [[nodiscard]] virtual proteus::traits::HomReplication getHomReplication()
      const = 0;
  [[nodiscard]] virtual proteus::traits::HomParallelization
  getHomParallelization() const {
    return (getDOP() == 1) ? proteus::traits::HomParallelization::SINGLE
                           : proteus::traits::HomParallelization::PARALLEL;
  }
  [[nodiscard]] virtual bool isPacked() const = 0;

 private:
  Operator *m_parent;
};

class UnaryOperator : public Operator {
 public:
  /**
   * @param child child operator. Parents hold shared ownership of their
   * children
   */
  UnaryOperator(std::shared_ptr<Operator> child)
      : Operator(), m_child(std::move(child)) {}
  ~UnaryOperator() override = default;

  [[nodiscard]] virtual std::shared_ptr<Operator> getChild() const {
    return m_child;
  }
  void setChild(std::shared_ptr<Operator> child) {
    this->m_child = std::move(child);
  }

  [[nodiscard]] DeviceType getDeviceType() const override {
    return getChild()->getDeviceType();
  }

  [[nodiscard]] DegreeOfParallelism getDOP() const override {
    return getChild()->getDOP();
  }
  [[nodiscard]] DegreeOfParallelism getDOPServers() const override {
    return getChild()->getDOPServers();
  }
  [[nodiscard]] bool isPacked() const override {
    return getChild()->isPacked();
  }
  [[nodiscard]] proteus::traits::HomReplication getHomReplication()
      const override {
    return getChild()->getHomReplication();
  }

 private:
  std::shared_ptr<Operator> m_child;
};

class BinaryOperator : public Operator {
 public:
  /**
   * @note Parents hold shared ownership of their children
   */
  BinaryOperator(std::shared_ptr<Operator> leftChild,
                 std::shared_ptr<Operator> rightChild)
      : Operator(),
        m_leftChild(std::move(leftChild)),
        m_rightChild(std::move(rightChild)) {}
  BinaryOperator(std::shared_ptr<Operator> leftChild,
                 std::shared_ptr<Operator> rightChild, Plugin *const leftPlugin,
                 Plugin *const rightPlugin)
      : Operator(),
        m_leftChild(std::move(leftChild)),
        m_rightChild(std::move(rightChild)) {}
  ~BinaryOperator() override = default;
  [[nodiscard]] std::shared_ptr<Operator> getLeftChild() const {
    return m_leftChild;
  }
  [[nodiscard]] std::shared_ptr<Operator> getRightChild() const {
    return m_rightChild;
  }
  void setLeftChild(std::shared_ptr<Operator> leftChild) {
    this->m_leftChild = std::move(leftChild);
  }
  void setRightChild(std::shared_ptr<Operator> rightChild) {
    this->m_rightChild = std::move(rightChild);
  }

  [[nodiscard]] DeviceType getDeviceType() const override {
    auto dev = getLeftChild()->getDeviceType();
    CHECK(dev == getRightChild()->getDeviceType());
    return dev;
  }

  [[nodiscard]] DegreeOfParallelism getDOP() const override {
    auto dop = getLeftChild()->getDOP();
    CHECK_EQ(dop, getRightChild()->getDOP());
    return getRightChild()->getDOP();
  }

  [[nodiscard]] DegreeOfParallelism getDOPServers() const override {
    auto dop = getLeftChild()->getDOPServers();
    CHECK_EQ(dop, getRightChild()->getDOPServers());
    return dop;
  }

  [[nodiscard]] bool isPacked() const override {
    auto pckd = getLeftChild()->isPacked();
    CHECK_EQ(pckd, getRightChild()->isPacked());
    return pckd;
  }

 protected:
  std::shared_ptr<Operator> m_leftChild;
  std::shared_ptr<Operator> m_rightChild;
};

/**
 * Parallel operators
 */
namespace experimental {
template <typename T>
class POperator : public T {
 public:
  using T::T;

  void consume(Context *const context, const OperatorState &childState) final {
    auto ctx = dynamic_cast<OlapParallelContext *>(context);
    CHECK(ctx) << " context must be a OlapParallelContext for POperator";

    consume(ctx, childState);
  }

  virtual void consume(OlapParallelContext *context,
                       const OperatorState &childState) = 0;
};

class Operator : public POperator<::Operator> {
  using POperator::POperator;
};
class UnaryOperator : public POperator<::UnaryOperator> {
  using POperator::POperator;
};
class BinaryOperator : public POperator<::BinaryOperator> {
  using POperator::POperator;
};
}  // namespace experimental

#endif /* OPERATORS_HPP_ */
