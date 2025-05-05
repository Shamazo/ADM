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

#ifndef GPU_MATERIALIZER_EXPR_HPP_
#define GPU_MATERIALIZER_EXPR_HPP_

#include <codegen/expressions/expressions.hpp>

/**
 * @struct GpuMatExpr
 *
 * Structure representing a materialization expression for serializing data in
 * memory. Despite its name, it is not used solely for GPU materialization. Used
 * in join operators, materializers, and other components that need to pack and
 * organize data into memory-efficient layouts.
 *
 * Materialization expressions help organize fields into packets (memory chunks)
 * for efficient storage and access, particularly important for hash joins where
 * data needs to be stored in a specific format.
 */
struct GpuMatExpr {
 public:
  expression_t expr;  ///< The expression to materialize (e.g., constant, or
                      ///< computed value)
  size_t packet;  ///< The packet/buffer index where this expression should be
                  ///< materialized
  size_t bitoffset;  ///< Bit offset within the packet where this expression's
                     ///< value starts
  size_t packind;    ///< Index within the packed struct after layout is
                     ///< determined This is initialized during code generation,
                     ///< not at construction time

  /**
   * Constructs a new GpuMatExpr with the given expression, packet index, and
   * bit offset.
   *
   * @param expr The expression to materialize
   * @param packet The packet/buffer index (used to group related fields)
   * @param bitoffset The bit offset within the packet where this value starts
   *
   * Note: packind is initialized to -1 and is set during code generation when
   * the final memory layout is determined (e.g., in
   * HashJoinChained::probeHashTableFormat).
   */
  GpuMatExpr(expression_t expr, size_t packet, size_t bitoffset)
      : expr(expr), packet(packet), bitoffset(bitoffset), packind(-1) {}
};

#endif /* GPU_MATERIALIZER_EXPR_HPP_ */
