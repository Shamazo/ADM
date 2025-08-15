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

#ifndef PROTEUS_GPU_AGGR_MAT_EXPR_HPP
#define PROTEUS_GPU_AGGR_MAT_EXPR_HPP

#include <codegen/expressions/expressions.hpp>
#include <olap/operators/monoids.hpp>

/**
 * @class GpuAggrMatExpr
 *
 * @brief encapsulates an expression along with metadata about how that
 * expression should be materialized and aggregated.
 * @details a packet represents an indexed chunk of memory or a structure that
 * can contain multiple fields. Each packet can hold multiple expressions or
 * values that are materialized together.
 */
class GpuAggrMatExpr {
 public:
  expression_t expr;  /// The expression to evaluate
  size_t packet;      /// Packet/structure index
  size_t bitoffset;   /// Bit offset within packet
  size_t packind;     /// Field index within packet
  Monoid m;
  bool is_m;

  GpuAggrMatExpr(expression_t expr, size_t packet, size_t bitoffset, Monoid m)
      : expr(expr),
        packet(packet),
        bitoffset(bitoffset),
        packind(-1),
        m(m),
        is_m(true) {}

  GpuAggrMatExpr(expression_t expr, size_t packet, size_t bitoffset)
      : expr(expr),
        packet(packet),
        bitoffset(bitoffset),
        packind(-1),
        m(SUM),
        is_m(false) {}

  bool is_aggregation() const { return is_m; }
};

#endif /* PROTEUS_GPU_AGGR_MAT_EXPR_HPP */
