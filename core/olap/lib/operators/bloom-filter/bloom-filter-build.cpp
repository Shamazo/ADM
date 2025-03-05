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

#include "bloom-filter-build.hpp"

#include <codegen/expressions/indexed-seq.hpp>
#include <lib/expressions/expressions-generator.hpp>

void BloomFilterBuild::produce_(OlapParallelContext *context) {
  auto t = getFilterType(context);

  filter_ptr = context->appendStateVar(
      t,
      [=](llvm::Value *) -> llvm::Value * {
        auto tarr = t->getPointerElementType();
        auto mem = context->allocateStateVar(tarr);
        context->CodegenMemset(
            mem, context->createInt8(0),
            (tarr->getArrayElementType()->getPrimitiveSizeInBits()) *
                tarr->getArrayNumElements());
        return mem;
      },
      [=](llvm::Value *pip, llvm::Value *s) {
        auto tarr = t->getPointerElementType();
        // note this overestimates by a factor of 8, as even for a single bit
        // the store size will still be a byte.
        // This is fine as it just means we end up overallocating when we copy
        // the filter to other nodes.
        const auto bloom_filter_size_bits =
            ((context->getModule()->getDataLayout().getTypeStoreSizeInBits(
                  tarr->getArrayElementType()))
                 .getFixedSize() *
             tarr->getArrayNumElements());
        CHECK_EQ(bloom_filter_size_bits % 8, 0);
        LOG(INFO) << "bf calculated size" << bloom_filter_size_bits / 8;

        context->gen_call("setBloomFilter",
                          {pip, s, context->createInt64(bloomId)}, t);
        for (const auto &target_node_id : copyToNumaNodes) {
          context->gen_call("copyBloomFilterToNode",
                            {pip, s, context->createInt64(bloomId),
                             context->createInt32(target_node_id),
                             context->createInt64(bloom_filter_size_bits / 8)},
                            t);
        }
      });

  getChild()->produce(context);
}

void BloomFilterBuild::consume(OlapParallelContext *context,
                               const OperatorState &childState) {
  auto ref = findInFilter(context, childState);

  ExpressionGeneratorVisitor v{context, childState};
  ref.assign(true).accept(v);

  getParent()->consume(context, childState);
}
