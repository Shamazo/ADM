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

#ifndef PROTEUS_BLOOM_FILTER_BUILD_HPP
#define PROTEUS_BLOOM_FILTER_BUILD_HPP

#include "bloom-filter.hpp"

class BloomFilterBuild : public BloomFilter {
 public:
  //  using BloomFilter::BloomFilter;
  BloomFilterBuild(std::shared_ptr<Operator> child, expression_t e,
                   size_t filterSize, uint64_t bloomId,
                   std::vector<uint32_t> copyToNumaNodes)
      : BloomFilter(child, e, filterSize, bloomId),
        copyToNumaNodes(std::move(copyToNumaNodes)) {}
  void produce_(OlapParallelContext *context) override;

  void consume(OlapParallelContext *context,
               const OperatorState &childState) override;

  [[nodiscard]] bool isFiltering() const override { return false; }
  const std::vector<uint32_t> copyToNumaNodes;
};

/**
 * reset the bloom filter registry and free any memory allocated to bloom
 * filters
 */
void cleanBloomFilterRegistry();

#endif /* PROTEUS_BLOOM_FILTER_BUILD_HPP */
