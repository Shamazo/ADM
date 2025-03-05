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

#ifndef PROTEUS_ROUTING_POLICY_TYPES_HPP
#define PROTEUS_ROUTING_POLICY_TYPES_HPP

enum class RoutingPolicy { RANDOM, LOCAL, FORCE_LOCAL, HASH_BASED };

/**
 * @see GeneralizedRouter
 */
enum class GeneralizedRoutingPolicy {
  /**
   * Random across all consumers and without regard for data locality
   */
  SHARED_RANDOM,
  /**
   * Random across all consumers but with regard for data locality
   */
  SHARED_LOCAL,
  /**
   * For now the same as SHARED_LOCAL
   */
  SHARED_FORCE_LOCAL,
  /**
   * Hash-based routing with consumers sharing queues. This is not implemented
   */
  SHARED_HASH_BASED,
  /**
   * Random across consumers but force use of a data local queue. Unlike
   * SHARED_LOCAL, this policy uses separate queues per consumer. This policy
   * will retry across  consumers, but each retry will use a data local queue
   */
  DISTINCT_RANDOM_SPLIT_FORCE_DATA_LOCAL,
  /**
   * Random across consumers but prefer use of a data local queue. Unlike
   * SHARED_LOCAL, this policy uses separate queues per consumer. This policy
   * will retry across consumers and retries will then use a random queue of the
   * consumer
   */
  DISTINCT_RANDOM_SPLIT_PREFER_DATA_LOCAL,
  DISTINCT_THROUGHPUT_SPLIT_PREFER_DATA_LOCAL
};

#endif  // PROTEUS_ROUTING_POLICY_TYPES_HPP
