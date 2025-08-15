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

#ifndef PROTEUS_ROUTING_POLICY_TYPES_V2_HPP
#define PROTEUS_ROUTING_POLICY_TYPES_V2_HPP
namespace proteus::routing {

/**
 * V2 Routing Policy Types for GeneralizedRouter refactoring.
 * This enum provides clean, descriptive policy names for the new FFI-based
 * routing architecture that moves policy decisions out of JIT code.
 */
enum class GeneralizedRoutingPolicyV2 {
    ROUND_ROBIN,
    LOCALITY_AWARE,
    LOCALITY_AWARE_WITH_RANDOM_CONS_RETRY,
    LOCALITY_AWARE_BACKPRESSURE_AWARE,
    THROUGHPUT_BASED,
    RANDOM_SPLIT, /// not implemented
    ML_BASED /// not implemented
};

}  // namespace proteus::routing

#endif  // PROTEUS_ROUTING_POLICY_TYPES_V2_HPP
