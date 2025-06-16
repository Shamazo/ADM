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

#ifndef PROTEUS_ROUTING_POLICY_FACTORY_HPP
#define PROTEUS_ROUTING_POLICY_FACTORY_HPP

#include <functional>
#include <memory>
#include <olap/routing/routing-policy-types-v2.hpp>
#include <unordered_map>

#include "routing-policy-v2.hpp"

namespace proteus::routing {

/**
 * Factory for creating V2 routing policy instances.
 * Uses singleton pattern with variant-based configuration for simplicity.
 * Only deals with RoutingPolicyV2 to avoid conflicts with legacy RoutingPolicy.
 */
class RoutingPolicyFactory {
public:
  using PolicyCreator = std::function<std::unique_ptr<RoutingPolicyV2>(
      const PolicyConfigVariant&)>;

  /**
   * Get the singleton factory instance.
   */
  static RoutingPolicyFactory& getInstance() {
    static RoutingPolicyFactory instance;
        return instance;
    }
    
    /**
     * Create a routing policy instance for the given policy type.
     * @param policy_type The policy type to create
     * @param config Variant configuration for the policy (defaults to no
     * config)
     * @return Unique pointer to the created policy, or nullptr if not found
     */
    std::unique_ptr<RoutingPolicyV2> createPolicy(
        GeneralizedRoutingPolicyV2 policy_type,
        const PolicyConfigVariant& config = std::monostate{}) const {
      auto it = creators_.find(policy_type);
      if (it != creators_.end()) {
        return it->second(config);
      }
      return nullptr;
    }

    /**
     * Register a policy creator function for a specific policy type.
     * @param policy_type The policy type to register
     * @param creator Function that creates instances of the policy with variant
     * config
     */
    void registerPolicy(GeneralizedRoutingPolicyV2 policy_type, PolicyCreator creator) {
        creators_[policy_type] = std::move(creator);
    }
    
    /**
     * Check if a policy type is registered.
     * @param policy_type The policy type to check
     * @return true if registered, false otherwise
     */
    bool isRegistered(GeneralizedRoutingPolicyV2 policy_type) const {
        return creators_.find(policy_type) != creators_.end();
    }

private:
    RoutingPolicyFactory() = default;
    std::unordered_map<GeneralizedRoutingPolicyV2, PolicyCreator> creators_;
};

/**
 * Macro for automatic policy registration using variant-based configuration.
 * Place this in the .cpp file for each policy implementation.
 *
 * Example usage:
 *   REGISTER_ROUTING_POLICY(RoundRobinPolicy,
 * GeneralizedRoutingPolicyV2::ROUND_ROBIN)
 */
#define REGISTER_ROUTING_POLICY(PolicyClass, PolicyType)                  \
  static auto g_##PolicyClass##_registrar = []() {                        \
    proteus::routing::RoutingPolicyFactory::getInstance().registerPolicy( \
        PolicyType,                                                       \
        [](const proteus::routing::PolicyConfigVariant& config)           \
            -> std::unique_ptr<proteus::routing::RoutingPolicyV2> {       \
          return std::make_unique<PolicyClass>(config);                   \
        });                                                               \
    return true;                                                          \
  }();

}  // namespace proteus::routing

#endif  // PROTEUS_ROUTING_POLICY_FACTORY_HPP
