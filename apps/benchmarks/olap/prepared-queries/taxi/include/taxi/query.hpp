/*
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

#ifndef PROTEUS_TAXI_QUERY_HPP
#define PROTEUS_TAXI_QUERY_HPP

#include <olap/plan/prepared-statement.hpp>
#include <query-shaping/input-prefix-query-shaper.hpp>
#include <query-shaping/query-shaper.hpp>

namespace taxi {
class Query {
 public:
  static PreparedStatement prepare1(proteus::QueryShaper &morph,
                                    double trip_distance_min,
                                    double trip_distance_max);
  // approx 0.1% selectivity on trip_distance
  static PreparedStatement prepare11(proteus::QueryShaper &morph) {
    morph.setQueryName("taxi_q11");
    return prepare1(morph, 0.0, 0.04);
  }
  // approx 1% selectivity on trip_distance
  static PreparedStatement prepare12(proteus::QueryShaper &morph) {
    morph.setQueryName("taxi_q12");
    return prepare1(morph, 0.0, 0.29);
  }
  // approx 10% selectivity on trip_distance
  static PreparedStatement prepare13(proteus::QueryShaper &morph) {
    morph.setQueryName("taxi_q13");
    return prepare1(morph, 0.0, 0.69);
  }
  // approx 30% selectivity on trip_distance
  static PreparedStatement prepare14(proteus::QueryShaper &morph) {
    morph.setQueryName("taxi_q14");
    return prepare1(morph, 0.0, 1.16);
  }

  static PreparedStatement prepare2(proteus::QueryShaper &morph,
                                    double fare_amount_min,
                                    double fare_amount_max);
  // approx 0.1% selectivity on fare_amount
  static PreparedStatement prepare21(proteus::QueryShaper &morph) {
    morph.setQueryName("taxi_q21");
    return prepare2(morph, 0.0, 2.5);
  }
  // approx 1% selectivity on fare_amount
  static PreparedStatement prepare22(proteus::QueryShaper &morph) {
    morph.setQueryName("taxi_q22");
    return prepare2(morph, 0.0, 3.3);
  }
  // approx 10% selectivity on fare_amount
  static PreparedStatement prepare23(proteus::QueryShaper &morph) {
    morph.setQueryName("taxi_q23");
    return prepare2(morph, 0.0, 5.0);
  }
  // approx 35% selectivity on fare_amount
  static PreparedStatement prepare24(proteus::QueryShaper &morph) {
    morph.setQueryName("taxi_q24");
    return prepare2(morph, 0.0, 7.5);
  }

  static std::map<std::string,
                  std::function<double(proteus::InputPrefixQueryShaper &)>>
  getStats();
};

}  // namespace taxi

#endif  // PROTEUS_TAXI_QUERY_HPP
