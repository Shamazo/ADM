/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2019
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

#ifndef PROTEUS_ADM_PREPARED_QUERIES_HPP
#define PROTEUS_ADM_PREPARED_QUERIES_HPP

#include <olap/plan/prepared-statement.hpp>
#include <query-shaping/query-shaper.hpp>

PreparedStatement small_scan(proteus::QueryShaper &morph,
                             const std::string &lo_column);

PreparedStatement scan_two_columns(proteus::QueryShaper &morph,
                                   const std::string &lo_col1,
                                   const std::string &lo_col2);

PreparedStatement scan_six_columns(proteus::QueryShaper &morph,
                                   const std::string &lo_col1,
                                   const std::string &lo_col2,
                                   const std::string &lo_col3,
                                   const std::string &lo_col4,
                                   const std::string &lo_col5,
                                   const std::string &lo_col6);

#endif  // PROTEUS_ADM_PREPARED_QUERIES_HPP
