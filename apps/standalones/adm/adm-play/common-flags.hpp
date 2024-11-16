/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2024
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

#ifndef PROTEUS_COMMON_FLAGS_HPP
#define PROTEUS_COMMON_FLAGS_HPP
#include <gflags/gflags.h>

DECLARE_int32(server_number);
DEFINE_int32(server_number, 46, "server number (DIAS internal)");

DECLARE_int32(num_iterations);
DEFINE_int32(num_iterations, 5, "Number of types to run each query");

DECLARE_string(result_file);
DEFINE_string(result_file, "",
              "[optional] output file for results [default: stdout]");

DECLARE_string(timestamp_file);
DEFINE_string(timestamp_file, "adm-timestamps.csv",
              "[optional] output file for TimeStampLogger logs  [default: "
              "adm-timestamps.csv]");

#endif  // PROTEUS_COMMON_FLAGS_HPP
