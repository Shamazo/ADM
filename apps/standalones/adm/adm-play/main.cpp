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

#include <cli-flags.hpp>
#include <codegen/expressions/expressionTypes.hpp>
#include <olap/operators/relbuilder-factory.hpp>
#include <platform/topology/affinity_manager.hpp>
#include <platform/topology/topology.hpp>
#include <query-shaping/nvme-shapers.hpp>
#include <ssb/query.hpp>

int main(int argc, char *argv[]) {
  auto ctx = proteus::from_cli::olap("Template", &argc, &argv);
  auto shaper = proteus::CPUOnlyNVMeMorsel({"/scratch2/nicholso/data/ssbm100"},
                                           "inputs/ssbm100",
                                           ssb::Query::getStats(100), true, 32);
  auto q11 = ssb::Query::prepare11(shaper);
  auto res = q11.execute();
  std::cout << res << std::endl;

  return 0;
}
