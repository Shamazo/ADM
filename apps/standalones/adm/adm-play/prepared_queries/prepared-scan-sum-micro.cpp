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

#include <olap/operators/relbuilder-factory.hpp>
#include <olap/operators/relbuilder.hpp>

#include "prepared-queries.hpp"

PreparedStatement scan_sum_micro_pushdown(proteus::QueryShaper &morph,
                                          double selectivity,
                                          bool move_after_pushdown) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro_pushdown");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "Query upperbound: " << query_upperbound;

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto filter_pip =
      morph.distribute_probe(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .pack()
          .router(morph.getDOP(), morph.getSlack(), RoutingPolicy::LOCAL,
                  morph.getDevice(), morph.getAffinitizer());
  if (move_after_pushdown || morph.getDevice() == DeviceType::GPU) {
    filter_pip = filter_pip.memmove(morph.getSlack(), morph.getDevice());
  }
  if (morph.getDevice() == DeviceType::GPU) {
    filter_pip = filter_pip.to_gpu();
  }

  auto reduce_threads = filter_pip.unpack().reduce(
      [&](const auto &arg) -> std::vector<expression_t> {
        return {arg["col2"]};
      },
      {SUM});

  // final reduction after per thread partial reduction
  return morph.collect(reduce_threads)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_sum_micro(proteus::QueryShaper &morph,
                                 double selectivity) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "query_upperbound: " << query_upperbound;

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto filter_sum_pip =
      morph.distribute_probe(scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["col2"]};
              },
              {SUM});

  // final reduction after per thread partial reduction
  return morph.collect(filter_sum_pip)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_sum_micro_adaptive(proteus::QueryShaper &morph,
                                          double selectivity,
                                          DegreeOfParallelism pushdown_dop,
                                          int scan_slack,
                                          GeneralizedRoutingPolicy policy,
                                          uint64_t throughput_sample_size,
                                          uint32_t skip_samples) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro_adaptive");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "query_upperbound: " << query_upperbound;
  CHECK_GT(pushdown_dop, 0);

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto split =
      scan.gsplit(scan_slack, policy, throughput_sample_size, skip_samples);
  auto &topo = topology::getInstance();
  auto socket_1_nodes = topo.getCpuNumaNodesByPackageId(1);
  std::vector<uint32_t> socket_1_node_ids(socket_1_nodes.size());
  std::transform(socket_1_nodes.begin(), socket_1_nodes.end(),
                 socket_1_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  const size_t count_per_numa_cores =
      socket_1_nodes[0].get().local_cores.size();

  auto socket_0_nodes = topo.getCpuNumaNodesByPackageId(0);
  std::vector<uint32_t> socket_0_node_ids(socket_0_nodes.size());
  std::transform(socket_0_nodes.begin(), socket_0_nodes.end(),
                 socket_0_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  auto pushdown_path = split.path(
      DeviceType::CPU, pushdown_dop,
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_0_node_ids));
  pushdown_path =
      pushdown_path.memmove(192/pushdown_dop, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {(arg["col2"])};
          })
          .pack();

  auto staging_path = split.path(
      DeviceType::CPU,
      DegreeOfParallelism{count_per_numa_cores * socket_1_node_ids.size()},
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids));
  staging_path =
      staging_path.memmove(4, DeviceType::CPU, std::vector<bool>{false, false})
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {(arg["col2"])};
          })
          .pack();

  auto standard_path = split.path(
      DeviceType::CPU,
      DegreeOfParallelism{count_per_numa_cores * socket_1_nodes.size()},
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids));
  standard_path =
      standard_path.memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {(arg["col2"])};
          })
          .pack();

  return pushdown_path
      .unionAll(
          {staging_path, standard_path},
          DegreeOfParallelism{count_per_numa_cores * socket_1_node_ids.size()},
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids),
          2)
      .unpack()
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .router(
          DegreeOfParallelism{1}, 64, RoutingPolicy::LOCAL, DeviceType::CPU,
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids))
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_sum_micro_grouter_staging(
    proteus::QueryShaper &morph, double selectivity,
    DegreeOfParallelism pushdown_dop, int scan_slack,
    GeneralizedRoutingPolicy policy) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro_adaptive");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "query_upperbound: " << query_upperbound;
  CHECK_GT(pushdown_dop, 0);

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto split = scan.gsplit(scan_slack, policy);
  auto &topo = topology::getInstance();
  auto socket_1_nodes = topo.getCpuNumaNodesByPackageId(1);
  std::vector<uint32_t> socket_1_node_ids(socket_1_nodes.size());
  std::transform(socket_1_nodes.begin(), socket_1_nodes.end(),
                 socket_1_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  const size_t count_per_numa_cores =
      socket_1_nodes[0].get().local_cores.size();

  auto socket_0_nodes = topo.getCpuNumaNodesByPackageId(0);
  std::vector<uint32_t> socket_0_node_ids(socket_0_nodes.size());
  std::transform(socket_0_nodes.begin(), socket_0_nodes.end(),
                 socket_0_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  auto staging_path = split.path(
      DeviceType::CPU,
      DegreeOfParallelism{count_per_numa_cores * socket_1_node_ids.size()},
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids));
  staging_path =
      staging_path.memmove(4, DeviceType::CPU, std::vector<bool>{false, false})
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {(arg["col2"])};
          })
          .pack();

  return staging_path
      .unionAll(
          {},
          DegreeOfParallelism{count_per_numa_cores * socket_1_node_ids.size()},
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids),
          2)
      .unpack()
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .router(
          DegreeOfParallelism{1}, 128, RoutingPolicy::RANDOM, DeviceType::CPU,
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids))
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_sum_micro_grouter_staging_partial_reduction(
    proteus::QueryShaper &morph, double selectivity,
    DegreeOfParallelism pushdown_dop, int scan_slack,
    GeneralizedRoutingPolicy policy) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro_adaptive");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "query_upperbound: " << query_upperbound;
  CHECK_GT(pushdown_dop, 0);

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto split = scan.gsplit(scan_slack, policy);
  auto &topo = topology::getInstance();
  auto socket_1_nodes = topo.getCpuNumaNodesByPackageId(1);
  std::vector<uint32_t> socket_1_node_ids(socket_1_nodes.size());
  std::transform(socket_1_nodes.begin(), socket_1_nodes.end(),
                 socket_1_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  const size_t count_per_numa_cores =
      socket_1_nodes[0].get().local_cores.size();

  auto socket_0_nodes = topo.getCpuNumaNodesByPackageId(0);
  std::vector<uint32_t> socket_0_node_ids(socket_0_nodes.size());
  std::transform(socket_0_nodes.begin(), socket_0_nodes.end(),
                 socket_0_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  auto staging_path = split.path(
      DeviceType::CPU,
      DegreeOfParallelism{count_per_numa_cores * socket_1_node_ids.size()},
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids));
  staging_path =
      staging_path.memmove(8, DeviceType::CPU, std::vector<bool>{false, false})
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["col2"]};
              },
              {SUM});

  return staging_path
      .unionAll(
          {}, DegreeOfParallelism{1},
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids),
          128)
      //      .reduce(
      //          [&](const auto &arg) -> std::vector<expression_t> {
      //            return {arg["col2"]};
      //          },
      //          {SUM})
      //      .router(
      //          DegreeOfParallelism{1}, 128, RoutingPolicy::RANDOM,
      //          DeviceType::CPU,
      //          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids))
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_sum_micro_grouter_pushdown(
    proteus::QueryShaper &morph, double selectivity,
    DegreeOfParallelism pushdown_dop, int scan_slack,
    GeneralizedRoutingPolicy policy) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro_adaptive");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "query_upperbound: " << query_upperbound;
  CHECK_GT(pushdown_dop, 0);

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto split = scan.gsplit(scan_slack, policy);
  auto &topo = topology::getInstance();
  auto socket_1_nodes = topo.getCpuNumaNodesByPackageId(1);
  std::vector<uint32_t> socket_1_node_ids(socket_1_nodes.size());
  std::transform(socket_1_nodes.begin(), socket_1_nodes.end(),
                 socket_1_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  const size_t count_per_numa_cores =
      socket_1_nodes[0].get().local_cores.size();

  auto socket_0_nodes = topo.getCpuNumaNodesByPackageId(0);
  std::vector<uint32_t> socket_0_node_ids(socket_0_nodes.size());
  std::transform(socket_0_nodes.begin(), socket_0_nodes.end(),
                 socket_0_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  auto pushdown_path = split.path(
      DeviceType::CPU, pushdown_dop,
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_0_node_ids));
  pushdown_path =
      pushdown_path.memmove(192/pushdown_dop, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {(arg["col2"])};
          })
          .pack();

  return pushdown_path
      .unionAll(
          {},
          DegreeOfParallelism{count_per_numa_cores * socket_1_node_ids.size()},
//          DegreeOfParallelism{1},
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids), 2)
      .unpack()
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .router(
          DegreeOfParallelism{1}, 64, RoutingPolicy::LOCAL, DeviceType::CPU,
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids))
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_sum_micro_grouter_direct(
    proteus::QueryShaper &morph, double selectivity,
    DegreeOfParallelism pushdown_dop, int scan_slack,
    GeneralizedRoutingPolicy policy) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro_adaptive");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "query_upperbound: " << query_upperbound;
  CHECK_GT(pushdown_dop, 0);

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto split = scan.gsplit(scan_slack, policy);
  auto &topo = topology::getInstance();
  auto socket_1_nodes = topo.getCpuNumaNodesByPackageId(1);
  std::vector<uint32_t> socket_1_node_ids(socket_1_nodes.size());
  std::transform(socket_1_nodes.begin(), socket_1_nodes.end(),
                 socket_1_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  const size_t count_per_numa_cores =
      socket_1_nodes[0].get().local_cores.size();

  auto socket_0_nodes = topo.getCpuNumaNodesByPackageId(0);
  std::vector<uint32_t> socket_0_node_ids(socket_0_nodes.size());
  std::transform(socket_0_nodes.begin(), socket_0_nodes.end(),
                 socket_0_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  auto standard_path = split.path(
      DeviceType::CPU,
      DegreeOfParallelism{count_per_numa_cores * socket_1_nodes.size()},
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids));
  standard_path =
      standard_path.memmove(4, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {(arg["col2"])};
          })
          .pack();

  return standard_path
      .unionAll(
          {},
          DegreeOfParallelism{count_per_numa_cores * socket_1_node_ids.size()},
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids),
          2)
      .unpack()
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .router(
          DegreeOfParallelism{1}, 64, RoutingPolicy::LOCAL, DeviceType::CPU,
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids))
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}

PreparedStatement scan_sum_micro_adaptivev2(proteus::QueryShaper &morph,
                                            double selectivity,
                                            DegreeOfParallelism pushdown_dop,
                                            int scan_slack,
                                            GeneralizedRoutingPolicy policy) {
  CHECK_GT(selectivity, 0.0);
  CHECK_LE(selectivity, 1.0);
  morph.setQueryName("scan_sum_micro_adaptive");
  /// by construction, generated data is uniform in this range
  constexpr int data_upperbound = 10000;
  const int query_upperbound =
      std::round(static_cast<double>(data_upperbound) * selectivity);
  CHECK_GT(query_upperbound, 0);
  CHECK_LE(query_upperbound, data_upperbound);
  LOG(INFO) << "query_upperbound: " << query_upperbound;
  CHECK_GT(pushdown_dop, 0);

  auto scan = morph.scan("random_ints_100GB_10000", {"col1", "col2"});
  auto split = scan.gsplit(scan_slack, policy);
  auto &topo = topology::getInstance();
  auto socket_1_nodes = topo.getCpuNumaNodesByPackageId(1);
  std::vector<uint32_t> socket_1_node_ids(socket_1_nodes.size());
  std::transform(socket_1_nodes.begin(), socket_1_nodes.end(),
                 socket_1_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  const size_t count_per_numa_cores =
      socket_1_nodes[0].get().local_cores.size();

  auto socket_0_nodes = topo.getCpuNumaNodesByPackageId(0);
  std::vector<uint32_t> socket_0_node_ids(socket_0_nodes.size());
  std::transform(socket_0_nodes.begin(), socket_0_nodes.end(),
                 socket_0_node_ids.begin(),
                 [](std::reference_wrapper<const topology::cpunumanode> node) {
                   return node.get().id;
                 });

  auto pushdown_path = split.path(
      DeviceType::CPU, pushdown_dop,
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_0_node_ids));
  pushdown_path =
      pushdown_path.memmove(8, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .pack()
          .router(DegreeOfParallelism{count_per_numa_cores *
                                      socket_1_node_ids.size()},
                  8, RoutingPolicy::LOCAL, DeviceType::CPU,
                  std::make_unique<SpecificCpuNumaNodeAffinitizer>(
                      socket_1_node_ids))
          .unpack()
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["col2"]};
              },
              {SUM});

  auto staging_path = split.path(
      DeviceType::CPU,
      DegreeOfParallelism{count_per_numa_cores * socket_1_node_ids.size()},
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids));
  staging_path =
      staging_path.memmove(2, DeviceType::CPU, std::vector<bool>{false, false})
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["col2"]};
              },
              {SUM});

  auto standard_path = split.path(
      DeviceType::CPU,
      DegreeOfParallelism{count_per_numa_cores * socket_1_nodes.size()},
      std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids));
  standard_path =
      standard_path.memmove(2, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(lt(arg["col1"], query_upperbound),
                                     expressions::Selectivity(selectivity));
          })
          .reduce(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["col2"]};
              },
              {SUM});

  return pushdown_path
      .unionAll(
          {staging_path, standard_path}, DegreeOfParallelism{1},
          std::make_unique<SpecificCpuNumaNodeAffinitizer>(socket_1_node_ids),
          64)
      .reduce(
          [&](const auto &arg) -> std::vector<expression_t> {
            return {arg["col2"]};
          },
          {SUM})
      .print(pg{"pm-csv"})
      .prepare();
}
