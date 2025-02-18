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

#include "prepared-queries.hpp"

// PreparedStatement prepare31_gpu_bloom_pd(proteus::QueryShaper &morph,
//                                          size_t bloomSize);

std::unique_ptr<Affinitizer> getAffinitizer() {
  //  if (getDevice() == DeviceType::GPU) {
  return std::make_unique<GPUAffinitizer>();
  //  } else {
  //      return std::make_unique<CpuNumaNodeAffinitizer>();
  //  }
}

PreparedStatement prepare31_gpu_bloom_pd(proteus::QueryShaper &morph,
                                         size_t bloomSize) {
  morph.setQueryName("ssb100_Q3_1_pd");
  auto date_scan = morph.scan("date", {"d_datekey", "d_year"});
  auto date_filtered = morph.distribute_probe(date_scan).unpack().filter(
      [&](const auto &arg) -> expression_t {
        return expressions::hint(
            ge(arg["d_year"], 1992) & le(arg["d_year"], 1997),
            expressions::Selectivity{6.0 / 7});
      });

  auto customer_scan =
      morph.scan("customer", {"c_custkey", "c_nation", "c_region"});

  auto customer_filtered =
      morph.distribute_probe(customer_scan)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["c_region"], "ASIA"),
                                     expressions::Selectivity{1.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["c_custkey"], arg["c_nation"]};
          });

  auto supplier_scan =
      morph.scan("supplier", {"s_suppkey", "s_nation", "s_region"});
  auto supplier_filter_build_bloom =
      supplier_scan
          .router(DegreeOfParallelism(1), 16, RoutingPolicy::LOCAL,
                  DeviceType::CPU, getAffinitizer())
          .memmove(24, DeviceType::CPU)
          .unpack()
          .filter([&](const auto &arg) -> expression_t {
            return expressions::hint(eq(arg["s_region"], "ASIA"),
                                     expressions::Selectivity{1.0 / 5});
          })
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["s_suppkey"], arg["s_nation"]};
          })
          .bloomfilter_build(
              [&](const auto &arg) -> expression_t { return arg["s_suppkey"]; },
              bloomSize, 1  // bloom filter ID
              )
          .project([&](const auto &arg) -> std::vector<expression_t> {
            return {arg["s_suppkey"], arg["s_nation"]};
          })
          .pack()
          .router(4, RoutingPolicy::LOCAL, DeviceType::GPU)
          .to_gpu()
          .unpack();

  auto lineorder_scan_filtered =
      morph
          .scan("lineorder",
                {"lo_custkey", "lo_suppkey", "lo_orderdate", "lo_revenue"})
          .router(DegreeOfParallelism(12), 16, RoutingPolicy::LOCAL,
                  DeviceType::CPU, getAffinitizer())
          .memmove(8, DeviceType::CPU)
          .bloomfilter_repack(
              [&](const auto &arg) { return arg["lo_suppkey"]; }, bloomSize, 1)
          .router(16, RoutingPolicy::LOCAL, DeviceType::GPU)
          .memmove(8, DeviceType::GPU)
          .to_gpu()
          .unpack();

  auto rel =
      lineorder_scan_filtered
          .join(
              customer_filtered,
              [&](const auto &build_arg) -> expression_t {
                return build_arg["c_custkey"];
              },
              [&](const auto &probe_arg) -> expression_t {
                return probe_arg["lo_custkey"];
              },
              20, 10485760)
          .join(
              supplier_filter_build_bloom,
              [&](const auto &build_arg) -> expression_t {
                return build_arg["s_suppkey"];
              },
              [&](const auto &probe_arg) -> expression_t {
                return probe_arg["lo_suppkey"];
              },
              16, 655360)
          .join(
              date_filtered,
              [&](const auto &build_arg) -> expression_t {
                return build_arg["d_datekey"];
              },
              [&](const auto &probe_arg) -> expression_t {
                return probe_arg["lo_orderdate"];
              },
              14, 2556)
          .groupby(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["c_nation"].as("PelagoAggregate#29307", "c_nation"),
                        arg["s_nation"].as("PelagoAggregate#29307", "s_nation"),
                        arg["d_year"].as("PelagoAggregate#29307", "d_year")};
              },
              [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                return {GpuAggrMatExpr{
                    (arg["lo_revenue"])
                        .as("PelagoAggregate#29307", "lo_revenue"),
                    1, 0, SUM}};
              },
              10, 131072)
          .pack()
          .to_cpu()
          .router(
              DegreeOfParallelism{1}, 128, RoutingPolicy::RANDOM,
              DeviceType::
                  CPU)  // (trait=[Pelago.[].packed.X86_64.homSingle.hetSingle])
          .memmove(8, DeviceType::CPU)
          .unpack()  // (trait=[Pelago.[].unpckd.NVPTX.homSingle.hetSingle])
          .groupby(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["c_nation"].as("PelagoAggregate#29313", "c_nation"),
                        arg["s_nation"].as("PelagoAggregate#29313", "s_nation"),
                        arg["d_year"].as("PelagoAggregate#29313", "d_year")};
              },
              [&](const auto &arg) -> std::vector<GpuAggrMatExpr> {
                return {GpuAggrMatExpr{
                    (arg["lo_revenue"])
                        .as("PelagoAggregate#29313", "lo_revenue"),
                    1, 0, SUM}};
              },
              10,
              131072)  // (group=[{0, 1, 2}], lo_revenue=[SUM($3)],
                       // trait=[Pelago.[].unpckd.NVPTX.homSingle.hetSingle])
          .sort(
              [&](const auto &arg) -> std::vector<expression_t> {
                return {arg["c_nation"], arg["s_nation"], arg["d_year"],
                        arg["lo_revenue"]};
              },
              {direction::NONE, direction::NONE, direction::ASC,
               direction::DESC})  // (sort0=[$2], sort1=[$3], dir0=[ASC],
                                  // dir1=[DESC], trait=[Pelago.[2, 3
                                  // DESC].unpckd.X86_64.homSingle.hetSingle])
          .print(pg("pm-csv"));
  return rel.prepare();
}
