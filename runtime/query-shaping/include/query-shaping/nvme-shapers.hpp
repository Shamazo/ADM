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
#ifndef PROTEUS_NVME_SHAPERS_HPP
#define PROTEUS_NVME_SHAPERS_HPP
#include <codegen/expressions/expressionTypes.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <query-shaping/input-prefix-query-shaper.hpp>
#include <utility>
#include <vector>

namespace proteus {

class CPUOnlyNVMeMorsel : public proteus::InputPrefixQueryShaper {
  [[nodiscard]] pg getPlugin() const override { return pg{"nvme-block"}; }
  [[nodiscard]] DeviceType getDevice() override { return DeviceType::CPU; }

  [[nodiscard]] std::string getRelName(const std::string &base) override {
    LOG(FATAL) << "N/A to NVMe shapers";
  }



  RelBuilder distribute_probe(RelBuilder input) override {
    auto rel = input.router(getDOP(), scan_router_slack, RoutingPolicy::LOCAL,
                            getDevice(), getAffinitizer());

    if (doMove())
      rel = rel.memmove(scan_memmove_slack, getDevice(), do_transfer);

    if (getDevice() == DeviceType::GPU) rel = rel.to_gpu();

    return rel;
  }

 public:
  CPUOnlyNVMeMorsel(
      std::vector<std::string> input_dirs, std::string catalog_path,
      decltype(input_sizes) input_sizes, bool allowMoves,
      size_t scan_memmove_slack, size_t scan_router_slack, size_t slack,
      std::optional<std::vector<bool>> do_transfer = std::nullopt,
      std::optional<std::vector<uint32_t>> numa_nodes = std::nullopt)
      : InputPrefixQueryShaper("N/A", std::move(input_sizes), allowMoves,
                               slack),
        input_dirs(sort_vector(std::move(
            input_dirs))),  // sort so we always iterate in the same order
        catalog_path(std::move(catalog_path)),
        scan_memmove_slack(scan_memmove_slack),
        scan_router_slack(scan_router_slack),
        do_transfer(do_transfer),
        numa_nodes(std::move(std::move(numa_nodes))) {}

  RelBuilder distribute_build(RelBuilder input) override {
    auto rel = input
                   .router(getDOP(), scan_router_slack, RoutingPolicy::LOCAL,
                           getDevice(), getAffinitizer())
                   .memmove(scan_memmove_slack, getDevice());

    if (getDevice() == DeviceType::GPU) rel = rel.to_gpu();

    return rel;
  }

  /**
   * @param relName in the legacy catalog, e.g. inputs/ssbm100/lineorder.csv
   * This is very very hacky dealing with types. We need to come back and fix
   * the catalog for this or create an actual file format
   */
  [[nodiscard]] RelBuilder scan(
      const std::string &relName,
      std::initializer_list<std::string> relAttrs) override;

  std::unique_ptr<Affinitizer> getAffinitizer() override {
    if (numa_nodes.has_value()) {
      LOG(INFO) << "using SpecificCpuNumaNodeAffinitizer";
      return std::make_unique<SpecificCpuNumaNodeAffinitizer>(
          numa_nodes.value());
    } else {
      return std::make_unique<CpuNumaNodeAffinitizer>();
    }
  }

  DegreeOfParallelism getDOP() override {
    if (numa_nodes.has_value()) {
      size_t num_compute_cores = 0;
      for (const auto compute_node_id : numa_nodes.value()) {
        num_compute_cores += topology::getInstance()
                                 .getCpuNumaNodeById(compute_node_id)
                                 .local_cores.size();
        LOG(INFO) << "compute_node_id: " << compute_node_id
                  << "  num cores: " << num_compute_cores;
      }
      return DegreeOfParallelism{num_compute_cores};
    } else {
      return DegreeOfParallelism{topology::getInstance().getCoreCount()};
    }
  }

 protected:
  [[nodiscard]] std::vector<std::filesystem::path> getMdForAttribute(
      const std::string &attr) const;

  /**
   * gross and hacky helper to construct dangling attributes for a relation and
   * vector of requested attributes using the legacy catalogue
   */
  [[nodiscard]] std::vector<DanglingAttr> constructDanglingAttrs(
      const std::string &relName,
      const std::vector<std::string> &relAttrs) const;

  const std::vector<std::string> input_dirs;
  const std::string catalog_path;  // the path to the legacy catalog file
 private:
  static std::vector<std::string> sort_vector(std::vector<std::string> vec) {
    std::sort(vec.begin(), vec.end());
    return vec;
  }

  /**
   * @param dirPath the path to the directory to list files in
   * @return a sorted vector of paths to regular files in the directory
   */
  static std::vector<std::filesystem::path> getSortedDirectoryFiles(
      const std::string &dirPath) {
    std::vector<std::filesystem::path> paths;
    for (const auto &entry : std::filesystem::directory_iterator(dirPath)) {
      if (std::filesystem::is_regular_file(entry.path())) {
        paths.push_back(entry.path());
      }
    }
    std::sort(paths.begin(), paths.end());
    return paths;
  }

 protected:
  size_t scan_memmove_slack;
  size_t scan_router_slack;
  std::optional<std::vector<uint32_t>> numa_nodes;
  std::optional<std::vector<bool>> do_transfer;
};

class GPUOnlyNVMe : public proteus::CPUOnlyNVMeMorsel {
  using proteus::CPUOnlyNVMeMorsel::CPUOnlyNVMeMorsel;
  [[nodiscard]] DeviceType getDevice() override { return DeviceType::GPU; }

  std::unique_ptr<Affinitizer> getAffinitizer() override {
    if (numa_nodes.has_value()) {
      LOG(FATAL) << "Should not pass numa_nodes to GPUOnlyNVMe";
    } else {
      return std::make_unique<GPUAffinitizer>();
    }
  }

  /**
   * For GPUS, we do scan -> router (fanout) -> memmove -> router (fanin to GPU
   * DOP) because the asynchronous cufile API does not appear to scale either
   * with the number of threads or streams. On a A40 with 16x PCIe 4.0 we
   * achieve at best ~60% of the pcie bandwidth.
   * Our solution is to use the synchronous API, but have multiple memove
   * instances for each GPU so that we can have multiple IOs in flight to
   * achieve bandwidth. This approach achieves the maximum practical PCIe
   * bandwidth.
   * We should revisit the async API in future cuda versions, in theory it
   * should be more efficient
   */
  RelBuilder distribute_probe(RelBuilder input) override {
    auto rel = input.router(
        DegreeOfParallelism(topology::getInstance().getGpuCount() * 12),
        scan_router_slack, RoutingPolicy::LOCAL, DeviceType::CPU,
        getAffinitizer());

    rel = rel.memmove(scan_memmove_slack, getDevice(), do_transfer);
    rel = rel.router(getDOP(), 8, RoutingPolicy::LOCAL, DeviceType::CPU,
                     getAffinitizer());

    if (getDevice() == DeviceType::GPU) rel = rel.to_gpu();

    return rel;
  }
};

class GPUOnlyNVMeProbeFilterPushdown : public proteus::CPUOnlyNVMeMorsel {
 public:
  GPUOnlyNVMeProbeFilterPushdown(std::vector<std::string> input_dirs,
                                 const std::string &catalog_path,
                                 decltype(input_sizes) input_sizes,
                                 bool allowMoves, size_t scan_memmove_slack,
                                 size_t scan_router_slack, size_t slack,
                                 DegreeOfParallelism pushdown_dop,
                                 std::vector<uint32_t> pushdown_numa_nodes)
      : CPUOnlyNVMeMorsel(std::move(input_dirs), catalog_path, input_sizes,
                          allowMoves, scan_memmove_slack, scan_router_slack,
                          slack),
        pushdown_dop(pushdown_dop),
        pushdown_numa_nodes(std::move(pushdown_numa_nodes)) {}

  [[nodiscard]] DeviceType getDevice() override { return DeviceType::GPU; }

  virtual std::unique_ptr<Affinitizer> getPushdownAffinitizer() {
    return std::make_unique<SpecificCpuNumaNodeAffinitizer>(
        pushdown_numa_nodes);
  }

  DegreeOfParallelism getDOP() override {
    size_t num_compute_cores = 0;
    for (const auto compute_node_id : pushdown_numa_nodes) {
      num_compute_cores += topology::getInstance()
                               .getCpuNumaNodeById(compute_node_id)
                               .local_cores.size();
    }
    return DegreeOfParallelism{num_compute_cores};
  }

  /**
   * For probe filter pushdown.
   * Scan and filter happens on the CPU
   * Expected that the caller packs and moves the data to the GPU
   */
  RelBuilder distribute_probe(RelBuilder input) override {
    auto rel = input.router(DegreeOfParallelism(pushdown_dop),
                            scan_router_slack, RoutingPolicy::LOCAL,
                            DeviceType::CPU, getPushdownAffinitizer());

    rel = rel.memmove(scan_memmove_slack, DeviceType::CPU);

    return rel;
  }

 protected:
  const DegreeOfParallelism pushdown_dop;
  const std::vector<uint32_t> pushdown_numa_nodes;
};

class CPUOnlyNvmeProbeFilterPushdown : public proteus::CPUOnlyNVMeMorsel {
 public:
  CPUOnlyNvmeProbeFilterPushdown(
      std::vector<std::string> input_dirs, const std::string &catalog_path,
      decltype(input_sizes) input_sizes, bool allowMoves,
      size_t scan_memmove_slack, size_t scan_router_slack, size_t slack,
      DegreeOfParallelism pushdown_dop,
      const std::vector<uint32_t> &pushdown_numa_nodes,
      const std::vector<uint32_t> &compute_numa_nodes,
      std::optional<std::vector<bool>> do_transfer = std::nullopt)
      : CPUOnlyNVMeMorsel(std::move(input_dirs), catalog_path,
                          std::move(input_sizes), allowMoves,
                          scan_memmove_slack, scan_router_slack, slack,
                          do_transfer),
        m_pushdown_dop(pushdown_dop),
        m_pushdown_numa_nodes(pushdown_numa_nodes),
        m_compute_numa_nodes(compute_numa_nodes) {
    CHECK(!pushdown_numa_nodes.empty());
    CHECK(!compute_numa_nodes.empty());
  }

  [[nodiscard]] DeviceType getDevice() override { return DeviceType::CPU; }

  virtual std::unique_ptr<Affinitizer> getPushdownAffinitizer() {
    return std::make_unique<SpecificCpuNumaNodeAffinitizer>(
        m_pushdown_numa_nodes);
  }

  DegreeOfParallelism getDOP() override {
    size_t num_compute_cores = 0;
    for (const auto compute_node_id : m_compute_numa_nodes) {
      num_compute_cores += topology::getInstance()
                               .getCpuNumaNodeById(compute_node_id)
                               .local_cores.size();
    }
    CHECK_GT(num_compute_cores, 0);
    return DegreeOfParallelism{num_compute_cores};
  }

  std::unique_ptr<Affinitizer> getAffinitizer() override {
    return std::make_unique<SpecificCpuNumaNodeAffinitizer>(
        m_compute_numa_nodes);
  }

  /**
   * For probe filter pushdown.
   * Scan and filter happens on one socket
   * Expected that the caller packs and moves the data to the other socket fpr
   * the rest of the query using getAffinitizer()
   */
  RelBuilder distribute_probe(RelBuilder input) override {
    auto rel = input.router(DegreeOfParallelism(m_pushdown_dop),
                            scan_router_slack, RoutingPolicy::LOCAL,
                            DeviceType::CPU, getPushdownAffinitizer());

    rel = rel.memmove(scan_memmove_slack, DeviceType::CPU, do_transfer);

    return rel;
  }

 protected:
  const DegreeOfParallelism m_pushdown_dop;
  const std::vector<uint32_t> m_pushdown_numa_nodes;
  const std::vector<uint32_t> m_compute_numa_nodes;
};

};  // namespace proteus

#endif  // PROTEUS_NVME_SHAPERS_HPP
