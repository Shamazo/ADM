/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2017
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

#ifndef AFFINITIZERS_HPP_
#define AFFINITIZERS_HPP_

#include <algorithm>
#include <numeric>
#include <olap/plugins/binary-block-nvme-plugin.hpp>
#include <platform/topology/device-types.hpp>
#include <platform/topology/gpu-index.hpp>
#include <platform/topology/topology.hpp>
#include <utility>

class Affinitizer {
 public:
  virtual ~Affinitizer() = default;
  [[nodiscard]] virtual size_t getAvailableCUIndex(size_t i) const = 0;

  /**
   * This is in essence provides a mapping from i to a compute unit (CU).
   * @param i Values of [0, size()) will return distinct CUs. Values
   * equal to or greater than size() will wrap around.
   * @return a reference to a topology::cu for input i
   */
  [[nodiscard]] virtual const topology::cu &getAvailableCU(size_t i) const = 0;

  /**
   * The number of distinct compute units (CUs) that can be returned by
   * getAvailableCU / getLocalCUIndex
   *
   * @details countAffCUs and countAllCUs are not necessarily the same. In the
   * case where the affinitizer affinitizes to a subset of CUs, countAffCUs()
   * will return the number of CUs in the subset, while countAllCUs() will
   * return the total number of CUs in the system. For example, the
   * SpecificCpuNumaNodeAffinitizer.
   * @see Affinitizer::CountCUs
   */
  [[nodiscard]] virtual size_t countAffCUs() const = 0;

  /**
   * The total number of compute units (CUs), of the type this Affinitizer
   * returns, in the system
   *
   * @see Affinitizer::countAffCUs
   */
  [[nodiscard]] virtual size_t countAllCUs() const = 0;

  /**
   * @param ptr a pointer to memory
   * @return the index_in_topo of the NUMA node containing the data that ptr
   * points to
   */
  [[nodiscard]] virtual size_t getLocalCUIndex(void *ptr) const = 0;
};

std::unique_ptr<Affinitizer> getDefaultAffinitizer(DeviceType);

/**
 * An affinity policy provides a mapping from data/memory location (void*) to
 * an index in [0, fanout).
 *
 * For example, if we have a thread pool we can use an AffinityPolicy to
 * allocate tasks to threads based on the locality of the data they will be
 * working on. Each thread would initially need to set its affinity to
 * aff.getAvailableCU(threads_index). They to allocate work to threads we would
 * use AffinityPolicy.getIndexOfRandLocalCU(void* inputs) to determine the index
 * of which thread to assign the work to. AffinityPolicy just provides a mapping
 * between data/memory location and a local compute unit. It does not set the
 * affinity of any threads or allocate any work itself.
 *
 */
class AffinityPolicy {
 protected:
  /// indexes contain the values [0, fanout)
  /// indexes[i] contains the values for which aff->getAvailableCUIndex(i)
  /// returns i.
  std::vector<std::vector<size_t>> indexes;
  const Affinitizer *aff;

 public:
  /**
   * @param fanout The number of indexes to we wish to assign ptrs to.
   * @param aff The underlying Affinitizer that determines a NUMA node affinity
   * given a pointer.
   */
  AffinityPolicy(size_t fanout, const Affinitizer *aff);

  /**
   * @return a value `X` in [0, fanout) such that that aff.getLocalCUIndex(ptr)
   * == aff.getAvailableCUIndex(X)
   */
  size_t getIndexOfRandLocalCU(void *ptr) const;
};

class CpuNumaNodeAffinitizer : public Affinitizer {
 protected:
  [[nodiscard]] const topology::cu &getAvailableCU(
      size_t cpu_req) const override {
    return topology::getInstance()
        .getCpuNumaNodes()[cpu_req %
                           topology::getInstance().getCpuNumaNodeCount()];
  }

 public:
  [[nodiscard]] size_t getAvailableCUIndex(size_t i) const override {
    return dynamic_cast<const topology::cpunumanode &>(getAvailableCU(i))
        .index_in_topo;
  }

  [[nodiscard]] size_t countAffCUs() const override {
    return topology::getInstance().getCpuNumaNodeCount();
  }

  [[nodiscard]] size_t countAllCUs() const override {
    return topology::getInstance().getCpuNumaNodeCount();
  }

  size_t getLocalCUIndex(void *p) const override {
    auto &topo = topology::getInstance();
    const auto *g = topo.getGpuAddressed(p);
    if (g) return g->getLocalCPUNumaNode().index_in_topo;
    auto *c = topo.getCpuNumaNodeAddressed(p);
    if (c) return c->index_in_topo;
    DCHECK(NvmePlugin::PageId_t::isPageIdPtr(p));
    auto page_id = NvmePlugin::PageId_t::from_ptr(p);
    return page_id.getCpuNumaAffinity();
  }
};

class SpecificCpuNumaNodeAffinitizer : public Affinitizer {
 public:
  /**
   * Construct an affinitizer for the specified NUMA nodes.
   * @param node_ids These are the NUMA IDs as reported by libnuma/numactl. (Not
   * the indexes of cpunumanode in topology)
   */
  explicit SpecificCpuNumaNodeAffinitizer(std::vector<uint32_t> node_ids)
      : m_node_ids(std::move(node_ids)) {
    CHECK(!m_node_ids.empty()) << "Need at least one CPU NUMA node id to "
                                  "construct a SpecificCpuNumaNodeAffinitizer";
  }

 protected:
  [[nodiscard]] const topology::cu &getAvailableCU(
      size_t cpu_req) const override {
    return topology::getInstance().getCpuNumaNodeById(
        m_node_ids[cpu_req % m_node_ids.size()]);
  }
  const std::vector<uint32_t> m_node_ids;

 public:
  [[nodiscard]] size_t getAvailableCUIndex(size_t i) const override {
    return dynamic_cast<const topology::cpunumanode &>(getAvailableCU(i))
        .index_in_topo;
  }

  [[nodiscard]] size_t countAffCUs() const override {
    return m_node_ids.size();
  }

  [[nodiscard]] size_t countAllCUs() const override {
    return topology::getInstance().getCpuNumaNodeCount();
  }

  size_t getLocalCUIndex(void *p) const override {
    auto &topo = topology::getInstance();
    const auto *g = topo.getGpuAddressed(p);
    auto *c = topo.getCpuNumaNodeAddressed(p);
    if (c) {
      if (std::find(m_node_ids.begin(), m_node_ids.end(), c->id) !=
          m_node_ids.end()) {
        return c->index_in_topo;
      } else {
        // find the closest node in m_node_ids
        std::vector<size_t> idx(c->distance.size());
        std::iota(idx.begin(), idx.end(), 0);
        stable_sort(idx.begin(), idx.end(),
                    [&v = std::as_const(c->distance)](size_t i1, size_t i2) {
                      return v[i1] < v[i2];
                    });
        for (const auto &i : idx) {
          const auto &cpu_node = topo.getCpuNumaNodes()[i];
          if (std::find(m_node_ids.begin(), m_node_ids.end(), cpu_node.id) !=
              m_node_ids.end()) {
            return cpu_node.index_in_topo;
          }
        }
        LOG(FATAL)
            << "SpecificCpuNumaNodeAffinitizer unreachable code! (in theory)";
      }
    }
    if (g) {
      if (std::find(m_node_ids.begin(), m_node_ids.end(),
                    g->getLocalCPUNumaNode().id) != m_node_ids.end()) {
        return g->getLocalCPUNumaNode().index_in_topo;
      } else {
        // find the closest cpunode in m_node_ids with a local GPU
        // Kinda wonky and assumes that CPU nodes "have" GPU nodes as we use
        // CPU NUMA distance as a proxy for GPU distance
        std::vector<size_t> idx(g->getLocalCPUNumaNode().distance.size());
        std::iota(idx.begin(), idx.end(), 0);
        stable_sort(idx.begin(), idx.end(),
                    [&v = std::as_const(g->getLocalCPUNumaNode().distance)](
                        size_t i1, size_t i2) { return v[i1] < v[i2]; });
        for (const auto &i : idx) {
          const auto &cpu_node = topo.getCpuNumaNodes()[i];
          const auto &local_gpus = cpu_node.local_gpus;
          if (std::find(m_node_ids.begin(), m_node_ids.end(), cpu_node.id) !=
                  m_node_ids.end() &&
              !local_gpus.empty()) {
            return local_gpus[rand() % local_gpus.size()];
          }
        }
        LOG(FATAL) << "SpecificCpuNumaNodeAffinitizer Could not find a CPU "
                      "node with a GPU in the specified node_ids";
      }
    }

    DCHECK(NvmePlugin::PageId_t::isPageIdPtr(p));
    auto page_id = NvmePlugin::PageId_t::from_ptr(p);
    const auto &node_local_to_nvme =
        topo.getCpuNumaNodes()[page_id.getCpuNumaAffinity()];

    if (std::find(m_node_ids.begin(), m_node_ids.end(),
                  node_local_to_nvme.id) != m_node_ids.end()) {
      return node_local_to_nvme.index_in_topo;
    } else {
      // find the closest node in m_node_ids
      std::vector<size_t> idx(node_local_to_nvme.distance.size());
      std::iota(idx.begin(), idx.end(), 0);
      stable_sort(idx.begin(), idx.end(),
                  [&v = std::as_const(node_local_to_nvme.distance)](
                      size_t i1, size_t i2) { return v[i1] < v[i2]; });
      for (const auto &i : idx) {
        const auto &cpu_node = topo.getCpuNumaNodes()[i];
        if (std::find(m_node_ids.begin(), m_node_ids.end(), cpu_node.id) !=
            m_node_ids.end()) {
          return cpu_node.index_in_topo;
        }
      }
      LOG(FATAL)
          << "SpecificCpuNumaNodeAffinitizer unreachable code! (in theory)";
    }
  }
};

class GPUAffinitizer : public Affinitizer {
 protected:
  [[nodiscard]] const topology::gpunode &getAvailableCU(
      size_t gpu_req) const override {
    static const gpu_index index;
    size_t gpu_i = gpu_req % topology::getInstance().getGpuCount();
    return topology::getInstance().getGpus()[index.d[gpu_i]];
  }

 public:
  [[nodiscard]] size_t getAvailableCUIndex(size_t i) const override {
    return getAvailableCU(i).index_in_topo;
  }

  [[nodiscard]] size_t countAffCUs() const override {
    return topology::getInstance().getGpuCount();
  }

  [[nodiscard]] size_t countAllCUs() const override {
    return topology::getInstance().getGpuCount();
  }

  [[nodiscard]] size_t getLocalCUIndex(void *p) const override {
    auto &topo = topology::getInstance();
    const auto *g = topo.getGpuAddressed(p);
    if (g) return g->index_in_topo;
    auto *c = topo.getCpuNumaNodeAddressed(p);
    if (!c) {
      return topology::getInstance()
          .getGpus()[rand() % countAffCUs()]
          .index_in_topo;
    }
    assert(c);
    const auto &gpus = c->local_gpus;
    // NUMA nodes can have 0 GPUs, even in a system with GPUS
    if (!gpus.empty()) {
      return gpus[rand() % gpus.size()];
    }
    // Find the closest CPU numa node with a GPU
    // initialize original index locations
    std::vector<size_t> idx(c->distance.size());
    std::iota(idx.begin(), idx.end(), 0);
    stable_sort(idx.begin(), idx.end(),
                [&v = std::as_const(c->distance)](size_t i1, size_t i2) {
                  return v[i1] < v[i2];
                });
    for (const auto &i : idx) {
      const auto &cpu_node = topo.getCpuNumaNodes()[i];
      const auto &local_gpus = cpu_node.local_gpus;
      if (cpu_node.local_gpus.size() > 0) {
        return local_gpus[rand() % local_gpus.size()];
      }
    }
    LOG(FATAL) << "GPUAffinitizer found no GPUs";
  }
};

class CpuCoreAffinitizer : public CpuNumaNodeAffinitizer {
 protected:
  [[nodiscard]] virtual const topology::core &getAvailableCore(
      size_t cpu_req) const {
    size_t core_index = cpu_req % topology::getInstance().getCoreCount();
    // NOTE: Assuming all CPUs have the same number of cores!
    size_t cpunumacnt = topology::getInstance().getCpuNumaNodeCount();
    size_t numanode = core_index % cpunumacnt;
    const auto &cpunumanode =
        topology::getInstance().getCpuNumaNodes()[numanode];
    return cpunumanode.getCore(core_index / cpunumacnt);
  }

 protected:
  [[nodiscard]] const topology::cu &getAvailableCU(size_t cpu_req) const final {
    return getAvailableCore(cpu_req);
  }

  [[nodiscard]] size_t getAvailableCUIndex(size_t i) const override {
    return getAvailableCore(i).getLocalCPUNumaNode().index_in_topo;
  }
};

class SpecificCpuCoreAffinitizer : public CpuCoreAffinitizer {
 public:
  typedef std::remove_cv_t<decltype(topology::core::id)> coreid_t;

 private:
  std::vector<coreid_t> core_ids;

 public:
  explicit SpecificCpuCoreAffinitizer(const std::vector<coreid_t> &core_ids)
      : core_ids(core_ids) {}

 protected:
  [[nodiscard]] const topology::core &getAvailableCore(
      size_t cpu_req) const override {
    assert(cpu_req < core_ids.size());
    return topology::getInstance().getCoreById(core_ids[cpu_req]);
  }
};
#endif /* AFFINITIZERS_HPP_ */
