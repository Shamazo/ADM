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

#include "bloom-filter.hpp"

#include <llvm/IR/Intrinsics.h>

#include <codegen/expressions/indexed-seq.hpp>
#include <platform/memory/memory-manager.hpp>
#include <platform/topology/affinity_manager.hpp>
#include <platform/topology/topology.hpp>
static std::mutex bloom_filter_registry_lock{};
static std::map<std::pair<uint64_t, decltype(topology::cpunumanode::id)>,
                void *>
    bloom_filter_registry{};

extern "C" void setBloomFilter(Pipeline *pip, void *s, uint64_t bloomId) {
  const auto &cpu = affinity::get();
  auto k = std::make_pair(bloomId, cpu.id);
  std::lock_guard<std::mutex> lock(bloom_filter_registry_lock);
  if (bloom_filter_registry.count(k)) {
    MemoryManager::freePinned(bloom_filter_registry[k]);
    //    LOG(INFO) << "setBloomFilter Freeing bloom filter with id: " <<
    //    bloomId
    //              << " on node: " << cpu.id;
  }
  //  LOG(INFO) << " setBloomFilter on node: " << cpu.id;
  bloom_filter_registry[k] = s;
}

extern "C" void copyBloomFilterToNode(Pipeline *pip, void *s, uint64_t bloomId,
                                      uint32_t target_numa_id,
                                      size_t bloom_filter_size) {
  const auto &this_cpu = affinity::get();
  const auto this_key = std::make_pair(bloomId, this_cpu.id);
  const auto target_key = std::make_pair(bloomId, target_numa_id);
  std::lock_guard<std::mutex> lock(bloom_filter_registry_lock);
  if (bloom_filter_registry.count(target_key)) {
    LOG(WARNING) << "target numa already has bloom filter " << target_numa_id;
    return;
  }
  CHECK(bloom_filter_registry.count(this_key));
  auto &topo = topology::getInstance();
  const uint32_t target_numa_index =
      topo.getCpuNumaNodeById(target_numa_id).index_in_topo;
  char *bloom_filter_copy = static_cast<char *>(
      MemoryManager::mallocPinnedOnNode(bloom_filter_size, target_numa_index));
  memcpy(bloom_filter_copy, s, bloom_filter_size);
  LOG(INFO) << "copying bloom filter with id: " << bloomId
            << " from node: " << this_cpu.id << " to node: " << target_numa_id;
  bloom_filter_registry[target_key] = bloom_filter_copy;
}

extern "C" void *getBloomFilter(Pipeline *pip, uint64_t bloomId) {
  const auto &cpu = affinity::get();
  auto k = std::make_pair(bloomId, cpu.id);
  // FIXME: how often is this called?
  DCHECK_GE(bloom_filter_registry.count(k), 0)
      << "no bloom filter with id: " << bloomId << " on node: " << cpu.id;
  CHECK(bloom_filter_registry[k])
      << "no bloom filter found. id: " << bloomId << " numa id: " << cpu.id;
  return bloom_filter_registry[k];
}

void cleanBloomFilterRegistry() {
  std::lock_guard<std::mutex> lock(bloom_filter_registry_lock);
  for (const auto &r : bloom_filter_registry) {
    LOG(INFO) << "Freeing bloom filter with id: " << r.first.first
              << " on node: " << r.first.second;
    MemoryManager::freePinned(r.second);
  }
  bloom_filter_registry.clear();
}

BloomFilter::BloomFilter(std::shared_ptr<Operator> child, expression_t e,
                         size_t filterSize, uint64_t bloomId)
    : experimental::UnaryOperator(child),
      bf_expr(std::move(e)),
      filterSize(filterSize),
      bloomId(bloomId) {
  CHECK_NE(filterSize, 0) << "cannot have a filter of size 0";
  CHECK_EQ(filterSize & (filterSize - 1), 0)
      << "Filter size is expected to be a power of 2";
}

llvm::Type *BloomFilter::getFilterType(OlapParallelContext *context) const {
  return llvm::PointerType::getUnqual(llvm::ArrayType::get(
      llvm::Type::getInt1Ty(context->getLLVMContext()), filterSize));
}

expressions::RefExpression BloomFilter::findInFilter(
    OlapParallelContext *context, const OperatorState &childState) const {
  auto fptr_v = context->getStateVar(filter_ptr);

  auto btype = new BoolType();
  expressions::ProteusValueExpression fptr{new type::IndexedSeq{*btype},
                                           {fptr_v, context->createFalse()}};

  // If probe
  //  auto ref = fptr[expressions::HashExpression{e}]
  CHECK(!(filterSize & (filterSize - 1)))
      << "Filter size is expectd to be a power of 2";
  //  auto f = llvm::Intrinsic::getDeclaration(context->getModule(),
  //  llvm::Intrinsic::x86_pclmulqdq); assert(f); ExpressionGeneratorVisitor
  //  vis{context, childState}; f->dump(); auto hvpv = e.accept(vis); auto hv =
  //  context->getBuilder()->CreateCall(f, {hvpv.value,
  //  context->createInt32(0x75ebca6b)}); auto h =
  //  expressions::ProteusValueExpression{new IntType, {hv, hvpv.isNull}} +
  //  0x85ebca6; auto h = expressions::HashExpression{e}; expression_t h = 0;
  //  uint32_t mul = 0x75ebca6bu & ((uint32_t)filterSize - 1);
  //  for (uint32_t i = 0 ; i < sizeof(uint32_t) * 8 ; ++i){
  //    if ((mul >> i) & 1u) h = h ^ (e << ((int32_t) (i)));
  //  }
  auto h = bf_expr;

  auto hash = (filterSize & (filterSize - 1))
                  ? expression_t{h % ((int32_t)filterSize)}
                  : expression_t{h & ((int32_t)filterSize - 1)};
  //
  //  auto h = expressions::HashExpression{e};
  //
  //  auto hash = (filterSize & (filterSize - 1))
  //              ? expression_t{h % ((int64_t)filterSize)}
  //              : expression_t{h & ((int64_t)filterSize - 1)};

  return fptr[hash];
}
