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

#ifndef PROTEUS_BLOCK_NVME_PLUGIN_HPP
#define PROTEUS_BLOCK_NVME_PLUGIN_HPP

#include <olap/plugins/binary-block-plugin.hpp>
#include <olap/values/types.hpp>
#include <variant>

class NvmePlugin : public BinaryBlockPlugin {
 public:
  struct PageId_t {
    const uint8_t cpu_numa_affinity;
    const uint8_t attribute_no;
    const uint8_t partition_no;
    const uint8_t _padding;
    const uint32_t block_no;
    static PageId_t from_ptr(void *ptr) {
      auto page_id = reinterpret_cast<uint64_t>(ptr);
      return PageId_t{static_cast<uint8_t>(page_id >> 56),
                      static_cast<uint8_t>(page_id >> 48),
                      static_cast<uint8_t>(page_id >> 40), 0,
                      static_cast<uint32_t>(page_id)};
    }
    friend std::ostream &operator<<(std::ostream &out, const PageId_t &page_id);
  };
  static_assert(sizeof(PageId_t) == sizeof(void *));

  struct AttributePartMetaData {
    explicit AttributePartMetaData(const std::filesystem::path &md_path);
    ~AttributePartMetaData();
    uint64_t num_blocks;
    int fd;
    std::filesystem::path data_file_path;
    std::vector<uint64_t> block_offsets;
    std::vector<int> block_sizes;
    bool compressed;
  };

  NvmePlugin(
      OlapParallelContext *context,
      const std::vector<std::pair<RecordAttribute *,
                                  std::vector<std::filesystem::path>>> &fields);

  void generate(const Operator &producer,
                OlapParallelContext *context) override;

  ~NvmePlugin() override;

 protected:
  llvm::Value *getDataPointersForFile(OlapParallelContext *context, size_t i,
                                      llvm::Value *session_ptr) const override {
    LOG(FATAL) << "not implemented for NvmePlugin";
  }

  void freeDataPointersForFile(OlapParallelContext *context, size_t i,
                               llvm::Value *v) const override {
    LOG(FATAL) << "not implemented for NvmePlugin";
  }

  /**
   * @return a ptr to an array of length Nparts storing the number of blocks in
   * each partition, and the size in blocks of the largest partition
   */
  std::pair<llvm::Value *, llvm::Value *> getPartitionSizes(
      OlapParallelContext *context, llvm::Value *session_ptr) const override;

  void freePartitionSizes(OlapParallelContext *context,
                          llvm::Value *v) const override;

 private:
  static constexpr auto blockCtrVar = "blockCtr";
  // Generates a for loop that emits a PageId_t for each block in each iteration
  void scan(const Operator &producer, OlapParallelContext *context);

  std::vector<std::vector<AttributePartMetaData>> m_attribute_metadata;
  std::vector<uint64_t> part_sizes;  /// in blocks
  void nextEntry(OlapParallelContext *context);
};
extern "C" {
void *getNvmePageIdPtr(uint8_t cpu_numa_affinity, uint8_t attribute_no,
                       uint8_t partition_no, uint32_t block_no) noexcept;
}

std::ostream &operator<<(std::ostream &out,
                         const NvmePlugin::PageId_t &page_id);

#endif /* PROTEUS_BLOCK_NVME_PLUGIN_HPP */
