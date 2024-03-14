/*
    Proteus -- High-performance query processing on heterogeneous hardware.

                            Copyright (c) 2023
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

#include <gtest/gtest.h>

#include <codegen/expressions/expressionTypes.hpp>
#include <olap/operators/relbuilder-factory.hpp>
#include <olap/operators/relbuilder.hpp>
#include <olap/plan/query-result.hpp>
#include <olap/plugins/binary-block-nvme-plugin.hpp>
#include <platform/common/common.hpp>

TEST(NvmePluginAttributePartMetaDataTest, from_file_small) {
  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/customer.csv.c_phone.metadata.json";
  NvmePlugin::AttributePartMetaData x{md_path};
  EXPECT_EQ(x.num_blocks, 6);
  EXPECT_EQ(x.block_sizes.size(), 6);
  EXPECT_EQ(x.block_offsets.size(), 6);
  EXPECT_EQ(x.block_sizes.front(), 2097152);
  EXPECT_EQ(x.block_sizes.back(), 1514240);
  EXPECT_EQ(x.block_offsets.front(), 0);
  EXPECT_EQ(x.block_offsets.back(), 10485760);
}

TEST(PageId_t, ptr_tagging) {
  void* page_id_as_ptr = getNvmePageIdPtr(1, 2, 3, 4);

  EXPECT_TRUE(NvmePlugin::PageId_t::isPageIdPtr(page_id_as_ptr));
  constexpr uintptr_t full_mask = 1ULL << 63;
  static_assert(sizeof(uintptr_t) == 8, "uintptr_t is not 8 bytes");
  EXPECT_TRUE(reinterpret_cast<uintptr_t>(page_id_as_ptr) & full_mask);

  auto page_id_from_ptr = NvmePlugin::PageId_t::from_ptr(page_id_as_ptr);

  constexpr int8_t numa_mask = 1u << 7;
  EXPECT_FALSE(
      reinterpret_cast<uint8_t>(page_id_from_ptr.getCpuNumaAffinity()) &
      numa_mask);
}

TEST(PageId_t, invertable) {
  const uint8_t expected_cpu_numa_affinity = 1;
  const uint8_t expected_attribute_no = 2;
  const uint8_t expected_partition_no = 3;
  const uint32_t expected_block_no = 4;

  NvmePlugin::PageId_t from_cpp_cons{expected_cpu_numa_affinity,
                                     expected_attribute_no,
                                     expected_partition_no, expected_block_no};
  void* page_id_as_ptr = getNvmePageIdPtr(
      from_cpp_cons.getCpuNumaAffinity(), from_cpp_cons.getAttributeNo(),
      from_cpp_cons.getPartitionNo(), from_cpp_cons.getBlockNo());
  NvmePlugin::PageId_t y = NvmePlugin::PageId_t::from_ptr(page_id_as_ptr);
  EXPECT_EQ(from_cpp_cons.getCpuNumaAffinity(), y.getCpuNumaAffinity());
  EXPECT_EQ(from_cpp_cons.getAttributeNo(), y.getAttributeNo());
  EXPECT_EQ(from_cpp_cons.getPartitionNo(), y.getPartitionNo());
  EXPECT_EQ(from_cpp_cons.getBlockNo(), y.getBlockNo());
}

uintptr_t hex_to_uintptr(const std::string& hex_str) {
  std::string hex_str_no_prefix = hex_str.substr(2);
  uintptr_t result;
  std::stringstream ss;
  ss << std::hex << hex_str_no_prefix;
  ss >> result;
  return result;
}

class NvmePluginTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}

  RelBuilderFactory getRelBuilderFactory() {
    auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    return RelBuilderFactory{std::string(test_info->test_suite_name()) + "." +
                             test_info->name()};
  }

  /**
   * @brief validate the result of a scan operation where there is only a
   * single partition per column. Assumes single threaded execution so that the
   * page_ids in the result are the same order as emitted by the scan
   */
  void validate_page_id_result_1_part_per_col(const QueryResult& res) {
    std::stringstream res_str;
    res_str << res;
    std::string out_tuple;
    int tuple_count = 0;
    while (std::getline(res_str, out_tuple)) {
      std::istringstream iss(out_tuple);
      std::string page_id_as_ptr;
      int attr = 0;
      while (std::getline(iss, page_id_as_ptr, ',')) {
        auto page_id_as_uintptr = hex_to_uintptr(page_id_as_ptr);
        auto page_id = NvmePlugin::PageId_t::from_ptr(
            reinterpret_cast<void*>(page_id_as_uintptr));
        ASSERT_TRUE(NvmePlugin::PageId_t::isPageIdPtr(page_id_as_uintptr));
        ASSERT_EQ(page_id.getAttributeNo(), attr)
            << "tuple#: " << tuple_count << " " << page_id;
        ASSERT_EQ(page_id.getPartitionNo(), 0)
            << "tuple#: " << tuple_count << " " << page_id;
        ASSERT_EQ(page_id.getBlockNo(), tuple_count)
            << "tuple#: " << tuple_count << " " << page_id;

        attr += 1;
      }
      tuple_count += 1;
    }
  }

  /**
   * @brief validate the result of a scan operation where there are n partition
   * per column. Assumes partitions are all equally sized.  Assumes single
   * threaded execution so that the page_ids in the result are the same order as
   * emitted by the scan
   */
  void validate_page_id_result_n_part_per_col(const QueryResult& res,
                                              size_t n_parts_per_col) {
    std::stringstream res_str;
    res_str << res;
    std::string out_tuple;
    int tuple_count = 0;
    int expected_partition = 0;
    // all partitions have block_no starting from 0
    int expected_block_no = 0;
    while (std::getline(res_str, out_tuple)) {
      std::istringstream iss(out_tuple);
      std::string page_id_as_ptr;
      int attr = 0;
      while (std::getline(iss, page_id_as_ptr, ',')) {
        auto page_id = NvmePlugin::PageId_t::from_ptr(
            reinterpret_cast<void*>(hex_to_uintptr(page_id_as_ptr)));
        ASSERT_EQ(page_id.getAttributeNo(), attr)
            << "tuple#: " << tuple_count << " " << page_id;
        ASSERT_EQ(page_id.getPartitionNo(), expected_partition)
            << "tuple#: " << tuple_count << " " << page_id;
        ASSERT_EQ(page_id.getBlockNo(), expected_block_no)
            << "tuple#: " << tuple_count << " " << page_id;
        attr += 1;
      }
      tuple_count += 1;
      expected_partition += 1;
      if (expected_partition == n_parts_per_col) {
        expected_partition = 0;
        expected_block_no += 1;
      }
    }
  }
};

TEST_F(NvmePluginTest, scan_one_col_one_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);
  using namespace dangling_attr;
  RecordType my_record_type = rel("ssbm100")(Int64("customer.csv.c_custkey"));

  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/customer.csv.c_custkey.metadata.json";
  std::vector<std::filesystem::path> attr_md{md_path};

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.push_back(
      std::make_pair(meta_data_records_map["customer.csv.c_custkey"], attr_md));

  RelBuilderFactory factory = getRelBuilderFactory();
  //  res should hold the page_ids in csv format for the 6 blocks
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
  SCOPED_TRACE("");
#pragma clang diagnostic pop
  validate_page_id_result_1_part_per_col(res);
}

TEST_F(NvmePluginTest, scan_two_col_one_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);
  using namespace dangling_attr;
  RecordType my_record_type = rel("ssbm100")(Int64("customer.csv.c_phone"),
                                             Int64("customer.csv.c_custkey"));

  const std::filesystem::path md_path_c_phone =
      "inputs/nvme-plugin-tests/customer.csv.c_phone.metadata.json";
  const std::filesystem::path md_path_c_custkey =
      "inputs/nvme-plugin-tests/customer.csv.c_phone.metadata.json";

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.push_back(
      std::make_pair(meta_data_records_map["customer.csv.c_phone"],
                     std::vector{md_path_c_phone}));

  relation_md.push_back(
      std::make_pair(meta_data_records_map["customer.csv.c_custkey"],
                     std::vector{md_path_c_custkey}));

  RelBuilderFactory factory = getRelBuilderFactory();
  //  res should hold the page_ids in csv format for the 6 blocks
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
  SCOPED_TRACE("");
#pragma clang diagnostic pop
  validate_page_id_result_1_part_per_col(res);
}

TEST_F(NvmePluginTest, scan_one_col_two_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);
  using namespace dangling_attr;
  RecordType my_record_type = rel("ssbm100")(Int64("customer.csv.c_custkey"));

  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/customer.csv.c_custkey.metadata.json";
  std::vector<std::filesystem::path> attr_md{md_path, md_path};

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.push_back(
      std::make_pair(meta_data_records_map["customer.csv.c_custkey"], attr_md));

  RelBuilderFactory factory = getRelBuilderFactory();
  //  res should hold the page_ids in csv format for the 6 blocks
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
  SCOPED_TRACE("");
#pragma clang diagnostic pop
  validate_page_id_result_n_part_per_col(res, 2);
}

TEST_F(NvmePluginTest, getPageIoInfoUsingMetadata) {
  using namespace dangling_attr;
  RecordType my_record_type = rel("ssbm100")(Int64("customer.csv.c_custkey"));

  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/customer.csv.c_custkey.metadata.json";
  std::vector<std::filesystem::path> attr_md{md_path, md_path};

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.push_back(
      std::make_pair(meta_data_records_map["customer.csv.c_custkey"], attr_md));
  auto context = OlapParallelContext("test");
  NvmePlugin testPlugin(&context, relation_md);

  // Assuming the second block of the first attribute for testing
  NvmePlugin::PageId_t testPageId(0,   // CPU NUMA affinity,
                                  0,   // attribute number,
                                  0,   // partition number,
                                  1);  //  block number

  // Retrieve IO information for the test page
  auto [fd, offset, size] = testPlugin.getPageIoInfo(testPageId);

  // Load expected values from the metadata file
  NvmePlugin::AttributePartMetaData partMetaData(md_path);
  const uint64_t expected_offset = partMetaData.block_offsets[1];
  const size_t expected_size = static_cast<size_t>(partMetaData.block_sizes[1]);

  // Can't test FD easily without a real file, but we can test the offset and
  // expected size
  EXPECT_EQ(offset, expected_offset);
  EXPECT_EQ(size, expected_size);
}
