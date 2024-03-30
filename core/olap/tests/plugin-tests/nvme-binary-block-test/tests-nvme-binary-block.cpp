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

// `SCOPED_TRACE("")` raises this error
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"

using namespace dangling_attr;

TEST(NvmePluginAttributePartMetaDataTest, from_file_small) {
  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_phone.metadata.json";
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
 public:
  static void TearDownTestSuite() {
    auto& sm = StorageManager::getInstance();
    sm.unloadAll();
  }

 protected:
  void SetUp() override {}
  void TearDown() override {}

  static RelBuilderFactory getRelBuilderFactory() {
    auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    return RelBuilderFactory{std::string(test_info->test_suite_name()) + "." +
                             test_info->name()};
  }

  /**
   * @brief validate the result of a scan operation where there is only a
   * single partition per column. Assumes single threaded execution so that the
   * page_ids in the result are the same order as emitted by the scan
   */
  static void validate_page_id_result_1_part_per_col(const QueryResult& res) {
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
  static void validate_page_id_result_n_part_per_col(const QueryResult& res,
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

  static std::tuple<int32_t, int32_t, int32_t> calculate_min_max_count_int32(
      const std::filesystem::path& data_file) {
    std::ifstream file(data_file, std::ios::binary);
    int32_t max_value = std::numeric_limits<int32_t>::min();
    int32_t min_value = std::numeric_limits<int32_t>::max();
    int32_t count = 0;
    while (file) {
      int32_t value;
      file.read(reinterpret_cast<char*>(&value), sizeof(value));
      if (file.gcount() != sizeof(value)) {
        break;
      }
      count += 1;
      if (value > max_value) {
        max_value = value;
      }
      if (value < min_value) {
        min_value = value;
      }
    }
    return {min_value, max_value, count};
  }

  static std::vector<int32_t> splitStringToInt32(const std::string& str) {
    std::vector<int32_t> result;
    std::stringstream ss(str);
    std::string token;

    while (std::getline(ss, token, ',')) {
      result.push_back(std::stoi(token));
    }

    return result;
  }
};

TEST_F(NvmePluginTest, scan_one_col_one_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type = rel("customer.csv")(Int("c_custkey"));

  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";
  std::vector<std::filesystem::path> attr_md{md_path};

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_custkey"], attr_md);

  RelBuilderFactory factory = getRelBuilderFactory();
  //  res should hold the page_ids in csv format for the 6 blocks
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();

  SCOPED_TRACE("");
  validate_page_id_result_1_part_per_col(res);
}

TEST_F(NvmePluginTest, scan_two_col_one_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type =
      rel("customer.csv")(Int("c_phone"), Int("c_custkey"));

  const std::filesystem::path md_path_c_phone =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_phone.metadata.json";
  const std::filesystem::path md_path_c_custkey =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_phone.metadata.json";

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_phone"],
                           std::vector{md_path_c_phone});

  relation_md.emplace_back(meta_data_records_map["c_custkey"],
                           std::vector{md_path_c_custkey});

  RelBuilderFactory factory = getRelBuilderFactory();
  //  res should hold the page_ids in csv format for the 6 blocks
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();

  SCOPED_TRACE("");
  validate_page_id_result_1_part_per_col(res);
}

TEST_F(NvmePluginTest, scan_one_col_two_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type = rel("customer.csv")(Int("c_custkey"));

  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";
  std::vector<std::filesystem::path> attr_md{md_path, md_path};

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_custkey"], attr_md);

  RelBuilderFactory factory = getRelBuilderFactory();
  //  res should hold the page_ids in csv format for the 6 blocks
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();

  SCOPED_TRACE("");
  validate_page_id_result_n_part_per_col(res, 2);
}

TEST_F(NvmePluginTest, getPageIoInfoUsingMetadata) {
  RecordType my_record_type = rel("customer.csv")(Int("c_custkey"));

  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";
  std::vector<std::filesystem::path> attr_md{md_path, md_path};

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_custkey"], attr_md);
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
  const auto expected_size = static_cast<size_t>(partMetaData.block_sizes[1]);

  // Can't test FD easily without a real file, but we can test the offset and
  // expected size
  EXPECT_EQ(offset, expected_offset);
  EXPECT_EQ(size, expected_size);
}

TEST_F(NvmePluginTest, scan_and_move_one_col_one_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type = rel("customer.csv")(Int("c_custkey"));

  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";
  std::vector<std::filesystem::path> attr_md{md_path};

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_custkey"], attr_md);

  RelBuilderFactory factory = getRelBuilderFactory();
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .memmove(32, DeviceType::CPU)
                 .unpack()
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["c_custkey"].as("tmp", "min"),
                               arg["c_custkey"].as("tmp", "max"),
                               expression_t{1}.as("tmp", "count")};
                     },
                     {MIN, MAX, SUM})
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
  std::stringstream output;
  output << res;
  auto split_output = splitStringToInt32(output.str());
  EXPECT_EQ(split_output.size(), 3);
  auto [expected_min, expected_max, expected_count] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_custkey");
  EXPECT_EQ(split_output[0], expected_min);
  EXPECT_EQ(split_output[1], expected_max);
  EXPECT_EQ(split_output[2], expected_count);
}

TEST_F(NvmePluginTest, scan_and_move_one_col_two_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type = rel("customer.csv")(Int("c_custkey"));

  const std::filesystem::path md_path =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";
  std::vector<std::filesystem::path> attr_md{md_path, md_path};

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_custkey"], attr_md);

  RelBuilderFactory factory = getRelBuilderFactory();
  //  res should hold the page_ids in csv format for the 6 blocks
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .memmove(32, DeviceType::CPU)
                 .unpack()
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["c_custkey"].as("tmp", "min"),
                               arg["c_custkey"].as("tmp", "max"),
                               expression_t{1}.as("tmp", "count")};
                     },
                     {MIN, MAX, SUM})
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
  std::stringstream output;
  output << res;
  auto split_output = splitStringToInt32(output.str());
  EXPECT_EQ(split_output.size(), 3)
      << "expected a single tuple with 3 aggregates";
  auto [expected_min, expected_max, expected_count] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_custkey");
  EXPECT_EQ(split_output[0], expected_min);
  EXPECT_EQ(split_output[1], expected_max);
  // 2 partitions that are the same base data
  EXPECT_EQ(split_output[2], 2 * expected_count);
}

TEST_F(NvmePluginTest, scan_and_move_two_col_two_part) {
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type =
      rel("customer.csv")(Int("c_phone"), Int("c_custkey"));

  const std::filesystem::path md_path_c_phone =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_phone.metadata.json";
  const std::filesystem::path md_path_c_custkey =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_phone"],
                           std::vector{md_path_c_phone, md_path_c_phone});

  relation_md.emplace_back(meta_data_records_map["c_custkey"],
                           std::vector{md_path_c_custkey, md_path_c_custkey});

  RelBuilderFactory factory = getRelBuilderFactory();
  //  res should hold the page_ids in csv format for the 6 blocks
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .memmove(32, DeviceType::CPU)
                 .unpack()
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["c_custkey"].as("tmp", "key_min"),
                               arg["c_custkey"].as("tmp", "key_max"),
                               arg["c_phone"].as("tmp", "phone_min"),
                               arg["c_phone"].as("tmp", "phone_max"),
                               expression_t{1}.as("tmp", "count")};
                     },
                     {MIN, MAX, MIN, MAX, SUM})
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
  std::stringstream output;
  output << res;
  auto split_output = splitStringToInt32(output.str());
  EXPECT_EQ(split_output.size(), 5)
      << "expected a single tuple with 5 aggregates";
  auto [expected_min_key, expected_max_key, expected_count] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_custkey");
  auto [expected_min_phone, expected_max_phone, _] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_phone");
  EXPECT_EQ(split_output[0], expected_min_key);
  EXPECT_EQ(split_output[1], expected_max_key);
  EXPECT_EQ(split_output[2], expected_min_phone);
  EXPECT_EQ(split_output[3], expected_max_phone);
  // 2 partitions that are the same base data
  EXPECT_EQ(split_output[4], 2 * expected_count);
}

TEST_F(NvmePluginTest, scan_and_move_then_router_two_col_two_part) {
  // here the memmove is single threaded and then the reduction in parallelized
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type =
      rel("customer.csv")(Int("c_phone"), Int("c_custkey"));

  const std::filesystem::path md_path_c_phone =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_phone.metadata.json";
  const std::filesystem::path md_path_c_custkey =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_phone"],
                           std::vector{md_path_c_phone, md_path_c_phone});

  relation_md.emplace_back(meta_data_records_map["c_custkey"],
                           std::vector{md_path_c_custkey, md_path_c_custkey});

  RelBuilderFactory factory = getRelBuilderFactory();
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .memmove(32, DeviceType::CPU)
                 .router(DegreeOfParallelism(4), 4, RoutingPolicy::LOCAL,
                         DeviceType::CPU)
                 .unpack()
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["c_custkey"].as("tmp", "key_min"),
                               arg["c_custkey"].as("tmp", "key_max"),
                               arg["c_phone"].as("tmp", "phone_min"),
                               arg["c_phone"].as("tmp", "phone_max"),
                               expression_t{1}.as("tmp", "count")};
                     },
                     {MIN, MAX, MIN, MAX, SUM})
                 .router(DegreeOfParallelism{1}, 32, RoutingPolicy::RANDOM,
                         DeviceType::CPU)
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["key_min"], arg["key_max"], arg["phone_min"],
                               arg["phone_max"], arg["count"]};
                     },
                     {MIN, MAX, MIN, MAX, SUM})
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
  std::stringstream output;
  output << res;
  auto split_output = splitStringToInt32(output.str());
  EXPECT_EQ(split_output.size(), 5)
      << "expected a single tuple with 5 aggregates";
  auto [expected_min_key, expected_max_key, expected_count] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_custkey");
  auto [expected_min_phone, expected_max_phone, _] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_phone");
  EXPECT_EQ(split_output[0], expected_min_key);
  EXPECT_EQ(split_output[1], expected_max_key);
  EXPECT_EQ(split_output[2], expected_min_phone);
  EXPECT_EQ(split_output[3], expected_max_phone);
  // 2 partitions that are the same base data
  EXPECT_EQ(split_output[4], 2 * expected_count);
}

TEST_F(NvmePluginTest, scan_and_router_then_move_two_col_two_part) {
  // here the memmove and the reduction are parallelized
  // This tests that the affinitizers can work with page_ids and that
  // router/memmove propagate the tuple/block count properly
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type =
      rel("customer.csv")(Int("c_phone"), Int("c_custkey"));

  const std::filesystem::path md_path_c_phone =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_phone.metadata.json";
  const std::filesystem::path md_path_c_custkey =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_phone"],
                           std::vector{md_path_c_phone, md_path_c_phone});

  relation_md.emplace_back(meta_data_records_map["c_custkey"],
                           std::vector{md_path_c_custkey, md_path_c_custkey});

  RelBuilderFactory factory = getRelBuilderFactory();
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .router(DegreeOfParallelism(4), 4, RoutingPolicy::LOCAL,
                         DeviceType::CPU)
                 .memmove(32, DeviceType::CPU)
                 .unpack()
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["c_custkey"].as("tmp", "key_min"),
                               arg["c_custkey"].as("tmp", "key_max"),
                               arg["c_phone"].as("tmp", "phone_min"),
                               arg["c_phone"].as("tmp", "phone_max"),
                               expression_t{1}.as("tmp", "count")};
                     },
                     {MIN, MAX, MIN, MAX, SUM})
                 .router(DegreeOfParallelism{1}, 32, RoutingPolicy::RANDOM,
                         DeviceType::CPU)
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["key_min"], arg["key_max"], arg["phone_min"],
                               arg["phone_max"], arg["count"]};
                     },
                     {MIN, MAX, MIN, MAX, SUM})
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
  std::stringstream output;
  output << res;
  auto split_output = splitStringToInt32(output.str());
  EXPECT_EQ(split_output.size(), 5)
      << "expected a single tuple with 5 aggregates";
  auto [expected_min_key, expected_max_key, expected_count] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_custkey");
  auto [expected_min_phone, expected_max_phone, _] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_phone");
  EXPECT_EQ(split_output[0], expected_min_key);
  EXPECT_EQ(split_output[1], expected_max_key);
  EXPECT_EQ(split_output[2], expected_min_phone);
  EXPECT_EQ(split_output[3], expected_max_phone);
  // 2 partitions that are the same base data
  EXPECT_EQ(split_output[4], 2 * expected_count);
}

TEST_F(NvmePluginTest, scan_and_membrdcst_two_col_two_part) {
  GTEST_SKIP_("membrdcst is not yet supported");
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunreachable-code"
  set_exec_location_on_scope exec(topology::getInstance().getCpuNumaNodes()[0]);

  RecordType my_record_type =
      rel("customer.csv")(Int("c_phone"), Int("c_custkey"));

  const std::filesystem::path md_path_c_phone =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_phone.metadata.json";
  const std::filesystem::path md_path_c_custkey =
      "inputs/nvme-plugin-tests/ssb100_customer.csv.c_custkey.metadata.json";

  auto meta_data_records_map = my_record_type.getArgsMap();
  std::vector<std::pair<RecordAttribute*, std::vector<std::filesystem::path>>>
      relation_md;
  relation_md.emplace_back(meta_data_records_map["c_phone"],
                           std::vector{md_path_c_phone, md_path_c_phone});

  relation_md.emplace_back(meta_data_records_map["c_custkey"],
                           std::vector{md_path_c_custkey, md_path_c_custkey});

  RelBuilderFactory factory = getRelBuilderFactory();
  auto res = factory.getBuilder()
                 .scan(relation_md)
                 .router(DegreeOfParallelism(4), 4, RoutingPolicy::LOCAL,
                         DeviceType::CPU)
                 .membrdcst(DegreeOfParallelism(2), true)
                 .unpack()
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["c_custkey"].as("tmp", "key_min"),
                               arg["c_custkey"].as("tmp", "key_max"),
                               arg["c_phone"].as("tmp", "phone_min"),
                               arg["c_phone"].as("tmp", "phone_max"),
                               expression_t{1}.as("tmp", "count")};
                     },
                     {MIN, MAX, MIN, MAX, SUM})
                 .router(DegreeOfParallelism{1}, 32, RoutingPolicy::RANDOM,
                         DeviceType::CPU)
                 .reduce(
                     [&](const auto& arg) -> std::vector<expression_t> {
                       return {arg["key_min"], arg["key_max"], arg["phone_min"],
                               arg["phone_max"], arg["count"]};
                     },
                     {MIN, MAX, MIN, MAX, SUM})
                 .print(pg("pm-csv"))
                 .prepare()
                 .execute();
  std::stringstream output;
  output << res;
  auto split_output = splitStringToInt32(output.str());
  EXPECT_EQ(split_output.size(), 5)
      << "expected a single tuple with 5 aggregates";
  auto [expected_min_key, expected_max_key, expected_count] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_custkey");
  auto [expected_min_phone, expected_max_phone, _] =
      calculate_min_max_count_int32("inputs/ssbm100/customer.csv.c_phone");
  EXPECT_EQ(split_output[0], expected_min_key);
  EXPECT_EQ(split_output[1], expected_max_key);
  EXPECT_EQ(split_output[2], expected_min_phone);
  EXPECT_EQ(split_output[3], expected_max_phone);
  // 2 partitions that are the same base data, which are then broadcasted to 2
  // threads
  EXPECT_EQ(split_output[4], 4 * expected_count);
#pragma clang diagnostic pop
}

#pragma clang diagnostic pop
