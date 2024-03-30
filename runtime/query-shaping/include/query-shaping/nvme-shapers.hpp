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
#ifndef PROTEUS_NVME_SHAPERS_HPP
#define PROTEUS_NVME_SHAPERS_HPP
#include <codegen/expressions/expressionTypes.hpp>
#include <olap/plan/catalog-parser.hpp>
#include <query-shaping/input-prefix-query-shaper.hpp>
#include <vector>

namespace proteus {

class CPUOnlyNVMeMorsel : public proteus::InputPrefixQueryShaper {
  [[nodiscard]] pg getPlugin() const override { return pg{"nvme-block"}; }
  [[nodiscard]] DeviceType getDevice() override { return DeviceType::CPU; }
  /**
   * @param relName in the legacy catalog, e.g. inputs/ssbm100/lineorder.csv
   * This is very very hacky dealing with types. We need to come back and fix
   * the catalog for this or create an actual file format
   */
  [[nodiscard]] RelBuilder scan(
      const std::string &relName,
      std::initializer_list<std::string> relAttrs) override {
    std::vector<DanglingAttr> attrs = constructDanglingAttrs(relName, relAttrs);
    RecordType scan_record_type = rel(relName)(attrs);

    auto meta_data_records_map = scan_record_type.getArgsMap();
    std::vector<
        std::pair<RecordAttribute *, std::vector<std::filesystem::path>>>
        relation_md;

    for (const auto &attr : relAttrs) {
      auto md_paths = getMdForAttribute(attr);
      relation_md.emplace_back(meta_data_records_map[attr], md_paths);
    }
    auto builder = getBuilder();
    auto rel = builder.scan(relation_md);
    rel = rel.hintRowCount(getRowHint(relName));
    return rel;
  }

  [[nodiscard]] std::string getRelName(const std::string &base) override {
    LOG(FATAL) << "N/A to NVMe shapers";
  }

  RelBuilder distribute_build(RelBuilder input) override {
    auto rel = input
                   .router(getDOP(), getSlack(), RoutingPolicy::LOCAL,
                           getDevice(), getAffinitizer())
                   .memmove(8, getDevice());

    if (getDevice() == DeviceType::GPU) rel = rel.to_gpu();

    return rel;
  }

 public:
  CPUOnlyNVMeMorsel(std::vector<std::string> input_dirs,
                    const std::string &catalog_path,
                    decltype(input_sizes) input_sizes, bool allowMoves,
                    size_t slack)
      : InputPrefixQueryShaper("N/A", input_sizes, allowMoves, slack),
        input_dirs(sort_vector(
            input_dirs)),  // sort so we always iterate in the same order
        catalog_path(catalog_path) {}

 protected:
  std::vector<std::filesystem::path> getMdForAttribute(
      const std::string &attr) const {
    std::vector<std::filesystem::path> md_paths;
    //     for each input dir, find all associated metadata files for this
    //     attribute
    for (const auto &dir : input_dirs) {
      auto dir_files = getSortedDirectoryFiles(dir);
      for (const auto &entry : dir_files) {
        if (entry.string().find(attr + "_") != std::string::npos &&
            entry.string().find("metadata.json") != std::string::npos) {
          md_paths.push_back(entry);
        }
      }
    }
    std::sort(md_paths.begin(), md_paths.end());
    return md_paths;
  }

  /**
   * gross and hacky helper to construct dangling attributes for a relation and
   * vector of requested attributes using the legacy catalogue
   */
  std::vector<DanglingAttr> constructDanglingAttrs(
      const std::string &relName,
      const std::vector<std::string> &relAttrs) const {
    std::vector<DanglingAttr> attrs;
    CatalogParser catalog = CatalogParser(catalog_path);
    auto inputInfo =
        catalog.getInputInfo(catalog_path + "/" + relName + ".csv");
    auto &collType = dynamic_cast<CollectionType &>(*(inputInfo->exprType));

    const ExpressionType &nestedType = collType.getNestedType();
    auto record_type = dynamic_cast<const RecordType &>(nestedType);

    for (const auto &attr : relAttrs) {
      auto arg = record_type.getArg(attr);
      if (!arg) {
        LOG(FATAL) << "Attribute " << attr << " not found";
      }
      switch (arg->getOriginalType()->getTypeID()) {
        case BOOL:
          attrs.push_back(dangling_attr::Bool(attr));
          break;
        case DSTRING:
          attrs.push_back(dangling_attr::DString(attr));
          break;
        case STRING:
          attrs.push_back(dangling_attr::String(attr));
          break;
        case FLOAT:
          attrs.push_back(dangling_attr::Float(attr));
          break;
        case INT:
          attrs.push_back(dangling_attr::Int(attr));
          break;
        case DATE:
          attrs.push_back(dangling_attr::Date(attr));
          break;
        case INT64:
          attrs.push_back(dangling_attr::Int64(attr));
          break;
        case RECORD:
        case LIST:
        case BAG:
        case SET:
        case COMPOSITE:
        case BLOCK:
        case INDEXEDSEQ:
          LOG(FATAL) << "Unsupported type";
          break;
      }
    }
    return attrs;
  }

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
};
};  // namespace proteus

#endif  // PROTEUS_NVME_SHAPERS_HPP
