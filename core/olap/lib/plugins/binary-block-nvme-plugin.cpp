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

#include <rapidjson/document.h>

#include <magic_enum.hpp>
#include <olap/plugins/binary-block-nvme-plugin.hpp>
#include <regex>
#include <variant>

#include "lib/operators/operators.hpp"
#include "olap/util/parallel-context.hpp"
#include "rapidjson/error/en.h"
#include "rapidjson/stringbuffer.h"
using namespace llvm;

extern "C" {
void *getNvmePageIdPtr(NvmePlugin *pg, uint8_t attribute_no,
                       uint8_t partition_no, uint32_t block_no) noexcept {
  DCHECK_LT(attribute_no, pg->m_attribute_metadata.size());
  DCHECK_LT(partition_no, pg->m_attribute_metadata[attribute_no].size());
  DCHECK_LT(partition_no,
            pg->m_attribute_metadata[attribute_no][partition_no].num_blocks);
  const uint8_t cpu_numa_affinity =
      pg->m_attribute_metadata[attribute_no][partition_no].numa_node;
  uint64_t page_id =
      (uint64_t)(cpu_numa_affinity | NvmePlugin::PageId_t::page_id_bit_in_numa)
          << 56 |
      (uint64_t)attribute_no << 48 | (uint64_t)partition_no << 40 |
      (uint64_t)block_no;
  return reinterpret_cast<void *>(page_id);
}
}

std::ostream &operator<<(std::ostream &out,
                         const NvmePlugin::PageId_t &page_id) {
  return out << "PageId_t{cpu_numa_affinity="
             << static_cast<int>(page_id.cpu_numa_affinity)
             << ", attribute_no=" << static_cast<int>(page_id.attribute_no)
             << ", partition_no=" << static_cast<int>(page_id.partition_no)
             << ", block_no=" << page_id.block_no << "}";
}

template <class T>
std::vector<RecordAttribute *> getAttributeVector(
    const std::vector<std::pair<RecordAttribute *, T>> &whichFields) {
  std::vector<RecordAttribute *> attrs;
  attrs.reserve(whichFields.size());
  for (const auto &val : whichFields) {
    attrs.emplace_back(new RecordAttribute{*val.first});
  }
  return attrs;
}

template <class T>
RecordType recordTypeFromDataVector(
    const std::vector<std::pair<RecordAttribute *, T>> &whichFields) {
  return getAttributeVector(whichFields);
}

NvmePlugin::NvmePlugin(
    OlapParallelContext *context,
    const std::vector<
        std::pair<RecordAttribute *, std::vector<std::filesystem::path>>>
        &whichFields)
    : BinaryBlockPlugin(context, whichFields.front().first->getRelationName(),
                        recordTypeFromDataVector(whichFields),
                        getAttributeVector(whichFields), false) {
  Nparts = whichFields.front().second.size();
  for (const auto &field : whichFields) {
    CHECK_EQ(field.second.size(), Nparts)
        << "All attributes must have the same number of partitions. This field "
           "attribute: "
        << field.first->getAttrName();
  }

  // TODO fix m_attribute_metadata move / copy constructors
  m_attribute_metadata.reserve(whichFields.size());
  for (auto &field : whichFields) {
    size_t blocks_for_field = 0;
    m_attribute_metadata.push_back({});
    m_attribute_metadata.back().reserve(Nparts);
    for (auto &attr_part_meta : field.second) {
      m_attribute_metadata.back().emplace_back(attr_part_meta);
      blocks_for_field += m_attribute_metadata.back().back().num_blocks;
      //      LOG(INFO) << field.first->getAttrName() << " has "
      //                << m_attribute_metadata.back().back().num_blocks
      //                << " blocks in partition "
      //                << m_attribute_metadata.back().back().data_file_path;
    }
    LOG(INFO) << field.first->getAttrName() << " has " << blocks_for_field
              << " total blocks";
    //  TODO store dict path in metadata
    //  this is ugly
    const auto data_path =
        m_attribute_metadata.back()[0].data_file_path.string();
    std::string dict_path;
    auto &attr = field.first;
    if (attr->getOriginalType()->getTypeID() == DSTRING) {
      std::regex pattern("(.*)(_[0-9]+_[0-9]+)");
      std::smatch matches;
      // fetch the dictionary
      if (std::regex_search(data_path, matches, pattern)) {
        dict_path = matches[1];
      } else {
        LOG(FATAL) << "bad regex";
      }
      void *dict = StorageManager::getInstance().getDictionaryOf(dict_path);
      ((DStringType *)(attr->getOriginalType()))->setDictionary(dict);
    }
  }

  for (int i = 0; i < Nparts; i++) {
    auto num_blocks_in_part = m_attribute_metadata.front()[i].num_blocks;
    part_sizes.emplace_back(num_blocks_in_part);
    for (auto &attr_meta : m_attribute_metadata) {
      CHECK_EQ(attr_meta[i].num_blocks, num_blocks_in_part)
          << "expect the same partition number of each attribute to have the "
             "same number of blocks";
    }
  }
}

NvmePlugin::~NvmePlugin() {}

NvmePlugin::PageIOInfo NvmePlugin::getPageIoInfo(
    const PageId_t &page_id) const {
  if (page_id.getAttributeNo() >= m_attribute_metadata.size() ||
      page_id.getPartitionNo() >=
          m_attribute_metadata[page_id.getAttributeNo()].size()) {
    LOG(WARNING) << "PageId_t references non-existent attribute or partition";
    throw std::out_of_range(
        "PageId_t references non-existent attribute or partition");
  }

  const auto &partMetaData =
      m_attribute_metadata[page_id.getAttributeNo()][page_id.getPartitionNo()];
  if (page_id.getBlockNo() >= partMetaData.num_blocks) {
    LOG(WARNING) << "PageId_t references non-existent block";
    throw std::out_of_range("PageId_t references non-existent block");
  }

  int fd = partMetaData.fd;
  DCHECK_GE(fd, 0) << "File descriptor is not valid";
#ifndef NDEBUG
  auto res = fcntl(fd, F_GETFD);
  PCHECK(res != -1) << "File descriptor is not valid for part no: "
                    << page_id.getPartitionNo()
                    << ", block no: " << page_id.getBlockNo()
                    << partMetaData.data_file_path;
#endif
  uint64_t offset = partMetaData.block_offsets[page_id.getBlockNo()];
  size_t size =
      static_cast<size_t>(partMetaData.block_sizes[page_id.getBlockNo()]);
  if (partMetaData.data_format ==
      NvmePlugin::AttributePartMetaData::DataFormat_t::COMPRESSED) {
    auto chunk_sizes = partMetaData.chunk_sizes[page_id.getBlockNo()];
    return {fd,          offset, size,
            chunk_sizes, true,   partMetaData.decompressed_chunk_size};
  }

  return {fd, offset, size, {}, false, 0};
}

std::pair<llvm::Value *, llvm::Value *> NvmePlugin::getPartitionSizes(
    OlapParallelContext *context, llvm::Value *session_ptr) const {
  uint64_t max_blocks_in_partition = 0;
  // this is assuming that all attrs have the same number of blocks
  for (int i = 0; i < Nparts; i++) {
    auto num_blocks_in_part = m_attribute_metadata.front()[i].num_blocks;
    max_blocks_in_partition =
        std::max(max_blocks_in_partition, num_blocks_in_part);
  }

  return {context->CastPtrToLlvmPtr(
              llvm::PointerType::getUnqual(
                  llvm::ArrayType::get(context->createSizeType(), Nparts)),
              part_sizes.data()),
          context->createSizeT(max_blocks_in_partition)};
}

void NvmePlugin::freePartitionSizes(OlapParallelContext *context,
                                    llvm::Value *v) const {}

void NvmePlugin::generate(const ::Operator &producer,
                          OlapParallelContext *context) {
  return scan(producer, context);
}

/**
 * Updates the loop control variables to point to the next data entry.
 * This method inserts code into the increment block (IncBB) during the scan
 * process. It handles incrementing the partition index or resetting it and
 * advancing block_idx if necessary,
 *
 */
void NvmePlugin::nextEntry(OlapParallelContext *context) {
  // Prepare
  LLVMContext &llvmContext = context->getLLVMContext();
  IRBuilder<> *Builder = context->getBuilder();
  Function *F = Builder->GetInsertBlock()->getParent();

  // Necessary because it's blockCtr that affects the scan loop
  auto mem_blockCtr = NamedValuesBinaryCol.at(blockCtrVar);

  // Necessary because it's the itemCtr that affects the scan loop
  auto part_idx_ptr = NamedValuesBinaryCol.at("part_idx_ptr");

  // Necessary because it's the itemCtr that affects the scan loop
  auto block_idx_ptr = NamedValuesBinaryCol.at("block_idx_ptr");

  // Increment and store back
  BasicBlock *wrapBB = BasicBlock::Create(llvmContext, "incWrap", F);
  BasicBlock *stepBB = BasicBlock::Create(llvmContext, "incStep", F);
  BasicBlock *afterBB = BasicBlock::Create(llvmContext, "incAfter", F);

  Value *part_idx =
      Builder->CreateLoad(part_idx_ptr->getType()->getPointerElementType(),
                          part_idx_ptr, "part_idx");

  auto *size_type = (IntegerType *)part_idx->getType();

  Value *part_N = ConstantInt::get(size_type, Nparts - 1);

  Value *cond = Builder->CreateICmpULT(part_idx, part_N);
  Builder->CreateCondBr(cond, stepBB, wrapBB);

  {
    // IfThen
    // increment part_idx (which partition we are accessing)
    Builder->SetInsertPoint(stepBB);

    Builder->CreateStore(
        Builder->CreateAdd(part_idx, ConstantInt::get(size_type, 1)),
        part_idx_ptr);

    Builder->CreateBr(afterBB);
  }

  {
    // IfElse
    // wrap around back to partition 0 and advance block_idx by 1
    Builder->SetInsertPoint(wrapBB);

    Builder->CreateStore(ConstantInt::get(size_type, 0), part_idx_ptr);

    Value *block_idx =
        Builder->CreateLoad(block_idx_ptr->getType()->getPointerElementType(),
                            block_idx_ptr, "block_idx");
    Builder->CreateStore(
        Builder->CreateAdd(block_idx, ConstantInt::get(size_type, 1)),
        block_idx_ptr);

    Builder->CreateBr(afterBB);
  }

  {
    // IfAfter
    // In both cases update mem_blockCtr which is used for the OID
    Builder->SetInsertPoint(afterBB);

    // itemCtr = block_idx_ptr * Nparts + part_idx_ptr * blockSize
    Value *itemCtr = Builder->CreateAdd(
        Builder->CreateMul(
            Builder->CreateLoad(
                block_idx_ptr->getType()->getPointerElementType(),
                block_idx_ptr),
            ConstantInt::get(size_type, Nparts)),
        Builder->CreateLoad(part_idx_ptr->getType()->getPointerElementType(),
                            part_idx_ptr));

    Builder->CreateStore(itemCtr, mem_blockCtr);
  }
}

/**
 * Iterates over all partitions of the binary input a round-robin manner.
 * Each iteration sets the OperatorState that will be used by producer
 * @param producer The operator that is producing tuples. (scan.hpp)
 */
void NvmePlugin::scan(const ::Operator &producer,
                      OlapParallelContext *context) {
  LLVMContext &llvmContext = context->getLLVMContext();

  context->setGlobalFunction(true);

  Function *F = context->getGlobalFunction();
  IRBuilder<> *Builder = context->getBuilder();

  // Prepare
  IntegerType *size_type = context->createSizeType();

  // Container for the variable bindings
  map<RecordAttribute, ProteusValueMemory> variableBindings;

  // Get the ENTRY BLOCK
  context->setCurrentEntryBlock(Builder->GetInsertBlock());

  llvm::Value *session = getSession(context);

  auto partsizes = getPartitionSizes(context, session);
  Value *part_sizes_ptr = partsizes.first;
  part_sizes_ptr->setName("part_sizes_ptr");
  Value *maxPackCnt = partsizes.second;  /// size of largest partition in blocks
  maxPackCnt->setName("maxPackCnt");

  // TODO this is now sort of the metadatas responsbility
  // but it also means we restrict ourselves to using the largest field size
  // in the whole table for the block size
  size_t max_field_size = 0;
  for (const auto &f : wantedFields) {
    size_t field_size = context->getSizeOf(f->getLLVMType(llvmContext));
    max_field_size = std::max(field_size, max_field_size);
  }

  ConstantInt *zero_idx = ConstantInt::get(size_type, 0);

  // index of the partition we are currently scanning
  AllocaInst *part_idx_ptr =
      context->CreateEntryBlockAlloca(F, "part_idx_ptr", size_type);
  Builder->CreateStore(zero_idx, part_idx_ptr);
  NamedValuesBinaryCol["part_idx_ptr"] = part_idx_ptr;

  // index of the block we are currently scanning in the partition
  AllocaInst *block_idx_ptr =
      context->CreateEntryBlockAlloca(F, "block_idx_ptr", size_type);
  Builder->CreateStore(zero_idx, block_idx_ptr);
  NamedValuesBinaryCol["block_idx_ptr"] = block_idx_ptr;

  AllocaInst *mem_blockCtr =
      context->CreateEntryBlockAlloca(F, blockCtrVar, size_type);
  Builder->CreateStore(zero_idx, mem_blockCtr);
  NamedValuesBinaryCol[blockCtrVar] = mem_blockCtr;

  auto blockSize =
      ConstantInt::get(size_type, BlockManager::block_size / max_field_size);

  BasicBlock *CondBB = BasicBlock::Create(llvmContext, "scanCond", F);

  // Make the new basic block for the loop header (BODY), inserting after
  // current block.
  BasicBlock *LoopBB = BasicBlock::Create(llvmContext, "scanBody", F);
  BasicBlock *MainBB = BasicBlock::Create(llvmContext, "scanMain", F);

  // Make the new basic block for the increment, inserting after current
  // block.
  BasicBlock *IncBB = BasicBlock::Create(llvmContext, "scanInc", F);

  // Create the "AFTER LOOP" block and insert it.
  BasicBlock *AfterBB = BasicBlock::Create(llvmContext, "scanEnd", F);
  context->setEndingBlock(AfterBB);

  {
    Builder->SetInsertPoint(CondBB);

    // /**
    //  * Equivalent:
    //  * while(block_idx < max(partsize))
    //  */

    Value *block_idx =
        Builder->CreateLoad(block_idx_ptr->getType()->getPointerElementType(),
                            block_idx_ptr, "block_idx");

    Value *cond = Builder->CreateICmpULT(block_idx, maxPackCnt);

    // Insert the conditional branch into the end of CondBB.
    Builder->CreateCondBr(cond, LoopBB, AfterBB);
  }

  // Start insertion in LoopBB.
  Builder->SetInsertPoint(LoopBB);

  Value *part_idx =
      Builder->CreateLoad(part_idx_ptr->getType()->getPointerElementType(),
                          part_idx_ptr, "part_idx");
  // This load may be redundant
  Value *block_idx =
      Builder->CreateLoad(block_idx_ptr->getType()->getPointerElementType(),
                          block_idx_ptr, "block_idx");

  auto partBlockCntPtr = Builder->CreateInBoundsGEP(
      part_sizes_ptr->getType()->getNonOpaquePointerElementType(),
      part_sizes_ptr, std::vector<Value *>{context->createInt64(0), part_idx});
  Value *partBlockCnt = Builder->CreateLoad(
      partBlockCntPtr->getType()->getPointerElementType(), partBlockCntPtr);
  partBlockCnt->setName("partBlockCnt");

  Value *part_unfinished = Builder->CreateICmpULT(block_idx, partBlockCnt);

  // If we are already at the end of this block jump to IncBB
  Builder->CreateCondBr(part_unfinished, MainBB, IncBB);

  Builder->SetInsertPoint(MainBB);

  // Get the 'oid' of each record and pass it along.
  // More general/lazy plugins will only perform this action,
  // instead of eagerly 'converting' fields
  // FIXME This action corresponds to materializing the oid. Do we want this?
  RecordAttribute tupleIdentifier{
      fnamePrefix, activeLoop,
      this->getOIDType()};  // FIXME: OID type for blocks ?

  // OID is block number (across all partitions)
  // We should probably materialize the OID in unpack as tuple number
  // Not sure if he _have_ to do that
  ProteusValueMemory mem_posWrapper{mem_blockCtr, context->createFalse()};
  variableBindings[tupleIdentifier] = mem_posWrapper;

  // Actual Work (Loop through attributes etc.)
  for (size_t i = 0; i < wantedFields.size(); ++i) {
    RecordAttribute attr(*(wantedFields[i]));
    RecordAttribute block_attr(attr, true);

    Type *ptr_t =
        PointerType::get(attr.getLLVMType(context->getLLVMContext()), 0);

    // _should_ be fine, but really we should check
    Value *block_idx_32bit =
        Builder->CreateTruncOrBitCast(block_idx, Builder->getInt32Ty());
    Value *this_ptr = context->getBuilder()->CreateIntToPtr(
        context->createInt64((uintptr_t)this),
        Type::getInt8PtrTy(context->getLLVMContext()));

    Value *page_id = context->gen_call(
        getNvmePageIdPtr,
        {this_ptr, context->createInt8(i), part_idx, block_idx_32bit});

    string bufVarStr = string(bufVar);
    string currBufVar = bufVarStr + "." + attr.getAttrName() + "_ptr";

    AllocaInst *mem_currResult =
        context->CreateEntryBlockAlloca(F, currBufVar, ptr_t);
    auto page_id_as_ptr = Builder->CreateBitCast(page_id, ptr_t);
    Builder->CreateStore(page_id_as_ptr, mem_currResult);

    ProteusValueMemory mem_valWrapper{mem_currResult, context->createFalse()};
    variableBindings[block_attr] = mem_valWrapper;
  }

  // This Alloca is used to store the number of blocks of each attribute emitted
  // from the scan to the parent operator in each iteration.
  // It is currently always 1 block per attribute per iteration.
  AllocaInst *blockCnt_ptr =
      context->CreateEntryBlockAlloca(F, "blockCnt", partBlockCnt->getType());
  Builder->CreateStore(ConstantInt::get(size_type, 1), blockCnt_ptr);

  /**
   * activeCnt is a special attrName.
   * Here it is used for blockCnt, in mem-move when we convert a rowgroup of
   * PageId_t to real in-memory blocks we need to set activeCnt to the tupleCnt
   * of the rowgroup
   * @see BlockToTuples::consume
   */
  RecordAttribute blockCnt =
      RecordAttribute(fnamePrefix, "activeCnt", this->getOIDType());

  ProteusValueMemory mem_blockCntWrapper;
  mem_blockCntWrapper.mem = blockCnt_ptr;
  mem_blockCntWrapper.isNull = context->createFalse();
  variableBindings[blockCnt] = mem_blockCntWrapper;

  AllocaInst *tupleCnt_ptr =
      context->CreateEntryBlockAlloca(F, "tupleCnt", partBlockCnt->getType());
  Builder->CreateStore(ConstantInt::get(size_type, 1), tupleCnt_ptr);

  RecordAttribute tupleCnt{fnamePrefix, "tupleCnt", this->getOIDType()};
  Value *this_ptr = context->getBuilder()->CreateIntToPtr(
      context->createInt64((uintptr_t)this),
      Type::getInt8PtrTy(context->getLLVMContext()));

  Builder->CreateStore(context->gen_call(&::getRowGroupTupleCount,
                                         {part_idx, block_idx, this_ptr}),
                       tupleCnt_ptr);
  ProteusValueMemory mem_tupleCntWrapper;
  mem_tupleCntWrapper.mem = tupleCnt_ptr;
  mem_tupleCntWrapper.isNull = context->createFalse();
  variableBindings[tupleCnt] = mem_tupleCntWrapper;

  // // Start insertion in IncBB.
  Builder->SetInsertPoint(IncBB);
  nextEntry(context);

  Builder->CreateBr(CondBB);

  Builder->SetInsertPoint(MainBB);

  OperatorState state{producer, variableBindings};
  producer.getParent()->consume(context, state);

  // Insert an explicit fall through from the current (body) block to IncBB.
  Builder->CreateBr(IncBB);

  // Insert an explicit fall through from the current (entry) block to the
  // CondBB.
  Builder->SetInsertPoint(context->getCurrentEntryBlock());
  Builder->CreateBr(CondBB);
  // Builder->CreateRetVoid();

  //  Finish up with end (the AfterLoop)
  //  Any new code will be inserted in AfterBB.
  Builder->SetInsertPoint(context->getEndingBlock());
  // Builder->SetInsertPoint(AfterBB);

  releaseSession(context, session);
}

NvmePlugin::AttributePartMetaData::AttributePartMetaData(
    const std::filesystem::path &md_path) {
  CHECK(std::filesystem::exists(md_path))
      << "AttributePartMetaData file does not exist: " << md_path;
  auto json_metadata = mmap_file{md_path, PAGEABLE};

  const char *buf_json =
      reinterpret_cast<const char *>(json_metadata.getData());

  rapidjson::Document document;
  auto &parsed = document.Parse(buf_json, json_metadata.getFileSize());
  if (parsed.HasParseError()) {
    auto ok = (rapidjson::ParseResult)parsed;
    auto *err = "Error parsing AttributePartMetaData";
    LOG(FATAL) << "Error parsing: " << md_path << " . JSON parse error: "
               << RAPIDJSON_NAMESPACE::GetParseError_En(ok.Code()) << " ("
               << ok.Offset() << ")";
  }

  CHECK(document.IsObject());

  CHECK(document.HasMember("data_file"));
  CHECK(document["data_file"].IsString());
  data_file_path = document["data_file"].GetString();
  if (data_file_path.is_relative()) {
    data_file_path = md_path.parent_path() / data_file_path;
  }
  CHECK(std::filesystem::exists(data_file_path))
      << "data_file does not exist: " << data_file_path;

  CHECK(document.HasMember("block_sizes"));
  CHECK(document["block_sizes"].IsArray());
  auto block_sizes_json_array = document["block_sizes"].GetArray();
  block_sizes.reserve(block_sizes_json_array.Size());
  for (auto &v : block_sizes_json_array) {
    CHECK(v.IsUint());
    block_sizes.push_back(v.GetUint());
  }

  CHECK(document.HasMember("block_offsets"));
  CHECK(document["block_offsets"].IsArray());
  auto block_offsets_json_array = document["block_offsets"].GetArray();
  block_offsets.reserve(block_offsets_json_array.Size());
  for (auto &v : block_offsets_json_array) {
    CHECK(v.IsUint64()) << " expected Uint64, is: "
                        << magic_enum::enum_name(v.GetType());
    block_offsets.push_back(v.GetUint64());
  }

  CHECK(document.HasMember("value_counts"));
  CHECK(document["value_counts"].IsArray());
  auto value_counts_json_array = document["value_counts"].GetArray();
  value_counts.reserve(value_counts_json_array.Size());
  for (auto &v : value_counts_json_array) {
    CHECK(v.IsUint());
    value_counts.push_back(v.GetUint());
  }

  CHECK_EQ(block_offsets.size(), block_sizes.size());
  CHECK_EQ(value_counts.size(), block_sizes.size());

  CHECK(document.HasMember("num_blocks"));
  CHECK(document["num_blocks"].IsUint64());

  num_blocks = document["num_blocks"].GetUint64();
  CHECK_EQ(block_sizes.size(), num_blocks);

  CHECK(document.HasMember("data_format"));
  CHECK(document["data_format"].IsString());
  auto maybe_data_format =
      magic_enum::enum_cast<DataFormat_t>(document["data_format"].GetString());
  CHECK(maybe_data_format.has_value());
  data_format = maybe_data_format.value();

  if (data_format == DataFormat_t::COMPRESSED) {
    CHECK(document.HasMember("chunk_sizes"));
    CHECK(document["chunk_sizes"].IsArray());
    auto chunk_sizes_json_array = document["chunk_sizes"].GetArray();
    chunk_sizes.reserve(chunk_sizes_json_array.Size());
    for (const auto &block_chunk_sizes_json : chunk_sizes_json_array) {
      CHECK(block_chunk_sizes_json.IsArray());
      std::vector<uint32_t> chunk_sizes_in_block;
      chunk_sizes_in_block.reserve(block_chunk_sizes_json.Size());
      for (const auto &chunk_size_json : block_chunk_sizes_json.GetArray()) {
        CHECK(chunk_size_json.IsUint());
        chunk_sizes_in_block.emplace_back(chunk_size_json.GetUint());
      }
      chunk_sizes.push_back(chunk_sizes_in_block);
    }

    // It is possible for a partition to be empty
    if (block_sizes.size() > 0) {
      max_compressed_block_size =
          *std::max_element(block_sizes.begin(), block_sizes.end());
    } else {
      max_compressed_block_size = 0;
    }

    CHECK(document.HasMember("decompressed_chunk_size"));
    CHECK(document["decompressed_chunk_size"].IsInt());
    decompressed_chunk_size = document["decompressed_chunk_size"].GetInt();
  }
  numa_node = fileNameToNumaNodeIndex(data_file_path);
  DLOG(INFO) << "opening data file: " << data_file_path << "for " << md_path;
  fd = open(data_file_path.c_str(), O_DIRECT);
  PCHECK(fd > 0);
#ifndef NDEBUG
  auto res = fcntl(fd, F_GETFD);
  PCHECK(res != -1) << "File descriptor is not valid";
#endif
}
NvmePlugin::AttributePartMetaData::~AttributePartMetaData() { close(fd); }

uint64_t NvmePlugin::getRowGroupTupleCount(uint64_t partIdx,
                                           uint64_t blockIdx) {
#ifndef NDEBUG
  auto values_in_first_col =
      m_attribute_metadata[0][partIdx].value_counts[blockIdx];
  for (const auto &attr_meta : m_attribute_metadata) {
    CHECK_EQ(values_in_first_col, attr_meta[partIdx].value_counts[blockIdx]);
  }
#endif
  return m_attribute_metadata[0][partIdx].value_counts[blockIdx];
}

uint64_t getRowGroupTupleCount(uint64_t partIdx, uint64_t blockIdx,
                               NvmePlugin *pg) {
  return pg->getRowGroupTupleCount(partIdx, blockIdx);
}
