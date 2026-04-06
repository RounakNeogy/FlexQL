#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "flexql/core_types.hpp"

namespace flexql {

struct __attribute__((packed)) RecordPointer {
  uint32_t page_id;
  uint16_t row_offset;
};

static_assert(sizeof(RecordPointer) == 6, "RecordPointer must be 6 bytes");

uint64_t xxhash64(const void* data, size_t len, uint64_t seed = 0);
uint64_t encodePrimaryKey(ColType type, const RowValue& value);

struct BPlusTreeNode {
  bool is_leaf;
  uint32_t next_leaf;
  std::vector<int64_t> keys;
  std::vector<RecordPointer> values;
  std::vector<RowValue::VarcharValue> varchar_values;
  std::vector<uint32_t> children;

  explicit BPlusTreeNode(bool leaf = true);
};

class BPlusTree {
 public:
  static constexpr size_t kOrder = 128;
  static constexpr size_t kMaxKeys = kOrder - 1;

  BPlusTree();

  void clear();
  bool empty() const;
  bool insert(int64_t key, RecordPointer ptr, const RowValue::VarcharValue* raw_varchar = nullptr);
  bool search(int64_t key, RecordPointer& out_ptr, const RowValue::VarcharValue* raw_varchar = nullptr) const;

  void buildBulkFromSorted(const std::vector<std::pair<int64_t, RecordPointer>>& sorted_pairs);

 private:
  struct SplitResult {
    bool has_split;
    int64_t promoted_key;
    uint32_t right_node;
  };

  uint32_t root_;
  std::vector<BPlusTreeNode> node_pool_;

  uint32_t allocateNode(bool is_leaf);
  SplitResult insertRecursive(uint32_t node_idx,
                              int64_t key,
                              RecordPointer ptr,
                              const RowValue::VarcharValue* raw_varchar);
  bool leafCollisionMatch(const BPlusTreeNode& node,
                          size_t pos,
                          const RowValue::VarcharValue* raw_varchar) const;
};

}  // namespace flexql

