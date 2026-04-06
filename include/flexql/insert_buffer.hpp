#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "flexql/buffer_pool.hpp"
#include "flexql/core_types.hpp"

namespace flexql {

class InsertBuffer {
 public:
  using RowPlacementCallback = void (*)(void* ctx, const Row& row, uint32_t page_id, uint16_t row_offset);

  static constexpr size_t kDefaultCapacity = 100000;

  explicit InsertBuffer(size_t capacity = kDefaultCapacity);

  bool append(const Row& row, int64_t pk);
  bool append(Row&& row, int64_t pk);
  bool shouldFlush() const;
  size_t size() const;
  void clear();

  bool findByPrimaryKey(int64_t pk, Row& out_row) const;
  bool containsPrimaryKey(int64_t pk) const;

  bool flush(Table& table,
             BufferPool& buffer_pool,
             uint32_t table_id,
             RowPlacementCallback callback = nullptr,
             void* callback_ctx = nullptr);

 private:
  bool flush_locked(Table& table,
                    BufferPool& buffer_pool,
                    uint32_t table_id,
                    RowPlacementCallback callback,
                    void* callback_ctx);

  size_t capacity_;
  std::vector<Row> buffer_;
  std::unordered_map<int64_t, Row*> buffer_index_;
  mutable std::mutex mtx_;
};

}  // namespace flexql
