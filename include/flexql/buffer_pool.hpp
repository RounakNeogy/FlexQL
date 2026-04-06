#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "flexql/storage_engine.hpp"

namespace flexql {

struct __attribute__((aligned(8))) BufferFrame {
  Page page;
  bool dirty;
  int pin_count;
  uint64_t last_access_time;
  std::mutex frame_mutex;

  BufferFrame();
};

class BufferPool {
 public:
  explicit BufferPool(size_t max_buffer_pages = 131072);

  void registerTable(uint32_t table_id, StorageEngine* storage);

  BufferFrame* fetchPage(uint32_t table_id, uint32_t page_id);
  const uint8_t* fetchPageRaw(uint32_t table_id, uint32_t page_id);
  void unpinPage(uint32_t table_id, uint32_t page_id);
  void markDirty(uint32_t table_id, uint32_t page_id);
  void flushAll();
  void evictTable(uint32_t table_id);

  static uint64_t encodePageKey(uint32_t table_id, uint32_t page_id);

 private:
  struct EvictionCandidate {
    bool found;
    bool dirty;
    uint32_t table_id;
    uint32_t page_id;
    Page page_copy;
  };

  using FrameOwnerMap = std::unordered_map<uint64_t, std::unique_ptr<BufferFrame>>;

  size_t max_buffer_pages_;
  std::unordered_map<uint64_t, BufferFrame*> page_table;
  FrameOwnerMap frame_store_;
  std::unordered_map<uint64_t, std::list<uint64_t>::iterator> lru_positions_;
  std::list<uint64_t> lru_list_;
  std::unordered_map<uint32_t, StorageEngine*> table_storage_;

  mutable std::shared_mutex pool_mutex_;

  static uint64_t nowMicros();
  static uint32_t decodeTableId(uint64_t key);
  static uint32_t decodePageId(uint64_t key);

  void touchMruLocked(uint64_t key);
  void insertFrameLocked(uint64_t key, std::unique_ptr<BufferFrame> frame);
  EvictionCandidate pickEvictionCandidateLocked();
  StorageEngine* storageForTableLocked(uint32_t table_id) const;
};

}  // namespace flexql
