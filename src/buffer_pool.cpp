#include "flexql/buffer_pool.hpp"

#include <chrono>
#include <tuple>
#include <utility>
#include <vector>

#include "flexql/lock_order.hpp"

namespace flexql {

BufferFrame::BufferFrame() : page(), dirty(false), pin_count(0), last_access_time(0), frame_mutex() {}

BufferPool::BufferPool(size_t max_buffer_pages) : max_buffer_pages_(max_buffer_pages) {
  page_table.reserve(max_buffer_pages_);
  frame_store_.reserve(max_buffer_pages_);
  lru_positions_.reserve(max_buffer_pages_);
}

void BufferPool::registerTable(uint32_t table_id, StorageEngine* storage) {
  DebugLockLevelGuard level_guard(LockLevel::BUFFER_POOL);
  std::unique_lock<std::shared_mutex> lock(pool_mutex_);
  table_storage_[table_id] = storage;
}

uint64_t BufferPool::encodePageKey(uint32_t table_id, uint32_t page_id) {
  return (static_cast<uint64_t>(table_id) << 32U) | static_cast<uint64_t>(page_id);
}

uint32_t BufferPool::decodeTableId(uint64_t key) {
  return static_cast<uint32_t>(key >> 32U);
}

uint32_t BufferPool::decodePageId(uint64_t key) {
  return static_cast<uint32_t>(key & 0xFFFFFFFFULL);
}

uint64_t BufferPool::nowMicros() {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

StorageEngine* BufferPool::storageForTableLocked(uint32_t table_id) const {
  const auto it = table_storage_.find(table_id);
  if (it == table_storage_.end()) {
    return nullptr;
  }
  return it->second;
}

void BufferPool::touchMruLocked(uint64_t key) {
  const auto it = lru_positions_.find(key);
  if (it == lru_positions_.end()) {
    return;
  }
  lru_list_.splice(lru_list_.end(), lru_list_, it->second);
  it->second = std::prev(lru_list_.end());
}

void BufferPool::insertFrameLocked(uint64_t key, std::unique_ptr<BufferFrame> frame) {
  BufferFrame* raw = frame.get();
  frame_store_[key] = std::move(frame);
  page_table[key] = raw;
  lru_list_.push_back(key);
  lru_positions_[key] = std::prev(lru_list_.end());
}

BufferPool::EvictionCandidate BufferPool::pickEvictionCandidateLocked() {
  EvictionCandidate result{false, false, 0, 0, Page()};

  for (auto it = lru_list_.begin(); it != lru_list_.end(); ++it) {
    const uint64_t key = *it;
    BufferFrame* frame = page_table[key];
    std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
    if (frame->pin_count == 0 && !frame->dirty) {
      result.found = true;
      result.dirty = false;
      result.table_id = decodeTableId(key);
      result.page_id = decodePageId(key);
      lru_positions_.erase(key);
      lru_list_.erase(it);
      page_table.erase(key);
      frame_store_.erase(key);
      return result;
    }
  }

  for (auto it = lru_list_.begin(); it != lru_list_.end(); ++it) {
    const uint64_t key = *it;
    BufferFrame* frame = page_table[key];
    std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
    if (frame->pin_count == 0 && frame->dirty) {
      result.found = true;
      result.dirty = true;
      result.table_id = decodeTableId(key);
      result.page_id = decodePageId(key);
      result.page_copy = frame->page;
      lru_positions_.erase(key);
      lru_list_.erase(it);
      page_table.erase(key);
      frame_store_.erase(key);
      return result;
    }
  }

  return result;
}

BufferFrame* BufferPool::fetchPage(uint32_t table_id, uint32_t page_id) {
  const uint64_t key = encodePageKey(table_id, page_id);

  {
    DebugLockLevelGuard level_guard(LockLevel::BUFFER_POOL);
    std::shared_lock<std::shared_mutex> shared_lock(pool_mutex_);
    const auto hit = page_table.find(key);
    if (hit != page_table.end()) {
      BufferFrame* frame = hit->second;
      {
        DebugLockLevelGuard frame_level(LockLevel::BUFFER_FRAME);
        std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
        frame->pin_count += 1;
        frame->last_access_time = nowMicros();
      }
      return frame;
    }
  }

  StorageEngine* storage = nullptr;
  {
    DebugLockLevelGuard level_guard(LockLevel::BUFFER_POOL);
    std::shared_lock<std::shared_mutex> shared_lock(pool_mutex_);
    storage = storageForTableLocked(table_id);
  }
  if (storage == nullptr) {
    return nullptr;
  }

  EvictionCandidate evicted{false, false, 0, 0, Page()};
  {
    DebugLockLevelGuard level_guard(LockLevel::BUFFER_POOL);
    std::unique_lock<std::shared_mutex> unique_lock(pool_mutex_);

    const auto hit = page_table.find(key);
    if (hit != page_table.end()) {
      BufferFrame* frame = hit->second;
      {
        DebugLockLevelGuard frame_level(LockLevel::BUFFER_FRAME);
        std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
        frame->pin_count += 1;
        frame->last_access_time = nowMicros();
      }
      touchMruLocked(key);
      return frame;
    }

    if (page_table.size() >= max_buffer_pages_) {
      evicted = pickEvictionCandidateLocked();
      if (!evicted.found) {
        return nullptr;
      }
    }
  }

  if (evicted.found && evicted.dirty) {
    StorageEngine* evicted_storage = nullptr;
    {
      DebugLockLevelGuard level3(LockLevel::BUFFER_POOL);
      std::shared_lock<std::shared_mutex> shared_lock(pool_mutex_);
      evicted_storage = storageForTableLocked(evicted.table_id);
    }
    if (evicted_storage == nullptr || !evicted_storage->writePage(evicted.page_id, evicted.page_copy)) {
      return nullptr;
    }
  }

  Page disk_page;
  if (!storage->readPage(page_id, disk_page)) {
    return nullptr;
  }

  std::unique_ptr<BufferFrame> new_frame = std::make_unique<BufferFrame>();
  new_frame->page = disk_page;
  new_frame->dirty = false;
  new_frame->pin_count = 1;
  new_frame->last_access_time = nowMicros();

  DebugLockLevelGuard level4(LockLevel::BUFFER_POOL);
  std::unique_lock<std::shared_mutex> unique_lock(pool_mutex_);
  const auto existing = page_table.find(key);
  if (existing != page_table.end()) {
    BufferFrame* frame = existing->second;
    {
      DebugLockLevelGuard frame_level(LockLevel::BUFFER_FRAME);
      std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
      frame->pin_count += 1;
      frame->last_access_time = nowMicros();
    }
    touchMruLocked(key);
    return frame;
  }

  insertFrameLocked(key, std::move(new_frame));
  return page_table[key];
}

const uint8_t* BufferPool::fetchPageRaw(uint32_t table_id, uint32_t page_id) {
  BufferFrame* frame = fetchPage(table_id, page_id);
  if (frame == nullptr) {
    return nullptr;
  }
  return reinterpret_cast<const uint8_t*>(frame->page.body.data());
}

void BufferPool::unpinPage(uint32_t table_id, uint32_t page_id) {
  const uint64_t key = encodePageKey(table_id, page_id);
  DebugLockLevelGuard level_guard(LockLevel::BUFFER_POOL);
  std::shared_lock<std::shared_mutex> shared_lock(pool_mutex_);
  const auto it = page_table.find(key);
  if (it == page_table.end()) {
    return;
  }
  BufferFrame* frame = it->second;
  DebugLockLevelGuard frame_level(LockLevel::BUFFER_FRAME);
  std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
  if (frame->pin_count > 0) {
    frame->pin_count -= 1;
  }
}

void BufferPool::markDirty(uint32_t table_id, uint32_t page_id) {
  const uint64_t key = encodePageKey(table_id, page_id);
  DebugLockLevelGuard level_guard(LockLevel::BUFFER_POOL);
  std::shared_lock<std::shared_mutex> shared_lock(pool_mutex_);
  const auto it = page_table.find(key);
  if (it == page_table.end()) {
    return;
  }
  BufferFrame* frame = it->second;
  DebugLockLevelGuard frame_level(LockLevel::BUFFER_FRAME);
  std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
  frame->dirty = true;
}

void BufferPool::flushAll() {
  std::vector<std::tuple<StorageEngine*, uint32_t, Page>> pending_writes;

  {
    DebugLockLevelGuard level_guard(LockLevel::BUFFER_POOL);
    std::shared_lock<std::shared_mutex> shared_lock(pool_mutex_);
    pending_writes.reserve(page_table.size());

    for (const auto& kv : page_table) {
      const uint64_t key = kv.first;
      BufferFrame* frame = kv.second;
      StorageEngine* storage = storageForTableLocked(decodeTableId(key));
      if (storage == nullptr) {
        continue;
      }

      DebugLockLevelGuard frame_level(LockLevel::BUFFER_FRAME);
      std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
      if (!frame->dirty) {
        continue;
      }

      pending_writes.emplace_back(storage, decodePageId(key), frame->page);
      frame->dirty = false;
    }
  }

  for (const auto& write_op : pending_writes) {
    StorageEngine* storage = std::get<0>(write_op);
    const uint32_t page_id = std::get<1>(write_op);
    const Page& page = std::get<2>(write_op);
    storage->writePage(page_id, page);
  }
}

void BufferPool::evictTable(uint32_t table_id) {
  std::vector<std::tuple<StorageEngine*, uint32_t, Page>> pending_writes;
  std::vector<uint64_t> to_remove;

  {
    DebugLockLevelGuard level_guard(LockLevel::BUFFER_POOL);
    std::unique_lock<std::shared_mutex> lock(pool_mutex_);
    for (const auto& kv : page_table) {
      const uint64_t key = kv.first;
      if (decodeTableId(key) != table_id) {
        continue;
      }

      BufferFrame* frame = kv.second;
      StorageEngine* storage = storageForTableLocked(table_id);
      if (storage != nullptr) {
        DebugLockLevelGuard frame_level(LockLevel::BUFFER_FRAME);
        std::lock_guard<std::mutex> frame_lock(frame->frame_mutex);
        if (frame->dirty) {
          pending_writes.emplace_back(storage, decodePageId(key), frame->page);
          frame->dirty = false;
        }
      }
      to_remove.push_back(key);
    }

    for (uint64_t key : to_remove) {
      const auto pos_it = lru_positions_.find(key);
      if (pos_it != lru_positions_.end()) {
        lru_list_.erase(pos_it->second);
        lru_positions_.erase(pos_it);
      }
      page_table.erase(key);
      frame_store_.erase(key);
    }
  }

  for (const auto& write_op : pending_writes) {
    StorageEngine* storage = std::get<0>(write_op);
    const uint32_t page_id = std::get<1>(write_op);
    const Page& page = std::get<2>(write_op);
    storage->writePage(page_id, page);
  }
}

}  // namespace flexql
