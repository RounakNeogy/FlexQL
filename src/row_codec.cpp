#include "flexql/row_codec.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace flexql {

uint16_t align8(uint16_t n) {
  return static_cast<uint16_t>((n + 7U) & ~static_cast<uint16_t>(7U));
}

uint16_t columnStorageSize(ColType type) {
  switch (type) {
    case ColType::INT:
    case ColType::DECIMAL:
    case ColType::DATETIME:
      return 8;
    case ColType::VARCHAR:
      return 256;
  }
  return 0;
}

bool serializeRowIntoPage(const Row& row, const Schema& schema, uint16_t row_size_bytes, Page& page) {
  if (row.values.size() != schema.columns.size()) {
    return false;
  }
  if (page.row_size_bytes != row_size_bytes) {
    return false;
  }

  const uint16_t offset = page.free_space_offset;
  if (static_cast<uint32_t>(offset) + row_size_bytes > kPageBodyBytes) {
    return false;
  }

  char* dst = page.body.data() + offset;
  std::memset(dst, 0, row_size_bytes);

  dst[0] = static_cast<char>(kTombstoneLive);
  std::memcpy(dst + 8, &row.expiration_timestamp, sizeof(int64_t));

  uint16_t cursor = kRowHeaderAlignedBytes;
  for (size_t i = 0; i < schema.columns.size(); ++i) {
    switch (schema.columns[i].type) {
      case ColType::INT:
        std::memcpy(dst + cursor, &row.values[i].as_int, sizeof(int64_t));
        cursor += 8;
        break;
      case ColType::DECIMAL:
        std::memcpy(dst + cursor, &row.values[i].as_double, sizeof(double));
        cursor += 8;
        break;
      case ColType::DATETIME:
        std::memcpy(dst + cursor, &row.values[i].as_datetime, sizeof(int64_t));
        cursor += 8;
        break;
      case ColType::VARCHAR:
        std::memcpy(dst + cursor, &row.values[i].as_varchar.len, sizeof(uint16_t));
        std::memcpy(dst + cursor + 2, row.values[i].as_varchar.buf, 254);
        cursor += 256;
        break;
    }
  }

  page.row_count = static_cast<uint16_t>(page.row_count + 1);
  page.free_space_offset = static_cast<uint16_t>(page.free_space_offset + row_size_bytes);
  return true;
}

bool deserializeRowFromPage(const Page& page, const Schema& schema, uint16_t row_size_bytes,
                            uint16_t row_slot, Row& out_row) {
  const uint32_t offset = static_cast<uint32_t>(row_slot) * row_size_bytes;
  if (offset + row_size_bytes > page.free_space_offset) {
    return false;
  }

  const char* src = page.body.data() + offset;
  const uint8_t tombstone = static_cast<uint8_t>(src[0]);
  if (tombstone == kTombstoneDeleted) {
    return false;
  }

  out_row = Row(schema.columns.size());
  out_row.values.resize(schema.columns.size());
  std::memcpy(&out_row.expiration_timestamp, src + 8, sizeof(int64_t));

  uint16_t cursor = kRowHeaderAlignedBytes;
  for (size_t i = 0; i < schema.columns.size(); ++i) {
    switch (schema.columns[i].type) {
      case ColType::INT:
        std::memcpy(&out_row.values[i].as_int, src + cursor, sizeof(int64_t));
        cursor += 8;
        break;
      case ColType::DECIMAL:
        std::memcpy(&out_row.values[i].as_double, src + cursor, sizeof(double));
        cursor += 8;
        break;
      case ColType::DATETIME:
        std::memcpy(&out_row.values[i].as_datetime, src + cursor, sizeof(int64_t));
        cursor += 8;
        break;
      case ColType::VARCHAR:
        std::memcpy(&out_row.values[i].as_varchar.len, src + cursor, sizeof(uint16_t));
        std::memcpy(out_row.values[i].as_varchar.buf, src + cursor + 2, 254);
        cursor += 256;
        break;
    }
  }

  return true;
}

}  // namespace flexql
