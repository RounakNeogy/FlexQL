#include "flexql/core_types.hpp"

#include <filesystem>
#include <limits>
#include <utility>

#include "flexql/row_codec.hpp"
#include "flexql/storage_engine.hpp"

namespace flexql {

Table::Table(std::string table_name, size_t expected_columns)
    : name(std::move(table_name)),
      schema(expected_columns),
      current_page_id(0),
      rows_per_page(0),
      row_size_bytes(0),
      storage(std::make_unique<StorageEngine>()) {}

Table::~Table() = default;

uint16_t Table::compute_row_size_bytes() const {
  uint32_t total = align8(kRowHeaderRawBytes);
  for (const Column& c : schema.columns) {
    total += columnStorageSize(c.type);
  }
  total = align8(static_cast<uint16_t>(total));
  if (total > std::numeric_limits<uint16_t>::max()) {
    return 0;
  }
  return static_cast<uint16_t>(total);
}

bool Table::open_storage() {
  if (!storage) {
    storage = std::make_unique<StorageEngine>();
  }

  const std::string table_dir = "data/" + name;
  const std::string preferred_path = table_dir + "/segment_0.db";
  const std::string legacy_path = table_dir + "/segment_0.dat";
  std::string segment_path = preferred_path;
  if (!std::filesystem::exists(preferred_path) && std::filesystem::exists(legacy_path)) {
    std::error_code ec;
    std::filesystem::rename(legacy_path, preferred_path, ec);
    segment_path = ec ? legacy_path : preferred_path;
  }
  if (!storage->open(segment_path)) {
    return false;
  }

  row_size_bytes = compute_row_size_bytes();
  if (row_size_bytes == 0 || row_size_bytes > kPageBodyBytes) {
    return false;
  }

  rows_per_page = static_cast<uint16_t>(kPageBodyBytes / row_size_bytes);
  if (rows_per_page == 0) {
    return false;
  }

  const uint32_t pages = storage->pageCount();
  if (pages == 0) {
    current_page_id = storage->allocateNewPage(row_size_bytes);
    return current_page_id != UINT32_MAX;
  }

  current_page_id = pages - 1;
  Page last;
  if (!storage->readPage(current_page_id, last)) {
    return false;
  }
  if (last.row_size_bytes != row_size_bytes) {
    return false;
  }

  return true;
}

bool Table::appendRow(const Row& row) {
  if (!storage) {
    return false;
  }

  Page page;
  if (!storage->readPage(current_page_id, page)) {
    return false;
  }
  if (page.row_size_bytes != row_size_bytes) {
    return false;
  }

  if (static_cast<uint32_t>(page.free_space_offset) + row_size_bytes > kPageBodyBytes) {
    const uint32_t new_page_id = storage->allocateNewPage(row_size_bytes);
    if (new_page_id == UINT32_MAX) {
      return false;
    }
    current_page_id = new_page_id;
    if (!storage->readPage(current_page_id, page)) {
      return false;
    }
  }

  if (!serializeRowIntoPage(row, schema, row_size_bytes, page)) {
    return false;
  }
  return storage->writePage(current_page_id, page);
}

bool Table::readAllRows(std::vector<Row>& out_rows) const {
  out_rows.clear();
  if (!storage) {
    return false;
  }

  const uint32_t pages = storage->pageCount();
  for (uint32_t p = 0; p < pages; ++p) {
    Page page;
    if (!storage->readPage(p, page)) {
      return false;
    }
    if (page.row_size_bytes != row_size_bytes) {
      return false;
    }

    for (uint16_t slot = 0; slot < page.row_count; ++slot) {
      Row row;
      if (deserializeRowFromPage(page, schema, row_size_bytes, slot, row)) {
        out_rows.emplace_back(std::move(row));
      }
    }
  }

  return true;
}

}  // namespace flexql
