#include "flexql/core_types.hpp"
#include "flexql/storage_engine.hpp"

#include <algorithm>
#include <cstring>

namespace flexql {

RowValue RowValue::from_int(int64_t value) {
  RowValue out;
  out.as_int = value;
  return out;
}

RowValue RowValue::from_decimal(double value) {
  RowValue out;
  out.as_double = value;
  return out;
}

RowValue RowValue::from_datetime(int64_t unix_ms) {
  RowValue out;
  out.as_datetime = unix_ms;
  return out;
}

RowValue RowValue::from_varchar(const char* data, uint16_t len) {
  RowValue out;
  const uint16_t bounded_len = static_cast<uint16_t>(std::min<uint16_t>(len, 254));
  out.as_varchar.len = bounded_len;
  if (bounded_len > 0) {
    std::memcpy(out.as_varchar.buf, data, bounded_len);
  }
  if (bounded_len < 254) {
    std::memset(out.as_varchar.buf + bounded_len, 0, static_cast<size_t>(254 - bounded_len));
  }
  return out;
}

Row::Row(size_t column_count) : expiration_timestamp(0) {
  if (column_count > 0) {
    values.reserve(column_count);
  }
}

void Row::reserve_columns(size_t column_count) {
  values.reserve(column_count);
}

Schema::Schema(size_t expected_columns) {
  reserve(expected_columns);
}

void Schema::reserve(size_t expected_columns) {
  columns.reserve(expected_columns);
  col_index.reserve(expected_columns);
}

bool Schema::add_column(std::string column_name, ColType column_type) {
  const auto [it, inserted] = col_index.emplace(column_name, static_cast<int>(columns.size()));
  if (!inserted) {
    return false;
  }
  columns.emplace_back(std::move(column_name), column_type);
  return true;
}

int Schema::find_column_index(const std::string& column_name) const {
  const auto it = col_index.find(column_name);
  if (it == col_index.end()) {
    return -1;
  }
  return it->second;
}

Table::Table() : current_page_id(0), rows_per_page(0), row_size_bytes(0) {}

}  // namespace flexql
