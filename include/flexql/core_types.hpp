#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace flexql {

class StorageEngine;
struct Page;

enum class ColType : uint8_t {
  INT = 0,
  DECIMAL = 1,
  VARCHAR = 2,
  DATETIME = 3,
};

struct __attribute__((aligned(8))) Column {
  std::string name;
  ColType type;

  Column() = default;
  Column(std::string column_name, ColType column_type)
      : name(std::move(column_name)), type(column_type) {}
};

union __attribute__((aligned(8))) RowValue {
  struct __attribute__((aligned(8))) VarcharValue {
    uint16_t len;
    char buf[254];
  };

  int64_t as_int;
  double as_double;
  int64_t as_datetime;
  VarcharValue as_varchar;

  RowValue() : as_int(0) {}

  static RowValue from_int(int64_t value);
  static RowValue from_decimal(double value);
  static RowValue from_datetime(int64_t unix_ms);
  static RowValue from_varchar(const char* data, uint16_t len);
};

struct __attribute__((aligned(8))) Row {
  std::vector<RowValue> values;
  int64_t expiration_timestamp;

  explicit Row(size_t column_count = 0);

  void reserve_columns(size_t column_count);
};

struct __attribute__((aligned(8))) Schema {
  std::vector<Column> columns;
  std::unordered_map<std::string, int> col_index;

  Schema() = default;
  explicit Schema(size_t expected_columns);

  void reserve(size_t expected_columns);
  bool add_column(std::string column_name, ColType column_type);
  int find_column_index(const std::string& column_name) const;
};

struct __attribute__((aligned(8))) Table {
  std::string name;
  Schema schema;
  mutable std::shared_mutex rw_lock;

  uint32_t current_page_id;
  uint16_t rows_per_page;
  uint16_t row_size_bytes;

  std::unique_ptr<StorageEngine> storage;

  Table();
  explicit Table(std::string table_name, size_t expected_columns = 0);
  ~Table();

  bool open_storage();
  bool appendRow(const Row& row);
  bool readAllRows(std::vector<Row>& out_rows) const;

  uint16_t compute_row_size_bytes() const;
};

static_assert(sizeof(RowValue) == 256, "RowValue must be exactly 256 bytes.");
static_assert(alignof(RowValue) == 8, "RowValue must be 8-byte aligned.");
static_assert(alignof(Column) == 8, "Column must be 8-byte aligned.");
static_assert(alignof(Row) == 8, "Row must be 8-byte aligned.");
static_assert(alignof(Schema) == 8, "Schema must be 8-byte aligned.");
static_assert(alignof(Table) == 8, "Table must be 8-byte aligned.");

}  // namespace flexql
