#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "flexql/core_types.hpp"

namespace flexql {

enum class QueryType : uint8_t {
  INSERT = 0,
  SELECT = 1,
  CREATE_TABLE = 2,
  DELETE = 3,
};

enum class TableSide : uint8_t {
  LEFT = 0,
  RIGHT = 1,
};

enum class CompareOp : uint8_t {
  EQ = 0,
  NE = 1,
  LT = 2,
  LE = 3,
  GT = 4,
  GE = 5,
};

struct Condition {
  TableSide table_side;
  int col_index;
  ColType col_type;
  std::string op;
  CompareOp op_code;
  RowValue value;
};

struct JoinCondition {
  int left_col_index;
  ColType left_col_type;
  int right_col_index;
  ColType right_col_type;
};

struct ProjectionRef {
  TableSide table_side;
  int col_index;
  size_t col_offset;
};

struct QueryAST {
  QueryType type;
  std::string table_name;
  std::string join_table_name;
  std::vector<int> projected_col_indices;
  std::vector<size_t> projected_col_offsets;
  std::vector<ProjectionRef> join_projected;
  std::optional<Condition> where;
  std::optional<JoinCondition> join;
  std::optional<int> order_by_col_index;
  bool order_by_desc;

  std::vector<Column> create_columns;
  bool create_if_not_exists;
  std::vector<RowValue> insert_values;
  std::vector<std::vector<RowValue>> insert_rows;
  int64_t insert_expiration_timestamp;
};

}  // namespace flexql
