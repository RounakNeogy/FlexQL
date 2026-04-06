#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "flexql/bplus_tree.hpp"
#include "flexql/core_types.hpp"
#include "flexql/query_ast.hpp"

namespace flexql {

enum class ScanType : uint8_t {
  INDEX_SCAN = 0,
  FULL_SCAN = 1,
};

enum class JoinType : uint8_t {
  NONE = 0,
  INDEXED_NESTED_LOOP = 1,
  HASH_JOIN = 2,
};

struct QueryPlan {
  ScanType scan_type;
  std::vector<int> projected_col_indices;
  std::vector<size_t> projected_col_offsets;
  std::optional<int64_t> index_key;
  uint64_t estimated_cost;
};

struct JoinQueryPlan {
  JoinType join_type;
  bool outer_is_left;
  std::vector<ProjectionRef> projected_columns;
  uint64_t estimated_cost;
};

class QueryPlanner {
 public:
  QueryPlan plan(const QueryAST& ast, const Table& table) const;
  JoinQueryPlan planJoin(const QueryAST& ast, const Table& left, const Table& right) const;
};

}  // namespace flexql
