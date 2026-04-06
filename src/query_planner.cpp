#include "flexql/query_planner.hpp"

#include "flexql/storage_engine.hpp"

namespace flexql {

QueryPlan QueryPlanner::plan(const QueryAST& ast, const Table& table) const {
  QueryPlan out;
  out.projected_col_indices = ast.projected_col_indices;
  out.projected_col_offsets = ast.projected_col_offsets;
  out.index_key.reset();

  if (ast.where.has_value() && ast.where->col_index == 0 && ast.where->op_code == CompareOp::EQ) {
    out.scan_type = ScanType::INDEX_SCAN;
    out.estimated_cost = 1;
    out.index_key = static_cast<int64_t>(encodePrimaryKey(table.schema.columns[0].type, ast.where->value));
  } else {
    out.scan_type = ScanType::FULL_SCAN;
    out.estimated_cost = static_cast<uint64_t>(table.storage->pageCount());
  }

  return out;
}

JoinQueryPlan QueryPlanner::planJoin(const QueryAST& ast, const Table& left, const Table& right) const {
  JoinQueryPlan out{};
  out.projected_columns = ast.join_projected;
  out.outer_is_left = true;
  out.join_type = JoinType::HASH_JOIN;
  out.estimated_cost =
      static_cast<uint64_t>(left.storage->pageCount()) + static_cast<uint64_t>(right.storage->pageCount());

  if (!ast.join.has_value()) {
    out.join_type = JoinType::NONE;
    return out;
  }

  const uint64_t left_rows =
      static_cast<uint64_t>(left.storage->pageCount()) * static_cast<uint64_t>(left.rows_per_page);
  const uint64_t right_rows =
      static_cast<uint64_t>(right.storage->pageCount()) * static_cast<uint64_t>(right.rows_per_page);
  out.outer_is_left = (left_rows <= right_rows);

  const bool left_join_is_pk = (ast.join->left_col_index == 0);
  const bool right_join_is_pk = (ast.join->right_col_index == 0);
  if (left_join_is_pk || right_join_is_pk) {
    out.join_type = JoinType::INDEXED_NESTED_LOOP;
    if (right_join_is_pk && !left_join_is_pk) {
      out.outer_is_left = true;
    } else if (left_join_is_pk && !right_join_is_pk) {
      out.outer_is_left = false;
    }
    out.estimated_cost = out.outer_is_left ? left_rows : right_rows;
  } else {
    out.join_type = JoinType::HASH_JOIN;
    out.estimated_cost = left_rows + right_rows;
  }
  return out;
}

}  // namespace flexql
