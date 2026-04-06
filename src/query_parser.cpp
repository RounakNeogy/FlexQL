#include "flexql/query_parser.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "flexql/row_codec.hpp"

namespace flexql {

namespace {

std::string trim(const std::string& s) {
  size_t b = 0;
  while (b < s.size() && std::isspace(static_cast<unsigned char>(s[b])) != 0) {
    ++b;
  }

  size_t e = s.size();
  while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1])) != 0) {
    --e;
  }
  return s.substr(b, e - b);
}

std::string to_upper(std::string s) {
  for (char& c : s) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return s;
}

bool starts_with_ci(const std::string& s, const std::string& prefix) {
  if (s.size() < prefix.size()) {
    return false;
  }
  return to_upper(s.substr(0, prefix.size())) == to_upper(prefix);
}

size_t find_ci(const std::string& s, const std::string& needle, size_t start = 0) {
  if (needle.empty() || start >= s.size()) {
    return std::string::npos;
  }
  const std::string su = to_upper(s);
  const std::string nu = to_upper(needle);
  return su.find(nu, start);
}

bool is_ident_char(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) != 0 || c == '_';
}

size_t find_keyword_ci(const std::string& s, const std::string& keyword, size_t start = 0) {
  const std::string su = to_upper(s);
  const std::string ku = to_upper(keyword);
  size_t pos = su.find(ku, start);
  while (pos != std::string::npos) {
    const bool left_ok = (pos == 0) || !is_ident_char(s[pos - 1]);
    const size_t end = pos + ku.size();
    const bool right_ok = (end >= s.size()) || !is_ident_char(s[end]);
    if (left_ok && right_ok) {
      return pos;
    }
    pos = su.find(ku, pos + 1);
  }
  return std::string::npos;
}

std::vector<std::string> split_csv(const std::string& src) {
  std::vector<std::string> out;
  std::string cur;
  bool in_quote = false;
  char quote_char = '\0';

  for (char c : src) {
    if ((c == '\'' || c == '"') && (!in_quote || c == quote_char)) {
      if (in_quote && c == quote_char) {
        in_quote = false;
      } else {
        in_quote = true;
        quote_char = c;
      }
      cur.push_back(c);
      continue;
    }

    if (!in_quote && c == ',') {
      out.push_back(trim(cur));
      cur.clear();
      continue;
    }

    cur.push_back(c);
  }

  if (!cur.empty()) {
    out.push_back(trim(cur));
  }
  return out;
}

bool parse_type(const std::string& tok, ColType& out_type) {
  const std::string u = to_upper(trim(tok));
  if (u == "INT") {
    out_type = ColType::INT;
    return true;
  }
  if (u == "DECIMAL") {
    out_type = ColType::DECIMAL;
    return true;
  }
  if (u == "TEXT" || u.rfind("VARCHAR", 0) == 0) {
    out_type = ColType::VARCHAR;
    return true;
  }
  if (u == "DATETIME") {
    out_type = ColType::DATETIME;
    return true;
  }
  return false;
}

bool parse_compare_op(const std::string& op, CompareOp& code) {
  if (op == "=") {
    code = CompareOp::EQ;
    return true;
  }
  if (op == "!=") {
    code = CompareOp::NE;
    return true;
  }
  if (op == "<") {
    code = CompareOp::LT;
    return true;
  }
  if (op == "<=") {
    code = CompareOp::LE;
    return true;
  }
  if (op == ">") {
    code = CompareOp::GT;
    return true;
  }
  if (op == ">=") {
    code = CompareOp::GE;
    return true;
  }
  return false;
}

bool parse_literal_to_rowvalue(const std::string& raw, ColType type, RowValue& out_value,
                               std::string& error) {
  const std::string lit = trim(raw);
  try {
    switch (type) {
      case ColType::INT:
        out_value = RowValue::from_int(std::stoll(lit));
        return true;
      case ColType::DECIMAL:
        out_value = RowValue::from_decimal(std::stod(lit));
        return true;
      case ColType::DATETIME:
        out_value = RowValue::from_datetime(std::stoll(lit));
        return true;
      case ColType::VARCHAR: {
        std::string s = lit;
        if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') ||
                              (s.front() == '"' && s.back() == '"'))) {
          s = s.substr(1, s.size() - 2);
        }
        out_value = RowValue::from_varchar(s.data(), static_cast<uint16_t>(s.size()));
        return true;
      }
    }
  } catch (const std::exception&) {
    error = "Failed to parse literal: " + lit;
    return false;
  }

  error = "Unsupported literal type";
  return false;
}

bool parse_insert_tuple_fast(const std::string& tuple,
                             const Schema& schema,
                             std::vector<RowValue>& out_values,
                             std::string& error) {
  out_values.clear();
  out_values.reserve(schema.columns.size());

  size_t pos = 0;
  for (size_t col = 0; col < schema.columns.size(); ++col) {
    while (pos < tuple.size() && std::isspace(static_cast<unsigned char>(tuple[pos])) != 0) {
      ++pos;
    }
    if (pos >= tuple.size()) {
      error = "INSERT value count mismatch";
      return false;
    }

    const ColType t = schema.columns[col].type;
    if (t == ColType::VARCHAR) {
      if (tuple[pos] != '\'' && tuple[pos] != '"') {
        error = "VARCHAR literal must be quoted";
        return false;
      }
      const char quote = tuple[pos++];
      const size_t start = pos;
      while (pos < tuple.size() && tuple[pos] != quote) {
        ++pos;
      }
      if (pos >= tuple.size()) {
        error = "Unterminated string literal";
        return false;
      }
      const size_t len = pos - start;
      out_values.emplace_back(RowValue::from_varchar(tuple.data() + start, static_cast<uint16_t>(len)));
      ++pos;
    } else {
      const size_t start = pos;
      while (pos < tuple.size() && tuple[pos] != ',') {
        ++pos;
      }
      size_t end = pos;
      while (end > start && std::isspace(static_cast<unsigned char>(tuple[end - 1])) != 0) {
        --end;
      }
      if (start == end) {
        error = "Empty numeric literal in INSERT";
        return false;
      }
      const std::string lit = tuple.substr(start, end - start);
      try {
        if (t == ColType::INT) {
          out_values.emplace_back(RowValue::from_int(std::stoll(lit)));
        } else if (t == ColType::DECIMAL) {
          out_values.emplace_back(RowValue::from_decimal(std::stod(lit)));
        } else {
          out_values.emplace_back(RowValue::from_datetime(std::stoll(lit)));
        }
      } catch (const std::exception&) {
        error = "Failed to parse literal: " + lit;
        return false;
      }
    }

    while (pos < tuple.size() && std::isspace(static_cast<unsigned char>(tuple[pos])) != 0) {
      ++pos;
    }
    if (col + 1 < schema.columns.size()) {
      if (pos >= tuple.size() || tuple[pos] != ',') {
        error = "INSERT value count mismatch";
        return false;
      }
      ++pos;
    }
  }

  while (pos < tuple.size() && std::isspace(static_cast<unsigned char>(tuple[pos])) != 0) {
    ++pos;
  }
  if (pos != tuple.size()) {
    error = "Malformed INSERT tuple";
    return false;
  }
  return true;
}

std::vector<size_t> compute_project_offsets(const Table& table, const std::vector<int>& col_indices) {
  std::vector<size_t> offsets;
  offsets.reserve(col_indices.size());

  std::vector<size_t> all_col_offsets(table.schema.columns.size(), 0);
  size_t cursor = kRowHeaderAlignedBytes;
  for (size_t i = 0; i < table.schema.columns.size(); ++i) {
    all_col_offsets[i] = cursor;
    cursor += columnStorageSize(table.schema.columns[i].type);
  }

  for (int col_idx : col_indices) {
    offsets.push_back(all_col_offsets[static_cast<size_t>(col_idx)]);
  }
  return offsets;
}

bool parse_where_clause(const std::string& where_part, const Table& table, QueryAST& out_ast,
                        std::string& error) {
  static const char* ops[] = {"<=", ">=", "!=", "=", "<", ">"};

  size_t op_pos = std::string::npos;
  std::string op;
  for (const char* candidate : ops) {
    const size_t p = where_part.find(candidate);
    if (p != std::string::npos) {
      op_pos = p;
      op = candidate;
      break;
    }
  }

  if (op_pos == std::string::npos) {
    error = "WHERE clause requires an operator";
    return false;
  }

  const std::string col_name = trim(where_part.substr(0, op_pos));
  const std::string lit = trim(where_part.substr(op_pos + op.size()));

  const int col_idx = table.schema.find_column_index(col_name);
  if (col_idx < 0) {
    error = "Unknown column in WHERE: " + col_name;
    return false;
  }

  Condition cond;
  cond.table_side = TableSide::LEFT;
  cond.col_index = col_idx;
  cond.col_type = table.schema.columns[static_cast<size_t>(col_idx)].type;
  cond.op = op;
  if (!parse_compare_op(op, cond.op_code)) {
    error = "Unsupported WHERE operator: " + op;
    return false;
  }

  if (!parse_literal_to_rowvalue(lit, cond.col_type, cond.value, error)) {
    return false;
  }
  out_ast.where = cond;
  return true;
}

bool resolve_join_col(const std::string& token,
                      const Table& left,
                      const Table& right,
                      int& out_left_idx,
                      int& out_right_idx,
                      bool& is_left,
                      std::string& error) {
  const std::string t = trim(token);
  const size_t dot = t.find('.');
  if (dot == std::string::npos) {
    const int lidx = left.schema.find_column_index(t);
    const int ridx = right.schema.find_column_index(t);
    if (lidx >= 0 && ridx >= 0) {
      error = "Ambiguous join column: " + t;
      return false;
    }
    if (lidx >= 0) {
      is_left = true;
      out_left_idx = lidx;
      out_right_idx = -1;
      return true;
    }
    if (ridx >= 0) {
      is_left = false;
      out_left_idx = -1;
      out_right_idx = ridx;
      return true;
    }
    error = "Unknown join column: " + t;
    return false;
  }

  const std::string table_name = trim(t.substr(0, dot));
  const std::string col_name = trim(t.substr(dot + 1));
  if (table_name == left.name) {
    const int idx = left.schema.find_column_index(col_name);
    if (idx < 0) {
      error = "Unknown join column: " + token;
      return false;
    }
    is_left = true;
    out_left_idx = idx;
    out_right_idx = -1;
    return true;
  }
  if (table_name == right.name) {
    const int idx = right.schema.find_column_index(col_name);
    if (idx < 0) {
      error = "Unknown join column: " + token;
      return false;
    }
    is_left = false;
    out_left_idx = -1;
    out_right_idx = idx;
    return true;
  }
  error = "Unknown table qualifier in join: " + table_name;
  return false;
}

bool parse_join_where_clause(const std::string& where_part,
                             const Table& left,
                             const Table& right,
                             QueryAST& out_ast,
                             std::string& error) {
  static const char* ops[] = {"<=", ">=", "!=", "=", "<", ">"};
  size_t op_pos = std::string::npos;
  std::string op;
  for (const char* candidate : ops) {
    const size_t p = where_part.find(candidate);
    if (p != std::string::npos) {
      op_pos = p;
      op = candidate;
      break;
    }
  }
  if (op_pos == std::string::npos) {
    error = "WHERE clause requires an operator";
    return false;
  }

  const std::string lhs = trim(where_part.substr(0, op_pos));
  const std::string rhs = trim(where_part.substr(op_pos + op.size()));
  const size_t dot = lhs.find('.');

  Condition cond;
  cond.table_side = TableSide::LEFT;
  if (dot == std::string::npos) {
    const int lidx = left.schema.find_column_index(lhs);
    const int ridx = right.schema.find_column_index(lhs);
    if (lidx >= 0 && ridx >= 0) {
      error = "Ambiguous WHERE column in JOIN: " + lhs;
      return false;
    }
    if (lidx >= 0) {
      cond.table_side = TableSide::LEFT;
      cond.col_index = lidx;
      cond.col_type = left.schema.columns[static_cast<size_t>(lidx)].type;
    } else if (ridx >= 0) {
      cond.table_side = TableSide::RIGHT;
      cond.col_index = ridx;
      cond.col_type = right.schema.columns[static_cast<size_t>(ridx)].type;
    } else {
      error = "Unknown WHERE column in JOIN: " + lhs;
      return false;
    }
  } else {
    const std::string table_name = trim(lhs.substr(0, dot));
    const std::string col_name = trim(lhs.substr(dot + 1));
    if (table_name == left.name) {
      cond.table_side = TableSide::LEFT;
      cond.col_index = left.schema.find_column_index(col_name);
      if (cond.col_index < 0) {
        error = "Unknown WHERE column in JOIN: " + lhs;
        return false;
      }
      cond.col_type = left.schema.columns[static_cast<size_t>(cond.col_index)].type;
    } else if (table_name == right.name) {
      cond.table_side = TableSide::RIGHT;
      cond.col_index = right.schema.find_column_index(col_name);
      if (cond.col_index < 0) {
        error = "Unknown WHERE column in JOIN: " + lhs;
        return false;
      }
      cond.col_type = right.schema.columns[static_cast<size_t>(cond.col_index)].type;
    } else {
      error = "Unknown table qualifier in JOIN WHERE: " + table_name;
      return false;
    }
  }

  cond.op = op;
  if (!parse_compare_op(op, cond.op_code)) {
    error = "Unsupported WHERE operator: " + op;
    return false;
  }
  if (!parse_literal_to_rowvalue(rhs, cond.col_type, cond.value, error)) {
    return false;
  }
  out_ast.where = cond;
  return true;
}

bool parse_create(const std::string& sql, QueryAST& out_ast, std::string& error) {
  const std::string upper_sql = to_upper(sql);
  const size_t table_kw = upper_sql.find("CREATE TABLE");
  if (table_kw != 0) {
    error = "CREATE TABLE must start at query beginning";
    return false;
  }

  const size_t open = sql.find('(');
  const size_t close = sql.rfind(')');
  if (open == std::string::npos || close == std::string::npos || close <= open) {
    error = "CREATE TABLE requires column list in parentheses";
    return false;
  }

  out_ast.type = QueryType::CREATE_TABLE;
  out_ast.join.reset();
  out_ast.join_table_name.clear();
  out_ast.where.reset();
  out_ast.projected_col_indices.clear();
  out_ast.projected_col_offsets.clear();
  out_ast.join_projected.clear();
  out_ast.insert_expiration_timestamp = 0;
  out_ast.order_by_col_index.reset();
  out_ast.order_by_desc = false;
  out_ast.create_if_not_exists = false;
  out_ast.insert_rows.clear();

  const std::string create_tail = trim(sql.substr(std::strlen("CREATE TABLE")));
  const std::string create_tail_upper = to_upper(create_tail);
  std::string name_part = create_tail;
  if (create_tail_upper.rfind("IF NOT EXISTS", 0) == 0) {
    out_ast.create_if_not_exists = true;
    name_part = trim(create_tail.substr(std::strlen("IF NOT EXISTS")));
  }
  const size_t local_open = name_part.find('(');
  if (local_open == std::string::npos) {
    error = "CREATE TABLE requires column list";
    return false;
  }
  out_ast.table_name = trim(name_part.substr(0, local_open));
  if (out_ast.table_name.empty()) {
    error = "Missing table name";
    return false;
  }

  const auto columns = split_csv(sql.substr(open + 1, close - open - 1));
  if (columns.empty()) {
    error = "CREATE TABLE requires at least one column";
    return false;
  }

  out_ast.create_columns.clear();
  out_ast.create_columns.reserve(columns.size());
  for (const std::string& col_def : columns) {
    std::istringstream iss(col_def);
    std::string col_name;
    std::string col_type;
    iss >> col_name >> col_type;
    if (col_name.empty() || col_type.empty()) {
      error = "Invalid column definition: " + col_def;
      return false;
    }

    ColType type;
    if (!parse_type(col_type, type)) {
      error = "Unsupported column type: " + col_type;
      return false;
    }
    out_ast.create_columns.emplace_back(col_name, type);
  }

  return true;
}

bool parse_insert(const std::string& sql,
                  const std::unordered_map<std::string, const Table*>& tables,
                  QueryAST& out_ast,
                  std::string& error) {
  const std::string u = to_upper(sql);
  const size_t into_pos = u.find("INSERT INTO");
  const size_t values_pos = u.find("VALUES");
  if (into_pos != 0 || values_pos == std::string::npos) {
    error = "INSERT syntax error";
    return false;
  }

  out_ast.type = QueryType::INSERT;
  out_ast.join.reset();
  out_ast.join_table_name.clear();
  out_ast.where.reset();
  out_ast.projected_col_indices.clear();
  out_ast.projected_col_offsets.clear();
  out_ast.join_projected.clear();
  out_ast.insert_expiration_timestamp = 0;
  out_ast.order_by_col_index.reset();
  out_ast.order_by_desc = false;
  out_ast.create_if_not_exists = false;
  out_ast.insert_rows.clear();
  out_ast.table_name = trim(sql.substr(std::strlen("INSERT INTO"), values_pos - std::strlen("INSERT INTO")));
  if (out_ast.table_name.empty()) {
    error = "INSERT missing table name";
    return false;
  }

  const auto table_it = tables.find(out_ast.table_name);
  if (table_it == tables.end()) {
    error = "Unknown table in INSERT: " + out_ast.table_name;
    return false;
  }
  const Table& table = *(table_it->second);

  const size_t open = sql.find('(', values_pos);
  const size_t close = sql.rfind(')');
  if (open == std::string::npos || close == std::string::npos || close <= open) {
    error = "INSERT VALUES requires parentheses";
    return false;
  }
  out_ast.insert_values.clear();
  out_ast.insert_rows.clear();

  std::vector<std::string> tuples;
  tuples.reserve(8);
  int depth = 0;
  size_t tuple_start = std::string::npos;
  for (size_t i = open; i < sql.size(); ++i) {
    const char c = sql[i];
    if (c == '(') {
      if (depth == 0) {
        tuple_start = i + 1;
      }
      depth += 1;
    } else if (c == ')') {
      depth -= 1;
      if (depth < 0) {
        error = "INSERT VALUES has mismatched parentheses";
        return false;
      }
      if (depth == 0) {
        if (tuple_start == std::string::npos || i < tuple_start) {
          error = "INSERT VALUES malformed tuple";
          return false;
        }
        tuples.push_back(sql.substr(tuple_start, i - tuple_start));
      }
    }
  }
  if (depth != 0 || tuples.empty()) {
    error = "INSERT VALUES malformed";
    return false;
  }

  out_ast.insert_rows.reserve(tuples.size());
  std::vector<RowValue> parsed_values;
  for (const std::string& tuple : tuples) {
    if (!parse_insert_tuple_fast(tuple, table.schema, parsed_values, error)) {
      // Fallback to legacy parser for edge cases (keeps compatibility).
      const auto values = split_csv(tuple);
      if (values.size() != table.schema.columns.size()) {
        error = "INSERT value count mismatch";
        return false;
      }
      parsed_values.clear();
      parsed_values.reserve(values.size());
      for (size_t i = 0; i < values.size(); ++i) {
        RowValue val;
        if (!parse_literal_to_rowvalue(values[i], table.schema.columns[i].type, val, error)) {
          return false;
        }
        parsed_values.emplace_back(val);
      }
    }
    out_ast.insert_rows.emplace_back(parsed_values);
  }
  if (!out_ast.insert_rows.empty()) {
    out_ast.insert_values = out_ast.insert_rows.front();
  }

  const std::string tail = trim(sql.substr(close + 1));
  if (!tail.empty()) {
    std::string tu = to_upper(tail);
    if (tu.rfind("EXPIRY", 0) != 0 && tu.rfind("EXPIRATION", 0) != 0) {
      error = "Unsupported INSERT suffix. Use EXPIRY <unix_ms>.";
      return false;
    }
    size_t pos = tail.find_first_of("= ");
    if (pos == std::string::npos) {
      error = "Missing expiry value";
      return false;
    }
    while (pos < tail.size() && (tail[pos] == '=' || std::isspace(static_cast<unsigned char>(tail[pos])) != 0)) {
      ++pos;
    }
    if (pos >= tail.size()) {
      error = "Missing expiry value";
      return false;
    }
    try {
      out_ast.insert_expiration_timestamp = std::stoll(trim(tail.substr(pos)));
    } catch (const std::exception&) {
      error = "Invalid expiry timestamp";
      return false;
    }
  }
  return true;
}

bool parse_select(const std::string& sql,
                  const std::unordered_map<std::string, const Table*>& tables,
                  QueryAST& out_ast,
                  std::string& error) {
  const std::string u = to_upper(sql);
  const size_t select_pos = u.find("SELECT");
  const size_t from_pos = u.find("FROM");
  if (select_pos != 0 || from_pos == std::string::npos) {
    error = "SELECT syntax error";
    return false;
  }

  out_ast.type = QueryType::SELECT;
  out_ast.projected_col_indices.clear();
  out_ast.projected_col_offsets.clear();
  out_ast.join_projected.clear();
  out_ast.where.reset();
  out_ast.join.reset();
  out_ast.join_table_name.clear();
  out_ast.insert_expiration_timestamp = 0;
  out_ast.order_by_col_index.reset();
  out_ast.order_by_desc = false;
  out_ast.create_if_not_exists = false;
  out_ast.insert_rows.clear();

  const std::string proj_part = trim(sql.substr(std::strlen("SELECT"), from_pos - std::strlen("SELECT")));
  const size_t where_pos = find_keyword_ci(sql, "WHERE", from_pos);
  const size_t join_pos = find_keyword_ci(sql, "JOIN", from_pos);
  const size_t order_by_pos = find_keyword_ci(sql, "ORDER BY", from_pos);

  if (join_pos == std::string::npos || (where_pos != std::string::npos && join_pos > where_pos)) {
    size_t table_end = sql.size();
    if (where_pos != std::string::npos) {
      table_end = where_pos;
    } else if (order_by_pos != std::string::npos) {
      table_end = order_by_pos;
    }
    const std::string table_part =
        trim(sql.substr(from_pos + std::strlen("FROM"), table_end - (from_pos + std::strlen("FROM"))));

    out_ast.table_name = table_part;
    if (out_ast.table_name.empty()) {
      error = "SELECT missing table name";
      return false;
    }

    const auto table_it = tables.find(out_ast.table_name);
    if (table_it == tables.end()) {
      error = "Unknown table in SELECT: " + out_ast.table_name;
      return false;
    }
    const Table& table = *(table_it->second);

    if (proj_part == "*") {
      out_ast.projected_col_indices.reserve(table.schema.columns.size());
      for (size_t i = 0; i < table.schema.columns.size(); ++i) {
        out_ast.projected_col_indices.push_back(static_cast<int>(i));
      }
    } else {
      const auto cols = split_csv(proj_part);
      out_ast.projected_col_indices.reserve(cols.size());
      for (const std::string& col_name : cols) {
        const int idx = table.schema.find_column_index(col_name);
        if (idx < 0) {
          error = "Unknown projected column: " + col_name;
          return false;
        }
        out_ast.projected_col_indices.push_back(idx);
      }
    }

    out_ast.projected_col_offsets = compute_project_offsets(table, out_ast.projected_col_indices);
    if (where_pos != std::string::npos) {
      const size_t where_end = (order_by_pos != std::string::npos && order_by_pos > where_pos)
                                   ? order_by_pos
                                   : sql.size();
      const std::string where_part =
          trim(sql.substr(where_pos + std::strlen("WHERE"), where_end - (where_pos + std::strlen("WHERE"))));
      if (!parse_where_clause(where_part, table, out_ast, error)) {
        return false;
      }
    }
    if (order_by_pos != std::string::npos) {
      std::string order_part = trim(sql.substr(order_by_pos + std::strlen("ORDER BY")));
      if (order_part.empty()) {
        error = "ORDER BY missing column";
        return false;
      }
      bool desc = false;
      const std::string up = to_upper(order_part);
      if (up.size() >= 5 && up.substr(up.size() - 5) == " DESC") {
        desc = true;
        order_part = trim(order_part.substr(0, order_part.size() - 5));
      } else if (up.size() >= 4 && up.substr(up.size() - 4) == " ASC") {
        order_part = trim(order_part.substr(0, order_part.size() - 4));
      }
      const int idx = table.schema.find_column_index(order_part);
      if (idx < 0) {
        error = "Unknown ORDER BY column: " + order_part;
        return false;
      }
      out_ast.order_by_col_index = idx;
      out_ast.order_by_desc = desc;
    }
    return true;
  }

  std::string left_name = trim(sql.substr(from_pos + std::strlen("FROM"),
                                          join_pos - (from_pos + std::strlen("FROM"))));
  const std::string left_upper = to_upper(left_name);
  if (left_upper.size() >= 5 && left_upper.substr(left_upper.size() - 5) == "INNER") {
    left_name = trim(left_name.substr(0, left_name.size() - 5));
  }
  const size_t on_pos = find_keyword_ci(sql, "ON", join_pos);
  if (on_pos == std::string::npos) {
    error = "JOIN query missing ON clause";
    return false;
  }

  const std::string right_name =
      trim(sql.substr(join_pos + std::strlen("JOIN"), on_pos - (join_pos + std::strlen("JOIN"))));
  if (left_name.empty() || right_name.empty()) {
    error = "JOIN query missing table name";
    return false;
  }

  out_ast.table_name = left_name;
  out_ast.join_table_name = right_name;
  const auto left_it = tables.find(left_name);
  const auto right_it = tables.find(right_name);
  if (left_it == tables.end() || right_it == tables.end()) {
    error = "Unknown table in JOIN query";
    return false;
  }
  const Table& left = *(left_it->second);
  const Table& right = *(right_it->second);

  const std::string on_part =
      where_pos == std::string::npos
          ? trim(sql.substr(on_pos + std::strlen("ON")))
          : trim(sql.substr(on_pos + std::strlen("ON"), where_pos - (on_pos + std::strlen("ON"))));
  const size_t eq = on_part.find('=');
  if (eq == std::string::npos) {
    error = "JOIN ON supports '=' only";
    return false;
  }

  const std::string lhs = trim(on_part.substr(0, eq));
  const std::string rhs = trim(on_part.substr(eq + 1));

  int ll = -1;
  int lr = -1;
  bool lhs_left = true;
  if (!resolve_join_col(lhs, left, right, ll, lr, lhs_left, error)) {
    return false;
  }
  int rl = -1;
  int rr = -1;
  bool rhs_left = true;
  if (!resolve_join_col(rhs, left, right, rl, rr, rhs_left, error)) {
    return false;
  }
  if (lhs_left == rhs_left) {
    error = "JOIN ON must compare one column from each table";
    return false;
  }

  JoinCondition jc{};
  if (lhs_left) {
    jc.left_col_index = ll;
    jc.left_col_type = left.schema.columns[static_cast<size_t>(ll)].type;
    jc.right_col_index = rr;
    jc.right_col_type = right.schema.columns[static_cast<size_t>(rr)].type;
  } else {
    jc.left_col_index = rl;
    jc.left_col_type = left.schema.columns[static_cast<size_t>(rl)].type;
    jc.right_col_index = lr;
    jc.right_col_type = right.schema.columns[static_cast<size_t>(lr)].type;
  }
  out_ast.join = jc;

  std::vector<size_t> left_offsets(left.schema.columns.size(), 0);
  std::vector<size_t> right_offsets(right.schema.columns.size(), 0);
  size_t cursor = kRowHeaderAlignedBytes;
  for (size_t i = 0; i < left.schema.columns.size(); ++i) {
    left_offsets[i] = cursor;
    cursor += columnStorageSize(left.schema.columns[i].type);
  }
  cursor = kRowHeaderAlignedBytes;
  for (size_t i = 0; i < right.schema.columns.size(); ++i) {
    right_offsets[i] = cursor;
    cursor += columnStorageSize(right.schema.columns[i].type);
  }

  if (proj_part == "*") {
    out_ast.join_projected.reserve(left.schema.columns.size() + right.schema.columns.size());
    for (size_t i = 0; i < left.schema.columns.size(); ++i) {
      out_ast.join_projected.push_back(
          ProjectionRef{TableSide::LEFT, static_cast<int>(i), left_offsets[i]});
    }
    for (size_t i = 0; i < right.schema.columns.size(); ++i) {
      out_ast.join_projected.push_back(
          ProjectionRef{TableSide::RIGHT, static_cast<int>(i), right_offsets[i]});
    }
  } else {
    const auto cols = split_csv(proj_part);
    out_ast.join_projected.reserve(cols.size());
    for (const std::string& c : cols) {
      int li = -1;
      int ri = -1;
      bool is_left = true;
      if (!resolve_join_col(c, left, right, li, ri, is_left, error)) {
        return false;
      }
      if (is_left) {
        out_ast.join_projected.push_back(ProjectionRef{TableSide::LEFT, li, left_offsets[static_cast<size_t>(li)]});
      } else {
        out_ast.join_projected.push_back(
            ProjectionRef{TableSide::RIGHT, ri, right_offsets[static_cast<size_t>(ri)]});
      }
    }
  }

  if (where_pos != std::string::npos) {
    const std::string where_part = trim(sql.substr(where_pos + std::strlen("WHERE")));
    if (!parse_join_where_clause(where_part, left, right, out_ast, error)) {
      return false;
    }
  }
  return true;
}

bool parse_delete(const std::string& sql,
                  const std::unordered_map<std::string, const Table*>& tables,
                  QueryAST& out_ast,
                  std::string& error) {
  const std::string u = to_upper(sql);
  const size_t from_pos = u.find("DELETE FROM");
  if (from_pos != 0) {
    error = "DELETE syntax error";
    return false;
  }

  out_ast.type = QueryType::DELETE;
  out_ast.join.reset();
  out_ast.join_table_name.clear();
  out_ast.where.reset();
  out_ast.projected_col_indices.clear();
  out_ast.projected_col_offsets.clear();
  out_ast.join_projected.clear();
  out_ast.insert_values.clear();
  out_ast.insert_expiration_timestamp = 0;
  out_ast.order_by_col_index.reset();
  out_ast.order_by_desc = false;
  out_ast.create_if_not_exists = false;
  out_ast.insert_rows.clear();

  out_ast.table_name = trim(sql.substr(std::strlen("DELETE FROM")));
  if (out_ast.table_name.empty()) {
    error = "DELETE missing table name";
    return false;
  }
  if (tables.find(out_ast.table_name) == tables.end()) {
    error = "Unknown table in DELETE: " + out_ast.table_name;
    return false;
  }
  return true;
}

}  // namespace

bool QueryParser::parse(const std::string& sql,
                        const std::unordered_map<std::string, const Table*>& tables,
                        QueryAST& out_ast,
                        std::string& error) const {
  error.clear();
  const std::string input = trim(sql);
  if (input.empty()) {
    error = "Empty query";
    return false;
  }

  std::string normalized = input;
  if (!normalized.empty() && normalized.back() == ';') {
    normalized.pop_back();
  }

  if (starts_with_ci(normalized, "CREATE TABLE")) {
    return parse_create(normalized, out_ast, error);
  }
  if (starts_with_ci(normalized, "INSERT INTO")) {
    return parse_insert(normalized, tables, out_ast, error);
  }
  if (starts_with_ci(normalized, "SELECT")) {
    return parse_select(normalized, tables, out_ast, error);
  }
  if (starts_with_ci(normalized, "DELETE FROM")) {
    return parse_delete(normalized, tables, out_ast, error);
  }

  error = "Unsupported query type";
  return false;
}

}  // namespace flexql
