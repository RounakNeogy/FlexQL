#pragma once

#include <string>
#include <unordered_map>

#include "flexql/core_types.hpp"
#include "flexql/query_ast.hpp"

namespace flexql {

class QueryParser {
 public:
  bool parse(const std::string& sql,
             const std::unordered_map<std::string, const Table*>& tables,
             QueryAST& out_ast,
             std::string& error) const;
};

}  // namespace flexql
