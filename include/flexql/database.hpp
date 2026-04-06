#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "flexql/core_types.hpp"

namespace flexql {

class Database {
 public:
  bool createTable(std::string table_name, const Schema& schema);
  Table* getTable(const std::string& table_name);

  mutable std::shared_mutex table_map_mutex;

 private:
  std::unordered_map<std::string, std::unique_ptr<Table>> tables_;
};

}  // namespace flexql

