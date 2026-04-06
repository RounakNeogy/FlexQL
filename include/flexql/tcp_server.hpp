#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "flexql/buffer_pool.hpp"
#include "flexql/database.hpp"
#include "flexql/execution_engine.hpp"
#include "flexql/query_parser.hpp"
#include "flexql/thread_pool.hpp"

namespace flexql {

class FlexQLServer {
 public:
  explicit FlexQLServer(int port, size_t worker_threads = 0);
  ~FlexQLServer();

  bool start();
  void stop();
  bool isRunning() const;

 private:
  struct QueryResultContext {
    int client_fd;
    std::vector<ColType> projected_types;
    std::vector<std::string> projected_names;
    std::vector<uint8_t> row_payload_buffer;
    std::vector<uint8_t> pending_wire_buffer;
    std::string error;
    bool aborted;
    uint64_t rows_sent;
  };

  int port_;
  int listen_fd_;
  std::atomic<bool> running_;
  std::thread accept_thread_;
  ThreadPool workers_;

  Database db_;
  BufferPool buffer_pool_;
  ExecutionEngine engine_;
  QueryParser parser_;

  mutable std::mutex catalog_mutex_;
  std::unordered_map<std::string, uint32_t> table_ids_;
  std::atomic<uint32_t> next_table_id_;

  void acceptLoop();
  void handleConnection(int client_fd);
  bool execQuery(int client_fd, const std::string& sql);

  bool handleCreate(const QueryAST& ast, std::string& error);
  bool handleInsert(const QueryAST& ast, std::string& error);
  bool handleDelete(const QueryAST& ast, std::string& error);
  bool handleSelect(const QueryAST& ast, int client_fd, std::string& error);
  bool loadCatalog();
  bool persistCatalog() const;

  std::unordered_map<std::string, const Table*> snapshotTables() const;
  bool resolveTable(const std::string& name, Table*& out_table, uint32_t& out_table_id) const;

  static bool rowCallback(void* ctx, const RowValue* projected_values, size_t projected_count);
  static bool flushPendingRows(QueryResultContext& qctx);
};

}  // namespace flexql
