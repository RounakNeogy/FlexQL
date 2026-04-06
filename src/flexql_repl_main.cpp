#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "flexql.h"

#if defined(__has_include)
#if __has_include(<readline/readline.h>) && __has_include(<readline/history.h>)
#define FLEXQL_HAS_READLINE 1
#include <readline/history.h>
#include <readline/readline.h>
#else
#define FLEXQL_HAS_READLINE 0
#endif
#else
#define FLEXQL_HAS_READLINE 0
#endif

namespace {

struct RowBuffer {
  std::vector<std::vector<std::string>> rows;
  std::vector<std::string> col_names;
};

int capture_row(void* arg, int col_count, char** values, char** column_names) {
  auto* buf = static_cast<RowBuffer*>(arg);
  if (buf->col_names.empty()) {
    buf->col_names.reserve(static_cast<size_t>(col_count));
    for (int i = 0; i < col_count; ++i) {
      buf->col_names.emplace_back(column_names && column_names[i] ? column_names[i]
                                                                   : ("col" + std::to_string(i)));
    }
  }
  std::vector<std::string> row;
  row.reserve(static_cast<size_t>(col_count));
  for (int i = 0; i < col_count; ++i) {
    row.emplace_back(values[i] ? values[i] : "");
  }
  buf->rows.emplace_back(std::move(row));
  return 0;
}

void print_table(const RowBuffer& buf) {
  if (buf.rows.empty()) {
    std::cout << "(ok)\n";
    return;
  }
  const size_t cols = buf.rows.front().size();
  std::vector<size_t> widths(cols, 4);
  for (size_t c = 0; c < cols; ++c) {
    const std::string header =
        (c < buf.col_names.size() && !buf.col_names[c].empty()) ? buf.col_names[c]
                                                                 : ("col" + std::to_string(c));
    widths[c] = std::max(widths[c], header.size());
  }
  for (const auto& row : buf.rows) {
    for (size_t c = 0; c < cols; ++c) {
      widths[c] = std::max(widths[c], row[c].size());
    }
  }

  for (size_t c = 0; c < cols; ++c) {
    const std::string header =
        (c < buf.col_names.size() && !buf.col_names[c].empty()) ? buf.col_names[c]
                                                                 : ("col" + std::to_string(c));
    std::cout << std::left << std::setw(static_cast<int>(widths[c])) << header;
    if (c + 1 < cols) std::cout << " | ";
  }
  std::cout << "\n";
  for (size_t c = 0; c < cols; ++c) {
    std::cout << std::string(widths[c], '-');
    if (c + 1 < cols) std::cout << "-+-";
  }
  std::cout << "\n";
  for (const auto& row : buf.rows) {
    for (size_t c = 0; c < cols; ++c) {
      std::cout << std::left << std::setw(static_cast<int>(widths[c])) << row[c];
      if (c + 1 < cols) std::cout << " | ";
    }
    std::cout << "\n";
  }
  std::cout << buf.rows.size() << " row(s)\n";
}

bool read_line(std::string& out_line) {
#if FLEXQL_HAS_READLINE
  char* line = readline("flexql> ");
  if (line == nullptr) {
    return false;
  }
  out_line.assign(line);
  if (!out_line.empty()) {
    add_history(line);
  }
  std::free(line);
  return true;
#else
  std::cout << "flexql> " << std::flush;
  return static_cast<bool>(std::getline(std::cin, out_line));
#endif
}

}  // namespace

int main(int argc, char** argv) {
  const char* host = "127.0.0.1";
  int port = 9090;
  if (argc >= 2) {
    host = argv[1];
  }
  if (argc >= 3) {
    port = std::atoi(argv[2]);
  }

  FlexQL_DB* db = nullptr;
  if (flexql_open(host, port, &db) != FLEXQL_OK) {
    std::cerr << "flexql_open failed\n";
    return 1;
  }

  std::cout << "Connected to FlexQL at " << host << ":" << port << "\n";
  std::cout << "Type SQL or \\quit to exit\n";

  while (true) {
    std::string line;
    if (!read_line(line)) {
      break;
    }
    if (line == "\\quit") {
      break;
    }
    if (line.empty()) {
      continue;
    }

    RowBuffer buf;
    char* err = nullptr;
    const int rc = flexql_exec(db, line.c_str(), capture_row, &buf, &err);
    if (rc != FLEXQL_OK) {
      std::cerr << "ERROR: " << (err ? err : "unknown") << "\n";
      flexql_free(err);
      continue;
    }
    print_table(buf);
  }

  (void)flexql_close(db);
  return 0;
}
