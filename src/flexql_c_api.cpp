#include "flexql.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <system_error>
#include <string>
#include <vector>

#include "flexql/core_types.hpp"
#include "flexql/tcp_protocol.hpp"

struct FlexQL_DB_Internal {
  int sockfd;
};

namespace {

bool write_all(int fd, const void* data, size_t len) {
  const uint8_t* ptr = static_cast<const uint8_t*>(data);
  size_t written = 0;
  while (written < len) {
    const ssize_t rc = ::send(fd, ptr + written, len - written, 0);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (rc == 0) {
      return false;
    }
    written += static_cast<size_t>(rc);
  }
  return true;
}

bool read_all(int fd, void* data, size_t len) {
  uint8_t* ptr = static_cast<uint8_t*>(data);
  size_t got = 0;
  while (got < len) {
    const ssize_t rc = ::recv(fd, ptr + got, len - got, 0);
    if (rc < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (rc == 0) {
      return false;
    }
    got += static_cast<size_t>(rc);
  }
  return true;
}

bool send_frame(int fd, flexql::MessageType type, const uint8_t* payload, uint32_t payload_len) {
  const uint32_t net_len = htonl(payload_len);
  uint8_t header[5];
  std::memcpy(header, &net_len, sizeof(uint32_t));
  header[4] = static_cast<uint8_t>(type);
  if (!write_all(fd, header, sizeof(header))) {
    return false;
  }
  if (payload_len == 0) {
    return true;
  }
  return write_all(fd, payload, payload_len);
}

bool recv_frame(int fd, flexql::MessageType& out_type, std::vector<uint8_t>& out_payload) {
  uint8_t header[5];
  if (!read_all(fd, header, sizeof(header))) {
    return false;
  }
  uint32_t payload_len = 0;
  std::memcpy(&payload_len, header, sizeof(uint32_t));
  payload_len = ntohl(payload_len);
  out_type = static_cast<flexql::MessageType>(header[4]);
  out_payload.assign(payload_len, 0);
  if (payload_len > 0 && !read_all(fd, out_payload.data(), payload_len)) {
    return false;
  }
  return true;
}

char* dup_cstr(const std::string& s) {
  char* out = static_cast<char*>(std::malloc(s.size() + 1));
  if (out == nullptr) {
    return nullptr;
  }
  std::memcpy(out, s.data(), s.size());
  out[s.size()] = '\0';
  return out;
}

bool value_to_string(flexql::ColType type, const uint8_t* ptr, size_t avail, std::string& out) {
  out.clear();
  if (type == flexql::ColType::INT) {
    if (avail < 8) return false;
    int64_t v = 0;
    std::memcpy(&v, ptr, 8);
    char buf[32];
    const auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), static_cast<long long>(v));
    if (ec != std::errc()) return false;
    out.assign(buf, static_cast<size_t>(p - buf));
    return true;
  }
  if (type == flexql::ColType::DATETIME) {
    if (avail < 8) return false;
    int64_t v = 0;
    std::memcpy(&v, ptr, 8);
    char buf[32];
    const auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), static_cast<long long>(v));
    if (ec != std::errc()) return false;
    out.assign(buf, static_cast<size_t>(p - buf));
    return true;
  }
  if (type == flexql::ColType::DECIMAL) {
    if (avail < 8) return false;
    double v = 0.0;
    std::memcpy(&v, ptr, 8);
#if defined(__cpp_lib_to_chars) && (__cpp_lib_to_chars >= 201611L)
    char buf[64];
    const auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), v, std::chars_format::general, 12);
    if (ec == std::errc()) {
      out.assign(buf, static_cast<size_t>(p - buf));
      return true;
    }
#endif
    char fallback_buf[64];
    std::snprintf(fallback_buf, sizeof(fallback_buf), "%.17g", v);
    out.assign(fallback_buf);
    return true;
  }

  if (avail < 256) return false;
  uint16_t len = 0;
  std::memcpy(&len, ptr, sizeof(uint16_t));
  if (len > 254) {
    len = 254;
  }
  out.assign(reinterpret_cast<const char*>(ptr + 2), reinterpret_cast<const char*>(ptr + 2 + len));
  return true;
}

void free_str_array(char** arr, int n) {
  if (arr == nullptr) {
    return;
  }
  for (int i = 0; i < n; ++i) {
    std::free(arr[i]);
  }
  std::free(arr);
}

char** make_col_names(int col_count) {
  char** names = static_cast<char**>(std::calloc(static_cast<size_t>(col_count), sizeof(char*)));
  if (names == nullptr) {
    return nullptr;
  }
  for (int i = 0; i < col_count; ++i) {
    char tmp[32];
    std::snprintf(tmp, sizeof(tmp), "col%d", i);
    names[i] = dup_cstr(tmp);
    if (names[i] == nullptr) {
      free_str_array(names, i);
      return nullptr;
    }
  }
  return names;
}

}  // namespace

extern "C" int flexql_open(const char* host, int port, FlexQL_DB** db) {
  if (host == nullptr || db == nullptr || port <= 0 || port > 65535) {
    return FLEXQL_ERROR;
  }
  *db = nullptr;

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  char port_buf[16];
  std::snprintf(port_buf, sizeof(port_buf), "%d", port);
  addrinfo* result = nullptr;
  if (::getaddrinfo(host, port_buf, &hints, &result) != 0) {
    return FLEXQL_ERROR;
  }

  int sockfd = -1;
  for (addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
    const int fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
      sockfd = fd;
      break;
    }
    ::close(fd);
  }
  ::freeaddrinfo(result);

  if (sockfd < 0) {
    return FLEXQL_ERROR;
  }

  FlexQL_DB* handle = static_cast<FlexQL_DB*>(std::calloc(1, sizeof(FlexQL_DB)));
  if (handle == nullptr) {
    ::close(sockfd);
    return FLEXQL_ERROR;
  }
  handle->sockfd = sockfd;
  *db = handle;
  return FLEXQL_OK;
}

extern "C" int flexql_close(FlexQL_DB* db) {
  if (db == nullptr) {
    return FLEXQL_ERROR;
  }
  (void)send_frame(db->sockfd, flexql::MessageType::QUIT, nullptr, 0);
  (void)::shutdown(db->sockfd, SHUT_RDWR);
  (void)::close(db->sockfd);
  std::free(db);
  return FLEXQL_OK;
}

extern "C" int flexql_exec(FlexQL_DB* db,
                           const char* sql,
                           int (*callback)(void*, int, char**, char**),
                           void* arg,
                           char** errmsg) {
  if (errmsg != nullptr) {
    *errmsg = nullptr;
  }
  if (db == nullptr || sql == nullptr) {
    if (errmsg != nullptr) {
      *errmsg = dup_cstr("invalid arguments");
    }
    return FLEXQL_ERROR;
  }

  const std::string query(sql);
  if (!send_frame(db->sockfd, flexql::MessageType::QUERY, reinterpret_cast<const uint8_t*>(query.data()),
                  static_cast<uint32_t>(query.size()))) {
    if (errmsg != nullptr) {
      *errmsg = dup_cstr("failed to send query");
    }
    return FLEXQL_ERROR;
  }

  bool abort_requested = false;
  char** schema_col_names = nullptr;
  int schema_col_count = 0;
  char** fallback_col_names = nullptr;
  int fallback_col_count = 0;
  std::vector<std::string> row_values;
  std::vector<char*> row_value_ptrs;
  while (true) {
    flexql::MessageType msg_type = flexql::MessageType::ERROR;
    std::vector<uint8_t> payload;
    if (!recv_frame(db->sockfd, msg_type, payload)) {
      free_str_array(schema_col_names, schema_col_count);
      if (errmsg != nullptr) {
        *errmsg = dup_cstr("failed to receive server response");
      }
      return FLEXQL_ERROR;
    }

    if (msg_type == flexql::MessageType::DONE) {
      free_str_array(schema_col_names, schema_col_count);
      free_str_array(fallback_col_names, fallback_col_count);
      return FLEXQL_OK;
    }

    if (msg_type == flexql::MessageType::ERROR) {
      free_str_array(schema_col_names, schema_col_count);
      free_str_array(fallback_col_names, fallback_col_count);
      if (errmsg != nullptr) {
        *errmsg = dup_cstr(std::string(reinterpret_cast<const char*>(payload.data()), payload.size()));
      }
      return FLEXQL_ERROR;
    }

    if (msg_type == flexql::MessageType::SCHEMA) {
      if (payload.size() < sizeof(uint16_t)) {
        free_str_array(schema_col_names, schema_col_count);
        if (errmsg != nullptr) {
          *errmsg = dup_cstr("protocol error: invalid SCHEMA payload");
        }
        return FLEXQL_ERROR;
      }

      uint16_t col_count = 0;
      std::memcpy(&col_count, payload.data(), sizeof(uint16_t));
      size_t cursor = sizeof(uint16_t);

      free_str_array(schema_col_names, schema_col_count);
      schema_col_count = static_cast<int>(col_count);
      schema_col_names = static_cast<char**>(std::calloc(col_count, sizeof(char*)));
      if (schema_col_names == nullptr) {
        schema_col_count = 0;
        if (errmsg != nullptr) {
          *errmsg = dup_cstr("out of memory");
        }
        return FLEXQL_ERROR;
      }

      bool ok = true;
      for (uint16_t i = 0; i < col_count; ++i) {
        if (cursor + sizeof(uint16_t) > payload.size()) {
          ok = false;
          break;
        }
        uint16_t len = 0;
        std::memcpy(&len, payload.data() + cursor, sizeof(uint16_t));
        cursor += sizeof(uint16_t);
        if (cursor + len > payload.size()) {
          ok = false;
          break;
        }
        schema_col_names[i] = static_cast<char*>(std::malloc(static_cast<size_t>(len) + 1));
        if (schema_col_names[i] == nullptr) {
          ok = false;
          break;
        }
        if (len > 0) {
          std::memcpy(schema_col_names[i], payload.data() + cursor, len);
        }
        schema_col_names[i][len] = '\0';
        cursor += len;
      }

      if (!ok) {
        free_str_array(schema_col_names, schema_col_count);
        free_str_array(fallback_col_names, fallback_col_count);
        schema_col_names = nullptr;
        schema_col_count = 0;
        if (errmsg != nullptr) {
          *errmsg = dup_cstr("protocol error: malformed schema");
        }
        return FLEXQL_ERROR;
      }
      continue;
    }

    if (msg_type != flexql::MessageType::ROW) {
      free_str_array(schema_col_names, schema_col_count);
      free_str_array(fallback_col_names, fallback_col_count);
      if (errmsg != nullptr) {
        *errmsg = dup_cstr("protocol error: unexpected message");
      }
      return FLEXQL_ERROR;
    }

    if (payload.size() < sizeof(uint16_t)) {
      free_str_array(schema_col_names, schema_col_count);
      free_str_array(fallback_col_names, fallback_col_count);
      if (errmsg != nullptr) {
        *errmsg = dup_cstr("protocol error: invalid ROW payload");
      }
      return FLEXQL_ERROR;
    }

    uint16_t col_count = 0;
    std::memcpy(&col_count, payload.data(), sizeof(uint16_t));
    size_t cursor = sizeof(uint16_t);
    if (row_values.size() != col_count) {
      row_values.assign(col_count, std::string());
      row_value_ptrs.assign(col_count, nullptr);
    }
    if (row_value_ptrs.size() != col_count) {
      free_str_array(schema_col_names, schema_col_count);
      free_str_array(fallback_col_names, fallback_col_count);
      if (errmsg != nullptr) {
        *errmsg = dup_cstr("out of memory");
      }
      return FLEXQL_ERROR;
    }

    if (schema_col_names == nullptr || schema_col_count != static_cast<int>(col_count)) {
      if (fallback_col_names == nullptr || fallback_col_count != static_cast<int>(col_count)) {
        free_str_array(fallback_col_names, fallback_col_count);
        fallback_col_names = make_col_names(col_count);
        fallback_col_count = static_cast<int>(col_count);
      }
      if (fallback_col_names == nullptr) {
        free_str_array(schema_col_names, schema_col_count);
        if (errmsg != nullptr) {
          *errmsg = dup_cstr("out of memory");
        }
        return FLEXQL_ERROR;
      }
    }

    bool row_ok = true;
    for (uint16_t i = 0; i < col_count; ++i) {
      if (cursor >= payload.size()) {
        row_ok = false;
        break;
      }
      const auto type = static_cast<flexql::ColType>(payload[cursor++]);
      if (type == flexql::ColType::VARCHAR) {
        if (cursor + sizeof(uint16_t) > payload.size()) {
          row_ok = false;
          break;
        }
        uint16_t len = 0;
        std::memcpy(&len, payload.data() + cursor, sizeof(uint16_t));
        cursor += sizeof(uint16_t);
        if (cursor + len > payload.size()) {
          row_ok = false;
          break;
        }
        row_values[i].assign(reinterpret_cast<const char*>(payload.data() + cursor),
                             reinterpret_cast<const char*>(payload.data() + cursor + len));
        row_value_ptrs[i] = row_values[i].empty() ? const_cast<char*>("") : row_values[i].data();
        cursor += len;
      } else {
        constexpr size_t kFixedSize = 8;
        if (cursor + kFixedSize > payload.size()) {
          row_ok = false;
          break;
        }
        if (!value_to_string(type, payload.data() + cursor, kFixedSize, row_values[i])) {
          row_ok = false;
          break;
        }
        row_value_ptrs[i] = row_values[i].empty() ? const_cast<char*>("") : row_values[i].data();
        cursor += kFixedSize;
      }
    }

    if (!row_ok) {
      free_str_array(schema_col_names, schema_col_count);
      free_str_array(fallback_col_names, fallback_col_count);
      if (errmsg != nullptr) {
        *errmsg = dup_cstr("protocol error: malformed row");
      }
      return FLEXQL_ERROR;
    }

    if (!abort_requested && callback != nullptr) {
      char** callback_names = (schema_col_names != nullptr && schema_col_count == static_cast<int>(col_count))
                                  ? schema_col_names
                                  : fallback_col_names;
      const int cb = callback(arg, static_cast<int>(col_count), row_value_ptrs.data(), callback_names);
      if (cb == 1) {
        abort_requested = true;
        (void)send_frame(db->sockfd, flexql::MessageType::ABORT, nullptr, 0);
      }
    }
  }
}

extern "C" void flexql_free(void* ptr) {
  std::free(ptr);
}
