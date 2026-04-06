#pragma once

#include <cstdint>

namespace flexql {

enum class MessageType : uint8_t {
  QUERY = 0x01,
  ROW = 0x02,
  DONE = 0x03,
  ERROR = 0x04,
  QUIT = 0x05,
  SCHEMA = 0x06,
  ABORT = 0x07,
};

}  // namespace flexql
