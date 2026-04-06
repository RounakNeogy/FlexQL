#pragma once

#include <cstdint>

#include "flexql/core_types.hpp"
#include "flexql/storage_engine.hpp"

namespace flexql {

constexpr uint8_t kTombstoneLive = 0;
constexpr uint8_t kTombstoneDeleted = 1;
constexpr uint16_t kRowHeaderRawBytes = 9;
constexpr uint16_t kRowHeaderAlignedBytes = 16;

uint16_t align8(uint16_t n);
uint16_t columnStorageSize(ColType type);

bool serializeRowIntoPage(const Row& row, const Schema& schema, uint16_t row_size_bytes, Page& page);
bool deserializeRowFromPage(const Page& page, const Schema& schema, uint16_t row_size_bytes,
                            uint16_t row_slot, Row& out_row);

}  // namespace flexql
