#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace flexql {

class BloomFilter {
 public:
  static constexpr size_t kBits = 2048;
  static constexpr size_t kWords = kBits / 64;

  BloomFilter();

  void clear();
  void add(uint64_t h);
  bool possiblyContains(uint64_t h) const;

 private:
  std::array<uint64_t, kWords> bits_;
};

}  // namespace flexql
