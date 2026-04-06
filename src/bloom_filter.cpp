#include "flexql/bloom_filter.hpp"

namespace flexql {

BloomFilter::BloomFilter() : bits_{} {
  clear();
}

void BloomFilter::clear() {
  bits_.fill(0);
}

void BloomFilter::add(uint64_t h) {
  const uint64_t h2 = h * 11400714819323198485ULL;
  const uint64_t h3 = h ^ 0x9E3779B185EBCA87ULL;

  const size_t b1 = static_cast<size_t>(h % kBits);
  const size_t b2 = static_cast<size_t>(h2 % kBits);
  const size_t b3 = static_cast<size_t>(h3 % kBits);

  bits_[b1 / 64] |= (1ULL << (b1 % 64));
  bits_[b2 / 64] |= (1ULL << (b2 % 64));
  bits_[b3 / 64] |= (1ULL << (b3 % 64));
}

bool BloomFilter::possiblyContains(uint64_t h) const {
  const uint64_t h2 = h * 11400714819323198485ULL;
  const uint64_t h3 = h ^ 0x9E3779B185EBCA87ULL;

  const size_t b1 = static_cast<size_t>(h % kBits);
  const size_t b2 = static_cast<size_t>(h2 % kBits);
  const size_t b3 = static_cast<size_t>(h3 % kBits);

  const bool p1 = (bits_[b1 / 64] & (1ULL << (b1 % 64))) != 0;
  const bool p2 = (bits_[b2 / 64] & (1ULL << (b2 % 64))) != 0;
  const bool p3 = (bits_[b3 / 64] & (1ULL << (b3 % 64))) != 0;

  return p1 && p2 && p3;
}

}  // namespace flexql
