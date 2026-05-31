/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQVarint.h>

#include <algorithm>
#include <bit>
#include <cstring>

namespace moxygen {

namespace {

// Prefix bits of the first byte for a given encoded length — the bits that
// encode the length, with value bits cleared. OR'd into the first byte
// alongside the value bits during encode. Index 0 is unused; index 9 is
// 0xFF (the L=9 first byte is pure prefix).
constexpr uint8_t kFirstBytePrefixMask[10] = {
    0,
    0x00,
    0x80,
    0xC0,
    0xE0,
    0xF0,
    0xF8,
    0xFC,
    0xFE,
    0xFF,
};

// Largest value encodable in `length` bytes. Index 0 is unused; index 9 is
// the full uint64_t range. Each length N stores a 7*N-bit value.
constexpr uint64_t kValueMax[10] = {
    0,
    (1ULL << 7) - 1,
    (1ULL << 14) - 1,
    (1ULL << 21) - 1,
    (1ULL << 28) - 1,
    (1ULL << 35) - 1,
    (1ULL << 42) - 1,
    (1ULL << 49) - 1,
    (1ULL << 56) - 1,
    ~0ULL,
};

// Mask of the value bits in the first byte for a given encoded length —
// the complement of the prefix mask. For L=9 the first byte is pure prefix
// (0xFF), so the value mask is 0.
constexpr uint8_t firstByteValueMask(size_t length) {
  return static_cast<uint8_t>(~kFirstBytePrefixMask[length]);
}

// Prefix bits positioned at the top byte of a width-T BE word, ready to be
// OR'd into the value before writeBE<T>. Compile-time constant for any
// fixed length L.
template <typename T, size_t L>
constexpr T kPrefixShifted =
    static_cast<T>(kFirstBytePrefixMask[L]) << ((sizeof(T) - 1) * 8);

// Encode helper for lengths 3, 5, 6, 7 — sizes with no native integer width
// (L=1/2/4/8 use typed writeBE, L=9 has its own dedicated path).
// Builds the encoded bytes in a BE-ordered u64 (value left-shifted into the
// high `length` bytes, prefix OR'd into the top byte so bswap places it at
// byte 0) and memcpys `length` bytes directly into the appender's writable
// area, avoiding the local-buffer copy that `push(bytes, length)` would do.
size_t encodeSpecialLength(
    uint64_t value,
    size_t length,
    folly::io::QueueAppender& appender) noexcept {
  appender.ensure(length);
  const uint64_t shifted = (value << ((8 - length) * 8)) |
      (static_cast<uint64_t>(kFirstBytePrefixMask[length]) << 56);
  const uint64_t be = folly::Endian::big(shifted);
  std::memcpy(appender.writableData(), &be, length);
  appender.append(length);
  return length;
}

} // namespace

size_t getMoQVarintSize(uint64_t value) noexcept {
  // bit_width(0) == 0 -> 0; clamp lifts to 1.
  // bit_width(2^64-1) == 64 -> 70/7 == 10; clamp lowers to 9.
  return std::clamp<size_t>((std::bit_width(value) + 6) / 7, 1, 9);
}

quic::Optional<std::pair<uint64_t, size_t>> decodeMoQVarintSlow(
    folly::io::Cursor& cursor,
    folly::ByteRange bytes,
    uint64_t atMost) {
  // L=1 is handled inline in the header. Caller has already peeked the first
  // byte and verified its high bit is set; `bytes` is non-empty.
  const uint8_t firstByte = bytes.data()[0];
  const size_t length = static_cast<size_t>(std::countl_one(firstByte)) + 1;
  if (length > atMost) {
    return std::nullopt;
  }

  // Fast path: the entire varint sits in the contiguous peek. Decode in
  // place from `bytes.data()` and skip the cursor once.
  if (FOLLY_LIKELY(bytes.size() >= length)) {
    if (length == 9) {
      uint64_t v;
      std::memcpy(&v, bytes.data() + 1, sizeof(v));
      cursor.skip(9);
      return std::pair<uint64_t, size_t>{folly::Endian::big(v), 9};
    }
    // Length 2..8: load right-aligned into a zero-initialized u64
    // (BE-ordered), mask the prefix bits off the high byte, bswap.
    uint64_t x = 0;
    auto* xb = reinterpret_cast<uint8_t*>(&x);
    std::memcpy(xb + (8 - length), bytes.data(), length);
    xb[8 - length] &= firstByteValueMask(length);
    cursor.skip(length);
    return std::pair<uint64_t, size_t>{folly::Endian::big(x), length};
  }

  // Chained-IOBuf fallback: bytes span multiple buffers; use cursor.pull.
  if (!cursor.canAdvance(length)) {
    return std::nullopt;
  }
  if (length == 9) {
    cursor.skip(1);
    return std::pair<uint64_t, size_t>{cursor.readBE<uint64_t>(), 9};
  }
  uint64_t x = 0;
  auto* xb = reinterpret_cast<uint8_t*>(&x);
  cursor.pull(xb + (8 - length), length);
  xb[8 - length] &= static_cast<uint8_t>(~kFirstBytePrefixMask[length]);
  return std::pair<uint64_t, size_t>{folly::Endian::big(x), length};
}

folly::Expected<size_t, quic::TransportErrorCode> encodeMoQVarintSlow(
    uint64_t value,
    folly::io::QueueAppender& appender) noexcept {
  // L=1 is handled inline in the header. Range cascade for L>=2: each
  // comparison both decides the length and selects the write path.
  // Interleaving dispatch with the writes avoids the LZCNT chain that
  // `getMoQVarintSize` would put on the critical path before any store could
  // issue. Power-of-two lengths use typed writeBE for single-store
  // throughput; lengths 3/5/6/7 use a direct-into-appender path that builds
  // the encoded bytes in a BE u64 word and memcpys `length` bytes out.
  if (value <= kValueMax[2]) {
    appender.writeBE<uint16_t>(
        static_cast<uint16_t>(value) | kPrefixShifted<uint16_t, 2>);
    return 2;
  }
  if (value > kValueMax[8]) {
    appender.writeBE<uint8_t>(kFirstBytePrefixMask[9]);
    appender.writeBE<uint64_t>(value);
    return 9;
  }
  if (value > kValueMax[7]) {
    appender.writeBE<uint64_t>(value | kPrefixShifted<uint64_t, 8>);
    return 8;
  }
  if (value > kValueMax[4]) {
    if (value <= kValueMax[5]) {
      return encodeSpecialLength(value, 5, appender);
    }
    if (value <= kValueMax[6]) {
      return encodeSpecialLength(value, 6, appender);
    }
    return encodeSpecialLength(value, 7, appender);
  }
  if (value <= kValueMax[3]) {
    return encodeSpecialLength(value, 3, appender);
  }
  appender.writeBE<uint32_t>(
      static_cast<uint32_t>(value) | kPrefixShifted<uint32_t, 4>);
  return 4;
}

} // namespace moxygen
