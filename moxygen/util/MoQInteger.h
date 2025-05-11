/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <folly/Optional.h>
#include <folly/String.h>
#include <folly/io/Cursor.h>
#include <folly/lang/Bits.h>
#include <quic/common/BufUtil.h>

namespace moxygen {

constexpr uint64_t kOneByteLimit = 0x7F;
constexpr uint64_t kTwoByteLimit = 0x3FFF;
constexpr uint64_t kFourByteLimit = 0x1FFFFFFF;
constexpr uint64_t kEightByteLimit = 0x0FFFFFFFFFFFFFFF;

namespace {
template <typename BufOp>
inline size_t encodeOneByte(BufOp bufop, uint64_t value) {
  auto modified = static_cast<uint8_t>(value);
  bufop(modified);
  return sizeof(modified);
}

template <typename BufOp>
inline size_t encodeTwoBytes(BufOp bufop, uint64_t value) {
  auto reduced = static_cast<uint16_t>(value);
  uint16_t modified = reduced | 0x8000;
  bufop(modified);
  return sizeof(modified);
}

template <typename BufOp>
inline size_t encodeFourBytes(BufOp bufop, uint64_t value) {
  auto reduced = static_cast<uint32_t>(value);
  uint32_t modified = reduced | 0xC0000000;
  bufop(modified);
  return sizeof(modified);
}

template <typename BufOp>
inline size_t encodeEightBytes(BufOp bufop, uint64_t value) {
  uint64_t modified = value | 0xE000000000000000;
  bufop(modified);
  return sizeof(modified);
}

template <typename BufOp>
inline size_t encodeNineBytes(BufOp bufop, uint64_t value) {
  bufop(uint8_t(0xF0));
  bufop(value);
  return 9;
}
} // namespace

/**
 * Encodes the integer and writes it out to appender. Returns the number of
 * bytes written, or an error if value is too large to be represented with the
 * variable length encoding.
 */
template <typename BufOp>
size_t encodeMoQInteger(uint64_t value, BufOp bufop) {
  if (value <= kOneByteLimit) {
    return encodeOneByte(std::move(bufop), value);
  } else if (value <= kTwoByteLimit) {
    return encodeTwoBytes(std::move(bufop), value);
  } else if (value <= kFourByteLimit) {
    return encodeFourBytes(std::move(bufop), value);
  } else if (value <= kEightByteLimit) {
    return encodeEightBytes(std::move(bufop), value);
  } else {
    return encodeNineBytes(std::move(bufop), value);
  }
}

template <typename BufOp>
size_t encodeMoQInteger(uint64_t value, BufOp bufop, int outputSize) {
  switch (outputSize) {
    case 1:
      CHECK(value <= kOneByteLimit);
      return encodeOneByte(std::move(bufop), value);
    case 2:
      CHECK(value <= kTwoByteLimit);
      return encodeTwoBytes(std::move(bufop), value);
    case 4:
      CHECK(value <= kFourByteLimit);
      return encodeFourBytes(std::move(bufop), value);
    case 8:
      CHECK(value <= kEightByteLimit);
      return encodeEightBytes(std::move(bufop), value);
    case 9:
      return encodeNineBytes(std::move(bufop), value);
  }
  CHECK(false);
}

/**
 * Reads an integer out of the cursor and returns a pair with the integer and
 * the numbers of bytes read, or none if there are not enough bytes to
 * read the int. It only advances the cursor in case of success.
 */
folly::Optional<std::pair<uint64_t, size_t>> decodeMoQInteger(
    folly::io::Cursor& cursor,
    uint64_t atMost = sizeof(uint64_t));

/**
 * Returns the length of a quic integer given the first byte
 */
uint8_t decodeMoQIntegerLength(uint8_t firstByte);

/**
 * Returns number of bytes needed to encode value as a QUIC integer, or an error
 * if value is too large to be represented with the variable
 * length encoding
 */
[[nodiscard]] size_t getMoQIntegerSize(uint64_t value);

} // namespace moxygen
