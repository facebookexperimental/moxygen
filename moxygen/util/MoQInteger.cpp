/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/util/MoQInteger.h>

namespace {
const std::array<uint8_t, 16>& getLengths() {
  static const std::array<uint8_t, 16> lengths = {
      1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 4, 4, 8, 9};
  return lengths;
}

const std::array<uint8_t, 16>& getMasks() {
  // clang-format off
  static const std::array<uint8_t, 16> masks = {
      0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
      0xc0, 0xc0, 0xc0, 0xc0, 0xe0, 0xe0, 0xf0, 0x00};
  // clang-format on
  return masks;
}
} // namespace
namespace moxygen {

size_t getMoQIntegerSize(uint64_t value) {
  if (value <= kOneByteLimit) {
    return 1;
  } else if (value <= kTwoByteLimit) {
    return 2;
  } else if (value <= kFourByteLimit) {
    return 4;
  } else if (value <= kEightByteLimit) {
    return 8;
  } else {
    return 9;
  }
}

uint8_t decodeMoQIntegerLength(uint8_t firstByte) {
  return (getLengths()[firstByte >> 4]);
}

folly::Optional<std::pair<uint64_t, size_t>> decodeMoQInteger(
    folly::io::Cursor& cursor,
    uint64_t atMost) {
  // checks
  if (atMost == 0 || !cursor.canAdvance(1)) {
    VLOG(10) << "Not enough bytes to decode integer, cursor len="
             << cursor.totalLength();
    return folly::none;
  }

  // get 2 msb of first byte that determines variable-length size expected
  const uint8_t firstByte = *cursor.peekBytes().data();
  const uint8_t prefix = (firstByte >> 4);
  uint8_t bytesExpected = getLengths()[prefix];

  // simple short-circuit eval for varint type == 0
  if (bytesExpected == 1) {
    cursor.skip(1);
    return std::pair<uint64_t, size_t>(firstByte & ~(getMasks()[0]), 1);
  }
  if (bytesExpected == 9) {
    cursor.skip(1);
    bytesExpected = 8;
  }

  // not enough bytes to decode, undo cursor
  if (!cursor.canAdvance(bytesExpected) || atMost < bytesExpected) {
    VLOG(10) << "Could not decode integer numBytes=" << bytesExpected;
    return folly::none;
  }
  // result storage
  uint64_t result{0};
  // pull number of bytes expected
  cursor.pull(&result, bytesExpected);
  // clear mask bites
  uint64_t msbMask = ~(uint64_t(getMasks()[prefix]) << 56);
  result = folly::Endian::big(result) & msbMask;
  // adjust quic integer
  result >>= (8 - bytesExpected) << 3;

  return std::pair<uint64_t, size_t>{result, bytesExpected};
}

} // namespace moxygen
