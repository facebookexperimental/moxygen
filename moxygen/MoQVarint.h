/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <quic/QuicConstants.h>
#include <quic/common/Optional.h>

#include <cstdint>
#include <utility>

namespace moxygen {

// MoQT variable-length integer encoding (draft-ietf-moq-transport-17+).
//
// Distinct from QUIC's RFC 9000 varint. The first byte's count of leading 1
// bits determines the encoded length:
//
//   0xxxxxxx          -> 1 byte,   7-bit value
//   10xxxxxx ...      -> 2 bytes, 14-bit value
//   110xxxxx ...      -> 3 bytes, 21-bit value
//   1110xxxx ...      -> 4 bytes, 28-bit value
//   11110xxx ...      -> 5 bytes, 35-bit value
//   111110xx ...      -> 6 bytes, 42-bit value
//   1111110x ...      -> 7 bytes, 49-bit value
//   11111110 ...      -> 8 bytes, 56-bit value
//   11111111 <8 bytes> -> 9 bytes, 64-bit value
//
// Non-minimal encodings are allowed by the spec; the decoder accepts them but
// the encoder always emits the minimal form.

// Slow path for decodeMoQVarint — handles lengths 2..9. Out-of-line so we
// avoid bloating every call site. `bytes` is the result of cursor.peekBytes()
// at entry; if it contains the full encoded varint (the common contiguous
// case), the slow path decodes in place; otherwise it falls back to
// cursor.pull for the chained-IOBuf case.
quic::Optional<std::pair<uint64_t, size_t>> decodeMoQVarintSlow(
    folly::io::Cursor& cursor,
    folly::ByteRange bytes,
    uint64_t atMost);

// Decodes a MoQ varint. Advances `cursor` only on success. Returns nullopt if
// fewer than `atMost` bytes are available or the encoded length exceeds
// `atMost`. The L=1 fast path is inlined since single-byte varints dominate
// real traffic.
inline quic::Optional<std::pair<uint64_t, size_t>> decodeMoQVarint(
    folly::io::Cursor& cursor,
    uint64_t atMost = 9) {
  const auto bytes = cursor.peekBytes();
  if (atMost == 0 || bytes.empty()) {
    return std::nullopt;
  }
  const uint8_t firstByte = bytes.data()[0];
  if ((firstByte & 0x80) == 0) {
    cursor.skip(1);
    return std::pair<uint64_t, size_t>{firstByte, 1};
  }
  return decodeMoQVarintSlow(cursor, bytes, atMost);
}

// Slow path for encodeMoQVarint — handles lengths 2..9. Out-of-line so we
// avoid bloating every call site with the full cascade.
folly::Expected<size_t, quic::TransportErrorCode> encodeMoQVarintSlow(
    uint64_t value,
    folly::io::QueueAppender& appender) noexcept;

// Encodes `value` as a MoQ varint into `appender`. Returns bytes written.
// Cannot fail for uint64_t (every value fits) but returns Expected to match
// the QUIC varint encode signature for symmetric dispatch. The L=1 fast path
// is inlined here since single-byte varints dominate real traffic (small
// IDs, lengths, field tags). Inlining additional length cases pushes the
// body past the compiler's inline budget and regresses L=1.
inline folly::Expected<size_t, quic::TransportErrorCode> encodeMoQVarint(
    uint64_t value,
    folly::io::QueueAppender& appender) noexcept {
  if (value <= 0x7FULL) {
    appender.writeBE<uint8_t>(static_cast<uint8_t>(value));
    return 1;
  }
  return encodeMoQVarintSlow(value, appender);
}

// Returns the number of bytes the minimal encoding of `value` would consume.
// Always in [1, 9].
size_t getMoQVarintSize(uint64_t value) noexcept;

} // namespace moxygen
