/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQVarint.h"

#include <folly/io/IOBufQueue.h>
#include <folly/portability/GTest.h>

#include <cstdint>
#include <utility>
#include <vector>

using namespace moxygen;

namespace {

std::unique_ptr<folly::IOBuf> hexToBuf(const std::string& hex) {
  auto bytes = folly::IOBuf::create(hex.size() / 2);
  for (size_t i = 0; i < hex.size(); i += 2) {
    uint8_t b = static_cast<uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16));
    bytes->writableData()[i / 2] = b;
  }
  bytes->append(hex.size() / 2);
  return bytes;
}

std::string encodeToHex(uint64_t value) {
  folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&q, 16);
  auto res = encodeMoQVarint(value, appender);
  EXPECT_FALSE(res.hasError());
  auto chain = q.move();
  std::string out;
  out.reserve(chain->computeChainDataLength() * 2);
  const char* hexChars = "0123456789abcdef";
  for (auto range : *chain) {
    for (uint8_t b : range) {
      out.push_back(hexChars[b >> 4]);
      out.push_back(hexChars[b & 0xF]);
    }
  }
  return out;
}

// The 8 example encodings from draft-ietf-moq-transport-18 §1.4.1 Table 2.
struct SpecExample {
  const char* hex;
  uint64_t value;
  size_t length;
};

const std::vector<SpecExample> kSpecExamples = {
    {"25", 37, 1},
    {"bbbd", 15293, 2},
    {"ed7f3e7d", 226442877, 4},
    {"faa1a0e403d8", 2893212287960ULL, 6},
    {"fc8998abc66bc0", 151288809941952ULL, 7},
    {"fefa318fa8e3ca11", 70423237261249041ULL, 8},
    {"ffffffffffffffffff", 18446744073709551615ULL, 9},
};

} // namespace

TEST(MoQVarintTest, DecodeSpecExamples) {
  for (const auto& ex : kSpecExamples) {
    auto buf = hexToBuf(ex.hex);
    folly::io::Cursor cursor(buf.get());
    auto result = decodeMoQVarint(cursor);
    ASSERT_TRUE(result.has_value()) << "hex=" << ex.hex;
    EXPECT_EQ(result->first, ex.value) << "hex=" << ex.hex;
    EXPECT_EQ(result->second, ex.length) << "hex=" << ex.hex;
    EXPECT_FALSE(cursor.canAdvance(1)) << "hex=" << ex.hex;
  }
}

TEST(MoQVarintTest, EncodeMinimalMatchesSpec) {
  for (const auto& ex : kSpecExamples) {
    EXPECT_EQ(encodeToHex(ex.value), ex.hex)
        << "value=" << ex.value << " expected hex=" << ex.hex;
  }
}

TEST(MoQVarintTest, GetSizeMatchesEncodedLength) {
  for (const auto& ex : kSpecExamples) {
    EXPECT_EQ(getMoQVarintSize(ex.value), ex.length) << "value=" << ex.value;
  }
}

TEST(MoQVarintTest, RoundTripBoundaries) {
  // Boundaries at each usable-bit width transition.
  std::vector<uint64_t> values = {
      0,
      1,
      127,
      128,
      16383,
      16384,
      (1ULL << 21) - 1,
      (1ULL << 21),
      (1ULL << 28) - 1,
      (1ULL << 28),
      (1ULL << 35) - 1,
      (1ULL << 35),
      (1ULL << 42) - 1,
      (1ULL << 42),
      (1ULL << 49) - 1,
      (1ULL << 49),
      (1ULL << 56) - 1,
      (1ULL << 56),
      (1ULL << 63),
      ~0ULL,
  };

  for (uint64_t v : values) {
    folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
    folly::io::QueueAppender appender(&q, 16);
    auto enc = encodeMoQVarint(v, appender);
    ASSERT_FALSE(enc.hasError());
    EXPECT_EQ(*enc, getMoQVarintSize(v)) << "v=" << v;

    auto chain = q.move();
    folly::io::Cursor cursor(chain.get());
    auto dec = decodeMoQVarint(cursor);
    ASSERT_TRUE(dec.has_value()) << "v=" << v;
    EXPECT_EQ(dec->first, v);
    EXPECT_EQ(dec->second, *enc);
  }
}

TEST(MoQVarintTest, AcceptsNonMinimalEncoding) {
  // 0x8025 is a non-minimal 2-byte encoding of 37 (minimal is 0x25).
  auto buf = hexToBuf("8025");
  folly::io::Cursor cursor(buf.get());
  auto result = decodeMoQVarint(cursor);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, 37);
  EXPECT_EQ(result->second, 2);
}

TEST(MoQVarintTest, TruncatedInputReturnsNullopt) {
  // 2-byte encoding declared but only 1 byte available.
  auto buf = hexToBuf("80");
  folly::io::Cursor cursor(buf.get());
  auto result = decodeMoQVarint(cursor);
  EXPECT_FALSE(result.has_value());
  // Cursor must not have been advanced.
  EXPECT_TRUE(cursor.canAdvance(1));

  // 9-byte encoding declared but only 5 bytes available.
  auto buf2 = hexToBuf("ff0102030405");
  folly::io::Cursor cursor2(buf2.get());
  auto result2 = decodeMoQVarint(cursor2);
  EXPECT_FALSE(result2.has_value());
  EXPECT_TRUE(cursor2.canAdvance(1));
}

TEST(MoQVarintTest, EmptyCursorReturnsNullopt) {
  auto buf = folly::IOBuf::create(0);
  folly::io::Cursor cursor(buf.get());
  auto result = decodeMoQVarint(cursor);
  EXPECT_FALSE(result.has_value());
}

TEST(MoQVarintTest, AtMostLimitsDecodedLength) {
  // 2-byte encoding present, but atMost=1 should reject.
  auto buf = hexToBuf("bbbd");
  folly::io::Cursor cursor(buf.get());
  auto result = decodeMoQVarint(cursor, /*atMost=*/1);
  EXPECT_FALSE(result.has_value());
  EXPECT_TRUE(cursor.canAdvance(2));
}

TEST(MoQVarintTest, GetSizeAtBitWidthTransitions) {
  EXPECT_EQ(getMoQVarintSize(0), 1u);
  EXPECT_EQ(getMoQVarintSize(127), 1u);
  EXPECT_EQ(getMoQVarintSize(128), 2u);
  EXPECT_EQ(getMoQVarintSize(16383), 2u);
  EXPECT_EQ(getMoQVarintSize(16384), 3u);
  EXPECT_EQ(getMoQVarintSize((1ULL << 21) - 1), 3u);
  EXPECT_EQ(getMoQVarintSize(1ULL << 21), 4u);
  EXPECT_EQ(getMoQVarintSize((1ULL << 28) - 1), 4u);
  EXPECT_EQ(getMoQVarintSize(1ULL << 28), 5u);
  EXPECT_EQ(getMoQVarintSize((1ULL << 35) - 1), 5u);
  EXPECT_EQ(getMoQVarintSize(1ULL << 35), 6u);
  EXPECT_EQ(getMoQVarintSize((1ULL << 42) - 1), 6u);
  EXPECT_EQ(getMoQVarintSize(1ULL << 42), 7u);
  EXPECT_EQ(getMoQVarintSize((1ULL << 49) - 1), 7u);
  EXPECT_EQ(getMoQVarintSize(1ULL << 49), 8u);
  EXPECT_EQ(getMoQVarintSize((1ULL << 56) - 1), 8u);
  EXPECT_EQ(getMoQVarintSize(1ULL << 56), 9u);
  EXPECT_EQ(getMoQVarintSize(~0ULL), 9u);
}
