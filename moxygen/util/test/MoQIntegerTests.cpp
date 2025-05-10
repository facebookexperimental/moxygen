/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Expected.h>
#include <folly/Optional.h>
#include <folly/String.h>
#include <folly/io/IOBuf.h>
#include <folly/portability/GTest.h>
#include <quic/common/BufUtil.h>

#include <moxygen/util/MoQInteger.h>

using namespace testing;
using namespace folly;

namespace moxygen::test {

struct IntegerParams {
  uint64_t decoded;
  std::string hexEncoded;
  bool error{false};
  uint8_t encodedLength{8};
};

class MoQIntegerDecodeTest : public TestWithParam<IntegerParams> {};

class MoQIntegerEncodeTest : public TestWithParam<IntegerParams> {};

TEST_P(MoQIntegerDecodeTest, DecodeTrim) {
  std::string encodedBytes = folly::unhexlify(GetParam().hexEncoded);

  for (int atMost = 0; atMost <= GetParam().encodedLength; atMost++) {
    auto wrappedEncoded = IOBuf::copyBuffer(encodedBytes);
    wrappedEncoded->trimEnd(std::min(
        (unsigned long)(wrappedEncoded->computeChainDataLength()),
        (unsigned long)(GetParam().encodedLength - atMost)));
    folly::io::Cursor cursor(wrappedEncoded.get());
    auto originalLength = cursor.length();
    auto decodedValue = decodeMoQInteger(cursor);
    if (GetParam().error || atMost != GetParam().encodedLength) {
      EXPECT_FALSE(decodedValue.has_value());
      EXPECT_EQ(cursor.length(), originalLength);
    } else {
      EXPECT_EQ(decodedValue->first, GetParam().decoded);
      EXPECT_EQ(decodedValue->second, GetParam().encodedLength);
      EXPECT_EQ(cursor.length(), originalLength - GetParam().encodedLength);
    }
  }
}

TEST_P(MoQIntegerDecodeTest, DecodeAtMost) {
  std::string encodedBytes = folly::unhexlify(GetParam().hexEncoded);
  auto wrappedEncoded = IOBuf::copyBuffer(encodedBytes);

  for (int atMost = 0; atMost <= GetParam().encodedLength; atMost++) {
    folly::io::Cursor cursor(wrappedEncoded.get());
    auto originalLength = cursor.length();
    auto decodedValue = decodeMoQInteger(cursor, atMost);
    if (GetParam().error || atMost != GetParam().encodedLength) {
      EXPECT_FALSE(decodedValue.has_value());
      EXPECT_EQ(cursor.length(), originalLength);
    } else {
      EXPECT_EQ(decodedValue->first, GetParam().decoded);
      EXPECT_EQ(decodedValue->second, GetParam().encodedLength);
      EXPECT_EQ(cursor.length(), originalLength - GetParam().encodedLength);
    }
  }
}

TEST_P(MoQIntegerEncodeTest, Encode) {
  auto queue = folly::IOBuf::create(0);
  quic::BufAppender appender(queue.get(), 10);
  auto appendOp = [&](auto val) { appender.writeBE(val); };
  auto written = encodeMoQInteger(GetParam().decoded, appendOp);
  auto encodedValue = folly::hexlify(queue->to<std::string>());
  LOG(INFO) << "encoded=" << encodedValue;
  LOG(INFO) << "expected=" << GetParam().hexEncoded;

  EXPECT_EQ(encodedValue, GetParam().hexEncoded);
  EXPECT_EQ(written, encodedValue.size() / 2);
}

TEST_P(MoQIntegerEncodeTest, GetSize) {
  auto size = getMoQIntegerSize(GetParam().decoded);
  EXPECT_EQ(size, GetParam().hexEncoded.size() / 2);
}

TEST_F(MoQIntegerEncodeTest, ForceFourBytes) {
  auto queue = folly::IOBuf::create(0);
  quic::BufAppender appender(queue.get(), 10);
  auto appendOp = [&](auto val) { appender.writeBE(val); };
  EXPECT_EQ(4, encodeMoQInteger(37, appendOp, 4));
  auto encodedValue = folly::hexlify(queue->to<std::string>());
  EXPECT_EQ("c0000025", encodedValue);
}

TEST_F(MoQIntegerEncodeTest, ForceEightBytes) {
  auto queue = folly::IOBuf::create(0);
  quic::BufAppender appender(queue.get(), 10);
  auto appendOp = [&](auto val) { appender.writeBE(val); };
  EXPECT_EQ(8, encodeMoQInteger(37, appendOp, 8));
  auto encodedValue = folly::hexlify(queue->to<std::string>());
  EXPECT_EQ("e000000000000025", encodedValue);
}

TEST_F(MoQIntegerEncodeTest, ForceWrongBytes) {
  auto queue = folly::IOBuf::create(0);
  quic::BufAppender appender(queue.get(), 10);
  auto appendOp = [&](auto val) { appender.writeBE(val); };
  EXPECT_DEATH(encodeMoQInteger(15293, appendOp, 1), "");
}

INSTANTIATE_TEST_SUITE_P(
    MoQIntegerTests,
    MoQIntegerDecodeTest,
    Values(
        IntegerParams({0, "00", false, 1}),
        IntegerParams({494878333, "dd7f3e7d", false, 4}),
        IntegerParams({15293, "bbbd", false, 2}),
        IntegerParams({37, "25", false, 1}),
        IntegerParams({37, "8025", false, 2}),
        IntegerParams({37, "C0000025", false, 4}),
        IntegerParams({37, "E000000000000025", false, 8}),
        IntegerParams({37, "80", true})));

INSTANTIATE_TEST_SUITE_P(
    MoQIntegerEncodeTests,
    MoQIntegerEncodeTest,
    Values(
        IntegerParams({0, "00", false, 1}),
        IntegerParams({127, "7f", false, 1}),
        IntegerParams({16383, "bfff", false, 2}),
        IntegerParams({536870911, "dfffffff", false, 4}),
        IntegerParams({151288809941952652, "e2197c5eff14e88c", false, 4}),
        IntegerParams({1152921504606846975, "efffffffffffffff", false, 8}),
        IntegerParams(
            {std::numeric_limits<uint64_t>::max(),
             "f0ffffffffffffffff",
             false,
             9})));

} // namespace moxygen::test
