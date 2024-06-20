/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include <folly/portability/GTest.h>
#include "moxygen/test/TestUtils.h"

using namespace moxygen;

namespace {
class TestUnderflow : public std::exception {};

FrameType parseFrameType(folly::io::Cursor& cursor) {
  auto frameType = quic::decodeQuicInteger(cursor);
  if (!frameType) {
    throw std::runtime_error("Failed to decode frame type");
  }
  return FrameType(frameType->first);
}

void skip(folly::io::Cursor& cursor, size_t i) {
  if (!cursor.canAdvance(i)) {
    throw TestUnderflow();
  }
  cursor.skip(i);
}

void parseAll(folly::io::Cursor& cursor, bool eom) {
  skip(cursor, 2);
  auto r1 = parseClientSetup(cursor);
  EXPECT_TRUE(r1 || (!eom && r1.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 2);
  auto r2 = parseServerSetup(cursor);
  EXPECT_TRUE(r2 || (!eom && r2.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r3 = parseSubscribeRequest(cursor);
  EXPECT_TRUE(r3 || (!eom && r3.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r4 = parseSubscribeOk(cursor);
  EXPECT_TRUE(r4 || (!eom && r4.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r5 = parseSubscribeError(cursor);
  EXPECT_TRUE(r5 || (!eom && r5.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r6 = parseUnsubscribe(cursor);
  EXPECT_TRUE(r6 || (!eom && r6.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r7 = parseSubscribeDone(cursor);
  EXPECT_TRUE(r1 || (!eom && r7.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r8 = parseSubscribeDone(cursor);
  EXPECT_TRUE(r1 || (!eom && r8.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r9 = parseAnnounce(cursor);
  EXPECT_TRUE(r9 || (!eom && r9.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r10 = parseAnnounceOk(cursor);
  EXPECT_TRUE(r10 || (!eom && r10.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r11 = parseAnnounceError(cursor);
  EXPECT_TRUE(r11 || (!eom && r11.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r12 = parseAnnounceCancel(cursor);
  EXPECT_TRUE(r12 || (!eom && r12.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r13 = parseUnannounce(cursor);
  EXPECT_TRUE(r13 || (!eom && r13.error() == ErrorCode::PARSE_UNDERFLOW));

  skip(cursor, 1);
  auto r14 = parseGoaway(cursor);
  EXPECT_TRUE(r14 || (!eom && r14.error() == ErrorCode::PARSE_UNDERFLOW));

  auto res = parseStreamHeader(cursor, FrameType::STREAM_HEADER_TRACK);
  EXPECT_TRUE(res || (!eom && res.error() == ErrorCode::PARSE_UNDERFLOW));
  if (!res) {
    throw TestUnderflow();
  }
  auto r15 = parseMultiObjectHeader(
      cursor, FrameType::STREAM_HEADER_TRACK, res.value());
  EXPECT_TRUE(r15 || (!eom && r15.error() == ErrorCode::PARSE_UNDERFLOW));
  skip(cursor, 1);
}
} // namespace

TEST(SerializeAndParse, All) {
  auto allMsgs = moxygen::test::writeAllMessages();
  folly::io::Cursor cursor(allMsgs.get());
  parseAll(cursor, true);
}

TEST(SerializeAndParse, ParseObjectHeader) {
  // Test OBJECT_STREAM with random payload
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeObject(
      writeBuf,
      {11, // subscribeID
       22, // trackAlias
       33, // group
       44, // id
       55, // sendOrder
       ForwardPreference::Object,
       ObjectStatus::NORMAL,
       4},
      folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseFrameType(cursor), FrameType::OBJECT_STREAM);
  auto parseResult = parseObjectHeader(cursor, FrameType::OBJECT_STREAM);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->subscribeID, 11);
  EXPECT_EQ(parseResult->trackAlias, 22);
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->sendOrder, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);

  // Test OBJECT_DATAGRAM with ObjectStatus::OBJECT_NOT_EXIST
  writeBuf = folly::IOBufQueue(folly::IOBufQueue::cacheChainLength());
  result = writeObject(
      writeBuf,
      {11, // subscribeID
       22, // trackAlias
       33, // group
       44, // id
       55, // sendOrder
       ForwardPreference::Datagram,
       ObjectStatus::OBJECT_NOT_EXIST,
       0},
      nullptr);
  EXPECT_TRUE(result.hasValue());
  serialized = writeBuf.move();
  cursor = folly::io::Cursor(serialized.get());

  EXPECT_EQ(parseFrameType(cursor), FrameType::OBJECT_DATAGRAM);
  parseResult = parseObjectHeader(cursor, FrameType::OBJECT_DATAGRAM);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->subscribeID, 11);
  EXPECT_EQ(parseResult->trackAlias, 22);
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->sendOrder, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST(SerializeAndParse, ParseStreamHeader) {
  ObjectHeader expectedObjectHeader = {
      11, // subscribeID
      22, // trackAlias
      33, // group
      44, // id
      55, // sendOrder
      ForwardPreference::Track,
      ObjectStatus::NORMAL,
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeStreamHeader(writeBuf, expectedObjectHeader);
  EXPECT_TRUE(result.hasValue());
  result = writeObject(
      writeBuf, expectedObjectHeader, folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseFrameType(cursor), FrameType::STREAM_HEADER_TRACK);
  auto parseStreamHeaderResult =
      parseStreamHeader(cursor, FrameType::STREAM_HEADER_TRACK);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult = parseMultiObjectHeader(
      cursor, FrameType::STREAM_HEADER_TRACK, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->subscribeID, 11);
  EXPECT_EQ(parseResult->trackAlias, 22);
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->sendOrder, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);

  // Test ObjectStatus::OBJECT_NOT_EXIST
  expectedObjectHeader.status = ObjectStatus::OBJECT_NOT_EXIST;
  writeBuf = folly::IOBufQueue(folly::IOBufQueue::cacheChainLength());
  result = writeStreamHeader(writeBuf, expectedObjectHeader);
  EXPECT_TRUE(result.hasValue());
  result = writeObject(writeBuf, expectedObjectHeader, nullptr);
  EXPECT_TRUE(result.hasValue());
  serialized = writeBuf.move();
  cursor = folly::io::Cursor(serialized.get());

  EXPECT_EQ(parseFrameType(cursor), FrameType::STREAM_HEADER_TRACK);
  parseStreamHeaderResult =
      parseStreamHeader(cursor, FrameType::STREAM_HEADER_TRACK);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  parseResult = parseMultiObjectHeader(
      cursor, FrameType::STREAM_HEADER_TRACK, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->subscribeID, 11);
  EXPECT_EQ(parseResult->trackAlias, 22);
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->sendOrder, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
}

TEST(Underflows, All) {
  auto allMsgs = moxygen::test::writeAllMessages();
  allMsgs->coalesce();
  auto len = allMsgs->computeChainDataLength();
  for (size_t i = 0; i < len; i++) {
    auto toParse = allMsgs->clone();
    toParse->trimEnd(len - 1 - i);
    folly::io::Cursor cursor(toParse.get());
    try {
      parseAll(cursor, i == len - 1);
    } catch (const TestUnderflow&) {
      // expected
    }
  }
}

/* Test cases to add
 *
 * parseStreamHeader (group)
 * parseMultiObjectHeader (group)
 * Location relativeNext -- removed in draft-04
 * content-exists = true
 * retry track alias
 * write errors
 * write datagram
 * string ify and operator <<
 */
