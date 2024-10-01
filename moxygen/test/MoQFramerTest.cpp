/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include <folly/logging/xlog.h>
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

StreamType parseStreamType(folly::io::Cursor& cursor) {
  auto frameType = quic::decodeQuicInteger(cursor);
  if (!frameType) {
    throw std::runtime_error("Failed to decode frame type");
  }
  return StreamType(frameType->first);
}

void skip(folly::io::Cursor& cursor, size_t i) {
  if (!cursor.canAdvance(i)) {
    throw TestUnderflow();
  }
  cursor.skip(i);
}

template <class T>
void testUnderflowResult(folly::Expected<T, ErrorCode> result) {
  EXPECT_TRUE(result || result.error() == ErrorCode::PARSE_UNDERFLOW);
  if (!result) {
    throw TestUnderflow();
  }
}

size_t frameLength(folly::io::Cursor& cursor) {
  auto res = quic::decodeQuicInteger(cursor);
  if (res && cursor.canAdvance(res->first)) {
    return res->first;
  } else {
    throw TestUnderflow();
  }
}

void parseAll(folly::io::Cursor& cursor, bool eom) {
  skip(cursor, 2);
  auto r1 = parseClientSetup(cursor, frameLength(cursor));
  testUnderflowResult(r1);

  skip(cursor, 2);
  auto r2 = parseServerSetup(cursor, frameLength(cursor));
  testUnderflowResult(r2);

  skip(cursor, 1);
  auto r3 = parseSubscribeRequest(cursor, frameLength(cursor));
  testUnderflowResult(r3);

  skip(cursor, 1);
  auto r3a = parseSubscribeUpdate(cursor, frameLength(cursor));
  testUnderflowResult(r3a);

  skip(cursor, 1);
  auto r4 = parseSubscribeOk(cursor, frameLength(cursor));
  testUnderflowResult(r4);

  skip(cursor, 1);
  auto r5 = parseSubscribeError(cursor, frameLength(cursor));
  testUnderflowResult(r5);

  skip(cursor, 1);
  auto r6 = parseUnsubscribe(cursor, frameLength(cursor));
  testUnderflowResult(r6);

  skip(cursor, 1);
  auto r7 = parseSubscribeDone(cursor, frameLength(cursor));
  testUnderflowResult(r7);

  skip(cursor, 1);
  auto r8 = parseSubscribeDone(cursor, frameLength(cursor));
  testUnderflowResult(r8);

  skip(cursor, 1);
  auto r9 = parseAnnounce(cursor, frameLength(cursor));
  testUnderflowResult(r9);

  skip(cursor, 1);
  auto r10 = parseAnnounceOk(cursor, frameLength(cursor));
  testUnderflowResult(r10);

  skip(cursor, 1);
  auto r11 = parseAnnounceError(cursor, frameLength(cursor));
  testUnderflowResult(r11);

  skip(cursor, 1);
  auto r12 = parseAnnounceCancel(cursor, frameLength(cursor));
  testUnderflowResult(r12);

  skip(cursor, 1);
  auto r13 = parseUnannounce(cursor, frameLength(cursor));
  testUnderflowResult(r13);

  skip(cursor, 1);
  auto r14a = parseTrackStatusRequest(cursor, frameLength(cursor));
  testUnderflowResult(r14a);

  skip(cursor, 1);
  auto r14b = parseTrackStatus(cursor, frameLength(cursor));
  testUnderflowResult(r14b);

  skip(cursor, 1);
  auto r14 = parseGoaway(cursor, frameLength(cursor));
  testUnderflowResult(r14);

  auto res = parseStreamHeader(cursor, StreamType::STREAM_HEADER_TRACK);
  testUnderflowResult(res);

  auto r15 = parseMultiObjectHeader(
      cursor, StreamType::STREAM_HEADER_TRACK, res.value());
  testUnderflowResult(r15);
  skip(cursor, 1);
}
} // namespace

TEST(SerializeAndParse, All) {
  auto allMsgs = moxygen::test::writeAllMessages();
  folly::io::Cursor cursor(allMsgs.get());
  parseAll(cursor, true);
}

TEST(SerializeAndParse, ParseObjectHeader) {
  // Test OBJECT_DATAGRAM with ObjectStatus::OBJECT_NOT_EXIST
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeObject(
      writeBuf,
      {11, // subscribeID
       22, // trackAlias
       33, // group
       0,  // subgroup
       44, // id
       55, // priority
       ForwardPreference::Datagram,
       ObjectStatus::OBJECT_NOT_EXIST,
       0},
      nullptr);
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::OBJECT_DATAGRAM);
  auto length = cursor.totalLength();
  auto parseResult = parseObjectHeader(cursor, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->subscribeID, 11);
  EXPECT_EQ(parseResult->trackAlias, 22);
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST(SerializeAndParse, ParseStreamHeader) {
  ObjectHeader expectedObjectHeader = {
      11, // subscribeID
      22, // trackAlias
      33, // group
      0,  // subgroup
      44, // id
      55, // priority
      ForwardPreference::Track,
      ObjectStatus::NORMAL,
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeStreamHeader(writeBuf, expectedObjectHeader);
  EXPECT_TRUE(result.hasValue());
  result = writeObject(
      writeBuf, expectedObjectHeader, folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());

  // Test ObjectStatus::OBJECT_NOT_EXIST
  expectedObjectHeader.status = ObjectStatus::OBJECT_NOT_EXIST;
  expectedObjectHeader.length = 0;
  result = writeObject(writeBuf, expectedObjectHeader, nullptr);
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::STREAM_HEADER_TRACK);
  auto parseStreamHeaderResult =
      parseStreamHeader(cursor, StreamType::STREAM_HEADER_TRACK);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult = parseMultiObjectHeader(
      cursor, StreamType::STREAM_HEADER_TRACK, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->subscribeID, 11);
  EXPECT_EQ(parseResult->trackAlias, 22);
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);

  parseResult = parseMultiObjectHeader(
      cursor, StreamType::STREAM_HEADER_TRACK, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->subscribeID, 11);
  EXPECT_EQ(parseResult->trackAlias, 22);
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
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
