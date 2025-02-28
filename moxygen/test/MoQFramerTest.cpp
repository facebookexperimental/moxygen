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
  auto r4a = parseMaxSubscribeId(cursor, frameLength(cursor));
  testUnderflowResult(r4a);

  skip(cursor, 1);
  auto r4b = parseSubscribesBlocked(cursor, frameLength(cursor));
  testUnderflowResult(r4b);

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

  skip(cursor, 1);
  auto r9a = parseSubscribeAnnounces(cursor, frameLength(cursor));
  testUnderflowResult(r9a);

  skip(cursor, 1);
  auto r10a = parseSubscribeAnnouncesOk(cursor, frameLength(cursor));
  testUnderflowResult(r10a);

  skip(cursor, 1);
  auto r11a = parseSubscribeAnnouncesError(cursor, frameLength(cursor));
  testUnderflowResult(r11a);

  skip(cursor, 1);
  auto r13a = parseUnsubscribeAnnounces(cursor, frameLength(cursor));
  testUnderflowResult(r13a);

  skip(cursor, 1);
  auto r16 = parseFetch(cursor, frameLength(cursor));
  testUnderflowResult(r16);

  skip(cursor, 1);
  auto r17 = parseFetchCancel(cursor, frameLength(cursor));
  testUnderflowResult(r17);

  skip(cursor, 1);
  auto r18 = parseFetchOk(cursor, frameLength(cursor));
  testUnderflowResult(r18);

  skip(cursor, 1);
  auto r19 = parseFetchError(cursor, frameLength(cursor));
  testUnderflowResult(r19);

  skip(cursor, 1);
  auto res = parseSubgroupHeader(cursor);
  testUnderflowResult(res);
  EXPECT_EQ(res.value().group, 2);

  auto r15 = parseSubgroupObjectHeader(cursor, res.value());
  testUnderflowResult(r15);
  EXPECT_EQ(r15.value().id, 4);
  skip(cursor, *r15.value().length);

  auto r20 = parseSubgroupObjectHeader(cursor, res.value());
  testUnderflowResult(r20);
  EXPECT_EQ(r20.value().status, ObjectStatus::END_OF_TRACK_AND_GROUP);

  skip(cursor, 1);
  auto r21 = parseFetchHeader(cursor);
  testUnderflowResult(r21);
  EXPECT_EQ(r21.value(), SubscribeID(1));

  ObjectHeader obj;
  obj.trackIdentifier = r21.value();
  auto r22 = parseFetchObjectHeader(cursor, obj);
  testUnderflowResult(r22);
  EXPECT_EQ(r22.value().id, 4);
  skip(cursor, *r22.value().length);

  auto r23 = parseFetchObjectHeader(cursor, obj);
  testUnderflowResult(r23);
  EXPECT_EQ(r23.value().status, ObjectStatus::END_OF_TRACK_AND_GROUP);

  skip(cursor, 1);
  size_t datagramLength = 5;
  auto r24 = parseDatagramObjectHeader(
      cursor, StreamType::OBJECT_DATAGRAM_STATUS, datagramLength);
  testUnderflowResult(r24);
  EXPECT_EQ(r24.value().status, ObjectStatus::OBJECT_NOT_EXIST);

  skip(cursor, 1);
  EXPECT_EQ(datagramLength, 0);
  datagramLength = cursor.totalLength();
  auto r25 = parseDatagramObjectHeader(
      cursor, StreamType::OBJECT_DATAGRAM, datagramLength);
  testUnderflowResult(r25);
  EXPECT_EQ(r25.value().id, 4);
  skip(cursor, *r25.value().length);
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
  auto result = writeDatagramObject(
      writeBuf,
      {TrackAlias(22), // trackAlias
       33,             // group
       0,              // subgroup
       44,             // id
       55,             // priority
       ObjectStatus::OBJECT_NOT_EXIST,
       0},
      nullptr);
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::OBJECT_DATAGRAM_STATUS);
  auto length = cursor.totalLength();
  auto parseResult = parseDatagramObjectHeader(
      cursor, StreamType::OBJECT_DATAGRAM_STATUS, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST(SerializeAndParse, ParseDatagramNormal) {
  // Test OBJECT_DATAGRAM
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeDatagramObject(
      writeBuf,
      {TrackAlias(22), // trackAlias
       33,             // group
       0,              // subgroup
       44,             // id
       55,             // priority
       ObjectStatus::NORMAL,
       8},
      folly::IOBuf::copyBuffer("datagram"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::OBJECT_DATAGRAM);
  auto length = cursor.totalLength();
  auto parseResult =
      parseDatagramObjectHeader(cursor, StreamType::OBJECT_DATAGRAM, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(parseResult->length, 8);
}

TEST(SerializeAndParse, ZeroLengthNormal) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeDatagramObject(
      writeBuf,
      {TrackAlias(22), // trackAlias
       33,             // group
       0,              // subgroup
       44,             // id
       55,             // priority
       ObjectStatus::NORMAL,
       0},
      nullptr);
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::OBJECT_DATAGRAM_STATUS);
  auto length = cursor.totalLength();
  auto parseResult = parseDatagramObjectHeader(
      cursor, StreamType::OBJECT_DATAGRAM_STATUS, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 0);
}

TEST(SerializeAndParse, ParseStreamHeader) {
  ObjectHeader expectedObjectHeader = {
      TrackAlias(22), // trackAlias
      33,             // group
      0,              // subgroup
      44,             // id
      55,             // priority
      ObjectStatus::NORMAL,
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeSubgroupHeader(writeBuf, expectedObjectHeader);
  EXPECT_TRUE(result.hasValue());
  result = writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      expectedObjectHeader,
      folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());

  // Test ObjectStatus::OBJECT_NOT_EXIST
  expectedObjectHeader.status = ObjectStatus::OBJECT_NOT_EXIST;
  expectedObjectHeader.length = 0;
  result = writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER, expectedObjectHeader, nullptr);
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::SUBGROUP_HEADER);
  auto parseStreamHeaderResult = parseSubgroupHeader(cursor);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult =
      parseSubgroupObjectHeader(cursor, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);

  parseResult = parseSubgroupObjectHeader(cursor, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST(SerializeAndParse, ParseFetchHeader) {
  ObjectHeader expectedObjectHeader = {
      SubscribeID(22), // subID
      33,              // group
      0,               // subgroup
      44,              // id
      55,              // priority
      ObjectStatus::NORMAL,
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeFetchHeader(
      writeBuf, std::get<SubscribeID>(expectedObjectHeader.trackIdentifier));
  EXPECT_TRUE(result.hasValue());
  result = writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      expectedObjectHeader,
      folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());

  // Test ObjectStatus::OBJECT_NOT_EXIST
  expectedObjectHeader.status = ObjectStatus::OBJECT_NOT_EXIST;
  expectedObjectHeader.length = 0;
  result = writeStreamObject(
      writeBuf, StreamType::FETCH_HEADER, expectedObjectHeader, nullptr);
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::FETCH_HEADER);
  auto parseStreamHeaderResult = parseFetchHeader(cursor);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  ObjectHeader headerTemplate;
  headerTemplate.trackIdentifier = *parseStreamHeaderResult;
  auto parseResult = parseFetchObjectHeader(cursor, headerTemplate);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(
      std::get<SubscribeID>(parseResult->trackIdentifier), SubscribeID(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);

  parseResult = parseFetchObjectHeader(cursor, headerTemplate);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(
      std::get<SubscribeID>(parseResult->trackIdentifier), SubscribeID(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST(SerializeAndParse, ParseClientSetupForMaxSubscribeId) {
  // Test different values for MAX_SUBSCRIBE_ID
  const std::vector<uint64_t> kTestMaxSubscribeIds = {
      0,
      quic::kOneByteLimit,
      quic::kOneByteLimit + 1,
      quic::kTwoByteLimit,
      quic::kTwoByteLimit + 1,
      quic::kFourByteLimit,
      quic::kFourByteLimit + 1,
      quic::kEightByteLimit};
  for (auto maxSubscribeId : kTestMaxSubscribeIds) {
    auto clientSetup = ClientSetup{
        .supportedVersions = {kVersionDraftCurrent},
        .params =
            {{{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
               .asString = "",
               .asUint64 = maxSubscribeId}}},
    };

    folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
    auto result = writeClientSetup(writeBuf, clientSetup);
    EXPECT_TRUE(result.hasValue())
        << "Failed to write client setup for maxSubscribeId:" << maxSubscribeId;
    auto buffer = writeBuf.move();
    auto cursor = folly::io::Cursor(buffer.get());
    skip(cursor, 2);
    auto parseClientSetupResult = parseClientSetup(cursor, frameLength(cursor));
    EXPECT_TRUE(parseClientSetupResult.hasValue())
        << "Failed to parse client setup for maxSubscribeId:" << maxSubscribeId;
    EXPECT_EQ(parseClientSetupResult->supportedVersions.size(), 1);
    EXPECT_EQ(
        parseClientSetupResult->supportedVersions[0], kVersionDraftCurrent);
    EXPECT_EQ(parseClientSetupResult->params.size(), 1);
    EXPECT_EQ(
        parseClientSetupResult->params[0].key,
        folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID));
    EXPECT_EQ(parseClientSetupResult->params[0].asUint64, maxSubscribeId);
  }
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

TEST(FramerTests, SingleObjectStream) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeSingleObjectStream(
      writeBuf,
      {TrackAlias(22), // trackAlias
       33,             // group
       0,              // subgroup
       44,             // id
       55,             // priority
       ObjectStatus::NORMAL,
       4},
      folly::IOBuf::copyBuffer("abcd"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::SUBGROUP_HEADER);
  auto parseStreamHeaderResult = parseSubgroupHeader(cursor);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult =
      parseSubgroupObjectHeader(cursor, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);
}

/* Test cases to add
 *
 * parseStreamHeader (group)
 * parseSubgroupObjectHeader (group)
 * parseFetchObjectHeader
 * Location relativeNext -- removed in draft-04
 * content-exists = true
 * retry track alias
 * write errors
 * write datagram
 * string ify and operator <<
 */
