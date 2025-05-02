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

// The parameter is the MoQ version
class MoQFramerTest : public ::testing::TestWithParam<uint64_t> {
 public:
  void SetUp() override {
    parser_.initializeVersion(GetParam());
    writer_.initializeVersion(GetParam());
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
    if (getDraftMajorVersion(GetParam()) >= 11) {
      if (!cursor.canAdvance(2)) {
        throw TestUnderflow();
      }
      size_t res = cursor.readBE<uint16_t>();
      if (!cursor.canAdvance(res)) {
        throw TestUnderflow();
      }
      return res;
    } else {
      auto res = quic::decodeQuicInteger(cursor);
      if (res && cursor.canAdvance(res->first)) {
        return res->first;
      } else {
        throw TestUnderflow();
      }
    }
  }

  void parseAll(folly::io::Cursor& cursor, bool eom) {
    skip(cursor, (getDraftMajorVersion(GetParam()) >= 11) ? 1 : 2);
    auto r1 = parseClientSetup(cursor, frameLength(cursor));
    testUnderflowResult(r1);

    skip(cursor, (getDraftMajorVersion(GetParam()) >= 11) ? 1 : 2);
    auto r2 = parseServerSetup(cursor, frameLength(cursor));
    testUnderflowResult(r2);

    skip(cursor, 1);
    auto r3 = parser_.parseSubscribeRequest(cursor, frameLength(cursor));
    testUnderflowResult(r3);

    skip(cursor, 1);
    auto r3a = parser_.parseSubscribeUpdate(cursor, frameLength(cursor));
    testUnderflowResult(r3a);

    skip(cursor, 1);
    auto r4 = parser_.parseSubscribeOk(cursor, frameLength(cursor));
    testUnderflowResult(r4);

    skip(cursor, 1);
    auto r4a = parser_.parseMaxSubscribeId(cursor, frameLength(cursor));
    testUnderflowResult(r4a);

    skip(cursor, 1);
    auto r4b = parser_.parseSubscribesBlocked(cursor, frameLength(cursor));
    testUnderflowResult(r4b);

    skip(cursor, 1);
    auto r5 = parser_.parseSubscribeError(cursor, frameLength(cursor));
    testUnderflowResult(r5);

    skip(cursor, 1);
    auto r6 = parser_.parseUnsubscribe(cursor, frameLength(cursor));
    testUnderflowResult(r6);

    skip(cursor, 1);
    auto r7 = parser_.parseSubscribeDone(cursor, frameLength(cursor));
    testUnderflowResult(r7);

    skip(cursor, 1);
    auto r9 = parser_.parseAnnounce(cursor, frameLength(cursor));
    testUnderflowResult(r9);

    skip(cursor, 1);
    auto r10 = parser_.parseAnnounceOk(cursor, frameLength(cursor));
    testUnderflowResult(r10);

    skip(cursor, 1);
    auto r11 = parser_.parseAnnounceError(cursor, frameLength(cursor));
    testUnderflowResult(r11);

    skip(cursor, 1);
    auto r12 = parser_.parseAnnounceCancel(cursor, frameLength(cursor));
    testUnderflowResult(r12);

    skip(cursor, 1);
    auto r13 = parser_.parseUnannounce(cursor, frameLength(cursor));
    testUnderflowResult(r13);

    skip(cursor, 1);
    auto r14a = parser_.parseTrackStatusRequest(cursor, frameLength(cursor));
    testUnderflowResult(r14a);

    skip(cursor, 1);
    auto r14b = parser_.parseTrackStatus(cursor, frameLength(cursor));
    testUnderflowResult(r14b);

    skip(cursor, 1);
    auto r14 = parser_.parseGoaway(cursor, frameLength(cursor));
    testUnderflowResult(r14);

    skip(cursor, 1);
    auto r9a = parser_.parseSubscribeAnnounces(cursor, frameLength(cursor));
    testUnderflowResult(r9a);

    skip(cursor, 1);
    auto r10a = parser_.parseSubscribeAnnouncesOk(cursor, frameLength(cursor));
    testUnderflowResult(r10a);

    skip(cursor, 1);
    auto r11a =
        parser_.parseSubscribeAnnouncesError(cursor, frameLength(cursor));
    testUnderflowResult(r11a);

    skip(cursor, 1);
    auto r13a = parser_.parseUnsubscribeAnnounces(cursor, frameLength(cursor));
    testUnderflowResult(r13a);

    skip(cursor, 1);
    auto r16 = parser_.parseFetch(cursor, frameLength(cursor));
    testUnderflowResult(r16);

    skip(cursor, 1);
    auto r17 = parser_.parseFetchCancel(cursor, frameLength(cursor));
    testUnderflowResult(r17);

    skip(cursor, 1);
    auto r18 = parser_.parseFetchOk(cursor, frameLength(cursor));
    testUnderflowResult(r18);

    skip(cursor, 1);
    auto r19 = parser_.parseFetchError(cursor, frameLength(cursor));
    testUnderflowResult(r19);

    skip(cursor, 1);
    auto res = parser_.parseSubgroupHeader(cursor);
    testUnderflowResult(res);
    EXPECT_EQ(res.value().group, 2);

    auto r15 = parser_.parseSubgroupObjectHeader(cursor, res.value());
    testUnderflowResult(r15);
    EXPECT_EQ(r15.value().id, 4);
    skip(cursor, *r15.value().length);

    auto r15a = parser_.parseSubgroupObjectHeader(cursor, res.value());
    testUnderflowResult(r15a);
    EXPECT_EQ(r15a.value().id, 5);
    EXPECT_EQ(r15a.value().extensions, test::getTestExtensions());
    skip(cursor, *r15a.value().length);

    auto r20 = parser_.parseSubgroupObjectHeader(cursor, res.value());
    testUnderflowResult(r20);
    EXPECT_EQ(r20.value().status, ObjectStatus::OBJECT_NOT_EXIST);

    auto r20a = parser_.parseSubgroupObjectHeader(cursor, res.value());
    testUnderflowResult(r20a);
    EXPECT_EQ(r20a.value().extensions, test::getTestExtensions());
    EXPECT_EQ(r20a.value().status, ObjectStatus::END_OF_TRACK_AND_GROUP);

    skip(cursor, 1);
    auto r21 = parser_.parseFetchHeader(cursor);
    testUnderflowResult(r21);
    EXPECT_EQ(r21.value(), SubscribeID(1));

    ObjectHeader obj;
    obj.trackIdentifier = r21.value();
    auto r22 = parser_.parseFetchObjectHeader(cursor, obj);
    testUnderflowResult(r22);
    EXPECT_EQ(r22.value().id, 4);
    skip(cursor, *r22.value().length);

    auto r22a = parser_.parseFetchObjectHeader(cursor, obj);
    testUnderflowResult(r22a);
    EXPECT_EQ(r22a.value().id, 5);
    EXPECT_EQ(r22a.value().extensions, test::getTestExtensions());
    skip(cursor, *r22a.value().length);

    auto r23 = parser_.parseFetchObjectHeader(cursor, obj);
    testUnderflowResult(r23);
    EXPECT_EQ(r23.value().status, ObjectStatus::END_OF_GROUP);

    auto r23a = parser_.parseFetchObjectHeader(cursor, obj);
    testUnderflowResult(r23a);
    EXPECT_EQ(r23a.value().extensions, test::getTestExtensions());
    EXPECT_EQ(r23a.value().status, ObjectStatus::END_OF_GROUP);

    skip(cursor, 1);
    size_t datagramLength = std::min(6lu, cursor.totalLength());
    auto r24 = parser_.parseDatagramObjectHeader(
        cursor, StreamType::OBJECT_DATAGRAM_STATUS, datagramLength);
    testUnderflowResult(r24);
    EXPECT_EQ(r24.value().status, ObjectStatus::OBJECT_NOT_EXIST);

    skip(cursor, 1);
    EXPECT_EQ(datagramLength, 0);
    datagramLength = std::min(13lu, cursor.totalLength());
    auto r24a = parser_.parseDatagramObjectHeader(
        cursor, StreamType::OBJECT_DATAGRAM_STATUS, datagramLength);
    testUnderflowResult(r24a);
    EXPECT_EQ(r24a.value().extensions, test::getTestExtensions());
    EXPECT_EQ(r24a.value().status, ObjectStatus::END_OF_GROUP);

    skip(cursor, 1);
    EXPECT_EQ(datagramLength, 0);
    datagramLength = std::min(16lu, cursor.totalLength());
    bool underflowPayload = datagramLength < 16lu;
    auto r25 = parser_.parseDatagramObjectHeader(
        cursor, StreamType::OBJECT_DATAGRAM, datagramLength);
    testUnderflowResult(r25);
    EXPECT_EQ(r25.value().id, 0);
    if (underflowPayload) {
      throw TestUnderflow();
    }
    EXPECT_EQ(datagramLength, *r25.value().length);
    skip(cursor, *r25.value().length);

    skip(cursor, 1);
    datagramLength = cursor.totalLength();
    underflowPayload = datagramLength < 27lu;
    auto r25a = parser_.parseDatagramObjectHeader(
        cursor, StreamType::OBJECT_DATAGRAM, datagramLength);
    testUnderflowResult(r25a);
    EXPECT_EQ(r25a.value().id, 1);
    EXPECT_EQ(r25a.value().extensions, test::getTestExtensions());
    if (underflowPayload) {
      throw TestUnderflow();
    }
    EXPECT_EQ(r25a.value().length, 15);
    skip(cursor, *r25a.value().length);
  }

 protected:
  MoQFrameParser parser_;
  MoQFrameWriter writer_;
};

} // namespace

TEST_P(MoQFramerTest, SerializeAndParseAll) {
  auto allMsgs = moxygen::test::writeAllMessages(writer_, GetParam());
  folly::io::Cursor cursor(allMsgs.get());
  parseAll(cursor, true);
}

TEST_P(MoQFramerTest, ParseObjectHeader) {
  // Test OBJECT_DATAGRAM with ObjectStatus::OBJECT_NOT_EXIST
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeDatagramObject(
      writeBuf,
      {TrackAlias(22), // trackAlias
       33,             // group
       0,              // subgroup
       44,             // id
       55,             // priority
       ObjectStatus::OBJECT_NOT_EXIST,
       noExtensions(),
       0},
      nullptr);
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::OBJECT_DATAGRAM_STATUS);
  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(
      cursor, StreamType::OBJECT_DATAGRAM_STATUS, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, ParseDatagramNormal) {
  // Test OBJECT_DATAGRAM
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeDatagramObject(
      writeBuf,
      {TrackAlias(22), // trackAlias
       33,             // group
       0,              // subgroup
       44,             // id
       55,             // priority
       ObjectStatus::NORMAL,
       noExtensions(),
       8},
      folly::IOBuf::copyBuffer("datagram"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::OBJECT_DATAGRAM);
  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(
      cursor, StreamType::OBJECT_DATAGRAM, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(parseResult->length, 8);
}

TEST_P(MoQFramerTest, ZeroLengthNormal) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeDatagramObject(
      writeBuf,
      {TrackAlias(22), // trackAlias
       33,             // group
       0,              // subgroup
       44,             // id
       55,             // priority
       ObjectStatus::NORMAL,
       noExtensions(),
       0},
      nullptr);
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::OBJECT_DATAGRAM_STATUS);
  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(
      cursor, StreamType::OBJECT_DATAGRAM_STATUS, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 0);
}

TEST_P(MoQFramerTest, ParseStreamHeader) {
  ObjectHeader expectedObjectHeader = {
      TrackAlias(22), // trackAlias
      33,             // group
      0,              // subgroup
      44,             // id
      55,             // priority
      ObjectStatus::NORMAL,
      noExtensions(),
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeSubgroupHeader(writeBuf, expectedObjectHeader);
  EXPECT_TRUE(result.hasValue());
  result = writer_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      expectedObjectHeader,
      folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());

  // Test ObjectStatus::OBJECT_NOT_EXIST
  expectedObjectHeader.status = ObjectStatus::OBJECT_NOT_EXIST;
  expectedObjectHeader.length = 0;
  result = writer_.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER, expectedObjectHeader, nullptr);
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::SUBGROUP_HEADER);
  auto parseStreamHeaderResult = parser_.parseSubgroupHeader(cursor);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult =
      parser_.parseSubgroupObjectHeader(cursor, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);

  parseResult =
      parser_.parseSubgroupObjectHeader(cursor, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, ParseFetchHeader) {
  ObjectHeader expectedObjectHeader = {
      SubscribeID(22), // subID
      33,              // group
      0,               // subgroup
      44,              // id
      55,              // priority
      ObjectStatus::NORMAL,
      noExtensions(),
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeFetchHeader(
      writeBuf, std::get<SubscribeID>(expectedObjectHeader.trackIdentifier));
  EXPECT_TRUE(result.hasValue());
  result = writer_.writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      expectedObjectHeader,
      folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());

  // Test ObjectStatus::OBJECT_NOT_EXIST
  expectedObjectHeader.status = ObjectStatus::OBJECT_NOT_EXIST;
  expectedObjectHeader.length = 0;
  result = writer_.writeStreamObject(
      writeBuf, StreamType::FETCH_HEADER, expectedObjectHeader, nullptr);
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::FETCH_HEADER);
  auto parseStreamHeaderResult = parser_.parseFetchHeader(cursor);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  ObjectHeader headerTemplate;
  headerTemplate.trackIdentifier = *parseStreamHeaderResult;
  auto parseResult = parser_.parseFetchObjectHeader(cursor, headerTemplate);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(
      std::get<SubscribeID>(parseResult->trackIdentifier), SubscribeID(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);

  parseResult = parser_.parseFetchObjectHeader(cursor, headerTemplate);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(
      std::get<SubscribeID>(parseResult->trackIdentifier), SubscribeID(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, ParseClientSetupForMaxSubscribeId) {
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
    auto result = writeClientSetup(writeBuf, clientSetup, GetParam());
    EXPECT_TRUE(result.hasValue())
        << "Failed to write client setup for maxSubscribeId:" << maxSubscribeId;
    auto buffer = writeBuf.move();
    auto cursor = folly::io::Cursor(buffer.get());
    auto frameType = quic::decodeQuicInteger(cursor);
    uint64_t expectedFrameType = (getDraftMajorVersion(GetParam()) >= 11)
        ? folly::to_underlying(FrameType::CLIENT_SETUP)
        : folly::to_underlying(FrameType::LEGACY_CLIENT_SETUP);
    EXPECT_EQ(frameType->first, expectedFrameType);
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

TEST_P(MoQFramerTest, All) {
  auto allMsgs = moxygen::test::writeAllMessages(writer_, GetParam());
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

TEST_P(MoQFramerTest, SingleObjectStream) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeSingleObjectStream(
      writeBuf,
      ObjectHeader(
          TrackAlias(22), // trackAlias
          33,             // group
          0,              // subgroup
          44,             // id
          55,             // priority
          4),
      folly::IOBuf::copyBuffer("abcd"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(parseStreamType(cursor), StreamType::SUBGROUP_HEADER);
  auto parseStreamHeaderResult = parser_.parseSubgroupHeader(cursor);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult =
      parser_.parseSubgroupObjectHeader(cursor, *parseStreamHeaderResult);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);
}

TEST_P(MoQFramerTest, ParseTrackStatusRequest) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  TrackStatusRequest tsr;
  tsr.fullTrackName = FullTrackName({TrackNamespace({"hello"}), "world"});
  std::vector<TrackRequestParameter> params;
  if (getDraftMajorVersion(GetParam()) >= 11) {
    // Add some parameters to the TrackStatusRequest.
    params.push_back(
        {folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
         "stampolli",
         0});
    params.push_back(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         "",
         999});
  }
  tsr.params = params;
  auto writeResult = writer_.writeTrackStatusRequest(writeBuf, tsr);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::decodeQuicInteger(cursor);
  EXPECT_EQ(
      frameType->first, folly::to_underlying(FrameType::TRACK_STATUS_REQUEST));
  auto parseResult =
      parser_.parseTrackStatusRequest(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->fullTrackName.trackNamespace.size(), 1);
  EXPECT_EQ(parseResult->fullTrackName.trackNamespace[0], "hello");
  EXPECT_EQ(parseResult->fullTrackName.trackName, "world");
  if (getDraftMajorVersion(GetParam()) >= 11) {
    EXPECT_EQ(parseResult->params.size(), 2);
    EXPECT_EQ(
        parseResult->params[0].key,
        folly::to_underlying(TrackRequestParamKey::AUTHORIZATION));
    EXPECT_EQ(parseResult->params[0].asString, "stampolli");
    EXPECT_EQ(
        parseResult->params[1].key,
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT));
    EXPECT_EQ(parseResult->params[1].asUint64, 999);
  }
}

TEST_P(MoQFramerTest, ParseTrackStatus) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  TrackStatus trackStatus;
  trackStatus.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatus.statusCode = TrackStatusCode::IN_PROGRESS;
  trackStatus.latestGroupAndObject = AbsoluteLocation({19, 77});
  std::vector<TrackRequestParameter> params;
  if (getDraftMajorVersion(GetParam()) >= 11) {
    // Add some parameters to the TrackStatusRequest.
    params.push_back(
        {folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
         "stampolli",
         0});
    params.push_back(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         "",
         999});
  }
  trackStatus.params = params;
  auto writeResult = writer_.writeTrackStatus(writeBuf, trackStatus);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::TRACK_STATUS));
  auto parseResult = parser_.parseTrackStatus(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->fullTrackName.trackNamespace.size(), 1);
  EXPECT_EQ(parseResult->fullTrackName.trackNamespace[0], "hello");
  EXPECT_EQ(parseResult->fullTrackName.trackName, "world");
  EXPECT_EQ(parseResult->latestGroupAndObject->group, 19);
  EXPECT_EQ(parseResult->latestGroupAndObject->object, 77);
  EXPECT_EQ(parseResult->statusCode, TrackStatusCode::IN_PROGRESS);
  if (getDraftMajorVersion(GetParam()) >= 11) {
    EXPECT_EQ(parseResult->params.size(), 2);
    EXPECT_EQ(
        parseResult->params[0].key,
        folly::to_underlying(TrackRequestParamKey::AUTHORIZATION));
    EXPECT_EQ(parseResult->params[0].asString, "stampolli");
    EXPECT_EQ(
        parseResult->params[1].key,
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT));
    EXPECT_EQ(parseResult->params[1].asUint64, 999);
  }
}

INSTANTIATE_TEST_SUITE_P(
    MoQFramerTest,
    MoQFramerTest,
    ::testing::Values(
        kVersionDraft08,
        kVersionDraft09,
        kVersionDraft10,
        kVersionDraft11));

TEST(MoQFramerTestUtils, DraftMajorVersion) {
  EXPECT_EQ(getDraftMajorVersion(0xff080001), 0x8);
  EXPECT_EQ(getDraftMajorVersion(0xffff0001), 0xff);
  EXPECT_EQ(getDraftMajorVersion(0xff000008), 0x8);
  EXPECT_EQ(getDraftMajorVersion(0xff00ffff), 0xffff);
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
