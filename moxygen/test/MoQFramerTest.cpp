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

  size_t frameLength(folly::io::Cursor& cursor, bool checkAdvance = true) {
    if (getDraftMajorVersion(GetParam()) >= 11) {
      if (!cursor.canAdvance(2)) {
        throw TestUnderflow();
      }
      size_t res = cursor.readBE<uint16_t>();
      if (checkAdvance && !cursor.canAdvance(res)) {
        throw TestUnderflow();
      }
      return res;
    } else {
      auto res = quic::decodeQuicInteger(cursor);
      if (res && (!checkAdvance || cursor.canAdvance(res->first))) {
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
    auto r4a = parser_.parseMaxRequestID(cursor, frameLength(cursor));
    testUnderflowResult(r4a);

    skip(cursor, 1);
    auto r4b = parser_.parseRequestsBlocked(cursor, frameLength(cursor));
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
    auto res =
        parser_.parseSubgroupHeader(cursor, SubgroupIDFormat::Present, true);
    testUnderflowResult(res);
    EXPECT_EQ(res.value().group, 2);

    auto r15 = parser_.parseSubgroupObjectHeader(
        cursor, res.value(), SubgroupIDFormat::Present, true);
    testUnderflowResult(r15);
    EXPECT_EQ(r15.value().id, 4);
    skip(cursor, *r15.value().length);

    auto r15a = parser_.parseSubgroupObjectHeader(
        cursor, res.value(), SubgroupIDFormat::Present, true);
    testUnderflowResult(r15a);
    EXPECT_EQ(r15a.value().id, 5);
    EXPECT_EQ(r15a.value().extensions, test::getTestExtensions());
    skip(cursor, *r15a.value().length);

    auto r20 = parser_.parseSubgroupObjectHeader(
        cursor, res.value(), SubgroupIDFormat::Present, true);
    testUnderflowResult(r20);
    EXPECT_EQ(r20.value().status, ObjectStatus::OBJECT_NOT_EXIST);

    auto r20a = parser_.parseSubgroupObjectHeader(
        cursor, res.value(), SubgroupIDFormat::Present, true);
    testUnderflowResult(r20a);
    EXPECT_EQ(r20a.value().extensions, test::getTestExtensions());
    EXPECT_EQ(r20a.value().status, ObjectStatus::END_OF_TRACK);

    skip(cursor, 1);
    auto r21 = parser_.parseFetchHeader(cursor);
    testUnderflowResult(r21);
    EXPECT_EQ(r21.value(), RequestID(1));

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
  }

 protected:
  MoQFrameParser parser_;
  MoQFrameWriter writer_;

  ObjectHeader testUnderflowDatagramHelper(
      folly::IOBufQueue& writeBuf,
      bool isStatus,
      bool hasExtensions,
      uint64_t expectedPayloadLen);
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

  EXPECT_EQ(parseStreamType(cursor), getDatagramType(GetParam(), false, false));
  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(
      cursor, getDatagramType(GetParam(), false, false), length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(parseResult->length, 8);
}

ObjectHeader MoQFramerTest::testUnderflowDatagramHelper(
    folly::IOBufQueue& writeBuf,
    bool isStatus,
    bool hasExtensions,
    uint64_t expectedPayloadLen) {
  for (size_t i = 1; i <= writeBuf.chainLength(); ++i) {
    folly::io::Cursor cursor(writeBuf.front());
    auto datagramType = getDatagramType(GetParam(), isStatus, hasExtensions);
    auto decodedType = quic::decodeQuicInteger(cursor, i);
    EXPECT_TRUE(decodedType.has_value());
    EXPECT_EQ(decodedType->first, folly::to_underlying(datagramType));

    auto len = i - decodedType->second;
    auto result = parser_.parseDatagramObjectHeader(cursor, datagramType, len);
    if (i < writeBuf.chainLength()) {
      if (result.hasValue()) {
        EXPECT_TRUE(result.value().status == ObjectStatus::NORMAL);
        EXPECT_LT(*result.value().length, expectedPayloadLen);
      } else {
        EXPECT_TRUE(result.error() == ErrorCode::PARSE_UNDERFLOW);
      }
      continue;
    }
    if (hasExtensions) {
      EXPECT_EQ(result.value().extensions, test::getTestExtensions());
    }
    return *result;
  }
  return ObjectHeader();
}

TEST_P(MoQFramerTest, testParseDatagramObjectHeader1) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(TrackAlias(1), 2, 3, 4, 5, ObjectStatus::OBJECT_NOT_EXIST);
  writer_.writeDatagramObject(writeBuf, obj, nullptr);

  auto pobj = testUnderflowDatagramHelper(writeBuf, true, false, 0);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, testParseDatagramObjectHeader2) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(
      TrackAlias(1),
      2,
      3,
      4,
      5,
      ObjectStatus::END_OF_GROUP,
      test::getTestExtensions());
  writer_.writeDatagramObject(writeBuf, obj, nullptr);

  auto pobj = testUnderflowDatagramHelper(writeBuf, true, true, 0);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::END_OF_GROUP);
}

TEST_P(MoQFramerTest, testParseDatagramObjectHeader3) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(
      TrackAlias(1), 2, 3, 4, 5, ObjectStatus::NORMAL, noExtensions(), 11);
  writer_.writeDatagramObject(
      writeBuf, obj, folly::IOBuf::copyBuffer("hello world"));
  auto pobj = testUnderflowDatagramHelper(writeBuf, false, false, 11);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::NORMAL);
  EXPECT_EQ(pobj.length, 11);
}

TEST_P(MoQFramerTest, testParseDatagramObjectHeader4) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(
      TrackAlias(1),
      2,
      3,
      4,
      5,
      ObjectStatus::NORMAL,
      test::getTestExtensions(),
      11);
  writer_.writeDatagramObject(
      writeBuf, obj, folly::IOBuf::copyBuffer("hello world"));

  auto pobj = testUnderflowDatagramHelper(writeBuf, false, true, 11);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::NORMAL);
  EXPECT_EQ(pobj.length, 11);
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
  auto streamType =
      getSubgroupStreamType(GetParam(), SubgroupIDFormat::Zero, false);
  auto result = writer_.writeSubgroupHeader(
      writeBuf, expectedObjectHeader, SubgroupIDFormat::Zero, false);
  EXPECT_TRUE(result.hasValue());
  result = writer_.writeStreamObject(
      writeBuf,
      streamType,
      expectedObjectHeader,
      folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());

  // Test ObjectStatus::OBJECT_NOT_EXIST
  expectedObjectHeader.status = ObjectStatus::OBJECT_NOT_EXIST;
  expectedObjectHeader.length = 0;
  result = writer_.writeStreamObject(
      writeBuf, streamType, expectedObjectHeader, nullptr);
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  EXPECT_EQ(parseStreamType(cursor), streamType);
  auto parseStreamHeaderResult =
      parser_.parseSubgroupHeader(cursor, SubgroupIDFormat::Zero, false);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult = parser_.parseSubgroupObjectHeader(
      cursor, *parseStreamHeaderResult, SubgroupIDFormat::Zero, false);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);

  parseResult = parser_.parseSubgroupObjectHeader(
      cursor, *parseStreamHeaderResult, SubgroupIDFormat::Zero, false);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<TrackAlias>(parseResult->trackIdentifier), TrackAlias(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, ParseFetchHeader) {
  ObjectHeader expectedObjectHeader = {
      RequestID(22), // reqID
      33,            // group
      0,             // subgroup
      44,            // id
      55,            // priority
      ObjectStatus::NORMAL,
      noExtensions(),
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeFetchHeader(
      writeBuf, std::get<RequestID>(expectedObjectHeader.trackIdentifier));
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
  EXPECT_EQ(std::get<RequestID>(parseResult->trackIdentifier), RequestID(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->length, 4);
  cursor.skip(*parseResult->length);

  parseResult = parser_.parseFetchObjectHeader(cursor, headerTemplate);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(std::get<RequestID>(parseResult->trackIdentifier), RequestID(22));
  EXPECT_EQ(parseResult->group, 33);
  EXPECT_EQ(parseResult->id, 44);
  EXPECT_EQ(parseResult->priority, 55);
  EXPECT_EQ(parseResult->status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, ParseClientSetupForMaxRequestID) {
  // Test different values for MAX_REQUEST_ID
  const std::vector<uint64_t> kTestMaxRequestIDs = {
      0,
      quic::kOneByteLimit,
      quic::kOneByteLimit + 1,
      quic::kTwoByteLimit,
      quic::kTwoByteLimit + 1,
      quic::kFourByteLimit,
      quic::kFourByteLimit + 1,
      quic::kEightByteLimit};
  for (auto maxRequestID : kTestMaxRequestIDs) {
    auto clientSetup = ClientSetup{
        .supportedVersions = {kVersionDraftCurrent},
        .params =
            {{{.key = folly::to_underlying(SetupKey::MAX_REQUEST_ID),
               .asString = "",
               .asUint64 = maxRequestID}}},
    };

    folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
    auto result = writeClientSetup(writeBuf, clientSetup, GetParam());
    EXPECT_TRUE(result.hasValue())
        << "Failed to write client setup for maxRequestID:" << maxRequestID;
    auto buffer = writeBuf.move();
    auto cursor = folly::io::Cursor(buffer.get());
    auto frameType = quic::decodeQuicInteger(cursor);
    uint64_t expectedFrameType = (getDraftMajorVersion(GetParam()) >= 11)
        ? folly::to_underlying(FrameType::CLIENT_SETUP)
        : folly::to_underlying(FrameType::LEGACY_CLIENT_SETUP);
    EXPECT_EQ(frameType->first, expectedFrameType);
    auto parseClientSetupResult = parseClientSetup(cursor, frameLength(cursor));
    EXPECT_TRUE(parseClientSetupResult.hasValue())
        << "Failed to parse client setup for maxRequestID:" << maxRequestID;
    EXPECT_EQ(parseClientSetupResult->supportedVersions.size(), 1);
    EXPECT_EQ(
        parseClientSetupResult->supportedVersions[0], kVersionDraftCurrent);
    EXPECT_EQ(parseClientSetupResult->params.size(), 1);
    EXPECT_EQ(
        parseClientSetupResult->params[0].key,
        folly::to_underlying(SetupKey::MAX_REQUEST_ID));
    EXPECT_EQ(parseClientSetupResult->params[0].asUint64, maxRequestID);
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
          44,             // subgroup
          44,             // id
          55,             // priority
          4),
      folly::IOBuf::copyBuffer("abcd"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto streamType =
      getSubgroupStreamType(GetParam(), SubgroupIDFormat::FirstObject, false);
  auto hasExtensions = getDraftMajorVersion(GetParam()) < 11;
  auto parsedST = parseStreamType(cursor);
  EXPECT_EQ(parsedST, streamType)
      << GetParam() << " " << folly::to_underlying(parsedST) << " "
      << folly::to_underlying(streamType);
  auto parseStreamHeaderResult = parser_.parseSubgroupHeader(
      cursor, SubgroupIDFormat::FirstObject, hasExtensions);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult = parser_.parseSubgroupObjectHeader(
      cursor,
      *parseStreamHeaderResult,
      SubgroupIDFormat::FirstObject,
      hasExtensions);
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
        {getAuthorizationParamKey(GetParam()),
         writer_.encodeTokenValue(0, "stampolli"),
         0,
         {}});
    params.push_back({getDeliveryTimeoutParamKey(GetParam()), "", 999, {}});
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
    EXPECT_EQ(parseResult->params[0].key, getAuthorizationParamKey(GetParam()));
    EXPECT_EQ(parseResult->params[0].asAuthToken.tokenType, 0);
    EXPECT_EQ(parseResult->params[0].asAuthToken.tokenValue, "stampolli");
    EXPECT_EQ(
        parseResult->params[1].key, getDeliveryTimeoutParamKey(GetParam()));
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
  // Add some parameters to the TrackStatusRequest.
  params.push_back(
      {getAuthorizationParamKey(GetParam()),
       writer_.encodeTokenValue(0, "stampolli"),
       0,
       {}});
  params.push_back({getDeliveryTimeoutParamKey(GetParam()), "", 999, {}});
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
    EXPECT_EQ(parseResult->params[0].key, getAuthorizationParamKey(GetParam()));
    EXPECT_EQ(parseResult->params[0].asAuthToken.tokenType, 0);
    EXPECT_EQ(parseResult->params[0].asAuthToken.tokenValue, "stampolli");
    EXPECT_EQ(
        parseResult->params[1].key, getDeliveryTimeoutParamKey(GetParam()));
    EXPECT_EQ(parseResult->params[1].asUint64, 999);
  }
}

static std::string encodeToken(
    MoQFrameWriter& writer,
    AliasType aliasType,
    uint64_t alias,
    uint64_t tokenType,
    const std::string& tokenValue) {
  switch (aliasType) {
    case AliasType::USE_VALUE:
      return writer.encodeTokenValue(tokenType, tokenValue);
    case AliasType::REGISTER:
      return writer.encodeRegisterToken(alias, tokenType, tokenValue);
    case AliasType::USE_ALIAS:
      return writer.encodeUseAlias(alias);
    case AliasType::DELETE:
      return writer.encodeDeleteTokenAlias(alias);
    default:
      throw std::invalid_argument("Invalid alias type");
  }
}

static size_t writeSubscribeRequestWithAuthToken(
    folly::IOBufQueue& writeBuf,
    MoQFrameWriter& writer,
    AliasType aliasType,
    uint64_t alias,
    uint64_t tokenType,
    const std::string& tokenValue) {
  SubscribeRequest req{
      RequestID(0),
      TrackAlias(1),
      FullTrackName({TrackNamespace({"test"}), "track"}),
      kDefaultPriority,
      GroupOrder::OldestFirst,
      true,
      LocationType::LatestObject,
      folly::none,
      0,
      {}};

  auto encodedToken =
      encodeToken(writer, aliasType, alias, tokenType, tokenValue);
  req.params.push_back(
      {getAuthorizationParamKey(*writer.getVersion()), encodedToken, 0, {}});
  auto writeResult = writer.writeSubscribeRequest(writeBuf, req);
  EXPECT_TRUE(writeResult.hasValue());
  return encodedToken.size();
}

using MoQFramerAuthTest = MoQFramerTest;

TEST_P(MoQFramerAuthTest, AuthTokenTest) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  parser_.setTokenCacheMaxSize(100);

  // Register token with type=0, value="abc"
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::REGISTER, 0, 0, "abc");

  // Register token with type=1, value="def"
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::REGISTER, 1, 1, "def");

  // Delete alias=0
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::DELETE, 0, 0, "");

  // Use alias=1
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::USE_ALIAS, 1, 0, "");

  // Use value with type=2, value="xyz"
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::USE_VALUE, 0, 2, "xyz");

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Parse and verify each token
  std::vector<uint64_t> expectedTokenType = {0, 1, 19, 1, 2};
  std::vector<std::string> expectedTokenValue = {
      "abc", "def", "", "def", "xyz"};
  for (int i = 0; i < 5; ++i) {
    auto frameType = quic::decodeQuicInteger(cursor);
    EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
    auto parseResult =
        parser_.parseSubscribeRequest(cursor, frameLength(cursor));
    EXPECT_TRUE(parseResult.hasValue());
    EXPECT_EQ(parseResult->fullTrackName.trackNamespace.size(), 1);
    EXPECT_EQ(parseResult->fullTrackName.trackNamespace[0], "test");
    EXPECT_EQ(parseResult->fullTrackName.trackName, "track");
    if (i == 2) {
      EXPECT_EQ(parseResult->params.size(), 0);
    } else {
      EXPECT_EQ(parseResult->params.size(), 1);
      EXPECT_EQ(
          parseResult->params[0].asAuthToken.tokenType, expectedTokenType[i])
          << i;
      EXPECT_EQ(
          parseResult->params[0].asAuthToken.tokenValue, expectedTokenValue[i])
          << i;
    }
  }
}

TEST_P(MoQFramerAuthTest, AuthTokenErrorCases) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  parser_.setTokenCacheMaxSize(22); // Set a small cache size for testing

  // Register token with alias=0, type=0, value="abc"
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::REGISTER, 0, 0, "abc");

  // Attempt to register another token with the same alias=0
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::REGISTER, 0, 1, "def");

  // Attempt to use an alias that doesn't exist (alias=2)
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::USE_ALIAS, 2, 0, "");

  // Attempt to delete an alias that doesn't exist (alias=3)
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::DELETE, 3, 0, "");

  // Register a token that exceeds the max token cache size
  writeSubscribeRequestWithAuthToken(
      writeBuf, writer_, AliasType::REGISTER, 1, 3, "jklmnop");

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  std::vector expectedErrors = {
      ErrorCode::NO_ERROR,
      ErrorCode::DUPLICATE_AUTH_TOKEN_ALIAS,
      ErrorCode::UNKNOWN_AUTH_TOKEN_ALIAS,
      ErrorCode::UNKNOWN_AUTH_TOKEN_ALIAS,
      ErrorCode::AUTH_TOKEN_CACHE_OVERFLOW};
  // Parse and verify each token
  for (int i = 0; i < 5; ++i) {
    auto frameType = quic::decodeQuicInteger(cursor);
    EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
    auto parseResult =
        parser_.parseSubscribeRequest(cursor, frameLength(cursor));
    if (i > 0) {
      // Expect errors for these cases
      EXPECT_FALSE(parseResult.hasValue()) << i;
      EXPECT_EQ(parseResult.error(), expectedErrors[i]) << i;
    } else {
      EXPECT_TRUE(parseResult.hasValue());
    }
  }
}

TEST_P(MoQFramerAuthTest, AuthTokenUnderflowTest) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  parser_.setTokenCacheMaxSize(100);

  std::vector<size_t> tokenLengths;
  folly::IOBufQueue writeBufs[4] = {
      folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()},
      folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()},
      folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()},
      folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()}};

  auto len = writeSubscribeRequestWithAuthToken(
      writeBufs[0],
      writer_,
      AliasType::REGISTER,
      0xff,
      0xff,
      std::string(65, 'a'));
  tokenLengths.push_back(len);

  len = writeSubscribeRequestWithAuthToken(
      writeBufs[1], writer_, AliasType::USE_ALIAS, 0xff, 0, "");
  tokenLengths.push_back(len);

  len = writeSubscribeRequestWithAuthToken(
      writeBufs[2],
      writer_,
      AliasType::USE_VALUE,
      0xff,
      0xff,
      std::string(65, 'b'));
  tokenLengths.push_back(len);

  len = writeSubscribeRequestWithAuthToken(
      writeBufs[3], writer_, AliasType::DELETE, 0xff, 0, "");
  tokenLengths.push_back(len);

  for (int j = 0; j < 4; ++j) {
    auto frameHeader = writeBufs[j].split(3);
    auto front = writeBufs[j].split(20);
    auto origTokenLengthBytes = tokenLengths[j] > 64 ? 2 : 1;
    auto tokenLengthBuf = writeBufs[j].split(origTokenLengthBytes);
    auto tail = writeBufs[j].move();
    folly::io::Cursor cursor(frameHeader.get());

    auto frameType = quic::decodeQuicInteger(cursor);
    EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));

    len = frameLength(cursor, false);
    for (auto i = 0; i < tokenLengths[j] - 1; ++i) {
      auto toParse = front->clone();
      auto shortTokenLengthBuf = folly::IOBuf::create(2);
      uint8_t tokenLengthBytes = 0;
      quic::encodeQuicInteger(i, [&](auto val) {
        if (sizeof(val) == 1) {
          shortTokenLengthBuf->writableData()[0] = val;
        } else {
          val = folly::Endian::big(val);
          memcpy(shortTokenLengthBuf->writableData(), &val, 2);
        }
        shortTokenLengthBuf->append(sizeof(val));
        tokenLengthBytes = sizeof(val);
      });
      toParse->appendToChain(std::move(shortTokenLengthBuf));
      toParse->appendToChain(tail->clone());
      folly::io::Cursor tmpCursor(toParse.get());
      cursor.reset(toParse.get());
      auto parseResult = parser_.parseSubscribeRequest(
          cursor, len - (origTokenLengthBytes - tokenLengthBytes));
      EXPECT_FALSE(parseResult.hasValue());
    }
    if (j == 1 || j == 2) { // register / delete mutate cache state
      auto toParse = front->clone();
      auto shortTokenLengthBuf = folly::IOBuf::create(2);
      uint8_t tokenLengthBytes = 0;
      quic::encodeQuicInteger(tokenLengths[j] + 1, [&](auto val) {
        if (sizeof(val) == 1) {
          shortTokenLengthBuf->writableData()[0] = val;
        } else {
          val = folly::Endian::big(val);
          memcpy(shortTokenLengthBuf->writableData(), &val, 2);
        }
        shortTokenLengthBuf->append(sizeof(val));
        tokenLengthBytes = sizeof(val);
      });
      toParse->appendToChain(std::move(shortTokenLengthBuf));
      toParse->appendToChain(tail->clone());
      toParse->appendToChain(folly::IOBuf::copyBuffer("a"));
      folly::io::Cursor tmpCursor(toParse.get());
      cursor.reset(toParse.get());
      auto parseResult = parser_.parseSubscribeRequest(
          cursor, len - (origTokenLengthBytes - tokenLengthBytes) + 1);
      EXPECT_FALSE(parseResult.hasValue()) << j;
    }
    auto toParse = front->clone();
    toParse->appendToChain(std::move(tokenLengthBuf));
    toParse->appendToChain(std::move(tail));
    cursor.reset(toParse.get());
    auto parseResult = parser_.parseSubscribeRequest(cursor, len);
    EXPECT_TRUE(parseResult.hasValue()) << j;
    if (parseResult.hasError()) {
      EXPECT_EQ(parseResult.error(), ErrorCode::NO_ERROR) << j;
    }
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

INSTANTIATE_TEST_SUITE_P(
    MoQFramerAuthTest,
    MoQFramerAuthTest,
    ::testing::Values(kVersionDraft11));

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
