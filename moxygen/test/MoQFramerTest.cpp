/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include "moxygen/test/TestUtils.h"

using namespace moxygen;

namespace {

// Helper to write a QUIC varint to an IOBufQueue succinctly
inline void writeVarintTo(folly::IOBufQueue& q, uint64_t v) {
  folly::io::QueueAppender appender(&q, kMaxFrameHeaderSize);
  auto appenderOp = [appender = std::move(appender)](auto val) mutable {
    appender.writeBE(val);
  };
  (void)quic::encodeQuicInteger(v, appenderOp);
}

class TestUnderflow : public std::exception {};

// The parameter is the MoQ version
class MoQFramerTest : public ::testing::TestWithParam<uint64_t> {
 public:
  void SetUp() override {
    parser_.initializeVersion(GetParam());
    writer_.initializeVersion(GetParam());
  }

  StreamType parseStreamType(folly::io::Cursor& cursor) {
    auto frameType = quic::follyutils::decodeQuicInteger(cursor);
    if (!frameType) {
      throw TestUnderflow();
    }
    return StreamType(frameType->first);
  }

  DatagramType parseDatagramType(folly::io::Cursor& cursor) {
    auto frameType = quic::follyutils::decodeQuicInteger(cursor);
    if (!frameType) {
      throw TestUnderflow();
    }
    return DatagramType(frameType->first);
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
    if (!cursor.canAdvance(2)) {
      throw TestUnderflow();
    }
    size_t res = cursor.readBE<uint16_t>();
    if (checkAdvance && !cursor.canAdvance(res)) {
      throw TestUnderflow();
    }
    return res;
  }

  void parseAll(folly::io::Cursor& cursor, bool eom) {
    skip(cursor, 1);
    auto r1 = parser_.parseClientSetup(cursor, frameLength(cursor));
    testUnderflowResult(r1);

    skip(cursor, 1);
    auto r2 = parser_.parseServerSetup(cursor, frameLength(cursor));
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
    auto r5 = parser_.parseRequestError(
        cursor, frameLength(cursor), FrameType::SUBSCRIBE_ERROR);
    testUnderflowResult(r5);

    skip(cursor, 1);
    auto r6 = parser_.parseUnsubscribe(cursor, frameLength(cursor));
    testUnderflowResult(r6);

    skip(cursor, 1);
    auto r7 = parser_.parseSubscribeDone(cursor, frameLength(cursor));
    testUnderflowResult(r7);

    skip(cursor, 1);
    auto r8a = parser_.parsePublish(cursor, frameLength(cursor));
    testUnderflowResult(r8a);
    EXPECT_TRUE(getFirstIntParam(
        r8a->params, TrackRequestParamKey::PUBLISHER_PRIORITY));

    skip(cursor, 1);
    auto r8b = parser_.parsePublishOk(cursor, frameLength(cursor));
    testUnderflowResult(r8b);

    skip(cursor, 1);
    auto r8c = parser_.parseRequestError(
        cursor, frameLength(cursor), FrameType::PUBLISH_ERROR);
    testUnderflowResult(r8c);

    skip(cursor, 1);
    auto r9 = parser_.parseAnnounce(cursor, frameLength(cursor));
    testUnderflowResult(r9);

    skip(cursor, 1);
    auto r10 = parser_.parseAnnounceOk(cursor, frameLength(cursor));
    testUnderflowResult(r10);

    skip(cursor, 1);
    auto r11 = parser_.parseRequestError(
        cursor, frameLength(cursor), FrameType::ANNOUNCE_ERROR);
    testUnderflowResult(r11);

    skip(cursor, 1);
    auto r12 = parser_.parseAnnounceCancel(cursor, frameLength(cursor));
    testUnderflowResult(r12);

    skip(cursor, 1);
    auto r13 = parser_.parseUnannounce(cursor, frameLength(cursor));
    testUnderflowResult(r13);

    skip(cursor, 1);
    auto r14a = parser_.parseTrackStatus(cursor, frameLength(cursor));
    testUnderflowResult(r14a);

    skip(cursor, 1);
    if (getDraftMajorVersion(GetParam()) < 15) {
      auto r14b = parser_.parseTrackStatusOk(cursor, frameLength(cursor));
      testUnderflowResult(r14b);
    } else {
      auto r14b = parser_.parseRequestOk(
          cursor, frameLength(cursor), FrameType::REQUEST_OK);
      testUnderflowResult(r14b);
    }

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
    auto r11a = parser_.parseRequestError(
        cursor, frameLength(cursor), FrameType::SUBSCRIBE_ANNOUNCES_ERROR);
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
    auto r19 = parser_.parseRequestError(
        cursor, frameLength(cursor), FrameType::FETCH_ERROR);
    testUnderflowResult(r19);

    auto streamType = parseStreamType(cursor);
    SubgroupOptions options = getSubgroupOptions(GetParam(), streamType);
    auto res =
        parser_.parseSubgroupHeader(cursor, cursor.totalLength(), options);
    testUnderflowResult(res);
    EXPECT_EQ(res->value.objectHeader.group, 2);

    auto r15 = parser_.parseSubgroupObjectHeader(
        cursor, cursor.totalLength(), res->value.objectHeader, options);
    testUnderflowResult(r15);
    EXPECT_EQ(r15->value.id, 4);
    skip(cursor, *r15->value.length);

    auto r15a = parser_.parseSubgroupObjectHeader(
        cursor, cursor.totalLength(), res->value.objectHeader, options);
    testUnderflowResult(r15a);
    EXPECT_EQ(r15a->value.id, 5);
    EXPECT_EQ(
        r15a->value.extensions, Extensions(test::getTestExtensions(), {}));
    skip(cursor, *r15a->value.length);

    auto r20 = parser_.parseSubgroupObjectHeader(
        cursor, cursor.totalLength(), res->value.objectHeader, options);
    testUnderflowResult(r20);
    EXPECT_EQ(r20->value.status, ObjectStatus::OBJECT_NOT_EXIST);

    auto r20a = parser_.parseSubgroupObjectHeader(
        cursor, cursor.totalLength(), res->value.objectHeader, options);
    testUnderflowResult(r20a);
    EXPECT_EQ(
        r20a->value.extensions, Extensions(test::getTestExtensions(), {}));
    EXPECT_EQ(r20a->value.status, ObjectStatus::END_OF_TRACK);

    skip(cursor, 1);
    auto r21 = parser_.parseFetchHeader(cursor, cursor.totalLength());
    testUnderflowResult(r21);
    EXPECT_EQ(r21->value, RequestID(1));

    ObjectHeader obj;
    // Fetch context uses placeholder TrackAlias(0)
    auto r22 =
        parser_.parseFetchObjectHeader(cursor, cursor.totalLength(), obj);
    testUnderflowResult(r22);
    EXPECT_EQ(r22->value.id, 4);
    skip(cursor, *r22->value.length);

    auto r22a =
        parser_.parseFetchObjectHeader(cursor, cursor.totalLength(), obj);
    testUnderflowResult(r22a);
    EXPECT_EQ(r22a->value.id, 5);
    EXPECT_EQ(
        r22a->value.extensions, Extensions(test::getTestExtensions(), {}));
    skip(cursor, *r22a->value.length);

    auto r23 =
        parser_.parseFetchObjectHeader(cursor, cursor.totalLength(), obj);
    testUnderflowResult(r23);
    EXPECT_EQ(r23->value.status, ObjectStatus::END_OF_GROUP);

    auto r23a =
        parser_.parseFetchObjectHeader(cursor, cursor.totalLength(), obj);
    testUnderflowResult(r23a);
    EXPECT_EQ(
        r23a->value.extensions, Extensions(test::getTestExtensions(), {}));
    EXPECT_EQ(r23a->value.status, ObjectStatus::END_OF_GROUP);
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
      TrackAlias(22), // trackAlias
      {33,            // group
       0,             // subgroup
       44,            // id
       55,            // priority
       ObjectStatus::OBJECT_NOT_EXIST,
       noExtensions(),
       0},
      nullptr);
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto dgType = parseDatagramType(cursor);
  EXPECT_EQ(dgType, getDatagramType(GetParam(), true, false, false, false));
  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(cursor, dgType, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->trackAlias, TrackAlias(22));
  EXPECT_EQ(parseResult->objectHeader.group, 33);
  EXPECT_EQ(parseResult->objectHeader.id, 44);
  EXPECT_EQ(parseResult->objectHeader.priority, 55);
  EXPECT_EQ(parseResult->objectHeader.status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, ParseDatagramNormal) {
  // Test OBJECT_DATAGRAM
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeDatagramObject(
      writeBuf,
      TrackAlias(22), // trackAlias
      {33,            // group
       0,             // subgroup
       44,            // id
       55,            // priority
       ObjectStatus::NORMAL,
       noExtensions(),
       8},
      folly::IOBuf::copyBuffer("datagram"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto dgType = parseDatagramType(cursor);
  EXPECT_EQ(dgType, getDatagramType(GetParam(), false, false, false, false));
  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(cursor, dgType, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->trackAlias, TrackAlias(22));
  EXPECT_EQ(parseResult->objectHeader.group, 33);
  EXPECT_EQ(parseResult->objectHeader.id, 44);
  EXPECT_EQ(parseResult->objectHeader.priority, 55);
  EXPECT_EQ(parseResult->objectHeader.status, ObjectStatus::NORMAL);
  EXPECT_EQ(parseResult->objectHeader.length, 8);
}

TEST(MoQFramerTest, ParseServerSetupQuicIntegerLength) {
  // Malformed server setup, see that we don't crash
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  size_t size = 0;
  bool error = false;

  // Encode the selected version
  uint64_t version = kVersionDraftCurrent;
  writeVarint(writeBuf, version, size, error);

  // Encode the number of parameters
  uint64_t numParams = 4;
  writeVarint(writeBuf, numParams, size, error);

  uint64_t param1key = 4;
  writeVarint(writeBuf, param1key, size, error);

  uint64_t param1length = 10;
  writeVarint(writeBuf, param1length, size, error);

  size_t sizeToGive = size;

  uint64_t param1value = 100;
  writeVarint(writeBuf, param1value, size, error);

  uint64_t param2key = 20;
  writeVarint(writeBuf, param2key, size, error);

  uint64_t param2length = 10;
  writeVarint(writeBuf, param2length, size, error);

  auto buf = writeBuf.move();
  folly::io::Cursor cursor(buf.get());
  MoQFrameParser parser;
  parser.parseServerSetup(cursor, sizeToGive);
}

TEST(MoQFramerTest, ParseServerSetupLengthParseParam) {
  // Malformed server setup, see that we don't crash
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  size_t size = 0;
  bool error = false;

  // Encode the selected version
  uint64_t version = kVersionDraftCurrent;
  writeVarint(writeBuf, version, size, error);

  // Encode the number of parameters
  uint64_t numParams = 4;
  writeVarint(writeBuf, numParams, size, error);

  uint64_t param1key = 4;
  writeVarint(writeBuf, param1key, size, error);

  size_t sizeToGive = size;

  uint64_t param1length = 10;
  writeVarint(writeBuf, param1length, size, error);

  uint64_t param1value = 100;
  writeVarint(writeBuf, param1value, size, error);

  uint64_t param2key = 20;
  writeVarint(writeBuf, param2key, size, error);

  uint64_t param2length = 10;
  writeVarint(writeBuf, param2length, size, error);

  auto buf = writeBuf.move();
  folly::io::Cursor cursor(buf.get());
  MoQFrameParser parser;
  parser.parseServerSetup(cursor, sizeToGive);
}

TEST(MoQFramerTest, ParseClientSetupWithUnknownAndSupportedVersions) {
  // Compose a CLIENT_SETUP with both supported and unknown versions, and extra
  // params
  // Compose a CLIENT_SETUP with two supported versions (one valid, one
  // unknown/unsupported)
  ClientSetup clientSetup{
      .supportedVersions = {kVersionDraftCurrent, kVersionDraftCurrent},
      .params =
          {
              {
                  folly::to_underlying(SetupKey::MAX_REQUEST_ID),
                  42,
              },
              {
                  folly::to_underlying(SetupKey::PATH),
                  "/foo/bar",
              },
          },
  };

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto resultWrite =
      writeClientSetup(writeBuf, clientSetup, kVersionDraftCurrent);
  EXPECT_TRUE(resultWrite.hasValue()) << "Failed to write CLIENT_SETUP";

  // Coalesce the buffer so we can index into it
  auto buffer = writeBuf.move();
  buffer->coalesce();

  // Overwrite the second version with kVersionDraft03 (unsupported) by splicing
  // in a new IOBuf Layout: [1 byte frame type][2 byte length][1 byte
  // num_versions][varint][varint] So offset = 1 (frame type) + 2 (length) + 1
  // (num_versions) + <size of first version>
  size_t offset = 1 + 2 + 1;
  size_t firstVersionSize = *quic::getQuicIntegerSize(kVersionDraftCurrent);
  size_t secondVersionOffset = offset + firstVersionSize;
  size_t secondVersionSize = *quic::getQuicIntegerSize(kVersionDraftCurrent);

  // Create a new IOBuf with kVersionDraft03 encoded as a quic varint
  folly::IOBufQueue patchBuf{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&patchBuf, kMaxFrameHeaderSize);
  XCHECK(quic::encodeQuicInteger(kVersionDraft03, [&](auto val) {
    appender.writeBE(val);
  }));
  auto patchIOBuf = patchBuf.move();

  // Splice: [head][patch][tail]
  auto head = buffer->cloneOne();
  head->trimEnd(buffer->length() - secondVersionOffset);
  auto tail = buffer->cloneOne();
  tail->trimStart(secondVersionOffset + secondVersionSize);

  // Build new buffer chain: head -> patchIOBuf -> tail
  head->appendToChain(std::move(patchIOBuf));
  head->appendToChain(std::move(tail));
  buffer = std::move(head);

  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraftCurrent);

  cursor.skip(3);
  auto length = buffer->computeChainDataLength() - 3;
  auto result = parser.parseClientSetup(cursor, length);
  EXPECT_TRUE(result.hasValue())
      << "Parsing CLIENT_SETUP with mixed versions should succeed";
  // Check only supported versions are present in the parsed setup
  ASSERT_EQ(result->supportedVersions.size(), 1);
  EXPECT_EQ(result->supportedVersions[0], kVersionDraftCurrent);
  // Check parameters
  ASSERT_EQ(result->params.size(), 2);
  auto it = result->params.begin();
  EXPECT_EQ(it->key, folly::to_underlying(SetupKey::MAX_REQUEST_ID));
  EXPECT_EQ(it->asUint64, 42);
  ++it;
  EXPECT_EQ(it->key, folly::to_underlying(SetupKey::PATH));
  EXPECT_EQ(it->asString, "/foo/bar");
}

ObjectHeader MoQFramerTest::testUnderflowDatagramHelper(
    folly::IOBufQueue& writeBuf,
    bool isStatus,
    bool hasExtensions,
    uint64_t expectedPayloadLen) {
  for (size_t i = 1; i <= writeBuf.chainLength(); ++i) {
    folly::io::Cursor cursor(writeBuf.front());
    auto datagramType =
        getDatagramType(GetParam(), isStatus, hasExtensions, false, false);
    auto decodedType = quic::follyutils::decodeQuicInteger(cursor, i);
    EXPECT_TRUE(decodedType.has_value());
    EXPECT_EQ(decodedType->first, folly::to_underlying(datagramType));

    auto len = i - decodedType->second;
    auto result = parser_.parseDatagramObjectHeader(cursor, datagramType, len);
    if (i < writeBuf.chainLength()) {
      if (result.hasValue()) {
        EXPECT_TRUE(result.value().objectHeader.status == ObjectStatus::NORMAL);
        EXPECT_LT(*result.value().objectHeader.length, expectedPayloadLen);
      } else {
        EXPECT_TRUE(result.error() == ErrorCode::PARSE_UNDERFLOW);
      }
      continue;
    }
    if (hasExtensions) {
      EXPECT_EQ(
          result.value().objectHeader.extensions,
          Extensions(test::getTestExtensions(), {}));
    }
    return result.value().objectHeader;
  }
  return ObjectHeader();
}

TEST_P(MoQFramerTest, testParseDatagramObjectHeader1) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(2, 3, 4, 5, ObjectStatus::OBJECT_NOT_EXIST);
  writer_.writeDatagramObject(writeBuf, TrackAlias(1), obj, nullptr);

  auto pobj = testUnderflowDatagramHelper(writeBuf, true, false, 0);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, parseFixedString) {
  // Create a buffer
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // General String
  std::string s("Hello, World");

  // Encode a QuicInteger onto the buffer
  auto quicIntegerSize = quic::getQuicIntegerSize(s.length());
  folly::io::QueueAppender appender(&writeBuf, *quicIntegerSize);
  CHECK(
      quic::encodeQuicInteger(
          s.length(), [appender = std::move(appender)](auto val) mutable {
            appender.writeBE(val);
          }));

  // Write a blob of bytes to buffer
  writeBuf.append(s.data(), s.length());

  // Parse and decode to check
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  size_t length = 13;
  auto decoded = parseFixedString(cursor, length);
  EXPECT_TRUE(decoded.hasValue());
  EXPECT_EQ(decoded.value(), s);
}

TEST_P(MoQFramerTest, testParseDatagramObjectHeader2) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(
      2,
      3,
      4,
      5,
      ObjectStatus::END_OF_GROUP,
      Extensions(test::getTestExtensions(), {}));
  writer_.writeDatagramObject(writeBuf, TrackAlias(1), obj, nullptr);

  auto pobj = testUnderflowDatagramHelper(writeBuf, true, true, 0);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::END_OF_GROUP);
}

TEST_P(MoQFramerTest, testParseDatagramObjectHeader3) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(2, 3, 4, 5, ObjectStatus::NORMAL, noExtensions(), 11);
  writer_.writeDatagramObject(
      writeBuf, TrackAlias(1), obj, folly::IOBuf::copyBuffer("hello world"));
  auto pobj = testUnderflowDatagramHelper(writeBuf, false, false, 11);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::NORMAL);
  EXPECT_EQ(pobj.length, 11);
}

TEST_P(MoQFramerTest, testParseDatagramObjectHeader4) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(
      2,
      3,
      4,
      5,
      ObjectStatus::NORMAL,
      Extensions(test::getTestExtensions(), {}),
      11);
  writer_.writeDatagramObject(
      writeBuf, TrackAlias(1), obj, folly::IOBuf::copyBuffer("hello world"));

  auto pobj = testUnderflowDatagramHelper(writeBuf, false, true, 11);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::NORMAL);
  EXPECT_EQ(pobj.length, 11);
}

TEST_P(MoQFramerTest, ZeroLengthNormal) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeDatagramObject(
      writeBuf,
      TrackAlias(22), // trackAlias
      {33,            // group
       0,             // subgroup
       44,            // id
       55,            // priority
       ObjectStatus::NORMAL,
       noExtensions(),
       0},
      nullptr);
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto dgType = parseDatagramType(cursor);
  EXPECT_EQ(dgType, getDatagramType(GetParam(), true, false, false, false));
  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(cursor, dgType, length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->trackAlias, TrackAlias(22));
  EXPECT_EQ(parseResult->objectHeader.group, 33);
  EXPECT_EQ(parseResult->objectHeader.id, 44);
  EXPECT_EQ(parseResult->objectHeader.priority, 55);
  EXPECT_EQ(parseResult->objectHeader.status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->objectHeader.length, 0);
}

TEST_P(MoQFramerTest, ParseStreamHeader) {
  ObjectHeader expectedObjectHeader = {
      33, // group
      0,  // subgroup
      44, // id
      55, // priority
      ObjectStatus::NORMAL,
      noExtensions(),
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto streamType =
      getSubgroupStreamType(GetParam(), SubgroupIDFormat::Zero, false, false);
  auto result = writer_.writeSubgroupHeader(
      writeBuf,
      TrackAlias(22),
      expectedObjectHeader,
      SubgroupIDFormat::Zero,
      false);
  EXPECT_TRUE(result.hasValue());
  result = writer_.writeStreamObject(
      writeBuf,
      streamType,
      expectedObjectHeader,
      folly::IOBuf::copyBuffer("EFGH"));
  EXPECT_TRUE(result.hasValue());
  // Update objectID to play nice with delta encoding.
  expectedObjectHeader.id = 45;
  // Test ObjectStatus::OBJECT_NOT_EXIST
  expectedObjectHeader.status = ObjectStatus::OBJECT_NOT_EXIST;
  expectedObjectHeader.length = 0;
  result = writer_.writeStreamObject(
      writeBuf, streamType, expectedObjectHeader, nullptr);
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  EXPECT_EQ(parseStreamType(cursor), streamType);
  auto sgOptions = getSubgroupOptions(GetParam(), streamType);
  auto parseStreamHeaderResult =
      parser_.parseSubgroupHeader(cursor, cursor.totalLength(), sgOptions);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult = parser_.parseSubgroupObjectHeader(
      cursor,
      cursor.totalLength(),
      parseStreamHeaderResult->value.objectHeader,
      sgOptions);
  EXPECT_TRUE(parseResult.hasValue());
  // trackAlias is no longer part of ObjectHeader, validated by function call
  // context
  EXPECT_EQ(parseResult->value.group, 33);
  EXPECT_EQ(parseResult->value.id, 44);
  EXPECT_EQ(parseResult->value.priority, 55);
  EXPECT_EQ(parseResult->value.status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->value.length, 4);
  cursor.skip(*parseResult->value.length);

  parseResult = parser_.parseSubgroupObjectHeader(
      cursor,
      cursor.totalLength(),
      parseStreamHeaderResult->value.objectHeader,
      sgOptions);
  EXPECT_TRUE(parseResult.hasValue());
  // trackAlias is no longer part of ObjectHeader, validated by function call
  // context
  EXPECT_EQ(parseResult->value.group, 33);
  EXPECT_EQ(parseResult->value.id, 45);
  EXPECT_EQ(parseResult->value.priority, 55);
  EXPECT_EQ(parseResult->value.status, ObjectStatus::OBJECT_NOT_EXIST);
}

TEST_P(MoQFramerTest, ParseFetchHeader) {
  ObjectHeader expectedObjectHeader = {
      33, // group
      0,  // subgroup
      44, // id
      55, // priority
      ObjectStatus::NORMAL,
      noExtensions(),
      4};
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeFetchHeader(
      writeBuf, RequestID(22)); // Original test expected RequestID(22)
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
  auto parseStreamHeaderResult =
      parser_.parseFetchHeader(cursor, cursor.totalLength());
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  ObjectHeader headerTemplate;
  auto parseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->value.group, 33);
  EXPECT_EQ(parseResult->value.id, 44);
  EXPECT_EQ(parseResult->value.priority, 55);
  EXPECT_EQ(parseResult->value.status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->value.length, 4);
  cursor.skip(*parseResult->value.length);

  parseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->value.group, 33);
  EXPECT_EQ(parseResult->value.id, 44);
  EXPECT_EQ(parseResult->value.priority, 55);
  EXPECT_EQ(parseResult->value.status, ObjectStatus::OBJECT_NOT_EXIST);
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
        .params = {Parameter(
            folly::to_underlying(SetupKey::MAX_REQUEST_ID), maxRequestID)},
    };

    folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
    auto result = writeClientSetup(writeBuf, clientSetup, GetParam());
    EXPECT_TRUE(result.hasValue())
        << "Failed to write client setup for maxRequestID:" << maxRequestID;
    auto buffer = writeBuf.move();
    auto cursor = folly::io::Cursor(buffer.get());
    auto frameType = quic::follyutils::decodeQuicInteger(cursor);
    uint64_t expectedFrameType = folly::to_underlying(FrameType::CLIENT_SETUP);
    EXPECT_EQ(frameType->first, expectedFrameType);
    auto parseClientSetupResult =
        parser_.parseClientSetup(cursor, frameLength(cursor));
    EXPECT_TRUE(parseClientSetupResult.hasValue())
        << "Failed to parse client setup for maxRequestID:" << maxRequestID;
    if (getDraftMajorVersion(GetParam()) < 15) {
      EXPECT_EQ(parseClientSetupResult->supportedVersions.size(), 1);
      EXPECT_EQ(
          parseClientSetupResult->supportedVersions[0], kVersionDraftCurrent);
    }
    EXPECT_EQ(parseClientSetupResult->params.size(), 1);
    EXPECT_EQ(
        parseClientSetupResult->params.at(0).key,
        folly::to_underlying(SetupKey::MAX_REQUEST_ID));
    EXPECT_EQ(parseClientSetupResult->params.at(0).asUint64, maxRequestID);
  }
}

TEST(MoQFramerTest, ParseClientSetupParamsUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Add version
  writeVarint(writeBuf, 1, size, error);
  writeVarint(writeBuf, kVersionDraftCurrent, size, error);

  // Signify 2 bytes Varint but append only 1 byte to trigger underflow
  writeVarint(writeBuf, 2, size, error);
  writeBuf.append("\x40", 1);
  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PARSE_UNDERFLOW);
}

TEST(MoQFramerTest, ParseClientSetupNoOfVersionsUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  writeBuf.append("\xC0\x00", 2);
  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PARSE_UNDERFLOW);
}

TEST(MoQFramerTest, ParseClientSetupVersionUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, 2, size, error);
  writeVarint(writeBuf, kVersionDraftCurrent, size, error);
  writeBuf.append("\xC0\x00", 2);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PARSE_UNDERFLOW);
}

TEST(MoQFramerTest, ParseClientSetupNoOfParamsUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, 1, size, error);
  writeVarint(writeBuf, kVersionDraftCurrent, size, error);

  writeBuf.append("\xC0\x00", 2);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PARSE_UNDERFLOW);
}

TEST(MoQFramerTest, ParseClientSetupParamsUnderflowString) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, 1, size, error);
  writeVarint(writeBuf, kVersionDraftCurrent, size, error);
  writeVarint(writeBuf, 1, size, error);
  writeVarint(writeBuf, folly::to_underlying(SetupKey::PATH), size, error);

  // Signify 2 byte string but append only 1 byte to trigger underflow
  writeVarint(writeBuf, 2, size, error);
  writeBuf.append("s", 1);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PARSE_UNDERFLOW);
}

TEST(MoQFramerTest, ParseClientSetupParamsIncorrectLength) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, 1, size, error);
  writeVarint(writeBuf, kVersionDraftCurrent, size, error);
  writeVarint(writeBuf, 1, size, error);
  writeVarint(writeBuf, folly::to_underlying(SetupKey::PATH), size, error);

  writeVarint(writeBuf, 1, size, error);
  writeBuf.append("s", 1);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength() + 1);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST(MoQFramerTest, ParseServerSetupVersionUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  writeBuf.append("\xC0\x00", 2);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  auto result =
      parser.parseServerSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PARSE_UNDERFLOW);
}

TEST(MoQFramerTest, ParseServerSetupNoOfParamsUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, kVersionDraftCurrent, size, error);

  writeBuf.append("\xC0\x00", 2);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  auto result =
      parser.parseServerSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PARSE_UNDERFLOW);
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
      parser_.reset();
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
      TrackAlias(22), // trackAlias
      ObjectHeader(
          33, // group
          44, // subgroup
          44, // id
          55, // priority
          4),
      folly::IOBuf::copyBuffer("abcd"));
  EXPECT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto streamType = getSubgroupStreamType(
      GetParam(), SubgroupIDFormat::FirstObject, false, false);
  auto parsedST = parseStreamType(cursor);
  EXPECT_EQ(parsedST, streamType)
      << GetParam() << " " << folly::to_underlying(parsedST) << " "
      << folly::to_underlying(streamType);
  auto sgOptions = getSubgroupOptions(GetParam(), streamType);
  auto parseStreamHeaderResult =
      parser_.parseSubgroupHeader(cursor, cursor.totalLength(), sgOptions);
  EXPECT_TRUE(parseStreamHeaderResult.hasValue());
  auto parseResult = parser_.parseSubgroupObjectHeader(
      cursor,
      cursor.totalLength(),
      parseStreamHeaderResult->value.objectHeader,
      sgOptions);
  EXPECT_TRUE(parseResult.hasValue());
  // trackAlias is no longer part of ObjectHeader, validated by function call
  // context
  EXPECT_EQ(parseResult->value.group, 33);
  EXPECT_EQ(parseResult->value.id, 44);
  EXPECT_EQ(parseResult->value.priority, 55);
  EXPECT_EQ(parseResult->value.status, ObjectStatus::NORMAL);
  EXPECT_EQ(*parseResult->value.length, 4);
  cursor.skip(*parseResult->value.length);
}

TEST_P(MoQFramerTest, ParseTrackStatus) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  TrackStatus ts =
      TrackStatus::make(FullTrackName({TrackNamespace({"hello"}), "world"}));
  ts.locType = LocationType::LargestObject;
  // Add some parameters to the TrackStatus.
  ts.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
      writer_.encodeTokenValue(0, "stampolli")));
  ts.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 999));
  auto writeResult = writer_.writeTrackStatus(writeBuf, ts);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::TRACK_STATUS));
  auto parseResult = parser_.parseTrackStatus(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->fullTrackName.trackNamespace.size(), 1);
  EXPECT_EQ(parseResult->fullTrackName.trackNamespace[0], "hello");
  EXPECT_EQ(parseResult->fullTrackName.trackName, "world");
  EXPECT_EQ(parseResult->params.size(), 2);
  EXPECT_EQ(
      parseResult->params.at(0).key,
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN));
  EXPECT_EQ(parseResult->params.at(0).asAuthToken.tokenType, 0);
  EXPECT_EQ(parseResult->params.at(0).asAuthToken.tokenValue, "stampolli");
  EXPECT_EQ(
      parseResult->params.at(1).key,
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT));
  EXPECT_EQ(parseResult->params.at(1).asUint64, 999);
}

TEST_P(MoQFramerTest, ParseTrackStatusOk) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = 7;
  trackStatusOk.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatusOk.statusCode = TrackStatusCode::IN_PROGRESS;
  trackStatusOk.largest = AbsoluteLocation({19, 77});
  trackStatusOk.groupOrder = GroupOrder::OldestFirst;
  TrackRequestParameters params;
  // Add some parameters to the TrackStatus.
  params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
      writer_.encodeTokenValue(0, "stampolli")));
  params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 999));
  trackStatusOk.params = params;
  auto writeResult = writer_.writeTrackStatusOk(writeBuf, trackStatusOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  folly::Expected<TrackStatusOk, ErrorCode> parseResult;
  if (getDraftMajorVersion(GetParam()) < 15) {
    EXPECT_EQ(
        frameType->first, folly::to_underlying(FrameType::TRACK_STATUS_OK));
    parseResult = parser_.parseTrackStatusOk(cursor, frameLength(cursor));
    EXPECT_TRUE(parseResult.hasValue());
  } else {
    EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::REQUEST_OK));
    auto result = parser_.parseRequestOk(
        cursor, frameLength(cursor), FrameType::REQUEST_OK);
    EXPECT_TRUE(result.hasValue());
    parseResult = result->toTrackStatusOk();
  }
  EXPECT_EQ(parseResult->requestID, 7);
  EXPECT_EQ(parseResult->largest->group, 19);
  EXPECT_EQ(parseResult->largest->object, 77);
  EXPECT_EQ(parseResult->statusCode, TrackStatusCode::IN_PROGRESS);
  EXPECT_EQ(parseResult->params.size(), 2);
  EXPECT_EQ(
      parseResult->params.at(0).key,
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN));
  EXPECT_EQ(parseResult->params.at(0).asAuthToken.tokenType, 0);
  EXPECT_EQ(parseResult->params.at(0).asAuthToken.tokenValue, "stampolli");
  EXPECT_EQ(
      parseResult->params.at(1).key,
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT));
  EXPECT_EQ(parseResult->params.at(1).asUint64, 999);
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
      LocationType::LargestObject,
      folly::none,
      0,
      {}};

  auto encodedToken =
      encodeToken(writer, aliasType, alias, tokenType, tokenValue);
  req.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
      encodedToken));
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
    auto frameType = quic::follyutils::decodeQuicInteger(cursor);
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
          parseResult->params.at(0).asAuthToken.tokenType, expectedTokenType[i])
          << i;
      EXPECT_EQ(
          parseResult->params.at(0).asAuthToken.tokenValue,
          expectedTokenValue[i])
          << i;
    }
  }
}

TEST_P(MoQFramerAuthTest, AuthTokenErrorCases) {
  folly::IOBufQueue writeBufs[5];
  parser_.setTokenCacheMaxSize(22); // Set a small cache size for testing

  // Register token with alias=0, type=0, value="abc"
  writeSubscribeRequestWithAuthToken(
      writeBufs[0], writer_, AliasType::REGISTER, 0, 0, "abc");

  // Attempt to register another token with the same alias=0
  writeSubscribeRequestWithAuthToken(
      writeBufs[1], writer_, AliasType::REGISTER, 0, 1, "def");

  // Attempt to use an alias that doesn't exist (alias=2)
  writeSubscribeRequestWithAuthToken(
      writeBufs[2], writer_, AliasType::USE_ALIAS, 2, 0, "");

  // Attempt to delete an alias that doesn't exist (alias=3)
  writeSubscribeRequestWithAuthToken(
      writeBufs[3], writer_, AliasType::DELETE, 3, 0, "");

  // Register a token that exceeds the max token cache size
  writeSubscribeRequestWithAuthToken(
      writeBufs[4], writer_, AliasType::REGISTER, 1, 3, "jklmnop");

  std::vector expectedErrors = {
      ErrorCode::NO_ERROR,
      ErrorCode::DUPLICATE_AUTH_TOKEN_ALIAS,
      ErrorCode::UNKNOWN_AUTH_TOKEN_ALIAS,
      ErrorCode::UNKNOWN_AUTH_TOKEN_ALIAS,
      ErrorCode::AUTH_TOKEN_CACHE_OVERFLOW};
  // Parse and verify each token
  for (int i = 0; i < 5; ++i) {
    auto serialized = writeBufs[i].move();
    folly::io::Cursor cursor(serialized.get());

    auto frameType = quic::follyutils::decodeQuicInteger(cursor);
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
    /*
     * The five buffer operations in AuthTokenUnderflowTest carve the serialized
     * SUBSCRIBE frame into pieces so the test can fiddle with the token-length
     * field while keeping the rest of the frame intact:
     */
    auto frameHeader = writeBufs[j].split(3);
    // Version 15+ don't have the filter within the request, but in the
    // parameters
    const uint32_t kDraft15PreambleLength = 13;
    const uint32_t kDraft14PreambleLength = 19;
    uint32_t frontLength = (getDraftMajorVersion(GetParam()) >= 15)
        ? kDraft15PreambleLength
        : kDraft14PreambleLength;
    auto front = writeBufs[j].split(frontLength);
    auto origTokenLengthBytes = tokenLengths[j] > 64 ? 2 : 1;
    auto tokenLengthBuf = writeBufs[j].split(origTokenLengthBytes);
    auto tail = writeBufs[j].move();
    folly::io::Cursor cursor(frameHeader.get());

    auto frameType = quic::follyutils::decodeQuicInteger(cursor);
    EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));

    len = frameLength(cursor, false);
    for (size_t i = 0; i < tokenLengths[j] - 1; ++i) {
      auto toParse = front->clone();
      auto shortTokenLengthBuf = folly::IOBuf::create(2);
      uint8_t tokenLengthBytes = 0;
      CHECK(quic::encodeQuicInteger(i, [&](auto val) {
        if (sizeof(val) == 1) {
          shortTokenLengthBuf->writableData()[0] = val;
        } else {
          val = folly::Endian::big(val);
          memcpy(shortTokenLengthBuf->writableData(), &val, 2);
        }
        shortTokenLengthBuf->append(sizeof(val));
        tokenLengthBytes = sizeof(val);
      }));
      toParse->appendToChain(std::move(shortTokenLengthBuf));
      toParse->appendToChain(tail->clone());
      folly::io::Cursor tmpCursor(toParse.get());
      cursor.reset(toParse.get());
      auto parseResult = parser_.parseSubscribeRequest(
          cursor, len - (origTokenLengthBytes - tokenLengthBytes));
      if (j == 0) {
        // clear token cache when registering
        parser_.setTokenCacheMaxSize(0);
        parser_.setTokenCacheMaxSize(100);
      }
      EXPECT_FALSE(parseResult.hasValue());
    }
    if (j == 1 || j == 2) { // register / delete mutate cache state
      auto toParse = front->clone();
      auto shortTokenLengthBuf = folly::IOBuf::create(2);
      uint8_t tokenLengthBytes = 0;
      auto newLength = j == 1 ? tokenLengths[j] + 1 : 5;
      CHECK(quic::encodeQuicInteger(newLength, [&](auto val) {
        if (sizeof(val) == 1) {
          shortTokenLengthBuf->writableData()[0] = val;
        } else {
          val = folly::Endian::big(val);
          memcpy(shortTokenLengthBuf->writableData(), &val, 2);
        }
        shortTokenLengthBuf->append(sizeof(val));
        tokenLengthBytes = sizeof(val);
      }));
      toParse->appendToChain(std::move(shortTokenLengthBuf));
      toParse->appendToChain(tail->clone());
      toParse->appendToChain(folly::IOBuf::copyBuffer("a"));
      folly::io::Cursor tmpCursor(toParse.get());
      cursor.reset(toParse.get());
      auto parseResult = parser_.parseSubscribeRequest(
          cursor, len - (origTokenLengthBytes - tokenLengthBytes) + 1);
      EXPECT_FALSE(parseResult.hasValue())
          << j
          << " len=" << len - (origTokenLengthBytes - tokenLengthBytes) + 1;
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

TEST_P(MoQFramerTest, SubscribeUpdateWithSubscribeReqIDSerialization) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeUpdate subscribeUpdate;
  subscribeUpdate.requestID = RequestID(123);
  subscribeUpdate.subscriptionRequestID = RequestID(456);
  subscribeUpdate.start = AbsoluteLocation{10, 20};
  subscribeUpdate.endGroup = 30;
  subscribeUpdate.priority = 5;
  subscribeUpdate.forward = true;
  subscribeUpdate.params = {};

  auto writeResult = writer_.writeSubscribeUpdate(writeBuf, subscribeUpdate);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(
      frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_UPDATE));

  auto parseResult = parser_.parseSubscribeUpdate(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  if (getDraftMajorVersion(GetParam()) >= 14) {
    // Version >= 14: Both requestID and subscriptionRequestID are
    // written/parsed
    EXPECT_EQ(parseResult->requestID.value, 123);
    EXPECT_EQ(parseResult->subscriptionRequestID.value, 456);
  } else {
    // Version < 14: Only requestID is on wire, subscriptionRequestID not set by
    // parser
    EXPECT_EQ(
        parseResult->requestID.value,
        123); // First field on wire goes to requestID
    EXPECT_EQ(
        parseResult->subscriptionRequestID.value,
        0); // Not set by parser for v<14
  }

  EXPECT_EQ(parseResult->start->group, 10);
  EXPECT_EQ(parseResult->start->object, 20);
  EXPECT_EQ(parseResult->endGroup, 30);
  EXPECT_EQ(parseResult->priority, 5);
  EXPECT_TRUE(parseResult->forward.hasValue());
  EXPECT_EQ(*parseResult->forward, true);
}

TEST(MoQFramerTest, SubscribeUpdateDraft15ForwardUnset) {
  // Test that in draft 15+, a SUBSCRIBE_UPDATE without forward parameter
  // is correctly serialized and parsed with forward field unset
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft15);
  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft15);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeUpdate subscribeUpdate;
  subscribeUpdate.requestID = RequestID(123);
  subscribeUpdate.subscriptionRequestID = RequestID(456);
  subscribeUpdate.start = AbsoluteLocation{0, 0};
  subscribeUpdate.endGroup = 0; // Open-ended subscription
  subscribeUpdate.priority = kDefaultPriority;
  // forward field intentionally left unset (folly::none)
  subscribeUpdate.params = {};

  auto writeResult = writer.writeSubscribeUpdate(writeBuf, subscribeUpdate);
  EXPECT_TRUE(writeResult.hasValue()) << "Failed to write SUBSCRIBE_UPDATE";

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  // Skip frame type
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(
      frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_UPDATE));

  // Skip frame length
  size_t frameLength = cursor.readBE<uint16_t>();

  // Parse the SUBSCRIBE_UPDATE
  auto parseResult = parser.parseSubscribeUpdate(cursor, frameLength);
  EXPECT_TRUE(parseResult.hasValue()) << "Failed to parse SUBSCRIBE_UPDATE";

  EXPECT_EQ(parseResult->requestID.value, 123);
  EXPECT_EQ(parseResult->subscriptionRequestID.value, 456);
  EXPECT_EQ(parseResult->start->group, 0);
  EXPECT_EQ(parseResult->start->object, 0);
  EXPECT_EQ(parseResult->endGroup, 0);
  EXPECT_EQ(parseResult->priority, kDefaultPriority);
  // Verify forward field is NOT set (preserves existing state per draft 15+)
  EXPECT_FALSE(parseResult->forward.hasValue());
}

TEST_P(MoQFramerTest, OddExtensionLengthVarintBoundary) {
  // This verifies that for odd-type extensions (length-prefixed), the length
  // varint size is computed from the extension payload length, not from
  // ext.intValue. Using ext.intValue (typically 0) leads to an incorrect
  // extension block length when the payload length crosses the QUIC varint
  // boundary (e.g., 64 -> 2-byte varint).
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Build an object with a single odd-type extension whose payload is 64 bytes
  // so its varint length takes 2 bytes.
  ObjectHeader obj(2, 3, 4, 5);
  std::string payload(64, 'x');
  std::vector<Extension> exts;
  exts.emplace_back(13, folly::IOBuf::copyBuffer(payload)); // odd type (13)
  obj.extensions.insertMutableExtensions(exts);

  // Write subgroup header (includeExtensions=true) and the stream object
  auto res = writer_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), obj, SubgroupIDFormat::Present, true);
  EXPECT_TRUE(res.hasValue());
  res = writer_.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER_SG_EXT, obj, nullptr);
  EXPECT_TRUE(res.hasValue());

  // Parse and validate
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto streamType = getSubgroupStreamType(
      GetParam(), SubgroupIDFormat::Present, true, /*endOfGroup=*/false);
  EXPECT_EQ(parseStreamType(cursor), streamType);
  auto sgOptions = getSubgroupOptions(GetParam(), streamType);
  auto hdrRes =
      parser_.parseSubgroupHeader(cursor, cursor.totalLength(), sgOptions);
  EXPECT_TRUE(hdrRes.hasValue());
  auto objRes = parser_.parseSubgroupObjectHeader(
      cursor, cursor.totalLength(), hdrRes->value.objectHeader, sgOptions);
  EXPECT_TRUE(objRes.hasValue());
  ASSERT_EQ(objRes->value.extensions.size(), 1);
  EXPECT_TRUE(objRes->value.extensions.getMutableExtensions()[0].isOddType());
  EXPECT_EQ(objRes->value.extensions.getMutableExtensions()[0].type, 13);
  EXPECT_EQ(
      objRes->value.extensions.getMutableExtensions()[0]
          .arrayValue->computeChainDataLength(),
      64);
}

TEST_P(MoQFramerTest, SubscribeRequestEncodeDecode) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Build a SubscribeRequest with non-default locType/start/endGroup
  FullTrackName ftn{TrackNamespace({"ns"}), "track"};
  AbsoluteLocation startLoc{10, 20};
  auto req = SubscribeRequest::make(
      ftn,
      /*priority*/ 7,
      /*groupOrder*/ GroupOrder::NewestFirst,
      /*forward*/ false,
      /*locType*/ LocationType::AbsoluteRange,
      /*start*/ folly::make_optional(startLoc),
      /*endGroup*/ 30,
      /*params*/ {});

  auto writeRes = writer_.writeSubscribeRequest(writeBuf, req);
  EXPECT_TRUE(writeRes.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Verify frame type and parse with a draft 15 parser
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
  auto parseRes = parser_.parseSubscribeRequest(cursor, frameLength(cursor));
  EXPECT_TRUE(parseRes.hasValue());

  // Check that parsed SubscribeRequest matches the original
  EXPECT_EQ(
      parseRes->fullTrackName.trackNamespace.size(),
      req.fullTrackName.trackNamespace.size());
  if (!req.fullTrackName.trackNamespace.empty()) {
    EXPECT_EQ(
        parseRes->fullTrackName.trackNamespace[0],
        req.fullTrackName.trackNamespace[0]);
  }
  EXPECT_EQ(parseRes->fullTrackName.trackName, req.fullTrackName.trackName);
  EXPECT_EQ(parseRes->priority, req.priority);
  EXPECT_EQ(parseRes->groupOrder, req.groupOrder);
  EXPECT_EQ(parseRes->forward, req.forward);
  EXPECT_EQ(parseRes->locType, req.locType);
  ASSERT_TRUE(parseRes->start.has_value());
  ASSERT_TRUE(req.start.has_value());
  EXPECT_EQ(parseRes->start->group, req.start->group);
  EXPECT_EQ(parseRes->start->object, req.start->object);
  EXPECT_EQ(parseRes->endGroup, req.endGroup);
  EXPECT_EQ(parseRes->params.size(), req.params.size());
}

TEST_P(MoQFramerTest, ParseSubscriptionFilterLargestGroup) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Build a SubscribeRequest with LargestGroup location type
  FullTrackName ftn{TrackNamespace({"ns"}), "track"};
  auto req = SubscribeRequest::make(
      ftn,
      /*priority*/ kDefaultPriority,
      /*groupOrder*/ GroupOrder::Default,
      /*forward*/ true,
      /*locType*/ LocationType::LargestGroup,
      /*start*/ folly::none,
      /*endGroup*/ 0,
      /*params*/ {});

  auto writeRes = writer_.writeSubscribeRequest(writeBuf, req);
  EXPECT_TRUE(writeRes.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Verify frame type and parse
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
  auto parseRes = parser_.parseSubscribeRequest(cursor, frameLength(cursor));
  EXPECT_TRUE(parseRes.hasValue());

  // Check that parsed SubscribeRequest matches the original
  EXPECT_EQ(parseRes->locType, LocationType::LargestGroup);
  EXPECT_FALSE(parseRes->start.has_value());
}

INSTANTIATE_TEST_SUITE_P(
    MoQFramerTest,
    MoQFramerTest,
    ::testing::ValuesIn(kSupportedVersions));

INSTANTIATE_TEST_SUITE_P(
    MoQFramerAuthTest,
    MoQFramerAuthTest,
    ::testing::ValuesIn(kSupportedVersions));

TEST(MoQFramerTestUtils, DraftMajorVersion) {
  EXPECT_EQ(getDraftMajorVersion(0xff080001), 0x8);
  EXPECT_EQ(getDraftMajorVersion(0xffff0001), 0xff);
  EXPECT_EQ(getDraftMajorVersion(0xff000008), 0x8);
  EXPECT_EQ(getDraftMajorVersion(0xff00ffff), 0xffff);
}

TEST(MoQFramerTestUtils, IsLegacyAlpn) {
  EXPECT_FALSE(isLegacyAlpn(""));

  EXPECT_TRUE(isLegacyAlpn("moq-00"));
  EXPECT_FALSE(isLegacyAlpn("moq-01"));

  EXPECT_FALSE(isLegacyAlpn("moqt-15"));
  EXPECT_FALSE(isLegacyAlpn("moqt-16"));
  EXPECT_FALSE(isLegacyAlpn("moqt-14"));
}

TEST(MoQFramerTestUtils, GetVersionFromAlpn) {
  auto legacyVersion = getVersionFromAlpn("moq-00");
  EXPECT_FALSE(legacyVersion.hasValue());

  auto draft15Meta = getVersionFromAlpn("moqt-15-meta-01");
  ASSERT_TRUE(draft15Meta.hasValue());
  EXPECT_EQ(*draft15Meta, 0xff00000f);

  auto draft15Meta02 = getVersionFromAlpn("moqt-15-meta-02");
  ASSERT_TRUE(draft15Meta02.hasValue());
  EXPECT_EQ(*draft15Meta02, 0xff00000f);

  auto invalidAlpn1 = getVersionFromAlpn("h3");
  EXPECT_FALSE(invalidAlpn1.hasValue());

  auto invalidAlpn2 = getVersionFromAlpn("moqt-");
  EXPECT_FALSE(invalidAlpn2.hasValue());

  auto invalidAlpn3 = getVersionFromAlpn("moqt-abc");
  EXPECT_FALSE(invalidAlpn3.hasValue());

  auto emptyAlpn = getVersionFromAlpn("");
  EXPECT_FALSE(emptyAlpn.hasValue());
}

TEST(MoQFramerTestUtils, GetAlpnFromVersion) {
  auto alpnDraft12 = getAlpnFromVersion(kVersionDraft12);
  ASSERT_TRUE(alpnDraft12.hasValue());
  EXPECT_EQ(*alpnDraft12, "moq-00");

  auto alpnDraft14 = getAlpnFromVersion(kVersionDraft14);
  ASSERT_TRUE(alpnDraft14.hasValue());
  EXPECT_EQ(*alpnDraft14, "moq-00");

  auto alpnDraft15 = getAlpnFromVersion(0xff00000f);
  ASSERT_TRUE(alpnDraft15.hasValue());
  EXPECT_EQ(*alpnDraft15, kAlpnMoqtDraft15Latest);
}

// Test class for immutable extensions feature (draft 14+)
class MoQImmutableExtensionsTest : public ::testing::TestWithParam<uint64_t> {
 public:
  void SetUp() override {
    parser_.initializeVersion(GetParam());
    writer_.initializeVersion(GetParam());
  }

 protected:
  MoQFrameParser parser_;
  MoQFrameWriter writer_;

  // Creates the following extensions (encoded back-to-back):
  // {type = 20, value = 100, immutable}
  // {type = 21, value = binary(0xAB,0xCD,0xEF), immutable}
  // Returns a binary blob suitable as the value for kImmutableExtensionType
  std::unique_ptr<folly::IOBuf> createImmutableExtensionsBuf() {
    folly::IOBufQueue immutableBuf{folly::IOBufQueue::cacheChainLength()};

    // Extension type 20 (even => integer value follows), integer value 100
    writeVarintTo(immutableBuf, 20);  // type
    writeVarintTo(immutableBuf, 100); // value

    // Extension type 21 (odd => length + bytes), binary value
    static uint8_t testData[] = {0xAB, 0xCD, 0xEF};
    writeVarintTo(immutableBuf, 21);                 // type
    writeVarintTo(immutableBuf, sizeof(testData));   // length
    immutableBuf.append(testData, sizeof(testData)); // data

    return immutableBuf.move();
  }

  // Creates a malformed immutable-extensions blob by taking the valid
  // immutable extensions produced above and appending another extension with
  // type = kImmutableExtensionType (odd) and an arbitrary byte value. This is
  // invalid when parsed as nested immutable extensions (parseImmutable=false).
  std::unique_ptr<folly::IOBuf> createImmutableExtensionsBufMalformed() {
    // Start with the valid immutable extensions blob
    auto base = createImmutableExtensionsBuf();

    // Append an additional immutable-extensions container entry with arbitrary
    // 1-byte payload. This makes the nested immutable blob malformed for our
    // parser (immutable container found while already parsing immutable).
    folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};

    if (base) {
      q.append(std::move(base));
    }

    // Write type = kImmutableExtensionType (odd)
    writeVarintTo(q, kImmutableExtensionType);

    // Write length = 1, then one arbitrary byte value 0xAA
    writeVarintTo(q, 1); // length
    static const uint8_t kArbitrary = 0xAA;
    q.append(&kArbitrary, 1);

    return q.move();
  }

  // Creates the following extensions sequence for draft-14+ and returns the
  // raw encoded bytes (without the outer "extensions block length" prefix):
  // {type = 10, value = 42, mutable}
  // {type = kImmutableExtensionType (0xB), value = <binary from above>}
  // {type = 30, value = 999, mutable}
  std::unique_ptr<folly::IOBuf> createExtensionsBuf() {
    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};

    // 1) type = 10 (even => integer value follows), value = 42
    writeVarintTo(buf, 10); // type
    writeVarintTo(buf, 42); // value

    // 2) type = kImmutableExtensionType (odd => length + bytes), value =
    //    output of createImmutableExtensionsBuf()
    auto imm = createImmutableExtensionsBuf();
    writeVarintTo(buf, kImmutableExtensionType);                 // type
    writeVarintTo(buf, imm ? imm->computeChainDataLength() : 0); // length
    if (imm) {
      buf.append(std::move(imm)); // payload
    }

    // 3) type = 30 (even => integer value follows), value = 999
    writeVarintTo(buf, 30);  // type
    writeVarintTo(buf, 999); // value

    return buf.move();
  }

  // Identical to createExtensionsBuf, but uses
  // createImmutableExtensionsBufMalformed() to produce a malformed nested
  // immutable extensions payload under the kImmutableExtensionType container.
  std::unique_ptr<folly::IOBuf> createExtensionsBufMalformed() {
    folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};

    // 1) type = 10 (even => integer value follows), value = 42
    writeVarintTo(buf, 10); // type
    writeVarintTo(buf, 42); // value

    // 2) type = kImmutableExtensionType (odd => length + bytes), value =
    //    output of createImmutableExtensionsBufMalformed()
    auto imm = createImmutableExtensionsBufMalformed();
    writeVarintTo(buf, kImmutableExtensionType);                 // type
    writeVarintTo(buf, imm ? imm->computeChainDataLength() : 0); // length
    if (imm) {
      buf.append(std::move(imm)); // payload
    }

    // 3) type = 30 (even => integer value follows), value = 999
    writeVarintTo(buf, 30);  // type
    writeVarintTo(buf, 999); // value

    return buf.move();
  }
};

TEST_P(MoQImmutableExtensionsTest, ParseEncodedExtensionsBlob) {
  // Build only the extensions block (length + encoded items), then
  // parseExtensions
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Extensions block: length prefix + encoded blob from createExtensionsBuf()
  auto blob = createExtensionsBuf();
  auto blobLen = blob ? blob->computeChainDataLength() : 0;
  writeVarint(writeBuf, blobLen, size, error);
  if (blob) {
    writeBuf.append(std::move(blob));
  }
  EXPECT_FALSE(error);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());
  size_t length = buffer->computeChainDataLength();

  ObjectHeader obj;
  auto parseExts = parser_.parseExtensions(cursor, length, obj);
  EXPECT_TRUE(parseExts.hasValue());

  auto& exts = obj.extensions;
  EXPECT_EQ(exts.size(), 4) << "Expected 4 flattened extensions";

  // Expected mutable extensions: type 10 value 42, type 30 value 999
  std::vector<Extension> expectedMutable = {
      Extension{10, 42}, Extension{30, 999}};

  // Expected immutable extensions: type 20 value 100, type 21 value bytes
  static const uint8_t kExpectedBin[] = {0xAB, 0xCD, 0xEF};
  std::vector<Extension> expectedImmutable = {
      Extension{20, 100},
      Extension{
          21, folly::IOBuf::copyBuffer(kExpectedBin, sizeof(kExpectedBin))}};

  // Check that mutable and immutable extensions match expected
  EXPECT_THAT(
      exts.getMutableExtensions(), testing::ContainerEq(expectedMutable));
  EXPECT_THAT(
      exts.getImmutableExtensions(), testing::ContainerEq(expectedImmutable));
}

// Only test immutable extensions on draft 14+

TEST_P(MoQImmutableExtensionsTest, ParseMalformedNestedImmutableExtensions) {
  // Build only the extensions block (length + encoded items) using the
  // malformed helper that nests an immutable container within immutable.
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  auto blob = createExtensionsBufMalformed();
  auto blobLen = blob ? blob->computeChainDataLength() : 0;
  writeVarint(writeBuf, blobLen, size, error);
  if (blob) {
    writeBuf.append(std::move(blob));
  }
  EXPECT_FALSE(error);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());
  size_t length = buffer->computeChainDataLength();

  ObjectHeader obj;
  auto parseExts = parser_.parseExtensions(cursor, length, obj);
  EXPECT_TRUE(parseExts.hasError());
  EXPECT_EQ(parseExts.error(), ErrorCode::PROTOCOL_VIOLATION);
}

// Test that immutable extensions are written correctly for draft 14+
TEST_P(MoQImmutableExtensionsTest, WriteImmutableExtensionsDraft) {
  // Create extensions with both mutable and immutable extensions
  std::vector<Extension> mutableExts = {Extension{10, 42}, Extension{30, 999}};

  static const uint8_t kTestBinary[] = {0xAB, 0xCD, 0xEF};
  std::vector<Extension> immutableExts = {
      Extension{20, 100},
      Extension{
          21, folly::IOBuf::copyBuffer(kTestBinary, sizeof(kTestBinary))}};

  Extensions extensions(mutableExts, immutableExts);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Write extensions using the writer
  writer_.writeExtensions(writeBuf, extensions, size, error);

  EXPECT_FALSE(error);
  EXPECT_GT(size, 0)
      << "Size should be greater than 0 when extensions are written";

  auto buffer = writeBuf.move();
  EXPECT_TRUE(buffer) << "Buffer should not be null";
  EXPECT_GT(buffer->computeChainDataLength(), 0)
      << "Buffer should contain data";

  // Parse the written extensions back to verify they were written correctly
  folly::io::Cursor cursor(buffer.get());
  ObjectHeader obj;
  size_t bufferLength = buffer->computeChainDataLength();
  auto parseResult = parser_.parseExtensions(cursor, bufferLength, obj);

  EXPECT_TRUE(parseResult.hasValue()) << "Parsing should succeed";

  // Verify both mutable and immutable extensions are present
  EXPECT_EQ(obj.extensions.size(), 4)
      << "Should have 4 total extensions (2 mutable + 2 immutable)";
  EXPECT_THAT(
      obj.extensions.getMutableExtensions(), testing::ContainerEq(mutableExts));
  EXPECT_THAT(
      obj.extensions.getImmutableExtensions(),
      testing::ContainerEq(immutableExts));
}

// Test edge case: Extensions with only immutable extensions
TEST_P(MoQImmutableExtensionsTest, WriteOnlyImmutableExtensionsDraft) {
  // Create extensions with only immutable extensions (no mutable)
  std::vector<Extension> mutableExts; // empty

  static const uint8_t kTestBinary[] = {0xDE, 0xAD, 0xBE, 0xEF};
  std::vector<Extension> immutableExts = {
      Extension{24, 555},
      Extension{
          27, folly::IOBuf::copyBuffer(kTestBinary, sizeof(kTestBinary))}};

  Extensions extensions(mutableExts, immutableExts);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Write extensions using the writer
  writer_.writeExtensions(writeBuf, extensions, size, error);

  EXPECT_FALSE(error);
  EXPECT_GT(size, 0)
      << "Size should be greater than 0 even with only immutable extensions";

  auto buffer = writeBuf.move();
  EXPECT_TRUE(buffer) << "Buffer should not be null";
  EXPECT_GT(buffer->computeChainDataLength(), 0)
      << "Buffer should contain data";

  // Parse the written extensions back
  folly::io::Cursor cursor(buffer.get());
  ObjectHeader obj;
  size_t bufferLength = buffer->computeChainDataLength();
  auto parseResult = parser_.parseExtensions(cursor, bufferLength, obj);

  EXPECT_TRUE(parseResult.hasValue()) << "Parsing should succeed";

  // Verify immutable extensions are present and mutable are empty
  EXPECT_EQ(obj.extensions.size(), 2) << "Should have 2 immutable extensions";
  EXPECT_TRUE(obj.extensions.getMutableExtensions().empty())
      << "Mutable extensions should be empty";
  EXPECT_THAT(
      obj.extensions.getImmutableExtensions(),
      testing::ContainerEq(immutableExts));
}

INSTANTIATE_TEST_SUITE_P(
    MoQImmutableExtensionsTest,
    MoQImmutableExtensionsTest,
    ::testing::Values(kVersionDraft14, kVersionDraft15));

// ALPN Version Negotiation Tests (version >= 15)
TEST(MoQFramerTest, ParseClientSetupWithAlpnVersion15NoVersionArray) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Write CLIENT_SETUP without version array (ALPN mode)
  // Just write number of params (0 in this case)
  writeVarint(writeBuf, 0, size, error);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;

  // When version >= 15 is pre-initialized via ALPN, CLIENT_SETUP should not
  // have version array in wire format
  parser.initializeVersion(kVersionDraft15);
  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength());

  EXPECT_TRUE(result.hasValue()) << "CLIENT_SETUP should parse successfully";
  EXPECT_TRUE(result->supportedVersions.empty())
      << "Version array should be empty when ALPN negotiated";
}

TEST(MoQFramerTest, WriteClientSetupWithAlpnVersion15NoVersionArray) {
  // When version >= 15, CLIENT_SETUP should not write version array

  auto clientSetup = ClientSetup{
      .supportedVersions = {kVersionDraft15},
      .params = {},
  };

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeClientSetup(writeBuf, clientSetup, kVersionDraft15);
  EXPECT_TRUE(result.hasValue()) << "Failed to write CLIENT_SETUP";

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  // Skip frame type
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::CLIENT_SETUP));

  // Skip frame length
  cursor.skip(2);

  // Next field should be number of params (not version array)
  auto numParams = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_TRUE(numParams.has_value());
  EXPECT_EQ(numParams->first, 0) << "Should have 0 params";

  // Verify we're at end of message (no version array was written)
  EXPECT_FALSE(cursor.canAdvance(1))
      << "No additional data should be present (version array not written)";
}

TEST(MoQFramerTest, ParseServerSetupWithAlpnVersion15NoVersionField) {
  // When version >= 15 is pre-initialized via ALPN, SERVER_SETUP should not
  // have version field in wire format

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Write SERVER_SETUP without version field (ALPN mode)
  // Just write number of params (0 in this case)
  writeVarint(writeBuf, 0, size, error);

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft15);
  auto result =
      parser.parseServerSetup(cursor, buffer->computeChainDataLength());

  EXPECT_TRUE(result.hasValue()) << "SERVER_SETUP should parse successfully";
}

TEST(MoQFramerTest, WriteServerSetupWithAlpnVersion15NoVersionField) {
  // When version >= 15, SERVER_SETUP should not write version field

  auto serverSetup = ServerSetup{
      .selectedVersion = kVersionDraft15,
      .params = {},
  };

  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft15);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeServerSetup(writeBuf, serverSetup, kVersionDraft15);
  EXPECT_TRUE(result.hasValue()) << "Failed to write SERVER_SETUP";

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  // Skip frame type
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SERVER_SETUP));

  // Skip frame length
  cursor.skip(2);

  // Next field should be number of params (not version field)
  auto numParams = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_TRUE(numParams.has_value());
  EXPECT_EQ(numParams->first, 0) << "Should have 0 params";

  // Verify we're at end of message (no version field was written)
  EXPECT_FALSE(cursor.canAdvance(1))
      << "No additional data should be present (version field not written)";
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

TEST(MoQFramerTest, ClientSetupRejectsDelete) {
  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraftCurrent);
  parser.setTokenCacheMaxSize(100);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Write CLIENT_SETUP header
  writeVarint(writeBuf, 1, size, error); // 1 version
  writeVarint(writeBuf, kVersionDraftCurrent, size, error);
  writeVarint(writeBuf, 1, size, error); // 1 parameter

  // Write AUTHORIZATION_TOKEN parameter with DELETE alias type
  writeVarint(
      writeBuf,
      folly::to_underlying(SetupKey::AUTHORIZATION_TOKEN),
      size,
      error);

  // Token content
  folly::IOBufQueue tokenBuf{folly::IOBufQueue::cacheChainLength()};
  size_t tokenSize = 0;
  writeVarint(
      tokenBuf, folly::to_underlying(AliasType::DELETE), tokenSize, error);
  writeVarint(tokenBuf, 42, tokenSize, error); // alias=42

  // Write token length
  writeVarint(writeBuf, tokenSize, size, error);
  // Write token content
  auto tokenChain = tokenBuf.move();
  writeBuf.append(std::move(tokenChain));
  size += tokenSize;

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PROTOCOL_VIOLATION)
      << "CLIENT_SETUP must reject DELETE (0x0) alias type";
}

TEST(MoQFramerTest, ClientSetupRejectsUseAlias) {
  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraftCurrent);
  parser.setTokenCacheMaxSize(100);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Write CLIENT_SETUP header
  writeVarint(writeBuf, 1, size, error); // 1 version
  writeVarint(writeBuf, kVersionDraftCurrent, size, error);
  writeVarint(writeBuf, 1, size, error); // 1 parameter

  // Write AUTHORIZATION_TOKEN parameter with USE_ALIAS alias type
  writeVarint(
      writeBuf,
      folly::to_underlying(SetupKey::AUTHORIZATION_TOKEN),
      size,
      error);

  // Token content
  folly::IOBufQueue tokenBuf{folly::IOBufQueue::cacheChainLength()};
  size_t tokenSize = 0;
  writeVarint(
      tokenBuf, folly::to_underlying(AliasType::USE_ALIAS), tokenSize, error);
  writeVarint(tokenBuf, 99, tokenSize, error); // alias=99

  // Write token length
  writeVarint(writeBuf, tokenSize, size, error);
  // Write token content
  auto tokenChain = tokenBuf.move();
  writeBuf.append(std::move(tokenChain));
  size += tokenSize;

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto result =
      parser.parseClientSetup(cursor, buffer->computeChainDataLength());
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PROTOCOL_VIOLATION)
      << "CLIENT_SETUP must reject USE_ALIAS (0x2) alias type";
}
// Helper to write a datagram to a buffer
static void writeDatagram(
    folly::IOBufQueue& writeBuf,
    DatagramType dgType,
    uint64_t trackAlias,
    uint64_t group,
    std::optional<uint64_t> objectId,
    std::optional<uint8_t> priority,
    std::optional<ObjectStatus> status,
    const std::string& payload = "") {
  size_t size = 0;
  bool error = false;
  writeVarint(writeBuf, folly::to_underlying(dgType), size, error); // type
  writeVarint(writeBuf, trackAlias, size, error); // track alias
  writeVarint(writeBuf, group, size, error);      // group
  if (objectId.has_value()) {
    writeVarint(writeBuf, *objectId, size, error); // object ID
  }
  if (priority.has_value()) {
    folly::io::QueueAppender appender(&writeBuf, 1);
    appender.writeBE<uint8_t>(*priority); // priority
  }
  if (status.has_value()) {
    writeVarint(writeBuf, folly::to_underlying(*status), size, error); // status
  }
  if (!payload.empty()) {
    writeBuf.append(folly::IOBuf::copyBuffer(payload));
  }
}

// Helper to parse and check datagram header
static auto parseAndCheckDatagram(
    MoQFrameParser& parser,
    folly::IOBuf* buf,
    DatagramType expectedType,
    uint64_t expectedTrackAlias,
    uint64_t expectedGroup,
    uint64_t expectedId,
    folly::Optional<uint8_t> expectedPriority,
    ObjectStatus expectedStatus,
    std::optional<uint64_t> expectedLength = std::nullopt) {
  folly::io::Cursor cursor(buf);
  auto parsedType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_TRUE(parsedType.has_value());
  EXPECT_EQ(parsedType->first, folly::to_underlying(expectedType));
  auto length = cursor.totalLength();
  auto parseResult = parser.parseDatagramObjectHeader(
      cursor, DatagramType(parsedType->first), length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->trackAlias, TrackAlias(expectedTrackAlias));
  EXPECT_EQ(parseResult->objectHeader.group, expectedGroup);
  EXPECT_EQ(parseResult->objectHeader.id, expectedId);
  EXPECT_EQ(parseResult->objectHeader.priority, expectedPriority);
  EXPECT_EQ(parseResult->objectHeader.status, expectedStatus);
  if (expectedLength.has_value()) {
    EXPECT_EQ(parseResult->objectHeader.length, *expectedLength);
  }
  return parseResult;
}

// Test datagram types without priority (v15+)
TEST(MoQFramerTest, DatagramWithoutPriority) {
  uint64_t version = kVersionDraft15;
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  MoQFrameParser parser;
  parser.initializeVersion(version);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto dgType = getDatagramType(
      version, false, false, false, false, false); // priority NOT present
  EXPECT_EQ(dgType, DatagramType::OBJECT_DATAGRAM_NO_EXT_NO_PRI);

  writeDatagram(
      writeBuf, dgType, 22, 33, 44, std::nullopt, std::nullopt, "payload");
  auto serialized = writeBuf.move();
  parseAndCheckDatagram(
      parser,
      serialized.get(),
      dgType,
      22,
      33,
      44,
      folly::none,
      ObjectStatus::NORMAL,
      7);
}

// Test datagram with priority present in v15
TEST(MoQFramerTest, DatagramWithPriority) {
  uint64_t version = kVersionDraft15;
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  MoQFrameParser parser;
  parser.initializeVersion(version);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto dgType = getDatagramType(
      version, false, false, false, false, true); // priority present
  EXPECT_EQ(dgType, DatagramType::OBJECT_DATAGRAM_NO_EXT);

  writeDatagram(writeBuf, dgType, 22, 33, 44, 200, std::nullopt, "payload");
  auto serialized = writeBuf.move();
  parseAndCheckDatagram(
      parser, serialized.get(), dgType, 22, 33, 44, 200, ObjectStatus::NORMAL);
}

// Test status datagram with Object ID
TEST(MoQFramerTest, StatusDatagramWithObjectID) {
  uint64_t version = kVersionDraft15;
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  MoQFrameParser parser;
  parser.initializeVersion(version);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto dgType = getDatagramType(
      version,
      true,
      false,
      false,
      true,
      true); // status, object ID zero, priority present
  EXPECT_EQ(dgType, DatagramType::OBJECT_DATAGRAM_STATUS_ID_ZERO);

  writeDatagram(
      writeBuf,
      dgType,
      22,
      33,
      std::nullopt, // object ID not on wire (zero)
      100,
      ObjectStatus::OBJECT_NOT_EXIST);
  auto serialized = writeBuf.move();
  parseAndCheckDatagram(
      parser,
      serialized.get(),
      dgType,
      22,
      33,
      0,
      100,
      ObjectStatus::OBJECT_NOT_EXIST,
      0);
}

// Test status datagram without priority
TEST(MoQFramerTest, StatusDatagramWithoutPriority) {
  uint64_t version = kVersionDraft15;
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  MoQFrameParser parser;
  parser.initializeVersion(version);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto dgType = getDatagramType(
      version,
      true,
      false,
      false,
      false,
      false); // status, object ID present, no priority
  EXPECT_EQ(dgType, DatagramType::OBJECT_DATAGRAM_STATUS_NO_PRI);

  writeDatagram(
      writeBuf, dgType, 22, 33, 55, std::nullopt, ObjectStatus::END_OF_GROUP);
  auto serialized = writeBuf.move();
  parseAndCheckDatagram(
      parser,
      serialized.get(),
      dgType,
      22,
      33,
      55,
      folly::none,
      ObjectStatus::END_OF_GROUP,
      0);
}

// Test that v14 doesn't support priority-less datagrams
TEST(MoQFramerTest, V14DoesNotSupportPriorityNotPresent) {
  uint64_t version = kVersionDraft14;
  auto dgType = getDatagramType(
      version,
      false,
      false,
      false,
      false,
      false); // priority NOT present (should be ignored in v14)
  EXPECT_EQ(dgType, DatagramType::OBJECT_DATAGRAM_NO_EXT);
}

// Test isValidDatagramType for v15 types
TEST(MoQFramerTest, ValidDatagramTypesV15) {
  uint64_t version = kVersionDraft15;
  // All payload types (0x00-0x0F) should be valid
  for (uint64_t type = 0x00; type <= 0x0F; ++type) {
    EXPECT_TRUE(isValidDatagramType(version, type))
        << "Type 0x" << std::hex << type << " should be valid";
  }
  // Status types (0x20-0x25, 0x28-0x2D) should be valid
  for (uint64_t type = 0x20; type <= 0x25; ++type) {
    EXPECT_TRUE(isValidDatagramType(version, type))
        << "Type 0x" << std::hex << type << " should be valid";
  }
  for (uint64_t type = 0x28; type <= 0x2D; ++type) {
    EXPECT_TRUE(isValidDatagramType(version, type))
        << "Type 0x" << std::hex << type << " should be valid";
  }
  // Invalid types should be rejected
  EXPECT_FALSE(isValidDatagramType(version, 0x10));
  EXPECT_FALSE(isValidDatagramType(version, 0x1F));
  EXPECT_FALSE(isValidDatagramType(version, 0x26));
  EXPECT_FALSE(isValidDatagramType(version, 0x27));
  EXPECT_FALSE(isValidDatagramType(version, 0x2E));
  EXPECT_FALSE(isValidDatagramType(version, 0x30));
}

// Test isValidDatagramType for v14 types
TEST(MoQFramerTest, ValidDatagramTypesV14) {
  uint64_t version = kVersionDraft14;
  // Only types 0x00-0x07 and 0x20-0x21 should be valid
  for (uint64_t type = 0x00; type <= 0x07; ++type) {
    EXPECT_TRUE(isValidDatagramType(version, type))
        << "Type 0x" << std::hex << type << " should be valid in v14";
  }
  EXPECT_TRUE(isValidDatagramType(version, 0x20));
  EXPECT_TRUE(isValidDatagramType(version, 0x21));
  // Types 0x08-0x0F should NOT be valid in v14
  for (uint64_t type = 0x08; type <= 0x0F; ++type) {
    EXPECT_FALSE(isValidDatagramType(version, type))
        << "Type 0x" << std::hex << type << " should NOT be valid in v14";
  }
  // Status types with Object ID should NOT be valid in v14
  EXPECT_FALSE(isValidDatagramType(version, 0x24));
  EXPECT_FALSE(isValidDatagramType(version, 0x25));
  EXPECT_FALSE(isValidDatagramType(version, 0x28));
}

TEST(MoQFramerTestUtils, IsValidSubgroupTypeSetBased) {
  static const std::set<uint64_t> validV14 = {
      0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D};
  static const std::set<uint64_t> validV15 = {
      0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D};

  uint64_t version14 = kVersionDraft14;
  uint64_t version15 = kVersionDraft15;

  for (uint64_t t = 0; t <= 255; ++t) {
    bool shouldBeValidV14 = validV14.count(t) > 0;
    EXPECT_EQ(isValidSubgroupType(version14, t), shouldBeValidV14)
        << "v14: 0x" << std::hex << t;
    bool shouldBeValidV15 = validV15.count(t) > 0;
    EXPECT_EQ(isValidSubgroupType(version15, t), shouldBeValidV15)
        << "v15: 0x" << std::hex << t;
  }
}

// Helper for round-trip datagram test
void testDatagramPriorityRoundTrip(
    uint64_t version,
    folly::Optional<uint8_t> priority,
    DatagramType expectedType,
    folly::Optional<uint8_t> expectedPriority) {
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  MoQFrameParser parser;
  parser.initializeVersion(version);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader objHeader = {
      100, 0, 200, priority, ObjectStatus::NORMAL, noExtensions(), 7};
  auto result = writer.writeDatagramObject(
      writeBuf, TrackAlias(50), objHeader, folly::IOBuf::copyBuffer("payload"));
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto parsedType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_TRUE(parsedType.has_value());
  EXPECT_EQ(parsedType->first, folly::to_underlying(expectedType));
  auto length = cursor.totalLength();
  auto parseResult = parser.parseDatagramObjectHeader(
      cursor, DatagramType(parsedType->first), length);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->trackAlias, TrackAlias(50));
  EXPECT_EQ(parseResult->objectHeader.group, 100);
  EXPECT_EQ(parseResult->objectHeader.id, 200);
  EXPECT_EQ(parseResult->objectHeader.priority, expectedPriority);
  EXPECT_EQ(parseResult->objectHeader.status, ObjectStatus::NORMAL);
}

// Then each test becomes a one-liner:
TEST(MoQFramerTest, OptionalPriorityDatagramRoundTripNone) {
  testDatagramPriorityRoundTrip(
      kVersionDraft15,
      folly::none,
      DatagramType::OBJECT_DATAGRAM_NO_EXT_NO_PRI,
      folly::none);
}
TEST(MoQFramerTest, OptionalPriorityDatagramRoundTripValue) {
  testDatagramPriorityRoundTrip(
      kVersionDraft15, 64, DatagramType::OBJECT_DATAGRAM_NO_EXT, 64);
}

// Helper for round-trip subgroup header test
void testSubgroupPriorityRoundTrip(
    uint64_t version,
    folly::Optional<uint8_t> priority,
    StreamType expectedType,
    folly::Optional<uint8_t> expectedPriority) {
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  MoQFrameParser parser;
  parser.initializeVersion(version);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  ObjectHeader objHeader = {
      100, 50, 200, priority, ObjectStatus::NORMAL, noExtensions(), 0};

  auto result = writer.writeSubgroupHeader(
      writeBuf, TrackAlias(25), objHeader, SubgroupIDFormat::Present, false);
  EXPECT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto parsedStreamType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_TRUE(parsedStreamType.has_value());
  auto streamType = StreamType(parsedStreamType->first);
  EXPECT_EQ(streamType, expectedType);
  auto sgOptions = getSubgroupOptions(version, streamType);
  auto parseResult =
      parser.parseSubgroupHeader(cursor, cursor.totalLength(), sgOptions);
  EXPECT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->value.trackAlias, TrackAlias(25));
  EXPECT_EQ(parseResult->value.objectHeader.group, 100);
  EXPECT_EQ(parseResult->value.objectHeader.subgroup, 50);
  EXPECT_EQ(parseResult->value.objectHeader.priority, expectedPriority);
}

// Test round-trip write/read with folly::none priority in subgroup (v15)
TEST(MoQFramerTest, OptionalPrioritySubgroupRoundTripNone) {
  testSubgroupPriorityRoundTrip(
      kVersionDraft15,
      folly::none,
      StreamType::SUBGROUP_HEADER_SG_NO_PRI,
      folly::none);
}

// Test round-trip write/read with explicit priority in subgroup (v15)
TEST(MoQFramerTest, OptionalPrioritySubgroupRoundTripValue) {
  testSubgroupPriorityRoundTrip(
      kVersionDraft15, 80, StreamType::SUBGROUP_HEADER_SG, 80);
}

// Test class for GroupOrder defaults feature (draft 15+)
// In v15+, GROUP_ORDER is passed as a parameter and parser uses defaults
class MoQFramerV15PlusTest : public ::testing::TestWithParam<uint64_t> {
 public:
  void SetUp() override {
    parser_.initializeVersion(GetParam());
    writer_.initializeVersion(GetParam());
  }

 protected:
  MoQFrameParser parser_;
  MoQFrameWriter writer_;

  size_t frameLength(folly::io::Cursor& cursor, bool checkAdvance = true) {
    if (!cursor.canAdvance(2)) {
      throw std::runtime_error("Cannot read frame length");
    }
    size_t res = cursor.readBE<uint16_t>();
    if (checkAdvance && !cursor.canAdvance(res)) {
      throw std::runtime_error("Frame length exceeds available data");
    }
    return res;
  }
};

// Test default GroupOrder for SubscribeRequest when param not present
TEST_P(MoQFramerV15PlusTest, SubscribeRequestDefaultGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeRequest req = SubscribeRequest::make(
      FullTrackName({TrackNamespace({"ns"}), "track"}),
      /*priority*/ 128,
      /*groupOrder*/ GroupOrder::Default, // Writer won't write GROUP_ORDER
                                          // param
      /*forward*/ true,
      /*locType*/ LocationType::LargestObject,
      /*start*/ folly::none,
      /*endGroup*/ 0,
      /*params*/ {});

  auto writeResult = writer_.writeSubscribeRequest(writeBuf, req);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));

  auto parseResult = parser_.parseSubscribeRequest(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // When GROUP_ORDER param is not written, parser should set to Default
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::Default);
}

// Test explicit GroupOrder param overrides default for SubscribeRequest
TEST_P(MoQFramerV15PlusTest, SubscribeRequestExplicitGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeRequest req = SubscribeRequest::make(
      FullTrackName({TrackNamespace({"ns"}), "track"}),
      /*priority*/ 128,
      /*groupOrder*/ GroupOrder::NewestFirst, // Non-default, writer will write
                                              // it
      /*forward*/ true,
      /*locType*/ LocationType::LargestObject,
      /*start*/ folly::none,
      /*endGroup*/ 0,
      /*params*/ {});

  auto writeResult = writer_.writeSubscribeRequest(writeBuf, req);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));

  auto parseResult = parser_.parseSubscribeRequest(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // Explicit value should be preserved
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::NewestFirst);
}

// Test default GroupOrder for SubscribeOk when param not present
TEST_P(MoQFramerV15PlusTest, SubscribeOkDefaultGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeOk subscribeOk;
  subscribeOk.requestID = RequestID(42);
  subscribeOk.trackAlias = TrackAlias(1);
  subscribeOk.expires = std::chrono::milliseconds(1000);
  subscribeOk.groupOrder =
      GroupOrder::Default; // Writer won't write GROUP_ORDER param
  subscribeOk.largest = AbsoluteLocation{10, 20};
  subscribeOk.params = {};

  auto writeResult = writer_.writeSubscribeOk(writeBuf, subscribeOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_OK));

  auto parseResult = parser_.parseSubscribeOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // When GROUP_ORDER param is not written, parser should set to OldestFirst
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::OldestFirst);
}

// Test explicit GroupOrder param overrides default for SubscribeOk
TEST_P(MoQFramerV15PlusTest, SubscribeOkExplicitGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeOk subscribeOk;
  subscribeOk.requestID = RequestID(42);
  subscribeOk.trackAlias = TrackAlias(1);
  subscribeOk.expires = std::chrono::milliseconds(1000);
  subscribeOk.groupOrder =
      GroupOrder::NewestFirst; // Non-default, will be written
  subscribeOk.largest = AbsoluteLocation{10, 20};
  subscribeOk.params = {};

  auto writeResult = writer_.writeSubscribeOk(writeBuf, subscribeOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_OK));

  auto parseResult = parser_.parseSubscribeOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // Explicit value should be preserved
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::NewestFirst);
}

TEST_P(MoQFramerV15PlusTest, SubscribeOkExpiresParameter) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeOk subscribeOk;
  subscribeOk.requestID = RequestID(42);
  subscribeOk.trackAlias = TrackAlias(1);
  subscribeOk.expires = std::chrono::milliseconds(5000);
  subscribeOk.groupOrder = GroupOrder::OldestFirst;
  subscribeOk.largest = AbsoluteLocation{10, 20};
  subscribeOk.params = {};

  auto writeResult = writer_.writeSubscribeOk(writeBuf, subscribeOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_OK));

  auto parseResult = parser_.parseSubscribeOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // Verify expires is correctly parsed from parameter
  EXPECT_EQ(parseResult->expires, std::chrono::milliseconds(5000));
  EXPECT_EQ(parseResult->requestID, RequestID(42));
  EXPECT_EQ(parseResult->trackAlias, TrackAlias(1));
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::OldestFirst);
}

TEST_P(MoQFramerV15PlusTest, SubscribeOkExpiresZeroNotWritten) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeOk subscribeOk;
  subscribeOk.requestID = RequestID(42);
  subscribeOk.trackAlias = TrackAlias(1);
  subscribeOk.expires = std::chrono::milliseconds(0); // Zero expires
  subscribeOk.groupOrder = GroupOrder::NewestFirst;
  subscribeOk.largest = AbsoluteLocation{10, 20};
  subscribeOk.params = {};

  auto writeResult = writer_.writeSubscribeOk(writeBuf, subscribeOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_OK));

  auto parseResult = parser_.parseSubscribeOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // When EXPIRES param not written (value=0), parser should default to 0
  EXPECT_EQ(parseResult->expires, std::chrono::milliseconds(0));
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::NewestFirst);
}

// Test default GroupOrder for Publish when param not present
TEST_P(MoQFramerV15PlusTest, PublishDefaultGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  PublishRequest publishRequest;
  publishRequest.requestID = RequestID(100);
  publishRequest.fullTrackName =
      FullTrackName({TrackNamespace({"test"}), "pub"});
  publishRequest.groupOrder =
      GroupOrder::Default;    // Will be overridden by parser default
  publishRequest.params = {}; // No GROUP_ORDER param

  auto writeResult = writer_.writePublish(writeBuf, publishRequest);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH));

  auto parseResult = parser_.parsePublish(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // When GROUP_ORDER param is not in params, parser should set to OldestFirst
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::OldestFirst);
}

// Test explicit GroupOrder param overrides default for Publish
TEST_P(MoQFramerV15PlusTest, PublishExplicitGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  PublishRequest publishRequest;
  publishRequest.requestID = RequestID(100);
  publishRequest.fullTrackName =
      FullTrackName({TrackNamespace({"test"}), "pub"});
  publishRequest.groupOrder =
      GroupOrder::NewestFirst; // Non-default, will be written
  publishRequest.params = {};

  auto writeResult = writer_.writePublish(writeBuf, publishRequest);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH));

  auto parseResult = parser_.parsePublish(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // Explicit value should be preserved
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::NewestFirst);
}

// Test default GroupOrder for PublishOk when param not present
TEST_P(MoQFramerV15PlusTest, PublishOkDefaultGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  PublishOk publishOk;
  publishOk.requestID = RequestID(200);
  publishOk.forward = true;
  publishOk.subscriberPriority = 128;
  publishOk.groupOrder =
      GroupOrder::Default; // Writer won't write GROUP_ORDER param
  publishOk.locType = LocationType::LargestObject;
  publishOk.params = {};

  auto writeResult = writer_.writePublishOk(writeBuf, publishOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH_OK));

  auto parseResult = parser_.parsePublishOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // When GROUP_ORDER param is not written, parser should set to Default
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::Default);
}

// Test explicit GroupOrder param overrides default for PublishOk
TEST_P(MoQFramerV15PlusTest, PublishOkExplicitGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  PublishOk publishOk;
  publishOk.requestID = RequestID(200);
  publishOk.forward = true;
  publishOk.subscriberPriority = 128;
  publishOk.groupOrder =
      GroupOrder::OldestFirst; // Non-default, will be written
  publishOk.locType = LocationType::LargestObject;
  publishOk.params = {};

  auto writeResult = writer_.writePublishOk(writeBuf, publishOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH_OK));

  auto parseResult = parser_.parsePublishOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // Explicit value should be preserved
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::OldestFirst);
}

// Test default GroupOrder for Fetch when param not present
TEST_P(MoQFramerV15PlusTest, FetchDefaultGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  Fetch fetchRequest(
      RequestID(300),
      FullTrackName({TrackNamespace({"test"}), "fetch"}),
      AbsoluteLocation{5, 10},  // start
      AbsoluteLocation{15, 20}, // end
      kDefaultPriority,         // priority
      GroupOrder::Default);     // Writer won't write GROUP_ORDER param

  auto writeResult = writer_.writeFetch(writeBuf, fetchRequest);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::FETCH));

  auto parseResult = parser_.parseFetch(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // When GROUP_ORDER param is not written, parser should set to OldestFirst
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::OldestFirst);
}

// Test explicit GroupOrder param overrides default for Fetch
TEST_P(MoQFramerV15PlusTest, FetchExplicitGroupOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  Fetch fetchRequest(
      RequestID(300),
      FullTrackName({TrackNamespace({"test"}), "fetch"}),
      AbsoluteLocation{5, 10},  // start
      AbsoluteLocation{15, 20}, // end
      kDefaultPriority,         // priority
      GroupOrder::NewestFirst); // Non-default, will be written

  auto writeResult = writer_.writeFetch(writeBuf, fetchRequest);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::FETCH));

  auto parseResult = parser_.parseFetch(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // Explicit value should be preserved
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::NewestFirst);
}

INSTANTIATE_TEST_SUITE_P(
    MoQFramerV15PlusTest,
    MoQFramerV15PlusTest,
    ::testing::Values(kVersionDraft15));
