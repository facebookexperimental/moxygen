/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQFramer.h"
#include "moxygen/MoQVarint.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include "moxygen/MoQTrackProperties.h"
#include "moxygen/test/TestUtils.h"

using namespace moxygen;

namespace {

// Helper to write a QUIC varint to an IOBufQueue succinctly
inline void writeVarintTo(folly::IOBufQueue& q, uint64_t v) {
  folly::io::QueueAppender appender(&q, kMaxFrameHeaderSize);
  auto appenderOp = [appender = std::move(appender)](auto val) mutable {
    appender.writeBE(folly::tag<decltype(val)>, val);
  };
  (void)quic::encodeQuicInteger(v, appenderOp);
}

inline void writeMoQVarintTo(folly::IOBufQueue& q, uint64_t v) {
  folly::io::QueueAppender appender(&q, kMaxFrameHeaderSize);
  (void)encodeMoQVarint(v, appender);
}

uint64_t readVarintFrom(folly::io::Cursor& cursor) {
  auto value = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_TRUE(value.has_value());
  return value.has_value() ? value->first : 0;
}

void writeFetchObjectWithSerializationFlags(
    folly::IOBufQueue& q,
    uint64_t flags,
    std::optional<uint64_t> groupField,
    std::optional<uint64_t> subgroupField,
    std::optional<uint64_t> objectField,
    std::optional<uint8_t> priority,
    uint64_t payloadLength = 1) {
  writeMoQVarintTo(q, flags);
  if (groupField.has_value()) {
    writeMoQVarintTo(q, *groupField);
  }
  if (subgroupField.has_value()) {
    writeMoQVarintTo(q, *subgroupField);
  }
  if (objectField.has_value()) {
    writeMoQVarintTo(q, *objectField);
  }
  if (priority.has_value()) {
    q.append(&*priority, 1);
  }
  writeMoQVarintTo(q, payloadLength);
}

// Build a legacy CLIENT_SETUP frame (versions array + 0 params).
// Returns the IOBuf and positions a cursor at the payload (past frame header).
// Usage: auto [buf, len] = makeLegacyClientSetupFrame({kVersionDraft14});
//        auto result = parser.parseClientSetup(cursor, len);
std::pair<std::unique_ptr<folly::IOBuf>, size_t> makeLegacyClientSetupFrame(
    const std::vector<uint64_t>& versions) {
  folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
  size_t payloadSize = 0;
  bool error = false;
  writeVarint(payloadBuf, versions.size(), payloadSize, error);
  for (auto v : versions) {
    writeVarint(payloadBuf, v, payloadSize, error);
  }
  writeVarint(payloadBuf, 0, payloadSize, error); // num_params

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::CLIENT_SETUP), size, error);
  writeVarint(writeBuf, payloadSize, size, error);
  writeBuf.append(payloadBuf.move());
  return {writeBuf.move(), payloadSize};
}

// Build a legacy SERVER_SETUP frame (selected version + 0 params).
std::pair<std::unique_ptr<folly::IOBuf>, size_t> makeLegacyServerSetupFrame(
    uint64_t selectedVersion) {
  folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
  size_t payloadSize = 0;
  bool error = false;
  writeVarint(payloadBuf, selectedVersion, payloadSize, error);
  writeVarint(payloadBuf, 0, payloadSize, error); // num_params

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::SERVER_SETUP), size, error);
  writeVarint(writeBuf, payloadSize, size, error);
  writeBuf.append(payloadBuf.move());
  return {writeBuf.move(), payloadSize};
}

// Skip the frame type and length varints; return payload length.
size_t skipFrameHeader(folly::io::Cursor& cursor) {
  quic::follyutils::decodeQuicInteger(cursor); // frame type
  return quic::follyutils::decodeQuicInteger(cursor)->first;
}

class TestUnderflow : public std::exception {};

// The parameter is the MoQ version
class MoQFramerTest : public ::testing::TestWithParam<uint64_t> {
 public:
  void SetUp() override {
    parser_.initializeVersion(GetParam());
    writer_.initializeVersion(GetParam());
    parser_.setTokenCache(&tokenCache_);
  }

  StreamType parseStreamType(folly::io::Cursor& cursor) {
    auto frameType = parser_.decodeVarint(cursor);
    if (!frameType) {
      throw TestUnderflow();
    }
    return StreamType(frameType->first);
  }

  DatagramType parseDatagramType(folly::io::Cursor& cursor) {
    auto frameType = parser_.decodeVarint(cursor);
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

  // Consume a varint-encoded frame type using the version-aware parser so
  // tests work across QUIC-varint (drafts <17) and MoQ-varint (drafts >=17).
  FrameType skipFrameType(folly::io::Cursor& cursor) {
    auto frameType = parser_.decodeVarint(cursor);
    if (!frameType) {
      throw TestUnderflow();
    }
    return FrameType(frameType->first);
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
    skipFrameType(cursor);
    auto r1 = parser_.parseClientSetup(cursor, frameLength(cursor));
    testUnderflowResult(r1);

    skipFrameType(cursor);
    auto r2 = parser_.parseServerSetup(cursor, frameLength(cursor));
    testUnderflowResult(r2);

    skipFrameType(cursor);
    auto r3 = parser_.parseSubscribeRequest(cursor, frameLength(cursor));
    testUnderflowResult(r3);

    skipFrameType(cursor);
    auto r3a = parser_.parseRequestUpdate(cursor, frameLength(cursor));
    testUnderflowResult(r3a);

    skipFrameType(cursor);
    auto r4 = parser_.parseSubscribeOk(cursor, frameLength(cursor));
    testUnderflowResult(r4);

    skipFrameType(cursor);
    auto r4a = parser_.parseMaxRequestID(cursor, frameLength(cursor));
    testUnderflowResult(r4a);

    skipFrameType(cursor);
    auto r4b = parser_.parseRequestsBlocked(cursor, frameLength(cursor));
    testUnderflowResult(r4b);

    skipFrameType(cursor);
    auto r5 = parser_.parseRequestError(
        cursor, frameLength(cursor), FrameType::SUBSCRIBE_ERROR);
    testUnderflowResult(r5);

    skipFrameType(cursor);
    auto r6 = parser_.parseUnsubscribe(cursor, frameLength(cursor));
    testUnderflowResult(r6);

    skipFrameType(cursor);
    auto r7 = parser_.parsePublishDone(cursor, frameLength(cursor));
    testUnderflowResult(r7);

    skipFrameType(cursor);
    auto r8a = parser_.parsePublish(cursor, frameLength(cursor));
    testUnderflowResult(r8a);
    EXPECT_TRUE(getPublisherPriority(*r8a).has_value());

    auto publishOkFrameType = skipFrameType(cursor);
    if (getDraftMajorVersion(GetParam()) >= 18) {
      EXPECT_EQ(publishOkFrameType, FrameType::REQUEST_OK);
      auto r8b = parser_.parseRequestOk(
          cursor, frameLength(cursor), FrameType::REQUEST_OK);
      testUnderflowResult(r8b);
    } else {
      auto r8b = parser_.parsePublishOk(cursor, frameLength(cursor));
      testUnderflowResult(r8b);
    }

    skipFrameType(cursor);
    auto r8c = parser_.parseRequestError(
        cursor, frameLength(cursor), FrameType::PUBLISH_ERROR);
    testUnderflowResult(r8c);

    skipFrameType(cursor);
    auto r9 = parser_.parsePublishNamespace(cursor, frameLength(cursor));
    testUnderflowResult(r9);

    skipFrameType(cursor);
    auto r10 = parser_.parsePublishNamespaceOk(cursor, frameLength(cursor));
    testUnderflowResult(r10);

    skipFrameType(cursor);
    auto r11 = parser_.parseRequestError(
        cursor, frameLength(cursor), FrameType::PUBLISH_NAMESPACE_ERROR);
    testUnderflowResult(r11);

    skipFrameType(cursor);
    auto r12 = parser_.parsePublishNamespaceCancel(cursor, frameLength(cursor));
    testUnderflowResult(r12);

    skipFrameType(cursor);
    auto r13 = parser_.parsePublishNamespaceDone(cursor, frameLength(cursor));
    testUnderflowResult(r13);

    skipFrameType(cursor);
    auto r14a = parser_.parseTrackStatus(cursor, frameLength(cursor));
    testUnderflowResult(r14a);

    skipFrameType(cursor);
    if (getDraftMajorVersion(GetParam()) < 15) {
      auto r14b = parser_.parseTrackStatusOk(cursor, frameLength(cursor));
      testUnderflowResult(r14b);
    } else {
      auto r14b = parser_.parseRequestOk(
          cursor, frameLength(cursor), FrameType::REQUEST_OK);
      testUnderflowResult(r14b);
    }

    skipFrameType(cursor);
    auto r14 = parser_.parseGoaway(cursor, frameLength(cursor));
    testUnderflowResult(r14);

    // SubscribeNamespace messages are not on control stream for draft 16+
    if (getDraftMajorVersion(GetParam()) < 16) {
      skipFrameType(cursor);
      auto r9a = parser_.parseSubscribeNamespace(cursor, frameLength(cursor));
      testUnderflowResult(r9a);

      skipFrameType(cursor);
      auto r10a =
          parser_.parseSubscribeNamespaceOk(cursor, frameLength(cursor));
      testUnderflowResult(r10a);

      skipFrameType(cursor);
      auto r11a = parser_.parseRequestError(
          cursor, frameLength(cursor), FrameType::SUBSCRIBE_NAMESPACE_ERROR);
      testUnderflowResult(r11a);

      skipFrameType(cursor);
      auto r13a =
          parser_.parseUnsubscribeNamespace(cursor, frameLength(cursor));
      testUnderflowResult(r13a);
    }

    skipFrameType(cursor);
    auto r16 = parser_.parseFetch(cursor, frameLength(cursor));
    testUnderflowResult(r16);

    skipFrameType(cursor);
    auto r17 = parser_.parseFetchCancel(cursor, frameLength(cursor));
    testUnderflowResult(r17);

    skipFrameType(cursor);
    auto r18 = parser_.parseFetchOk(cursor, frameLength(cursor));
    testUnderflowResult(r18);

    skipFrameType(cursor);
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
    EXPECT_EQ(r20->value.status, ObjectStatus::END_OF_GROUP);
    // END_OF_GROUP terminates the subgroup - no more objects to parse

    skipFrameType(cursor);
    auto r21 = parser_.parseFetchHeader(cursor, cursor.totalLength());
    testUnderflowResult(r21);
    EXPECT_EQ(r21->value, RequestID(1));

    ObjectHeader obj;
    // Fetch context uses placeholder TrackAlias(0)
    auto r22 =
        parser_.parseFetchObjectHeader(cursor, cursor.totalLength(), obj);
    testUnderflowResult(r22);
    ASSERT_TRUE(std::holds_alternative<ObjectHeader>(r22->value));
    auto& r22obj = std::get<ObjectHeader>(r22->value);
    EXPECT_EQ(r22obj.id, 4);
    skip(cursor, *r22obj.length);

    auto r22a =
        parser_.parseFetchObjectHeader(cursor, cursor.totalLength(), obj);
    testUnderflowResult(r22a);
    ASSERT_TRUE(std::holds_alternative<ObjectHeader>(r22a->value));
    auto& r22aobj = std::get<ObjectHeader>(r22a->value);
    EXPECT_EQ(r22aobj.id, 5);
    EXPECT_EQ(r22aobj.extensions, Extensions(test::getTestExtensions(), {}));
    skip(cursor, *r22aobj.length);

    auto r23 =
        parser_.parseFetchObjectHeader(cursor, cursor.totalLength(), obj);
    testUnderflowResult(r23);
    ASSERT_TRUE(std::holds_alternative<ObjectHeader>(r23->value));
    auto& r23obj = std::get<ObjectHeader>(r23->value);
    EXPECT_EQ(r23obj.status, ObjectStatus::END_OF_GROUP);

    auto r23a =
        parser_.parseFetchObjectHeader(cursor, cursor.totalLength(), obj);
    testUnderflowResult(r23a);
    ASSERT_TRUE(std::holds_alternative<ObjectHeader>(r23a->value));
    auto& r23aobj = std::get<ObjectHeader>(r23a->value);
    EXPECT_EQ(r23aobj.extensions, Extensions({}, {}));
    EXPECT_EQ(r23aobj.status, ObjectStatus::END_OF_GROUP);
  }

 protected:
  MoQFrameParser parser_;
  MoQFrameWriter writer_;
  MoQTokenCache tokenCache_;

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

TEST(MoQFramerGoawayTest, Draft17RemainsUriOnly) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft17);
  Goaway goaway;
  goaway.newSessionUri = "/new-session";
  goaway.timeout = 1500;
  goaway.requestID = RequestID(6);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer.writeGoaway(writeBuf, goaway).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::GOAWAY));
  auto bodyLen = cursor.readBE<uint16_t>();
  EXPECT_EQ(bodyLen, goaway.newSessionUri.size() + 1);

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft17);
  auto parsed = parser.parseGoaway(cursor, bodyLen);
  ASSERT_TRUE(parsed.hasValue());
  EXPECT_EQ(parsed->newSessionUri, goaway.newSessionUri);
  EXPECT_EQ(parsed->timeout, 0);
  EXPECT_FALSE(parsed->requestID.has_value());
}

TEST(MoQFramerGoawayTest, Draft18RoundtripIncludesTimeoutAndRequestID) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  Goaway goaway;
  goaway.newSessionUri = "/new-session";
  goaway.timeout = 1500;
  goaway.requestID = RequestID(6);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer.writeGoaway(writeBuf, goaway).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::GOAWAY));
  auto bodyLen = cursor.readBE<uint16_t>();

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft18);
  auto parsed = parser.parseGoaway(cursor, bodyLen);
  ASSERT_TRUE(parsed.hasValue());
  EXPECT_EQ(parsed->newSessionUri, goaway.newSessionUri);
  EXPECT_EQ(parsed->timeout, goaway.timeout);
  ASSERT_TRUE(parsed->requestID.has_value());
  EXPECT_EQ(*parsed->requestID, *goaway.requestID);
}

TEST(MoQFramerGoawayTest, Draft18RequiresTimeoutAndRequestID) {
  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft18);

  folly::IOBufQueue uriOnly{folly::IOBufQueue::cacheChainLength()};
  const std::string uri = "/new-session";
  writeVarintTo(uriOnly, uri.size());
  uriOnly.append(uri);
  const auto uriOnlyLen = uriOnly.chainLength();
  auto uriOnlySerialized = uriOnly.move();
  folly::io::Cursor uriOnlyCursor(uriOnlySerialized.get());
  auto uriOnlyParsed = parser.parseGoaway(uriOnlyCursor, uriOnlyLen);
  ASSERT_TRUE(uriOnlyParsed.hasError());
  EXPECT_EQ(uriOnlyParsed.error(), ErrorCode::PARSE_UNDERFLOW);

  folly::IOBufQueue missingRequestID{folly::IOBufQueue::cacheChainLength()};
  writeVarintTo(missingRequestID, uri.size());
  missingRequestID.append(uri);
  writeVarintTo(missingRequestID, 1500);
  const auto missingRequestIDLen = missingRequestID.chainLength();
  auto missingRequestIDSerialized = missingRequestID.move();
  folly::io::Cursor missingRequestIDCursor(missingRequestIDSerialized.get());
  auto missingRequestIDParsed =
      parser.parseGoaway(missingRequestIDCursor, missingRequestIDLen);
  ASSERT_TRUE(missingRequestIDParsed.hasError());
  EXPECT_EQ(missingRequestIDParsed.error(), ErrorCode::PARSE_UNDERFLOW);
}

TEST_P(MoQFramerTest, ParseObjectHeader) {
  // Test OBJECT_DATAGRAM with ObjectStatus::END_OF_GROUP
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer_.writeDatagramObject(
      writeBuf,
      TrackAlias(22), // trackAlias
      {33,            // group
       0,             // subgroup
       44,            // id
       55,            // priority
       ObjectStatus::END_OF_GROUP,
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
  EXPECT_EQ(parseResult->objectHeader.status, ObjectStatus::END_OF_GROUP);
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
  // Hand-roll a CLIENT_SETUP frame with 2 versions: one unsupported, one
  // supported (draft-14), plus 2 params (MAX_REQUEST_ID=42, PATH="/foo/bar").
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  // Build the payload first to compute its length
  folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
  size_t payloadSize = 0;

  // num_versions = 2
  writeVarint(payloadBuf, 2, payloadSize, error);
  // version 1: unsupported (draft-03)
  writeVarint(payloadBuf, kVersionDraft03, payloadSize, error);
  // version 2: supported (draft-14)
  writeVarint(payloadBuf, kVersionDraft14, payloadSize, error);
  // num_params = 2
  writeVarint(payloadBuf, 2, payloadSize, error);
  // param 1: MAX_REQUEST_ID = 42 (even key -> varint value, no length prefix)
  writeVarint(
      payloadBuf,
      folly::to_underlying(SetupKey::MAX_REQUEST_ID),
      payloadSize,
      error);
  writeVarint(payloadBuf, 42, payloadSize, error);
  // param 2: PATH = "/foo/bar" (odd key -> length-prefixed string)
  writeVarint(
      payloadBuf, folly::to_underlying(SetupKey::PATH), payloadSize, error);
  std::string path = "/foo/bar";
  writeVarint(payloadBuf, path.size(), payloadSize, error);
  payloadBuf.append(path.data(), path.size());
  payloadSize += path.size();

  EXPECT_FALSE(error);

  // Write frame header: type + length
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::CLIENT_SETUP), size, error);
  writeVarint(writeBuf, payloadSize, size, error);
  writeBuf.append(payloadBuf.move());

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());
  MoQFrameParser parser;
  auto len = skipFrameHeader(cursor);

  auto result = parser.parseClientSetup(cursor, len);
  EXPECT_TRUE(result.hasValue())
      << "Parsing CLIENT_SETUP with mixed versions should succeed";
  // Check parameters
  ASSERT_EQ(result->params.size(), 2);
  auto it = result->params.begin();
  EXPECT_EQ(it->key, folly::to_underlying(SetupKey::MAX_REQUEST_ID));
  EXPECT_EQ(it->asUint64, 42);
  ++it;
  EXPECT_EQ(it->key, folly::to_underlying(SetupKey::PATH));
  EXPECT_EQ(it->asString, "/foo/bar");
}

TEST(MoQFramerTest, Draft16AllowsZeroMaxRequestIDSetupParam) {
  folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
  size_t payloadSize = 0;
  bool error = false;

  for (auto value :
       {uint64_t{1},
        folly::to_underlying(SetupKey::MAX_REQUEST_ID),
        uint64_t{0}}) {
    writeVarint(payloadBuf, value, payloadSize, error);
  }
  ASSERT_FALSE(error);

  auto payload = payloadBuf.move();
  for (auto parseSetup :
       {&MoQFrameParser::parseClientSetup, &MoQFrameParser::parseServerSetup}) {
    MoQFrameParser parser;
    parser.initializeVersion(kVersionDraft16);
    folly::io::Cursor cursor(payload.get());
    auto result = (parser.*parseSetup)(cursor, payloadSize);
    ASSERT_TRUE(result.hasValue());
    ASSERT_EQ(result->params.size(), 1);
    EXPECT_EQ(
        result->params.at(0).key,
        folly::to_underlying(SetupKey::MAX_REQUEST_ID));
    EXPECT_EQ(result->params.at(0).asUint64, 0);
  }
}

TEST(MoQFramerTest, ParseClientSetupWithOnlyUnsupportedVersionsFails) {
  auto [buf, len] = makeLegacyClientSetupFrame({kVersionDraft03});
  folly::io::Cursor cursor(buf.get());
  MoQFrameParser parser;
  skipFrameHeader(cursor);
  auto result = parser.parseClientSetup(cursor, len);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::VERSION_NEGOTIATION_FAILED);
}

TEST(MoQFramerTest, ParseClientSetupWithOnlyDraft15InLegacyModeFails) {
  // Client offers only draft-15 without ALPN — should fail because
  // legacy mode only supports draft-14
  auto [buf, len] = makeLegacyClientSetupFrame({kVersionDraft15});
  folly::io::Cursor cursor(buf.get());
  MoQFrameParser parser; // no ALPN set — legacy mode
  skipFrameHeader(cursor);
  auto result = parser.parseClientSetup(cursor, len);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::VERSION_NEGOTIATION_FAILED);
}

TEST(MoQFramerTest, ParseServerSetupWithNon14VersionInLegacyModeFails) {
  // SERVER_SETUP selecting draft-15 without ALPN — should error
  auto [buf, len] = makeLegacyServerSetupFrame(kVersionDraft15);
  folly::io::Cursor cursor(buf.get());
  MoQFrameParser parser; // no ALPN set — legacy mode
  skipFrameHeader(cursor);
  auto result = parser.parseServerSetup(cursor, len);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::VERSION_NEGOTIATION_FAILED);
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
  ObjectHeader obj(2, 3, 4, 5, ObjectStatus::END_OF_GROUP);
  writer_.writeDatagramObject(writeBuf, TrackAlias(1), obj, nullptr);

  auto pobj = testUnderflowDatagramHelper(writeBuf, true, false, 0);
  EXPECT_EQ(pobj.id, 4);
  EXPECT_EQ(pobj.status, ObjectStatus::END_OF_GROUP);
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
            appender.writeBE(folly::tag<decltype(val)>, val);
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
  // Test ObjectStatus::END_OF_GROUP
  expectedObjectHeader.status = ObjectStatus::END_OF_GROUP;
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
  EXPECT_EQ(parseResult->value.status, ObjectStatus::END_OF_GROUP);
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

  // Test ObjectStatus::END_OF_GROUP
  expectedObjectHeader.status = ObjectStatus::END_OF_GROUP;
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
  ASSERT_TRUE(std::holds_alternative<ObjectHeader>(parseResult->value));
  auto& obj1 = std::get<ObjectHeader>(parseResult->value);
  EXPECT_EQ(obj1.group, 33);
  EXPECT_EQ(obj1.id, 44);
  EXPECT_EQ(obj1.priority, 55);
  EXPECT_EQ(obj1.status, ObjectStatus::NORMAL);
  EXPECT_EQ(*obj1.length, 4);
  cursor.skip(*obj1.length);

  parseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  EXPECT_TRUE(parseResult.hasValue());
  ASSERT_TRUE(std::holds_alternative<ObjectHeader>(parseResult->value));
  auto& obj2 = std::get<ObjectHeader>(parseResult->value);
  EXPECT_EQ(obj2.group, 33);
  EXPECT_EQ(obj2.id, 44);
  EXPECT_EQ(obj2.priority, 55);
  EXPECT_EQ(obj2.status, ObjectStatus::END_OF_GROUP);
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
    moxygen::Setup clientSetup;
    clientSetup.params.insertParam(Parameter(
        folly::to_underlying(SetupKey::MAX_REQUEST_ID), maxRequestID));

    folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
    auto result = writeClientSetup(writeBuf, clientSetup, GetParam());
    EXPECT_TRUE(result.hasValue())
        << "Failed to write client setup for maxRequestID:" << maxRequestID;
    auto buffer = writeBuf.move();
    auto cursor = folly::io::Cursor(buffer.get());
    auto frameType = parser_.decodeVarint(cursor);
    uint64_t expectedFrameType = getDraftMajorVersion(GetParam()) >= 17
        ? folly::to_underlying(FrameType::SETUP)
        : folly::to_underlying(FrameType::CLIENT_SETUP);
    EXPECT_EQ(frameType->first, expectedFrameType);
    auto parseClientSetupResult =
        parser_.parseClientSetup(cursor, frameLength(cursor));
    EXPECT_TRUE(parseClientSetupResult.hasValue())
        << "Failed to parse client setup for maxRequestID:" << maxRequestID;
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

TEST(MoQFramerTest, ParsePublishOkOrderUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  writeVarint(writeBuf, 1, size, error); // requestID = 1
  writeBuf.append("\x01", 1);            // forwardFlag = 1 (true)
  writeBuf.append("\x80", 1);            // subscriberPriority = 128
                                         // omit order byte to trigger underflow

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft14);
  auto result = parser.parsePublishOk(cursor, buffer->computeChainDataLength());
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
      GetParam(),
      SubgroupIDFormat::FirstObject,
      /*includeExtensions=*/false,
      /*endOfGroup=*/false,
      /*priorityPresent=*/true,
      /*beginsWithFirstObject=*/true);
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

  // For v16+, params are sorted by key, so DELIVERY_TIMEOUT (2) comes before
  // AUTHORIZATION_TOKEN (3)
  if (getDraftMajorVersion(GetParam()) >= 16) {
    EXPECT_EQ(
        parseResult->params.at(0).key,
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT));
    EXPECT_EQ(parseResult->params.at(0).asUint64, 999);
    EXPECT_EQ(
        parseResult->params.at(1).key,
        folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN));
    EXPECT_EQ(parseResult->params.at(1).asAuthToken.tokenType, 0);
    EXPECT_EQ(parseResult->params.at(1).asAuthToken.tokenValue, "stampolli");
  } else {
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
}

// A DELIVERY_TIMEOUT of 0 ("no timeout") is only valid from draft 17 onward.
// Drafts <= 16 must reject a received 0 with PROTOCOL_VIOLATION and must not
// write one. (Draft 18 acceptance is covered by the session-level tests.)
static std::unique_ptr<folly::IOBuf> encodeTrackStatusWithDeliveryTimeout(
    uint64_t writerVersion,
    uint64_t deliveryTimeout) {
  MoQFrameWriter writer;
  writer.initializeVersion(writerVersion);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  TrackStatus ts =
      TrackStatus::make(FullTrackName({TrackNamespace({"hello"}), "world"}));
  ts.locType = LocationType::LargestObject;
  ts.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
      deliveryTimeout));
  EXPECT_TRUE(writer.writeTrackStatus(writeBuf, ts).hasValue());
  return writeBuf.move();
}

TEST(MoQFramerDeliveryTimeoutTest, Draft16RejectsZeroOnParse) {
  // Encode with a draft-17 writer (which permits 0 and shares draft 16's
  // parameter wire format), then confirm a draft-16 parser rejects it.
  auto bytes = encodeTrackStatusWithDeliveryTimeout(kVersionDraft17, 0);

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft16);
  MoQTokenCache tokenCache;
  parser.setTokenCache(&tokenCache);

  folly::io::Cursor cursor(bytes.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  ASSERT_EQ(frameType->first, folly::to_underlying(FrameType::TRACK_STATUS));
  auto frameLen = cursor.readBE<uint16_t>();
  auto parseResult = parser.parseTrackStatus(cursor, frameLen);
  ASSERT_TRUE(parseResult.hasError());
  EXPECT_EQ(parseResult.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST(MoQFramerDeliveryTimeoutTest, Draft17AcceptsZeroOnParse) {
  auto bytes = encodeTrackStatusWithDeliveryTimeout(kVersionDraft17, 0);

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft17);
  MoQTokenCache tokenCache;
  parser.setTokenCache(&tokenCache);

  folly::io::Cursor cursor(bytes.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  ASSERT_EQ(frameType->first, folly::to_underlying(FrameType::TRACK_STATUS));
  auto frameLen = cursor.readBE<uint16_t>();
  auto parseResult = parser.parseTrackStatus(cursor, frameLen);
  ASSERT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->params.size(), 1);
  EXPECT_EQ(
      parseResult->params.at(0).key,
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT));
  EXPECT_EQ(parseResult->params.at(0).asUint64, 0);
}

TEST(MoQFramerDeliveryTimeoutDeathTest, Draft16WriteZeroDies) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft16);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  TrackStatus ts =
      TrackStatus::make(FullTrackName({TrackNamespace({"hello"}), "world"}));
  ts.locType = LocationType::LargestObject;
  ts.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 0));
  EXPECT_DEATH(
      writer.writeTrackStatus(writeBuf, ts),
      "Cannot write a DELIVERY_TIMEOUT of 0 for draft versions <= 16");
}

TEST(MoQFramerDeliveryTimeoutTest, Draft16RejectsZeroExtensionOnParse) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft17);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  SubscribeOk subscribeOk;
  subscribeOk.requestID = RequestID(1);
  subscribeOk.trackAlias = TrackAlias(1);
  subscribeOk.expires = std::chrono::milliseconds(0);
  subscribeOk.groupOrder = GroupOrder::OldestFirst;
  subscribeOk.largest = AbsoluteLocation{0, 0};
  setPublisherDeliveryTimeout(subscribeOk, std::chrono::milliseconds(0));
  ASSERT_TRUE(writer.writeSubscribeOk(writeBuf, subscribeOk).hasValue());
  auto bytes = writeBuf.move();

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft16);
  MoQTokenCache tokenCache;
  parser.setTokenCache(&tokenCache);

  folly::io::Cursor cursor(bytes.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  ASSERT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_OK));
  auto frameLen = cursor.readBE<uint16_t>();
  auto parseResult = parser.parseSubscribeOk(cursor, frameLen);
  ASSERT_TRUE(parseResult.hasError());
  EXPECT_EQ(parseResult.error(), ErrorCode::PROTOCOL_VIOLATION);
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
  trackStatusOk.expires = std::chrono::milliseconds(1000);
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
  // Draft 18+ removed requestID from the REQUEST_OK wire format; it is
  // populated from the bidi stream context by the codec, not the parser.
  if (getDraftMajorVersion(GetParam()) < 18) {
    EXPECT_EQ(parseResult->requestID, 7);
  }
  EXPECT_EQ(parseResult->largest->group, 19);
  EXPECT_EQ(parseResult->largest->object, 77);
  EXPECT_EQ(parseResult->statusCode, TrackStatusCode::IN_PROGRESS);
  EXPECT_EQ(parseResult->expires, std::chrono::milliseconds(1000));
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
    case AliasType::DELETE_ALIAS:
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
      FullTrackName({TrackNamespace({"test"}), "track"}),
      kDefaultPriority,
      GroupOrder::OldestFirst,
      true,
      LocationType::LargestObject,
      std::nullopt,
      0};

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
      writeBuf, writer_, AliasType::DELETE_ALIAS, 0, 0, "");

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
      writeBufs[3], writer_, AliasType::DELETE_ALIAS, 3, 0, "");

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
      writeBufs[3], writer_, AliasType::DELETE_ALIAS, 0xff, 0, "");
  tokenLengths.push_back(len);

  // Encode `v` as a version-aware varint into a fresh IOBuf so the test can
  // splice it back into the serialized frame. Works on both QUIC-varint
  // (drafts <17) and MoQ-varint (drafts >=17) without the test knowing which.
  auto encodeVarintBuf = [&](uint64_t v) -> std::unique_ptr<folly::IOBuf> {
    folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};
    size_t s = 0;
    bool err = false;
    writer_.writeVarint(q, v, s, err);
    CHECK(!err);
    return q.move();
  };

  for (int j = 0; j < 4; ++j) {
    /*
     * Carve the serialized SUBSCRIBE frame into pieces so the test can fiddle
     * with the token-length field while keeping the rest of the frame intact.
     * The frame layout is [3-byte header][preamble][tokenLengthVarint][token].
     * The token bytes are always the tail because AUTH_TOKEN is the only param
     * and there is nothing after the parameter section.
     */
    auto frameHeader = writeBufs[j].split(3);
    // SUBSCRIBE preamble layout differs per draft. v14 had SUBSCRIPTION_FILTER
    // inline in the frame body; v15 moved it into a parameter (sorted after
    // AUTH_TOKEN); v16+ uses delta-encoded param keys so AUTH_TOKEN (key=3)
    // comes first; v17+ MoQ varint encodes most preamble values identically
    // because they are all <128. There is also a trailing param-section tail
    // after the AUTH_TOKEN value, so frontLength can't be derived purely from
    // tokenLength and totalSize.
    uint32_t frontLength;
    auto major = getDraftMajorVersion(GetParam());
    if (major >= 16) {
      frontLength = 15;
    } else if (major >= 15) {
      frontLength = 20;
    } else {
      frontLength = 19;
    }
    bool sizeErr = false;
    const size_t origTokenLengthBytes =
        writer_.getVarintSize(tokenLengths[j], sizeErr);
    CHECK(!sizeErr);
    auto front = writeBufs[j].split(frontLength);
    auto tokenLengthBuf = writeBufs[j].split(origTokenLengthBytes);
    auto tail = writeBufs[j].move();
    folly::io::Cursor cursor(frameHeader.get());

    auto frameType = parser_.decodeVarint(cursor);
    EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));

    len = frameLength(cursor, false);
    for (size_t i = 0; i < tokenLengths[j] - 1; ++i) {
      auto toParse = front->clone();
      auto shortTokenLengthBuf = encodeVarintBuf(i);
      size_t tokenLengthBytes = shortTokenLengthBuf->computeChainDataLength();
      toParse->appendToChain(std::move(shortTokenLengthBuf));
      toParse->appendToChain(tail->clone());
      cursor.reset(toParse.get());
      auto parseResult = parser_.parseSubscribeRequest(
          cursor, len - (origTokenLengthBytes - tokenLengthBytes));
      if (j == 0) {
        // clear token cache when registering
        parser_.setTokenCacheMaxSize(0);
        parser_.setTokenCacheMaxSize(100);
      }
      EXPECT_FALSE(parseResult.hasValue()) << "j=" << j << " i=" << i;
    }
    if (j == 1 || j == 2) { // register / delete mutate cache state
      auto toParse = front->clone();
      auto newLength = j == 1 ? tokenLengths[j] + 1 : 5;
      auto shortTokenLengthBuf = encodeVarintBuf(newLength);
      size_t tokenLengthBytes = shortTokenLengthBuf->computeChainDataLength();
      toParse->appendToChain(std::move(shortTokenLengthBuf));
      toParse->appendToChain(tail->clone());
      toParse->appendToChain(folly::IOBuf::copyBuffer("a"));
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
  subscribeUpdate.existingRequestID = RequestID(456);
  subscribeUpdate.start = AbsoluteLocation{10, 20};
  subscribeUpdate.endGroup = 30;
  subscribeUpdate.priority = 5;
  subscribeUpdate.forward = true;

  auto writeResult = writer_.writeSubscribeUpdate(writeBuf, subscribeUpdate);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(
      frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_UPDATE));

  auto parseResult = parser_.parseRequestUpdate(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->requestID.value, 123);
  if (getDraftMajorVersion(GetParam()) >= 18) {
    // Draft 18+: existingRequestID is implicit from the bidi request stream the
    // update travels on; it is not on the wire, so the parser leaves it
    // default-initialized (the codec substitutes the stream's id on ingress).
    EXPECT_EQ(parseResult->existingRequestID.value, 0);
  } else if (getDraftMajorVersion(GetParam()) >= 14) {
    // Versions 14-17: both requestID and existingRequestID are written/parsed.
    EXPECT_EQ(parseResult->existingRequestID.value, 456);
  } else {
    // Version < 14: only requestID is on wire, existingRequestID not set by
    // parser.
    EXPECT_EQ(parseResult->existingRequestID.value, 0);
  }

  EXPECT_EQ(parseResult->start->group, 10);
  EXPECT_EQ(parseResult->start->object, 20);
  EXPECT_EQ(parseResult->endGroup, 30);
  EXPECT_EQ(parseResult->priority, 5);
  EXPECT_TRUE(parseResult->forward.has_value());
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
  subscribeUpdate.existingRequestID = RequestID(456);
  subscribeUpdate.start = AbsoluteLocation{0, 0};
  subscribeUpdate.endGroup = 0; // Open-ended subscription
  subscribeUpdate.priority = kDefaultPriority;
  // forward field intentionally left unset (std::nullopt)

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
  auto parseResult = parser.parseRequestUpdate(cursor, frameLength);
  EXPECT_TRUE(parseResult.hasValue()) << "Failed to parse SUBSCRIBE_UPDATE";

  EXPECT_EQ(parseResult->requestID.value, 123);
  EXPECT_EQ(parseResult->existingRequestID.value, 456);
  EXPECT_EQ(parseResult->start->group, 0);
  EXPECT_EQ(parseResult->start->object, 0);
  EXPECT_EQ(parseResult->endGroup, 0);
  EXPECT_EQ(parseResult->priority, kDefaultPriority);
  // Verify forward field is NOT set (preserves existing state per draft 15+)
  EXPECT_FALSE(parseResult->forward.has_value());
}

// The TRACK_NAMESPACE_PREFIX parameter carries a Track Namespace tuple (§2.4.1)
// in its value; encode and decode must round-trip (draft 18+).
TEST(MoQFramerTest, TrackNamespacePrefixParamRoundTrip) {
  const TrackNamespace prefix(std::vector<std::string>{"foo", "bar"});
  auto param =
      MoQFrameWriter::encodeTrackNamespacePrefixParam(prefix, kVersionDraft18);
  EXPECT_EQ(
      param.key,
      folly::to_underlying(TrackRequestParamKey::TRACK_NAMESPACE_PREFIX));

  auto decoded = MoQFrameParser::parseTrackNamespacePrefixParam(
      param.asString, kVersionDraft18);
  ASSERT_TRUE(decoded.hasValue());
  EXPECT_EQ(*decoded, prefix);
}

// A value that is not a valid Track Namespace tuple must fail to decode (here
// the tuple claims 5 elements but carries no element data).
TEST(MoQFramerTest, TrackNamespacePrefixParamMalformed) {
  auto decoded = MoQFrameParser::parseTrackNamespacePrefixParam(
      std::string("\x05", 1), kVersionDraft18);
  EXPECT_TRUE(decoded.hasError());
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
      /*start*/ std::make_optional(startLoc),
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
      /*start*/ std::nullopt,
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

namespace {

// Encode a SubscribeRequest with AbsoluteRange and return the serialized bytes
// produced by writeSubscribeRequest at the given draft version.
std::unique_ptr<folly::IOBuf> writeAbsoluteRangeSubscribe(
    uint64_t version,
    AbsoluteLocation start,
    uint64_t endGroup) {
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  auto req = SubscribeRequest::make(
      FullTrackName{TrackNamespace({"ns"}), "track"},
      /*priority*/ kDefaultPriority,
      /*groupOrder*/ GroupOrder::Default,
      /*forward*/ true,
      /*locType*/ LocationType::AbsoluteRange,
      /*start*/ std::make_optional(start),
      /*endGroup*/ endGroup,
      /*params*/ {});
  CHECK(writer.writeSubscribeRequest(buf, req).hasValue());
  return buf.move();
}

} // namespace

// Draft-18 encodes EndGroup as a delta from StartLocation.group. Verify the
// wire encoding differs from earlier drafts by exactly that delta and that
// round-tripping recovers the absolute endGroup.
TEST(MoQFramerSubscriptionFilter, EndGroupDeltaV18) {
  constexpr AbsoluteLocation kStart{10, 20};
  constexpr uint64_t kEndGroup = 30;
  constexpr uint64_t kDelta = kEndGroup - kStart.group;

  auto v17Bytes =
      writeAbsoluteRangeSubscribe(kVersionDraft17, kStart, kEndGroup);
  auto v18Bytes =
      writeAbsoluteRangeSubscribe(kVersionDraft18, kStart, kEndGroup);

  // Both versions encode EndGroup with a 1-byte varint here (values < 64), so
  // the encoded frames must differ in length by zero but in the EndGroup byte
  // by exactly (kEndGroup - kDelta). Confirm by parsing the v18 frame with the
  // matching parser and seeing the absolute value emerge.
  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft18);
  MoQTokenCache tokenCache;
  parser.setTokenCache(&tokenCache);

  folly::io::Cursor cursor(v18Bytes.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  ASSERT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
  auto frameLen = cursor.readBE<uint16_t>();
  auto parseRes = parser.parseSubscribeRequest(cursor, frameLen);
  ASSERT_TRUE(parseRes.hasValue());
  EXPECT_EQ(parseRes->locType, LocationType::AbsoluteRange);
  ASSERT_TRUE(parseRes->start.has_value());
  EXPECT_EQ(parseRes->start->group, kStart.group);
  EXPECT_EQ(parseRes->start->object, kStart.object);
  EXPECT_EQ(parseRes->endGroup, kEndGroup);

  // The v17 and v18 frames must differ only by the EndGroup byte: v17 carries
  // the absolute value while v18 carries the delta.
  ASSERT_EQ(
      v17Bytes->computeChainDataLength(), v18Bytes->computeChainDataLength());
  std::string v17Str = v17Bytes->moveToFbString().toStdString();
  std::string v18Str = v18Bytes->moveToFbString().toStdString();
  size_t diffCount = 0;
  size_t diffIdx = 0;
  for (size_t i = 0; i < v17Str.size(); ++i) {
    if (v17Str[i] != v18Str[i]) {
      ++diffCount;
      diffIdx = i;
    }
  }
  ASSERT_EQ(diffCount, 1u);
  EXPECT_EQ(static_cast<uint8_t>(v17Str[diffIdx]), kEndGroup);
  EXPECT_EQ(static_cast<uint8_t>(v18Str[diffIdx]), kDelta);
}

// Regression: pre-v18 drafts must continue to treat EndGroup as absolute.
TEST(MoQFramerSubscriptionFilter, EndGroupAbsoluteV17Roundtrip) {
  constexpr AbsoluteLocation kStart{10, 20};
  constexpr uint64_t kEndGroup = 30;

  auto bytes = writeAbsoluteRangeSubscribe(kVersionDraft17, kStart, kEndGroup);

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft17);
  MoQTokenCache tokenCache;
  parser.setTokenCache(&tokenCache);

  folly::io::Cursor cursor(bytes.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  ASSERT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
  auto frameLen = cursor.readBE<uint16_t>();
  auto parseRes = parser.parseSubscribeRequest(cursor, frameLen);
  ASSERT_TRUE(parseRes.hasValue());
  EXPECT_EQ(parseRes->endGroup, kEndGroup);
}

// A v18 peer that sends an EndGroup delta whose absolute value would exceed
// kEightByteLimit (the MoQ varint range) must trigger a protocol violation,
// matching the spec text "Close session when delta encoding wraps".
TEST(MoQFramerSubscriptionFilter, EndGroupDeltaOverflowRejectedV18) {
  // Use the v17 writer (absolute encoding) to plant the wire endGroup = max
  // alongside start.group = 1, so the v18 parser will compute
  // 1 + kEightByteLimit and overflow.
  auto bytes = writeAbsoluteRangeSubscribe(
      kVersionDraft17, AbsoluteLocation{1, 0}, kEightByteLimit);

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft18);
  MoQTokenCache tokenCache;
  parser.setTokenCache(&tokenCache);

  folly::io::Cursor cursor(bytes.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  ASSERT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
  auto frameLen = cursor.readBE<uint16_t>();
  auto parseRes = parser.parseSubscribeRequest(cursor, frameLen);
  ASSERT_TRUE(parseRes.hasError());
  EXPECT_EQ(parseRes.error(), ErrorCode::PROTOCOL_VIOLATION);
}

INSTANTIATE_TEST_SUITE_P(
    MoQFramerTest,
    MoQFramerTest,
    ::testing::Values(
        kVersionDraft14,
        kVersionDraft15,
        kVersionDraft16,
        kVersionDraft18));

INSTANTIATE_TEST_SUITE_P(
    MoQFramerAuthTest,
    MoQFramerAuthTest,
    ::testing::Values(
        kVersionDraft14,
        kVersionDraft15,
        kVersionDraft16,
        kVersionDraft18));

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
  EXPECT_FALSE(legacyVersion.has_value());

  auto draft15Meta = getVersionFromAlpn("moqt-15-meta-01");
  ASSERT_TRUE(draft15Meta.has_value());
  EXPECT_EQ(*draft15Meta, 0xff00000f);

  auto draft15Meta02 = getVersionFromAlpn("moqt-15-meta-02");
  ASSERT_TRUE(draft15Meta02.has_value());
  EXPECT_EQ(*draft15Meta02, 0xff00000f);

  auto draft16Meta = getVersionFromAlpn("moqt-16-meta-00");
  ASSERT_TRUE(draft16Meta.has_value());
  EXPECT_EQ(*draft16Meta, 0xff000010);

  auto invalidAlpn1 = getVersionFromAlpn("h3");
  EXPECT_FALSE(invalidAlpn1.has_value());

  auto invalidAlpn2 = getVersionFromAlpn("moqt-");
  EXPECT_FALSE(invalidAlpn2.has_value());

  auto invalidAlpn3 = getVersionFromAlpn("moqt-abc");
  EXPECT_FALSE(invalidAlpn3.has_value());

  auto emptyAlpn = getVersionFromAlpn("");
  EXPECT_FALSE(emptyAlpn.has_value());
}

TEST(MoQFramerTestUtils, GetAlpnFromVersion) {
  auto alpnDraft14 = getAlpnFromVersion(kVersionDraft14);
  ASSERT_TRUE(alpnDraft14.has_value());
  EXPECT_EQ(*alpnDraft14, "moq-00");

  auto alpnDraft15 = getAlpnFromVersion(0xff00000f);
  ASSERT_TRUE(alpnDraft15.has_value());
  EXPECT_EQ(*alpnDraft15, kAlpnMoqtDraft15Latest);

  auto alpnDraft16 = getAlpnFromVersion(kVersionDraft16);
  ASSERT_TRUE(alpnDraft16.has_value());
  EXPECT_EQ(*alpnDraft16, kAlpnMoqtDraft16Latest);

  // Standard ALPNs (moqt-NN format)
  auto stdAlpnDraft14 = getAlpnFromVersion(kVersionDraft14, true);
  ASSERT_TRUE(stdAlpnDraft14.has_value());
  EXPECT_EQ(*stdAlpnDraft14, "moq-00"); // legacy always

  auto stdAlpnDraft15 = getAlpnFromVersion(kVersionDraft15, true);
  ASSERT_TRUE(stdAlpnDraft15.has_value());
  EXPECT_EQ(*stdAlpnDraft15, "moqt-15");

  auto stdAlpnDraft16 = getAlpnFromVersion(kVersionDraft16, true);
  ASSERT_TRUE(stdAlpnDraft16.has_value());
  EXPECT_EQ(*stdAlpnDraft16, "moqt-16");
}

TEST(MoQFramerTestUtils, GetMoqtProtocols) {
  // Empty = all supported versions
  auto all = getMoqtProtocols("", true);
  EXPECT_EQ(all.size(), 4);
  EXPECT_EQ(all[0], "moqt-18");
  EXPECT_EQ(all[1], "moqt-16");
  EXPECT_EQ(all[2], "moqt-15");
  EXPECT_EQ(all[3], "moq-00");

  // Single version
  auto just16 = getMoqtProtocols("16", true);
  EXPECT_EQ(just16.size(), 1);
  EXPECT_EQ(just16[0], "moqt-16");

  // Multiple versions
  auto v14and16 = getMoqtProtocols("14,16", true);
  EXPECT_EQ(v14and16.size(), 2);
  EXPECT_EQ(v14and16[0], "moq-00");
  EXPECT_EQ(v14and16[1], "moqt-16");

  // Meta-specific ALPNs
  auto meta16 = getMoqtProtocols("16");
  EXPECT_EQ(meta16.size(), 1);
  EXPECT_EQ(meta16[0], kAlpnMoqtDraft16Latest);
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

  bool deltaEncoding() const {
    return getDraftMajorVersion(GetParam()) >= 16;
  }

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
    writeVarintTo(immutableBuf, deltaEncoding() ? 21 - 20 : 21); // type
    writeVarintTo(immutableBuf, sizeof(testData));               // length
    immutableBuf.append(testData, sizeof(testData));             // data

    return immutableBuf.move();
  }

  // Creates a malformed immutable-extensions blob by taking the valid
  // immutable extensions produced above and appending another extension with
  // type = kImmutableExtensionType (odd) and an arbitrary byte value. This is
  // invalid when parsed as nested immutable extensions (parseImmutable=false).
  std::unique_ptr<folly::IOBuf> createImmutableExtensionsBufMalformed() {
    // Append an additional immutable-extensions container entry with arbitrary
    // 1-byte payload. This makes the nested immutable blob malformed for our
    // parser (immutable container found while already parsing immutable).
    folly::IOBufQueue q{folly::IOBufQueue::cacheChainLength()};

    if (deltaEncoding()) {
      // Different path to support delta encoding.
      writeVarintTo(q, 2);                           // type
      writeVarintTo(q, 1);                           // value
      writeVarintTo(q, kImmutableExtensionType - 2); // nested
      writeVarintTo(q, 1);                           // length
      static const uint8_t kArbitrary = 0xAA;
      q.append(&kArbitrary, 1);
    } else {
      auto base = createImmutableExtensionsBuf();
      if (base) {
        q.append(std::move(base));
      }

      // Write type = kImmutableExtensionType (odd)
      writeVarintTo(q, kImmutableExtensionType);

      // Write length = 1, then one arbitrary byte value 0xAA
      writeVarintTo(q, 1); // length
      static const uint8_t kArbitrary = 0xAA;
      q.append(&kArbitrary, 1);
    }

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
    writeVarintTo(
        buf,
        deltaEncoding() ? kImmutableExtensionType - 10
                        : kImmutableExtensionType);              // type
    writeVarintTo(buf, imm ? imm->computeChainDataLength() : 0); // length
    if (imm) {
      buf.append(std::move(imm)); // payload
    }

    // 3) type = 30 (even => integer value follows), value = 999
    writeVarintTo(
        buf, deltaEncoding() ? 30 - kImmutableExtensionType : 30); // type
    writeVarintTo(buf, 999);                                       // value

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
    writeVarintTo(
        buf,
        deltaEncoding() ? kImmutableExtensionType - 10
                        : kImmutableExtensionType);              // type
    writeVarintTo(buf, imm ? imm->computeChainDataLength() : 0); // length
    if (imm) {
      buf.append(std::move(imm)); // payload
    }

    // 3) type = 30 (even => integer value follows), value = 999
    writeVarintTo(
        buf, deltaEncoding() ? 30 - kImmutableExtensionType : 30); // type
    writeVarintTo(buf, 999);                                       // value

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
      Extension{2, 2},
      Extension{20, 100},
      Extension{21, folly::IOBuf::copyBuffer(kTestBinary, sizeof(kTestBinary))},
      Extension{32, 32}};

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
  EXPECT_EQ(obj.extensions.size(), 6)
      << "Should have 6 total extensions (2 mutable + 4 immutable)";
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
    ::testing::Values(kVersionDraft14, kVersionDraft15, kVersionDraft16));

TEST_P(MoQFramerTest, DatagramWithExtensionsAndNonNormalStatus) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  ObjectHeader obj(
      2, // group
      3, // subgroup
      4, // id
      5, // priority
      ObjectStatus::END_OF_GROUP,
      Extensions(test::getTestExtensions(), {}));

  auto writeResult =
      writer_.writeDatagramObject(writeBuf, TrackAlias(1), obj, nullptr);
  EXPECT_TRUE(writeResult.hasValue());

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto datagramType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(datagramType.has_value());

  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(
      cursor, DatagramType(datagramType->first), length);

  if (GetParam() >= kVersionDraft15) {
    EXPECT_TRUE(parseResult.hasError());
    EXPECT_EQ(parseResult.error(), ErrorCode::PROTOCOL_VIOLATION)
        << "Datagram with extensions and non-NORMAL status should return "
           "PROTOCOL_VIOLATION in v15+";
  } else {
    EXPECT_TRUE(parseResult.hasValue())
        << "Datagram with extensions and non-NORMAL status should succeed in "
           "v14";
  }
}

TEST_P(MoQFramerTest, SubgroupObjectWithExtensionsAndNonNormalStatus) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  ObjectHeader obj(
      2, // group
      3, // subgroup
      4, // id
      5, // priority
      ObjectStatus::END_OF_TRACK,
      Extensions(test::getTestExtensions(), {}));

  auto streamType =
      getSubgroupStreamType(GetParam(), SubgroupIDFormat::Present, true, false);
  auto headerResult = writer_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), obj, SubgroupIDFormat::Present, true);
  EXPECT_TRUE(headerResult.hasValue());

  auto objResult =
      writer_.writeStreamObject(writeBuf, streamType, obj, nullptr);
  EXPECT_TRUE(objResult.hasValue());

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto parsedStreamType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(parsedStreamType.has_value());

  auto sgOptions =
      getSubgroupOptions(GetParam(), StreamType(parsedStreamType->first));
  auto headerParseResult =
      parser_.parseSubgroupHeader(cursor, cursor.totalLength(), sgOptions);
  EXPECT_TRUE(headerParseResult.hasValue());

  auto objParseResult = parser_.parseSubgroupObjectHeader(
      cursor,
      cursor.totalLength(),
      headerParseResult->value.objectHeader,
      sgOptions);

  if (GetParam() >= kVersionDraft15) {
    EXPECT_TRUE(objParseResult.hasError());
    EXPECT_EQ(objParseResult.error(), ErrorCode::PROTOCOL_VIOLATION)
        << "Subgroup object with extensions and non-NORMAL status should "
           "return PROTOCOL_VIOLATION in v15+";
  } else {
    EXPECT_TRUE(objParseResult.hasValue())
        << "Subgroup object with extensions and non-NORMAL status should "
           "succeed in v14";
  }
}

TEST_P(MoQFramerTest, FetchObjectWithExtensionsAndNonNormalStatus) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  auto headerResult = writer_.writeFetchHeader(writeBuf, RequestID(1));
  EXPECT_TRUE(headerResult.hasValue());

  ObjectHeader obj(
      2, // group
      3, // subgroup
      4, // id
      5, // priority
      ObjectStatus::END_OF_GROUP,
      Extensions(test::getTestExtensions(), {}));

  auto objResult = writer_.writeStreamObject(
      writeBuf, StreamType::FETCH_HEADER, obj, nullptr);
  EXPECT_TRUE(objResult.hasValue());

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto parsedStreamType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(parsedStreamType.has_value());
  EXPECT_EQ(
      parsedStreamType->first, folly::to_underlying(StreamType::FETCH_HEADER));

  auto fetchHeaderResult =
      parser_.parseFetchHeader(cursor, cursor.totalLength());
  EXPECT_TRUE(fetchHeaderResult.hasValue());

  ObjectHeader headerTemplate;
  auto objParseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);

  if (GetParam() >= kVersionDraft15) {
    EXPECT_TRUE(objParseResult.hasError());
    EXPECT_EQ(objParseResult.error(), ErrorCode::PROTOCOL_VIOLATION)
        << "Fetch object with extensions and non-NORMAL status should return "
           "PROTOCOL_VIOLATION in v15+";
  } else {
    EXPECT_TRUE(objParseResult.hasValue())
        << "Fetch object with extensions and non-NORMAL status should succeed "
           "in v14";
  }
}

TEST_P(MoQFramerTest, DatagramWithExtensionsAndNormalStatusSucceeds) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  ObjectHeader obj(
      2, // group
      3, // subgroup
      4, // id
      5, // priority
      ObjectStatus::NORMAL,
      Extensions(test::getTestExtensions(), {}),
      7); // length

  // Write the datagram with payload
  auto writeResult = writer_.writeDatagramObject(
      writeBuf, TrackAlias(1), obj, folly::IOBuf::copyBuffer("payload"));
  EXPECT_TRUE(writeResult.hasValue());

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto datagramType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(datagramType.has_value());

  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(
      cursor, DatagramType(datagramType->first), length);

  EXPECT_TRUE(parseResult.hasValue())
      << "Datagram with extensions and NORMAL status should parse successfully";
  EXPECT_EQ(parseResult->objectHeader.status, ObjectStatus::NORMAL);
  EXPECT_FALSE(parseResult->objectHeader.extensions.empty());
}

TEST_P(MoQFramerTest, DatagramWithNonNormalStatusNoExtensionsSucceeds) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  ObjectHeader obj(
      2, // group
      3, // subgroup
      4, // id
      5, // priority
      ObjectStatus::END_OF_GROUP,
      Extensions({}, {}));

  auto writeResult =
      writer_.writeDatagramObject(writeBuf, TrackAlias(1), obj, nullptr);
  EXPECT_TRUE(writeResult.hasValue());

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto datagramType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(datagramType.has_value());

  auto length = cursor.totalLength();
  auto parseResult = parser_.parseDatagramObjectHeader(
      cursor, DatagramType(datagramType->first), length);

  EXPECT_TRUE(parseResult.hasValue())
      << "Datagram with non-NORMAL status but no extensions should parse "
         "successfully";
  EXPECT_EQ(parseResult->objectHeader.status, ObjectStatus::END_OF_GROUP);
  EXPECT_TRUE(parseResult->objectHeader.extensions.empty());
}

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
}

TEST(MoQFramerTest, WriteClientSetupWithAlpnVersion15NoVersionArray) {
  // When version >= 15, CLIENT_SETUP should not write version array

  auto clientSetup = moxygen::Setup{};

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

  auto serverSetup = moxygen::Setup{};

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

TEST(MoQFramerTest, WriteClientSetupUsesSetupFrameTypeForDraft18) {
  auto clientSetup = moxygen::Setup{};

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeClientSetup(writeBuf, clientSetup, kVersionDraft18);
  EXPECT_TRUE(result.hasValue()) << "Failed to write CLIENT_SETUP";

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto frameType = decodeMoQVarint(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SETUP));
}

TEST(MoQFramerTest, WriteServerSetupUsesSetupFrameTypeForDraft18) {
  auto serverSetup = moxygen::Setup{};

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeServerSetup(writeBuf, serverSetup, kVersionDraft18);
  EXPECT_TRUE(result.hasValue()) << "Failed to write SERVER_SETUP";

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto frameType = decodeMoQVarint(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SETUP));
}

TEST(MoQFramerTest, WriteClientSetupUsesClientSetupFrameTypeForDraft16) {
  auto clientSetup = moxygen::Setup{};

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writeClientSetup(writeBuf, clientSetup, kVersionDraft16);
  EXPECT_TRUE(result.hasValue()) << "Failed to write CLIENT_SETUP";

  auto buffer = writeBuf.move();
  folly::io::Cursor cursor(buffer.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::CLIENT_SETUP));
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
  MoQTokenCache tokenCache;
  parser.setTokenCache(&tokenCache);
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
      tokenBuf,
      folly::to_underlying(AliasType::DELETE_ALIAS),
      tokenSize,
      error);
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
  MoQTokenCache tokenCache;
  parser.setTokenCache(&tokenCache);
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
    std::optional<uint8_t> expectedPriority,
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
      std::nullopt,
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
      ObjectStatus::END_OF_GROUP);
  auto serialized = writeBuf.move();
  parseAndCheckDatagram(
      parser,
      serialized.get(),
      dgType,
      22,
      33,
      0,
      100,
      ObjectStatus::END_OF_GROUP,
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
      std::nullopt,
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
  static const std::set<uint64_t> validV18 = {
      0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
      0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x38, 0x39, 0x3A, 0x3B, 0x3C, 0x3D,
      0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D,
      0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x78, 0x79, 0x7A, 0x7B, 0x7C, 0x7D};

  uint64_t version14 = kVersionDraft14;
  uint64_t version15 = kVersionDraft15;
  uint64_t version18 = kVersionDraft18;

  for (uint64_t t = 0; t <= 255; ++t) {
    bool shouldBeValidV14 = validV14.count(t) > 0;
    EXPECT_EQ(isValidSubgroupType(version14, t), shouldBeValidV14)
        << "v14: 0x" << std::hex << t;
    bool shouldBeValidV15 = validV15.count(t) > 0;
    EXPECT_EQ(isValidSubgroupType(version15, t), shouldBeValidV15)
        << "v15: 0x" << std::hex << t;
    bool shouldBeValidV18 = validV18.count(t) > 0;
    EXPECT_EQ(isValidSubgroupType(version18, t), shouldBeValidV18)
        << "v18: 0x" << std::hex << t;
  }
}

// Helper for round-trip datagram test
void testDatagramPriorityRoundTrip(
    uint64_t version,
    std::optional<uint8_t> priority,
    DatagramType expectedType,
    std::optional<uint8_t> expectedPriority) {
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
      std::nullopt,
      DatagramType::OBJECT_DATAGRAM_NO_EXT_NO_PRI,
      std::nullopt);
}
TEST(MoQFramerTest, OptionalPriorityDatagramRoundTripValue) {
  testDatagramPriorityRoundTrip(
      kVersionDraft15, 64, DatagramType::OBJECT_DATAGRAM_NO_EXT, 64);
}

// Helper for round-trip subgroup header test
void testSubgroupPriorityRoundTrip(
    uint64_t version,
    std::optional<uint8_t> priority,
    StreamType expectedType,
    std::optional<uint8_t> expectedPriority) {
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

// Test round-trip write/read with std::nullopt priority in subgroup (v15)
TEST(MoQFramerTest, OptionalPrioritySubgroupRoundTripNone) {
  testSubgroupPriorityRoundTrip(
      kVersionDraft15,
      std::nullopt,
      StreamType::SUBGROUP_HEADER_SG_NO_PRI,
      std::nullopt);
}

// Test round-trip write/read with explicit priority in subgroup (v15)
TEST(MoQFramerTest, OptionalPrioritySubgroupRoundTripValue) {
  testSubgroupPriorityRoundTrip(
      kVersionDraft15, 80, StreamType::SUBGROUP_HEADER_SG, 80);
}

TEST(MoQFramerTest, FirstObjectSubgroupHeaderRoundTripDraft18) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft18);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader objHeader = {
      100, 50, 200, 64, ObjectStatus::NORMAL, noExtensions(), 0};
  auto result = writer.writeSubgroupHeader(
      writeBuf,
      TrackAlias(25),
      objHeader,
      SubgroupIDFormat::Present,
      /*includeExtensions=*/false,
      /*beginsWithFirstObject=*/true);
  ASSERT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto parsedStreamType = decodeMoQVarint(cursor);
  ASSERT_TRUE(parsedStreamType.has_value());
  EXPECT_EQ(parsedStreamType->first, 0x54);

  auto sgOptions =
      getSubgroupOptions(kVersionDraft18, StreamType(parsedStreamType->first));
  EXPECT_TRUE(sgOptions.beginsWithFirstObject);
  auto parseResult =
      parser.parseSubgroupHeader(cursor, cursor.totalLength(), sgOptions);
  ASSERT_TRUE(parseResult.hasValue());
  EXPECT_EQ(parseResult->value.trackAlias, TrackAlias(25));
  EXPECT_EQ(parseResult->value.objectHeader.group, 100);
  EXPECT_EQ(parseResult->value.objectHeader.subgroup, 50);
  EXPECT_EQ(parseResult->value.objectHeader.priority, 64);
}

TEST(MoQFramerTest, FirstObjectSubgroupHeaderIgnoredBeforeDraft18) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft17);

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader objHeader = {
      100, 50, 200, 64, ObjectStatus::NORMAL, noExtensions(), 0};
  auto result = writer.writeSubgroupHeader(
      writeBuf,
      TrackAlias(25),
      objHeader,
      SubgroupIDFormat::Present,
      /*includeExtensions=*/false,
      /*beginsWithFirstObject=*/true);
  ASSERT_TRUE(result.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto parsedStreamType = decodeMoQVarint(cursor);
  ASSERT_TRUE(parsedStreamType.has_value());
  EXPECT_EQ(parsedStreamType->first, 0x14);
  EXPECT_FALSE(isValidSubgroupType(kVersionDraft17, 0x54));
  EXPECT_FALSE(getSubgroupOptions(kVersionDraft17, StreamType(0x54))
                   .beginsWithFirstObject);
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

// Drafts 15-17: legacy control-stream framing that draft 18 moved off the
// control stream or removed from the wire.
class MoQFramerV15_17Test : public MoQFramerV15PlusTest {};

// Drafts 16-17: legacy messages (e.g. SUBSCRIBE_NAMESPACE options) that only
// exist in this window.
class MoQFramerV16_17Test : public MoQFramerV15PlusTest {};

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
      /*start*/ std::nullopt,
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
      /*start*/ std::nullopt,
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
  setPublisherGroupOrder(subscribeOk, GroupOrder::NewestFirst);
  subscribeOk.largest = AbsoluteLocation{10, 20};

  auto writeResult = writer_.writeSubscribeOk(writeBuf, subscribeOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_OK));

  auto parseResult = parser_.parseSubscribeOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

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
  // Draft 18+: requestID is implicit from the bidi stream context and not on
  // the wire; the parser leaves it default-initialized.
  if (getDraftMajorVersion(GetParam()) < 18) {
    EXPECT_EQ(parseResult->requestID, RequestID(42));
  }
  EXPECT_EQ(parseResult->trackAlias, TrackAlias(1));
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::OldestFirst);
}

TEST_P(MoQFramerV15PlusTest, SubscribeOkExpiresZeroNotWritten) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeOk subscribeOk;
  subscribeOk.requestID = RequestID(42);
  subscribeOk.trackAlias = TrackAlias(1);
  subscribeOk.expires = std::chrono::milliseconds(0); // Zero expires
  setPublisherGroupOrder(subscribeOk, GroupOrder::NewestFirst);
  subscribeOk.largest = AbsoluteLocation{10, 20};

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
      GroupOrder::Default; // Will be overridden by parser default

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
  setPublisherGroupOrder(publishRequest, GroupOrder::NewestFirst);

  auto writeResult = writer_.writePublish(writeBuf, publishRequest);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH));

  auto parseResult = parser_.parsePublish(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->groupOrder, GroupOrder::NewestFirst);
}

// Test default GroupOrder for PublishOk when param not present
// At draft 18+, PUBLISH_OK is sent as REQUEST_OK on a per-request bidi stream;
// parse via parseRequestOk + toPublishOk in that case.
PublishOk roundTripPublishOk(
    MoQFrameWriter& writer,
    MoQFrameParser& parser,
    const PublishOk& publishOk,
    uint64_t majorVersion,
    std::function<size_t(folly::io::Cursor&)> frameLength) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  EXPECT_TRUE(writer.writePublishOk(writeBuf, publishOk).hasValue());
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  if (majorVersion >= 18) {
    EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::REQUEST_OK));
    auto requestOk = parser.parseRequestOk(
        cursor, frameLength(cursor), FrameType::PUBLISH_OK);
    EXPECT_TRUE(requestOk.hasValue());
    auto parsed = requestOk->toPublishOk(majorVersion);
    EXPECT_TRUE(parsed.hasValue());
    return parsed.value();
  }
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH_OK));
  auto parseResult = parser.parsePublishOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());
  return parseResult.value();
}

TEST_P(MoQFramerV15PlusTest, PublishOkDefaultGroupOrder) {
  PublishOk publishOk;
  publishOk.requestID = RequestID(200);
  publishOk.forward = true;
  publishOk.subscriberPriority = 128;
  publishOk.groupOrder =
      GroupOrder::Default; // Writer won't write GROUP_ORDER param
  publishOk.locType = LocationType::LargestObject;

  auto parsed = roundTripPublishOk(
      writer_,
      parser_,
      publishOk,
      getDraftMajorVersion(GetParam()),
      [this](folly::io::Cursor& c) { return frameLength(c); });
  // When GROUP_ORDER param is not written, parser should set to Default
  EXPECT_EQ(parsed.groupOrder, GroupOrder::Default);
}

// Test explicit GroupOrder param overrides default for PublishOk
TEST_P(MoQFramerV15PlusTest, PublishOkExplicitGroupOrder) {
  PublishOk publishOk;
  publishOk.requestID = RequestID(200);
  publishOk.forward = true;
  publishOk.subscriberPriority = 128;
  publishOk.groupOrder =
      GroupOrder::OldestFirst; // Non-default, will be written
  publishOk.locType = LocationType::LargestObject;

  auto parsed = roundTripPublishOk(
      writer_,
      parser_,
      publishOk,
      getDraftMajorVersion(GetParam()),
      [this](folly::io::Cursor& c) { return frameLength(c); });
  EXPECT_EQ(parsed.groupOrder, GroupOrder::OldestFirst);
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

TEST_P(MoQFramerV15PlusTest, ParseFetchObjectHeaderCursorUnderflow) {
  auto emptyBuf = folly::IOBuf::create(0);
  folly::io::Cursor cursor(emptyBuf.get());

  ObjectHeader headerTemplate;
  // Pass length = 1 so remainingLength >= 1 check passes,
  // but cursor has no data to actually read
  size_t length = 1;
  auto parseResult =
      parser_.parseFetchObjectHeader(cursor, length, headerTemplate);

  // Should return PARSE_UNDERFLOW, not crash
  EXPECT_TRUE(parseResult.hasError());
  EXPECT_EQ(parseResult.error(), ErrorCode::PARSE_UNDERFLOW);
}

// Test SubscribeNamespace with forward = false (draft 18+ moves
// SUBSCRIBE_NAMESPACE off the control stream).
TEST_P(MoQFramerV15_17Test, SubscribeNamespaceForwardFalse) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeNamespace subscribeNamespace;
  subscribeNamespace.requestID = RequestID(42);
  subscribeNamespace.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"ns", "prefix"});
  subscribeNamespace.forward = false;

  auto writeResult =
      writer_.writeSubscribeNamespace(writeBuf, subscribeNamespace);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(
      frameType->first,
      folly::to_underlying(FrameType::LEGACY_SUBSCRIBE_NAMESPACE));

  auto parseResult =
      parser_.parseSubscribeNamespace(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  // Verify all fields match
  EXPECT_EQ(parseResult->requestID, subscribeNamespace.requestID);
  EXPECT_EQ(
      parseResult->trackNamespacePrefix,
      subscribeNamespace.trackNamespacePrefix);
  EXPECT_EQ(parseResult->forward, subscribeNamespace.forward);
  EXPECT_EQ(parseResult->forward, false);
}

// Test PUBLISH with largest location roundtrips correctly for v15+
// (largest is sent as LARGEST_OBJECT parameter, not fixed fields)
TEST_P(MoQFramerV15PlusTest, PublishWithLargestLocation) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  PublishRequest publishRequest;
  publishRequest.requestID = RequestID(100);
  publishRequest.fullTrackName =
      FullTrackName({TrackNamespace({"test"}), "pub"});
  publishRequest.trackAlias = TrackAlias(42);
  setPublisherGroupOrder(publishRequest, GroupOrder::NewestFirst);
  publishRequest.largest = AbsoluteLocation{5, 10};
  publishRequest.forward = true;

  auto writeResult = writer_.writePublish(writeBuf, publishRequest);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH));

  auto parseResult = parser_.parsePublish(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->requestID, publishRequest.requestID);
  EXPECT_EQ(parseResult->trackAlias, publishRequest.trackAlias);
  EXPECT_EQ(parseResult->groupOrder, GroupOrder::NewestFirst);
  EXPECT_TRUE(parseResult->largest.has_value());
  EXPECT_EQ(parseResult->largest->group, 5);
  EXPECT_EQ(parseResult->largest->object, 10);
  EXPECT_TRUE(parseResult->forward);
}

// Test PUBLISH without largest location roundtrips correctly for v15+
TEST_P(MoQFramerV15PlusTest, PublishWithoutLargestLocation) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  PublishRequest publishRequest;
  publishRequest.requestID = RequestID(101);
  publishRequest.fullTrackName =
      FullTrackName({TrackNamespace({"test"}), "pub"});
  publishRequest.trackAlias = TrackAlias(43);
  publishRequest.largest = std::nullopt;

  auto writeResult = writer_.writePublish(writeBuf, publishRequest);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH));

  auto parseResult = parser_.parsePublish(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->requestID, publishRequest.requestID);
  EXPECT_FALSE(parseResult->largest.has_value());
}

// Test SubscribeOk with largest location roundtrips correctly for v15+
TEST_P(MoQFramerV15PlusTest, SubscribeOkWithLargestLocation) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeOk subscribeOk;
  subscribeOk.requestID = RequestID(42);
  subscribeOk.trackAlias = TrackAlias(1);
  subscribeOk.expires = std::chrono::milliseconds(0);
  subscribeOk.groupOrder = GroupOrder::OldestFirst;
  subscribeOk.largest = AbsoluteLocation{5, 10};

  auto writeResult = writer_.writeSubscribeOk(writeBuf, subscribeOk);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE_OK));

  auto parseResult = parser_.parseSubscribeOk(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_TRUE(parseResult->largest.has_value());
  EXPECT_EQ(parseResult->largest->group, 5);
  EXPECT_EQ(parseResult->largest->object, 10);
}

INSTANTIATE_TEST_SUITE_P(
    MoQFramerV15PlusTest,
    MoQFramerV15PlusTest,
    ::testing::Values(kVersionDraft15, kVersionDraft16, kVersionDraft18));

INSTANTIATE_TEST_SUITE_P(
    MoQFramerV15_17Test,
    MoQFramerV15_17Test,
    ::testing::Values(kVersionDraft15, kVersionDraft16, kVersionDraft17));

INSTANTIATE_TEST_SUITE_P(
    MoQFramerV16_17Test,
    MoQFramerV16_17Test,
    ::testing::Values(kVersionDraft16, kVersionDraft17));

// Test class for v16+ specific features
class MoQFramerV16PlusTest : public ::testing::TestWithParam<uint64_t> {
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

// Test Namespace message roundtrip
TEST_P(MoQFramerV16PlusTest, NamespaceRoundtrip) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  Namespace ns;
  ns.trackNamespaceSuffix =
      TrackNamespace(std::vector<std::string>{"suffix", "part"});

  auto writeResult = writer_.writeNamespace(writeBuf, ns);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::NAMESPACE));

  auto parseResult = parser_.parseNamespace(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->trackNamespaceSuffix, ns.trackNamespaceSuffix);
}

// Test Namespace message with empty suffix
TEST_P(MoQFramerV16PlusTest, NamespaceEmptySuffix) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  Namespace ns;
  ns.trackNamespaceSuffix = TrackNamespace(std::vector<std::string>{});

  auto writeResult = writer_.writeNamespace(writeBuf, ns);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::NAMESPACE));

  auto parseResult = parser_.parseNamespace(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->trackNamespaceSuffix, ns.trackNamespaceSuffix);
}

// Test that a namespace field with length 0 is rejected with PROTOCOL_VIOLATION
// in draft >= 16
TEST_P(MoQFramerV16PlusTest, NamespaceEmptyFieldValueProtocolViolation) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Manually construct a NAMESPACE frame with one empty field value:
  //   Frame type: NAMESPACE
  //   Frame length: 2 bytes
  //   Tuple item count: 1
  //   Field 1 length: 0 (empty - violates draft 16 requirement)
  bool error = false;
  size_t size = 0;
  writeVarint(
      writeBuf, folly::to_underlying(FrameType::NAMESPACE), size, error);
  // Frame length (2 bytes): tuple count (1 byte) + field length 0 (1 byte) = 2
  writeBuf.append("\x00\x02", 2);
  // Tuple with 1 item, where the item has length 0
  writeBuf.append("\x01\x00", 2);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Skip frame type
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::NAMESPACE));

  auto parseResult = parser_.parseNamespace(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasError());
  EXPECT_EQ(parseResult.error(), ErrorCode::PROTOCOL_VIOLATION);
}

// Test NamespaceDone message roundtrip
TEST_P(MoQFramerV16PlusTest, NamespaceDoneRoundtrip) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  NamespaceDone namespaceDone;
  namespaceDone.trackNamespaceSuffix =
      TrackNamespace(std::vector<std::string>{"done", "suffix"});

  auto writeResult = writer_.writeNamespaceDone(writeBuf, namespaceDone);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::NAMESPACE_DONE));

  auto parseResult = parser_.parseNamespaceDone(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(
      parseResult->trackNamespaceSuffix, namespaceDone.trackNamespaceSuffix);
}

// Test SubscribeNamespace with Subscribe Options (v16+)
TEST_P(MoQFramerV16_17Test, SubscribeNamespaceWithOptions) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeNamespace subscribeNamespace;
  subscribeNamespace.requestID = RequestID(100);
  subscribeNamespace.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"test", "namespace"});
  subscribeNamespace.options = SubscribeNamespaceOptions::NAMESPACE;
  subscribeNamespace.forward = true;

  auto writeResult =
      writer_.writeSubscribeNamespace(writeBuf, subscribeNamespace);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(
      frameType->first,
      folly::to_underlying(FrameType::LEGACY_SUBSCRIBE_NAMESPACE));

  auto parseResult =
      parser_.parseSubscribeNamespace(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->requestID, subscribeNamespace.requestID);
  EXPECT_EQ(
      parseResult->trackNamespacePrefix,
      subscribeNamespace.trackNamespacePrefix);
  EXPECT_EQ(parseResult->options, SubscribeNamespaceOptions::NAMESPACE);
  EXPECT_EQ(parseResult->forward, subscribeNamespace.forward);
}

// Test SubscribeNamespace with BOTH option (v16+)
TEST_P(MoQFramerV16_17Test, SubscribeNamespaceWithBothOption) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeNamespace subscribeNamespace;
  subscribeNamespace.requestID = RequestID(200);
  subscribeNamespace.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"both", "test"});
  subscribeNamespace.options = SubscribeNamespaceOptions::BOTH;
  subscribeNamespace.forward = false;

  auto writeResult =
      writer_.writeSubscribeNamespace(writeBuf, subscribeNamespace);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(
      frameType->first,
      folly::to_underlying(FrameType::LEGACY_SUBSCRIBE_NAMESPACE));

  auto parseResult =
      parser_.parseSubscribeNamespace(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->requestID, subscribeNamespace.requestID);
  EXPECT_EQ(
      parseResult->trackNamespacePrefix,
      subscribeNamespace.trackNamespacePrefix);
  EXPECT_EQ(parseResult->options, SubscribeNamespaceOptions::BOTH);
  EXPECT_EQ(parseResult->forward, false);
}

// Test SubscribeNamespace with PUBLISH option (v16+)
TEST_P(MoQFramerV16_17Test, SubscribeNamespaceWithPublishOption) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeNamespace subscribeNamespace;
  subscribeNamespace.requestID = RequestID(300);
  subscribeNamespace.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"publish", "only"});
  subscribeNamespace.options = SubscribeNamespaceOptions::PUBLISH;
  subscribeNamespace.forward = true;

  auto writeResult =
      writer_.writeSubscribeNamespace(writeBuf, subscribeNamespace);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(
      frameType->first,
      folly::to_underlying(FrameType::LEGACY_SUBSCRIBE_NAMESPACE));

  auto parseResult =
      parser_.parseSubscribeNamespace(cursor, frameLength(cursor));
  EXPECT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->requestID, subscribeNamespace.requestID);
  EXPECT_EQ(
      parseResult->trackNamespacePrefix,
      subscribeNamespace.trackNamespacePrefix);
  EXPECT_EQ(parseResult->options, SubscribeNamespaceOptions::PUBLISH);
  EXPECT_EQ(parseResult->forward, true);
}

// Test RequestError with retryInterval (v16+ only)
TEST_P(MoQFramerV16PlusTest, RequestErrorWithRetryInterval) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  RequestError requestError;
  requestError.requestID = RequestID(42);
  requestError.errorCode = RequestErrorCode::INTERNAL_ERROR;
  requestError.reasonPhrase = "temporary failure";
  requestError.retryInterval =
      std::chrono::milliseconds{5000}; // 5 seconds (minus one per spec)

  auto writeResult = writer_.writeRequestError(
      writeBuf, requestError, FrameType::REQUEST_ERROR);
  EXPECT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::REQUEST_ERROR));

  auto parseResult = parser_.parseRequestError(
      cursor, frameLength(cursor), FrameType::REQUEST_ERROR);
  EXPECT_TRUE(parseResult.hasValue());

  // Draft 18+: requestID is implicit from the bidi stream context and not on
  // the wire; the parser leaves it default-initialized.
  if (getDraftMajorVersion(GetParam()) < 18) {
    EXPECT_EQ(parseResult->requestID, requestError.requestID);
  }
  EXPECT_EQ(parseResult->errorCode, requestError.errorCode);
  EXPECT_EQ(parseResult->retryInterval, requestError.retryInterval);
  EXPECT_EQ(parseResult->reasonPhrase, requestError.reasonPhrase);
}

// Test parsing End of Unknown Range marker (0x10C) - v16+ only
// End of Range markers require varint encoding which is only supported in v16+
TEST_P(MoQFramerV16PlusTest, ParseEndOfUnknownRange) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Write FETCH header
  writer_.writeFetchHeader(writeBuf, RequestID(1));

  // Write End of Unknown Range marker (0x10C) + Group ID + Object ID via the
  // version-aware writeVarint (QUIC varint at <17, MoQ varint at >=17).
  size_t size = 0;
  bool error = false;
  writer_.writeVarint(
      writeBuf, kSerializationFlagEndOfUnknownRange, size, error);
  writer_.writeVarint(writeBuf, 5, size, error);  // Group ID
  writer_.writeVarint(writeBuf, 10, size, error); // Object ID
  ASSERT_FALSE(error);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Skip FETCH stream header
  auto streamType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(streamType->first, folly::to_underlying(StreamType::FETCH_HEADER));

  auto fetchHeaderResult =
      parser_.parseFetchHeader(cursor, cursor.totalLength());
  EXPECT_TRUE(fetchHeaderResult.hasValue());

  ObjectHeader headerTemplate;
  auto parseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);

  EXPECT_TRUE(parseResult.hasValue());
  ASSERT_TRUE(
      std::holds_alternative<MoQFrameParser::EndOfRangeMarker>(
          parseResult->value));
  auto& marker = std::get<MoQFrameParser::EndOfRangeMarker>(parseResult->value);
  EXPECT_EQ(marker.groupId, 5);
  EXPECT_EQ(marker.objectId, 10);
  EXPECT_TRUE(marker.isUnknownOrNonexistent);
}

// Test parsing End of Non-Existent Range marker (0x8C) - v16+ only
// End of Range markers require varint encoding which is only supported in v16+
TEST_P(MoQFramerV16PlusTest, ParseEndOfNonExistentRange) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Write FETCH header
  writer_.writeFetchHeader(writeBuf, RequestID(1));

  // Write End of Non-Existent Range marker (0x8C) + Group ID + Object ID via
  // the version-aware writeVarint (QUIC varint at <17, MoQ varint at >=17).
  size_t size = 0;
  bool error = false;
  writer_.writeVarint(
      writeBuf, kSerializationFlagEndOfNonExistentRange, size, error);
  writer_.writeVarint(writeBuf, 3, size, error); // Group ID
  writer_.writeVarint(writeBuf, 7, size, error); // Object ID
  ASSERT_FALSE(error);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Skip FETCH stream header
  auto streamType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(streamType->first, folly::to_underlying(StreamType::FETCH_HEADER));

  auto fetchHeaderResult =
      parser_.parseFetchHeader(cursor, cursor.totalLength());
  EXPECT_TRUE(fetchHeaderResult.hasValue());

  ObjectHeader headerTemplate;
  auto parseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);

  EXPECT_TRUE(parseResult.hasValue());
  ASSERT_TRUE(
      std::holds_alternative<MoQFrameParser::EndOfRangeMarker>(
          parseResult->value));
  auto& marker = std::get<MoQFrameParser::EndOfRangeMarker>(parseResult->value);
  EXPECT_EQ(marker.groupId, 3);
  EXPECT_EQ(marker.objectId, 7);
  EXPECT_FALSE(marker.isUnknownOrNonexistent);
}

// Test that invalid serialization flags >= 128 (not 0x8C or 0x10C) cause error
// v16+ only since values >= 128 require varint encoding
TEST_P(MoQFramerV16PlusTest, ParseInvalidSerializationFlags) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Write FETCH header
  writer_.writeFetchHeader(writeBuf, RequestID(1));

  // Write invalid serialization flags (0x80 = 128, which is >= 128 but not
  // 0x8C or 0x10C)
  // 128 as varint: 128 >= 64, so 2-byte encoding: 0x40 | (128 >> 8), 128 & 0xFF
  // = 0x40 | 0, 0x80
  writeBuf.append(folly::IOBuf::copyBuffer("\x40\x80"));

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Skip FETCH stream header
  auto streamType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(streamType->first, folly::to_underlying(StreamType::FETCH_HEADER));

  auto fetchHeaderResult =
      parser_.parseFetchHeader(cursor, cursor.totalLength());
  EXPECT_TRUE(fetchHeaderResult.hasValue());

  ObjectHeader headerTemplate;
  auto parseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);

  EXPECT_TRUE(parseResult.hasError());
  EXPECT_EQ(parseResult.error(), ErrorCode::PROTOCOL_VIOLATION);
}

// Test that parsing FETCH object with datagram forwarding preference (bit 0x40)
// succeeds in draft-16+
TEST_P(MoQFramerV16PlusTest, FetchObjectWithDatagramForwardingPreference) {
  // Use the public writeStreamObject API with
  // forwardingPreferenceIsDatagram=true
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Write FETCH header using the public API
  auto headerResult = writer_.writeFetchHeader(writeBuf, RequestID(1));
  EXPECT_TRUE(headerResult.hasValue());

  // Create an object header
  ObjectHeader obj(
      1, // group
      2, // subgroup
      3, // id
      5, // priority
      ObjectStatus::NORMAL,
      Extensions({}, {}),
      4 /* length */);

  // Write a FETCH object with forwardingPreferenceIsDatagram = true
  auto objResult = writer_.writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      obj,
      folly::IOBuf::copyBuffer("test"),
      true /* forwardingPreferenceIsDatagram */);
  EXPECT_TRUE(objResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Skip stream type
  auto streamType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_TRUE(streamType.has_value());
  EXPECT_EQ(streamType->first, folly::to_underlying(StreamType::FETCH_HEADER));

  // Parse FETCH header
  auto fetchHeaderResult =
      parser_.parseFetchHeader(cursor, cursor.totalLength());
  EXPECT_TRUE(fetchHeaderResult.hasValue());

  // Parse the FETCH object - should succeed with bit 0x40 set in draft-16+
  ObjectHeader headerTemplate;
  auto objParseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  EXPECT_TRUE(objParseResult.hasValue())
      << "FETCH object with datagram forwarding preference (bit 0x40) should "
         "parse successfully in draft-16+";
  auto& objHeader = std::get<ObjectHeader>(objParseResult->value);
  EXPECT_EQ(objHeader.group, 1);
  EXPECT_EQ(objHeader.id, 3);
  EXPECT_EQ(objHeader.status, ObjectStatus::NORMAL);
}

// Test that parsing FETCH object with reserved bit 0x80 set returns error
TEST_P(MoQFramerV16PlusTest, FetchObjectReservedBit0x80ReturnsError) {
  // Manually construct a FETCH object with bit 0x80 set
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Write FETCH header
  writer_.writeFetchHeader(writeBuf, RequestID(1));

  // Now write a malformed FETCH object with reserved bit 0x80 set
  // In draft-16+, flags are varint-encoded. 0x80 (128) as QUIC varint:
  // 128 >= 64, so 2-byte encoding: 0x40 | (128 >> 8), 128 & 0xFF = 0x40, 0x80
  writeBuf.append(folly::IOBuf::copyBuffer("\x40\x80"));

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  // Skip stream type
  auto parsedStreamType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_TRUE(parsedStreamType.has_value());

  auto fetchHeaderResult =
      parser_.parseFetchHeader(cursor, cursor.totalLength());
  EXPECT_TRUE(fetchHeaderResult.hasValue());

  ObjectHeader headerTemplate;
  auto objParseResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  EXPECT_TRUE(objParseResult.hasError())
      << "Should return error when reserved bit 0x80 is set";
  EXPECT_EQ(objParseResult.error(), ErrorCode::PROTOCOL_VIOLATION);
}

// Regression test: calculateExtensionVectorSize must account for delta-encoded
// extension types in draft >= 16.
TEST_P(MoQFramerV16PlusTest, ExtensionBlockLengthWithDeltaEncoding) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Use large extension types with small deltas between them.
  std::vector<Extension> exts = {
      Extension{765966, 1}, // 4-byte varint absolute
      Extension{765968, 2}, // delta=2, 1-byte varint
      Extension{765970, 3}, // delta=2, 1-byte varint
  };

  ObjectHeader obj(
      2, // group
      3, // subgroup
      4, // id
      5, // priority
      ObjectStatus::NORMAL,
      Extensions(exts, {}),
      4); // length

  auto streamType =
      getSubgroupStreamType(GetParam(), SubgroupIDFormat::Present, true, false);
  auto res = writer_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), obj, SubgroupIDFormat::Present, true);
  ASSERT_TRUE(res.hasValue());
  res = writer_.writeStreamObject(
      writeBuf, streamType, obj, folly::IOBuf::copyBuffer("AAAA"));
  ASSERT_TRUE(res.hasValue());

  // Parse back
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto parsedStreamType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(parsedStreamType.has_value());
  EXPECT_EQ(StreamType(parsedStreamType->first), streamType);
  auto sgOptions = getSubgroupOptions(GetParam(), streamType);
  auto hdrRes =
      parser_.parseSubgroupHeader(cursor, cursor.totalLength(), sgOptions);
  ASSERT_TRUE(hdrRes.hasValue());

  auto objRes = parser_.parseSubgroupObjectHeader(
      cursor, cursor.totalLength(), hdrRes->value.objectHeader, sgOptions);
  ASSERT_TRUE(objRes.hasValue())
      << "Object should parse successfully (delta-encoded extensions)";
  EXPECT_EQ(objRes->value.group, 2);
  EXPECT_EQ(objRes->value.id, 4);
  EXPECT_EQ(objRes->value.priority, 5);
  EXPECT_EQ(objRes->value.status, ObjectStatus::NORMAL);
  EXPECT_EQ(*objRes->value.length, 4);
  ASSERT_EQ(objRes->value.extensions.size(), 3);
  EXPECT_EQ(objRes->value.extensions.getMutableExtensions()[0].type, 765966);
  EXPECT_EQ(objRes->value.extensions.getMutableExtensions()[0].intValue, 1);
  EXPECT_EQ(objRes->value.extensions.getMutableExtensions()[1].type, 765968);
  EXPECT_EQ(objRes->value.extensions.getMutableExtensions()[1].intValue, 2);
  EXPECT_EQ(objRes->value.extensions.getMutableExtensions()[2].type, 765970);
  EXPECT_EQ(objRes->value.extensions.getMutableExtensions()[2].intValue, 3);
}

// Verify calculateExtensionVectorSize matches actual written size for
// delta-encoded extensions.
TEST_P(MoQFramerV16PlusTest, CalculateExtensionVectorSizeMatchesWritten) {
  // Extensions with large types that benefit from delta encoding
  std::vector<Extension> exts = {
      Extension{100000, 42},
      Extension{100002, 99},
      Extension{100004, 7},
  };
  Extensions extensions(exts, {});

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;
  writer_.writeExtensions(writeBuf, extensions, size, error);
  ASSERT_FALSE(error);

  auto buffer = writeBuf.move();
  auto totalWritten = buffer->computeChainDataLength();

  // The length prefix varint encodes the extension block size.
  // Parse it to get the declared block length.
  folly::io::Cursor cursor(buffer.get());
  auto blockLen = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(blockLen.has_value());

  // The remaining bytes after the length prefix should exactly equal
  // the declared block length.
  auto remainingBytes = cursor.totalLength();
  EXPECT_EQ(blockLen->first, remainingBytes)
      << "Extension block length prefix must match actual extension data size";

  // Also verify the extensions parse correctly
  ObjectHeader obj;
  size_t parseLen = totalWritten;
  folly::io::Cursor parseCursor(buffer.get());
  auto parseResult = parser_.parseExtensions(parseCursor, parseLen, obj);
  ASSERT_TRUE(parseResult.hasValue()) << "Extensions should parse successfully";
  EXPECT_EQ(obj.extensions.size(), 3);
}

// Test TRACK_FILTER parameter parsing and writing for SubscribeNamespace (v16+)
TEST_P(MoQFramerV16PlusTest, SubscribeNamespaceWithTrackFilter) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeNamespace subscribeNamespace;
  subscribeNamespace.requestID = RequestID(42);
  subscribeNamespace.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"conference", "room1"});
  subscribeNamespace.options = SubscribeNamespaceOptions::PUBLISH;
  subscribeNamespace.forward = true;

  // Add TRACK_FILTER parameter: propType=0x10 (audio level), maxSelected=5
  TrackFilter trackFilter{0x10, 5};
  Parameter trackFilterParam(
      folly::to_underlying(TrackRequestParamKey::TRACK_FILTER), trackFilter);
  subscribeNamespace.params.insertParam(trackFilterParam);

  auto writeResult =
      writer_.writeSubscribeNamespace(writeBuf, subscribeNamespace);
  ASSERT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto majorVersion = getDraftMajorVersion(GetParam());
  // Draft 16/17 use the legacy SUBSCRIBE_NAMESPACE wire type 0x11
  // (draft-ietf-moq-transport-16 section 9.25); draft 18 renumbers it to
  // 0x50 via the LEGACY_SUBSCRIBE_NAMESPACE / SUBSCRIBE_NAMESPACE split.
  // This test runs for both draft 16 and draft 18. Use decodeMoQVarint so the
  // 0x50 type (which has a 2-byte QUIC varint prefix) decodes correctly.
  auto frameType = decodeMoQVarint(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(
      frameType->first,
      folly::to_underlying(
          majorVersion >= 18 ? FrameType::SUBSCRIBE_NAMESPACE
                             : FrameType::LEGACY_SUBSCRIBE_NAMESPACE));

  auto parseResult =
      parser_.parseSubscribeNamespace(cursor, frameLength(cursor));
  ASSERT_TRUE(parseResult.hasValue());

  EXPECT_EQ(parseResult->requestID, subscribeNamespace.requestID);
  EXPECT_EQ(
      parseResult->trackNamespacePrefix,
      subscribeNamespace.trackNamespacePrefix);
  EXPECT_EQ(parseResult->forward, subscribeNamespace.forward);
  // Draft 18 dropped the Subscribe Options field from the wire; the parser
  // restores the NAMESPACE default. Pre-18 carries the PUBLISH option set above.
  EXPECT_EQ(
      parseResult->options,
      majorVersion >= 18 ? SubscribeNamespaceOptions::NAMESPACE
                         : SubscribeNamespaceOptions::PUBLISH);

  // Check TRACK_FILTER was parsed correctly
  ASSERT_EQ(parseResult->params.size(), 1);
  auto& parsedParam = parseResult->params.at(0);
  EXPECT_EQ(
      parsedParam.key, folly::to_underlying(TrackRequestParamKey::TRACK_FILTER));
  EXPECT_EQ(parsedParam.asTrackFilter.propertyType, 0x10);
  EXPECT_EQ(parsedParam.asTrackFilter.maxSelected, 5);
}

// Test extractTrackFilter utility function (v16+)
TEST_P(MoQFramerV16PlusTest, ExtractTrackFilter) {
  std::vector<Parameter> params;

  // Add a TRACK_FILTER parameter
  TrackFilter trackFilter{0x22, 10};
  Parameter trackFilterParam(
      folly::to_underlying(TrackRequestParamKey::TRACK_FILTER), trackFilter);
  params.push_back(trackFilterParam);

  // Add another random parameter
  Parameter otherParam(
      folly::to_underlying(TrackRequestParamKey::FORWARD), uint64_t{1});
  params.push_back(otherParam);

  // Extract should find the track filter
  auto extracted = parser_.extractTrackFilter(params);
  ASSERT_TRUE(extracted.has_value());
  EXPECT_EQ(extracted->propertyType, 0x22);
  EXPECT_EQ(extracted->maxSelected, 10);
}

// Test extractTrackFilter returns nullopt when not present (v16+)
TEST_P(MoQFramerV16PlusTest, ExtractTrackFilterNotPresent) {
  std::vector<Parameter> params;

  // Add a non-TRACK_FILTER parameter
  Parameter otherParam(
      folly::to_underlying(TrackRequestParamKey::FORWARD), uint64_t{1});
  params.push_back(otherParam);

  // Extract should return nullopt
  auto extracted = parser_.extractTrackFilter(params);
  EXPECT_FALSE(extracted.has_value());
}

// Test TRACK_FILTER with large values (v16+)
TEST_P(MoQFramerV16PlusTest, TrackFilterLargeValues) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  SubscribeNamespace subscribeNamespace;
  subscribeNamespace.requestID = RequestID(1);
  subscribeNamespace.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"ns"});
  subscribeNamespace.options = SubscribeNamespaceOptions::BOTH;
  subscribeNamespace.forward = true;

  // Use large varint values
  TrackFilter trackFilter{0x3FFFFFFFFFFF, 15};
  Parameter trackFilterParam(
      folly::to_underlying(TrackRequestParamKey::TRACK_FILTER), trackFilter);
  subscribeNamespace.params.insertParam(trackFilterParam);

  auto writeResult =
      writer_.writeSubscribeNamespace(writeBuf, subscribeNamespace);
  ASSERT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto majorVersion = getDraftMajorVersion(GetParam());
  // 0x11 pre-18, 0x50 in draft 18 — see SubscribeNamespaceWithTrackFilter.
  auto frameType = decodeMoQVarint(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(
      frameType->first,
      folly::to_underlying(
          majorVersion >= 18 ? FrameType::SUBSCRIBE_NAMESPACE
                             : FrameType::LEGACY_SUBSCRIBE_NAMESPACE));

  auto parseResult =
      parser_.parseSubscribeNamespace(cursor, frameLength(cursor));
  ASSERT_TRUE(parseResult.hasValue());

  // Check TRACK_FILTER with large propType was parsed correctly
  ASSERT_EQ(parseResult->params.size(), 1);
  auto& parsedParam = parseResult->params.at(0);
  EXPECT_EQ(parsedParam.asTrackFilter.propertyType, 0x3FFFFFFFFFFF);
  EXPECT_EQ(parsedParam.asTrackFilter.maxSelected, 15);
}

// Test TrackFilter struct equality operator
TEST(TrackFilterTest, Equality) {
  TrackFilter a{0x10, 5};
  TrackFilter b{0x10, 5};
  TrackFilter c{0x10, 6};
  TrackFilter d{0x20, 5};

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
  EXPECT_NE(a, d);
}

// Test TrackFilter default constructor
TEST(TrackFilterTest, DefaultConstructor) {
  TrackFilter filter;
  EXPECT_EQ(filter.propertyType, 0);
  EXPECT_EQ(filter.maxSelected, 0);
}

INSTANTIATE_TEST_SUITE_P(
    MoQFramerV16PlusTest,
    MoQFramerV16PlusTest,
    ::testing::Values(kVersionDraft16, kVersionDraft18));

// ===========================================================================
// Draft 18+ tests: SUBSCRIBE_NAMESPACE / SUBSCRIBE_TRACKS split
// ===========================================================================

class MoQFramerV18Test : public ::testing::Test {
 public:
  void SetUp() override {
    parser_.initializeVersion(kVersionDraft18);
    writer_.initializeVersion(kVersionDraft18);
  }

 protected:
  MoQFrameParser parser_;
  MoQFrameWriter writer_;

  size_t frameLength(folly::io::Cursor& cursor) {
    if (!cursor.canAdvance(2)) {
      throw std::runtime_error("Cannot read frame length");
    }
    size_t res = cursor.readBE<uint16_t>();
    if (!cursor.canAdvance(res)) {
      throw std::runtime_error("Frame length exceeds available data");
    }
    return res;
  }
};

TEST_F(MoQFramerV18Test, PaddingTypesAreDraft18Only) {
  EXPECT_TRUE(isPaddingStreamType(
      kVersionDraft18, folly::to_underlying(StreamType::PADDING)));
  EXPECT_FALSE(isPaddingStreamType(
      kVersionDraft17, folly::to_underlying(StreamType::PADDING)));

  EXPECT_TRUE(isPaddingDatagramType(
      kVersionDraft18, folly::to_underlying(DatagramType::PADDING)));
  EXPECT_FALSE(isPaddingDatagramType(
      kVersionDraft17, folly::to_underlying(DatagramType::PADDING)));
}

TEST_F(MoQFramerV18Test, WritePaddingStream) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  auto result = writer_.writePaddingStream(writeBuf, 3);

  ASSERT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  EXPECT_EQ(*result, serialized->computeChainDataLength());
  folly::io::Cursor cursor(serialized.get());
  auto type = parser_.decodeVarint(cursor);
  ASSERT_TRUE(type.has_value());
  EXPECT_EQ(type->first, folly::to_underlying(StreamType::PADDING));

  size_t remainingLength = cursor.totalLength();
  EXPECT_TRUE(parsePaddingData(cursor, remainingLength).hasValue());
  EXPECT_EQ(remainingLength, 0);
  EXPECT_EQ(cursor.totalLength(), 0);
}

TEST_F(MoQFramerV18Test, WritePaddingDatagram) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  auto result = writer_.writePaddingDatagram(writeBuf, 0);

  ASSERT_TRUE(result.hasValue());
  auto serialized = writeBuf.move();
  EXPECT_EQ(*result, serialized->computeChainDataLength());
  folly::io::Cursor cursor(serialized.get());
  auto type = parser_.decodeVarint(cursor);
  ASSERT_TRUE(type.has_value());
  EXPECT_EQ(type->first, folly::to_underlying(DatagramType::PADDING));

  size_t remainingLength = cursor.totalLength();
  EXPECT_TRUE(parsePaddingData(cursor, remainingLength).hasValue());
  EXPECT_EQ(remainingLength, 0);
}

TEST_F(MoQFramerV18Test, PaddingDataRejectsNonZeroBytes) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  const uint8_t invalidPadding[] = {0x00, 0x01};
  writeBuf.append(invalidPadding, sizeof(invalidPadding));

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  size_t remainingLength = serialized->computeChainDataLength();
  auto result = parsePaddingData(cursor, remainingLength);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST_F(MoQFramerV18Test, Draft17WriterRejectsPadding) {
  MoQFrameWriter draft17Writer;
  draft17Writer.initializeVersion(kVersionDraft17);
  folly::IOBufQueue streamBuf{folly::IOBufQueue::cacheChainLength()};
  folly::IOBufQueue datagramBuf{folly::IOBufQueue::cacheChainLength()};

  EXPECT_TRUE(draft17Writer.writePaddingStream(streamBuf, 1).hasError());
  EXPECT_TRUE(draft17Writer.writePaddingDatagram(datagramBuf, 1).hasError());
  EXPECT_TRUE(streamBuf.empty());
  EXPECT_TRUE(datagramBuf.empty());
}

TEST_F(MoQFramerV18Test, PublishOkUsesRequestOkWireType) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  PublishOk publishOk;
  auto majorVersion = getDraftMajorVersion(kVersionDraft18);
  publishOk.requestID = RequestID(55);
  publishOk.forward = false;
  publishOk.subscriberPriority = 42;
  publishOk.groupOrder = GroupOrder::NewestFirst;
  publishOk.locType = LocationType::AbsoluteRange;
  publishOk.start = AbsoluteLocation{3, 4};
  publishOk.endGroup = 9;
  publishOk.params.setMajorVersion(majorVersion);
  ASSERT_TRUE(
      publishOk.params
          .insertParam(Parameter(
              folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST),
              uint64_t(11)))
          .hasValue());
  ASSERT_TRUE(publishOk.params
                  .insertParam(Parameter(
                      folly::to_underlying(TrackRequestParamKey::EXPIRES),
                      uint64_t(1234)))
                  .hasValue());
  ASSERT_TRUE(publishOk.params
                  .insertParam(Parameter(
                      folly::to_underlying(
                          TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT),
                      uint64_t(1000)))
                  .hasValue());
  ASSERT_TRUE(publishOk.params
                  .insertParam(Parameter(
                      folly::to_underlying(
                          TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT),
                      uint64_t(2000)))
                  .hasValue());

  auto writeResult = writer_.writePublishOk(writeBuf, publishOk);
  ASSERT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::REQUEST_OK));

  auto requestOk = parser_.parseRequestOk(
      cursor, frameLength(cursor), FrameType::REQUEST_OK);
  ASSERT_TRUE(requestOk.hasValue());
  EXPECT_EQ(
      getFirstIntParam(
          requestOk->params, TrackRequestParamKey::NEW_GROUP_REQUEST),
      std::nullopt);
  EXPECT_EQ(
      getFirstIntParam(requestOk->params, TrackRequestParamKey::EXPIRES),
      std::nullopt);
  EXPECT_EQ(
      getFirstIntParam(
          requestOk->params, TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT),
      uint64_t(1000));
  EXPECT_EQ(
      getFirstIntParam(
          requestOk->params, TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT),
      uint64_t(2000));

  auto countRequestSpecificParam = [](const std::vector<Parameter>& params,
                                      TrackRequestParamKey key) {
    size_t count = 0;
    auto keyValue = folly::to_underlying(key);
    for (const auto& param : params) {
      if (param.key == keyValue) {
        ++count;
      }
    }
    return count;
  };
  EXPECT_EQ(
      countRequestSpecificParam(
          requestOk->requestSpecificParams,
          TrackRequestParamKey::NEW_GROUP_REQUEST),
      1);
  EXPECT_EQ(
      countRequestSpecificParam(
          requestOk->requestSpecificParams, TrackRequestParamKey::EXPIRES),
      1);

  auto parsed = requestOk->toPublishOk(majorVersion);
  ASSERT_TRUE(parsed.hasValue());
  const auto& parsedPublishOk = parsed.value();
  // Draft 18+ omits request_id from the REQUEST_OK wire format; it is implicit
  // from the bidi stream the response arrives on, so the framer round-trip does
  // not preserve it (the codec substitutes the stream's id on ingress).
  EXPECT_EQ(parsedPublishOk.requestID, RequestID(0));
  EXPECT_EQ(parsedPublishOk.forward, publishOk.forward);
  EXPECT_EQ(parsedPublishOk.subscriberPriority, publishOk.subscriberPriority);
  EXPECT_EQ(parsedPublishOk.groupOrder, publishOk.groupOrder);
  EXPECT_EQ(parsedPublishOk.locType, publishOk.locType);
  EXPECT_EQ(parsedPublishOk.start, publishOk.start);
  EXPECT_EQ(parsedPublishOk.endGroup, publishOk.endGroup);
  EXPECT_EQ(
      getFirstIntParam(
          parsedPublishOk.params, TrackRequestParamKey::NEW_GROUP_REQUEST),
      uint64_t(11));
  EXPECT_EQ(
      getFirstIntParam(parsedPublishOk.params, TrackRequestParamKey::EXPIRES),
      uint64_t(1234));
  EXPECT_EQ(
      getFirstIntParam(
          parsedPublishOk.params,
          TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT),
      uint64_t(1000));
  EXPECT_EQ(
      getFirstIntParam(
          parsedPublishOk.params,
          TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT),
      uint64_t(2000));
}

TEST_F(MoQFramerV18Test, RequestOkToPublishOkRejectsNonPublishOkParams) {
  RequestOk requestOk;
  requestOk.requestID = RequestID(55);
  requestOk.requestSpecificParams.emplace_back(
      folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT),
      std::optional<AbsoluteLocation>(AbsoluteLocation{3, 4}));

  auto parsed = requestOk.toPublishOk(getDraftMajorVersion(kVersionDraft18));
  EXPECT_TRUE(parsed.hasError());
  EXPECT_EQ(parsed.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST_F(MoQFramerV18Test, PublishOkWireTypeRejected) {
  MoQFrameWriter draft17Writer;
  draft17Writer.initializeVersion(kVersionDraft17);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  PublishOk publishOk;
  publishOk.requestID = RequestID(55);
  auto writeResult = draft17Writer.writePublishOk(writeBuf, publishOk);
  ASSERT_TRUE(writeResult.hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::PUBLISH_OK));

  auto parsed = parser_.parsePublishOk(cursor, frameLength(cursor));
  ASSERT_TRUE(parsed.hasError());
  EXPECT_EQ(parsed.error(), ErrorCode::PROTOCOL_VIOLATION);
}

// Draft 18 renumbers SUBSCRIBE_NAMESPACE on the wire from 0x11 to 0x50 and
// drops the Subscribe Options + Forward fields from the message body.
TEST_F(MoQFramerV18Test, SubscribeNamespaceUsesNewWireTypeAndOmitsOptions) {
  SubscribeNamespace req;
  req.requestID = RequestID(42);
  req.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"example.com", "meeting=123"});
  // These pre-18 fields must be ignored on the wire.
  req.options = SubscribeNamespaceOptions::PUBLISH;
  req.forward = false;

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer_.writeSubscribeNamespace(writeBuf, req).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto wireType = decodeMoQVarint(cursor);
  ASSERT_TRUE(wireType.has_value());
  EXPECT_EQ(wireType->first, 0x50u);

  auto bodyLen = frameLength(cursor);
  auto parsed = parser_.parseSubscribeNamespace(cursor, bodyLen);
  ASSERT_TRUE(parsed.hasValue());

  EXPECT_EQ(parsed->requestID, req.requestID);
  EXPECT_EQ(parsed->trackNamespacePrefix, req.trackNamespacePrefix);
  // Defaults are restored on the parse side because the fields aren't on the
  // wire in draft 18. v18 SUBSCRIBE_NAMESPACE is NAMESPACE-only — PUBLISH
  // fan-out moved to the new SUBSCRIBE_TRACKS message.
  EXPECT_EQ(parsed->options, SubscribeNamespaceOptions::NAMESPACE);
  EXPECT_EQ(parsed->forward, true);
}

TEST_F(MoQFramerV18Test, SubscribeTracksRoundtrip) {
  SubscribeTracks req;
  req.requestID = RequestID(7);
  req.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"example.com", "live"});
  req.forward = true;

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer_.writeSubscribeTracks(writeBuf, req).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto wireType = decodeMoQVarint(cursor);
  ASSERT_TRUE(wireType.has_value());
  EXPECT_EQ(wireType->first, folly::to_underlying(FrameType::SUBSCRIBE_TRACKS));
  EXPECT_EQ(wireType->first, 0x51u);

  auto bodyLen = frameLength(cursor);
  auto parsed = parser_.parseSubscribeTracks(cursor, bodyLen);
  ASSERT_TRUE(parsed.hasValue());

  EXPECT_EQ(parsed->requestID, req.requestID);
  EXPECT_EQ(parsed->trackNamespacePrefix, req.trackNamespacePrefix);
  EXPECT_EQ(parsed->forward, true);
}

TEST_F(MoQFramerV18Test, SubscribeTracksForwardFalseSerializedAsParameter) {
  SubscribeTracks req;
  req.requestID = RequestID(99);
  req.trackNamespacePrefix =
      TrackNamespace(std::vector<std::string>{"announce-only"});
  req.forward = false;

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer_.writeSubscribeTracks(writeBuf, req).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ASSERT_TRUE(decodeMoQVarint(cursor).has_value());

  auto bodyLen = frameLength(cursor);
  auto parsed = parser_.parseSubscribeTracks(cursor, bodyLen);
  ASSERT_TRUE(parsed.hasValue());
  EXPECT_EQ(parsed->forward, false);
}

// Draft 18+ REQUEST_ERROR carries a Redirect structure when errorCode ==
// REDIRECT. Verify round-trip of a fully-populated Redirect.
TEST_F(MoQFramerV18Test, RequestErrorRedirectRoundtrip) {
  RequestError requestError;
  requestError.requestID = RequestID(7);
  requestError.errorCode = RequestErrorCode::REDIRECT;
  requestError.reasonPhrase = "moved";
  requestError.retryInterval = std::chrono::milliseconds{1};
  Redirect redirect;
  redirect.connectUri = "https://relay.example.com/moq";
  redirect.fullTrackName.trackNamespace =
      TrackNamespace(std::vector<std::string>{"example.com", "live"});
  redirect.fullTrackName.trackName = "alt-track";
  requestError.redirect = redirect;

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(
      writer_
          .writeRequestError(writeBuf, requestError, FrameType::REQUEST_ERROR)
          .hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = decodeMoQVarint(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::REQUEST_ERROR));

  auto bodyLen = frameLength(cursor);
  auto parsed =
      parser_.parseRequestError(cursor, bodyLen, FrameType::REQUEST_ERROR);
  ASSERT_TRUE(parsed.hasValue());

  // Draft 18+ omits request_id from REQUEST_ERROR (implicit from the bidi
  // stream); the framer round-trip leaves it at the default.
  EXPECT_EQ(parsed->requestID, RequestID(0));
  EXPECT_EQ(parsed->errorCode, RequestErrorCode::REDIRECT);
  EXPECT_EQ(parsed->reasonPhrase, requestError.reasonPhrase);
  EXPECT_EQ(parsed->retryInterval, requestError.retryInterval);
  ASSERT_TRUE(parsed->redirect.has_value());
  EXPECT_EQ(parsed->redirect->connectUri, redirect.connectUri);
  EXPECT_EQ(
      parsed->redirect->fullTrackName.trackNamespace,
      redirect.fullTrackName.trackNamespace);
  EXPECT_EQ(
      parsed->redirect->fullTrackName.trackName,
      redirect.fullTrackName.trackName);
}

// When errorCode == REDIRECT but no Redirect is supplied, the writer
// must refuse — silently substituting an empty Redirect would hide a
// caller bug and produce a misleading on-wire message. Callers that
// genuinely want "reuse current session URI / original Full Track Name"
// must set `redirect` to a default-constructed Redirect explicitly.
TEST_F(MoQFramerV18Test, RequestErrorRedirectWithoutRedirectIsRejected) {
  RequestError requestError;
  requestError.requestID = RequestID(3);
  requestError.errorCode = RequestErrorCode::REDIRECT;
  requestError.reasonPhrase = "";
  requestError.retryInterval = std::chrono::milliseconds{0};
  // Intentionally leave requestError.redirect unset.

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  EXPECT_FALSE(
      writer_
          .writeRequestError(writeBuf, requestError, FrameType::REQUEST_ERROR)
          .hasValue());
}

// Per draft-ietf-moq-transport-18 §10.6.2, REDIRECT is only permitted in
// response to SUBSCRIBE, FETCH, TRACK_STATUS, PUBLISH_NAMESPACE and
// SUBSCRIBE_NAMESPACE. The framer does not know that original request
// context for v18+ REQUEST_ERROR; session-layer validation handles it.
TEST_F(MoQFramerV18Test, RequestErrorRedirectDoesNotValidateRequestType) {
  RequestError requestError;
  requestError.requestID = RequestID(1);
  requestError.errorCode = RequestErrorCode::REDIRECT;
  requestError.reasonPhrase = "";
  requestError.retryInterval = std::chrono::milliseconds{0};
  requestError.redirect = Redirect{};

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  EXPECT_TRUE(
      writer_
          .writeRequestError(writeBuf, requestError, FrameType::PUBLISH_ERROR)
          .hasValue());
  EXPECT_TRUE(writer_
                  .writeRequestError(
                      writeBuf, requestError, FrameType::SUBSCRIBE_UPDATE)
                  .hasValue());
}

// The parser only decodes REQUEST_ERROR bytes. It cannot decide whether a
// REDIRECT is valid for the original request; session-layer enforcement handles
// that using pending request state.
TEST_F(MoQFramerV18Test, ParseRequestErrorDoesNotValidateRequestType) {
  // Hand-craft a REQUEST_ERROR body with errorCode == REDIRECT and a
  // valid Redirect payload.
  RequestError requestError;
  requestError.requestID = RequestID(42);
  requestError.errorCode = RequestErrorCode::REDIRECT;
  requestError.reasonPhrase = "moved";
  requestError.retryInterval = std::chrono::milliseconds{0};
  requestError.redirect = Redirect{};

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(
      writer_.writeRequestError(writeBuf, requestError, FrameType::FETCH_ERROR)
          .hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ASSERT_TRUE(decodeMoQVarint(cursor).has_value()); // frame type
  auto bodyLen = frameLength(cursor);

  // Parsing the same bytes as if they were a PUBLISH_ERROR still succeeds:
  // request-type redirect validation requires session context.
  auto parsed =
      parser_.parseRequestError(cursor, bodyLen, FrameType::PUBLISH_ERROR);
  ASSERT_TRUE(parsed.hasValue());
  EXPECT_EQ(parsed->errorCode, RequestErrorCode::REDIRECT);
  EXPECT_TRUE(parsed->redirect.has_value());
}

// When errorCode is anything other than REDIRECT, no Redirect bytes are
// written, and the parsed RequestError has no redirect.
TEST_F(MoQFramerV18Test, RequestErrorNonRedirectOmitsRedirect) {
  RequestError requestError;
  requestError.requestID = RequestID(9);
  requestError.errorCode = RequestErrorCode::INTERNAL_ERROR;
  requestError.reasonPhrase = "boom";
  requestError.retryInterval = std::chrono::milliseconds{0};
  // Set redirect anyway to confirm the writer ignores it for non-REDIRECT.
  Redirect redirect;
  redirect.connectUri = "ignored";
  requestError.redirect = redirect;

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(
      writer_
          .writeRequestError(writeBuf, requestError, FrameType::REQUEST_ERROR)
          .hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = decodeMoQVarint(cursor);
  ASSERT_TRUE(frameType.has_value());

  auto bodyLen = frameLength(cursor);
  auto parsed =
      parser_.parseRequestError(cursor, bodyLen, FrameType::REQUEST_ERROR);
  ASSERT_TRUE(parsed.hasValue());
  EXPECT_FALSE(parsed->redirect.has_value());
}

TEST_F(MoQFramerV18Test, PublishBlockedRoundtrip) {
  PublishBlocked req;
  req.trackNamespaceSuffix =
      TrackNamespace(std::vector<std::string>{"sports", "soccer"});
  req.trackName = "highlights";

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer_.writePublishBlocked(writeBuf, req).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  auto wireType = decodeMoQVarint(cursor);
  ASSERT_TRUE(wireType.has_value());
  EXPECT_EQ(wireType->first, folly::to_underlying(FrameType::PUBLISH_BLOCKED));
  EXPECT_EQ(wireType->first, 0xFu);

  auto bodyLen = frameLength(cursor);
  auto parsed = parser_.parsePublishBlocked(cursor, bodyLen);
  ASSERT_TRUE(parsed.hasValue());

  EXPECT_EQ(parsed->trackNamespaceSuffix, req.trackNamespaceSuffix);
  EXPECT_EQ(parsed->trackName, req.trackName);
}

// Draft 18 added a Track Properties block at the end of REQUEST_OK. Verify
// that a populated TRACK_STATUS_OK round-trips its Track Properties through
// the wire and back into the TrackStatusOk struct.
TEST_F(MoQFramerV18Test, RequestOkTrackPropertiesRoundtrip) {
  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = 42;
  trackStatusOk.statusCode = TrackStatusCode::IN_PROGRESS;
  trackStatusOk.largest = AbsoluteLocation({3, 7});
  trackStatusOk.groupOrder = GroupOrder::OldestFirst;
  trackStatusOk.expires = std::chrono::milliseconds(2500);
  // Populate Track Properties with a couple of well-known properties.
  trackStatusOk.trackProperties.insertMutableExtension(
      Extension{kPublisherPriorityExtensionType, 100});
  trackStatusOk.trackProperties.insertMutableExtension(
      Extension{kDynamicGroupsExtensionType, 1});

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer_.writeTrackStatusOk(writeBuf, trackStatusOk).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::REQUEST_OK));

  auto bodyLen = frameLength(cursor);
  auto result = parser_.parseRequestOk(cursor, bodyLen, FrameType::REQUEST_OK);
  ASSERT_TRUE(result.hasValue());

  auto parsed = result->toTrackStatusOk();
  // Draft 18+ removed requestID from REQUEST_OK; codec sets it from stream ctx.
  EXPECT_EQ(parsed.expires, std::chrono::milliseconds(2500));
  ASSERT_TRUE(parsed.largest.has_value());
  EXPECT_EQ(parsed.largest->group, 3);
  EXPECT_EQ(parsed.largest->object, 7);

  EXPECT_EQ(parsed.trackProperties, trackStatusOk.trackProperties);
}

// REQUEST_OK with empty Track Properties must not emit any additional bytes
// past the parameter block (the spec encodes "no properties" by the absence
// of bytes — there is no count or length prefix).
TEST_F(MoQFramerV18Test, RequestOkEmptyTrackPropertiesEmitsNoBytes) {
  RequestOk requestOk;
  requestOk.requestID = RequestID(11);
  // trackProperties left empty intentionally.

  folly::IOBufQueue v18Buf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer_.writeRequestOk(v18Buf, requestOk, FrameType::REQUEST_OK)
                  .hasValue());

  // Draft 18 wire layout is: frame_type + length + num_params. requestID is
  // no longer on the wire (it is implicit from the bidi request stream).
  auto v18Bytes = v18Buf.move();
  folly::io::Cursor cursor(v18Bytes.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::REQUEST_OK));
  auto bodyLen = frameLength(cursor);
  auto numParams = quic::follyutils::decodeQuicInteger(cursor, bodyLen);
  ASSERT_TRUE(numParams.has_value());
  EXPECT_EQ(numParams->first, 0u);
  EXPECT_EQ(bodyLen, numParams->second);
}

// writeRequestOk must refuse to emit a frame whose semantic frame type is
// anything other than TRACK_STATUS_OK when trackProperties is non-empty - the
// spec requires Track Properties to be empty for those response shorthands.
TEST_F(
    MoQFramerV18Test,
    WriteRequestOkRejectsTrackPropertiesForNonTrackStatus) {
  RequestOk requestOk;
  requestOk.requestID = RequestID(5);
  requestOk.trackProperties.insertMutableExtension(
      Extension{kMaxCacheDurationExtensionType, 30000});

  const std::array<FrameType, 4> nonTrackStatusFrameTypes{
      FrameType::REQUEST_OK,
      FrameType::PUBLISH_NAMESPACE_OK,
      FrameType::SUBSCRIBE_NAMESPACE_OK,
      FrameType::PUBLISH_OK,
  };
  for (auto frameType : nonTrackStatusFrameTypes) {
    folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
    auto result = writer_.writeRequestOk(writeBuf, requestOk, frameType);
    ASSERT_TRUE(result.hasError()) << "Expected write failure for frameType="
                                   << folly::to_underlying(frameType);
    EXPECT_EQ(result.error(), quic::TransportErrorCode::PROTOCOL_VIOLATION);
  }

  // Sanity check: writing TRACK_STATUS_OK with the same trackProperties does
  // succeed so the test isn't accidentally validating the wrong invariant.
  folly::IOBufQueue okBuf{folly::IOBufQueue::cacheChainLength()};
  EXPECT_TRUE(
      writer_.writeRequestOk(okBuf, requestOk, FrameType::TRACK_STATUS_OK)
          .hasValue());
}

// Older drafts (e.g. draft 17) must not serialize or parse Track Properties
// on REQUEST_OK; a TrackStatusOk with trackProperties populated must
// roundtrip without the extra bytes.
TEST(MoQFramerRequestOkTrackProperties, Draft17DoesNotEmitTrackProperties) {
  MoQFrameWriter writer;
  MoQFrameParser parser;
  writer.initializeVersion(kVersionDraft17);
  parser.initializeVersion(kVersionDraft17);

  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = 1;
  trackStatusOk.statusCode = TrackStatusCode::IN_PROGRESS;
  trackStatusOk.largest = AbsoluteLocation({1, 2});
  trackStatusOk.expires = std::chrono::milliseconds(500);
  // Populate trackProperties even though draft 17 should ignore them.
  trackStatusOk.trackProperties.insertMutableExtension(
      Extension{kPublisherPriorityExtensionType, 100});

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer.writeTrackStatusOk(writeBuf, trackStatusOk).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ASSERT_TRUE(quic::follyutils::decodeQuicInteger(cursor).has_value());
  size_t bodyLen = cursor.readBE<uint16_t>();
  auto result = parser.parseRequestOk(cursor, bodyLen, FrameType::REQUEST_OK);
  ASSERT_TRUE(result.hasValue());

  // Draft 17 has no Track Properties on the wire so it parses back as empty.
  EXPECT_TRUE(result->trackProperties.empty());
}

TEST_F(MoQFramerV18Test, FetchObjectDeltaDecodesAscendingOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x1c, // group delta, object delta, priority
      10,
      std::nullopt,
      4,
      7);
  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x04, // same group, object delta
      std::nullopt,
      std::nullopt,
      3,
      std::nullopt);
  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x0c, // group delta, object delta
      1,
      std::nullopt,
      2,
      std::nullopt);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ObjectHeader headerTemplate;

  auto first = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(first.hasValue());
  ASSERT_TRUE(std::holds_alternative<ObjectHeader>(first->value));
  const auto& firstObj = std::get<ObjectHeader>(first->value);
  EXPECT_EQ(firstObj.group, 10);
  EXPECT_EQ(firstObj.id, 4);
  EXPECT_EQ(firstObj.priority, 7);

  auto second = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(second.hasValue());
  ASSERT_TRUE(std::holds_alternative<ObjectHeader>(second->value));
  const auto& secondObj = std::get<ObjectHeader>(second->value);
  EXPECT_EQ(secondObj.group, 10);
  EXPECT_EQ(secondObj.id, 7);
  EXPECT_EQ(secondObj.priority, 7);

  auto third = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(third.hasValue());
  ASSERT_TRUE(std::holds_alternative<ObjectHeader>(third->value));
  const auto& thirdObj = std::get<ObjectHeader>(third->value);
  EXPECT_EQ(thirdObj.group, 12);
  EXPECT_EQ(thirdObj.id, 2);
}

TEST_F(MoQFramerV18Test, FetchEndOfRangeSetsPriorGroupAndObject) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x1c, // group delta, object delta, priority
      5,
      std::nullopt,
      10,
      7);
  writeMoQVarintTo(writeBuf, kSerializationFlagEndOfUnknownRange);
  writeMoQVarintTo(writeBuf, 5);
  writeMoQVarintTo(writeBuf, 20);
  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x00, // same group, object ID is prior object ID plus one
      std::nullopt,
      std::nullopt,
      std::nullopt,
      std::nullopt);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ObjectHeader headerTemplate;

  auto first = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(first.hasValue());

  auto markerResult = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(markerResult.hasValue());
  ASSERT_TRUE(
      std::holds_alternative<MoQFrameParser::EndOfRangeMarker>(
          markerResult->value));
  const auto& marker =
      std::get<MoQFrameParser::EndOfRangeMarker>(markerResult->value);
  EXPECT_EQ(marker.groupId, 5);
  EXPECT_EQ(marker.objectId, 20);
  EXPECT_TRUE(marker.isUnknownOrNonexistent);

  auto next = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(next.hasValue());
  ASSERT_TRUE(std::holds_alternative<ObjectHeader>(next->value));
  const auto& nextObj = std::get<ObjectHeader>(next->value);
  EXPECT_EQ(nextObj.group, 5);
  EXPECT_EQ(nextObj.id, 21);
  EXPECT_EQ(nextObj.priority, 7);
}

TEST_F(MoQFramerV18Test, FetchObjectDeltaDecodesDescendingOrder) {
  parser_.setFetchGroupOrder(GroupOrder::NewestFirst);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x1c, // group delta, object delta, priority
      10,
      std::nullopt,
      5,
      9);
  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x0c, // group delta, object delta
      2,
      std::nullopt,
      1,
      std::nullopt);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ObjectHeader headerTemplate;

  auto first = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(first.hasValue());
  auto second = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(second.hasValue());
  ASSERT_TRUE(std::holds_alternative<ObjectHeader>(second->value));
  const auto& secondObj = std::get<ObjectHeader>(second->value);
  EXPECT_EQ(secondObj.group, 7);
  EXPECT_EQ(secondObj.id, 1);
  EXPECT_EQ(secondObj.priority, 9);
}

TEST_F(MoQFramerV18Test, FetchFirstObjectRequiresGroupAndObjectDeltas) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x18, // group delta and priority, but no object delta
      10,
      std::nullopt,
      std::nullopt,
      7);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ObjectHeader headerTemplate;
  auto result = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST_F(MoQFramerV18Test, FetchRejectsInvalidSerializationFlagsAbove127) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x1c, // group delta, object delta, priority
      5,
      std::nullopt,
      10,
      7);
  writeMoQVarintTo(writeBuf, 0x100);
  writeMoQVarintTo(writeBuf, 1);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ObjectHeader headerTemplate;
  auto first = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(first.hasValue());

  auto result = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST_F(MoQFramerV18Test, FetchObjectDeltaRejectsDescendingGroupUnderflow) {
  parser_.setFetchGroupOrder(GroupOrder::NewestFirst);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x1c, // group delta, object delta, priority
      0,
      std::nullopt,
      1,
      7);
  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x0c, // group delta, object delta
      0,
      std::nullopt,
      1,
      std::nullopt);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ObjectHeader headerTemplate;
  auto first = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(first.hasValue());

  auto second = parser_.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(second.hasError());
  EXPECT_EQ(second.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST(MoQFramerFetchObjectDelta, Draft17FieldsRemainAbsolute) {
  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft17);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x1c, // group ID, object ID, priority
      10,
      std::nullopt,
      4,
      7);
  writeFetchObjectWithSerializationFlags(
      writeBuf,
      0x0c, // group ID, object ID
      20,
      std::nullopt,
      3,
      std::nullopt);

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  ObjectHeader headerTemplate;
  auto first = parser.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(first.hasValue());

  auto second = parser.parseFetchObjectHeader(
      cursor, cursor.totalLength(), headerTemplate);
  ASSERT_TRUE(second.hasValue());
  ASSERT_TRUE(std::holds_alternative<ObjectHeader>(second->value));
  const auto& secondObj = std::get<ObjectHeader>(second->value);
  EXPECT_EQ(secondObj.group, 20);
  EXPECT_EQ(secondObj.id, 3);
}

TEST_F(MoQFramerV18Test, WriteFetchObjectDeltaEncodesAscendingOrder) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer_.writeFetchHeader(writeBuf, RequestID(1)).hasValue());

  ObjectHeader first(10, 0, 4, 7, 1);
  ASSERT_TRUE(writer_
                  .writeStreamObject(
                      writeBuf,
                      StreamType::FETCH_HEADER,
                      first,
                      folly::IOBuf::copyBuffer("a"))
                  .hasValue());
  ObjectHeader second(10, 0, 7, 7, 1);
  ASSERT_TRUE(writer_
                  .writeStreamObject(
                      writeBuf,
                      StreamType::FETCH_HEADER,
                      second,
                      folly::IOBuf::copyBuffer("b"))
                  .hasValue());
  ObjectHeader third(12, 0, 2, 7, 1);
  ASSERT_TRUE(writer_
                  .writeStreamObject(
                      writeBuf,
                      StreamType::FETCH_HEADER,
                      third,
                      folly::IOBuf::copyBuffer("c"))
                  .hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());

  EXPECT_EQ(
      readVarintFrom(cursor), folly::to_underlying(StreamType::FETCH_HEADER));
  EXPECT_EQ(readVarintFrom(cursor), RequestID(1).value);

  EXPECT_EQ(readVarintFrom(cursor), 0x1c);
  EXPECT_EQ(readVarintFrom(cursor), 10);
  EXPECT_EQ(readVarintFrom(cursor), 4);
  EXPECT_EQ(cursor.readBE<uint8_t>(), 7);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  cursor.skip(1);

  EXPECT_EQ(readVarintFrom(cursor), 0x04);
  EXPECT_EQ(readVarintFrom(cursor), 3);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  cursor.skip(1);

  EXPECT_EQ(readVarintFrom(cursor), 0x0c);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  EXPECT_EQ(readVarintFrom(cursor), 2);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  cursor.skip(1);
  EXPECT_EQ(cursor.totalLength(), 0);
}

TEST_F(MoQFramerV18Test, WriteFetchObjectDeltaEncodesDescendingOrder) {
  writer_.setFetchGroupOrder(GroupOrder::NewestFirst);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer_.writeFetchHeader(writeBuf, RequestID(1)).hasValue());

  ObjectHeader first(10, 0, 5, 9, 1);
  ASSERT_TRUE(writer_
                  .writeStreamObject(
                      writeBuf,
                      StreamType::FETCH_HEADER,
                      first,
                      folly::IOBuf::copyBuffer("a"))
                  .hasValue());
  ObjectHeader second(7, 0, 1, 9, 1);
  ASSERT_TRUE(writer_
                  .writeStreamObject(
                      writeBuf,
                      StreamType::FETCH_HEADER,
                      second,
                      folly::IOBuf::copyBuffer("b"))
                  .hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  EXPECT_EQ(
      readVarintFrom(cursor), folly::to_underlying(StreamType::FETCH_HEADER));
  EXPECT_EQ(readVarintFrom(cursor), RequestID(1).value);

  EXPECT_EQ(readVarintFrom(cursor), 0x1c);
  EXPECT_EQ(readVarintFrom(cursor), 10);
  EXPECT_EQ(readVarintFrom(cursor), 5);
  EXPECT_EQ(cursor.readBE<uint8_t>(), 9);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  cursor.skip(1);

  EXPECT_EQ(readVarintFrom(cursor), 0x0c);
  EXPECT_EQ(readVarintFrom(cursor), 2);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  cursor.skip(1);
  EXPECT_EQ(cursor.totalLength(), 0);
}

TEST(MoQFramerFetchObjectDelta, Draft17WriterFieldsRemainAbsolute) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft17);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer.writeFetchHeader(writeBuf, RequestID(1)).hasValue());

  ObjectHeader first(10, 0, 4, 7, 1);
  ASSERT_TRUE(writer
                  .writeStreamObject(
                      writeBuf,
                      StreamType::FETCH_HEADER,
                      first,
                      folly::IOBuf::copyBuffer("a"))
                  .hasValue());
  ObjectHeader second(20, 0, 3, 7, 1);
  ASSERT_TRUE(writer
                  .writeStreamObject(
                      writeBuf,
                      StreamType::FETCH_HEADER,
                      second,
                      folly::IOBuf::copyBuffer("b"))
                  .hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  EXPECT_EQ(
      readVarintFrom(cursor), folly::to_underlying(StreamType::FETCH_HEADER));
  EXPECT_EQ(readVarintFrom(cursor), RequestID(1).value);

  EXPECT_EQ(readVarintFrom(cursor), 0x1c);
  EXPECT_EQ(readVarintFrom(cursor), 10);
  EXPECT_EQ(readVarintFrom(cursor), 4);
  EXPECT_EQ(cursor.readBE<uint8_t>(), 7);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  cursor.skip(1);

  EXPECT_EQ(readVarintFrom(cursor), 0x0c);
  EXPECT_EQ(readVarintFrom(cursor), 20);
  EXPECT_EQ(readVarintFrom(cursor), 3);
  EXPECT_EQ(readVarintFrom(cursor), 1);
  cursor.skip(1);
  EXPECT_EQ(cursor.totalLength(), 0);
}

// Draft 17 must continue to use wire type 0x11 for SUBSCRIBE_NAMESPACE.
TEST(MoQFramerWireTypeTranslation, Draft17UsesLegacyWireType) {
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft17);
  SubscribeNamespace req;
  req.requestID = RequestID(1);
  req.trackNamespacePrefix = TrackNamespace(std::vector<std::string>{"x"});

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(writer.writeSubscribeNamespace(writeBuf, req).hasValue());

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto wireType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(wireType.has_value());
  EXPECT_EQ(wireType->first, 0x11u);
}

// Wire-level enumerator values: each FrameType integer IS a wire integer.
// LEGACY_SUBSCRIBE_NAMESPACE is the v17- wire value (0x11);
// SUBSCRIBE_NAMESPACE is the v18+ wire value (0x50). The writer picks between
// them based on the negotiated version; the parser accepts either and
// dispatches to the same handler.
TEST(MoQFramerSubscribeNamespaceWireType, EnumValuesMatchSpec) {
  EXPECT_EQ(folly::to_underlying(FrameType::LEGACY_SUBSCRIBE_NAMESPACE), 0x11u);
  EXPECT_EQ(folly::to_underlying(FrameType::SUBSCRIBE_NAMESPACE), 0x50u);
  EXPECT_EQ(folly::to_underlying(FrameType::SUBSCRIBE_TRACKS), 0x51u);
}

// On v18, the writer emits the renumbered SUBSCRIBE_NAMESPACE wire type 0x50
// (covered by SubscribeNamespaceUsesNewWireTypeAndOmitsOptions above) and the
// parser must continue to accept the legacy 0x11 wire type as input — round
// tripping a v17-emitted frame through the parser still produces a valid
// SubscribeNamespace.
TEST(MoQFramerSubscribeNamespaceWireType, ParserAcceptsLegacyWireType) {
  MoQFrameWriter v17Writer;
  v17Writer.initializeVersion(kVersionDraft17);
  SubscribeNamespace req;
  req.requestID = RequestID(7);
  req.trackNamespacePrefix = TrackNamespace(std::vector<std::string>{"foo"});
  folly::IOBufQueue buf{folly::IOBufQueue::cacheChainLength()};
  ASSERT_TRUE(v17Writer.writeSubscribeNamespace(buf, req).hasValue());

  auto serialized = buf.move();
  folly::io::Cursor cursor(serialized.get());
  auto wireType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(wireType.has_value());
  EXPECT_EQ(wireType->first, 0x11u);

  MoQFrameParser v17Parser;
  v17Parser.initializeVersion(kVersionDraft17);
  ASSERT_TRUE(cursor.canAdvance(2));
  size_t bodyLen = cursor.readBE<uint16_t>();
  auto parsed = v17Parser.parseSubscribeNamespace(cursor, bodyLen);
  ASSERT_TRUE(parsed.hasValue());
  EXPECT_EQ(parsed->requestID, req.requestID);
  EXPECT_EQ(parsed->trackNamespacePrefix, req.trackNamespacePrefix);
}

// Death tests for v16+ PublishNamespaceDone/PublishNamespaceCancel without
// requestID
TEST(MoQFramerV16DeathTest, PublishNamespaceDoneWithoutRequestIDDies) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft16);

  // Create PublishNamespaceDone without requestID set (only trackNamespace)
  PublishNamespaceDone publishNamespaceDone;
  publishNamespaceDone.trackNamespace = TrackNamespace({"hello"});
  // Note: requestID is not set

  EXPECT_DEATH(
      writer.writePublishNamespaceDone(writeBuf, publishNamespaceDone),
      "RequestID required for v16\\+ PublishNamespaceDone");
}

TEST(MoQFramerV16DeathTest, PublishNamespaceCancelWithoutRequestIDDies) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft16);

  // Create PublishNamespaceCancel without requestID set (only trackNamespace)
  PublishNamespaceCancel publishNamespaceCancel;
  publishNamespaceCancel.trackNamespace = TrackNamespace({"hello"});
  publishNamespaceCancel.errorCode = PublishNamespaceErrorCode::INTERNAL_ERROR;
  publishNamespaceCancel.reasonPhrase = "internal error";
  // Note: requestID is not set

  EXPECT_DEATH(
      writer.writePublishNamespaceCancel(writeBuf, publishNamespaceCancel),
      "RequestID required for v16\\+ PublishNamespaceCancel");
}

// Tests for Parameters::isParamAllowed()
class ParametersIsParamAllowedTest : public ::testing::Test {};

TEST_F(ParametersIsParamAllowedTest, ParamAllowedForFrameType) {
  Parameters params(FrameType::SUBSCRIBE);
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::DELIVERY_TIMEOUT));
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::AUTHORIZATION_TOKEN));
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::SUBSCRIBER_PRIORITY));
}

TEST_F(ParametersIsParamAllowedTest, ParamNotAllowedForFrameType) {
  Parameters params(FrameType::FETCH);
  EXPECT_FALSE(params.isParamAllowed(TrackRequestParamKey::DELIVERY_TIMEOUT));
  EXPECT_FALSE(params.isParamAllowed(TrackRequestParamKey::EXPIRES));
}

TEST_F(ParametersIsParamAllowedTest, TrackFilterAllowedOnlyForSubscribeNamespace) {
  // TRACK_FILTER should only be allowed for SUBSCRIBE_NAMESPACE
  Parameters paramsSubNs(FrameType::SUBSCRIBE_NAMESPACE);
  EXPECT_TRUE(paramsSubNs.isParamAllowed(TrackRequestParamKey::TRACK_FILTER));

  // Not allowed for other frame types
  Parameters paramsSubscribe(FrameType::SUBSCRIBE);
  EXPECT_FALSE(
      paramsSubscribe.isParamAllowed(TrackRequestParamKey::TRACK_FILTER));

  Parameters paramsFetch(FrameType::FETCH);
  EXPECT_FALSE(paramsFetch.isParamAllowed(TrackRequestParamKey::TRACK_FILTER));

  Parameters paramsPublishOk(FrameType::PUBLISH_OK);
  EXPECT_FALSE(
      paramsPublishOk.isParamAllowed(TrackRequestParamKey::TRACK_FILTER));
}

TEST_F(ParametersIsParamAllowedTest, ParamAllowedForAllFrameTypes) {
  // MAX_CACHE_DURATION and PUBLISHER_PRIORITY have empty sets = allowed for
  // all
  Parameters paramsPublishNamespace(FrameType::PUBLISH_NAMESPACE);
  EXPECT_TRUE(paramsPublishNamespace.isParamAllowed(
      TrackRequestParamKey::MAX_CACHE_DURATION));
  EXPECT_TRUE(paramsPublishNamespace.isParamAllowed(
      TrackRequestParamKey::PUBLISHER_PRIORITY));

  Parameters paramsFetch(FrameType::FETCH);
  EXPECT_TRUE(
      paramsFetch.isParamAllowed(TrackRequestParamKey::MAX_CACHE_DURATION));
  EXPECT_TRUE(
      paramsFetch.isParamAllowed(TrackRequestParamKey::PUBLISHER_PRIORITY));
}

// TODO: Should return false when we drop v15- support
TEST_F(ParametersIsParamAllowedTest, UnknownParamKeyReturnsTrue) {
  Parameters params(FrameType::SUBSCRIBE);
  // Cast an unknown value to TrackRequestParamKey
  auto unknownKey = static_cast<TrackRequestParamKey>(9999);
  EXPECT_TRUE(params.isParamAllowed(unknownKey));
}

TEST_F(ParametersIsParamAllowedTest, MultipleParamsMixedResults) {
  Parameters params(FrameType::PUBLISH_OK);
  // Allowed for PUBLISH_OK
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::DELIVERY_TIMEOUT));
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::SUBSCRIBER_PRIORITY));
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::SUBSCRIPTION_FILTER));
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::EXPIRES));
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::GROUP_ORDER));
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::FORWARD));
  // NOT allowed for PUBLISH_OK
  EXPECT_FALSE(
      params.isParamAllowed(TrackRequestParamKey::AUTHORIZATION_TOKEN));
  EXPECT_FALSE(params.isParamAllowed(TrackRequestParamKey::LARGEST_OBJECT));
}

// Draft 18 dropped FORWARD from SUBSCRIBE_NAMESPACE (split into
// SUBSCRIBE_TRACKS). Receivers MUST treat its presence in a draft-18
// SUBSCRIBE_NAMESPACE as PROTOCOL_VIOLATION (§10.2.12, §10.2.1).
TEST_F(ParametersIsParamAllowedTest, ForwardForbiddenOnSubscribeNamespaceV18) {
  Parameters paramsV18(FrameType::SUBSCRIBE_NAMESPACE);
  paramsV18.setMajorVersion(18);
  EXPECT_FALSE(paramsV18.isParamAllowed(TrackRequestParamKey::FORWARD));

  // SUBSCRIBE_TRACKS still accepts FORWARD on draft 18.
  Parameters tracksV18(FrameType::SUBSCRIBE_TRACKS);
  tracksV18.setMajorVersion(18);
  EXPECT_TRUE(tracksV18.isParamAllowed(TrackRequestParamKey::FORWARD));

  // Pre-18 (draft 17), FORWARD on SUBSCRIBE_NAMESPACE is still allowed.
  Parameters paramsV17(FrameType::SUBSCRIBE_NAMESPACE);
  paramsV17.setMajorVersion(17);
  EXPECT_TRUE(paramsV17.isParamAllowed(TrackRequestParamKey::FORWARD));
}

// Draft-18-only parameter keys (SUBGROUP_DELIVERY_TIMEOUT 0x06, FILL_TIMEOUT
// 0x0A, TRACK_NAMESPACE_PREFIX 0x34) reuse no earlier key value, so below
// draft 18 they must be unknown -- which makes parseParams hard-reject them
// with PROTOCOL_VIOLATION, preserving pre-18 behavior. At v18 they are known.
TEST_F(ParametersIsParamAllowedTest, V18OnlyParamKeysUnknownBeforeV18) {
  for (auto key :
       {TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT,
        TrackRequestParamKey::FILL_TIMEOUT,
        TrackRequestParamKey::TRACK_NAMESPACE_PREFIX}) {
    auto rawKey = folly::to_underlying(key);
    EXPECT_FALSE(Parameters::isKnownParamKey(rawKey, 17));
    EXPECT_TRUE(Parameters::isKnownParamKey(rawKey, 18));
  }
}

// Keys present in every supported draft -- including the key values that are
// reinterpreted in v18 (0x02 DELIVERY_TIMEOUT/OBJECT_DELIVERY_TIMEOUT, 0x04
// MAX_CACHE_DURATION/RENDEZVOUS_TIMEOUT) -- stay known across versions.
TEST_F(ParametersIsParamAllowedTest, StableParamKeysKnownAcrossVersions) {
  for (auto key :
       {TrackRequestParamKey::DELIVERY_TIMEOUT,
        TrackRequestParamKey::MAX_CACHE_DURATION,
        TrackRequestParamKey::SUBSCRIBER_PRIORITY}) {
    auto rawKey = folly::to_underlying(key);
    EXPECT_TRUE(Parameters::isKnownParamKey(rawKey, 17));
    EXPECT_TRUE(Parameters::isKnownParamKey(rawKey, 18));
  }
}

class ParameterValidationFlowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    writer_.initializeVersion(kVersionDraft15);
    parser_.initializeVersion(kVersionDraft15);
  }

  size_t frameLength(folly::io::Cursor& cursor) {
    size_t res = cursor.readBE<uint16_t>();
    return res;
  }

  MoQFrameWriter writer_;
  MoQFrameParser parser_;
};

TEST_F(ParameterValidationFlowTest, SubscribeRequestMakeSkipsInvalidParam) {
  // EXPIRES is NOT allowed for SUBSCRIBE
  // Pass as vector - make will validate and skip invalid params
  std::vector<Parameter> inputParams{
      Parameter(folly::to_underlying(TrackRequestParamKey::EXPIRES), 1000)};

  auto req = SubscribeRequest::make(
      FullTrackName({TrackNamespace({"ns"}), "track"}),
      kDefaultPriority,
      GroupOrder::Default,
      true,
      LocationType::LargestGroup,
      std::nullopt,
      0,
      inputParams);

  // The invalid param should have been skipped
  EXPECT_EQ(req.params.size(), 0);
}

TEST_F(ParameterValidationFlowTest, FetchConstructorSkipsInvalidParam) {
  // DELIVERY_TIMEOUT is NOT allowed for FETCH
  // Fetch constructor validates and skips invalid params
  std::vector<Parameter> inputParams{Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 5000)};

  Fetch fetch(
      RequestID(1),
      FullTrackName({TrackNamespace({"ns"}), "track"}),
      AbsoluteLocation{0, 0},
      AbsoluteLocation{10, 10},
      kDefaultPriority,
      GroupOrder::Default,
      inputParams);

  // The invalid param should have been skipped
  EXPECT_EQ(fetch.params.size(), 0);
}

TEST_F(ParameterValidationFlowTest, ParseParamsIgnoresInvalidParam) {
  // Create a SubscribeRequest with a VALID param first
  std::vector<Parameter> validParams{Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 5000)};

  auto req = SubscribeRequest::make(
      FullTrackName({TrackNamespace({"ns"}), "track"}),
      kDefaultPriority,
      GroupOrder::Default,
      true,
      LocationType::LargestGroup,
      std::nullopt,
      0,
      validParams);

  // Serialize
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto writeRes = writer_.writeSubscribeRequest(writeBuf, req);
  EXPECT_TRUE(writeRes.hasValue());

  // Now manually append an invalid param (EXPIRES) to the serialized buffer
  // For testing the receive path, we create a separate SubscribeRequest
  // that bypasses write-time validation by using a Parameters without
  // frameType

  // Create a SubscribeRequest struct directly (not via make()) so we can
  // add params that bypass validation
  SubscribeRequest reqWithInvalidParam{
      RequestID(0),
      FullTrackName({TrackNamespace({"ns2"}), "track2"}),
      kDefaultPriority,
      GroupOrder::Default,
      true,
      LocationType::LargestGroup,
      std::nullopt,
      0};

  // Add valid param using insertParam
  reqWithInvalidParam.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 5000));
  // Try to insert EXPIRES which is invalid for SUBSCRIBE - it will be
  // rejected
  auto insertResult = reqWithInvalidParam.params.insertParam(
      Parameter(folly::to_underlying(TrackRequestParamKey::EXPIRES), 1000));
  // The insertion of invalid param should fail
  EXPECT_TRUE(insertResult.hasError());

  // Verify only the valid param is present
  EXPECT_EQ(reqWithInvalidParam.params.size(), 1);

  // Serialize the request
  folly::IOBufQueue writeBuf2{folly::IOBufQueue::cacheChainLength()};
  auto writeRes2 =
      writer_.writeSubscribeRequest(writeBuf2, reqWithInvalidParam);
  EXPECT_TRUE(writeRes2.hasValue());

  // Parse and verify DELIVERY_TIMEOUT is present
  auto serialized = writeBuf2.move();
  folly::io::Cursor cursor(serialized.get());

  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));

  auto parseRes = parser_.parseSubscribeRequest(cursor, frameLength(cursor));
  EXPECT_TRUE(parseRes.hasValue());

  // DELIVERY_TIMEOUT should be present (valid for SUBSCRIBE)
  // EXPIRES should not be present (was rejected during insertion)
  bool foundDeliveryTimeout = false;
  bool foundExpires = false;
  for (const auto& param : parseRes->params) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT)) {
      foundDeliveryTimeout = true;
    }
    if (param.key == folly::to_underlying(TrackRequestParamKey::EXPIRES)) {
      foundExpires = true;
    }
  }
  EXPECT_TRUE(foundDeliveryTimeout);
  EXPECT_FALSE(foundExpires);
}

TEST_F(ParameterValidationFlowTest, SubscribeOkRejectsInvalidParam) {
  // This tests the same code path as MoQForwarder::Subscriber::setParam()
  // DELIVERY_TIMEOUT is NOT allowed for SUBSCRIBE_OK (only for PUBLISH_OK,
  // SUBSCRIBE, SUBSCRIBE_UPDATE)
  SubscribeOk subscribeOk;
  subscribeOk.requestID = RequestID(1);
  subscribeOk.trackAlias = TrackAlias(1);
  subscribeOk.expires = std::chrono::milliseconds(1000);
  subscribeOk.groupOrder = GroupOrder::Default;
  subscribeOk.largest = std::nullopt;

  // Try to insert a param that's not allowed for SUBSCRIBE_OK
  auto result = subscribeOk.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 5000));

  // The insertion should fail
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error(), ErrorCode::INVALID_REQUEST_ID);
  EXPECT_EQ(subscribeOk.params.size(), 0);

  // Try to insert a param that IS allowed for SUBSCRIBE_OK (EXPIRES)
  auto result2 = subscribeOk.params.insertParam(
      Parameter(folly::to_underlying(TrackRequestParamKey::EXPIRES), 1000));

  // This should succeed
  EXPECT_TRUE(result2.hasValue());
  EXPECT_EQ(subscribeOk.params.size(), 1);
}

class UnknownParamTest : public ::testing::Test {
 protected:
  // Build a minimal SUBSCRIBE payload with one unknown parameter.
  // For v16+, the param key is delta-encoded; for v15-, absolute.
  folly::IOBufQueue buildSubscribeWithUnknownParam(uint64_t version) {
    folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
    size_t size = 0;
    bool error = false;

    // Request ID = 0
    writeVarint(writeBuf, 0, size, error);

    // Full Track Name: 1 namespace tuple with "ns", track name "track"
    // Namespace count
    writeVarint(writeBuf, 1, size, error);
    // Namespace entry "ns" - length-prefixed
    writeVarint(writeBuf, 2, size, error);
    writeBuf.append("ns", 2);
    size += 2;
    // Track name "t" - length-prefixed
    writeVarint(writeBuf, 1, size, error);
    writeBuf.append("t", 1);
    size += 1;

    // Number of params = 1 (the unknown param)
    writeVarint(writeBuf, 1, size, error);

    // Unknown even param key = 9998 (not in kParamAllowlist)
    uint64_t unknownKey = 9998;
    if (getDraftMajorVersion(version) >= 16) {
      // Delta from 0 = key itself
      writeVarint(writeBuf, unknownKey, size, error);
    } else {
      writeVarint(writeBuf, unknownKey, size, error);
    }
    // Even key -> int param: value varint
    writeVarint(writeBuf, 42, size, error);

    // Prepend frame header: frame type + 2-byte BE length
    folly::IOBufQueue headerBuf{folly::IOBufQueue::cacheChainLength()};
    size_t headerSize = 0;
    writeVarint(
        headerBuf,
        folly::to_underlying(FrameType::SUBSCRIBE),
        headerSize,
        error);
    // 2-byte big-endian length
    uint16_t sizeVal = folly::Endian::big(static_cast<uint16_t>(size));
    headerBuf.append(&sizeVal, 2);

    headerBuf.append(writeBuf.move());
    return headerBuf;
  }
};

TEST_F(UnknownParamTest, UnknownParamRejectedInV16) {
  auto buf = buildSubscribeWithUnknownParam(kVersionDraft16);
  auto data = buf.move();
  folly::io::Cursor cursor(data.get());

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft16);

  // Skip frame type
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));

  // Read length
  auto length = static_cast<size_t>(cursor.readBE<uint16_t>());

  auto result = parser.parseSubscribeRequest(cursor, length);
  EXPECT_FALSE(result.hasValue());
  EXPECT_EQ(result.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST_F(UnknownParamTest, UnknownParamAcceptedInV15) {
  auto buf = buildSubscribeWithUnknownParam(kVersionDraft15);
  auto data = buf.move();
  folly::io::Cursor cursor(data.get());

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft15);

  // Skip frame type
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));

  // Read length
  auto length = static_cast<size_t>(cursor.readBE<uint16_t>());

  auto result = parser.parseSubscribeRequest(cursor, length);
  EXPECT_TRUE(result.hasValue());
}

namespace {
folly::Expected<SubscribeRequest, ErrorCode> roundtripSubscribeWithRendezvous(
    uint64_t version,
    uint64_t timeoutMs) {
  MoQFrameWriter writer;
  writer.initializeVersion(version);
  MoQFrameParser parser;
  parser.initializeVersion(version);

  SubscribeRequest req;
  req.requestID = RequestID(1);
  req.fullTrackName = FullTrackName({TrackNamespace({"ns"}), "track"});
  req.locType = LocationType::LargestObject;
  // Insert key 0x04 directly so the test exercises the parser path that
  // distinguishes RENDEZVOUS_TIMEOUT from MAX_CACHE_DURATION by version.
  req.params.setMajorVersion(getDraftMajorVersion(version));
  CHECK(req.params
            .insertParam(Parameter(
                folly::to_underlying(TrackRequestParamKey::RENDEZVOUS_TIMEOUT),
                timeoutMs))
            .hasValue());

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto writeRes = writer.writeSubscribeRequest(writeBuf, req);
  if (!writeRes) {
    return folly::makeUnexpected(ErrorCode::INTERNAL_ERROR);
  }

  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  CHECK(frameType.has_value());
  CHECK_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
  auto bodyLen = static_cast<size_t>(cursor.readBE<uint16_t>());
  return parser.parseSubscribeRequest(cursor, bodyLen);
}
} // namespace

// v18 SUBSCRIBE with key 0x04 must surface as an integer param on the parsed
// SubscribeRequest so the relay can consume RENDEZVOUS_TIMEOUT.
TEST(RendezvousTimeoutParamTest, V18SubscribeExposesParam) {
  auto parsed = roundtripSubscribeWithRendezvous(kVersionDraft18, 1500);
  ASSERT_TRUE(parsed.hasValue());
  auto val = getFirstIntParam(
      parsed->params, TrackRequestParamKey::RENDEZVOUS_TIMEOUT);
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, 1500u);
}

// In drafts < 18, key 0x04 still means MAX_CACHE_DURATION. In v14 it is a
// param allowed on all frame types and must roundtrip unchanged.
TEST(RendezvousTimeoutParamTest, PreV18Key0x04PreservesMaxCacheDuration) {
  auto parsed = roundtripSubscribeWithRendezvous(kVersionDraft14, 4242);
  ASSERT_TRUE(parsed.hasValue());
  auto val = getFirstIntParam(
      parsed->params, TrackRequestParamKey::MAX_CACHE_DURATION);
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, 4242u);
}

// v18 non-SUBSCRIBE frames must not accept key 0x04 — insertParam rejects.
TEST(RendezvousTimeoutParamTest, V18NonSubscribeInsertRejected) {
  Parameters fetchParams(FrameType::FETCH);
  fetchParams.setMajorVersion(18);
  auto res = fetchParams.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::RENDEZVOUS_TIMEOUT), 1000));
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(fetchParams.size(), 0);
}

namespace {
// Hand-build a SUBSCRIBE wire frame carrying a single integer parameter
// (`key`, `value`). `key` MUST be even (parser treats odd keys as
// length-prefixed). Bypasses Parameters::insertParam validation so the test
// can produce frames whose params would be rejected at construction time.
folly::IOBufQueue
buildSubscribeWithIntParam(uint64_t version, uint64_t key, uint64_t value) {
  folly::IOBufQueue body{folly::IOBufQueue::cacheChainLength()};
  size_t size = 0;
  bool error = false;

  writeVarint(body, 0, size, error); // Request ID = 0
  writeVarint(body, 1, size, error); // Namespace count = 1
  writeVarint(body, 2, size, error); // "ns"
  body.append("ns", 2);
  size += 2;
  writeVarint(body, 1, size, error); // track name "t"
  body.append("t", 1);
  size += 1;

  writeVarint(body, 1, size, error); // numParams = 1
  // v16+ uses delta encoding from previous key (initially 0); first param's
  // delta equals the absolute key. Pre-v16 uses the absolute key directly.
  writeVarint(body, key, size, error);
  writeVarint(body, value, size, error);

  folly::IOBufQueue framed{folly::IOBufQueue::cacheChainLength()};
  size_t headerSize = 0;
  writeVarint(
      framed, folly::to_underlying(FrameType::SUBSCRIBE), headerSize, error);
  uint16_t sizeVal = folly::Endian::big(static_cast<uint16_t>(size));
  framed.append(&sizeVal, 2);
  framed.append(body.move());
  return framed;
}

folly::Expected<SubscribeRequest, ErrorCode> parseFramedSubscribe(
    uint64_t version,
    folly::IOBufQueue framed) {
  MoQFrameParser parser;
  parser.initializeVersion(version);
  auto serialized = framed.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  CHECK(frameType.has_value());
  CHECK_EQ(frameType->first, folly::to_underlying(FrameType::SUBSCRIBE));
  auto bodyLen = static_cast<size_t>(cursor.readBE<uint16_t>());
  return parser.parseSubscribeRequest(cursor, bodyLen);
}
} // namespace

// Draft 18 §10.2.1 (Parameter Scope): a known parameter that appears on a
// message type its definition does not list MUST cause PROTOCOL_VIOLATION.
// PUBLISHER_PRIORITY is extensions-only in v16+ (rejected by isParamAllowed
// for SUBSCRIBE) and is not request-specific, so it exercises the
// insertParam path that the new strict-scope check guards.
TEST(ParamScopeTest, V18SubscribeWithDisallowedKnownParamIsProtocolViolation) {
  auto framed = buildSubscribeWithIntParam(
      kVersionDraft18,
      folly::to_underlying(TrackRequestParamKey::PUBLISHER_PRIORITY),
      42);
  auto res = parseFramedSubscribe(kVersionDraft18, std::move(framed));
  ASSERT_FALSE(res.hasValue());
  EXPECT_EQ(res.error(), ErrorCode::PROTOCOL_VIOLATION);
}

// Pre-v18 drafts predate §10.2.1's strict requirement: receivers should
// silently ignore out-of-scope params for compatibility with peers that
// produced them. Verify v17 keeps that behavior so this commit doesn't
// regress older deployments.
TEST(ParamScopeTest, PreV18SubscribeWithDisallowedKnownParamSilentlyIgnored) {
  auto framed = buildSubscribeWithIntParam(
      kVersionDraft17,
      folly::to_underlying(TrackRequestParamKey::PUBLISHER_PRIORITY),
      42);
  auto res = parseFramedSubscribe(kVersionDraft17, std::move(framed));
  ASSERT_TRUE(res.hasValue());
  // Param was dropped during parse, not retained on SubscribeRequest.
  auto val =
      getFirstIntParam(res->params, TrackRequestParamKey::PUBLISHER_PRIORITY);
  EXPECT_FALSE(val.has_value());
}

// RENDEZVOUS_TIMEOUT is valid only on SUBSCRIBE in v18. The receive path
// for any other message type must also yield PROTOCOL_VIOLATION — this
// covers the wire-receive analogue of V18NonSubscribeInsertRejected, which
// only exercised the insertParam API.
TEST(ParamScopeTest, V18FetchWithRendezvousTimeoutIsProtocolViolation) {
  // Build a FETCH whose params contain RENDEZVOUS_TIMEOUT by going through
  // the writer with no majorVersion on the Parameters object (so
  // insertParam's v18-scope check is bypassed and the writer just serializes
  // the requested key/value pair). The receiver, initialized to v18, must
  // then reject the frame per §10.2.1.
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  Fetch fetch;
  fetch.requestID = RequestID(1);
  fetch.fullTrackName = FullTrackName({TrackNamespace({"ns"}), "track"});
  fetch.priority = kDefaultPriority;
  fetch.groupOrder = GroupOrder::OldestFirst;
  fetch.args = StandaloneFetch(AbsoluteLocation{0, 0}, AbsoluteLocation{1, 0});
  // Do NOT call setMajorVersion — leaves MAX_CACHE_DURATION's allow-all set
  // active so insertParam succeeds for FETCH.
  CHECK(fetch.params
            .insertParam(Parameter(
                folly::to_underlying(TrackRequestParamKey::RENDEZVOUS_TIMEOUT),
                1000))
            .hasValue());

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto writeRes = writer.writeFetch(writeBuf, fetch);
  ASSERT_TRUE(writeRes.hasValue());

  MoQFrameParser parser;
  parser.initializeVersion(kVersionDraft18);
  auto serialized = writeBuf.move();
  folly::io::Cursor cursor(serialized.get());
  auto frameType = quic::follyutils::decodeQuicInteger(cursor);
  ASSERT_TRUE(frameType.has_value());
  EXPECT_EQ(frameType->first, folly::to_underlying(FrameType::FETCH));
  auto bodyLen = static_cast<size_t>(cursor.readBE<uint16_t>());
  auto parsed = parser.parseFetch(cursor, bodyLen);
  ASSERT_FALSE(parsed.hasValue());
  EXPECT_EQ(parsed.error(), ErrorCode::PROTOCOL_VIOLATION);
}

// Tests for v16-specific track property param restrictions
TEST_F(ParametersIsParamAllowedTest, TrackPropertyParamRejectedInV16) {
  // MAX_CACHE_DURATION and PUBLISHER_PRIORITY are extensions-only in v16
  Parameters params(FrameType::SUBSCRIBE_OK);
  params.setMajorVersion(16);

  EXPECT_FALSE(params.isParamAllowed(TrackRequestParamKey::MAX_CACHE_DURATION));
  EXPECT_FALSE(params.isParamAllowed(TrackRequestParamKey::PUBLISHER_PRIORITY));
}

TEST_F(ParametersIsParamAllowedTest, TrackPropertyParamAcceptedInV15) {
  // Same params should be allowed in v15
  Parameters params(FrameType::SUBSCRIBE_OK);
  params.setMajorVersion(15);

  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::MAX_CACHE_DURATION));
  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::PUBLISHER_PRIORITY));
}

TEST_F(ParametersIsParamAllowedTest, DeliveryTimeoutAllowedInV16Subscribe) {
  Parameters params(FrameType::SUBSCRIBE);
  params.setMajorVersion(16);

  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::DELIVERY_TIMEOUT));
}

// In draft 18+, key 0x04 is RENDEZVOUS_TIMEOUT (only valid on SUBSCRIBE).
// In draft 16/17 it remains MAX_CACHE_DURATION which is extensions-only.
TEST_F(ParametersIsParamAllowedTest, RendezvousTimeoutAllowedInV18Subscribe) {
  Parameters subV18(FrameType::SUBSCRIBE);
  subV18.setMajorVersion(18);
  EXPECT_TRUE(subV18.isParamAllowed(TrackRequestParamKey::RENDEZVOUS_TIMEOUT));
}

TEST_F(
    ParametersIsParamAllowedTest,
    RendezvousTimeoutDisallowedInV18NonSubscribe) {
  for (auto frame :
       {FrameType::FETCH,
        FrameType::SUBSCRIBE_OK,
        FrameType::PUBLISH,
        FrameType::PUBLISH_OK}) {
    Parameters params(frame);
    params.setMajorVersion(18);
    EXPECT_FALSE(
        params.isParamAllowed(TrackRequestParamKey::RENDEZVOUS_TIMEOUT))
        << "frame=" << folly::to_underlying(frame);
  }
}

TEST_F(
    ParametersIsParamAllowedTest,
    MaxCacheDurationParamStillDisallowedInV17) {
  // In v16/v17 MAX_CACHE_DURATION is extensions-only — not a param.
  Parameters subV17(FrameType::SUBSCRIBE);
  subV17.setMajorVersion(17);
  EXPECT_FALSE(subV17.isParamAllowed(TrackRequestParamKey::MAX_CACHE_DURATION));
}

TEST_F(ParametersIsParamAllowedTest, IsRendezvousTimeoutParamHelper) {
  const auto key =
      folly::to_underlying(TrackRequestParamKey::RENDEZVOUS_TIMEOUT);
  EXPECT_TRUE(isRendezvousTimeoutParam(key, 18));
  EXPECT_TRUE(isRendezvousTimeoutParam(key, 20));
  EXPECT_FALSE(isRendezvousTimeoutParam(key, 17));
  EXPECT_FALSE(isRendezvousTimeoutParam(key, 15));
  // Other keys are never rendezvous timeout.
  EXPECT_FALSE(isRendezvousTimeoutParam(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 18));
}

TEST_F(ParametersIsParamAllowedTest, DeliveryTimeoutAllowedInV16PublishOk) {
  // DELIVERY_TIMEOUT is allowed in PUBLISH_OK for both v15 and v16
  Parameters params(FrameType::PUBLISH_OK);
  params.setMajorVersion(16);

  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::DELIVERY_TIMEOUT));
}

TEST_F(ParametersIsParamAllowedTest, GroupOrderStillAllowedInV16PublishOk) {
  Parameters params(FrameType::PUBLISH_OK);
  params.setMajorVersion(16);

  EXPECT_TRUE(params.isParamAllowed(TrackRequestParamKey::GROUP_ORDER));
}

// Verify that a parse underflow doesn't corrupt delta-encoded object ID
// state.
TEST_P(MoQFramerTest, SubgroupObjectUnderflowDoesNotCorruptDeltaState) {
  if (getDraftMajorVersion(GetParam()) < 14) {
    return;
  }

  auto streamType =
      getSubgroupStreamType(GetParam(), SubgroupIDFormat::Zero, false, false);
  auto sgOptions = getSubgroupOptions(GetParam(), streamType);
  ObjectHeader obj(1, 0, 0, 128, ObjectStatus::NORMAL, noExtensions(), 4);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  writer_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), obj, SubgroupIDFormat::Zero, false);
  writer_.writeStreamObject(
      writeBuf, streamType, obj, folly::IOBuf::copyBuffer("AAAA"));
  obj.id = 1;
  writer_.writeStreamObject(
      writeBuf, streamType, obj, folly::IOBuf::copyBuffer("BBBB"));
  auto serialized = writeBuf.move();

  folly::io::Cursor cursor(serialized.get());
  parseStreamType(cursor);
  auto hdr =
      parser_.parseSubgroupHeader(cursor, cursor.totalLength(), sgOptions);
  ASSERT_TRUE(hdr.hasValue());

  // Parse object 0 successfully
  auto r0 = parser_.parseSubgroupObjectHeader(
      cursor, cursor.totalLength(), hdr->value.objectHeader, sgOptions);
  ASSERT_TRUE(r0.hasValue());
  EXPECT_EQ(r0->value.id, 0);
  cursor.skip(*r0->value.length);

  // Trigger underflow on object 1 by passing only 1 byte (ID varint only)
  auto obj1Offset = cursor.getCurrentPosition();
  {
    folly::io::Cursor truncCursor(serialized.get());
    truncCursor.skip(obj1Offset);
    auto uf = parser_.parseSubgroupObjectHeader(
        truncCursor, 1, hdr->value.objectHeader, sgOptions);
    EXPECT_EQ(uf.error(), ErrorCode::PARSE_UNDERFLOW);
  }

  // Re-parse object 1 with full data — ID must still be 1, not 2
  auto r1 = parser_.parseSubgroupObjectHeader(
      cursor, cursor.totalLength(), hdr->value.objectHeader, sgOptions);
  ASSERT_TRUE(r1.hasValue());
  EXPECT_EQ(r1->value.id, 1);
}
