/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQCodec.h"
#include "moxygen/test/Mocks.h"
#include "moxygen/test/TestUtils.h"

#include <folly/portability/GTest.h>

namespace {
moxygen::FrameType getErrorFrameType(
    uint64_t version,
    moxygen::FrameType frameType) {
  return (moxygen::getDraftMajorVersion(version) < 15)
      ? frameType
      : moxygen::FrameType::REQUEST_ERROR;
}

void expectOnRequestError(
    testing::NiceMock<moxygen::MockMoQCodecCallback>& callback,
    uint64_t version,
    moxygen::FrameType frameType) {
  EXPECT_CALL(
      callback,
      onRequestError(testing::_, getErrorFrameType(version, frameType)))
      .RetiresOnSaturation();
}
void expectOnRequestOk(
    testing::NiceMock<moxygen::MockMoQCodecCallback>& callback,
    uint64_t version,
    moxygen::FrameType frameType) {
  auto expectedFrameType = (moxygen::getDraftMajorVersion(version) < 15)
      ? frameType
      : moxygen::FrameType::REQUEST_OK;
  EXPECT_CALL(callback, onRequestOk(testing::_, expectedFrameType))
      .RetiresOnSaturation();
}

void expectOnTrackStatusOk(
    testing::NiceMock<moxygen::MockMoQCodecCallback>& callback,
    uint64_t version) {
  if (moxygen::getDraftMajorVersion(version) < 15) {
    EXPECT_CALL(callback, onTrackStatusOk(testing::_)).RetiresOnSaturation();
  } else {
    EXPECT_CALL(
        callback, onRequestOk(testing::_, moxygen::FrameType::REQUEST_OK))
        .RetiresOnSaturation();
  }
}

} // namespace

namespace moxygen::test {
using testing::_;

// The parameter is the MoQ version
class MoQCodecTest : public ::testing::TestWithParam<uint64_t> {
 public:
  void SetUp() override {
    moqFrameWriter_.initializeVersion(GetParam());
    objectStreamCodec_.initializeVersion(GetParam());
    serverControlCodec_.initializeVersion(GetParam());
    clientControlCodec_.initializeVersion(GetParam());
  }

  void testAll(MoQControlCodec::Direction dir) {
    auto allMsgs = moxygen::test::writeAllControlMessages(
        fromDir(dir), moqFrameWriter_, GetParam());
    testing::NiceMock<MockMoQCodecCallback>* callbackPtr = nullptr;
    MoQControlCodec* codecPtr = nullptr;

    if (dir == MoQControlCodec::Direction::SERVER) {
      codecPtr = &serverControlCodec_;
      callbackPtr = &serverControlCodecCallback_;
      EXPECT_CALL(*callbackPtr, onClientSetup(testing::_));
    } else {
      codecPtr = &clientControlCodec_;
      callbackPtr = &clientControlCodecCallback_;
      EXPECT_CALL(*callbackPtr, onServerSetup(testing::_));
    }
    MoQControlCodec& codec = *codecPtr;
    auto& callback = *callbackPtr;
    EXPECT_CALL(callback, onSubscribe(testing::_));
    EXPECT_CALL(callback, onRequestUpdate(testing::_));
    EXPECT_CALL(callback, onSubscribeOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::SUBSCRIBE_ERROR);
    EXPECT_CALL(callback, onUnsubscribe(testing::_));
    EXPECT_CALL(callback, onPublishDone(testing::_));
    EXPECT_CALL(callback, onPublish(testing::_));
    EXPECT_CALL(callback, onPublishOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::PUBLISH_ERROR);
    EXPECT_CALL(callback, onPublishNamespace(testing::_));
    expectOnRequestOk(callback, GetParam(), FrameType::PUBLISH_NAMESPACE_OK);
    expectOnRequestError(
        callback, GetParam(), FrameType::PUBLISH_NAMESPACE_ERROR);
    EXPECT_CALL(callback, onPublishNamespaceDone(testing::_));
    EXPECT_CALL(callback, onTrackStatus(testing::_));
    expectOnTrackStatusOk(callback, GetParam());
    EXPECT_CALL(callback, onGoaway(testing::_));
    EXPECT_CALL(callback, onMaxRequestID(testing::_));
    // SubscribeNamespace messages are not on the control stream for draft 16+
    if (getDraftMajorVersion(GetParam()) < 16) {
      EXPECT_CALL(callback, onSubscribeNamespace(testing::_));
      expectOnRequestOk(
          callback, GetParam(), FrameType::SUBSCRIBE_NAMESPACE_OK);
      expectOnRequestError(
          callback, GetParam(), FrameType::SUBSCRIBE_NAMESPACE_ERROR);
      EXPECT_CALL(callback, onUnsubscribeNamespace(testing::_));
    }
    EXPECT_CALL(callback, onFetch(testing::_));
    EXPECT_CALL(callback, onFetchCancel(testing::_));
    EXPECT_CALL(callback, onFetchOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::FETCH_ERROR);

    // 28 frames for draft < 16, 24 for draft 16+ (no subscribe namespace msgs)
    int expectedFrames = (getDraftMajorVersion(GetParam()) < 16) ? 28 : 24;
    EXPECT_CALL(callback, onFrame(testing::_)).Times(expectedFrames);

    codec.onIngress(std::move(allMsgs), true);
  }

  void testUnderflow(MoQControlCodec::Direction dir) {
    auto allMsgs = moxygen::test::writeAllControlMessages(
        fromDir(dir), moqFrameWriter_, GetParam());
    testing::NiceMock<MockMoQCodecCallback>* callbackPtr = nullptr;
    MoQControlCodec* codecPtr = nullptr;

    folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
    readBuf.append(std::move(allMsgs));

    if (dir == MoQControlCodec::Direction::SERVER) {
      codecPtr = &serverControlCodec_;
      callbackPtr = &serverControlCodecCallback_;
      EXPECT_CALL(*callbackPtr, onClientSetup(testing::_));
    } else {
      codecPtr = &clientControlCodec_;
      callbackPtr = &clientControlCodecCallback_;
      EXPECT_CALL(*callbackPtr, onServerSetup(testing::_));
    }
    MoQControlCodec& codec = *codecPtr;
    auto& callback = *callbackPtr;
    EXPECT_CALL(callback, onSubscribe(testing::_));
    EXPECT_CALL(callback, onRequestUpdate(testing::_));
    EXPECT_CALL(callback, onSubscribeOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::SUBSCRIBE_ERROR);
    EXPECT_CALL(callback, onUnsubscribe(testing::_));
    EXPECT_CALL(callback, onPublishDone(testing::_));
    EXPECT_CALL(callback, onPublish(testing::_));
    EXPECT_CALL(callback, onPublishOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::PUBLISH_ERROR);
    EXPECT_CALL(callback, onPublishNamespace(testing::_));
    expectOnRequestOk(callback, GetParam(), FrameType::PUBLISH_NAMESPACE_OK);

    expectOnRequestError(
        callback, GetParam(), FrameType::PUBLISH_NAMESPACE_ERROR);
    EXPECT_CALL(callback, onPublishNamespaceDone(testing::_));
    EXPECT_CALL(callback, onTrackStatus(testing::_));
    expectOnTrackStatusOk(callback, GetParam());
    EXPECT_CALL(callback, onGoaway(testing::_));
    EXPECT_CALL(callback, onMaxRequestID(testing::_));
    // SubscribeNamespace messages are not on the control stream for draft 16+
    if (getDraftMajorVersion(GetParam()) < 16) {
      EXPECT_CALL(callback, onSubscribeNamespace(testing::_));
      expectOnRequestOk(
          callback, GetParam(), FrameType::SUBSCRIBE_NAMESPACE_OK);
      expectOnRequestError(
          callback, GetParam(), FrameType::SUBSCRIBE_NAMESPACE_ERROR);
      EXPECT_CALL(callback, onUnsubscribeNamespace(testing::_));
    }
    EXPECT_CALL(callback, onFetch(testing::_));
    EXPECT_CALL(callback, onFetchCancel(testing::_));
    EXPECT_CALL(callback, onFetchOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::FETCH_ERROR);
    // 28 frames for draft < 16, 24 for draft 16+ (no subscribe namespace msgs)
    int expectedFrames = (getDraftMajorVersion(GetParam()) < 16) ? 28 : 24;
    EXPECT_CALL(callback, onFrame(testing::_)).Times(expectedFrames);
    while (!readBuf.empty()) {
      codec.onIngress(readBuf.split(1), false);
    }
    codec.onIngress(nullptr, true);
  }

  TestControlMessages fromDir(MoQControlCodec::Direction dir) {
    // The control messages to write are the opposite of dir
    return dir == MoQControlCodec::Direction::CLIENT
        ? TestControlMessages::SERVER
        : TestControlMessages::CLIENT;
  }

 protected:
  MoQFrameWriter moqFrameWriter_;
  testing::NiceMock<MockMoQCodecCallback> objectStreamCodecCallback_;
  MoQObjectStreamCodec objectStreamCodec_{&objectStreamCodecCallback_};

  testing::NiceMock<MockMoQCodecCallback> serverControlCodecCallback_;
  MoQControlCodec serverControlCodec_{
      MoQControlCodec::Direction::SERVER,
      &serverControlCodecCallback_};

  testing::NiceMock<MockMoQCodecCallback> clientControlCodecCallback_;
  MoQControlCodec clientControlCodec_{
      MoQControlCodec::Direction::CLIENT,
      &clientControlCodecCallback_};
};

TEST_P(MoQCodecTest, All) {
  testAll(MoQControlCodec::Direction::CLIENT);
  testAll(MoQControlCodec::Direction::SERVER);
}

TEST_P(MoQCodecTest, AllObject) {
  auto allMsgs = moxygen::test::writeAllObjectMessages(moqFrameWriter_);

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(testing::_, testing::_, testing::_, testing::_, testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          true,
          false,
          testing::_))
      .Times(2);
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectStatus(
          testing::_, testing::_, testing::_, testing::_, testing::_))
      .Times(2);
  objectStreamCodec_.onIngress(std::move(allMsgs), true);
}

TEST_P(MoQCodecTest, Underflow) {
  testUnderflow(MoQControlCodec::Direction::CLIENT);
  testUnderflow(MoQControlCodec::Direction::SERVER);
}

TEST_P(MoQCodecTest, UnderflowObjects) {
  auto allMsgs = moxygen::test::writeAllObjectMessages(moqFrameWriter_);

  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  readBuf.append(std::move(allMsgs));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(testing::_, testing::_, testing::_, testing::_, testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .Times(2);
  EXPECT_CALL(
      objectStreamCodecCallback_, onObjectPayload(testing::_, testing::_))
      .Times(strlen("hello world") + strlen("hello world ext"));
  while (!readBuf.empty()) {
    objectStreamCodec_.onIngress(readBuf.split(1), false);
  }
  objectStreamCodec_.onIngress(nullptr, true);
}

TEST_P(MoQCodecTest, ObjectStreamPayloadFin) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeSingleObjectStream(
      writeBuf,
      TrackAlias(1),
      ObjectHeader(2, 3, 4, 5, 11),
      folly::IOBuf::copyBuffer("hello world"));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2, 3, 4, testing::_, testing::_, testing::_, true, true, testing::_));

  objectStreamCodec_.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, ObjectStreamPayload) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeSingleObjectStream(
      writeBuf,
      TrackAlias(1),
      ObjectHeader(2, 3, 4, 5, 11),
      folly::IOBuf::copyBuffer("hello world"));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2, 3, 4, testing::_, testing::_, _, true, false, testing::_));

  objectStreamCodec_.onIngress(writeBuf.move(), false);
  EXPECT_CALL(objectStreamCodecCallback_, onEndOfStream());
  objectStreamCodec_.onIngress(std::unique_ptr<folly::IOBuf>(), true);
}

TEST_P(MoQCodecTest, EmptyObjectPayload) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeSingleObjectStream(
      writeBuf,
      TrackAlias(1),
      ObjectHeader(2, 3, 4, 5, ObjectStatus::OBJECT_NOT_EXIST),
      nullptr);

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectStatus(
          2, 3, 4, std::optional<uint8_t>(5), ObjectStatus::OBJECT_NOT_EXIST));
  EXPECT_CALL(objectStreamCodecCallback_, onEndOfStream());
  // extra coverage of underflow in header
  objectStreamCodec_.onIngress(writeBuf.split(3), false);
  objectStreamCodec_.onIngress(writeBuf.move(), false);
  objectStreamCodec_.onIngress(std::unique_ptr<folly::IOBuf>(), true);
}

TEST_P(MoQCodecTest, TruncatedObject) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), ObjectHeader(2, 3, 4, 5));
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG,
      ObjectHeader(2, 3, 4, 5, 11),
      folly::IOBuf::copyBuffer("hello")); // missing " world"

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(testing::_, testing::_, testing::_, testing::_, testing::_));
  EXPECT_CALL(objectStreamCodecCallback_, onConnectionError(testing::_));

  objectStreamCodec_.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, TruncatedObjectPayload) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), ObjectHeader(2, 3, 4, 5));
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      ObjectHeader(2, 3, 4, 5, 11),
      nullptr);

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(testing::_, testing::_, testing::_, testing::_, testing::_));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2, 3, 4, testing::_, testing::_, _, false, false, testing::_));
  objectStreamCodec_.onIngress(writeBuf.move(), false);
  EXPECT_CALL(objectStreamCodecCallback_, onConnectionError(testing::_));
  writeBuf.append(std::string("hello"));
  objectStreamCodec_.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, StreamTypeUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  uint8_t big = 0xff;
  writeBuf.append(&big, 1);

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onConnectionError(ErrorCode::PARSE_UNDERFLOW));
  objectStreamCodec_.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, UnknownStreamType) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  uint8_t bad = 0x22;
  writeBuf.append(&bad, 1);

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onConnectionError(ErrorCode::PROTOCOL_VIOLATION));
  objectStreamCodec_.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, Fetch) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  RequestID requestID(1);
  ObjectHeader obj(
      2, 3, 4, 5); // Fetch context - TrackAlias(0) passed separately
  StreamType streamType = StreamType::FETCH_HEADER;
  auto res = moqFrameWriter_.writeFetchHeader(writeBuf, requestID);
  obj.length = 5;
  res = moqFrameWriter_.writeStreamObject(
      writeBuf, streamType, obj, folly::IOBuf::copyBuffer("hello"));
  obj.group++;
  obj.id = 0;
  obj.status = ObjectStatus::END_OF_TRACK;
  obj.length = 0;
  res = moqFrameWriter_.writeStreamObject(writeBuf, streamType, obj, nullptr);
  obj.id++;
  obj.status = ObjectStatus::GROUP_NOT_EXIST;
  obj.length = 0;
  res = moqFrameWriter_.writeStreamObject(writeBuf, streamType, obj, nullptr);

  EXPECT_CALL(objectStreamCodecCallback_, onFetchHeader(testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(2, 3, 4, testing::_, 5, _, true, false, testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectStatus(
          3, 3, 0, std::optional<uint8_t>(5), ObjectStatus::END_OF_TRACK));
  // object after terminal status
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onConnectionError(ErrorCode::PROTOCOL_VIOLATION));
  objectStreamCodec_.onIngress(writeBuf.move(), false);
}

TEST_P(MoQCodecTest, FetchHeaderUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  RequestID requestID(0xffffffffffffff);
  moqFrameWriter_.writeFetchHeader(writeBuf, requestID);
  // only deliver first byte of fetch header
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onConnectionError(ErrorCode::PARSE_UNDERFLOW));
  objectStreamCodec_.onIngress(writeBuf.splitAtMost(2), true);
}

TEST_P(MoQCodecTest, InvalidFrame) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  writeBuf.append(std::string("\x23"));
  EXPECT_CALL(clientControlCodecCallback_, onConnectionError(testing::_));
  clientControlCodec_.onIngress(writeBuf.move(), false);
}

TEST_P(MoQCodecTest, ClientGetsClientSetup) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // client gets client setup
  auto setupFrame = [version = GetParam()]() {
    ClientSetup setup;
    setup.supportedVersions = {version};
    setup.params.insertParam(
        Parameter(folly::to_underlying(SetupKey::PATH), "/foo"));
    setup.params.insertParam(
        Parameter(folly::to_underlying(SetupKey::MAX_REQUEST_ID), 100));
    return setup;
  }();
  writeClientSetup(writeBuf, setupFrame, GetParam());

  EXPECT_CALL(clientControlCodecCallback_, onConnectionError(testing::_));
  clientControlCodec_.onIngress(writeBuf.move(), false);
}

TEST_P(MoQCodecTest, CodecGetsNonSetupFirst) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // codec gets non-setup first
  moqFrameWriter_.writeUnsubscribe(
      writeBuf,
      Unsubscribe({
          0,
      }));

  EXPECT_CALL(clientControlCodecCallback_, onConnectionError(testing::_));
  clientControlCodec_.onIngress(writeBuf.move(), false);
}

TEST_P(MoQCodecTest, TwoSetups) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  auto setupFrame = [version = GetParam()]() {
    ServerSetup setup{.selectedVersion = version};
    setup.params.insertParam(
        Parameter(folly::to_underlying(SetupKey::PATH), "/foo"));
    return setup;
  }();
  writeServerSetup(writeBuf, setupFrame, GetParam());
  auto serverSetup = writeBuf.front()->clone();
  // This is legal, to setup next test
  EXPECT_CALL(clientControlCodecCallback_, onServerSetup(testing::_));
  clientControlCodec_.onIngress(writeBuf.move(), false);
  // Second setup = error
  EXPECT_CALL(clientControlCodecCallback_, onConnectionError(testing::_));
  clientControlCodec_.onIngress(serverSetup->clone(), false);
}

TEST_P(MoQCodecTest, ServerGetsServerSetup) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  auto setupFrame = [version = GetParam()]() {
    ServerSetup setup{.selectedVersion = version};
    setup.params.insertParam(
        Parameter(folly::to_underlying(SetupKey::PATH), "/foo"));
    return setup;
  }();
  writeServerSetup(writeBuf, setupFrame, GetParam());
  auto serverSetup = writeBuf.front()->clone();
  // Server gets server setup = error
  EXPECT_CALL(serverControlCodecCallback_, onConnectionError(testing::_));
  serverControlCodec_.onIngress(serverSetup->clone(), false);
}

// Test for codec fix: stream with only subgroup header and EOF
// This tests the fix where a stream with only a STREAM_HEADER_SG_EXT and EOF
// should only call onSubgroup and onEndOfStream, NOT fall through to
// MULTI_OBJECT_HEADER. Before the fix, there was a [[fallthrough]] that would
// incorrectly try to parse a multi-object header.
TEST_P(MoQCodecTest, SubgroupHeaderWithEOF) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // Write a subgroup header with extensions
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), ObjectHeader(2, 3, 4, 5));
  EXPECT_TRUE(res);

  // Expect only onSubgroup and onEndOfStream
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_));
  EXPECT_CALL(objectStreamCodecCallback_, onEndOfStream());

  // Deliver with FIN=true and no additional data
  objectStreamCodec_.onIngress(writeBuf.move(), true);
}

// Test that when onObjectBegin returns ERROR_TERMINATE, the codec
// short-circuits and returns immediately without processing more data
TEST_P(MoQCodecTest, CallbackReturnsErrorTerminateOnObjectBegin) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), ObjectHeader(2, 3, 4, 5));
  // First object - will trigger ERROR_TERMINATE
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      ObjectHeader(2, 3, 4, 5, 11),
      folly::IOBuf::copyBuffer("hello world"));
  // Second object that should NOT be parsed after error
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      ObjectHeader(2, 3, 5, 5, 5),
      folly::IOBuf::copyBuffer("after"));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2, 3, 4, testing::_, testing::_, testing::_, true, false, testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::ERROR_TERMINATE));

  // onObjectBegin for second object should NOT be called due to short-circuit
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2,
          3,
          5,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .Times(0);

  auto result = objectStreamCodec_.onIngress(writeBuf.move(), true);
  EXPECT_EQ(result, MoQCodec::ParseResult::ERROR_TERMINATE);
}

// Test that when onObjectPayload returns ERROR_TERMINATE, parsing stops
TEST_P(MoQCodecTest, CallbackReturnsErrorTerminateOnObjectPayload) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), ObjectHeader(2, 3, 4, 5));
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      ObjectHeader(2, 3, 4, 5, 20),
      nullptr);

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2,
          3,
          4,
          testing::_,
          testing::_,
          testing::_,
          false,
          false,
          testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));

  objectStreamCodec_.onIngress(writeBuf.move(), false);

  // Now send payload - return ERROR_TERMINATE
  folly::IOBufQueue payloadBuf{folly::IOBufQueue::cacheChainLength()};
  payloadBuf.append(folly::IOBuf::copyBuffer("hello"));

  EXPECT_CALL(objectStreamCodecCallback_, onObjectPayload(testing::_, false))
      .WillOnce(testing::Return(MoQCodec::ParseResult::ERROR_TERMINATE));

  auto result = objectStreamCodec_.onIngress(payloadBuf.move(), false);
  EXPECT_EQ(result, MoQCodec::ParseResult::ERROR_TERMINATE);
}

// Test that when onObjectStatus returns ERROR_TERMINATE, parsing stops
TEST_P(MoQCodecTest, CallbackReturnsErrorTerminateOnObjectStatus) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), ObjectHeader(2, 3, 4, 5));
  // First object with status - will trigger ERROR_TERMINATE
  ObjectHeader statusObj(2, 3, 4, 5);
  statusObj.status = ObjectStatus::OBJECT_NOT_EXIST;
  statusObj.length = 0;
  res = moqFrameWriter_.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER_SG_EXT, statusObj, nullptr);
  // Second object that should NOT be parsed
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      ObjectHeader(2, 3, 5, 5, 5),
      folly::IOBuf::copyBuffer("after"));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectStatus(
          2, 3, 4, std::optional<uint8_t>(5), ObjectStatus::OBJECT_NOT_EXIST))
      .WillOnce(testing::Return(MoQCodec::ParseResult::ERROR_TERMINATE));

  // Second object should NOT be parsed
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2,
          3,
          5,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .Times(0);

  auto result = objectStreamCodec_.onIngress(writeBuf.move(), true);
  EXPECT_EQ(result, MoQCodec::ParseResult::ERROR_TERMINATE);
}

// Test that when onSubgroup returns ERROR_TERMINATE, parsing stops
TEST_P(MoQCodecTest, CallbackReturnsErrorTerminateOnSubgroup) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, TrackAlias(1), ObjectHeader(2, 3, 4, 5));
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG,
      ObjectHeader(2, 3, 4, 5, 5),
      folly::IOBuf::copyBuffer("hello"));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::ERROR_TERMINATE));

  // onObjectBegin should NOT be called
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .Times(0);

  auto result = objectStreamCodec_.onIngress(writeBuf.move(), true);
  EXPECT_EQ(result, MoQCodec::ParseResult::ERROR_TERMINATE);
}

// Test that when onFetchHeader returns ERROR_TERMINATE, parsing stops
TEST_P(MoQCodecTest, CallbackReturnsErrorTerminateOnFetchHeader) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  RequestID requestID(1);
  ObjectHeader obj(2, 3, 4, 5);
  StreamType streamType = StreamType::FETCH_HEADER;
  auto res = moqFrameWriter_.writeFetchHeader(writeBuf, requestID);
  obj.length = 5;
  res = moqFrameWriter_.writeStreamObject(
      writeBuf, streamType, obj, folly::IOBuf::copyBuffer("hello"));

  EXPECT_CALL(objectStreamCodecCallback_, onFetchHeader(testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::ERROR_TERMINATE));

  // onObjectBegin should NOT be called
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .Times(0);

  auto result = objectStreamCodec_.onIngress(writeBuf.move(), false);
  EXPECT_EQ(result, MoQCodec::ParseResult::ERROR_TERMINATE);
}

// Test that callbacks returning CONTINUE work as expected
TEST_P(MoQCodecTest, CallbackReturnsContinue) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeSingleObjectStream(
      writeBuf,
      TrackAlias(1),
      ObjectHeader(2, 3, 4, 5, 11),
      folly::IOBuf::copyBuffer("hello world"));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2, 3, 4, testing::_, testing::_, testing::_, true, false, testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));

  auto result = objectStreamCodec_.onIngress(writeBuf.move(), false);
  EXPECT_EQ(result, MoQCodec::ParseResult::CONTINUE);

  EXPECT_CALL(objectStreamCodecCallback_, onEndOfStream());
  result = objectStreamCodec_.onIngress(std::unique_ptr<folly::IOBuf>(), true);
  EXPECT_EQ(result, MoQCodec::ParseResult::CONTINUE);
}

// Test zero-length status object followed by normal object
// This exposes a cursor invalidation bug where trimStart() invalidates the
// cursor, but when chunkLen == 0, splitAndResetCursor() isn't called,
// leaving the cursor stale for the next object parse.
TEST_P(MoQCodecTest, ZeroLengthObjectFollowedByNormalObject) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf,
      TrackAlias(1),
      ObjectHeader(2, 3, 0, 5),
      SubgroupIDFormat::Present,
      false);

  // Write a zero-length status object (valid zero-length case)
  ObjectHeader statusObj(0, 0, 4, 0);
  statusObj.status = ObjectStatus::OBJECT_NOT_EXIST;
  statusObj.length = 0;
  res = moqFrameWriter_.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER_SG, statusObj, nullptr);

  // Write another object with non-zero length
  ObjectHeader normalObj(0, 0, 5, 0, 5);
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG,
      normalObj,
      folly::IOBuf::copyBuffer("hello"));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onSubgroup(TrackAlias(1), 2, 3, std::optional<uint8_t>(5), testing::_));

  // Expect onObjectStatus for the zero-length status object
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectStatus(
          2, 3, 4, std::optional<uint8_t>(5), ObjectStatus::OBJECT_NOT_EXIST))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));

  // Expect onObjectBegin for the normal object (this would crash without the
  // fix)
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(
          2, 3, 5, testing::_, 5, testing::_, true, false, testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));

  auto result = objectStreamCodec_.onIngress(writeBuf.move(), false);
  EXPECT_EQ(result, MoQCodec::ParseResult::CONTINUE);
}

// Test that onEndOfRange callback is invoked for End of Unknown Range (0x10C)
TEST_P(MoQCodecTest, FetchEndOfUnknownRange) {
  // End of Range markers require varint encoding, only supported in v16+
  if (getDraftMajorVersion(GetParam()) < 16) {
    GTEST_SKIP() << "End of Range not supported in draft < 16";
  }
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  RequestID requestID(1);
  ObjectHeader obj(2, 3, 4, 5);
  StreamType streamType = StreamType::FETCH_HEADER;

  auto res = moqFrameWriter_.writeFetchHeader(writeBuf, requestID);
  EXPECT_TRUE(res);
  obj.length = 5;
  res = moqFrameWriter_.writeStreamObject(
      writeBuf, streamType, obj, folly::IOBuf::copyBuffer("hello"));
  EXPECT_TRUE(res);

  // Write End of Unknown Range marker (0x10C) + Group ID (5) + Object ID (10)
  // 0x10C as 2-byte varint: 0x41 0x0C
  folly::IOBufQueue endOfRangeBuf{folly::IOBufQueue::cacheChainLength()};
  endOfRangeBuf.append(folly::IOBuf::copyBuffer("\x41\x0C")); // 0x10C varint
  endOfRangeBuf.append(folly::IOBuf::copyBuffer("\x05"));     // Group 5
  endOfRangeBuf.append(folly::IOBuf::copyBuffer("\x0A"));     // Object 10
  writeBuf.append(endOfRangeBuf.move());

  EXPECT_CALL(objectStreamCodecCallback_, onFetchHeader(testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(2, 3, 4, testing::_, 5, testing::_, true, false, false))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));
  EXPECT_CALL(objectStreamCodecCallback_, onEndOfRange(5, 10, true))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));

  auto result = objectStreamCodec_.onIngress(writeBuf.move(), false);
  EXPECT_EQ(result, MoQCodec::ParseResult::CONTINUE);
}

// Test that onEndOfRange callback is invoked for End of Non-Existent Range
// (0x8C)
TEST_P(MoQCodecTest, FetchEndOfNonExistentRange) {
  // End of Range markers require varint encoding, only supported in v16+
  if (getDraftMajorVersion(GetParam()) < 16) {
    GTEST_SKIP() << "End of Range not supported in draft < 16";
  }
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  RequestID requestID(1);
  ObjectHeader obj(2, 3, 4, 5);
  StreamType streamType = StreamType::FETCH_HEADER;

  auto res = moqFrameWriter_.writeFetchHeader(writeBuf, requestID);
  EXPECT_TRUE(res);
  obj.length = 5;
  res = moqFrameWriter_.writeStreamObject(
      writeBuf, streamType, obj, folly::IOBuf::copyBuffer("hello"));
  EXPECT_TRUE(res);

  // Write End of Non-Existent Range marker (0x8C) + Group ID (3) + Object ID
  // (7) 0x8C (140) as 2-byte varint: 0x40 0x8C
  folly::IOBufQueue endOfRangeBuf{folly::IOBufQueue::cacheChainLength()};
  endOfRangeBuf.append(folly::IOBuf::copyBuffer("\x40\x8C")); // 0x8C varint
  endOfRangeBuf.append(folly::IOBuf::copyBuffer("\x03"));     // Group 3
  endOfRangeBuf.append(folly::IOBuf::copyBuffer("\x07"));     // Object 7
  writeBuf.append(endOfRangeBuf.move());

  EXPECT_CALL(objectStreamCodecCallback_, onFetchHeader(testing::_))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(2, 3, 4, testing::_, 5, testing::_, true, false, false))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));
  EXPECT_CALL(objectStreamCodecCallback_, onEndOfRange(3, 7, false))
      .WillOnce(testing::Return(MoQCodec::ParseResult::CONTINUE));

  auto result = objectStreamCodec_.onIngress(writeBuf.move(), false);
  EXPECT_EQ(result, MoQCodec::ParseResult::CONTINUE);
}

INSTANTIATE_TEST_SUITE_P(
    MoQCodecTest,
    MoQCodecTest,
    ::testing::ValuesIn(kSupportedVersions));
} // namespace moxygen::test
