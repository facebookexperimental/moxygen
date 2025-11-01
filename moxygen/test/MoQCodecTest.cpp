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
    EXPECT_CALL(callback, onSubscribeUpdate(testing::_));
    EXPECT_CALL(callback, onSubscribeOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::SUBSCRIBE_ERROR);
    EXPECT_CALL(callback, onUnsubscribe(testing::_));
    EXPECT_CALL(callback, onSubscribeDone(testing::_));
    EXPECT_CALL(callback, onPublish(testing::_));
    EXPECT_CALL(callback, onPublishOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::PUBLISH_ERROR);
    EXPECT_CALL(callback, onAnnounce(testing::_));
    expectOnRequestOk(callback, GetParam(), FrameType::ANNOUNCE_OK);
    expectOnRequestError(callback, GetParam(), FrameType::ANNOUNCE_ERROR);
    EXPECT_CALL(callback, onUnannounce(testing::_));
    EXPECT_CALL(callback, onTrackStatus(testing::_));
    EXPECT_CALL(callback, onTrackStatusOk(testing::_));
    EXPECT_CALL(callback, onGoaway(testing::_));
    EXPECT_CALL(callback, onMaxRequestID(testing::_));
    EXPECT_CALL(callback, onSubscribeAnnounces(testing::_));
    expectOnRequestOk(callback, GetParam(), FrameType::SUBSCRIBE_ANNOUNCES_OK);
    expectOnRequestError(
        callback, GetParam(), FrameType::SUBSCRIBE_ANNOUNCES_ERROR);
    EXPECT_CALL(callback, onUnsubscribeAnnounces(testing::_));
    EXPECT_CALL(callback, onFetch(testing::_));
    EXPECT_CALL(callback, onFetchCancel(testing::_));
    EXPECT_CALL(callback, onFetchOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::FETCH_ERROR);

    EXPECT_CALL(callback, onFrame(testing::_)).Times(28);

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
    EXPECT_CALL(callback, onSubscribeUpdate(testing::_));
    EXPECT_CALL(callback, onSubscribeOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::SUBSCRIBE_ERROR);
    EXPECT_CALL(callback, onUnsubscribe(testing::_));
    EXPECT_CALL(callback, onSubscribeDone(testing::_));
    EXPECT_CALL(callback, onPublish(testing::_));
    EXPECT_CALL(callback, onPublishOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::PUBLISH_ERROR);
    EXPECT_CALL(callback, onAnnounce(testing::_));
    expectOnRequestOk(callback, GetParam(), FrameType::ANNOUNCE_OK);

    expectOnRequestError(callback, GetParam(), FrameType::ANNOUNCE_ERROR);
    EXPECT_CALL(callback, onUnannounce(testing::_));
    EXPECT_CALL(callback, onTrackStatus(testing::_));
    EXPECT_CALL(callback, onTrackStatusOk(testing::_));
    EXPECT_CALL(callback, onGoaway(testing::_));
    EXPECT_CALL(callback, onMaxRequestID(testing::_));
    EXPECT_CALL(callback, onSubscribeAnnounces(testing::_));
    expectOnRequestOk(callback, GetParam(), FrameType::SUBSCRIBE_ANNOUNCES_OK);

    expectOnRequestError(
        callback, GetParam(), FrameType::SUBSCRIBE_ANNOUNCES_ERROR);
    EXPECT_CALL(callback, onUnsubscribeAnnounces(testing::_));
    EXPECT_CALL(callback, onFetch(testing::_));
    EXPECT_CALL(callback, onFetchCancel(testing::_));
    EXPECT_CALL(callback, onFetchOk(testing::_));
    expectOnRequestError(callback, GetParam(), FrameType::FETCH_ERROR);
    EXPECT_CALL(callback, onFrame(testing::_)).Times(28);
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
      onSubgroup(testing::_, testing::_, testing::_, testing::_));
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
          false))
      .Times(2);
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectStatus(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
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
      onSubgroup(testing::_, testing::_, testing::_, testing::_));
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
      onSubgroup(TrackAlias(1), 2, 3, folly::Optional<uint8_t>(5)));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(2, 3, 4, testing::_, testing::_, testing::_, true, true));

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
      onSubgroup(TrackAlias(1), 2, 3, folly::Optional<uint8_t>(5)));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(2, 3, 4, testing::_, testing::_, _, true, false));

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
      onSubgroup(TrackAlias(1), 2, 3, folly::Optional<uint8_t>(5)));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectStatus(
          2,
          3,
          4,
          folly::Optional<uint8_t>(5),
          ObjectStatus::OBJECT_NOT_EXIST,
          _));
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
      onSubgroup(testing::_, testing::_, testing::_, testing::_));
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
      onSubgroup(testing::_, testing::_, testing::_, testing::_));

  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectBegin(2, 3, 4, testing::_, testing::_, _, false, false));
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
      onObjectBegin(2, 3, 4, testing::_, 5, _, true, false));
  EXPECT_CALL(
      objectStreamCodecCallback_,
      onObjectStatus(
          3, 3, 0, folly::Optional<uint8_t>(5), ObjectStatus::END_OF_TRACK, _));
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
  writeClientSetup(
      writeBuf,
      ClientSetup(
          {{GetParam()},
           {
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
               {folly::to_underlying(SetupKey::MAX_REQUEST_ID), "", 100},
           }}),
      GetParam());

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

  writeServerSetup(
      writeBuf,
      ServerSetup(
          {GetParam(),
           {
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
           }}),
      GetParam());
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

  writeServerSetup(
      writeBuf,
      ServerSetup(
          {GetParam(),
           {
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
           }}),
      GetParam());
  auto serverSetup = writeBuf.front()->clone();
  // Server gets server setup = error
  EXPECT_CALL(serverControlCodecCallback_, onConnectionError(testing::_));
  serverControlCodec_.onIngress(serverSetup->clone(), false);
}

INSTANTIATE_TEST_SUITE_P(
    MoQCodecTest,
    MoQCodecTest,
    ::testing::ValuesIn(kSupportedVersions));
} // namespace moxygen::test
