/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQCodec.h"
#include "moxygen/test/Mocks.h"
#include "moxygen/test/TestUtils.h"

#include <folly/portability/GTest.h>

namespace moxygen::test {
using testing::_;

// The parameter is the MoQ version
class MoQCodecTest : public ::testing::TestWithParam<uint64_t> {
 public:
  void SetUp() override {
    moqFrameWriter_.initializeVersion(GetParam());
  }

  void testAll(MoQControlCodec::Direction dir) {
    auto allMsgs =
        moxygen::test::writeAllControlMessages(fromDir(dir), moqFrameWriter_);
    testing::NiceMock<MockMoQCodecCallback> callback;
    MoQControlCodec codec(dir, &callback);

    if (dir == MoQControlCodec::Direction::SERVER) {
      EXPECT_CALL(callback, onClientSetup(testing::_));
    } else {
      EXPECT_CALL(callback, onServerSetup(testing::_));
    }
    EXPECT_CALL(callback, onSubscribe(testing::_));
    EXPECT_CALL(callback, onSubscribeUpdate(testing::_));
    EXPECT_CALL(callback, onSubscribeOk(testing::_));
    EXPECT_CALL(callback, onSubscribeError(testing::_));
    EXPECT_CALL(callback, onUnsubscribe(testing::_));
    EXPECT_CALL(callback, onSubscribeDone(testing::_)).Times(2);
    EXPECT_CALL(callback, onAnnounce(testing::_));
    EXPECT_CALL(callback, onAnnounceOk(testing::_));
    EXPECT_CALL(callback, onAnnounceError(testing::_));
    EXPECT_CALL(callback, onUnannounce(testing::_));
    EXPECT_CALL(callback, onTrackStatusRequest(testing::_));
    EXPECT_CALL(callback, onTrackStatus(testing::_));
    EXPECT_CALL(callback, onGoaway(testing::_));
    EXPECT_CALL(callback, onMaxSubscribeId(testing::_));
    EXPECT_CALL(callback, onSubscribeAnnounces(testing::_));
    EXPECT_CALL(callback, onSubscribeAnnouncesOk(testing::_));
    EXPECT_CALL(callback, onSubscribeAnnouncesError(testing::_));
    EXPECT_CALL(callback, onUnsubscribeAnnounces(testing::_));
    EXPECT_CALL(callback, onFetch(testing::_));
    EXPECT_CALL(callback, onFetchCancel(testing::_));
    EXPECT_CALL(callback, onFetchOk(testing::_));
    EXPECT_CALL(callback, onFetchError(testing::_));
    EXPECT_CALL(callback, onFrame(testing::_)).Times(26);

    codec.onIngress(std::move(allMsgs), true);
  }

  void testUnderflow(MoQControlCodec::Direction dir) {
    auto allMsgs =
        moxygen::test::writeAllControlMessages(fromDir(dir), moqFrameWriter_);
    testing::NiceMock<MockMoQCodecCallback> callback;
    MoQControlCodec codec(dir, &callback);

    folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
    readBuf.append(std::move(allMsgs));

    if (dir == MoQControlCodec::Direction::SERVER) {
      EXPECT_CALL(callback, onClientSetup(testing::_));
    } else {
      EXPECT_CALL(callback, onServerSetup(testing::_));
    }
    EXPECT_CALL(callback, onSubscribe(testing::_));
    EXPECT_CALL(callback, onSubscribeUpdate(testing::_));
    EXPECT_CALL(callback, onSubscribeOk(testing::_));
    EXPECT_CALL(callback, onSubscribeError(testing::_));
    EXPECT_CALL(callback, onUnsubscribe(testing::_));
    EXPECT_CALL(callback, onSubscribeDone(testing::_)).Times(2);
    EXPECT_CALL(callback, onAnnounce(testing::_));
    EXPECT_CALL(callback, onAnnounceOk(testing::_));
    EXPECT_CALL(callback, onAnnounceError(testing::_));
    EXPECT_CALL(callback, onUnannounce(testing::_));
    EXPECT_CALL(callback, onTrackStatusRequest(testing::_));
    EXPECT_CALL(callback, onTrackStatus(testing::_));
    EXPECT_CALL(callback, onGoaway(testing::_));
    EXPECT_CALL(callback, onMaxSubscribeId(testing::_));
    EXPECT_CALL(callback, onSubscribeAnnounces(testing::_));
    EXPECT_CALL(callback, onSubscribeAnnouncesOk(testing::_));
    EXPECT_CALL(callback, onSubscribeAnnouncesError(testing::_));
    EXPECT_CALL(callback, onUnsubscribeAnnounces(testing::_));
    EXPECT_CALL(callback, onFetch(testing::_));
    EXPECT_CALL(callback, onFetchCancel(testing::_));
    EXPECT_CALL(callback, onFetchOk(testing::_));
    EXPECT_CALL(callback, onFetchError(testing::_));
    EXPECT_CALL(callback, onFrame(testing::_)).Times(26);
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
};

TEST_P(MoQCodecTest, All) {
  testAll(MoQControlCodec::Direction::CLIENT);
  testAll(MoQControlCodec::Direction::SERVER);
}

TEST_P(MoQCodecTest, AllObject) {
  auto allMsgs = moxygen::test::writeAllObjectMessages(moqFrameWriter_);
  testing::StrictMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  EXPECT_CALL(
      callback, onSubgroup(testing::_, testing::_, testing::_, testing::_));
  EXPECT_CALL(
      callback,
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
      callback,
      onObjectStatus(
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_,
          testing::_))
      .Times(2);
  codec.onIngress(std::move(allMsgs), true);
}

TEST_P(MoQCodecTest, Underflow) {
  testUnderflow(MoQControlCodec::Direction::CLIENT);
  testUnderflow(MoQControlCodec::Direction::SERVER);
}

TEST_P(MoQCodecTest, UnderflowObjects) {
  auto allMsgs = moxygen::test::writeAllObjectMessages(moqFrameWriter_);
  testing::NiceMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  folly::IOBufQueue readBuf{folly::IOBufQueue::cacheChainLength()};
  readBuf.append(std::move(allMsgs));

  EXPECT_CALL(
      callback, onSubgroup(testing::_, testing::_, testing::_, testing::_));
  EXPECT_CALL(
      callback,
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
  EXPECT_CALL(callback, onObjectPayload(testing::_, testing::_))
      .Times(strlen("hello world") + strlen("hello world ext"));
  while (!readBuf.empty()) {
    codec.onIngress(readBuf.split(1), false);
  }
  codec.onIngress(nullptr, true);
}

TEST_P(MoQCodecTest, ObjectStreamPayloadFin) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeSingleObjectStream(
      writeBuf,
      ObjectHeader(TrackAlias(1), 2, 3, 4, 5, 11),
      folly::IOBuf::copyBuffer("hello world"));
  testing::StrictMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  EXPECT_CALL(callback, onSubgroup(TrackAlias(1), 2, 3, 5));
  EXPECT_CALL(
      callback,
      onObjectBegin(2, 3, 4, testing::_, testing::_, testing::_, true, true));

  codec.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, ObjectStreamPayload) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeSingleObjectStream(
      writeBuf,
      ObjectHeader(TrackAlias(1), 2, 3, 4, 5, 11),
      folly::IOBuf::copyBuffer("hello world"));
  testing::NiceMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  EXPECT_CALL(callback, onSubgroup(TrackAlias(1), 2, 3, 5));
  EXPECT_CALL(
      callback, onObjectBegin(2, 3, 4, testing::_, testing::_, _, true, false));

  codec.onIngress(writeBuf.move(), false);
  EXPECT_CALL(callback, onEndOfStream());
  codec.onIngress(std::unique_ptr<folly::IOBuf>(), true);
}

TEST_P(MoQCodecTest, EmptyObjectPayload) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moqFrameWriter_.writeSingleObjectStream(
      writeBuf,
      ObjectHeader(TrackAlias(1), 2, 3, 4, 5, ObjectStatus::OBJECT_NOT_EXIST),
      nullptr);
  testing::NiceMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  EXPECT_CALL(callback, onSubgroup(TrackAlias(1), 2, 3, 5));
  EXPECT_CALL(
      callback, onObjectStatus(2, 3, 4, 5, ObjectStatus::OBJECT_NOT_EXIST, _));
  EXPECT_CALL(callback, onEndOfStream());
  // extra coverage of underflow in header
  codec.onIngress(writeBuf.split(3), false);
  codec.onIngress(writeBuf.move(), false);
  codec.onIngress(std::unique_ptr<folly::IOBuf>(), true);
}

TEST_P(MoQCodecTest, TruncatedObject) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, ObjectHeader(TrackAlias(1), 2, 3, 4, 5));
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      ObjectHeader(TrackAlias(1), 2, 3, 4, 5, 11),
      folly::IOBuf::copyBuffer("hello")); // missing " world"
  testing::NiceMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  EXPECT_CALL(
      callback, onSubgroup(testing::_, testing::_, testing::_, testing::_));
  EXPECT_CALL(callback, onConnectionError(testing::_));

  codec.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, TruncatedObjectPayload) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter_.writeSubgroupHeader(
      writeBuf, ObjectHeader(TrackAlias(1), 2, 3, 4, 5));
  res = moqFrameWriter_.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      ObjectHeader(TrackAlias(1), 2, 3, 4, 5, 11),
      nullptr);
  testing::NiceMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  EXPECT_CALL(
      callback, onSubgroup(testing::_, testing::_, testing::_, testing::_));

  EXPECT_CALL(
      callback,
      onObjectBegin(2, 3, 4, testing::_, testing::_, _, false, false));
  codec.onIngress(writeBuf.move(), false);
  EXPECT_CALL(callback, onConnectionError(testing::_));
  writeBuf.append(std::string("hello"));
  codec.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, StreamTypeUnderflow) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  uint8_t big = 0xff;
  writeBuf.append(&big, 1);
  testing::NiceMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  EXPECT_CALL(callback, onConnectionError(ErrorCode::PARSE_UNDERFLOW));
  codec.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, UnknownStreamType) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  uint8_t bad = 0x12;
  writeBuf.append(&bad, 1);
  testing::NiceMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);

  EXPECT_CALL(callback, onConnectionError(ErrorCode::PARSE_ERROR));
  codec.onIngress(writeBuf.move(), true);
}

TEST_P(MoQCodecTest, Fetch) {
  testing::StrictMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  SubscribeID subscribeId(1);
  ObjectHeader obj(subscribeId, 2, 3, 4, 5);
  StreamType streamType = StreamType::FETCH_HEADER;
  auto res = moqFrameWriter_.writeFetchHeader(writeBuf, subscribeId);
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

  EXPECT_CALL(callback, onFetchHeader(testing::_));
  EXPECT_CALL(callback, onObjectBegin(2, 3, 4, testing::_, 5, _, true, false));
  EXPECT_CALL(
      callback, onObjectStatus(3, 3, 0, 5, ObjectStatus::END_OF_TRACK, _));
  // object after terminal status
  EXPECT_CALL(callback, onConnectionError(ErrorCode::PARSE_ERROR));
  codec.onIngress(writeBuf.move(), false);
}

TEST_P(MoQCodecTest, FetchHeaderUnderflow) {
  testing::StrictMock<MockMoQCodecCallback> callback;
  MoQObjectStreamCodec codec(&callback);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  SubscribeID subscribeId(0xffffffffffffff);
  moqFrameWriter_.writeFetchHeader(writeBuf, subscribeId);
  // only deliver first byte of fetch header
  EXPECT_CALL(callback, onConnectionError(ErrorCode::PARSE_UNDERFLOW));
  codec.onIngress(writeBuf.splitAtMost(2), true);
}

TEST_P(MoQCodecTest, InvalidFrame) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  writeBuf.append(std::string(" "));
  testing::NiceMock<MockMoQCodecCallback> callback;
  MoQControlCodec codec(MoQControlCodec::Direction::CLIENT, &callback);

  EXPECT_CALL(callback, onConnectionError(testing::_));

  codec.onIngress(writeBuf.move(), false);
}

TEST_P(MoQCodecTest, InvalidSetups) {
  testing::NiceMock<MockMoQCodecCallback> callback;
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};

  // client gets client setup
  writeClientSetup(
      writeBuf,
      ClientSetup(
          {{1},
           {
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
               {folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID), "", 100},
           }}));
  {
    MoQControlCodec clientCodec(MoQControlCodec::Direction::CLIENT, &callback);
    EXPECT_CALL(callback, onConnectionError(testing::_));
    clientCodec.onIngress(writeBuf.move(), false);
  }

  // codec gets non-setup first
  moqFrameWriter_.writeUnsubscribe(
      writeBuf,
      Unsubscribe({
          0,
      }));
  {
    MoQControlCodec clientCodec(MoQControlCodec::Direction::CLIENT, &callback);
    EXPECT_CALL(callback, onConnectionError(testing::_));
    clientCodec.onIngress(writeBuf.move(), false);
  }

  writeServerSetup(
      writeBuf,
      ServerSetup(
          {1,
           {
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
           }}));
  auto serverSetup = writeBuf.front()->clone();
  {
    MoQControlCodec clientCodec(MoQControlCodec::Direction::CLIENT, &callback);
    // This is legal, to setup next test
    EXPECT_CALL(callback, onServerSetup(testing::_));
    clientCodec.onIngress(writeBuf.move(), false);
    // Second setup = error
    EXPECT_CALL(callback, onConnectionError(testing::_));
    clientCodec.onIngress(serverSetup->clone(), false);
  }

  {
    // Server gets server setup = error
    MoQControlCodec serverCodec(MoQControlCodec::Direction::SERVER, &callback);
    EXPECT_CALL(callback, onConnectionError(testing::_));
    serverCodec.onIngress(serverSetup->clone(), false);
  }
}

INSTANTIATE_TEST_SUITE_P(
    MoQCodecTest,
    MoQCodecTest,
    ::testing::Values(kVersionDraftCurrent));
} // namespace moxygen::test
