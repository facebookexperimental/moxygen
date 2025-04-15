/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/TestUtils.h"

#include <folly/Random.h>
#include <folly/io/Cursor.h>
#include "moxygen/MoQFramer.h"

namespace moxygen::test {

std::vector<Extension> getTestExtensions() {
  static uint8_t extTestBuff[3] = {0x01, 0x02, 0x03};
  static std::vector<Extension> extensions = {
      {10, 10},
      {11, folly::IOBuf::copyBuffer(extTestBuff, sizeof(extTestBuff))}};
  return extensions;
}

std::unique_ptr<folly::IOBuf> writeAllControlMessages(
    TestControlMessages in,
    const MoQFrameWriter& moqFrameWriter) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  WriteResult res;
  if (in != TestControlMessages::SERVER) {
    res = writeClientSetup(
        writeBuf,
        ClientSetup(
            {{1},
             {
                 {folly::to_underlying(SetupKey::PATH), "/foo", 0},
                 {folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID), "", 100},
             }}));
  }
  if (in != TestControlMessages::CLIENT) {
    res = writeServerSetup(
        writeBuf,
        ServerSetup(
            {1,
             {
                 {folly::to_underlying(SetupKey::PATH), "/foo", 0},
             }}));
  }
  res = moqFrameWriter.writeSubscribeRequest(
      writeBuf,
      SubscribeRequest(
          {0,
           0,
           FullTrackName({TrackNamespace({"hello"}), "world"}),
           255,
           GroupOrder::Default,
           LocationType::LatestObject,
           folly::none,
           0,
           {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
             "binky",
             0},
            {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
             "",
             1000},
            {folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
             "",
             3600000}}}));
  res = moqFrameWriter.writeSubscribeUpdate(
      writeBuf,
      SubscribeUpdate(
          {0,
           {1, 2},
           3,
           255,
           {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
             "binky",
             0},
            {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
             "",
             1000},
            {folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
             "",
             3600000}}}));
  res = moqFrameWriter.writeSubscribeOk(
      writeBuf,
      SubscribeOk(
          {0,
           std::chrono::milliseconds(0),
           GroupOrder::OldestFirst,
           AbsoluteLocation{2, 5},
           {{folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
             "",
             3600000}}}));
  res = moqFrameWriter.writeMaxSubscribeId(writeBuf, {.subscribeID = 50000});
  res = moqFrameWriter.writeSubscribesBlocked(
      writeBuf, {.maxSubscribeID = 50000});
  res = moqFrameWriter.writeSubscribeError(
      writeBuf,
      SubscribeError(
          {0, SubscribeErrorCode::TRACK_NOT_EXIST, "not found", folly::none}));
  res = moqFrameWriter.writeUnsubscribe(
      writeBuf,
      Unsubscribe({
          0,
      }));
  res = moqFrameWriter.writeSubscribeDone(
      writeBuf,
      SubscribeDone({0, SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, 7, ""}));
  res = moqFrameWriter.writeAnnounce(
      writeBuf,
      Announce(
          {TrackNamespace({"hello"}),
           {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
             "binky",
             0},
            {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
             "",
             1000},
            {folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
             "",
             3600000}}}));
  res = moqFrameWriter.writeAnnounceOk(
      writeBuf, AnnounceOk({TrackNamespace({"hello"})}));
  res = moqFrameWriter.writeAnnounceError(
      writeBuf,
      AnnounceError(
          {TrackNamespace({"hello"}),
           AnnounceErrorCode::INTERNAL_ERROR,
           "server error"}));
  res = moqFrameWriter.writeAnnounceCancel(
      writeBuf,
      AnnounceCancel(
          {TrackNamespace({"hello"}),
           AnnounceErrorCode::INTERNAL_ERROR,
           "internal error"}));
  res = moqFrameWriter.writeUnannounce(
      writeBuf,
      Unannounce({
          TrackNamespace({"hello"}),
      }));
  res = moqFrameWriter.writeTrackStatusRequest(
      writeBuf,
      TrackStatusRequest(
          {FullTrackName({TrackNamespace({"hello"}), "world"})}));
  res = moqFrameWriter.writeTrackStatus(
      writeBuf,
      TrackStatus(
          {FullTrackName({TrackNamespace({"hello"}), "world"}),
           TrackStatusCode::IN_PROGRESS,
           AbsoluteLocation({19, 77})}));
  res = moqFrameWriter.writeGoaway(writeBuf, Goaway({"new uri"}));
  res = moqFrameWriter.writeSubscribeAnnounces(
      writeBuf,
      SubscribeAnnounces(
          {TrackNamespace({"hello"}),
           {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
             "binky"}}}));
  res = moqFrameWriter.writeSubscribeAnnouncesOk(
      writeBuf, SubscribeAnnouncesOk({TrackNamespace({"hello"})}));
  res = moqFrameWriter.writeSubscribeAnnouncesError(
      writeBuf,
      SubscribeAnnouncesError(
          {TrackNamespace({"hello"}),
           SubscribeAnnouncesErrorCode::INTERNAL_ERROR,
           "server error"}));
  res = moqFrameWriter.writeUnsubscribeAnnounces(
      writeBuf, UnsubscribeAnnounces({TrackNamespace({"hello"})}));
  res = moqFrameWriter.writeFetch(
      writeBuf,
      Fetch(
          {0,
           FullTrackName({TrackNamespace({"hello"}), "world"}),
           AbsoluteLocation({0, 0}),
           AbsoluteLocation({1, 1}),
           255,
           GroupOrder::NewestFirst,
           {TrackRequestParameter(
               {folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
                "binky",
                0})}}));
  res = moqFrameWriter.writeFetchCancel(writeBuf, FetchCancel({0}));
  res = moqFrameWriter.writeFetchOk(
      writeBuf,
      FetchOk(
          {0,
           GroupOrder::NewestFirst,
           1,
           AbsoluteLocation({0, 0}),
           {TrackRequestParameter(
               {folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
                "binky",
                0})}}));
  res = moqFrameWriter.writeFetchError(
      writeBuf,
      FetchError({0, FetchErrorCode::INVALID_RANGE, "Invalid range"}));

  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllObjectMessages(
    const MoQFrameWriter& moqFrameWriter) {
  // writes a subgroup header, object without extensions, object with
  // extensions, status without extensions, status with extensions
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(TrackAlias(1), 2, 3, 4, 5);
  auto res = moqFrameWriter.writeSubgroupHeader(writeBuf, obj);
  obj.length = 11;
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.length = 15;
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world ext"));
  obj.id++;
  obj.length = folly::none;
  obj.status = ObjectStatus::OBJECT_NOT_EXIST;
  obj.extensions.clear();
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER, obj, nullptr);
  obj.id++;
  obj.status = ObjectStatus::END_OF_TRACK_AND_GROUP;
  obj.extensions = getTestExtensions();
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER, obj, nullptr);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllFetchMessages(
    const MoQFrameWriter& moqFrameWriter) {
  // writes a fetch header, object without extensions, object with
  // extensions, status without extensions, status with extensions
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter.writeFetchHeader(writeBuf, SubscribeID(1));
  ObjectHeader obj(TrackAlias(1), 2, 3, 4, 5, 11);
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.length = 15;
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world ext"));
  obj.id++;
  obj.length = folly::none;
  obj.extensions.clear();
  obj.status = ObjectStatus::END_OF_GROUP;
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::FETCH_HEADER, obj, nullptr);
  obj.group++;
  obj.id = 0;
  obj.extensions = getTestExtensions();
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::FETCH_HEADER, obj, nullptr);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllDatagramMessages(
    const MoQFrameWriter& moqFrameWriter) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(TrackAlias(1), 2, 3, 4, 5, ObjectStatus::OBJECT_NOT_EXIST);
  auto res = moqFrameWriter.writeDatagramObject(writeBuf, obj, nullptr);
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.status = ObjectStatus::END_OF_GROUP;
  res = moqFrameWriter.writeDatagramObject(writeBuf, obj, nullptr);
  obj.group++;
  obj.id = 0;
  obj.extensions.clear();
  obj.status = ObjectStatus::NORMAL;
  obj.length = 11;
  res = moqFrameWriter.writeDatagramObject(
      writeBuf, obj, folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.length = 15;
  res = moqFrameWriter.writeDatagramObject(
      writeBuf, obj, folly::IOBuf::copyBuffer("hello world ext"));
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> makeBuf(uint32_t size) {
  auto out = folly::IOBuf::create(size);
  out->append(size);
  // fill with random junk
  folly::io::RWPrivateCursor cursor(out.get());
  while (cursor.length() >= 8) {
    cursor.write<uint64_t>(folly::Random::rand64());
  }
  while (cursor.length()) {
    cursor.write<uint8_t>((uint8_t)folly::Random::rand32());
  }
  return out;
}

} // namespace moxygen::test
