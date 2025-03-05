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
  static std::vector<Extension> extensions = {{10, 10, {}}, {11, 0, {1, 2, 3}}};
  return extensions;
}

std::unique_ptr<folly::IOBuf> writeAllControlMessages(TestControlMessages in) {
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
  res = writeSubscribeRequest(
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
  res = writeSubscribeUpdate(
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
  res = writeSubscribeOk(
      writeBuf,
      SubscribeOk(
          {0,
           std::chrono::milliseconds(0),
           GroupOrder::OldestFirst,
           AbsoluteLocation{2, 5},
           {{folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
             "",
             3600000}}}));
  res = writeMaxSubscribeId(writeBuf, {.subscribeID = 50000});
  res = writeSubscribesBlocked(writeBuf, {.maxSubscribeID = 50000});
  res = writeSubscribeError(
      writeBuf,
      SubscribeError(
          {0, SubscribeErrorCode::TRACK_NOT_EXIST, "not found", folly::none}));
  res = writeUnsubscribe(
      writeBuf,
      Unsubscribe({
          0,
      }));
  res = writeSubscribeDone(
      writeBuf,
      SubscribeDone(
          {0,
           SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
           7,
           "",
           folly::none}));
  res = writeSubscribeDone(
      writeBuf,
      SubscribeDone(
          {0,
           SubscribeDoneStatusCode::INTERNAL_ERROR,
           0,
           "not found",
           AbsoluteLocation({0, 0})}));
  res = writeAnnounce(
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
  res = writeAnnounceOk(writeBuf, AnnounceOk({TrackNamespace({"hello"})}));
  res = writeAnnounceError(
      writeBuf,
      AnnounceError(
          {TrackNamespace({"hello"}),
           AnnounceErrorCode::INTERNAL_ERROR,
           "server error"}));
  res = writeAnnounceCancel(
      writeBuf,
      AnnounceCancel(
          {TrackNamespace({"hello"}),
           AnnounceErrorCode::INTERNAL_ERROR,
           "internal error"}));
  res = writeUnannounce(
      writeBuf,
      Unannounce({
          TrackNamespace({"hello"}),
      }));
  res = writeTrackStatusRequest(
      writeBuf,
      TrackStatusRequest(
          {FullTrackName({TrackNamespace({"hello"}), "world"})}));
  res = writeTrackStatus(
      writeBuf,
      TrackStatus(
          {FullTrackName({TrackNamespace({"hello"}), "world"}),
           TrackStatusCode::IN_PROGRESS,
           AbsoluteLocation({19, 77})}));
  res = writeGoaway(writeBuf, Goaway({"new uri"}));
  res = writeSubscribeAnnounces(
      writeBuf,
      SubscribeAnnounces(
          {TrackNamespace({"hello"}),
           {{folly::to_underlying(TrackRequestParamKey::AUTHORIZATION),
             "binky"}}}));
  res = writeSubscribeAnnouncesOk(
      writeBuf, SubscribeAnnouncesOk({TrackNamespace({"hello"})}));
  res = writeSubscribeAnnouncesError(
      writeBuf,
      SubscribeAnnouncesError(
          {TrackNamespace({"hello"}),
           SubscribeAnnouncesErrorCode::INTERNAL_ERROR,
           "server error"}));
  res = writeUnsubscribeAnnounces(
      writeBuf, UnsubscribeAnnounces({TrackNamespace({"hello"})}));
  res = writeFetch(
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
  res = writeFetchCancel(writeBuf, FetchCancel({0}));
  res = writeFetchOk(
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
  res = writeFetchError(
      writeBuf,
      FetchError({0, FetchErrorCode::INVALID_RANGE, "Invalid range"}));

  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllObjectMessages() {
  // writes a subgroup header, object without extensions, object with
  // extensions, status without extensions, status with extensions
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(TrackAlias(1), 2, 3, 4, 5);
  auto res = writeSubgroupHeader(writeBuf, obj);
  obj.length = 11;
  res = writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.length = 15;
  res = writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world ext"));
  obj.id++;
  obj.length = folly::none;
  obj.status = ObjectStatus::OBJECT_NOT_EXIST;
  obj.extensions.clear();
  res = writeStreamObject(writeBuf, StreamType::SUBGROUP_HEADER, obj, nullptr);
  obj.id++;
  obj.status = ObjectStatus::END_OF_TRACK_AND_GROUP;
  obj.extensions = getTestExtensions();
  res = writeStreamObject(writeBuf, StreamType::SUBGROUP_HEADER, obj, nullptr);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllFetchMessages() {
  // writes a fetch header, object without extensions, object with
  // extensions, status without extensions, status with extensions
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = writeFetchHeader(writeBuf, SubscribeID(1));
  ObjectHeader obj(TrackAlias(1), 2, 3, 4, 5, 11);
  res = writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.length = 15;
  res = writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world ext"));
  obj.id++;
  obj.length = folly::none;
  obj.extensions.clear();
  obj.status = ObjectStatus::END_OF_GROUP;
  res = writeStreamObject(writeBuf, StreamType::FETCH_HEADER, obj, nullptr);
  obj.group++;
  obj.id = 0;
  obj.extensions = getTestExtensions();
  res = writeStreamObject(writeBuf, StreamType::FETCH_HEADER, obj, nullptr);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllDatagramMessages() {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(TrackAlias(1), 2, 3, 4, 5, ObjectStatus::OBJECT_NOT_EXIST);
  auto res = writeDatagramObject(writeBuf, obj, nullptr);
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.status = ObjectStatus::END_OF_GROUP;
  res = writeDatagramObject(writeBuf, obj, nullptr);
  obj.group++;
  obj.id = 0;
  obj.extensions.clear();
  obj.status = ObjectStatus::NORMAL;
  obj.length = 11;
  res = writeDatagramObject(
      writeBuf, obj, folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.length = 15;
  res = writeDatagramObject(
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
