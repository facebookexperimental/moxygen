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
           folly::none,
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
           {3, 4},
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
  res = writeSubscribeError(
      writeBuf, SubscribeError({0, 404, "not found", folly::none}));
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
      AnnounceError({TrackNamespace({"hello"}), 500, "server error"}));
  res = writeAnnounceCancel(
      writeBuf,
      AnnounceCancel({TrackNamespace({"hello"}), 500, "internal error"}));
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
          {TrackNamespace({"hello"}), 500, "server error"}));
  res = writeUnsubscribeAnnounces(
      writeBuf, UnsubscribeAnnounces({TrackNamespace({"hello"})}));
  res = writeFetch(
      writeBuf,
      Fetch(
          {0,
           FullTrackName({TrackNamespace({"hello"}), "world"}),
           255,
           GroupOrder::NewestFirst,
           AbsoluteLocation({0, 0}),
           AbsoluteLocation({1, 1}),
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
      FetchError(
          {0,
           folly::to_underlying(FetchErrorCode::INVALID_RANGE),
           "Invalid range"}));

  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllObjectMessages() {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = writeSubgroupHeader(
      writeBuf,
      ObjectHeader({
          TrackAlias(1),
          2,
          3,
          4,
          5,
          ObjectStatus::NORMAL,
          folly::none,
      }));
  res = writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      ObjectHeader({TrackAlias(1), 2, 3, 4, 5, ObjectStatus::NORMAL, 11}),
      folly::IOBuf::copyBuffer("hello world"));
  res = writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER,
      ObjectHeader(
          {TrackAlias(1), 2, 3, 4, 5, ObjectStatus::END_OF_TRACK_AND_GROUP, 0}),
      nullptr);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllFetchMessages() {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = writeFetchHeader(writeBuf, SubscribeID(1));
  res = writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      ObjectHeader({TrackAlias(1), 2, 3, 4, 5, ObjectStatus::NORMAL, 11}),
      folly::IOBuf::copyBuffer("hello world"));
  res = writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      ObjectHeader(
          {TrackAlias(1), 2, 3, 4, 5, ObjectStatus::END_OF_TRACK_AND_GROUP, 0}),
      nullptr);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllDatagramMessages() {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = writeDatagramObject(
      writeBuf,
      ObjectHeader(
          {TrackAlias(1), 2, 3, 4, 5, ObjectStatus::OBJECT_NOT_EXIST, 0}),
      nullptr);
  res = writeDatagramObject(
      writeBuf,
      ObjectHeader({TrackAlias(1), 2, 3, 4, 5, ObjectStatus::NORMAL, 11}),
      folly::IOBuf::copyBuffer("hello world"));
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
