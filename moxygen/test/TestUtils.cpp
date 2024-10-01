/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/TestUtils.h"

#include "moxygen/MoQFramer.h"

namespace moxygen::test {

std::unique_ptr<folly::IOBuf> writeAllControlMessages() {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = writeClientSetup(
      writeBuf,
      ClientSetup(
          {{1},
           {
               {folly::to_underlying(SetupKey::ROLE),
                "",
                folly::to_underlying(Role::SUBSCRIBER)},
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
           }}));
  res = writeServerSetup(
      writeBuf,
      ServerSetup(
          {1,
           {
               {folly::to_underlying(SetupKey::ROLE),
                "",
                folly::to_underlying(Role::SUBSCRIBER)},
               {folly::to_underlying(SetupKey::PATH), "/foo", 0},
           }}));
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
          {0, SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "", folly::none}));
  res = writeSubscribeDone(
      writeBuf,
      SubscribeDone(
          {0,
           SubscribeDoneStatusCode::INTERNAL_ERROR,
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

  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllObjectMessages() {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = writeStreamHeader(
      writeBuf,
      ObjectHeader({
          0,
          1,
          2,
          3,
          4,
          5,
          ForwardPreference::Track,
          ObjectStatus::NORMAL,
          folly::none,
      }));
  res = writeObject(
      writeBuf,
      ObjectHeader(
          {0,
           1,
           2,
           3,
           4,
           5,
           ForwardPreference::Track,
           ObjectStatus::NORMAL,
           11}),
      folly::IOBuf::copyBuffer("hello world"));
  res = writeObject(
      writeBuf,
      ObjectHeader(
          {0,
           1,
           2,
           3,
           4,
           5,
           ForwardPreference::Track,
           ObjectStatus::END_OF_TRACK_AND_GROUP,
           0}),
      nullptr);
  return writeBuf.move();
}

} // namespace moxygen::test
