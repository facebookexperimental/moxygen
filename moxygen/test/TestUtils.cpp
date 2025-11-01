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
      {13, folly::IOBuf::copyBuffer(extTestBuff, sizeof(extTestBuff))}};
  return extensions;
}

static TrackRequestParameter getTestAuthParam(
    const MoQFrameWriter& moqFrameWriter,
    const std::string& authValue) {
  return TrackRequestParameter{
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
      moqFrameWriter.encodeTokenValue(0, authValue),
      0,
      {}};
}

TrackRequestParameters getTestTrackRequestParameters(
    const MoQFrameWriter& moqFrameWriter) {
  return {
      getTestAuthParam(moqFrameWriter, "binky"),
      {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
       "",
       1000,
       {}},
      {folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
       "",
       3600000,
       {}}};
}

TrackRequestParameters getTestPublisherTrackRequestParams(
    const MoQFrameWriter& moqFrameWriter) {
  auto params = getTestTrackRequestParameters(moqFrameWriter);
  params.insertParam(
      {folly::to_underlying(TrackRequestParamKey::PUBLISHER_PRIORITY),
       "",
       100,
       {}});
  return params;
}

std::unique_ptr<folly::IOBuf> writeAllControlMessages(
    TestControlMessages in,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  WriteResult res;
  if (in != TestControlMessages::SERVER) {
    res = writeClientSetup(
        writeBuf,
        ClientSetup(
            {{version},
             {
                 {folly::to_underlying(SetupKey::PATH), "/foo", 0},
                 {folly::to_underlying(SetupKey::MAX_REQUEST_ID), "", 100},
             }}),
        version);
  }
  if (in != TestControlMessages::CLIENT) {
    res = writeServerSetup(
        writeBuf,
        ServerSetup(
            {version,
             {
                 {folly::to_underlying(SetupKey::PATH), "/foo", 0},
             }}),
        version);
  }
  auto req = SubscribeRequest::make(
      FullTrackName({TrackNamespace({"hello"}), "world"}),
      255,
      GroupOrder::Default,
      true,
      LocationType::LargestObject,
      folly::none,
      0,
      getTestTrackRequestParameters(moqFrameWriter));
  req.trackAlias = TrackAlias(0); // required for draft-11 and below
  res = moqFrameWriter.writeSubscribeRequest(writeBuf, req);
  res = moqFrameWriter.writeSubscribeUpdate(
      writeBuf,
      SubscribeUpdate(
          {RequestID(0),
           RequestID(0),
           {1, 2},
           3,
           255,
           true,
           getTestTrackRequestParameters(moqFrameWriter)}));
  res = moqFrameWriter.writeSubscribeOk(
      writeBuf,
      SubscribeOk(
          {0,
           TrackAlias(17),
           std::chrono::milliseconds(0),
           GroupOrder::OldestFirst,
           AbsoluteLocation{2, 5},
           TrackRequestParameters(
               {{folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
                 "",
                 3600000,
                 {}}})}));
  res = moqFrameWriter.writeMaxRequestID(writeBuf, {.requestID = 50000});
  res = moqFrameWriter.writeRequestsBlocked(writeBuf, {.maxRequestID = 50000});
  res = moqFrameWriter.writeRequestError(
      writeBuf,
      SubscribeError(
          {RequestID(0), SubscribeErrorCode::TRACK_NOT_EXIST, "not found"}),
      FrameType::SUBSCRIBE_ERROR);
  res = moqFrameWriter.writeUnsubscribe(
      writeBuf,
      Unsubscribe({
          0,
      }));
  res = moqFrameWriter.writeSubscribeDone(
      writeBuf,
      SubscribeDone(
          {RequestID(0), SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, 7, ""}));
  res = moqFrameWriter.writePublish(
      writeBuf,
      PublishRequest(
          {RequestID(0),
           FullTrackName({TrackNamespace({"hello"}), "world"}),
           255,
           GroupOrder::Default,
           folly::none,
           0,
           getTestPublisherTrackRequestParams(moqFrameWriter)}));
  res = moqFrameWriter.writePublishOk(
      writeBuf,
      PublishOk(
          {0,
           false,
           128,
           GroupOrder::Default,
           LocationType::LargestObject,
           folly::none,
           folly::none,
           getTestTrackRequestParameters(moqFrameWriter)}));
  res = moqFrameWriter.writeRequestError(
      writeBuf,
      PublishError({
          0,
          PublishErrorCode::INTERNAL_ERROR,
          "server error",
      }),
      FrameType::PUBLISH_ERROR);
  res = moqFrameWriter.writeAnnounce(
      writeBuf,
      Announce(
          {1,
           TrackNamespace({"hello"}),
           TrackRequestParameters(
               {getTestAuthParam(moqFrameWriter, "binky"),
                {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
                 "",
                 1000,
                 {}},
                {folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
                 "",
                 3600000,
                 {}}})}));
  res = moqFrameWriter.writeAnnounceOk(writeBuf, AnnounceOk({1, {}}));
  res = moqFrameWriter.writeRequestError(
      writeBuf,
      AnnounceError(
          {RequestID(1), AnnounceErrorCode::INTERNAL_ERROR, "server error"}),
      FrameType::ANNOUNCE_ERROR);
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
  TrackStatus trackStatus;
  trackStatus.requestID = 3;
  trackStatus.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatus.groupOrder = GroupOrder::OldestFirst;
  trackStatus.locType = LocationType::LargestObject;
  // Params will be ignored for draft-11 and below
  trackStatus.params = getTestTrackRequestParameters(moqFrameWriter);
  res = moqFrameWriter.writeTrackStatus(writeBuf, trackStatus);

  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = 3;
  trackStatusOk.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatusOk.statusCode = TrackStatusCode::IN_PROGRESS;
  trackStatusOk.largest = AbsoluteLocation({19, 77});
  trackStatusOk.groupOrder = GroupOrder::OldestFirst;
  // Params will be ignored for draft-11 and below
  trackStatusOk.params = getTestTrackRequestParameters(moqFrameWriter);

  res = moqFrameWriter.writeTrackStatusOk(writeBuf, trackStatusOk);
  res = moqFrameWriter.writeGoaway(writeBuf, Goaway({"new uri"}));
  res = moqFrameWriter.writeSubscribeAnnounces(
      writeBuf,
      SubscribeAnnounces(
          {2,
           TrackNamespace({"hello"}),
           {getTestAuthParam(moqFrameWriter, "binky")}}));
  res = moqFrameWriter.writeSubscribeAnnouncesOk(
      writeBuf, SubscribeAnnouncesOk({2, {}}));
  res = moqFrameWriter.writeRequestError(
      writeBuf,
      SubscribeAnnouncesError(
          {RequestID(2),
           SubscribeAnnouncesErrorCode::INTERNAL_ERROR,
           "server error"}),
      FrameType::SUBSCRIBE_ANNOUNCES_ERROR);
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
           {getTestAuthParam(moqFrameWriter, "binky")}}));
  res = moqFrameWriter.writeFetchCancel(writeBuf, FetchCancel({0}));
  res = moqFrameWriter.writeFetchOk(
      writeBuf,
      FetchOk(
          {0,
           GroupOrder::NewestFirst,
           1,
           AbsoluteLocation({0, 0}),
           {{folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
             "",
             1000,
             {}}}}));
  res = moqFrameWriter.writeRequestError(
      writeBuf,
      FetchError({0, FetchErrorCode::INVALID_RANGE, "Invalid range"}),
      FrameType::FETCH_ERROR);

  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllObjectMessages(
    const MoQFrameWriter& moqFrameWriter) {
  // writes a subgroup header, object without extensions, object with
  // extensions, status without extensions, status with extensions
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(2, 3, 4, 5);
  auto res = moqFrameWriter.writeSubgroupHeader(
      writeBuf, TrackAlias(1), obj, SubgroupIDFormat::Present, true);
  obj.length = 11;
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      obj,
      folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  ObjectHeader objWithExts(
      obj.group,
      obj.subgroup,
      obj.id,
      obj.priority,
      obj.status,
      Extensions(getTestExtensions(), {}),
      15);
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      objWithExts,
      folly::IOBuf::copyBuffer("hello world ext"));
  obj.id++;
  ObjectHeader objNoExts(
      obj.group,
      obj.subgroup,
      obj.id,
      obj.priority,
      ObjectStatus::OBJECT_NOT_EXIST,
      Extensions({}, {}));
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER_SG_EXT, objNoExts, nullptr);
  obj.id++;
  ObjectHeader objWithExts2(
      obj.group,
      obj.subgroup,
      obj.id,
      obj.priority,
      ObjectStatus::END_OF_TRACK,
      Extensions(getTestExtensions(), {}));
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER_SG_EXT, objWithExts2, nullptr);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllFetchMessages(
    const MoQFrameWriter& moqFrameWriter) {
  // writes a fetch header, object without extensions, object with
  // extensions, status without extensions, status with extensions
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto res = moqFrameWriter.writeFetchHeader(writeBuf, RequestID(1));
  ObjectHeader obj(2, 3, 4, 5, 11);
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      obj,
      folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  ObjectHeader objWithExts3(
      obj.group,
      obj.subgroup,
      obj.id,
      obj.priority,
      obj.status,
      Extensions(getTestExtensions(), {}),
      15);
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::FETCH_HEADER,
      objWithExts3,
      folly::IOBuf::copyBuffer("hello world ext"));
  obj.id++;
  ObjectHeader objNoExts2(
      obj.group,
      obj.subgroup,
      obj.id,
      obj.priority,
      ObjectStatus::END_OF_GROUP,
      Extensions({}, {}));
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::FETCH_HEADER, objNoExts2, nullptr);
  obj.group++;
  obj.id = 0;
  ObjectHeader objWithExts4(
      obj.group,
      obj.subgroup,
      obj.id,
      obj.priority,
      ObjectStatus::END_OF_GROUP,
      Extensions(getTestExtensions(), {}));
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::FETCH_HEADER, objWithExts4, nullptr);
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
