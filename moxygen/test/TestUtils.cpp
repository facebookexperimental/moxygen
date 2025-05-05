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

TrackRequestParameter getTestAuthParam(
    const MoQFrameWriter& moqFrameWriter,
    const std::string& authValue) {
  return TrackRequestParameter{
      getAuthorizationParamKey(*moqFrameWriter.getVersion()),
      moqFrameWriter.encodeTokenValue(0, authValue),
      0,
      {}};
}

std::vector<TrackRequestParameter> getTestTrackRequestParameters(
    const MoQFrameWriter& moqFrameWriter) {
  return {
      getTestAuthParam(moqFrameWriter, "binky"),
      {getDeliveryTimeoutParamKey(*moqFrameWriter.getVersion()), "", 1000, {}},
      {getMaxCacheDurationParamKey(*moqFrameWriter.getVersion()),
       "",
       3600000,
       {}}};
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
            {{1},
             {
                 {folly::to_underlying(SetupKey::PATH), "/foo", 0},
                 {folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID), "", 100},
             }}),
        version);
  }
  if (in != TestControlMessages::CLIENT) {
    res = writeServerSetup(
        writeBuf,
        ServerSetup(
            {1,
             {
                 {folly::to_underlying(SetupKey::PATH), "/foo", 0},
             }}),
        version);
  }
  res = moqFrameWriter.writeSubscribeRequest(
      writeBuf,
      SubscribeRequest(
          {0,
           0,
           FullTrackName({TrackNamespace({"hello"}), "world"}),
           255,
           GroupOrder::Default,
           true,
           LocationType::LatestObject,
           folly::none,
           0,
           getTestTrackRequestParameters(moqFrameWriter)}));
  res = moqFrameWriter.writeSubscribeUpdate(
      writeBuf,
      SubscribeUpdate(
          {0,
           {1, 2},
           3,
           255,
           true,
           getTestTrackRequestParameters(moqFrameWriter)}));
  res = moqFrameWriter.writeSubscribeOk(
      writeBuf,
      SubscribeOk(
          {0,
           std::chrono::milliseconds(0),
           GroupOrder::OldestFirst,
           AbsoluteLocation{2, 5},
           {{getMaxCacheDurationParamKey(version), "", 3600000}}}));
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
           {getTestAuthParam(moqFrameWriter, "binky"),
            {getDeliveryTimeoutParamKey(version), "", 1000},
            {getMaxCacheDurationParamKey(version), "", 3600000}}}));
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
  TrackStatusRequest trackStatusRequest;
  trackStatusRequest.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  // Params will be ignored for draft-11 and below
  trackStatusRequest.params = getTestTrackRequestParameters(moqFrameWriter);
  res = moqFrameWriter.writeTrackStatusRequest(writeBuf, trackStatusRequest);

  TrackStatus trackStatus;
  trackStatus.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatus.statusCode = TrackStatusCode::IN_PROGRESS;
  trackStatus.latestGroupAndObject = AbsoluteLocation({19, 77});
  // Params will be ignored for draft-11 and below
  trackStatus.params = getTestTrackRequestParameters(moqFrameWriter);

  res = moqFrameWriter.writeTrackStatus(writeBuf, trackStatus);
  res = moqFrameWriter.writeGoaway(writeBuf, Goaway({"new uri"}));
  res = moqFrameWriter.writeSubscribeAnnounces(
      writeBuf,
      SubscribeAnnounces(
          {TrackNamespace({"hello"}),
           {getTestAuthParam(moqFrameWriter, "binky")}}));
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
           {getTestAuthParam(moqFrameWriter, "binky")}}));
  res = moqFrameWriter.writeFetchCancel(writeBuf, FetchCancel({0}));
  res = moqFrameWriter.writeFetchOk(
      writeBuf,
      FetchOk(
          {0,
           GroupOrder::NewestFirst,
           1,
           AbsoluteLocation({0, 0}),
           {{getMaxCacheDurationParamKey(version), "", 1000, {}}}}));
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
  auto res = moqFrameWriter.writeSubgroupHeader(
      writeBuf, obj, SubgroupIDFormat::Present, true);
  obj.length = 11;
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      obj,
      folly::IOBuf::copyBuffer("hello world"));
  obj.id++;
  obj.extensions = getTestExtensions();
  obj.length = 15;
  res = moqFrameWriter.writeStreamObject(
      writeBuf,
      StreamType::SUBGROUP_HEADER_SG_EXT,
      obj,
      folly::IOBuf::copyBuffer("hello world ext"));
  obj.id++;
  obj.length = folly::none;
  obj.status = ObjectStatus::OBJECT_NOT_EXIST;
  obj.extensions.clear();
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER_SG_EXT, obj, nullptr);
  obj.id++;
  obj.status = ObjectStatus::END_OF_TRACK;
  obj.extensions = getTestExtensions();
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER_SG_EXT, obj, nullptr);
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
