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
  return TrackRequestParameter(
      folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN),
      moqFrameWriter.encodeTokenValue(0, authValue));
}

std::vector<Parameter> getTestTrackRequestParams(
    const MoQFrameWriter& moqFrameWriter) {
  return {
      getTestAuthParam(moqFrameWriter, "binky"),
      Parameter(
          folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 1000),
      Parameter(
          folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION),
          3600000)};
}

std::vector<Parameter> getTestPublisherTrackRequestParams(
    const MoQFrameWriter& moqFrameWriter) {
  auto params = getTestTrackRequestParams(moqFrameWriter);
  params.emplace_back(
      folly::to_underlying(TrackRequestParamKey::PUBLISHER_PRIORITY), 100);
  return params;
}

// Helper to add test params to a Parameters object
static void addTestParams(
    TrackRequestParameters& params,
    const MoQFrameWriter& moqFrameWriter) {
  params.insertParam(getTestAuthParam(moqFrameWriter, "binky"));
  params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 1000));
  params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION), 3600000));
}

// Helper to create a ClientSetup with test params
static ClientSetup makeClientSetup(uint64_t version) {
  ClientSetup setup;
  setup.supportedVersions = {version};
  setup.params.insertParam(
      Parameter(folly::to_underlying(SetupKey::PATH), "/foo"));
  setup.params.insertParam(
      Parameter(folly::to_underlying(SetupKey::MAX_REQUEST_ID), 100));
  return setup;
}

// Helper to create a ServerSetup with test params
static ServerSetup makeServerSetup(uint64_t version) {
  ServerSetup setup;
  setup.selectedVersion = version;
  setup.params.insertParam(
      Parameter(folly::to_underlying(SetupKey::PATH), "/foo"));
  return setup;
}

std::unique_ptr<folly::IOBuf> writeAllControlMessages(
    TestControlMessages in,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  WriteResult res;
  if (in != TestControlMessages::SERVER) {
    res = writeClientSetup(writeBuf, makeClientSetup(version), version);
  }
  if (in != TestControlMessages::CLIENT) {
    res = writeServerSetup(writeBuf, makeServerSetup(version), version);
  }
  auto req = SubscribeRequest::make(
      FullTrackName({TrackNamespace({"hello"}), "world"}),
      255,
      GroupOrder::Default,
      true,
      LocationType::LargestObject,
      std::nullopt,
      0,
      getTestTrackRequestParams(moqFrameWriter));
  res = moqFrameWriter.writeSubscribeRequest(writeBuf, req);

  // SubscribeUpdate
  SubscribeUpdate subscribeUpdate;
  subscribeUpdate.requestID = RequestID(0);
  subscribeUpdate.existingRequestID = RequestID(0);
  subscribeUpdate.start = AbsoluteLocation{1, 2};
  subscribeUpdate.endGroup = std::optional<uint64_t>(3);
  subscribeUpdate.priority = 255;
  subscribeUpdate.forward = std::optional<bool>(true);
  addTestParams(subscribeUpdate.params, moqFrameWriter);
  res = moqFrameWriter.writeSubscribeUpdate(writeBuf, subscribeUpdate);

  // SubscribeOk
  SubscribeOk subscribeOk;
  subscribeOk.requestID = 0;
  subscribeOk.trackAlias = TrackAlias(17);
  subscribeOk.expires = std::chrono::milliseconds(0);
  subscribeOk.groupOrder = GroupOrder::OldestFirst;
  subscribeOk.largest = AbsoluteLocation{2, 5};
  // Draft 16+: Add extensions
  if (getDraftMajorVersion(version) >= 16) {
    subscribeOk.extensions.insertMutableExtensions(getTestExtensions());
  }
  subscribeOk.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION), 3600000));
  res = moqFrameWriter.writeSubscribeOk(writeBuf, subscribeOk);

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
  res = moqFrameWriter.writePublishDone(
      writeBuf,
      PublishDone(
          {RequestID(0), PublishDoneStatusCode::SUBSCRIPTION_ENDED, 7, ""}));

  // PublishRequest
  PublishRequest publishRequest;
  publishRequest.requestID = RequestID(0);
  publishRequest.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  publishRequest.groupOrder = GroupOrder::Default;
  publishRequest.largest = std::nullopt;
  publishRequest.forward = true;
  // Draft 16+: Add extensions
  if (getDraftMajorVersion(version) >= 16) {
    publishRequest.extensions.insertMutableExtensions(getTestExtensions());
  }
  addTestParams(publishRequest.params, moqFrameWriter);
  publishRequest.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::PUBLISHER_PRIORITY), 100));
  res = moqFrameWriter.writePublish(writeBuf, publishRequest);

  // PublishOk
  PublishOk publishOk;
  publishOk.requestID = 0;
  publishOk.forward = false;
  publishOk.subscriberPriority = 128;
  publishOk.groupOrder = GroupOrder::Default;
  publishOk.locType = LocationType::LargestObject;
  publishOk.start = std::nullopt;
  publishOk.endGroup = std::nullopt;
  addTestParams(publishOk.params, moqFrameWriter);
  res = moqFrameWriter.writePublishOk(writeBuf, publishOk);

  res = moqFrameWriter.writeRequestError(
      writeBuf,
      PublishError({
          0,
          PublishErrorCode::INTERNAL_ERROR,
          "server error",
      }),
      FrameType::PUBLISH_ERROR);

  // PublishNamespace
  PublishNamespace publishNamespace;
  publishNamespace.requestID = 1;
  publishNamespace.trackNamespace = TrackNamespace({"hello"});
  addTestParams(publishNamespace.params, moqFrameWriter);
  res = moqFrameWriter.writePublishNamespace(writeBuf, publishNamespace);

  // PublishNamespaceOk
  PublishNamespaceOk publishNamespaceOk;
  publishNamespaceOk.requestID = 1;
  res = moqFrameWriter.writePublishNamespaceOk(writeBuf, publishNamespaceOk);
  res = moqFrameWriter.writeRequestError(
      writeBuf,
      PublishNamespaceError(
          {RequestID(1),
           PublishNamespaceErrorCode::INTERNAL_ERROR,
           "server error"}),
      FrameType::PUBLISH_NAMESPACE_ERROR);
  PublishNamespaceCancel publishNamespaceCancel;
  if (getDraftMajorVersion(version) >= 16) {
    publishNamespaceCancel.requestID = RequestID(1);
  } else {
    publishNamespaceCancel.trackNamespace = TrackNamespace({"hello"});
  }
  publishNamespaceCancel.errorCode = PublishNamespaceErrorCode::INTERNAL_ERROR;
  publishNamespaceCancel.reasonPhrase = "internal error";
  res = moqFrameWriter.writePublishNamespaceCancel(
      writeBuf, publishNamespaceCancel);
  PublishNamespaceDone publishNamespaceDone;
  if (getDraftMajorVersion(version) >= 16) {
    publishNamespaceDone.requestID = RequestID(1);
  } else {
    publishNamespaceDone.trackNamespace = TrackNamespace({"hello"});
  }
  res =
      moqFrameWriter.writePublishNamespaceDone(writeBuf, publishNamespaceDone);

  // TrackStatus
  TrackStatus trackStatus;
  trackStatus.requestID = 3;
  trackStatus.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatus.groupOrder = GroupOrder::OldestFirst;
  trackStatus.locType = LocationType::LargestObject;
  addTestParams(trackStatus.params, moqFrameWriter);
  res = moqFrameWriter.writeTrackStatus(writeBuf, trackStatus);

  // TrackStatusOk
  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = 3;
  trackStatusOk.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatusOk.statusCode = TrackStatusCode::IN_PROGRESS;
  trackStatusOk.largest = AbsoluteLocation({19, 77});
  trackStatusOk.groupOrder = GroupOrder::OldestFirst;
  addTestParams(trackStatusOk.params, moqFrameWriter);

  res = moqFrameWriter.writeTrackStatusOk(writeBuf, trackStatusOk);
  res = moqFrameWriter.writeGoaway(writeBuf, Goaway({"new uri"}));

  // SubscribeNamespace
  SubscribeNamespace subscribeNamespace;
  subscribeNamespace.requestID = 2;
  subscribeNamespace.trackNamespacePrefix = TrackNamespace({"hello"});
  subscribeNamespace.forward = true;
  subscribeNamespace.params.insertParam(
      getTestAuthParam(moqFrameWriter, "binky"));
  res = moqFrameWriter.writeSubscribeNamespace(writeBuf, subscribeNamespace);

  // SubscribeNamespaceOk
  SubscribeNamespaceOk subscribeNamespaceOk;
  subscribeNamespaceOk.requestID = 2;
  res =
      moqFrameWriter.writeSubscribeNamespaceOk(writeBuf, subscribeNamespaceOk);
  res = moqFrameWriter.writeRequestError(
      writeBuf,
      SubscribeNamespaceError(
          {RequestID(2),
           SubscribeNamespaceErrorCode::INTERNAL_ERROR,
           "server error"}),
      FrameType::SUBSCRIBE_NAMESPACE_ERROR);
  res = moqFrameWriter.writeUnsubscribeNamespace(
      writeBuf,
      UnsubscribeNamespace({RequestID(2), TrackNamespace({"hello"})}));

  // Fetch - using StandaloneFetch variant
  Fetch fetch;
  fetch.requestID = 0;
  fetch.fullTrackName = FullTrackName({TrackNamespace({"hello"}), "world"});
  fetch.args =
      StandaloneFetch(AbsoluteLocation({0, 0}), AbsoluteLocation({1, 1}));
  fetch.priority = 255;
  fetch.groupOrder = GroupOrder::NewestFirst;
  fetch.params.insertParam(getTestAuthParam(moqFrameWriter, "binky"));
  res = moqFrameWriter.writeFetch(writeBuf, fetch);

  res = moqFrameWriter.writeFetchCancel(writeBuf, FetchCancel({0}));

  // FetchOk
  FetchOk fetchOk;
  fetchOk.requestID = 0;
  fetchOk.groupOrder = GroupOrder::NewestFirst;
  fetchOk.endOfTrack = 1;
  fetchOk.endLocation = AbsoluteLocation({0, 0});
  // Draft 16+: Add extensions
  if (getDraftMajorVersion(version) >= 16) {
    fetchOk.extensions.insertMutableExtensions(getTestExtensions());
  }
  fetchOk.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION), 1000));
  res = moqFrameWriter.writeFetchOk(writeBuf, fetchOk);
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
      Extensions({}, {}));
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
      Extensions({}, {}));
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
