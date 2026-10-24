/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/TestUtils.h"

#include <folly/Random.h>
#include <folly/io/Cursor.h>
#include "moxygen/MoQFramer.h"
#include "moxygen/MoQTrackProperties.h"

namespace moxygen::test {

WriteResult writeSingleObjectStream(
    const MoQFrameWriter& moqFrameWriter,
    folly::IOBufQueue& writeBuf,
    TrackAlias trackAlias,
    const ObjectHeader& objectHeader,
    std::unique_ptr<folly::IOBuf> objectPayload,
    bool endOfGroup) {
  bool hasExtensions = objectHeader.extensions.size() > 0;
  auto res = moqFrameWriter.writeSubgroupHeader(
      writeBuf,
      trackAlias,
      objectHeader,
      SubgroupOptions{
          .hasExtensions = hasExtensions,
          .subgroupIDFormat = objectHeader.subgroup == objectHeader.id
              ? SubgroupIDFormat::FirstObject
              : SubgroupIDFormat::Present,
          .hasEndOfGroup = endOfGroup,
          .beginsWithFirstObject = true});
  if (!res) {
    return res;
  }
  return moqFrameWriter.writeStreamObject(
      writeBuf,
      hasExtensions ? StreamType::SUBGROUP_HEADER_SG_EXT
                    : StreamType::SUBGROUP_HEADER_SG,
      objectHeader,
      std::move(objectPayload));
}

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

// Helper to add test params to a Parameters object
static void addTestParams(
    TrackRequestParameters& params,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  params.setMajorVersion(getDraftMajorVersion(version));
  params.insertParam(getTestAuthParam(moqFrameWriter, "binky"));
  params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 1000));
  params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::MAX_CACHE_DURATION), 3600000));
}

// Helper to create a ClientSetup with test params
static Setup makeClientSetup(uint64_t /*version*/) {
  Setup setup;
  setup.params.insertParam(
      Parameter(folly::to_underlying(SetupKey::PATH), "/foo"));
  setup.params.insertParam(
      Parameter(folly::to_underlying(SetupKey::MAX_REQUEST_ID), 100));
  return setup;
}

// Helper to create a ServerSetup with test params
static Setup makeServerSetup(uint64_t /*version*/) {
  Setup setup;
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
  subscribeUpdate.filter = LocationFilter::range({1, 2}, {3, 0});
  subscribeUpdate.priority = 255;
  subscribeUpdate.forward = std::optional<bool>(true);
  addTestParams(subscribeUpdate.params, moqFrameWriter, version);
  res = moqFrameWriter.writeSubscribeUpdate(writeBuf, subscribeUpdate);

  // SubscribeOk
  SubscribeOk subscribeOk;
  subscribeOk.requestID = 0;
  subscribeOk.trackAlias = TrackAlias(17);
  subscribeOk.expires = std::chrono::milliseconds(0);
  subscribeOk.groupOrder = GroupOrder::OldestFirst;
  subscribeOk.largest = AbsoluteLocation{2, 5};
  subscribeOk.extensions.insertMutableExtensions(getTestExtensions());
  // Use setter which stores in extensions
  setPublisherMaxCacheDuration(subscribeOk, std::chrono::milliseconds(3600000));
  res = moqFrameWriter.writeSubscribeOk(writeBuf, subscribeOk);

  res = moqFrameWriter.writeMaxRequestID(writeBuf, {.requestID = 50000});
  res = moqFrameWriter.writeRequestsBlocked(writeBuf, {.maxRequestID = 50000});
  res = moqFrameWriter.writeRequestError(
      writeBuf,
      SubscribeError(
          {RequestID(0), SubscribeErrorCode::DOES_NOT_EXIST, "not found"}),
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
  publishRequest.extensions.insertMutableExtensions(getTestExtensions());
  // Auth param stays as param (not a track property)
  publishRequest.params.insertParam(getTestAuthParam(moqFrameWriter, "binky"));
  // Use setters which store in extensions
  setPublisherDeliveryTimeout(publishRequest, std::chrono::milliseconds(1000));
  setPublisherMaxCacheDuration(
      publishRequest, std::chrono::milliseconds(3600000));
  setPublisherPriority(publishRequest, 100);
  res = moqFrameWriter.writePublish(writeBuf, publishRequest);

  // PublishOk
  PublishOk publishOk;
  publishOk.requestID = 0;
  publishOk.forward = false;
  publishOk.subscriberPriority = 128;
  publishOk.groupOrder = GroupOrder::Default;
  publishOk.filter = LocationFilter::nextObject();
  addTestParams(publishOk.params, moqFrameWriter, version);
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
  addTestParams(publishNamespace.params, moqFrameWriter, version);
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
  publishNamespaceCancel.requestID = RequestID(1);
  publishNamespaceCancel.errorCode = PublishNamespaceErrorCode::INTERNAL_ERROR;
  publishNamespaceCancel.reasonPhrase = "internal error";
  res = moqFrameWriter.writePublishNamespaceCancel(
      writeBuf, publishNamespaceCancel);
  PublishNamespaceDone publishNamespaceDone;
  publishNamespaceDone.requestID = RequestID(1);
  res =
      moqFrameWriter.writePublishNamespaceDone(writeBuf, publishNamespaceDone);

  // TrackStatus
  TrackStatus trackStatus;
  trackStatus.requestID = 3;
  trackStatus.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatus.groupOrder = GroupOrder::OldestFirst;
  trackStatus.filter = LocationFilter::nextObject();
  trackStatus.params = TrackRequestParameters{FrameType::TRACK_STATUS};
  addTestParams(trackStatus.params, moqFrameWriter, version);
  res = moqFrameWriter.writeTrackStatus(writeBuf, trackStatus);

  // TrackStatusOk
  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = 3;
  trackStatusOk.largest = AbsoluteLocation({19, 77});
  trackStatusOk.groupOrder = GroupOrder::OldestFirst;
  addTestParams(trackStatusOk.params, moqFrameWriter, version);

  res = moqFrameWriter.writeTrackStatusOk(writeBuf, trackStatusOk);
  Goaway goaway{"new uri"};
  if (getDraftMajorVersion(version) >= 18) {
    goaway.requestID = RequestID(in == TestControlMessages::CLIENT ? 1 : 0);
  }
  res = moqFrameWriter.writeGoaway(writeBuf, goaway);

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
  fetchOk.extensions.insertMutableExtensions(getTestExtensions());
  // Use setter which stores in extensions
  setPublisherMaxCacheDuration(fetchOk, std::chrono::milliseconds(1000));
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
  // extensions, then a status object (END_OF_GROUP terminates the subgroup)
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  ObjectHeader obj(2, 3, 4, 5);
  auto res = moqFrameWriter.writeSubgroupHeader(
      writeBuf, TrackAlias(1), obj, SubgroupOptions{.hasExtensions = true});
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
  // END_OF_GROUP terminates the subgroup - no more objects can follow
  ObjectHeader statusObj(
      obj.group,
      obj.subgroup,
      obj.id,
      obj.priority,
      ObjectStatus::END_OF_GROUP,
      Extensions({}, {}));
  res = moqFrameWriter.writeStreamObject(
      writeBuf, StreamType::SUBGROUP_HEADER_SG_EXT, statusObj, nullptr);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeAllFetchMessages(
    const MoQFrameWriter& moqFrameWriter) {
  // writes a fetch header, object without extensions, object with
  // extensions, then two empty objects. FETCH objects have no status field.
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
      ObjectStatus::NORMAL,
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
      ObjectStatus::NORMAL,
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
