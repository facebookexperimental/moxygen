/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/TestUtils.h"

#include <folly/Random.h>
#include <folly/io/Cursor.h>
#include <functional>
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

static WriteResult writeTestSubscribe(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  auto req = SubscribeRequest::make(
      FullTrackName({TrackNamespace({"hello"}), "world"}),
      255,
      GroupOrder::Default,
      true,
      LocationType::LargestObject,
      std::nullopt,
      0,
      getTestTrackRequestParams(moqFrameWriter));
  return moqFrameWriter.writeSubscribeRequest(writeBuf, req);
}

static WriteResult writeTestRequestUpdate(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version,
    RequestID requestID) {
  SubscribeUpdate subscribeUpdate;
  subscribeUpdate.requestID = requestID;
  subscribeUpdate.existingRequestID = RequestID(0);
  subscribeUpdate.filter = LocationFilter::range({1, 2}, {3, 0});
  subscribeUpdate.priority = 255;
  subscribeUpdate.forward = std::optional<bool>(true);
  addTestParams(subscribeUpdate.params, moqFrameWriter, version);
  return moqFrameWriter.writeSubscribeUpdate(writeBuf, subscribeUpdate);
}

static WriteResult writeTestSubscribeOk(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  SubscribeOk subscribeOk;
  subscribeOk.requestID = 0;
  subscribeOk.trackAlias = TrackAlias(17);
  subscribeOk.expires = std::chrono::milliseconds(0);
  subscribeOk.groupOrder = GroupOrder::OldestFirst;
  subscribeOk.largest = AbsoluteLocation{2, 5};
  subscribeOk.extensions.insertMutableExtensions(getTestExtensions());
  // Use setter which stores in extensions
  setPublisherMaxCacheDuration(subscribeOk, std::chrono::milliseconds(3600000));
  return moqFrameWriter.writeSubscribeOk(writeBuf, subscribeOk);
}

static WriteResult writeTestSubscribeError(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  return moqFrameWriter.writeRequestError(
      writeBuf,
      SubscribeError(
          {RequestID(0), SubscribeErrorCode::DOES_NOT_EXIST, "not found"}),
      FrameType::SUBSCRIBE_ERROR);
}

static WriteResult writeTestPublishDone(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  return moqFrameWriter.writePublishDone(
      writeBuf,
      PublishDone(
          {RequestID(0), PublishDoneStatusCode::SUBSCRIPTION_ENDED, 7, ""}));
}

static WriteResult writeTestPublish(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
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
  return moqFrameWriter.writePublish(writeBuf, publishRequest);
}

static WriteResult writeTestPublishOk(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  PublishOk publishOk;
  publishOk.requestID = 0;
  publishOk.forward = false;
  publishOk.subscriberPriority = 128;
  publishOk.groupOrder = GroupOrder::Default;
  publishOk.filter = LocationFilter::nextObject();
  addTestParams(publishOk.params, moqFrameWriter, version);
  return moqFrameWriter.writePublishOk(writeBuf, publishOk);
}

static WriteResult writeTestPublishError(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  return moqFrameWriter.writeRequestError(
      writeBuf,
      PublishError({
          0,
          PublishErrorCode::INTERNAL_ERROR,
          "server error",
      }),
      FrameType::PUBLISH_ERROR);
}

static WriteResult writeTestPublishNamespace(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  PublishNamespace publishNamespace;
  publishNamespace.requestID = 1;
  publishNamespace.trackNamespace = TrackNamespace({"hello"});
  addTestParams(publishNamespace.params, moqFrameWriter, version);
  return moqFrameWriter.writePublishNamespace(writeBuf, publishNamespace);
}

static WriteResult writeTestPublishNamespaceOk(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  PublishNamespaceOk publishNamespaceOk;
  publishNamespaceOk.requestID = 1;
  return moqFrameWriter.writePublishNamespaceOk(writeBuf, publishNamespaceOk);
}

static WriteResult writeTestPublishNamespaceError(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  return moqFrameWriter.writeRequestError(
      writeBuf,
      PublishNamespaceError(
          {RequestID(1),
           PublishNamespaceErrorCode::INTERNAL_ERROR,
           "server error"}),
      FrameType::PUBLISH_NAMESPACE_ERROR);
}

static WriteResult writeTestTrackStatus(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  TrackStatus trackStatus;
  trackStatus.requestID = 3;
  trackStatus.fullTrackName =
      FullTrackName({TrackNamespace({"hello"}), "world"});
  trackStatus.groupOrder = GroupOrder::OldestFirst;
  trackStatus.filter = LocationFilter::nextObject();
  trackStatus.params = TrackRequestParameters{FrameType::TRACK_STATUS};
  addTestParams(trackStatus.params, moqFrameWriter, version);
  return moqFrameWriter.writeTrackStatus(writeBuf, trackStatus);
}

static WriteResult writeTestTrackStatusOk(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  TrackStatusOk trackStatusOk;
  trackStatusOk.requestID = 3;
  trackStatusOk.largest = AbsoluteLocation({19, 77});
  trackStatusOk.groupOrder = GroupOrder::OldestFirst;
  addTestParams(trackStatusOk.params, moqFrameWriter, version);
  return moqFrameWriter.writeTrackStatusOk(writeBuf, trackStatusOk);
}

static std::optional<RequestID> controlGoawayRequestID(
    TestControlMessages in,
    uint64_t version) {
  if (getDraftMajorVersion(version) < 18) {
    return std::nullopt;
  }
  return RequestID(in == TestControlMessages::CLIENT ? 1 : 0);
}

static WriteResult writeTestGoaway(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter,
    std::optional<RequestID> requestID,
    bool onRequestStream = false) {
  Goaway goaway{"new uri"};
  goaway.requestID = requestID;
  return moqFrameWriter.writeGoaway(writeBuf, goaway, onRequestStream);
}

static WriteResult writeTestFetch(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  // Fetch - using StandaloneFetch variant
  Fetch fetch;
  fetch.requestID = 0;
  fetch.fullTrackName = FullTrackName({TrackNamespace({"hello"}), "world"});
  fetch.args =
      StandaloneFetch(AbsoluteLocation({0, 0}), AbsoluteLocation({1, 1}));
  fetch.priority = 255;
  fetch.groupOrder = GroupOrder::NewestFirst;
  fetch.params.insertParam(getTestAuthParam(moqFrameWriter, "binky"));
  return moqFrameWriter.writeFetch(writeBuf, fetch);
}

static WriteResult writeTestFetchOk(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  FetchOk fetchOk;
  fetchOk.requestID = 0;
  fetchOk.groupOrder = GroupOrder::NewestFirst;
  fetchOk.endOfTrack = 1;
  fetchOk.endLocation = AbsoluteLocation({0, 0});
  fetchOk.extensions.insertMutableExtensions(getTestExtensions());
  // Use setter which stores in extensions
  setPublisherMaxCacheDuration(fetchOk, std::chrono::milliseconds(1000));
  return moqFrameWriter.writeFetchOk(writeBuf, fetchOk);
}

static WriteResult writeTestFetchError(
    folly::IOBufQueue& writeBuf,
    const MoQFrameWriter& moqFrameWriter) {
  return moqFrameWriter.writeRequestError(
      writeBuf,
      FetchError({0, FetchErrorCode::INVALID_RANGE, "Invalid range"}),
      FrameType::FETCH_ERROR);
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
  res = writeTestSubscribe(writeBuf, moqFrameWriter);
  res = writeTestRequestUpdate(writeBuf, moqFrameWriter, version, RequestID(0));
  res = writeTestSubscribeOk(writeBuf, moqFrameWriter);
  res = moqFrameWriter.writeMaxRequestID(writeBuf, {.requestID = 50000});
  res = moqFrameWriter.writeRequestsBlocked(writeBuf, {.maxRequestID = 50000});
  res = writeTestSubscribeError(writeBuf, moqFrameWriter);
  res = moqFrameWriter.writeUnsubscribe(
      writeBuf,
      Unsubscribe({
          0,
      }));
  res = writeTestPublishDone(writeBuf, moqFrameWriter);
  res = writeTestPublish(writeBuf, moqFrameWriter);
  res = writeTestPublishOk(writeBuf, moqFrameWriter, version);
  res = writeTestPublishError(writeBuf, moqFrameWriter);
  res = writeTestPublishNamespace(writeBuf, moqFrameWriter, version);
  res = writeTestPublishNamespaceOk(writeBuf, moqFrameWriter);
  res = writeTestPublishNamespaceError(writeBuf, moqFrameWriter);
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
  res = writeTestTrackStatus(writeBuf, moqFrameWriter, version);
  res = writeTestTrackStatusOk(writeBuf, moqFrameWriter, version);
  res = writeTestGoaway(
      writeBuf, moqFrameWriter, controlGoawayRequestID(in, version));
  res = writeTestFetch(writeBuf, moqFrameWriter);
  res = moqFrameWriter.writeFetchCancel(writeBuf, FetchCancel({0}));
  res = writeTestFetchOk(writeBuf, moqFrameWriter);
  res = writeTestFetchError(writeBuf, moqFrameWriter);
  return writeBuf.move();
}

std::unique_ptr<folly::IOBuf> writeControlStreamMessages(
    TestControlMessages in,
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  WriteResult res;
  if (in == TestControlMessages::CLIENT) {
    res = writeClientSetup(writeBuf, makeClientSetup(version), version);
  } else {
    res = writeServerSetup(writeBuf, makeServerSetup(version), version);
  }
  res = writeTestGoaway(
      writeBuf, moqFrameWriter, controlGoawayRequestID(in, version));
  return writeBuf.move();
}

using MessageWriter = std::function<WriteResult(folly::IOBufQueue&)>;

static std::unique_ptr<folly::IOBuf> writeStream(
    std::initializer_list<MessageWriter> writers) {
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  for (const auto& write : writers) {
    (void)write(writeBuf);
  }
  return writeBuf.move();
}

std::vector<TestRequestExchange> writeAllRequestStreamMessages(
    const MoQFrameWriter& moqFrameWriter,
    uint64_t version) {
  // The codec configurations mirror MoQSession::getBidiStreamConfig for the
  // responder, and the sendRequest callers in MoQSession.cpp and
  // MoQRelaySession.cpp for the requester.
  const std::vector<FrameType> subscribePostTerminal{
      FrameType::PUBLISH_DONE,
      FrameType::REQUEST_OK,
      FrameType::REQUEST_ERROR,
      FrameType::GOAWAY};
  const std::vector<FrameType> subscribeResponder{
      FrameType::SUBSCRIBE, FrameType::REQUEST_UPDATE};
  const std::vector<FrameType> publishPostTerminal{
      FrameType::REQUEST_UPDATE,
      FrameType::REQUEST_OK,
      FrameType::REQUEST_ERROR};
  const std::vector<FrameType> publishResponder{
      FrameType::PUBLISH,
      FrameType::REQUEST_UPDATE,
      FrameType::PUBLISH_DONE,
      FrameType::REQUEST_OK,
      FrameType::REQUEST_ERROR};
  const std::vector<FrameType> publishNamespacePostTerminal{
      FrameType::REQUEST_OK, FrameType::REQUEST_ERROR};
  const std::vector<FrameType> publishNamespaceResponder{
      FrameType::PUBLISH_NAMESPACE, FrameType::REQUEST_UPDATE};
  const std::vector<FrameType> fetchPostTerminal{
      FrameType::REQUEST_OK, FrameType::REQUEST_ERROR, FrameType::GOAWAY};
  const std::vector<FrameType> fetchResponder{
      FrameType::FETCH, FrameType::REQUEST_UPDATE};

  const RequestID subscribeID(0);
  const RequestID updateID(2);
  const RequestID publishID(0);
  const RequestID publishNamespaceID(1);
  const RequestID trackStatusID(3);
  const RequestID fetchID(0);
  const auto& w = moqFrameWriter;

  std::vector<TestRequestExchange> exchanges;
  exchanges.push_back(
      {.name = "subscribe",
       .requestID = subscribeID,
       .okType = FrameType::SUBSCRIBE_OK,
       .postTerminal = subscribePostTerminal,
       .pendingUpdateIDs = {updateID},
       .responderAllowedFrames = subscribeResponder,
       .toResponder = writeStream(
           {[&](auto& buf) { return writeTestSubscribe(buf, w); },
            [&](auto& buf) {
              return writeTestRequestUpdate(buf, w, version, updateID);
            }}),
       .atResponder =
           {{FrameType::SUBSCRIBE, subscribeID},
            {FrameType::REQUEST_UPDATE, updateID}},
       .toRequester = writeStream(
           {[&](auto& buf) { return writeTestSubscribeOk(buf, w); },
            [&](auto& buf) {
              return w.writeRequestOk(
                  buf, RequestOk{.requestID = updateID}, FrameType::REQUEST_OK);
            },
            [&](auto& buf) { return writeTestPublishDone(buf, w); },
            [&](auto& buf) {
              return writeTestGoaway(
                  buf, w, std::nullopt, /*onRequestStream=*/true);
            }}),
       .atRequester = {
           {FrameType::SUBSCRIBE_OK, subscribeID},
           {FrameType::REQUEST_OK, updateID},
           {FrameType::PUBLISH_DONE, subscribeID},
           {FrameType::GOAWAY, subscribeID}}});
  exchanges.push_back(
      {.name = "subscribe error",
       .requestID = subscribeID,
       .okType = FrameType::SUBSCRIBE_OK,
       .postTerminal = subscribePostTerminal,
       .responderAllowedFrames = subscribeResponder,
       .toResponder =
           writeStream({[&](auto& buf) { return writeTestSubscribe(buf, w); }}),
       .atResponder = {{FrameType::SUBSCRIBE, subscribeID}},
       .toRequester = writeStream(
           {[&](auto& buf) { return writeTestSubscribeError(buf, w); }}),
       .atRequester = {{FrameType::REQUEST_ERROR, subscribeID}}});
  exchanges.push_back(
      {.name = "publish",
       .requestID = publishID,
       .okType = FrameType::REQUEST_OK,
       .postTerminal = publishPostTerminal,
       .responderAllowedFrames = publishResponder,
       .toResponder = writeStream(
           {[&](auto& buf) { return writeTestPublish(buf, w); },
            [&](auto& buf) { return writeTestPublishDone(buf, w); }}),
       .atResponder =
           {{FrameType::PUBLISH, publishID},
            {FrameType::PUBLISH_DONE, publishID}},
       .toRequester = writeStream(
           {[&](auto& buf) { return writeTestPublishOk(buf, w, version); }}),
       .atRequester = {{FrameType::REQUEST_OK, publishID}}});
  exchanges.push_back(
      {.name = "publish error",
       .requestID = publishID,
       .okType = FrameType::REQUEST_OK,
       .postTerminal = publishPostTerminal,
       .responderAllowedFrames = publishResponder,
       .toResponder =
           writeStream({[&](auto& buf) { return writeTestPublish(buf, w); }}),
       .atResponder = {{FrameType::PUBLISH, publishID}},
       .toRequester = writeStream(
           {[&](auto& buf) { return writeTestPublishError(buf, w); }}),
       .atRequester = {{FrameType::REQUEST_ERROR, publishID}}});
  exchanges.push_back(
      {.name = "publish namespace",
       .requestID = publishNamespaceID,
       .okType = FrameType::REQUEST_OK,
       .postTerminal = publishNamespacePostTerminal,
       .responderAllowedFrames = publishNamespaceResponder,
       .toResponder = writeStream({[&](auto& buf) {
         return writeTestPublishNamespace(buf, w, version);
       }}),
       .atResponder = {{FrameType::PUBLISH_NAMESPACE, publishNamespaceID}},
       .toRequester = writeStream(
           {[&](auto& buf) { return writeTestPublishNamespaceOk(buf, w); }}),
       .atRequester = {{FrameType::REQUEST_OK, publishNamespaceID}}});
  exchanges.push_back(
      {.name = "publish namespace error",
       .requestID = publishNamespaceID,
       .okType = FrameType::REQUEST_OK,
       .postTerminal = publishNamespacePostTerminal,
       .responderAllowedFrames = publishNamespaceResponder,
       .toResponder = writeStream({[&](auto& buf) {
         return writeTestPublishNamespace(buf, w, version);
       }}),
       .atResponder = {{FrameType::PUBLISH_NAMESPACE, publishNamespaceID}},
       .toRequester = writeStream(
           {[&](auto& buf) { return writeTestPublishNamespaceError(buf, w); }}),
       .atRequester = {{FrameType::REQUEST_ERROR, publishNamespaceID}}});
  exchanges.push_back(
      {.name = "track status",
       .requestID = trackStatusID,
       .okType = FrameType::REQUEST_OK,
       .responderAllowedFrames = {FrameType::TRACK_STATUS},
       .toResponder = writeStream(
           {[&](auto& buf) { return writeTestTrackStatus(buf, w, version); }}),
       .atResponder = {{FrameType::TRACK_STATUS, trackStatusID}},
       .toRequester = writeStream({[&](auto& buf) {
         return writeTestTrackStatusOk(buf, w, version);
       }}),
       .atRequester = {{FrameType::REQUEST_OK, trackStatusID}}});
  exchanges.push_back(
      {.name = "fetch",
       .requestID = fetchID,
       .okType = FrameType::FETCH_OK,
       .postTerminal = fetchPostTerminal,
       .responderAllowedFrames = fetchResponder,
       .toResponder =
           writeStream({[&](auto& buf) { return writeTestFetch(buf, w); }}),
       .atResponder = {{FrameType::FETCH, fetchID}},
       .toRequester =
           writeStream({[&](auto& buf) { return writeTestFetchOk(buf, w); }}),
       .atRequester = {{FrameType::FETCH_OK, fetchID}}});
  exchanges.push_back(
      {.name = "fetch error",
       .requestID = fetchID,
       .okType = FrameType::FETCH_OK,
       .postTerminal = fetchPostTerminal,
       .responderAllowedFrames = fetchResponder,
       .toResponder =
           writeStream({[&](auto& buf) { return writeTestFetch(buf, w); }}),
       .atResponder = {{FrameType::FETCH, fetchID}},
       .toRequester = writeStream(
           {[&](auto& buf) { return writeTestFetchError(buf, w); }}),
       .atRequester = {{FrameType::REQUEST_ERROR, fetchID}}});
  return exchanges;
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
