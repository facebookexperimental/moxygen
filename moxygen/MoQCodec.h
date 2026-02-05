/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQFramer.h"

namespace moxygen {

class MoQCodec {
 public:
  enum class ParseResult {
    CONTINUE,
    BLOCKED,
    ERROR_TERMINATE,
  };

  virtual ~MoQCodec() = default;

  class Callback {
   public:
    virtual ~Callback() = default;

    virtual void onConnectionError(ErrorCode error) = 0;
  };

  void setStreamId(uint64_t streamId) {
    streamId_ = streamId;
  }

  void initializeVersion(uint64_t version) {
    moqFrameParser_.initializeVersion(version);
  }

  void setMaxAuthTokenCacheSize(size_t size) {
    moqFrameParser_.setTokenCacheMaxSize(size);
  }

  // If ParseResult::BLOCKED is returned, must call onIngress again to restart
  virtual ParseResult onIngress(
      std::unique_ptr<folly::IOBuf> data,
      bool eom) = 0;

 protected:
  void onIngressStart(std::unique_ptr<folly::IOBuf> data);
  void onIngressEnd(size_t remainingLength, bool eom, Callback* callback);

  uint64_t streamId_{std::numeric_limits<uint64_t>::max()};
  folly::IOBufQueue ingress_{folly::IOBufQueue::cacheChainLength()};

  std::optional<ErrorCode> connError_;
  ObjectHeader curObjectHeader_;
  MoQFrameParser moqFrameParser_;
};

class MoQControlCodec : public MoQCodec {
 public:
  class ControlCallback : public Callback {
   public:
    ~ControlCallback() override = default;

    virtual void onFrame(FrameType /*frameType*/) {}
    virtual void onClientSetup(ClientSetup clientSetup) = 0;
    virtual void onServerSetup(ServerSetup serverSetup) = 0;
    virtual void onSubscribe(SubscribeRequest subscribeRequest) = 0;
    virtual void onSubscribeUpdate(SubscribeUpdate subscribeUpdate) = 0;
    virtual void onSubscribeOk(SubscribeOk subscribeOk) = 0;
    virtual void onRequestOk(RequestOk ok, FrameType frameType) = 0;
    virtual void onRequestError(RequestError error, FrameType frameType) = 0;
    virtual void onPublishDone(PublishDone publishDone) = 0;
    virtual void onUnsubscribe(Unsubscribe unsubscribe) = 0;
    virtual void onPublish(PublishRequest publish) = 0;
    virtual void onPublishOk(PublishOk publishOk) = 0;
    virtual void onMaxRequestID(MaxRequestID maxSubId) = 0;
    virtual void onRequestsBlocked(RequestsBlocked subscribesBlocked) = 0;
    virtual void onFetch(Fetch fetch) = 0;
    virtual void onFetchCancel(FetchCancel fetchCancel) = 0;
    virtual void onFetchOk(FetchOk fetchOk) = 0;
    virtual void onPublishNamespace(PublishNamespace publishNamespace) = 0;
    virtual void onPublishNamespaceDone(
        PublishNamespaceDone publishNamespaceDone) = 0;
    virtual void onPublishNamespaceCancel(
        PublishNamespaceCancel publishNamespaceCancel) = 0;
    virtual void onSubscribeNamespace(
        SubscribeNamespace subscribeNamespace) = 0;
    virtual void onUnsubscribeNamespace(
        UnsubscribeNamespace unsubscribeNamespace) = 0;
    virtual void onTrackStatus(TrackStatus trackStatus) = 0;
    virtual void onTrackStatusOk(TrackStatusOk trackStatusOk) = 0;
    virtual void onTrackStatusError(TrackStatusError trackStatusError) = 0;
    virtual void onGoaway(Goaway goaway) = 0;
  };

  enum class Direction { CLIENT, SERVER };
  MoQControlCodec(Direction dir, ControlCallback* callback)
      : dir_(dir), callback_(callback) {}

  void setCallback(ControlCallback* callback) {
    callback_ = callback;
  }

  // If ParseResult::BLOCKED is returned, must call onIngress again to restart
  ParseResult onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) override;

 private:
  bool checkFrameAllowed(FrameType f) {
    switch (f) {
      case FrameType::SUBSCRIBE:
      case FrameType::SUBSCRIBE_UPDATE:
      case FrameType::SUBSCRIBE_OK:
      // case FrameType::SUBSCRIBE_ERROR:
      case FrameType::REQUEST_ERROR:
      case FrameType::PUBLISH_NAMESPACE:
      // case FrameType::PUBLISH_NAMESPACE_OK:
      case FrameType::REQUEST_OK:
      case FrameType::PUBLISH_NAMESPACE_ERROR:
      case FrameType::PUBLISH_NAMESPACE_DONE:
      case FrameType::UNSUBSCRIBE:
      case FrameType::PUBLISH_DONE:
      case FrameType::PUBLISH:
      case FrameType::PUBLISH_OK:
      case FrameType::PUBLISH_ERROR:
      case FrameType::PUBLISH_NAMESPACE_CANCEL:
      case FrameType::TRACK_STATUS:
      case FrameType::TRACK_STATUS_OK:
      case FrameType::TRACK_STATUS_ERROR:
      case FrameType::GOAWAY:
      case FrameType::SUBSCRIBE_NAMESPACE:
      case FrameType::SUBSCRIBE_NAMESPACE_OK:
      case FrameType::SUBSCRIBE_NAMESPACE_ERROR:
      case FrameType::UNSUBSCRIBE_NAMESPACE:
      case FrameType::CLIENT_SETUP:
      case FrameType::SERVER_SETUP:
      case FrameType::LEGACY_CLIENT_SETUP:
      case FrameType::LEGACY_SERVER_SETUP:
      case FrameType::MAX_REQUEST_ID:
      case FrameType::REQUESTS_BLOCKED:
      case FrameType::FETCH:
      case FrameType::FETCH_CANCEL:
      case FrameType::FETCH_OK:
      case FrameType::FETCH_ERROR:
        return true;
    }
    return false;
  }

  folly::Expected<folly::Unit, ErrorCode> parseFrame(folly::io::Cursor& cursor);

  Direction dir_;
  ControlCallback* callback_{nullptr};
  FrameType curFrameType_;
  size_t curFrameLength_{0};
  enum class ParseState {
    FRAME_HEADER_TYPE,
    FRAME_LENGTH,
    FRAME_PAYLOAD,
  };
  ParseState parseState_{ParseState::FRAME_HEADER_TYPE};
  bool seenSetup_{false};
};

class MoQObjectStreamCodec : public MoQCodec {
 public:
  class ObjectCallback : public Callback {
   public:
    ~ObjectCallback() override = default;

    virtual ParseResult onFetchHeader(RequestID requestID) = 0;
    virtual ParseResult onSubgroup(
        TrackAlias alias,
        uint64_t group,
        uint64_t subgroup,
        std::optional<uint8_t> priority,
        const SubgroupOptions& options) = 0;
    virtual ParseResult onObjectBegin(
        uint64_t group,
        uint64_t subgroup,
        uint64_t objectID,
        Extensions extensions,
        uint64_t length,
        Payload initialPayload,
        bool objectComplete,
        bool subgroupComplete) = 0;
    virtual ParseResult onObjectStatus(
        uint64_t group,
        uint64_t subgroup,
        uint64_t objectID,
        std::optional<uint8_t> priority,
        ObjectStatus status) = 0;
    virtual ParseResult onObjectPayload(
        Payload payload,
        bool objectComplete) = 0;
    virtual void onEndOfStream() = 0;
  };

  MoQObjectStreamCodec(ObjectCallback* callback) : callback_(callback) {}

  void setCallback(ObjectCallback* callback) {
    callback_ = callback;
  }

  ParseResult onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) override;

 private:
  enum class ParseState {
    STREAM_HEADER_TYPE,
    OBJECT_STREAM,
    FETCH_HEADER,
    MULTI_OBJECT_HEADER,
    OBJECT_PAYLOAD,
    STREAM_FIN_DELIVERED,
    // OBJECT_PAYLOAD_NO_LENGTH
  };
  ParseState parseState_{ParseState::STREAM_HEADER_TYPE};
  StreamType streamType_{StreamType::SUBGROUP_HEADER_SG};
  SubgroupOptions subgroupOptions_;
  ObjectCallback* callback_;
};

} // namespace moxygen
