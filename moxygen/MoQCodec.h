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
    negotiatedVersion_ = version;
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
  std::optional<uint64_t> negotiatedVersion_;
  ObjectHeader curObjectHeader_;
  MoQFrameParser moqFrameParser_;
};

class MoQControlCodec : public MoQCodec {
 public:
  class ControlCallback : public Callback {
   public:
    ~ControlCallback() override = default;

    virtual void onFrame(FrameType) {}
    virtual void onClientSetup(ClientSetup) {}
    virtual void onServerSetup(ServerSetup) {}
    virtual void onSubscribe(SubscribeRequest) {}
    virtual void onRequestUpdate(RequestUpdate) {}
    virtual void onSubscribeOk(SubscribeOk) {}
    virtual void onRequestOk(RequestOk, FrameType) {}
    virtual void onRequestError(RequestError, FrameType) {}
    virtual void onPublishDone(PublishDone) {}
    virtual void onUnsubscribe(Unsubscribe) {}
    virtual void onPublish(PublishRequest) {}
    virtual void onPublishOk(PublishOk) {}
    virtual void onMaxRequestID(MaxRequestID) {}
    virtual void onRequestsBlocked(RequestsBlocked) {}
    virtual void onFetch(Fetch) {}
    virtual void onFetchCancel(FetchCancel) {}
    virtual void onFetchOk(FetchOk) {}
    virtual void onPublishNamespace(PublishNamespace) {}
    virtual void onPublishNamespaceDone(PublishNamespaceDone) {}
    virtual void onPublishNamespaceCancel(PublishNamespaceCancel) {}
    virtual void onSubscribeNamespace(SubscribeNamespace) {}
    virtual void onUnsubscribeNamespace(UnsubscribeNamespace) {}
    virtual void onTrackStatus(TrackStatus) {}
    virtual void onTrackStatusOk(TrackStatusOk) {}
    virtual void onNamespace(Namespace) {}
    virtual void onNamespaceDone(NamespaceDone) {}
    virtual void onTrackStatusError(TrackStatusError) {}
    virtual void onGoaway(Goaway) {}
  };

  enum class Direction { CLIENT, SERVER };
  MoQControlCodec(Direction dir, ControlCallback* callback)
      : dir_(dir), callback_(callback) {}

  void setCallback(ControlCallback* callback) {
    callback_ = callback;
  }

  // If ParseResult::BLOCKED is returned, must call onIngress again to restart
  ParseResult onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) override;

 protected:
  virtual bool checkFrameAllowed(FrameType f) {
    switch (f) {
      case FrameType::SUBSCRIBE:
      case FrameType::SUBSCRIBE_UPDATE:
      case FrameType::SUBSCRIBE_OK:
      // case FrameType::SUBSCRIBE_ERROR:
      case FrameType::REQUEST_ERROR:
      case FrameType::PUBLISH_NAMESPACE:
      // case FrameType::PUBLISH_NAMESPACE_OK:
      case FrameType::REQUEST_OK:
      case FrameType::PUBLISH_NAMESPACE_DONE:
      case FrameType::UNSUBSCRIBE:
      case FrameType::PUBLISH_DONE:
      case FrameType::PUBLISH:
      case FrameType::PUBLISH_OK:
      case FrameType::PUBLISH_ERROR:
      case FrameType::PUBLISH_NAMESPACE_CANCEL:
      case FrameType::TRACK_STATUS:
      case FrameType::TRACK_STATUS_ERROR:
      case FrameType::GOAWAY:
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
      case FrameType::SUBSCRIBE_NAMESPACE:
      case FrameType::SUBSCRIBE_NAMESPACE_OK:
      case FrameType::SUBSCRIBE_NAMESPACE_ERROR:
      case FrameType::UNSUBSCRIBE_NAMESPACE:
      // In draft 16, NAMESPACE is PUBLISH_NAMESPACE_ERROR (0x8) and
      // NAMESPACE_DONE is TRACK_STATUS_OK (0xE). They must be on a
      // separate bidi stream, not the control stream.
      case FrameType::PUBLISH_NAMESPACE_ERROR:
      case FrameType::TRACK_STATUS_OK:
        return true;
        // TODO: change to return getDraftMajorVersion(*negotiatedVersion_) <
        // 16; once we actually send the SUBSCRIBE_NAMESPACE on a bidi stream
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

class MoQSubNsReceiverCodec : public MoQControlCodec {
 public:
  // The direction doesn't really matter, so I'm just setting it
  // to SERVER for now.
  explicit MoQSubNsReceiverCodec(ControlCallback* callback)
      : MoQControlCodec(Direction::SERVER, callback) {
    // Bypass the setup gating in MoQControlCodec::parseFrame().
    seenSetup_ = true;
  }

 protected:
  bool checkFrameAllowed(FrameType f) override {
    return f == FrameType::SUBSCRIBE_NAMESPACE ||
        f == FrameType::REQUEST_UPDATE;
  }
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
        bool subgroupComplete,
        bool forwardingPreferenceIsDatagram = false) = 0;
    virtual ParseResult onObjectStatus(
        uint64_t group,
        uint64_t subgroup,
        uint64_t objectID,
        std::optional<uint8_t> priority,
        ObjectStatus status) = 0;
    virtual ParseResult onObjectPayload(
        Payload payload,
        bool objectComplete) = 0;
    // Called when an End of Range marker is parsed from a FETCH response
    // isUnknownOrNonexistent: true for 0x10C (unknown), false for 0x8C
    // (non-existent)
    virtual ParseResult onEndOfRange(
        uint64_t groupId,
        uint64_t objectId,
        bool isUnknownOrNonexistent) = 0;
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

// Parses incoming NAMESPACE, NAMESPACE_DONE, REQUEST_OK, and REQUEST_ERROR
// frames and calls the appropriate callback method.
class MoQSubNsSenderCodec : public MoQControlCodec {
 public:
  // The direction doesn't really matter, so I'm just setting it
  // to SERVER for now.
  explicit MoQSubNsSenderCodec(ControlCallback* callback)
      : MoQControlCodec(Direction::SERVER, callback) {
    seenSetup_ = true;
  }

 protected:
  bool checkFrameAllowed(FrameType f) override {
    switch (f) {
      case FrameType::NAMESPACE:
      case FrameType::NAMESPACE_DONE:
      case FrameType::REQUEST_OK:
      case FrameType::REQUEST_ERROR:
        return true;
      default:
        return false;
    }
  }
};

} // namespace moxygen
