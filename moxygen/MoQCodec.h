/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQFramer.h"

#include <deque>

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

  // Share an external token cache with this codec's frame parser.
  // The caller must ensure the external cache outlives this codec.
  void setTokenCache(MoQTokenCache* cache) {
    moqFrameParser_.setTokenCache(cache);
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
    virtual void onClientSetup(Setup) {}
    virtual void onServerSetup(Setup) {}
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
    // Draft 18+ only.
    virtual void onSubscribeTracks(SubscribeTracks) {}
    // Draft 18+ only.
    virtual void onPublishBlocked(PublishBlocked) {}
  };

  enum class Direction { CLIENT, SERVER };
  MoQControlCodec(Direction dir, ControlCallback* callback)
      : dir_(dir), callback_(callback) {}

  void setCallback(ControlCallback* callback) {
    callback_ = callback;
  }

  // If ParseResult::BLOCKED is returned, must call onIngress again to restart
  ParseResult onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) override;

 public:
  // Draft 18+: REQUEST_OK / REQUEST_ERROR carry no on-wire requestID; codec
  // supplies it (terminal = stream's primary; post-terminal = FIFO).
  virtual std::optional<RequestID> takeNextResponseRequestID() {
    return std::nullopt;
  }
  virtual bool hasPendingPostTerminalResponse() const {
    return false;
  }

 protected:
  virtual std::optional<RequestID> getStreamRequestID() const {
    return std::nullopt;
  }
  virtual void setStreamRequestID(RequestID) {}

  virtual bool checkFrameAllowed(FrameType f) {
    const auto major =
        negotiatedVersion_ ? getDraftMajorVersion(*negotiatedVersion_) : 0;
    switch (f) {
      case FrameType::GOAWAY:
        return true;
      // Request/reply frames moved to per-request bidi streams in draft 18.
      case FrameType::SUBSCRIBE:
      case FrameType::SUBSCRIBE_UPDATE:
      case FrameType::SUBSCRIBE_OK:
      // case FrameType::SUBSCRIBE_ERROR:
      case FrameType::REQUEST_ERROR:
      case FrameType::PUBLISH_NAMESPACE:
      // case FrameType::PUBLISH_NAMESPACE_OK:
      case FrameType::REQUEST_OK:
      case FrameType::PUBLISH_DONE:
      case FrameType::PUBLISH:
      case FrameType::PUBLISH_OK:
      case FrameType::PUBLISH_ERROR:
      case FrameType::TRACK_STATUS:
      case FrameType::FETCH:
      case FrameType::FETCH_OK:
      case FrameType::FETCH_ERROR:
        return negotiatedVersion_ &&
            !useBidiRequestStreams(*negotiatedVersion_);
      // Setup frames are version-gated; allow before negotiation.
      case FrameType::CLIENT_SETUP:
      case FrameType::SERVER_SETUP:
        return !negotiatedVersion_ || major < 17;
      case FrameType::SETUP:
        return negotiatedVersion_ && major >= 17;
      // Removed in draft 18: replaced by per-stream RESET, by bidi
      // request-stream semantics, or governed by QUIC bidi stream limits.
      case FrameType::UNSUBSCRIBE:
      case FrameType::FETCH_CANCEL:
      case FrameType::MAX_REQUEST_ID:
      case FrameType::REQUESTS_BLOCKED:
      case FrameType::PUBLISH_NAMESPACE_DONE:
      case FrameType::PUBLISH_NAMESPACE_CANCEL:
        return negotiatedVersion_ &&
            !useBidiRequestStreams(*negotiatedVersion_);
      case FrameType::LEGACY_SUBSCRIBE_NAMESPACE:
      case FrameType::SUBSCRIBE_NAMESPACE_OK:
      case FrameType::SUBSCRIBE_NAMESPACE_ERROR:
      case FrameType::UNSUBSCRIBE_NAMESPACE:
      // In draft 16, NAMESPACE is PUBLISH_NAMESPACE_ERROR (0x8) and
      // NAMESPACE_DONE is TRACK_STATUS_OK (0xE). They must be on a
      // separate bidi stream, not the control stream.
      case FrameType::NAMESPACE:
      case FrameType::NAMESPACE_DONE:
        return negotiatedVersion_ && major < 16;
      // Draft 18+ only, and never on the control stream. The renumbered
      // SUBSCRIBE_NAMESPACE wire type (0x50) replaces
      // LEGACY_SUBSCRIBE_NAMESPACE (0x11) in draft 18 and only ever appears on
      // a fresh bidi stream.
      case FrameType::SUBSCRIBE_NAMESPACE:
      case FrameType::SUBSCRIBE_TRACKS:
        return false;
      // Wire type 0xF is TRACK_STATUS_ERROR in drafts < 18, but PUBLISH_BLOCKED
      // in draft 18+. It shouldn't be allowed on the control stream. It is only
      // allowed on a SUBSCRIBE_TRACKS stream.
      case FrameType::TRACK_STATUS_ERROR:
        return !negotiatedVersion_ ||
            getDraftMajorVersion(*negotiatedVersion_) < 18;
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

 private:
  // State-machine helpers called from onIngress().
  folly::Expected<folly::Unit, ErrorCode> parseFrameHeaderType(
      folly::io::Cursor& cursor,
      size_t& remainingLength);
  folly::Expected<folly::Unit, ErrorCode> parseFrameLength(
      folly::io::Cursor& cursor,
      size_t& remainingLength);
};

// Generic bidi stream codec parameterized by allowed frame types.
// Replaces MoQSubNsReceiverCodec.
class MoQBidiStreamCodec : public MoQControlCodec {
 public:
  // Request-side: `allowedFrames` lists every frame this stream accepts; no
  // first-frame FrameType constraint (RequestID matching handled elsewhere).
  // Response-side: pass `okType`. The codec admits okType + REQUEST_ERROR
  // automatically, and requires the first frame to be one of them; entries
  // in `allowedFrames` are treated as post-terminal followups.
  explicit MoQBidiStreamCodec(
      ControlCallback* callback,
      std::vector<FrameType> allowedFrames,
      std::optional<RequestID> requestID = std::nullopt,
      std::optional<FrameType> okType = std::nullopt)
      : MoQControlCodec(Direction::SERVER, callback),
        allowedFrames_(std::move(allowedFrames)),
        streamRequestID_(requestID),
        okType_(okType),
        primaryResponseIDConsumed_(!okType_) {
    if (okType_) {
      allowedFrames_.push_back(*okType_);
      allowedFrames_.push_back(FrameType::REQUEST_ERROR);
    }
    seenSetup_ = true;
  }

  explicit MoQBidiStreamCodec(
      ControlCallback* callback,
      std::initializer_list<FrameType> allowedFrames,
      std::optional<RequestID> requestID = std::nullopt,
      std::optional<FrameType> okType = std::nullopt)
      : MoQBidiStreamCodec(
            callback,
            std::vector<FrameType>(allowedFrames),
            requestID,
            okType) {}

 protected:
  bool checkFrameAllowed(FrameType f) override {
    if (std::find(allowedFrames_.begin(), allowedFrames_.end(), f) ==
        allowedFrames_.end()) {
      return false;
    }
    if (!firstFrameSeen_) {
      // Response-side: first frame MUST be the terminal reply.
      if (okType_ && f != *okType_ && f != FrameType::REQUEST_ERROR) {
        return false;
      }
    } else {
      // Post-first-frame: no fresh request-initiating frame, and a non-
      // REQUEST_OK okType_ (e.g. SUBSCRIBE_OK) may only appear once.
      if (isRequestInitiating(f)) {
        return false;
      }
      if (okType_ && *okType_ != FrameType::REQUEST_OK && f == *okType_) {
        return false;
      }
      // Post-terminal REQUEST_OK / REQUEST_ERROR only valid with a pending
      // update.
      if ((f == FrameType::REQUEST_OK || f == FrameType::REQUEST_ERROR) &&
          !hasPendingPostTerminalResponse()) {
        return false;
      }
    }
    firstFrameSeen_ = true;
    return true;
  }

  static bool isRequestInitiating(FrameType f) {
    return f == FrameType::SUBSCRIBE || f == FrameType::FETCH ||
        f == FrameType::PUBLISH || f == FrameType::TRACK_STATUS ||
        f == FrameType::PUBLISH_NAMESPACE ||
        f == FrameType::LEGACY_SUBSCRIBE_NAMESPACE ||
        f == FrameType::SUBSCRIBE_NAMESPACE || f == FrameType::SUBSCRIBE_TRACKS;
  }

  std::optional<RequestID> getStreamRequestID() const override {
    return streamRequestID_;
  }

  void setStreamRequestID(RequestID requestID) override {
    streamRequestID_ = requestID;
  }

 public:
  // Terminal uses the stream's primary request id once. Post-terminal pops from
  // the queue (sender appends one entry per REQUEST_UPDATE sent).
  void setResponseIDQueue(std::deque<RequestID>* queue) {
    responseIDQueue_ = queue;
  }
  std::optional<RequestID> takeNextResponseRequestID() override {
    if (!primaryResponseIDConsumed_) {
      primaryResponseIDConsumed_ = true;
      if (streamRequestID_) {
        return streamRequestID_;
      }
    }
    if (!responseIDQueue_ || responseIDQueue_->empty()) {
      return std::nullopt;
    }
    auto id = responseIDQueue_->front();
    responseIDQueue_->pop_front();
    return id;
  }
  bool hasPendingPostTerminalResponse() const override {
    return responseIDQueue_ && !responseIDQueue_->empty();
  }

 private:
  std::vector<FrameType> allowedFrames_;
  std::optional<RequestID> streamRequestID_;
  std::optional<FrameType> okType_;
  std::deque<RequestID>* responseIDQueue_{nullptr};
  bool primaryResponseIDConsumed_{false};
  bool firstFrameSeen_{false};
};

using MoQSubNsReceiverCodec = MoQBidiStreamCodec;

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

  void setFetchGroupOrder(GroupOrder groupOrder) {
    moqFrameParser_.setFetchGroupOrder(groupOrder);
  }

  ParseResult onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) override;

 private:
  enum class ParseState {
    STREAM_HEADER_TYPE,
    OBJECT_STREAM,
    FETCH_HEADER,
    PADDING_STREAM,
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
