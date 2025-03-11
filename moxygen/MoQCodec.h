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
  virtual ~MoQCodec() = default;

  class Callback {
   public:
    virtual ~Callback() = default;

    virtual void onConnectionError(ErrorCode error) = 0;
  };

  void setStreamId(uint64_t streamId) {
    streamId_ = streamId;
  }

  virtual void onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) = 0;

 protected:
  void onIngressStart(std::unique_ptr<folly::IOBuf> data);
  void onIngressEnd(size_t remainingLength, bool eom, Callback* callback);

  uint64_t streamId_{std::numeric_limits<uint64_t>::max()};
  folly::IOBufQueue ingress_{folly::IOBufQueue::cacheChainLength()};

  folly::Optional<ErrorCode> connError_;
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
    virtual void onSubscribeError(SubscribeError subscribeError) = 0;
    virtual void onSubscribeDone(SubscribeDone subscribeDone) = 0;
    virtual void onUnsubscribe(Unsubscribe unsubscribe) = 0;
    virtual void onMaxSubscribeId(MaxSubscribeId maxSubId) = 0;
    virtual void onSubscribesBlocked(SubscribesBlocked subscribesBlocked) = 0;
    virtual void onFetch(Fetch fetch) = 0;
    virtual void onFetchCancel(FetchCancel fetchCancel) = 0;
    virtual void onFetchOk(FetchOk fetchOk) = 0;
    virtual void onFetchError(FetchError fetchError) = 0;
    virtual void onAnnounce(Announce announce) = 0;
    virtual void onAnnounceOk(AnnounceOk announceOk) = 0;
    virtual void onAnnounceError(AnnounceError announceError) = 0;
    virtual void onUnannounce(Unannounce unannounce) = 0;
    virtual void onAnnounceCancel(AnnounceCancel announceCancel) = 0;
    virtual void onSubscribeAnnounces(
        SubscribeAnnounces subscribeAnnounces) = 0;
    virtual void onSubscribeAnnouncesOk(
        SubscribeAnnouncesOk subscribeAnnouncesOk) = 0;
    virtual void onSubscribeAnnouncesError(
        SubscribeAnnouncesError announceError) = 0;
    virtual void onUnsubscribeAnnounces(
        UnsubscribeAnnounces unsubscribeAnnounces) = 0;
    virtual void onTrackStatusRequest(
        TrackStatusRequest trackStatusRequest) = 0;
    virtual void onTrackStatus(TrackStatus trackStatus) = 0;
    virtual void onGoaway(Goaway goaway) = 0;
  };

  enum class Direction { CLIENT, SERVER };
  MoQControlCodec(Direction dir, ControlCallback* callback)
      : dir_(dir), callback_(callback) {}

  void setCallback(ControlCallback* callback) {
    callback_ = callback;
  }

  void onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) override;

 private:
  bool checkFrameAllowed(FrameType f) {
    switch (f) {
      case FrameType::SUBSCRIBE:
      case FrameType::SUBSCRIBE_UPDATE:
      case FrameType::SUBSCRIBE_OK:
      case FrameType::SUBSCRIBE_ERROR:
      case FrameType::ANNOUNCE:
      case FrameType::ANNOUNCE_OK:
      case FrameType::ANNOUNCE_ERROR:
      case FrameType::UNANNOUNCE:
      case FrameType::UNSUBSCRIBE:
      case FrameType::SUBSCRIBE_DONE:
      case FrameType::ANNOUNCE_CANCEL:
      case FrameType::TRACK_STATUS_REQUEST:
      case FrameType::TRACK_STATUS:
      case FrameType::GOAWAY:
      case FrameType::SUBSCRIBE_ANNOUNCES:
      case FrameType::SUBSCRIBE_ANNOUNCES_OK:
      case FrameType::SUBSCRIBE_ANNOUNCES_ERROR:
      case FrameType::UNSUBSCRIBE_ANNOUNCES:
      case FrameType::CLIENT_SETUP:
      case FrameType::SERVER_SETUP:
      case FrameType::MAX_SUBSCRIBE_ID:
      case FrameType::SUBSCRIBES_BLOCKED:
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

    virtual void onFetchHeader(SubscribeID subscribeID) = 0;
    virtual void onSubgroup(
        TrackAlias alias,
        uint64_t group,
        uint64_t subgroup,
        uint8_t priority) = 0;
    virtual void onObjectBegin(
        uint64_t group,
        uint64_t subgroup,
        uint64_t objectID,
        Extensions extensions,
        uint64_t length,
        Payload initialPayload,
        bool objectComplete,
        bool subgroupComplete) = 0;
    virtual void onObjectStatus(
        uint64_t group,
        uint64_t subgroup,
        uint64_t objectID,
        Priority pri,
        ObjectStatus status,
        Extensions extensions) = 0;
    virtual void onObjectPayload(Payload payload, bool objectComplete) = 0;
    virtual void onEndOfStream() = 0;
  };

  MoQObjectStreamCodec(ObjectCallback* callback) : callback_(callback) {}

  void setCallback(ObjectCallback* callback) {
    callback_ = callback;
  }

  void onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) override;

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
  StreamType streamType_{StreamType::SUBGROUP_HEADER};
  ObjectCallback* callback_;
};

} // namespace moxygen
