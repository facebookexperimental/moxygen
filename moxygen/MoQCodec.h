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
  void onIngressEnd(folly::io::Cursor& cursor, bool eom, Callback* callback);

  uint64_t streamId_{std::numeric_limits<uint64_t>::max()};
  folly::IOBufQueue ingress_{folly::IOBufQueue::cacheChainLength()};

  folly::Optional<ErrorCode> connError_;
  ObjectHeader curObjectHeader_;
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
    virtual void onAnnounce(Announce announce) = 0;
    virtual void onAnnounceOk(AnnounceOk announceOk) = 0;
    virtual void onAnnounceError(AnnounceError announceError) = 0;
    virtual void onUnannounce(Unannounce unannounce) = 0;
    virtual void onAnnounceCancel(AnnounceCancel announceCancel) = 0;
    virtual void onTrackStatusRequest(
        TrackStatusRequest trackStatusRequest) = 0;
    virtual void onTrackStatus(TrackStatus trackStatus) = 0;
    virtual void onGoaway(Goaway goaway) = 0;
  };

  enum class Direction { CLIENT, SERVER };
  MoQControlCodec(Direction dir, ControlCallback* callback)
      : callback_(callback) {}

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
      case FrameType::CLIENT_SETUP:
      case FrameType::SERVER_SETUP:
        return true;
    }
    return false;
  }

  folly::Expected<folly::Unit, ErrorCode> parseFrame(folly::io::Cursor& cursor);

  Direction dir_;
  ControlCallback* callback_{nullptr};
  FrameType curFrameType_;
  enum class ParseState {
    FRAME_HEADER_TYPE,
    FRAME_PAYLOAD,
  };
  ParseState parseState_{ParseState::FRAME_HEADER_TYPE};
};

class MoQObjectStreamCodec : public MoQCodec {
 public:
  class ObjectCallback : public Callback {
   public:
    ~ObjectCallback() override = default;

    virtual void onObjectHeader(ObjectHeader objectHeader) = 0;
    virtual void onObjectPayload(
        uint64_t subscribeID,
        uint64_t trackAlias,
        uint64_t groupID,
        uint64_t id,
        std::unique_ptr<folly::IOBuf> payload,
        bool eom) = 0;
  };

  MoQObjectStreamCodec(ObjectCallback* callback) : callback_(callback) {}

  void setCallback(ObjectCallback* callback) {
    callback_ = callback;
  }

  void onIngress(std::unique_ptr<folly::IOBuf> data, bool eom) override;

 private:
  enum class ParseState {
    STREAM_HEADER_TYPE,
    DATAGRAM,
    OBJECT_STREAM,
    MULTI_OBJECT_HEADER,
    OBJECT_PAYLOAD,
    // OBJECT_PAYLOAD_NO_LENGTH
  };
  ParseState parseState_{ParseState::STREAM_HEADER_TYPE};
  StreamType streamType_{StreamType::OBJECT_DATAGRAM};
  ObjectCallback* callback_;
};

} // namespace moxygen
