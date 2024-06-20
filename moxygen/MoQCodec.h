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
  class Callback {
   public:
    virtual ~Callback() = default;

    virtual void onFrame(FrameType /*frameType*/) {}
    virtual void onClientSetup(ClientSetup clientSetup) = 0;
    virtual void onServerSetup(ServerSetup serverSetup) = 0;
    virtual void onObjectHeader(ObjectHeader objectHeader) = 0;
    virtual void onObjectPayload(
        uint64_t subscribeID,
        uint64_t trackAlias,
        uint64_t groupID,
        uint64_t id,
        std::unique_ptr<folly::IOBuf> payload,
        bool eom) = 0;
    virtual void onSubscribe(SubscribeRequest subscribeRequest) = 0;
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
    virtual void onConnectionError(ErrorCode error) = 0;
  };
  enum class Direction { CLIENT, SERVER };
  MoQCodec(Direction dir, Callback* callback)
      : dir_(dir), callback_(callback) {}

  void setCallback(Callback* callback) {
    callback_ = callback;
  }

  void onIngress(std::unique_ptr<folly::IOBuf> data, bool eom);

 private:
  bool checkFrameAllowed(FrameType f) {
    switch (f) {
      case FrameType::OBJECT_STREAM:
      case FrameType::OBJECT_DATAGRAM:
      case FrameType::SUBSCRIBE:
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
      case FrameType::STREAM_HEADER_TRACK:
      case FrameType::STREAM_HEADER_GROUP:
        return true;
    }
    return false;
  }

  folly::Expected<folly::Unit, ErrorCode> parseFrame(folly::io::Cursor& cursor);

  uint64_t streamId_{std::numeric_limits<uint64_t>::max()};
  Direction dir_;
  Callback* callback_{nullptr};
  folly::IOBufQueue ingress_{folly::IOBufQueue::cacheChainLength()};

  enum class ParseState {
    FRAME_HEADER_TYPE,
    FRAME_PAYLOAD,
    MULTI_OBJECT_HEADER,
    OBJECT_PAYLOAD,
    OBJECT_PAYLOAD_NO_LENGTH
  };
  ParseState parseState_{ParseState::FRAME_HEADER_TYPE};
  FrameType curFrameType_;
  folly::Optional<ErrorCode> connError_;
  ObjectHeader curObjectHeader_;
};

} // namespace moxygen
