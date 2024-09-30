#pragma once

#include <folly/portability/GMock.h>
#include <moxygen/MoQCodec.h>

namespace moxygen {

class MockMoQCodecCallback : public MoQControlCodec::ControlCallback,
                             public MoQObjectStreamCodec::ObjectCallback {
 public:
  ~MockMoQCodecCallback() override = default;

  MOCK_METHOD(void, onFrame, (FrameType /*frameType*/));
  MOCK_METHOD(void, onClientSetup, (ClientSetup clientSetup));
  MOCK_METHOD(void, onServerSetup, (ServerSetup serverSetup));
  MOCK_METHOD(void, onObjectHeader, (ObjectHeader objectHeader));
  MOCK_METHOD(
      void,
      onObjectPayload,
      (uint64_t subscribeID,
       uint64_t trackAlias,
       uint64_t groupID,
       uint64_t id,
       std::unique_ptr<folly::IOBuf> payload,
       bool eom));
  MOCK_METHOD(void, onSubscribe, (SubscribeRequest subscribeRequest));
  MOCK_METHOD(void, onSubscribeUpdate, (SubscribeUpdate subscribeUpdate));
  MOCK_METHOD(void, onSubscribeOk, (SubscribeOk subscribeOk));
  MOCK_METHOD(void, onSubscribeError, (SubscribeError subscribeError));
  MOCK_METHOD(void, onSubscribeDone, (SubscribeDone subscribeDone));
  MOCK_METHOD(void, onUnsubscribe, (Unsubscribe unsubscribe));
  MOCK_METHOD(void, onAnnounce, (Announce announce));
  MOCK_METHOD(void, onAnnounceOk, (AnnounceOk announceOk));
  MOCK_METHOD(void, onAnnounceError, (AnnounceError announceError));
  MOCK_METHOD(void, onUnannounce, (Unannounce unannounce));
  MOCK_METHOD(void, onAnnounceCancel, (AnnounceCancel announceCancel));
  MOCK_METHOD(
      void,
      onTrackStatusRequest,
      (TrackStatusRequest trackStatusRequest));
  MOCK_METHOD(void, onTrackStatus, (TrackStatus trackStatus));
  MOCK_METHOD(void, onGoaway, (Goaway goaway));
  MOCK_METHOD(void, onConnectionError, (ErrorCode error));
};
} // namespace moxygen
