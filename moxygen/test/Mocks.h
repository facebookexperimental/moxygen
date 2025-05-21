#pragma once

#include <folly/portability/GMock.h>
#include <moxygen/MoQCodec.h>
#include <moxygen/MoQConsumers.h>
#include <moxygen/Publisher.h>
#include <moxygen/Subscriber.h>
#include <moxygen/stats/MoQStats.h>

namespace moxygen {

class MockMoQCodecCallback : public MoQControlCodec::ControlCallback,
                             public MoQObjectStreamCodec::ObjectCallback {
 public:
  ~MockMoQCodecCallback() override = default;

  MOCK_METHOD(void, onFrame, (FrameType /*frameType*/));
  MOCK_METHOD(void, onClientSetup, (ClientSetup clientSetup));
  MOCK_METHOD(void, onServerSetup, (ServerSetup serverSetup));
  MOCK_METHOD(void, onSubscribe, (SubscribeRequest subscribeRequest));
  MOCK_METHOD(void, onSubscribeUpdate, (SubscribeUpdate subscribeUpdate));
  MOCK_METHOD(void, onSubscribeOk, (SubscribeOk subscribeOk));
  MOCK_METHOD(void, onSubscribeError, (SubscribeError subscribeError));
  MOCK_METHOD(void, onSubscribeDone, (SubscribeDone subscribeDone));
  MOCK_METHOD(void, onUnsubscribe, (Unsubscribe unsubscribe));
  MOCK_METHOD(void, onMaxRequestID, (MaxRequestID maxSubId));
  MOCK_METHOD(void, onRequestsBlocked, (RequestsBlocked subscribesBlocked));
  MOCK_METHOD(void, onFetch, (Fetch fetch));
  MOCK_METHOD(void, onFetchCancel, (FetchCancel fetchCancel));
  MOCK_METHOD(void, onFetchOk, (FetchOk fetchOk));
  MOCK_METHOD(void, onFetchError, (FetchError fetchError));
  MOCK_METHOD(void, onAnnounce, (Announce announce));
  MOCK_METHOD(void, onAnnounceOk, (AnnounceOk announceOk));
  MOCK_METHOD(void, onAnnounceError, (AnnounceError announceError));
  MOCK_METHOD(void, onUnannounce, (Unannounce unannounce));
  MOCK_METHOD(void, onAnnounceCancel, (AnnounceCancel announceCancel));
  MOCK_METHOD(void, onSubscribeAnnounces, (SubscribeAnnounces announce));
  MOCK_METHOD(void, onSubscribeAnnouncesOk, (SubscribeAnnouncesOk announceOk));
  MOCK_METHOD(
      void,
      onSubscribeAnnouncesError,
      (SubscribeAnnouncesError announceError));
  MOCK_METHOD(
      void,
      onUnsubscribeAnnounces,
      (UnsubscribeAnnounces unsubscribeAnnounces));
  MOCK_METHOD(
      void,
      onTrackStatusRequest,
      (TrackStatusRequest trackStatusRequest));
  MOCK_METHOD(void, onTrackStatus, (TrackStatus trackStatus));
  MOCK_METHOD(void, onGoaway, (Goaway goaway));
  MOCK_METHOD(void, onConnectionError, (ErrorCode error));

  MOCK_METHOD(void, onFetchHeader, (RequestID));
  MOCK_METHOD(void, onSubgroup, (TrackAlias, uint64_t, uint64_t, uint8_t));
  MOCK_METHOD(
      void,
      onObjectBegin,
      (uint64_t,
       uint64_t,
       uint64_t,
       Extensions,
       uint64_t,
       Payload,
       bool,
       bool));
  MOCK_METHOD(
      void,
      onObjectStatus,
      (uint64_t, uint64_t, uint64_t, Priority, ObjectStatus, Extensions));
  MOCK_METHOD(void, onObjectPayload, (Payload, bool));
  MOCK_METHOD(void, onEndOfStream, ());
};

class MockTrackConsumer : public TrackConsumer {
 public:
  MOCK_METHOD(
      (folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>),
      beginSubgroup,
      (uint64_t groupID, uint64_t subgroupID, Priority priority),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>),
      awaitStreamCredit,
      (),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      objectStream,
      (const ObjectHeader& header, Payload payload),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      groupNotExists,
      (uint64_t groupID,
       uint64_t subgroupID,
       Priority priority,
       Extensions ext),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      datagram,
      (const ObjectHeader& header, Payload payload),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      subscribeDone,
      (SubscribeDone),
      (override));
};

class MockFetchConsumer : public FetchConsumer {
 public:
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      object,
      (uint64_t, uint64_t, uint64_t, Payload, Extensions, bool),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      objectNotExists,
      (uint64_t, uint64_t, uint64_t, Extensions, bool),
      (override));

  MOCK_METHOD(void, checkpoint, (), (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      beginObject,
      (uint64_t, uint64_t, uint64_t, uint64_t, Payload, Extensions),
      (override));

  MOCK_METHOD(
      (folly::Expected<ObjectPublishStatus, MoQPublishError>),
      objectPayload,
      (Payload, bool),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      groupNotExists,
      (uint64_t groupID, uint64_t subgroupID, Extensions ext, bool finFetch),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfGroup,
      (uint64_t, uint64_t, uint64_t, Extensions, bool),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfTrackAndGroup,
      (uint64_t, uint64_t, uint64_t, Extensions),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfFetch,
      (),
      (override));

  MOCK_METHOD(void, reset, (ResetStreamErrorCode), (override));

  MOCK_METHOD(
      (folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>),
      awaitReadyToConsume,
      (),
      (override));
};

class MockSubgroupConsumer : public SubgroupConsumer {
 public:
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      object,
      (uint64_t, Payload, Extensions, bool),
      (override));
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      objectNotExists,
      (uint64_t, Extensions, bool),
      (override));
  MOCK_METHOD(void, checkpoint, (), (override));
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      beginObject,
      (uint64_t, uint64_t, Payload, Extensions),
      (override));
  MOCK_METHOD(
      (folly::Expected<ObjectPublishStatus, MoQPublishError>),
      objectPayload,
      (Payload, bool),
      (override));
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfGroup,
      (uint64_t, Extensions),
      (override));
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfTrackAndGroup,
      (uint64_t, Extensions),
      (override));
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfSubgroup,
      (),
      (override));
  MOCK_METHOD(void, reset, (ResetStreamErrorCode), (override));
};

class MockSubscriptionHandle : public Publisher::SubscriptionHandle {
 public:
  explicit MockSubscriptionHandle(SubscribeOk ok)
      : SubscriptionHandle(std::move(ok)) {}

  MOCK_METHOD(void, unsubscribe, (), (override));
  MOCK_METHOD(void, subscribeUpdate, (SubscribeUpdate), (override));
};

class MockFetchHandle : public Publisher::FetchHandle {
 public:
  explicit MockFetchHandle(FetchOk ok)
      : Publisher::FetchHandle(std::move(ok)) {}

  MOCK_METHOD(void, fetchCancel, (), (override));
};

class MockSubscribeAnnouncesHandle
    : public Publisher::SubscribeAnnouncesHandle {
 public:
  MockSubscribeAnnouncesHandle() = default;
  explicit MockSubscribeAnnouncesHandle(SubscribeAnnouncesOk ok)
      : Publisher::SubscribeAnnouncesHandle(std::move(ok)) {}

  MOCK_METHOD(void, unsubscribeAnnounces, (), (override));
};

class MockAnnounceHandle : public Subscriber::AnnounceHandle {
 public:
  MockAnnounceHandle() = default;
  explicit MockAnnounceHandle(AnnounceOk ok)
      : Subscriber::AnnounceHandle(std::move(ok)) {}

  MOCK_METHOD(void, unannounce, (), (override));
};

class MockAnnounceCallback : public Subscriber::AnnounceCallback {
 public:
  MockAnnounceCallback() : Subscriber::AnnounceCallback() {}

  MOCK_METHOD(
      void,
      announceCancel,
      (AnnounceErrorCode, std::string),
      (override));
};

class MockPublisher : public Publisher {
 public:
  MOCK_METHOD(
      folly::coro::Task<TrackStatusResult>,
      trackStatus,
      (TrackStatusRequest),
      (override));

  MOCK_METHOD(
      folly::coro::Task<SubscribeResult>,
      subscribe,
      (SubscribeRequest, std::shared_ptr<TrackConsumer>),
      (override));

  MOCK_METHOD(
      folly::coro::Task<FetchResult>,
      fetch,
      (Fetch, std::shared_ptr<FetchConsumer>),
      (override));

  MOCK_METHOD(
      folly::coro::Task<SubscribeAnnouncesResult>,
      subscribeAnnounces,
      (SubscribeAnnounces),
      (override));
};

class MockSubscriber : public Subscriber {
 public:
  MOCK_METHOD(
      folly::coro::Task<AnnounceResult>,
      announce,
      (Announce, std::shared_ptr<AnnounceCallback>),
      (override));
};

class MockPublisherStats : public MoQPublisherStatsCallback {
 public:
  MockPublisherStats() = default;

  MOCK_METHOD(void, onSubscribeSuccess, (), (override));

  MOCK_METHOD(void, onSubscribeError, (SubscribeErrorCode), (override));

  MOCK_METHOD(void, onSubscribeUpdate, (), (override));

  MOCK_METHOD(void, onFetchSuccess, (), (override));

  MOCK_METHOD(void, onFetchError, (FetchErrorCode), (override));

  MOCK_METHOD(void, onAnnounceSuccess, (), (override));

  MOCK_METHOD(void, onAnnounceError, (AnnounceErrorCode), (override));

  MOCK_METHOD(void, onUnannounce, (), (override));

  MOCK_METHOD(void, onAnnounceCancel, (), (override));

  MOCK_METHOD(void, onSubscribeAnnouncesSuccess, (), (override));

  MOCK_METHOD(
      void,
      onSubscribeAnnouncesError,
      (SubscribeAnnouncesErrorCode),
      (override));

  MOCK_METHOD(void, onUnsubscribeAnnounces, (), (override));

  MOCK_METHOD(void, onTrackStatus, (), (override));

  MOCK_METHOD(void, onUnsubscribe, (), (override));

  MOCK_METHOD(void, onSubscribeDone, (SubscribeDoneStatusCode), (override));
};

class MockSubscriberStats : public MoQSubscriberStatsCallback {
 public:
  MockSubscriberStats() = default;

  MOCK_METHOD(void, onSubscribeSuccess, (), (override));

  MOCK_METHOD(void, onSubscribeError, (SubscribeErrorCode), (override));

  MOCK_METHOD(void, onSubscribeUpdate, (), (override));

  MOCK_METHOD(void, onFetchSuccess, (), (override));

  MOCK_METHOD(void, onFetchError, (FetchErrorCode), (override));

  MOCK_METHOD(void, onAnnounceSuccess, (), (override));

  MOCK_METHOD(void, onAnnounceError, (AnnounceErrorCode), (override));

  MOCK_METHOD(void, onUnannounce, (), (override));

  MOCK_METHOD(void, onAnnounceCancel, (), (override));

  MOCK_METHOD(void, onSubscribeAnnouncesSuccess, (), (override));

  MOCK_METHOD(
      void,
      onSubscribeAnnouncesError,
      (SubscribeAnnouncesErrorCode),
      (override));

  MOCK_METHOD(void, onUnsubscribeAnnounces, (), (override));

  MOCK_METHOD(void, onTrackStatus, (), (override));

  MOCK_METHOD(void, onUnsubscribe, (), (override));

  MOCK_METHOD(void, onSubscribeDone, (SubscribeDoneStatusCode), (override));
};

} // namespace moxygen
