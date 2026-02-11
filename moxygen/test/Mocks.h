/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

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
  MOCK_METHOD(void, onRequestUpdate, (RequestUpdate requestUpdate));
  MOCK_METHOD(void, onSubscribeOk, (SubscribeOk subscribeOk));
  MOCK_METHOD(void, onRequestOk, (RequestOk reqOk, FrameType frameType));
  MOCK_METHOD(void, onRequestError, (RequestError error, FrameType frameType));
  MOCK_METHOD(void, onPublishDone, (PublishDone publishDone));
  MOCK_METHOD(void, onUnsubscribe, (Unsubscribe unsubscribe));
  MOCK_METHOD(void, onPublish, (PublishRequest publish));
  MOCK_METHOD(void, onPublishOk, (PublishOk publishOk));
  MOCK_METHOD(void, onMaxRequestID, (MaxRequestID maxSubId));
  MOCK_METHOD(void, onRequestsBlocked, (RequestsBlocked subscribesBlocked));
  MOCK_METHOD(void, onFetch, (Fetch fetch));
  MOCK_METHOD(void, onFetchCancel, (FetchCancel fetchCancel));
  MOCK_METHOD(void, onFetchOk, (FetchOk fetchOk));
  MOCK_METHOD(void, onPublishNamespace, (PublishNamespace publishNamespace));
  MOCK_METHOD(
      void,
      onPublishNamespaceDone,
      (PublishNamespaceDone publishNamespaceDone));
  MOCK_METHOD(
      void,
      onPublishNamespaceCancel,
      (PublishNamespaceCancel publishNamespaceCancel));
  MOCK_METHOD(
      void,
      onSubscribeNamespace,
      (SubscribeNamespace publishNamespace));

  MOCK_METHOD(
      void,
      onUnsubscribeNamespace,
      (UnsubscribeNamespace unsubscribeNamespace));
  MOCK_METHOD(void, onTrackStatus, (TrackStatus trackStatus));
  MOCK_METHOD(void, onTrackStatusOk, (TrackStatusOk trackStatusOk));
  MOCK_METHOD(void, onTrackStatusError, (TrackStatusError trackStatusError));
  MOCK_METHOD(void, onGoaway, (Goaway goaway));
  MOCK_METHOD(void, onConnectionError, (ErrorCode error));

  MOCK_METHOD(MoQCodec::ParseResult, onFetchHeader, (RequestID));
  MOCK_METHOD(
      MoQCodec::ParseResult,
      onSubgroup,
      (TrackAlias,
       uint64_t,
       uint64_t,
       std::optional<uint8_t>,
       const SubgroupOptions&));
  MOCK_METHOD(
      MoQCodec::ParseResult,
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
      MoQCodec::ParseResult,
      onObjectStatus,
      (uint64_t, uint64_t, uint64_t, std::optional<uint8_t>, ObjectStatus));
  MOCK_METHOD(MoQCodec::ParseResult, onObjectPayload, (Payload, bool));
  MOCK_METHOD(MoQCodec::ParseResult, onEndOfRange, (uint64_t, uint64_t, bool));
  MOCK_METHOD(void, onEndOfStream, ());
};

class MockTrackConsumer : public TrackConsumer {
 public:
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      setTrackAlias,
      (TrackAlias alias),
      (override));

  MOCK_METHOD(
      (folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>),
      beginSubgroup,
      (uint64_t groupID,
       uint64_t subgroupID,
       Priority priority,
       bool containsLastInGroup),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>),
      awaitStreamCredit,
      (),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      objectStream,
      (const ObjectHeader& header, Payload payload, bool lastInGroup),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      datagram,
      (const ObjectHeader& header, Payload payload, bool lastInGroup),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      publishDone,
      (PublishDone),
      (override));
};

class MockFetchConsumer : public FetchConsumer {
 public:
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      object,
      (uint64_t, uint64_t, uint64_t, Payload, Extensions, bool),
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
      endOfGroup,
      (uint64_t, uint64_t, uint64_t, bool),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfTrackAndGroup,
      (uint64_t, uint64_t, uint64_t),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfFetch,
      (),
      (override));

  MOCK_METHOD(void, reset, (ResetStreamErrorCode), (override));

  MOCK_METHOD(
      (folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>),
      awaitReadyToConsume,
      (),
      (override));

  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfUnknownRange,
      (uint64_t, uint64_t, bool),
      (override));
};

class MockSubgroupConsumer : public SubgroupConsumer {
 public:
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      object,
      (uint64_t, Payload, Extensions, bool),
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
      (uint64_t),
      (override));
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfTrackAndGroup,
      (uint64_t),
      (override));
  MOCK_METHOD(
      (folly::Expected<folly::Unit, MoQPublishError>),
      endOfSubgroup,
      (),
      (override));
  MOCK_METHOD(void, reset, (ResetStreamErrorCode), (override));
  MOCK_METHOD(
      (folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>),
      awaitReadyToConsume,
      (),
      (override));
};

class MockSubscriptionHandle : public SubscriptionHandle {
 public:
  explicit MockSubscriptionHandle(SubscribeOk ok)
      : SubscriptionHandle(std::move(ok)) {}

  MOCK_METHOD(void, unsubscribe, (), (override));

  // For async methods like requestUpdate, we can't use MOCK_METHOD directly
  // Instead, provide a delegating implementation
  folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
      RequestUpdate update) override {
    requestUpdateCalled(update);
    co_return requestUpdateResult();
  }

  // Mock these instead
  MOCK_METHOD(void, requestUpdateCalled, (RequestUpdate));
  MOCK_METHOD(
      (folly::Expected<RequestOk, RequestError>),
      requestUpdateResult,
      ());
};

class MockFetchHandle : public Publisher::FetchHandle {
 public:
  explicit MockFetchHandle(FetchOk ok)
      : Publisher::FetchHandle(std::move(ok)) {}

  MOCK_METHOD(void, fetchCancel, (), (override));

  folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
      RequestUpdate update) override {
    requestUpdateCalled(update);
    co_return requestUpdateResult();
  }
  MOCK_METHOD(void, requestUpdateCalled, (RequestUpdate));
  MOCK_METHOD(
      (folly::Expected<RequestOk, RequestError>),
      requestUpdateResult,
      ());
};

class MockSubscribeNamespaceHandle
    : public Publisher::SubscribeNamespaceHandle {
 public:
  MockSubscribeNamespaceHandle() = default;
  explicit MockSubscribeNamespaceHandle(SubscribeNamespaceOk ok)
      : Publisher::SubscribeNamespaceHandle(std::move(ok)) {}

  MOCK_METHOD(void, unsubscribeNamespace, (), (override));

  folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
      RequestUpdate update) override {
    requestUpdateCalled(update);
    co_return requestUpdateResult();
  }
  MOCK_METHOD(void, requestUpdateCalled, (RequestUpdate));
  MOCK_METHOD(
      (folly::Expected<RequestOk, RequestError>),
      requestUpdateResult,
      ());
};

class MockPublishNamespaceHandle : public Subscriber::PublishNamespaceHandle {
 public:
  MockPublishNamespaceHandle() = default;
  explicit MockPublishNamespaceHandle(PublishNamespaceOk ok)
      : Subscriber::PublishNamespaceHandle(std::move(ok)) {}

  MOCK_METHOD(void, publishNamespaceDone, (), (override));

  folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
      RequestUpdate update) override {
    requestUpdateCalled(update);
    co_return requestUpdateResult();
  }
  MOCK_METHOD(void, requestUpdateCalled, (RequestUpdate));
  MOCK_METHOD(
      (folly::Expected<RequestOk, RequestError>),
      requestUpdateResult,
      ());
};

class MockPublishNamespaceCallback
    : public Subscriber::PublishNamespaceCallback {
 public:
  MockPublishNamespaceCallback() : Subscriber::PublishNamespaceCallback() {}

  MOCK_METHOD(
      void,
      publishNamespaceCancel,
      (PublishNamespaceErrorCode, std::string),
      (override));
};

class MockPublisher : public Publisher {
 public:
  MOCK_METHOD(
      folly::coro::Task<TrackStatusResult>,
      trackStatus,
      (TrackStatus),
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
      folly::coro::Task<SubscribeNamespaceResult>,
      subscribeNamespace,
      (SubscribeNamespace, std::shared_ptr<NamespacePublishHandle>),
      (override));

  MOCK_METHOD(void, goaway, (Goaway), (override));
};

class MockSubscriber : public Subscriber {
 public:
  MOCK_METHOD(
      PublishResult,
      publish,
      (PublishRequest, std::shared_ptr<SubscriptionHandle>),
      (override));

  MOCK_METHOD(
      folly::coro::Task<PublishNamespaceResult>,
      publishNamespace,
      (PublishNamespace, std::shared_ptr<PublishNamespaceCallback>),
      (override));
};

class MockPublisherStats : public MoQPublisherStatsCallback {
 public:
  MockPublisherStats() = default;

  MOCK_METHOD(void, onSubscribeSuccess, (), (override));

  MOCK_METHOD(void, onSubscribeError, (SubscribeErrorCode), (override));

  MOCK_METHOD(void, onRequestUpdate, (), (override));

  MOCK_METHOD(void, onFetchSuccess, (), (override));

  MOCK_METHOD(void, onFetchError, (FetchErrorCode), (override));

  MOCK_METHOD(void, onPublishNamespaceSuccess, (), (override));

  MOCK_METHOD(
      void,
      onPublishNamespaceError,
      (PublishNamespaceErrorCode),
      (override));

  MOCK_METHOD(void, onPublishNamespaceDone, (), (override));

  MOCK_METHOD(void, onPublishNamespaceCancel, (), (override));

  MOCK_METHOD(void, onSubscribeNamespaceSuccess, (), (override));

  MOCK_METHOD(
      void,
      onSubscribeNamespaceError,
      (SubscribeNamespaceErrorCode),
      (override));

  MOCK_METHOD(void, onUnsubscribeNamespace, (), (override));

  MOCK_METHOD(void, onTrackStatus, (), (override));

  MOCK_METHOD(void, onUnsubscribe, (), (override));

  MOCK_METHOD(void, onPublishDone, (PublishDoneStatusCode), (override));

  MOCK_METHOD(void, onSubscriptionStreamOpened, (), (override));

  MOCK_METHOD(void, onSubscriptionStreamClosed, (), (override));

  MOCK_METHOD(void, recordPublishNamespaceLatency, (uint64_t), (override));

  MOCK_METHOD(void, recordPublishLatency, (uint64_t), (override));

  MOCK_METHOD(void, onPublishError, (PublishErrorCode), (override));

  MOCK_METHOD(void, onPublishSuccess, (), (override));
};

class MockSubscriberStats : public MoQSubscriberStatsCallback {
 public:
  MockSubscriberStats() = default;

  MOCK_METHOD(void, onSubscribeSuccess, (), (override));

  MOCK_METHOD(void, onSubscribeError, (SubscribeErrorCode), (override));

  MOCK_METHOD(void, onRequestUpdate, (), (override));

  MOCK_METHOD(void, onFetchSuccess, (), (override));

  MOCK_METHOD(void, onFetchError, (FetchErrorCode), (override));

  MOCK_METHOD(void, onPublishNamespaceSuccess, (), (override));

  MOCK_METHOD(
      void,
      onPublishNamespaceError,
      (PublishNamespaceErrorCode),
      (override));

  MOCK_METHOD(void, onPublishNamespaceDone, (), (override));

  MOCK_METHOD(void, onPublishNamespaceCancel, (), (override));

  MOCK_METHOD(void, onSubscribeNamespaceSuccess, (), (override));

  MOCK_METHOD(
      void,
      onSubscribeNamespaceError,
      (SubscribeNamespaceErrorCode),
      (override));

  MOCK_METHOD(void, onUnsubscribeNamespace, (), (override));

  MOCK_METHOD(void, onTrackStatus, (), (override));

  MOCK_METHOD(void, onUnsubscribe, (), (override));

  MOCK_METHOD(void, onPublishDone, (PublishDoneStatusCode), (override));

  MOCK_METHOD(void, onSubscriptionStreamOpened, (), (override));

  MOCK_METHOD(void, onSubscriptionStreamClosed, (), (override));

  MOCK_METHOD(void, recordSubscribeLatency, (uint64_t), (override));

  MOCK_METHOD(void, recordFetchLatency, (uint64_t), (override));

  MOCK_METHOD(void, onPublish, (), (override));

  MOCK_METHOD(void, onPublishOk, (), (override));

  MOCK_METHOD(void, onPublishError, (PublishErrorCode), (override));
};

} // namespace moxygen
