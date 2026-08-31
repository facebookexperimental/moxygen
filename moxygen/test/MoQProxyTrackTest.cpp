/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <memory>
#include <optional>
#include <utility>

#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQProxyTrack.h"
#include "moxygen/relay/MoQForwarder.h"
#include "moxygen/test/MockMoQSession.h"
#include "moxygen/test/Mocks.h"

namespace moxygen { namespace {

using namespace testing;

const FullTrackName kTrackName{TrackNamespace{{"live"}}, "video"};

class TestUpstreamProvider final : public MoQUpstreamProvider {
 public:
  explicit TestUpstreamProvider(std::shared_ptr<MoQSession> session)
      : session_(std::move(session)) {}

  folly::coro::Task<MoQUpstreamSessionResult> getSession(
      const FullTrackName& fullTrackName,
      const TrackRequestParameters& params,
      bool hasFallbackProvider) override {
    ++calls;
    receivedTrackName = fullTrackName;
    receivedParams = params;
    receivedHasFallbackProvider = hasFallbackProvider;
    if (gate) {
      co_await *gate;
    }
    if (error) {
      co_return folly::makeUnexpected(MoQUpstreamProviderError{*error});
    }
    co_return session_;
  }

  size_t calls{0};
  std::optional<FullTrackName> receivedTrackName;
  std::optional<TrackRequestParameters> receivedParams;
  std::optional<bool> receivedHasFallbackProvider;
  std::optional<std::string> error;
  folly::coro::Baton* gate{nullptr};

 private:
  std::shared_ptr<MoQSession> session_;
};

class TestTrackCallback final : public MoQProxyTrack::Callback {
 public:
  void onNoSubscribers(MoQProxyTrack* track) override {
    ++calls;
    lastTrack = track;
  }

  size_t calls{0};
  MoQProxyTrack* lastTrack{nullptr};
};

folly::coro::Task<void> saveSubscribeResult(
    folly::coro::Task<Publisher::SubscribeResult> task,
    std::optional<Publisher::SubscribeResult>& result) {
  result = co_await std::move(task);
}

class MoQProxyTrackTest : public Test {
 protected:
  void SetUp() override {
    executor_ = std::make_shared<MoQFollyExecutorImpl>(&eventBase_);
    upstreamSession_ =
        std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
    ON_CALL(*upstreamSession_, getNegotiatedVersion())
        .WillByDefault(Return(kVersionDraftCurrent));
    provider_ = std::make_shared<TestUpstreamProvider>(upstreamSession_);
    track_ = MoQProxyTrack::create(kTrackName, {provider_});
    callback_ = std::make_shared<TestTrackCallback>();
    track_->setCallback(callback_);
  }

  std::shared_ptr<NiceMock<test::MockMoQSession>> makeDownstreamSession() {
    auto session = std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
    ON_CALL(*session, getNegotiatedVersion())
        .WillByDefault(Return(kVersionDraftCurrent));
    return session;
  }

  std::shared_ptr<NiceMock<MockTrackConsumer>> makeConsumer() {
    auto consumer = std::make_shared<NiceMock<MockTrackConsumer>>();
    ON_CALL(*consumer, setTrackAlias(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    ON_CALL(*consumer, publishDone(_))
        .WillByDefault(
            Return(folly::makeExpected<MoQPublishError>(folly::unit)));
    return consumer;
  }

  SubscribeRequest makeSubscribeRequest(RequestID requestID) {
    auto request = SubscribeRequest::make(kTrackName);
    request.requestID = requestID;
    request.priority = 7;
    request.groupOrder = GroupOrder::NewestFirst;
    return request;
  }

  std::shared_ptr<NiceMock<MockSubscriptionHandle>> makeUpstreamHandle() {
    SubscribeOk subscribeOk{
        .requestID = RequestID(100),
        .trackAlias = TrackAlias(9),
        .groupOrder = GroupOrder::OldestFirst,
        .largest = AbsoluteLocation{10, 2}};
    return std::make_shared<NiceMock<MockSubscriptionHandle>>(
        std::move(subscribeOk));
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_;
  std::shared_ptr<NiceMock<test::MockMoQSession>> upstreamSession_;
  std::shared_ptr<TestUpstreamProvider> provider_;
  std::shared_ptr<MoQProxyTrack> track_;
  std::shared_ptr<TestTrackCallback> callback_;
};

TEST_F(MoQProxyTrackTest, FirstSubscriberEstablishesUpstreamSubscription) {
  auto downstreamSession = makeDownstreamSession();
  auto consumer = makeConsumer();
  auto upstreamHandle = makeUpstreamHandle();
  SubscribeRequest upstreamRequest;
  std::shared_ptr<TrackConsumer> upstreamConsumer;
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [&](SubscribeRequest request, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            upstreamRequest = std::move(request);
            upstreamConsumer = std::move(cb);
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(1)), consumer, downstreamSession),
      &eventBase_);

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(provider_->calls, 1);
  EXPECT_EQ(provider_->receivedTrackName, kTrackName);
  EXPECT_EQ(provider_->receivedHasFallbackProvider, false);
  EXPECT_EQ(upstreamRequest.priority, kDefaultPriority);
  EXPECT_EQ(upstreamRequest.groupOrder, GroupOrder::Default);
  EXPECT_EQ(upstreamRequest.locType, LocationType::LargestObject);
  EXPECT_NE(std::dynamic_pointer_cast<MoQForwarder>(upstreamConsumer), nullptr);
  EXPECT_EQ(result.value()->subscribeOk().largest, AbsoluteLocation(10, 2));

  EXPECT_CALL(*upstreamHandle, unsubscribe());
  result.value()->unsubscribe();
  EXPECT_EQ(callback_->calls, 1);
  EXPECT_EQ(callback_->lastTrack, track_.get());
}

TEST_F(MoQProxyTrackTest, ConcurrentSubscribersShareUpstreamSubscription) {
  folly::coro::Baton providerGate;
  provider_->gate = &providerGate;
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));

  std::optional<Publisher::SubscribeResult> firstResult;
  std::optional<Publisher::SubscribeResult> secondResult;
  auto firstSession = makeDownstreamSession();
  auto secondSession = makeDownstreamSession();
  folly::coro::co_withExecutor(
      executor_.get(),
      saveSubscribeResult(
          track_->subscribe(
              makeSubscribeRequest(RequestID(1)), makeConsumer(), firstSession),
          firstResult))
      .start();
  eventBase_.loopOnce();
  folly::coro::co_withExecutor(
      executor_.get(),
      saveSubscribeResult(
          track_->subscribe(
              makeSubscribeRequest(RequestID(2)),
              makeConsumer(),
              secondSession),
          secondResult))
      .start();
  eventBase_.loopOnce();

  EXPECT_EQ(provider_->calls, 1);
  providerGate.post();
  eventBase_.loop();

  ASSERT_TRUE(firstResult.has_value());
  ASSERT_TRUE(firstResult->hasValue());
  ASSERT_TRUE(secondResult.has_value());
  ASSERT_TRUE(secondResult->hasValue());
  EXPECT_EQ(provider_->calls, 1);

  EXPECT_CALL(*upstreamHandle, unsubscribe());
  firstResult->value()->unsubscribe();
  EXPECT_EQ(callback_->calls, 0);
  secondResult->value()->unsubscribe();
  EXPECT_EQ(callback_->calls, 1);
}

TEST_F(MoQProxyTrackTest, ProviderFailureIsReturnedDownstream) {
  provider_->error = "no upstream available";

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(4)),
          makeConsumer(),
          makeDownstreamSession()),
      &eventBase_);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().requestID, RequestID(4));
  EXPECT_EQ(result.error().errorCode, SubscribeErrorCode::INTERNAL_ERROR);
  EXPECT_THAT(result.error().reasonPhrase, HasSubstr("no upstream available"));
  EXPECT_EQ(callback_->calls, 1);
}

TEST_F(MoQProxyTrackTest, ProviderFailureFallsBackToNextProvider) {
  auto primaryProvider =
      std::make_shared<TestUpstreamProvider>(upstreamSession_);
  primaryProvider->error = "primary unavailable";
  track_ = MoQProxyTrack::create(kTrackName, {primaryProvider, provider_});
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(5)),
          makeConsumer(),
          makeDownstreamSession()),
      &eventBase_);

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(primaryProvider->calls, 1);
  EXPECT_EQ(provider_->calls, 1);
  EXPECT_EQ(primaryProvider->receivedHasFallbackProvider, true);
  EXPECT_EQ(provider_->receivedHasFallbackProvider, false);
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  result.value()->unsubscribe();
}

TEST_F(MoQProxyTrackTest, ClosingDuringProviderAttemptDoesNotTryFallback) {
  folly::coro::Baton providerGate;
  provider_->gate = &providerGate;
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(upstreamSession_);
  track_ = MoQProxyTrack::create(kTrackName, {provider_, fallbackProvider});
  track_->setCallback(callback_);

  std::optional<Publisher::SubscribeResult> result;
  folly::coro::co_withExecutor(
      executor_.get(),
      saveSubscribeResult(
          track_->subscribe(
              makeSubscribeRequest(RequestID(11)),
              makeConsumer(),
              makeDownstreamSession()),
          result))
      .start();
  eventBase_.loopOnce();
  ASSERT_EQ(provider_->calls, 1);

  track_->close();
  providerGate.post();
  eventBase_.loop();

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->hasError());
  EXPECT_EQ(result->error().errorCode, SubscribeErrorCode::GOING_AWAY);
  EXPECT_EQ(fallbackProvider->calls, 0);
}

TEST_F(MoQProxyTrackTest, UpstreamSubscribeErrorIsReturnedDownstream) {
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return folly::makeUnexpected(
                SubscribeError{
                    RequestID(100),
                    SubscribeErrorCode::UNAUTHORIZED,
                    "denied"});
          }));

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(9)),
          makeConsumer(),
          makeDownstreamSession()),
      &eventBase_);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().requestID, RequestID(9));
  EXPECT_EQ(result.error().errorCode, SubscribeErrorCode::UNAUTHORIZED);
  EXPECT_THAT(result.error().reasonPhrase, HasSubstr("denied"));
  EXPECT_EQ(callback_->calls, 1);
}

TEST_F(MoQProxyTrackTest, ReleasingDownstreamHandleRemovesSubscriber) {
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));
  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(8)),
          makeConsumer(),
          makeDownstreamSession()),
      &eventBase_);
  ASSERT_TRUE(result.hasValue());

  EXPECT_CALL(*upstreamHandle, unsubscribe());
  auto downstreamHandle = std::move(result).value();
  downstreamHandle.reset();

  EXPECT_EQ(callback_->calls, 1);
}

TEST_F(MoQProxyTrackTest, UpstreamPublishDoneDrainsDownstreamSubscriber) {
  auto downstreamConsumer = makeConsumer();
  auto upstreamHandle = makeUpstreamHandle();
  std::shared_ptr<TrackConsumer> upstreamConsumer;
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [&](SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            upstreamConsumer = std::move(cb);
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));
  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(5)),
          downstreamConsumer,
          makeDownstreamSession()),
      &eventBase_);
  ASSERT_TRUE(result.hasValue());
  ASSERT_NE(upstreamConsumer, nullptr);

  EXPECT_CALL(*upstreamHandle, unsubscribe()).Times(0);
  EXPECT_CALL(*downstreamConsumer, publishDone(_));
  auto publishResult = upstreamConsumer->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::TRACK_ENDED, 0, "ended"});

  EXPECT_TRUE(publishResult.hasValue());
  EXPECT_EQ(callback_->calls, 1);
}

TEST_F(MoQProxyTrackTest, UpstreamSubscribeErrorFallsBackToNextProvider) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  ON_CALL(*fallbackSession, getNegotiatedVersion())
      .WillByDefault(Return(kVersionDraftCurrent));
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(fallbackSession);
  track_ = MoQProxyTrack::create(kTrackName, {provider_, fallbackProvider});

  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return folly::makeUnexpected(
                SubscribeError{
                    RequestID(100),
                    SubscribeErrorCode::UNAUTHORIZED,
                    "primary denied"});
          }));
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*fallbackSession, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(10)),
          makeConsumer(),
          makeDownstreamSession()),
      &eventBase_);

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(provider_->calls, 1);
  EXPECT_EQ(fallbackProvider->calls, 1);
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  result.value()->unsubscribe();
}

TEST_F(MoQProxyTrackTest, DownstreamRequestUpdateIsNotSupported) {
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));
  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(6)),
          makeConsumer(),
          makeDownstreamSession()),
      &eventBase_);
  ASSERT_TRUE(result.hasValue());

  RequestUpdate update;
  update.requestID = RequestID(7);
  update.existingRequestID = RequestID(6);
  auto updateResult = folly::coro::blockingWait(
      result.value()->requestUpdate(std::move(update)), &eventBase_);

  ASSERT_TRUE(updateResult.hasError());
  EXPECT_EQ(updateResult.error().requestID, RequestID(7));
  EXPECT_EQ(updateResult.error().errorCode, RequestErrorCode::NOT_SUPPORTED);
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  result.value()->unsubscribe();
  EXPECT_EQ(callback_->calls, 1);
}

}} // namespace moxygen
