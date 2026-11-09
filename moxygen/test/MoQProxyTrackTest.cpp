/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <folly/futures/ManualTimekeeper.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQProxyTrack.h"
#include "moxygen/relay/MoQForwarder.h"
#include "moxygen/test/MockMoQSession.h"
#include "moxygen/test/Mocks.h"

namespace moxygen { namespace {

using namespace testing;

const FullTrackName kTrackName{TrackNamespace{{"live"}}, "video"};
// Long enough to only end by cancellation in a passing test, short enough
// that a test whose wait isn't cancelled fails rather than hangs.
constexpr std::chrono::seconds kCancellationWait{5};

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
    if (waitForCancellation) {
      auto waited = co_await folly::coro::co_awaitTry(
          folly::coro::sleep(kCancellationWait));
      cancelled += waited.hasException();
      co_return folly::makeUnexpected(MoQUpstreamProviderError{"cancelled"});
    }
    if (error || calls <= failFirstCalls) {
      co_return folly::makeUnexpected(
          MoQUpstreamProviderError{error.value_or("unavailable")});
    }
    co_return session_;
  }

  size_t calls{0};
  std::optional<FullTrackName> receivedTrackName;
  std::optional<TrackRequestParameters> receivedParams;
  std::optional<bool> receivedHasFallbackProvider;
  std::optional<std::string> error;
  size_t failFirstCalls{0};
  folly::coro::Baton* gate{nullptr};
  // Waits until cancelled, counted in cancelled, or kCancellationWait, then
  // fails.
  bool waitForCancellation{false};
  size_t cancelled{0};

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
    track_ = MoQProxyTrack::create(
        kTrackName,
        {provider_},
        /*cacheConfig=*/{},
        MoQProxyTrack::UpstreamConfig{.switchStats = switchStats_});
    callback_ = std::make_shared<TestTrackCallback>();
    track_->setCallback(callback_);
  }

  // A real session supplies the id: the proxy rejects a subscribe whose
  // downstream id matches the upstream session's.
  MoQProxyTrack::DownstreamPeer makeDownstreamPeer() {
    auto peer = std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
    return MoQProxyTrack::DownstreamPeer{
        peer->sessionId(), kVersionDraftCurrent};
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

  std::shared_ptr<NiceMock<MockTrackConsumer>> makeConsumerWithSubgroups() {
    auto consumer = makeConsumer();
    ON_CALL(*consumer, beginSubgroup(_, _, _, _))
        .WillByDefault(Return(
            folly::makeExpected<MoQPublishError>(
                std::static_pointer_cast<SubgroupConsumer>(
                    std::make_shared<NiceMock<MockSubgroupConsumer>>()))));
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

  // Establishes the upstream for one subscriber; returns its downstream handle
  // and saves the upstream consumer, which stands in for the upstream session.
  std::shared_ptr<Publisher::SubscriptionHandle> subscribeThroughUpstream(
      const std::shared_ptr<NiceMock<MockSubscriptionHandle>>& upstreamHandle,
      const std::shared_ptr<TrackConsumer>& consumer) {
    EXPECT_CALL(*upstreamSession_, subscribe(_, _))
        .WillOnce(Invoke(
            [this, upstreamHandle](
                SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
                -> folly::coro::Task<Publisher::SubscribeResult> {
              upstreamConsumer_ = std::move(cb);
              co_return Publisher::SubscribeResult(upstreamHandle);
            }));
    auto result = folly::coro::blockingWait(
        track_->subscribe(
            makeSubscribeRequest(RequestID(1)), consumer, makeDownstreamPeer()),
        &eventBase_);
    EXPECT_TRUE(result.hasValue());
    return result.hasValue() ? result.value() : nullptr;
  }

  // Starts a subscribe whose upstream SUBSCRIBE stays in flight until
  // upstreamReply is posted, and saves the upstream consumer. The upstream
  // first opens subgroup 11/0, which the viewer may refuse or keep open.
  void startSubscribeWithUpstreamInFlight(
      folly::coro::Baton& upstreamReply,
      const std::shared_ptr<NiceMock<MockSubscriptionHandle>>& upstreamHandle,
      const std::shared_ptr<TrackConsumer>& consumer,
      std::shared_ptr<SubgroupConsumer>& openSubgroup,
      std::optional<Publisher::SubscribeResult>& result) {
    bool subscribing = false;
    EXPECT_CALL(*upstreamSession_, subscribe(_, _))
        .WillOnce(Invoke(
            [&upstreamReply,
             &openSubgroup,
             &subscribing,
             upstreamHandle,
             &upstreamConsumer = upstreamConsumer_](
                SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
                -> folly::coro::Task<Publisher::SubscribeResult> {
              subscribing = true;
              upstreamConsumer = cb;
              auto subgroup = cb->beginSubgroup(11, 0, kDefaultPriority, {});
              if (subgroup.hasValue()) {
                openSubgroup = subgroup.value();
              }
              co_await upstreamReply;
              co_return Publisher::SubscribeResult(upstreamHandle);
            }));
    folly::coro::co_withExecutor(
        executor_.get(),
        saveSubscribeResult(
            track_->subscribe(
                makeSubscribeRequest(RequestID(1)),
                consumer,
                makeDownstreamPeer()),
            result))
        .start();
    eventBase_.loopOnce();
    EXPECT_TRUE(subscribing);
  }

  // Delivers a session GOAWAY as if parsed from the upstream peer.
  void receiveSessionGoaway() {
    static_cast<MoQControlCodec::ControlCallback&>(*upstreamSession_)
        .onGoaway(Goaway{});
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_;
  std::shared_ptr<NiceMock<test::MockMoQSession>> upstreamSession_;
  std::shared_ptr<TestUpstreamProvider> provider_;
  std::shared_ptr<MoQProxySwitchStats> switchStats_{
      std::make_shared<MoQProxySwitchStats>()};
  std::shared_ptr<MoQProxyTrack> track_;
  std::shared_ptr<TestTrackCallback> callback_;
  std::shared_ptr<TrackConsumer> upstreamConsumer_;
};

TEST_F(MoQProxyTrackTest, FirstSubscriberEstablishesUpstreamSubscription) {
  auto downstreamPeer = makeDownstreamPeer();
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
          makeSubscribeRequest(RequestID(1)), consumer, downstreamPeer),
      &eventBase_);

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(provider_->calls, 1);
  EXPECT_EQ(provider_->receivedTrackName, kTrackName);
  EXPECT_EQ(provider_->receivedHasFallbackProvider, false);
  EXPECT_EQ(upstreamRequest.priority, kDefaultPriority);
  EXPECT_EQ(upstreamRequest.groupOrder, std::nullopt);
  EXPECT_EQ(upstreamRequest.filter, LocationFilter::nextObject());
  EXPECT_EQ(result.value()->subscribeOk().largest, AbsoluteLocation(10, 2));
  // The upstream subscription feeds the downstream subscriber.
  ASSERT_NE(upstreamConsumer, nullptr);
  EXPECT_CALL(*consumer, datagram(_, _, _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_TRUE(upstreamConsumer
                  ->datagram(
                      ObjectHeader(11, 0, 0, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasValue());

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
  auto firstPeer = makeDownstreamPeer();
  auto secondPeer = makeDownstreamPeer();
  folly::coro::co_withExecutor(
      executor_.get(),
      saveSubscribeResult(
          track_->subscribe(
              makeSubscribeRequest(RequestID(1)), makeConsumer(), firstPeer),
          firstResult))
      .start();
  eventBase_.loopOnce();
  folly::coro::co_withExecutor(
      executor_.get(),
      saveSubscribeResult(
          track_->subscribe(
              makeSubscribeRequest(RequestID(2)), makeConsumer(), secondPeer),
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
          makeDownstreamPeer()),
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
          makeDownstreamPeer()),
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
              makeDownstreamPeer()),
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

// The upstream ends its subscription before the track sees its SUBSCRIBE_OK.
TEST_F(MoQProxyTrackTest, UpstreamEndingWhileSubscribingFailsTheSubscribe) {
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            cb->publishDone(
                PublishDone{
                    RequestID(100),
                    PublishDoneStatusCode::TRACK_ENDED,
                    0,
                    "track ended"});
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));
  EXPECT_CALL(*upstreamHandle, unsubscribe());

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(1)),
          makeConsumer(),
          makeDownstreamPeer()),
      &eventBase_);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, SubscribeErrorCode::DOES_NOT_EXIST);
  EXPECT_EQ(callback_->calls, 1);
}

// As above, but an open subgroup keeps the viewer draining, so the forwarder
// does not empty.
TEST_F(
    MoQProxyTrackTest,
    UpstreamEndingWithAnOpenSubgroupWhileSubscribingFailsTheSubscribe) {
  auto upstreamHandle = makeUpstreamHandle();
  auto consumer = makeConsumer();
  ON_CALL(*consumer, beginSubgroup(_, _, _, _))
      .WillByDefault(Return(
          folly::makeExpected<MoQPublishError>(
              std::static_pointer_cast<SubgroupConsumer>(
                  std::make_shared<NiceMock<MockSubgroupConsumer>>()))));
  // Held so the subgroup stays open.
  std::shared_ptr<SubgroupConsumer> openSubgroup;
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [&openSubgroup, upstreamHandle](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            auto subgroup = cb->beginSubgroup(11, 0, kDefaultPriority, {});
            if (subgroup.hasValue()) {
              openSubgroup = subgroup.value();
            }
            cb->publishDone(
                PublishDone{
                    RequestID(100),
                    PublishDoneStatusCode::TRACK_ENDED,
                    0,
                    "track ended"});
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));
  EXPECT_CALL(*upstreamHandle, unsubscribe());

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(1)), consumer, makeDownstreamPeer()),
      &eventBase_);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, SubscribeErrorCode::DOES_NOT_EXIST);
}

// The only viewer fails before the track sees the upstream SUBSCRIBE_OK, so
// the forwarder empties.
TEST_F(
    MoQProxyTrackTest,
    LosingTheOnlyViewerWhileSubscribingFailsTheSubscribe) {
  auto upstreamHandle = makeUpstreamHandle();
  auto consumer = makeConsumer();
  ON_CALL(*consumer, beginSubgroup(_, _, _, _))
      .WillByDefault(Return(
          folly::makeUnexpected(MoQPublishError(
              MoQPublishError::CANCELLED, "viewer went away"))));
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            cb->beginSubgroup(11, 0, kDefaultPriority, {});
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));
  EXPECT_CALL(*upstreamHandle, unsubscribe());

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(1)), consumer, makeDownstreamPeer()),
      &eventBase_);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, SubscribeErrorCode::CANCELLED);
  EXPECT_EQ(callback_->calls, 1);
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
          makeDownstreamPeer()),
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
          makeDownstreamPeer()),
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
          makeDownstreamPeer()),
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
          makeDownstreamPeer()),
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
          makeDownstreamPeer()),
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

TEST_F(MoQProxyTrackTest, DestroyingTheTrackEndsViewersWithGoingAway) {
  auto consumer = makeConsumer();
  auto upstreamHandle = makeUpstreamHandle();
  auto downstreamHandle = subscribeThroughUpstream(upstreamHandle, consumer);
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)));

  track_.reset();
}

// A viewer still finishing a subgroup stays in the forwarder after
// PUBLISH_DONE, so later shutdown steps must not end it again.
class MoQProxyTrackViewerEndTest : public MoQProxyTrackTest {
 protected:
  // Subscribes a viewer and opens a subgroup to it.
  void subscribeWithAnOpenSubgroup() {
    consumer_ = makeConsumer();
    ON_CALL(*consumer_, beginSubgroup(_, _, _, _))
        .WillByDefault(Return(
            folly::makeExpected<MoQPublishError>(
                std::static_pointer_cast<SubgroupConsumer>(
                    std::make_shared<NiceMock<MockSubgroupConsumer>>()))));
    upstreamHandle_ = makeUpstreamHandle();
    downstreamHandle_ = subscribeThroughUpstream(upstreamHandle_, consumer_);
    ASSERT_NE(upstreamConsumer_, nullptr);
    auto subgroup = upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority);
    ASSERT_TRUE(subgroup.hasValue());
    openSubgroup_ = subgroup.value();
    EXPECT_CALL(*upstreamHandle_, unsubscribe()).Times(AtMost(1));
  }

  std::shared_ptr<NiceMock<MockTrackConsumer>> consumer_;
  std::shared_ptr<NiceMock<MockSubscriptionHandle>> upstreamHandle_;
  std::shared_ptr<Publisher::SubscriptionHandle> downstreamHandle_;
  std::shared_ptr<SubgroupConsumer> openSubgroup_;
};

TEST_F(MoQProxyTrackViewerEndTest, ClosingThenDestroyingEndsViewersOnce) {
  subscribeWithAnOpenSubgroup();
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .Times(1);

  track_->close();
  track_.reset();
}

TEST_F(
    MoQProxyTrackViewerEndTest,
    ShutdownAfterTheUpstreamEndedEndsNoOneAgain) {
  subscribeWithAnOpenSubgroup();
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::TRACK_ENDED)))
      .Times(1);
  upstreamConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::TRACK_ENDED, 0, "ended"});

  track_->close();
  track_.reset();
}

TEST_F(MoQProxyTrackTest, UpstreamGoawaysAreCountedWhileTheUpstreamServes) {
  auto upstreamHandle = makeUpstreamHandle();
  auto consumer = makeConsumer();
  auto downstreamHandle = subscribeThroughUpstream(upstreamHandle, consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  EXPECT_CALL(*upstreamHandle, unsubscribe()).Times(0);

  receiveSessionGoaway();
  upstreamConsumer_->goaway(Goaway{});

  EXPECT_EQ(switchStats_->upstreamGoaways, 2);
  EXPECT_CALL(*consumer, datagram(_, _, _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_TRUE(upstreamConsumer_
                  ->datagram(
                      ObjectHeader(11, 0, 0, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasValue());
  Mock::VerifyAndClearExpectations(upstreamHandle.get());
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  downstreamHandle->unsubscribe();
}

TEST_F(MoQProxyTrackTest, GoawayWhileTheUpstreamSubscribeIsInFlightIsCounted) {
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [this, upstreamHandle](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            upstreamConsumer_ = std::move(cb);
            receiveSessionGoaway();
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(1)),
          makeConsumer(),
          makeDownstreamPeer()),
      &eventBase_);

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(switchStats_->upstreamGoaways, 1);
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  result.value()->unsubscribe();
}

TEST_F(MoQProxyTrackTest, ClosingStopsTheWatchOfAnUpstreamSubscribeInFlight) {
  folly::coro::Baton upstreamReply;
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  std::shared_ptr<SubgroupConsumer> openSubgroup;
  std::optional<Publisher::SubscribeResult> result;
  startSubscribeWithUpstreamInFlight(
      upstreamReply,
      upstreamHandle,
      makeConsumerWithSubgroups(),
      openSubgroup,
      result);
  ASSERT_NE(openSubgroup, nullptr);

  // The viewer keeps draining its open subgroup, so the forwarder doesn't
  // empty.
  track_->close();
  receiveSessionGoaway();
  upstreamReply.post();
  eventBase_.loop();

  EXPECT_EQ(switchStats_->upstreamGoaways, 0);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->hasError());
}

TEST_F(
    MoQProxyTrackTest,
    UpstreamEndingStopsTheWatchOfAnUpstreamSubscribeInFlight) {
  folly::coro::Baton upstreamReply;
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  std::shared_ptr<SubgroupConsumer> openSubgroup;
  std::optional<Publisher::SubscribeResult> result;
  startSubscribeWithUpstreamInFlight(
      upstreamReply,
      upstreamHandle,
      makeConsumerWithSubgroups(),
      openSubgroup,
      result);
  ASSERT_NE(openSubgroup, nullptr);

  // The viewer keeps draining its open subgroup, so the forwarder doesn't
  // empty.
  upstreamConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::TRACK_ENDED, 0, "ended"});
  receiveSessionGoaway();
  upstreamReply.post();
  eventBase_.loop();

  EXPECT_EQ(switchStats_->upstreamGoaways, 0);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->hasError());
}

TEST_F(
    MoQProxyTrackTest,
    LosingTheOnlyViewerStopsTheWatchOfAnUpstreamSubscribeInFlight) {
  folly::coro::Baton upstreamReply;
  auto upstreamHandle = makeUpstreamHandle();
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  auto consumer = makeConsumer();
  ON_CALL(*consumer, beginSubgroup(_, _, _, _))
      .WillByDefault(Return(
          folly::makeUnexpected(MoQPublishError(
              MoQPublishError::CANCELLED, "viewer went away"))));
  std::shared_ptr<SubgroupConsumer> openSubgroup;
  std::optional<Publisher::SubscribeResult> result;
  startSubscribeWithUpstreamInFlight(
      upstreamReply, upstreamHandle, consumer, openSubgroup, result);

  receiveSessionGoaway();
  upstreamReply.post();
  eventBase_.loop();

  EXPECT_EQ(switchStats_->upstreamGoaways, 0);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->hasError());
}

TEST_F(MoQProxyTrackTest, GoawaysAfterTheUpstreamIsReleasedAreIgnored) {
  auto upstreamHandle = makeUpstreamHandle();
  auto downstreamHandle =
      subscribeThroughUpstream(upstreamHandle, makeConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  downstreamHandle->unsubscribe();

  receiveSessionGoaway();
  upstreamConsumer_->goaway(Goaway{});

  EXPECT_EQ(switchStats_->upstreamGoaways, 0);
}

// A viewer still finishing a subgroup keeps a closed track.
TEST_F(MoQProxyTrackTest, GoawaysAfterCloseAreIgnored) {
  auto consumer = makeConsumerWithSubgroups();
  auto upstreamHandle = makeUpstreamHandle();
  auto downstreamHandle = subscribeThroughUpstream(upstreamHandle, consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  auto subgroup = upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority);
  ASSERT_TRUE(subgroup.hasValue());
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  track_->close();
  ASSERT_EQ(callback_->calls, 0);

  receiveSessionGoaway();
  upstreamConsumer_->goaway(Goaway{});

  EXPECT_EQ(switchStats_->upstreamGoaways, 0);
}

// A viewer still finishing a subgroup keeps the track once its upstream ends.
TEST_F(MoQProxyTrackTest, GoawaysAfterTheUpstreamEndsAreIgnored) {
  auto consumer = makeConsumerWithSubgroups();
  auto upstreamHandle = makeUpstreamHandle();
  auto downstreamHandle = subscribeThroughUpstream(upstreamHandle, consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  auto subgroup = upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority);
  ASSERT_TRUE(subgroup.hasValue());
  upstreamConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::TRACK_ENDED, 0, "ended"});
  ASSERT_EQ(callback_->calls, 0);

  receiveSessionGoaway();

  EXPECT_EQ(switchStats_->upstreamGoaways, 0);
}

TEST_F(MoQProxyTrackTest, GoawaysOnTheSessionOfAFailedAttemptAreIgnored) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  track_ = MoQProxyTrack::create(
      kTrackName,
      {provider_, std::make_shared<TestUpstreamProvider>(fallbackSession)},
      {},
      MoQProxyTrack::UpstreamConfig{.switchStats = switchStats_});
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
          makeDownstreamPeer()),
      &eventBase_);
  ASSERT_TRUE(result.hasValue());

  receiveSessionGoaway();

  EXPECT_EQ(switchStats_->upstreamGoaways, 0);
  EXPECT_CALL(*upstreamHandle, unsubscribe());
  result.value()->unsubscribe();
}

// An upstream PUBLISH_DONE status that ends the track, rather than meaning the
// upstream was lost.
struct UpstreamEndCase {
  std::string name;
  PublishDoneStatusCode statusCode;
};

class MoQProxyTrackUpstreamEndTest
    : public MoQProxyTrackTest,
      public WithParamInterface<UpstreamEndCase> {};

TEST_P(MoQProxyTrackUpstreamEndTest, ViewersGetTheUpstreamPublishDone) {
  const auto& upstreamEnd = GetParam();
  auto consumer = makeConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  EXPECT_CALL(
      *consumer,
      publishDone(Field(&PublishDone::statusCode, upstreamEnd.statusCode)));

  upstreamConsumer_->publishDone(
      PublishDone{RequestID(100), upstreamEnd.statusCode, 0, "upstream done"});

  EXPECT_EQ(switchStats_->upstreamsLost, 0);
  EXPECT_EQ(switchStats_->lostSwitches.started, 0);
}

INSTANTIATE_TEST_SUITE_P(
    UpstreamPublishDone,
    MoQProxyTrackUpstreamEndTest,
    Values(
        UpstreamEndCase{"TrackEnded", PublishDoneStatusCode::TRACK_ENDED},
        UpstreamEndCase{
            "SubscriptionEnded",
            PublishDoneStatusCode::SUBSCRIPTION_ENDED},
        UpstreamEndCase{"Unauthorized", PublishDoneStatusCode::UNAUTHORIZED},
        UpstreamEndCase{"Expired", PublishDoneStatusCode::EXPIRED},
        UpstreamEndCase{
            "MalformedTrack",
            PublishDoneStatusCode::MALFORMED_TRACK},
        UpstreamEndCase{"TooFarBehind", PublishDoneStatusCode::TOO_FAR_BEHIND},
        UpstreamEndCase{"UpdateFailed", PublishDoneStatusCode::UPDATE_FAILED},
        // Another upstream might serve it, but every proxy on this upstream
        // switching at once would only move the load.
        UpstreamEndCase{"ExcessiveLoad", PublishDoneStatusCode::EXCESSIVE_LOAD},
        // Kept as a track end, as before: nothing says another relay would
        // fare better.
        UpstreamEndCase{
            "InternalError",
            PublishDoneStatusCode::INTERNAL_ERROR}),
    [](const TestParamInfo<UpstreamEndCase>& info) { return info.param.name; });

TEST_F(MoQProxyTrackTest, UpstreamLossAfterTheTrackClosedIsNotCounted) {
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  track_->close();

  upstreamConsumer_->publishDone(
      PublishDone{
          RequestID(100),
          PublishDoneStatusCode::SESSION_CLOSED,
          0,
          "session closed"});

  EXPECT_EQ(switchStats_->upstreamsLost, 0);
}

// A track with a second provider on its own session, and switch timers short
// enough for a test.
class MoQProxyTrackSwitchTest : public MoQProxyTrackTest {
 protected:
  void SetUp() override {
    MoQProxyTrackTest::SetUp();
    replacementSession_ = makeSession();
    replacementProvider_ =
        std::make_shared<TestUpstreamProvider>(replacementSession_);
    makeTrack({provider_, replacementProvider_});
  }

  // A switch still running holds the track until it stops. On manual time,
  // a timer a broken switch left running still fires, so the test fails
  // rather than hangs.
  void TearDown() override {
    if (track_) {
      track_->close();
    }
    if (timekeeper_) {
      timekeeper_->advance(std::chrono::hours(1));
    }
    std::weak_ptr<MoQProxyTrack> track = std::exchange(track_, nullptr);
    runUntil([&] { return track.expired(); });
  }

  void makeTrack(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> providers,
      std::chrono::milliseconds retryBackoff = std::chrono::milliseconds(10),
      std::shared_ptr<folly::Timekeeper> timekeeper = nullptr) {
    track_ = MoQProxyTrack::create(
        kTrackName,
        std::move(providers),
        /*cacheConfig=*/{},
        MoQProxyTrack::UpstreamConfig{
            .switchStats = switchStats_,
            .switchOptions =
                {.deadline = kDeadline,
                 .retryBackoff = retryBackoff,
                 .maxRetryBackoff = 2 * retryBackoff},
            .timekeeper = std::move(timekeeper)});
    track_->setCallback(callback_);
  }

  // A track whose switch timers run on timekeeper_.
  void makeTrackOnManualTime(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> providers) {
    timekeeper_ = std::make_shared<folly::ManualTimekeeper>();
    makeTrack(std::move(providers), std::chrono::milliseconds(10), timekeeper_);
  }

  // Moves timekeeper_ forward in steps, running what each step wakes.
  void advanceTime(std::chrono::milliseconds duration) {
    constexpr std::chrono::milliseconds kStep{5};
    while (duration.count() > 0) {
      runPending();
      const auto step = std::min(kStep, duration);
      timekeeper_->advance(step);
      duration -= step;
    }
    runPending();
  }

  void runPending() {
    for (int i = 0; i < 20; ++i) {
      eventBase_.loopOnce(EVLOOP_NONBLOCK);
    }
  }

  // Runs the event base until done() holds, or for at most two seconds.
  template <typename Done>
  void runUntil(Done done) {
    const auto giveUp =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!done() && std::chrono::steady_clock::now() < giveUp) {
      eventBase_.loopOnce(EVLOOP_NONBLOCK);
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  void runFor(std::chrono::milliseconds duration) {
    const auto end = std::chrono::steady_clock::now() + duration;
    runUntil([&] { return std::chrono::steady_clock::now() >= end; });
  }

  // Whether the track adopted session's upstream within two seconds.
  [[nodiscard]] bool runUntilAdopted(
      const std::shared_ptr<MoQSession>& session) {
    runUntil([&] { return track_->upstreamSession() == session; });
    return track_->upstreamSession() == session;
  }

  std::shared_ptr<NiceMock<test::MockMoQSession>> makeSession() {
    auto session = std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
    ON_CALL(*session, getNegotiatedVersion())
        .WillByDefault(Return(kVersionDraftCurrent));
    return session;
  }

  // The replacement session accepts the next SUBSCRIBE; saves its request
  // and consumer.
  void expectReplacementSubscribe() {
    EXPECT_CALL(*replacementSession_, subscribe(_, _))
        .WillOnce(Invoke(
            [this](SubscribeRequest request, std::shared_ptr<TrackConsumer> cb)
                -> folly::coro::Task<Publisher::SubscribeResult> {
              replacementRequest_ = std::move(request);
              replacementConsumer_ = std::move(cb);
              co_return Publisher::SubscribeResult(makeUpstreamHandle());
            }));
  }

  void loseUpstream(
      PublishDoneStatusCode statusCode = PublishDoneStatusCode::GOING_AWAY) {
    upstreamConsumer_->publishDone(
        PublishDone{RequestID(100), statusCode, 0, "upstream lost"});
  }

  // The upstream opens the group and sends its first object.
  [[nodiscard]] static bool sendFirstObject(
      TrackConsumer& upstream,
      uint64_t group) {
    auto subgroup = upstream.beginSubgroup(group, 0, kDefaultPriority);
    return subgroup.hasValue() &&
        subgroup.value()
            ->object(0, folly::IOBuf::copyBuffer("x"), noExtensions(), false)
            .hasValue();
  }

  // A viewer whose stream for each group it is sent is saved.
  std::shared_ptr<NiceMock<MockTrackConsumer>> makeRecordingConsumer() {
    auto consumer = makeConsumer();
    ON_CALL(*consumer, beginSubgroup(_, _, _, _))
        .WillByDefault(Invoke([this](
                                  uint64_t group,
                                  uint64_t,
                                  Priority,
                                  TrackConsumer::BeginSubgroupOptions) {
          auto subgroup = std::make_shared<NiceMock<MockSubgroupConsumer>>();
          ON_CALL(*subgroup, object(_, _, _, _))
              .WillByDefault(
                  Return(folly::makeExpected<MoQPublishError>(folly::unit)));
          viewerSubgroups_[group] = subgroup;
          return folly::makeExpected<MoQPublishError>(
              std::static_pointer_cast<SubgroupConsumer>(subgroup));
        }));
    return consumer;
  }

  static constexpr std::chrono::milliseconds kDeadline{300};

  std::shared_ptr<folly::ManualTimekeeper> timekeeper_;
  std::shared_ptr<NiceMock<test::MockMoQSession>> replacementSession_;
  std::shared_ptr<TestUpstreamProvider> replacementProvider_;
  std::optional<SubscribeRequest> replacementRequest_;
  std::shared_ptr<TrackConsumer> replacementConsumer_;
  std::map<uint64_t, std::shared_ptr<NiceMock<MockSubgroupConsumer>>>
      viewerSubgroups_;
};

class MoQProxyTrackLostUpstreamTest
    : public MoQProxyTrackSwitchTest,
      public WithParamInterface<PublishDoneStatusCode> {};

// The upstream's SUBSCRIBE_OK said 10/2 and it opened group 11 before it was
// lost: the replacement, on the next provider since the lost session is
// refused, starts at its next group, and only groups after 11 reach the
// viewer. The switch completes with the first object of them, not the stream
// header before it.
TEST_P(MoQProxyTrackLostUpstreamTest, ViewersResumeFromAReplacement) {
  auto consumer = makeRecordingConsumer();
  EXPECT_CALL(*consumer, publishDone(_)).Times(0);
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  ASSERT_TRUE(upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority));
  expectReplacementSubscribe();

  loseUpstream(GetParam());
  ASSERT_TRUE(runUntilAdopted(replacementSession_));

  EXPECT_EQ(switchStats_->upstreamsLost, 1);
  EXPECT_EQ(switchStats_->lostSwitches.started, 1);
  EXPECT_EQ(provider_->calls, 2);
  ASSERT_TRUE(replacementRequest_.has_value());
  EXPECT_EQ(replacementRequest_->filter, LocationFilter::nextObject());
  ASSERT_NE(replacementConsumer_, nullptr);
  EXPECT_TRUE(
      replacementConsumer_->beginSubgroup(11, 0, kDefaultPriority).hasError());
  auto subgroup = replacementConsumer_->beginSubgroup(12, 0, kDefaultPriority);
  ASSERT_TRUE(subgroup.hasValue());
  EXPECT_EQ(switchStats_->lostSwitches.completed, 0);
  EXPECT_EQ(switchStats_->lostGaps.gaps, 0);
  EXPECT_TRUE(subgroup.value()->object(
      0, folly::IOBuf::copyBuffer("x"), noExtensions(), false));
  EXPECT_EQ(switchStats_->lostSwitches.completed, 1);
  EXPECT_EQ(switchStats_->refusedStreams, 1);
  EXPECT_TRUE(viewerSubgroups_.contains(12));
  EXPECT_EQ(switchStats_->lostGaps.gaps, 1);
  EXPECT_EQ(switchStats_->lostGaps.groups, 1);
  EXPECT_EQ(switchStats_->lateReplacementGaps.gaps, 0);
  Mock::VerifyAndClearExpectations(consumer.get());
}

INSTANTIATE_TEST_SUITE_P(
    LostUpstream,
    MoQProxyTrackLostUpstreamTest,
    Values(
        PublishDoneStatusCode::GOING_AWAY,
        PublishDoneStatusCode::SESSION_CLOSED),
    [](const TestParamInfo<PublishDoneStatusCode>& info) {
      return info.param == PublishDoneStatusCode::GOING_AWAY ? "GoingAway"
                                                             : "SessionClosed";
    });

// The lost upstream ended, so the replacement owns every group after its
// last, in whatever order they arrive.
TEST_F(MoQProxyTrackSwitchTest, TheReplacementOwnsEveryLaterGroupInAnyOrder) {
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeRecordingConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  ASSERT_TRUE(upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority));
  expectReplacementSubscribe();
  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_NE(replacementConsumer_, nullptr);

  EXPECT_TRUE(replacementConsumer_->beginSubgroup(13, 0, kDefaultPriority));
  EXPECT_TRUE(replacementConsumer_->beginSubgroup(12, 0, kDefaultPriority));
  EXPECT_TRUE(
      replacementConsumer_->beginSubgroup(11, 0, kDefaultPriority).hasError());

  EXPECT_TRUE(viewerSubgroups_.contains(12));
  EXPECT_TRUE(viewerSubgroups_.contains(13));
  EXPECT_EQ(switchStats_->refusedStreams, 1);
}

// The lost upstream finishes subgroup 11/0 after the replacement took over,
// but can't open another.
TEST_F(MoQProxyTrackSwitchTest, TheLostUpstreamsOpenSubgroupsFinish) {
  auto consumer = makeRecordingConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  auto subgroup = upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority);
  ASSERT_TRUE(subgroup.hasValue());
  ASSERT_TRUE(viewerSubgroups_.contains(11));
  auto lostSubgroup = viewerSubgroups_.at(11);
  expectReplacementSubscribe();

  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_TRUE(sendFirstObject(*replacementConsumer_, 12));
  ASSERT_EQ(switchStats_->lostSwitches.completed, 1);

  EXPECT_CALL(*lostSubgroup, object(1, _, _, true));
  EXPECT_TRUE(subgroup.value()->object(
      1, folly::IOBuf::copyBuffer("x"), noExtensions(), /*finSubgroup=*/true));
  EXPECT_TRUE(
      upstreamConsumer_->beginSubgroup(13, 0, kDefaultPriority).hasError());
  EXPECT_FALSE(viewerSubgroups_.contains(13));
}

TEST_F(
    MoQProxyTrackSwitchTest,
    AnUpstreamLostAfterAGoawayLeavesALateReplacementGap) {
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeRecordingConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();

  receiveSessionGoaway();
  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_NE(replacementConsumer_, nullptr);
  ASSERT_TRUE(sendFirstObject(*replacementConsumer_, 12));

  EXPECT_EQ(switchStats_->lateReplacementGaps.gaps, 1);
  // From the SUBSCRIBE_OK's group 10 to 12.
  EXPECT_EQ(switchStats_->lateReplacementGaps.groups, 2);
  EXPECT_EQ(switchStats_->lostGaps.gaps, 0);
}

// The provider keeps returning the lost upstream's session.
TEST_F(
    MoQProxyTrackSwitchTest,
    WithNoReplacementViewersGetGoingAwayAtTheDeadline) {
  makeTrackOnManualTime({provider_});
  auto consumer = makeConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  EXPECT_CALL(*consumer, publishDone(_)).Times(0);

  loseUpstream();
  advanceTime(kDeadline - std::chrono::milliseconds(1));
  Mock::VerifyAndClearExpectations(consumer.get());
  const auto attempts = provider_->calls;
  EXPECT_GT(attempts, 3);
  EXPECT_EQ(switchStats_->lostSwitches.failed, 0);
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  advanceTime(std::chrono::milliseconds(1));

  EXPECT_EQ(switchStats_->lostSwitches.failed, 1);
  advanceTime(std::chrono::milliseconds(50));
  EXPECT_EQ(provider_->calls, attempts);
  EXPECT_EQ(switchStats_->lostSwitches.completed, 0);
}

// The replacement answered the SUBSCRIBE and opened a stream, but never
// forwarded an object.
TEST_F(
    MoQProxyTrackSwitchTest,
    AReplacementThatForwardsNoObjectIsDroppedAtTheDeadline) {
  makeTrackOnManualTime({provider_, replacementProvider_});
  auto consumer = makeRecordingConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  auto replacementHandle = makeUpstreamHandle();
  EXPECT_CALL(*replacementSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [this, replacementHandle](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            replacementConsumer_ = std::move(cb);
            co_return Publisher::SubscribeResult(replacementHandle);
          }));
  loseUpstream();
  runPending();
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);
  ASSERT_TRUE(replacementConsumer_->beginSubgroup(12, 0, kDefaultPriority));
  EXPECT_CALL(*replacementHandle, unsubscribe());
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  advanceTime(kDeadline);

  EXPECT_EQ(switchStats_->lostSwitches.failed, 1);
  EXPECT_EQ(switchStats_->lostSwitches.completed, 0);
  EXPECT_TRUE(
      replacementConsumer_->beginSubgroup(13, 0, kDefaultPriority).hasError());
}

TEST_F(MoQProxyTrackSwitchTest, ASwitchThatCompletedOutlivesItsDeadline) {
  makeTrackOnManualTime({provider_, replacementProvider_});
  auto consumer = makeRecordingConsumer();
  EXPECT_CALL(*consumer, publishDone(_)).Times(0);
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  loseUpstream();
  advanceTime(kDeadline - std::chrono::milliseconds(1));
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);
  ASSERT_TRUE(sendFirstObject(*replacementConsumer_, 12));

  advanceTime(kDeadline);

  EXPECT_EQ(switchStats_->lostSwitches.completed, 1);
  EXPECT_EQ(switchStats_->lostSwitches.failed, 0);
  EXPECT_TRUE(replacementConsumer_->beginSubgroup(13, 0, kDefaultPriority));
  EXPECT_TRUE(viewerSubgroups_.contains(13));
  Mock::VerifyAndClearExpectations(consumer.get());
}

// The replacement the track adopted goes away before it forwards anything:
// the same switch goes on to a third upstream, against the deadline from the
// first loss.
TEST_F(
    MoQProxyTrackSwitchTest,
    AReplacementLostBeforeItForwardsKeepsTheSwitchAndItsDeadline) {
  auto thirdSession = makeSession();
  auto thirdProvider = std::make_shared<TestUpstreamProvider>(thirdSession);
  makeTrackOnManualTime({provider_, replacementProvider_, thirdProvider});
  auto consumer = makeConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  EXPECT_CALL(*thirdSession, subscribe(_, _))
      .WillOnce(Invoke(
          [this](SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            replacementConsumer_ = std::move(cb);
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));
  loseUpstream();
  advanceTime(std::chrono::milliseconds(200));
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);

  replacementConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::GOING_AWAY, 0, "going away"});
  runPending();
  ASSERT_EQ(track_->upstreamSession(), thirdSession);
  EXPECT_EQ(switchStats_->lostSwitches.started, 1);
  advanceTime(kDeadline - std::chrono::milliseconds(201));
  EXPECT_EQ(switchStats_->lostSwitches.failed, 0);
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  advanceTime(std::chrono::milliseconds(1));

  EXPECT_EQ(switchStats_->lostSwitches.failed, 1);
  EXPECT_EQ(provider_->calls, 3);
  EXPECT_EQ(replacementProvider_->calls, 2);
}

// The upstream went away after a GOAWAY, and its replacement went away
// without one before forwarding anything: the gap viewers saw is still the
// late replacement's.
TEST_F(MoQProxyTrackSwitchTest, AGapKeepsTheCauseOfTheLossThatOpenedIt) {
  auto thirdSession = makeSession();
  auto thirdProvider = std::make_shared<TestUpstreamProvider>(thirdSession);
  makeTrack({provider_, replacementProvider_, thirdProvider});
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeRecordingConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  std::shared_ptr<TrackConsumer> thirdConsumer;
  EXPECT_CALL(*thirdSession, subscribe(_, _))
      .WillOnce(Invoke(
          [this, &thirdConsumer](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            thirdConsumer = std::move(cb);
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));
  receiveSessionGoaway();
  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));

  replacementConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::GOING_AWAY, 0, "going away"});
  ASSERT_TRUE(runUntilAdopted(thirdSession));
  ASSERT_TRUE(sendFirstObject(*thirdConsumer, 12));

  EXPECT_EQ(switchStats_->lateReplacementGaps.gaps, 1);
  EXPECT_EQ(switchStats_->lostGaps.gaps, 0);
  EXPECT_EQ(switchStats_->lostSwitches.started, 1);
  EXPECT_EQ(switchStats_->lostSwitches.completed, 1);
}

TEST_F(MoQProxyTrackSwitchTest, ClosingDuringASwitchStopsIt) {
  folly::coro::Baton providerGate;
  replacementProvider_->gate = &providerGate;
  auto consumer = makeConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  EXPECT_CALL(*replacementSession_, subscribe(_, _)).Times(0);
  loseUpstream();
  runUntil([&] { return replacementProvider_->calls == 1; });
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)));

  track_->close();
  providerGate.post();
  std::weak_ptr<MoQProxyTrack> track = std::exchange(track_, nullptr);
  runUntil([&] { return track.expired(); });

  EXPECT_TRUE(track.expired());
  EXPECT_EQ(switchStats_->lostSwitches.completed, 0);
  EXPECT_EQ(switchStats_->lostSwitches.failed, 0);
}

// Every provider failed, so the switch waits out its backoff when the track
// closes.
TEST_F(MoQProxyTrackSwitchTest, ClosingDuringTheRetryBackoffStopsTheSwitch) {
  makeTrack({provider_}, /*retryBackoff=*/std::chrono::milliseconds(100));
  auto consumer = makeConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  loseUpstream();
  runUntil([&] { return provider_->calls == 2; });
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)));

  track_->close();
  std::weak_ptr<MoQProxyTrack> track = std::exchange(track_, nullptr);
  runUntil([&] { return track.expired(); });

  EXPECT_TRUE(track.expired());
  EXPECT_EQ(provider_->calls, 2);
  EXPECT_EQ(switchStats_->lostSwitches.failed, 0);
}

enum class AttemptWaitsIn { Provider, Subscribe };
enum class SwitchStops { Close, Deadline };

class MoQProxyTrackSwitchStopTest
    : public MoQProxyTrackSwitchTest,
      public WithParamInterface<std::tuple<AttemptWaitsIn, SwitchStops>> {};

// The replacement's provider, or its SUBSCRIBE, waits until it is cancelled.
// A SUBSCRIBE answered once the track stopped waiting is unsubscribed.
TEST_P(MoQProxyTrackSwitchStopTest, StoppingTheSwitchCancelsTheAttempt) {
  const auto [waitsIn, stops] = GetParam();
  makeTrackOnManualTime({provider_, replacementProvider_});
  auto consumer = makeConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  auto subscribeCancelled = std::make_shared<bool>(false);
  auto lateHandle = makeUpstreamHandle();
  if (waitsIn == AttemptWaitsIn::Provider) {
    replacementProvider_->waitForCancellation = true;
    EXPECT_CALL(*replacementSession_, subscribe(_, _)).Times(0);
  } else {
    EXPECT_CALL(*replacementSession_, subscribe(_, _))
        .WillOnce(Invoke(
            [subscribeCancelled, lateHandle](
                SubscribeRequest, std::shared_ptr<TrackConsumer>)
                -> folly::coro::Task<Publisher::SubscribeResult> {
              auto waited = co_await folly::coro::co_awaitTry(
                  folly::coro::sleep(kCancellationWait));
              *subscribeCancelled = waited.hasException();
              co_return Publisher::SubscribeResult(lateHandle);
            }));
    EXPECT_CALL(*lateHandle, unsubscribe());
  }
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)));
  loseUpstream();
  runPending();
  ASSERT_EQ(replacementProvider_->calls, 1);

  if (stops == SwitchStops::Close) {
    track_->close();
  } else {
    advanceTime(kDeadline);
  }
  runUntil([&] {
    return replacementProvider_->cancelled == 1 || *subscribeCancelled;
  });

  EXPECT_TRUE(replacementProvider_->cancelled == 1 || *subscribeCancelled);
  EXPECT_EQ(
      switchStats_->lostSwitches.failed,
      stops == SwitchStops::Deadline ? 1 : 0);
}

INSTANTIATE_TEST_SUITE_P(
    SwitchStop,
    MoQProxyTrackSwitchStopTest,
    Combine(
        Values(AttemptWaitsIn::Provider, AttemptWaitsIn::Subscribe),
        Values(SwitchStops::Close, SwitchStops::Deadline)),
    [](const TestParamInfo<std::tuple<AttemptWaitsIn, SwitchStops>>& info) {
      return std::string(
                 std::get<0>(info.param) == AttemptWaitsIn::Provider
                     ? "Provider"
                     : "Subscribe") +
          (std::get<1>(info.param) == SwitchStops::Close ? "Close"
                                                         : "Deadline");
    });

// The replacement session went away before the track adopted it, so the next
// pass doesn't try it again.
TEST_F(MoQProxyTrackSwitchTest, AReplacementLostBeforeItIsAdoptedIsNotRetried) {
  auto thirdSession = makeSession();
  auto thirdProvider = std::make_shared<TestUpstreamProvider>(thirdSession);
  thirdProvider->failFirstCalls = 1;
  makeTrack({provider_, replacementProvider_, thirdProvider});
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  EXPECT_CALL(*replacementSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [this](SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            cb->publishDone(
                PublishDone{
                    RequestID(100),
                    PublishDoneStatusCode::SESSION_CLOSED,
                    0,
                    "closed"});
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));
  EXPECT_CALL(*thirdSession, subscribe(_, _))
      .WillOnce(Invoke(
          [this](SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            replacementConsumer_ = std::move(cb);
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));

  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(thirdSession));

  EXPECT_EQ(replacementProvider_->calls, 2);
  EXPECT_EQ(thirdProvider->calls, 2);
  EXPECT_EQ(switchStats_->upstreamsLost, 2);
  EXPECT_EQ(switchStats_->lostSwitches.started, 1);
}

TEST_F(
    MoQProxyTrackSwitchTest,
    AViewerSubscribingDuringASwitchGetsTheReplacementsGroups) {
  folly::coro::Baton providerGate;
  replacementProvider_->gate = &providerGate;
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeRecordingConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  loseUpstream();
  runUntil([&] { return replacementProvider_->calls == 1; });
  auto lateViewer = makeConsumerWithSubgroups();
  EXPECT_CALL(*lateViewer, beginSubgroup(12, 0, _, _));

  auto lateResult = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(2)), lateViewer, makeDownstreamPeer()),
      &eventBase_);
  ASSERT_TRUE(lateResult.hasValue());
  providerGate.post();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_TRUE(replacementConsumer_->beginSubgroup(12, 0, kDefaultPriority));

  EXPECT_TRUE(viewerSubgroups_.contains(12));
}

// Aliases are hop-local, so a viewer that subscribes after the switch still
// gets the alias the first upstream gave the track.
TEST_F(MoQProxyTrackSwitchTest, TheReplacementKeepsItsTrackAliasToItself) {
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  ASSERT_TRUE(upstreamConsumer_->setTrackAlias(TrackAlias(9)));
  expectReplacementSubscribe();
  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_TRUE(replacementConsumer_->setTrackAlias(TrackAlias(77)));
  auto lateViewer = makeConsumer();
  EXPECT_CALL(*lateViewer, setTrackAlias(TrackAlias(9)));

  auto lateResult = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(2)), lateViewer, makeDownstreamPeer()),
      &eventBase_);

  EXPECT_TRUE(lateResult.hasValue());
}

// The upstream goes away between its SUBSCRIBE_OK and the track resuming.
TEST_F(MoQProxyTrackSwitchTest, AnUpstreamLostWhileSubscribingIsReplaced) {
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [this](SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            cb->publishDone(
                PublishDone{
                    RequestID(100),
                    PublishDoneStatusCode::GOING_AWAY,
                    0,
                    "going away"});
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));
  expectReplacementSubscribe();
  auto consumer = makeConsumer();
  EXPECT_CALL(*consumer, publishDone(_)).Times(0);

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(1)), consumer, makeDownstreamPeer()),
      &eventBase_);

  EXPECT_TRUE(result.hasValue());
  ASSERT_TRUE(replacementRequest_.has_value());
  EXPECT_EQ(replacementRequest_->filter, LocationFilter::nextObject());
  EXPECT_EQ(switchStats_->upstreamsLost, 1);
  Mock::VerifyAndClearExpectations(consumer.get());
}

TEST_F(
    MoQProxyTrackSwitchTest,
    AnUpstreamLostWhileSubscribingWithNoFallbackAnswersGoingAway) {
  makeTrack({provider_});
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [this](SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            cb->publishDone(
                PublishDone{
                    RequestID(100),
                    PublishDoneStatusCode::GOING_AWAY,
                    0,
                    "going away"});
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));

  auto result = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(1)),
          makeConsumer(),
          makeDownstreamPeer()),
      &eventBase_);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, SubscribeErrorCode::GOING_AWAY);
}

// Draft 18+ says when a stream starts mid-subgroup.
TEST_F(
    MoQProxyTrackSwitchTest,
    AReplacementStreamStartingMidSubgroupInItsOwnGroupIsAGap) {
  ON_CALL(*replacementSession_, getNegotiatedVersion())
      .WillByDefault(Return(kVersionDraft18));
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeRecordingConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_NE(replacementConsumer_, nullptr);

  ASSERT_TRUE(replacementConsumer_->beginSubgroup(12, 0, kDefaultPriority));
  EXPECT_TRUE(replacementConsumer_
                  ->beginSubgroup(
                      12, 1, kDefaultPriority, {.beginsWithFirstObject = false})
                  .hasError());

  EXPECT_EQ(switchStats_->midSubgroupGaps, 1);
  EXPECT_EQ(switchStats_->refusedStreams, 1);
}

// Draft 16 doesn't say, so the stream is taken.
TEST_F(
    MoQProxyTrackSwitchTest,
    BelowDraft18AReplacementStreamIsTakenAsStartingItsSubgroup) {
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeRecordingConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_NE(replacementConsumer_, nullptr);

  EXPECT_TRUE(replacementConsumer_->beginSubgroup(
      12, 1, kDefaultPriority, {.beginsWithFirstObject = false}));

  EXPECT_EQ(switchStats_->midSubgroupGaps, 0);
}

}} // namespace moxygen
