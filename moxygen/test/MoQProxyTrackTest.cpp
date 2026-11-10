/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fmt/core.h>
#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Sleep.h>
#include <folly/futures/ManualTimekeeper.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
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
// The switch tests' deadline.
constexpr std::chrono::milliseconds kSwitchDeadline{300};

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

  std::shared_ptr<NiceMock<MockSubscriptionHandle>> makeUpstreamHandle(
      AbsoluteLocation largest = AbsoluteLocation{10, 2}) {
    SubscribeOk subscribeOk{
        .requestID = RequestID(100),
        .trackAlias = TrackAlias(9),
        .groupOrder = GroupOrder::OldestFirst,
        .largest = largest};
    return std::make_shared<NiceMock<MockSubscriptionHandle>>(
        std::move(subscribeOk));
  }

  // Establishes the upstream for one subscriber; returns its downstream handle
  // and saves the upstream consumer, which stands in for the upstream session.
  // whileSubscribing runs while the upstream SUBSCRIBE is in flight.
  std::shared_ptr<Publisher::SubscriptionHandle> subscribeThroughUpstream(
      const std::shared_ptr<NiceMock<MockSubscriptionHandle>>& upstreamHandle,
      const std::shared_ptr<TrackConsumer>& consumer,
      std::function<void()> whileSubscribing = nullptr,
      bool forward = true) {
    EXPECT_CALL(*upstreamSession_, subscribe(_, _))
        .WillOnce(Invoke(
            [this, upstreamHandle, whileSubscribing](
                SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
                -> folly::coro::Task<Publisher::SubscribeResult> {
              upstreamConsumer_ = std::move(cb);
              if (whileSubscribing) {
                whileSubscribing();
              }
              co_return Publisher::SubscribeResult(upstreamHandle);
            }));
    auto request = makeSubscribeRequest(RequestID(1));
    request.forward = forward;
    auto result = folly::coro::blockingWait(
        track_->subscribe(std::move(request), consumer, makeDownstreamPeer()),
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
  void receiveSessionGoaway(Goaway goaway = {}) {
    static_cast<MoQControlCodec::ControlCallback&>(*upstreamSession_)
        .onGoaway(std::move(goaway));
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
      std::shared_ptr<folly::Timekeeper> timekeeper = nullptr,
      MoQProxyTrack::CacheConfig cacheConfig = {}) {
    auto switchOptions = switchOptions_;
    switchOptions.retryBackoff = retryBackoff;
    switchOptions.maxRetryBackoff = 2 * retryBackoff;
    std::function<std::chrono::steady_clock::time_point()> clock;
    if (auto manual =
            std::dynamic_pointer_cast<folly::ManualTimekeeper>(timekeeper)) {
      clock = [manual] { return manual->now(); };
    }
    track_ = MoQProxyTrack::create(
        kTrackName,
        std::move(providers),
        std::move(cacheConfig),
        MoQProxyTrack::UpstreamConfig{
            .switchStats = switchStats_,
            .switchOptions = switchOptions,
            .timekeeper = std::move(timekeeper),
            .clock = std::move(clock)});
    track_->setCallback(callback_);
  }

  // A track whose switch timers run on timekeeper_.
  void makeTrackOnManualTime(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> providers,
      MoQProxyTrack::CacheConfig cacheConfig = {}) {
    timekeeper_ = std::make_shared<folly::ManualTimekeeper>();
    makeTrack(
        std::move(providers),
        std::chrono::milliseconds(10),
        timekeeper_,
        std::move(cacheConfig));
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

  // The replacement session accepts the next SUBSCRIBE with handle, or a
  // new one; saves its request and consumer.
  void expectReplacementSubscribe(
      std::shared_ptr<NiceMock<MockSubscriptionHandle>> handle = nullptr) {
    if (!handle) {
      handle = makeUpstreamHandle();
    }
    EXPECT_CALL(*replacementSession_, subscribe(_, _))
        .WillOnce(Invoke(
            [this, handle](
                SubscribeRequest request, std::shared_ptr<TrackConsumer> cb)
                -> folly::coro::Task<Publisher::SubscribeResult> {
              replacementRequest_ = std::move(request);
              replacementConsumer_ = std::move(cb);
              co_return Publisher::SubscribeResult(handle);
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
          ON_CALL(*subgroup, endOfSubgroup())
              .WillByDefault(
                  Return(folly::makeExpected<MoQPublishError>(folly::unit)));
          viewerSubgroups_[group] = subgroup;
          return folly::makeExpected<MoQPublishError>(
              std::static_pointer_cast<SubgroupConsumer>(subgroup));
        }));
    return consumer;
  }

  static constexpr std::chrono::milliseconds kDeadline{kSwitchDeadline};

  // makeTrack sets the retry backoffs.
  MoQProxySwitchOptions switchOptions_{.deadline = kDeadline, .randomSeed = 1};
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

// A quiet track, such as the catalog: the replacement answered with the
// track's Largest and has sent nothing by the deadline. It has nothing newer
// to send, so the switch completes, and the viewer keeps its subscription.
TEST_F(MoQProxyTrackSwitchTest, AQuietTracksReplacementCompletesTheSwitch) {
  makeTrackOnManualTime({provider_, replacementProvider_});
  auto consumer = makeRecordingConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  auto replacementHandle = makeUpstreamHandle();
  expectReplacementSubscribe(replacementHandle);
  EXPECT_CALL(*consumer, publishDone(_)).Times(0);
  EXPECT_CALL(*replacementHandle, unsubscribe()).Times(0);
  loseUpstream();
  runPending();
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);

  advanceTime(kDeadline);

  EXPECT_EQ(switchStats_->lostSwitches.completed, 1);
  EXPECT_EQ(switchStats_->lostSwitches.failed, 0);
  EXPECT_TRUE(sendFirstObject(*replacementConsumer_, 11));
  EXPECT_TRUE(viewerSubgroups_.contains(11));
  Mock::VerifyAndClearExpectations(consumer.get());
  Mock::VerifyAndClearExpectations(replacementHandle.get());
}

// The replacement answered with the track's Largest, but what it sent of group
// 10 by the deadline, a stream header or an object stream, was refused: the
// replacement of a lost upstream takes only the groups after the track's last.
// It isn't quiet, so the switch fails, and the viewer goes away.
class MoQProxyTrackRefusedQuietTest : public MoQProxyTrackSwitchTest,
                                      public WithParamInterface<bool> {};

TEST_P(MoQProxyTrackRefusedQuietTest, AReplacementWhoseDataWasRefusedFails) {
  const bool objectStream = GetParam();
  makeTrackOnManualTime({provider_, replacementProvider_});
  auto consumer = makeRecordingConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  auto replacementHandle = makeUpstreamHandle();
  expectReplacementSubscribe(replacementHandle);
  loseUpstream();
  runPending();
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);
  if (objectStream) {
    EXPECT_TRUE(replacementConsumer_
                    ->objectStream(
                        ObjectHeader(10, 0, 3, 0, uint64_t(1)),
                        folly::IOBuf::copyBuffer("x"))
                    .hasError());
  } else {
    EXPECT_TRUE(replacementConsumer_->beginSubgroup(10, 1, kDefaultPriority)
                    .hasError());
  }
  EXPECT_CALL(*replacementHandle, unsubscribe());
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  advanceTime(kDeadline);

  EXPECT_EQ(switchStats_->lostSwitches.failed, 1);
  EXPECT_EQ(switchStats_->lostSwitches.completed, 0);
  Mock::VerifyAndClearExpectations(consumer.get());
  Mock::VerifyAndClearExpectations(replacementHandle.get());
}

INSTANTIATE_TEST_SUITE_P(
    TheReplacementSentGroup10,
    MoQProxyTrackRefusedQuietTest,
    Values(false, true),
    [](const TestParamInfo<bool>& info) {
      return info.param ? "OnAnObjectStream" : "WithAStreamHeader";
    });

struct QuietLargestCase {
  std::string name;
  std::optional<AbsoluteLocation> largest;
  bool completes;
};

class MoQProxyTrackQuietLargestTest
    : public MoQProxyTrackSwitchTest,
      public WithParamInterface<QuietLargestCase> {};

// The track's Largest is in group 10. A replacement that sends nothing by the
// deadline completes the switch only with its Largest in that group; behind
// it, ahead of it or with none, the viewer goes away.
TEST_P(MoQProxyTrackQuietLargestTest, OnlyTheTracksLargestGroupIsQuiet) {
  makeTrackOnManualTime({provider_, replacementProvider_});
  auto consumer = makeConsumer();
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), consumer);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe(
      std::make_shared<NiceMock<MockSubscriptionHandle>>(SubscribeOk{
          .requestID = RequestID(100),
          .trackAlias = TrackAlias(9),
          .groupOrder = GroupOrder::OldestFirst,
          .largest = GetParam().largest}));
  EXPECT_CALL(
      *consumer,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .Times(GetParam().completes ? 0 : 1)
      .WillRepeatedly(
          Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  loseUpstream();
  runPending();
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);

  advanceTime(kDeadline);

  EXPECT_EQ(switchStats_->lostSwitches.completed, GetParam().completes ? 1 : 0);
  EXPECT_EQ(switchStats_->lostSwitches.failed, GetParam().completes ? 0 : 1);
  Mock::VerifyAndClearExpectations(consumer.get());
}

INSTANTIATE_TEST_SUITE_P(
    QuietLargest,
    MoQProxyTrackQuietLargestTest,
    Values(
        QuietLargestCase{"SameGroup", AbsoluteLocation{10, 0}, true},
        QuietLargestCase{"GroupBehind", AbsoluteLocation{9, 5}, false},
        QuietLargestCase{"GroupAhead", AbsoluteLocation{11, 0}, false},
        QuietLargestCase{"NoLargest", std::nullopt, false}),
    [](const TestParamInfo<QuietLargestCase>& info) {
      return info.param.name;
    });

// Its viewer subscribed with forward 0, so the replacement, subscribed the
// same way, sends nothing though it has newer groups: the switch completes.
TEST_F(MoQProxyTrackSwitchTest, ATrackNoViewerForwardsCompletesTheSwitch) {
  makeTrackOnManualTime({provider_, replacementProvider_});
  auto consumer = makeConsumer();
  auto downstreamHandle = subscribeThroughUpstream(
      makeUpstreamHandle(),
      consumer,
      /*whileSubscribing=*/nullptr,
      /*forward=*/false);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe(makeUpstreamHandle(AbsoluteLocation{12, 0}));
  EXPECT_CALL(*consumer, publishDone(_)).Times(0);
  loseUpstream();
  runPending();
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_FALSE(replacementRequest_->forward);

  advanceTime(kDeadline);

  EXPECT_EQ(switchStats_->lostSwitches.completed, 1);
  EXPECT_EQ(switchStats_->lostSwitches.failed, 0);
  Mock::VerifyAndClearExpectations(consumer.get());
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
// first loss. The third has groups the track doesn't, and sends none.
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
            co_return Publisher::SubscribeResult(
                makeUpstreamHandle(AbsoluteLocation{12, 0}));
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

// Aliases are hop-local, so a viewer that subscribes after the switch gets its
// own request ID as its alias.
TEST_F(MoQProxyTrackSwitchTest, AViewerAfterASwitchGetsNeitherUpstreamsAlias) {
  auto downstreamHandle =
      subscribeThroughUpstream(makeUpstreamHandle(), makeConsumer());
  ASSERT_NE(upstreamConsumer_, nullptr);
  ASSERT_TRUE(upstreamConsumer_->setTrackAlias(TrackAlias(9)));
  expectReplacementSubscribe();
  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_TRUE(replacementConsumer_->setTrackAlias(TrackAlias(77)));
  auto lateViewer = makeConsumer();
  EXPECT_CALL(*lateViewer, setTrackAlias(TrackAlias(2)));

  auto lateResult = folly::coro::blockingWait(
      track_->subscribe(
          makeSubscribeRequest(RequestID(2)), lateViewer, makeDownstreamPeer()),
      &eventBase_);

  ASSERT_TRUE(lateResult.hasValue());
  EXPECT_EQ(lateResult.value()->subscribeOk().trackAlias, TrackAlias(2));
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

// A switch on a GOAWAY, on manual time, with no start delay unless a test
// sets one. The viewer subscribes through the upstream, which serves group 11
// on subgroup 11/0, still open.
class MoQProxyTrackGracefulSwitchTest : public MoQProxyTrackSwitchTest {
 protected:
  void SetUp() override {
    MoQProxyTrackSwitchTest::SetUp();
    switchOptions_.maxStartDelay = std::chrono::milliseconds(0);
    makeTrackOnManualTime({provider_, replacementProvider_});
  }

  void subscribeWithGroup11Open(
      std::function<void()> whileSubscribing = nullptr) {
    downstreamHandle_ =
        subscribeThroughUpstream(oldHandle_, consumer_, whileSubscribing);
    ASSERT_NE(upstreamConsumer_, nullptr);
    auto subgroup = upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority);
    ASSERT_TRUE(subgroup.hasValue());
    oldSubgroup11_ = subgroup.value();
  }

  // The old upstream finishes a subgroup it opened.
  [[nodiscard]] static bool finish(SubgroupConsumer& subgroup) {
    return subgroup.endOfSubgroup().hasValue();
  }

  [[nodiscard]] bool replacementOpens(uint64_t group) {
    return sendFirstObject(*replacementConsumer_, group);
  }

  [[nodiscard]] bool oldOpens(uint64_t group) {
    auto subgroup =
        upstreamConsumer_->beginSubgroup(group, 0, kDefaultPriority);
    if (subgroup.hasError()) {
      return false;
    }
    oldSubgroups_[group] = subgroup.value();
    return true;
  }

  static constexpr std::chrono::milliseconds kReorderGrace{200};

  std::shared_ptr<NiceMock<MockTrackConsumer>> consumer_{
      makeRecordingConsumer()};
  std::shared_ptr<NiceMock<MockSubscriptionHandle>> oldHandle_{
      makeUpstreamHandle()};
  std::shared_ptr<Publisher::SubscriptionHandle> downstreamHandle_;
  std::shared_ptr<SubgroupConsumer> oldSubgroup11_;
  std::map<uint64_t, std::shared_ptr<SubgroupConsumer>> oldSubgroups_;
};

// The replacement, on the next provider since the session going away is
// refused, starts at the next group, 12. The old upstream finishes group 11
// and is refused group 12; once it has stayed caught up for the reorder grace,
// the track unsubscribes from it and serves from the replacement.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    TheOldUpstreamGoesOnceItHasCaughtUpForTheReorderGrace) {
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  subscribeWithGroup11Open();
  expectReplacementSubscribe();

  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  EXPECT_EQ(replacementRequest_->filter, LocationFilter::nextObject());
  EXPECT_EQ(provider_->calls, 2);
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_FALSE(oldOpens(12));
  EXPECT_TRUE(viewerSubgroups_.contains(12));
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  ASSERT_TRUE(finish(*oldSubgroup11_));
  advanceTime(kReorderGrace - std::chrono::milliseconds(1));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 0);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
  EXPECT_CALL(*oldHandle_, unsubscribe());

  advanceTime(std::chrono::milliseconds(1));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  EXPECT_EQ(switchStats_->handoffs.equal, 1);
  EXPECT_EQ(switchStats_->upstreamsLost, 0);
  EXPECT_EQ(switchStats_->lostSwitches.started, 0);
  EXPECT_EQ(switchStats_->refusedStreams, 0);
  EXPECT_FALSE(oldOpens(13));
  EXPECT_FALSE(viewerSubgroups_.contains(13));
  EXPECT_TRUE(replacementOpens(13));
  EXPECT_TRUE(viewerSubgroups_.contains(13));
  advanceTime(2 * switchOptions_.defaultGroupDuration);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 0);
  Mock::VerifyAndClearExpectations(consumer_.get());
}

// A late stream for group 11 reopens it, which restarts the reorder grace
// once it finishes.
TEST_F(MoQProxyTrackGracefulSwitchTest, ALateOldStreamRestartsTheReorderGrace) {
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_FALSE(oldOpens(12));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  advanceTime(kReorderGrace / 2);
  auto late = upstreamConsumer_->beginSubgroup(11, 1, kDefaultPriority);
  ASSERT_TRUE(late.hasValue());
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  advanceTime(kReorderGrace);
  ASSERT_TRUE(finish(*late.value()));
  advanceTime(kReorderGrace - std::chrono::milliseconds(1));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
  EXPECT_CALL(*oldHandle_, unsubscribe());

  advanceTime(std::chrono::milliseconds(1));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
}

// The old upstream opens a second stream for 11/0. The first one ending,
// before or after the GOAWAY, leaves group 11 open until the second ends.
class MoQProxyTrackReopenedSubgroupTest
    : public MoQProxyTrackGracefulSwitchTest,
      public WithParamInterface<bool> {};

TEST_P(
    MoQProxyTrackReopenedSubgroupTest,
    TheHandoverWaitsForTheStreamThatReopenedASubgroup) {
  const bool firstEndsBeforeGoaway = GetParam();
  subscribeWithGroup11Open();
  auto reopened = upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority);
  ASSERT_TRUE(reopened.hasValue());
  if (firstEndsBeforeGoaway) {
    oldSubgroup11_.reset();
  }
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_FALSE(oldOpens(12));
  oldSubgroup11_.reset();
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  advanceTime(2 * kReorderGrace);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
  EXPECT_CALL(*oldHandle_, unsubscribe());

  reopened.value().reset();
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
}

INSTANTIATE_TEST_SUITE_P(
    ReopenedSubgroup,
    MoQProxyTrackReopenedSubgroupTest,
    Values(true, false),
    [](const TestParamInfo<bool>& info) {
      return info.param ? "FirstEndsBeforeTheGoaway" : "FirstEndsAfterIt";
    });

// The replacement starts at group 12, which the old upstream has opened, so
// its copies of the groups through 12 are refused and it takes over at 13.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AReplacementBehindTakesOverAfterTheOldUpstreamsGroups) {
  subscribeWithGroup11Open();
  ASSERT_TRUE(oldOpens(12));
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();

  EXPECT_FALSE(replacementOpens(12));
  EXPECT_FALSE(oldOpens(13));
  EXPECT_TRUE(replacementOpens(13));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  ASSERT_TRUE(finish(*oldSubgroups_[12]));
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->refusedStreams, 1);
  EXPECT_EQ(switchStats_->handoffs.behind, 1);
}

// The replacement starts at group 13, so the old upstream still serves group
// 12 before it is refused 13.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AReplacementAheadLetsTheOldUpstreamFillTheGroupsBeforeIt) {
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();

  ASSERT_TRUE(replacementOpens(13));
  EXPECT_TRUE(oldOpens(12));
  EXPECT_TRUE(viewerSubgroups_.contains(12));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  ASSERT_TRUE(finish(*oldSubgroups_[12]));
  advanceTime(kReorderGrace);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  EXPECT_FALSE(oldOpens(13));
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->refusedStreams, 0);
  EXPECT_EQ(switchStats_->handoffs.ahead, 1);
  // The deadline passed while the replacement waited.
  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 0);
}

// Draft 18+: a replacement stream that starts mid-subgroup is refused. Before
// the split, it keeps its group the old upstream's; after, its group reaches
// viewers without it.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AReplacementStreamStartingMidSubgroupIsRefused) {
  ON_CALL(*replacementSession_, getNegotiatedVersion())
      .WillByDefault(Return(kVersionDraft18));
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  auto midSubgroup = [this](uint64_t group, uint64_t subgroup) {
    return replacementConsumer_
        ->beginSubgroup(
            group, subgroup, kDefaultPriority, {.beginsWithFirstObject = false})
        .hasError();
  };

  EXPECT_TRUE(midSubgroup(12, 1));
  EXPECT_EQ(switchStats_->midSubgroupGaps, 0);
  EXPECT_FALSE(replacementOpens(12));
  EXPECT_TRUE(oldOpens(12));
  ASSERT_TRUE(replacementOpens(13));
  EXPECT_TRUE(midSubgroup(13, 1));

  EXPECT_EQ(switchStats_->midSubgroupGaps, 1);
  EXPECT_EQ(switchStats_->refusedStreams, 3);
}

// What the old upstream had open when it went away holds the handover until
// it finishes, on every kind of delivery.
enum class OldDelivery { SubgroupStream, ObjectStream, Datagram };

class MoQProxyTrackHandoverDeliveryTest
    : public MoQProxyTrackGracefulSwitchTest,
      public WithParamInterface<OldDelivery> {};

TEST_P(MoQProxyTrackHandoverDeliveryTest, TheOldUpstreamFinishesItsOpenGroup) {
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  ON_CALL(*consumer_, objectStream(_, _, _))
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  ON_CALL(*consumer_, datagram(_, _, _))
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  auto payload = [] { return folly::IOBuf::copyBuffer("x"); };
  // Sends object 0 of group 11; what finishes the group, if anything is left
  // open.
  folly::Function<bool()> finishGroup11;
  switch (GetParam()) {
    case OldDelivery::SubgroupStream: {
      auto subgroup = upstreamConsumer_->beginSubgroup(11, 0, kDefaultPriority);
      ASSERT_TRUE(subgroup.hasValue());
      finishGroup11 = [subgroup = subgroup.value()] {
        return subgroup->endOfSubgroup().hasValue();
      };
      break;
    }
    case OldDelivery::ObjectStream:
      ASSERT_TRUE(upstreamConsumer_->objectStream(
          ObjectHeader(11, 0, 0, 0, uint64_t(1)), payload()));
      break;
    case OldDelivery::Datagram:
      ASSERT_TRUE(upstreamConsumer_->datagram(
          ObjectHeader(11, 0, 0, 0, uint64_t(1)), payload(), false));
      finishGroup11 = [this, payload] {
        return upstreamConsumer_
            ->datagram(ObjectHeader(11, 0, 1, 0, uint64_t(1)), payload(), true)
            .hasValue();
      };
      break;
  }
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  ASSERT_TRUE(replacementOpens(12));
  // A stream, since a datagram of a later group would end group 11 itself.
  EXPECT_FALSE(oldOpens(12));
  advanceTime(kReorderGrace);
  if (!finishGroup11) {
    EXPECT_EQ(track_->upstreamSession(), replacementSession_);
    return;
  }
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);

  ASSERT_TRUE(finishGroup11());
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
}

INSTANTIATE_TEST_SUITE_P(
    HandoverDelivery,
    MoQProxyTrackHandoverDeliveryTest,
    Values(
        OldDelivery::SubgroupStream,
        OldDelivery::ObjectStream,
        OldDelivery::Datagram),
    [](const TestParamInfo<OldDelivery>& info) {
      switch (info.param) {
        case OldDelivery::SubgroupStream:
          return "SubgroupStream";
        case OldDelivery::ObjectStream:
          return "ObjectStream";
        case OldDelivery::Datagram:
          return "Datagram";
      }
      return "Unknown";
    });

// However the old upstream's open subgroup ends, it counts as finished.
enum class SubgroupEnd {
  FinObject,
  FinPayload,
  EndOfSubgroup,
  EndOfGroup,
  EndOfTrack,
  Reset,
  Dropped,
};

class MoQProxyTrackOldSubgroupEndTest : public MoQProxyTrackGracefulSwitchTest,
                                        public WithParamInterface<SubgroupEnd> {
 protected:
  // The session holds the subgroup after it ends, unless it dropped it.
  void end(std::shared_ptr<SubgroupConsumer>& subgroup) {
    auto payload = [] { return folly::IOBuf::copyBuffer("x"); };
    switch (GetParam()) {
      case SubgroupEnd::FinObject:
        EXPECT_TRUE(subgroup->object(0, payload(), noExtensions(), true));
        break;
      case SubgroupEnd::FinPayload:
        EXPECT_TRUE(subgroup->beginObject(0, 2, payload(), noExtensions()));
        EXPECT_TRUE(subgroup->objectPayload(payload(), true));
        break;
      case SubgroupEnd::EndOfSubgroup:
        EXPECT_TRUE(subgroup->endOfSubgroup());
        break;
      case SubgroupEnd::EndOfGroup:
        EXPECT_TRUE(subgroup->endOfGroup(0));
        break;
      case SubgroupEnd::EndOfTrack:
        EXPECT_TRUE(subgroup->endOfTrackAndGroup(0));
        break;
      case SubgroupEnd::Reset:
        subgroup->reset(ResetStreamErrorCode::CANCELLED);
        break;
      case SubgroupEnd::Dropped:
        subgroup.reset();
        break;
    }
  }
};

TEST_P(MoQProxyTrackOldSubgroupEndTest, TheHandoverGoesOnOnceItEnds) {
  ON_CALL(*consumer_, beginSubgroup(_, _, _, _))
      .WillByDefault(Invoke([](uint64_t,
                               uint64_t,
                               Priority,
                               TrackConsumer::BeginSubgroupOptions) {
        auto subgroup = std::make_shared<NiceMock<MockSubgroupConsumer>>();
        auto ok = folly::makeExpected<MoQPublishError>(folly::unit);
        ON_CALL(*subgroup, object(_, _, _, _)).WillByDefault(Return(ok));
        ON_CALL(*subgroup, beginObject(_, _, _, _)).WillByDefault(Return(ok));
        ON_CALL(*subgroup, objectPayload(_, _))
            .WillByDefault(Return(
                folly::makeExpected<MoQPublishError>(
                    ObjectPublishStatus::DONE)));
        ON_CALL(*subgroup, endOfSubgroup()).WillByDefault(Return(ok));
        ON_CALL(*subgroup, endOfGroup(_)).WillByDefault(Return(ok));
        ON_CALL(*subgroup, endOfTrackAndGroup(_)).WillByDefault(Return(ok));
        return folly::makeExpected<MoQPublishError>(
            std::static_pointer_cast<SubgroupConsumer>(subgroup));
      }));
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_FALSE(oldOpens(12));
  advanceTime(kReorderGrace);
  ASSERT_EQ(track_->upstreamSession(), upstreamSession_);

  end(oldSubgroup11_);
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
}

INSTANTIATE_TEST_SUITE_P(
    OldSubgroupEnd,
    MoQProxyTrackOldSubgroupEndTest,
    Values(
        SubgroupEnd::FinObject,
        SubgroupEnd::FinPayload,
        SubgroupEnd::EndOfSubgroup,
        SubgroupEnd::EndOfGroup,
        SubgroupEnd::EndOfTrack,
        SubgroupEnd::Reset,
        SubgroupEnd::Dropped),
    [](const TestParamInfo<SubgroupEnd>& info) {
      switch (info.param) {
        case SubgroupEnd::FinObject:
          return "FinObject";
        case SubgroupEnd::FinPayload:
          return "FinPayload";
        case SubgroupEnd::EndOfSubgroup:
          return "EndOfSubgroup";
        case SubgroupEnd::EndOfGroup:
          return "EndOfGroup";
        case SubgroupEnd::EndOfTrack:
          return "EndOfTrack";
        case SubgroupEnd::Reset:
          return "Reset";
        case SubgroupEnd::Dropped:
          return "Dropped";
      }
      return "Unknown";
    });

// Lost before a replacement was adopted, the old upstream leaves a gap that
// the switch, now a lost one, fills without waiting out its start delay.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AnUpstreamLostBeforeItsReplacementCarriesOnAsALostSwitch) {
  switchOptions_.maxStartDelay = std::chrono::seconds(1);
  makeTrackOnManualTime({provider_, replacementProvider_});
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  // The seed's start delay isn't zero.
  ASSERT_EQ(replacementConsumer_, nullptr);

  loseUpstream();
  runPending();

  ASSERT_EQ(track_->upstreamSession(), replacementSession_);
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 0);
  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 0);
  EXPECT_EQ(switchStats_->lostSwitches.started, 1);
  EXPECT_EQ(switchStats_->lostSwitches.completed, 1);
  EXPECT_EQ(switchStats_->lateReplacementGaps.gaps, 1);
  Mock::VerifyAndClearExpectations(consumer_.get());
}

// The old upstream's session closes while it is still in group 11, and the
// replacement already serves group 13: the replacement takes over at once, and
// owns group 12 too.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AnUpstreamLostOnceItsReplacementWasAdoptedHandsOverAtOnce) {
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(13));

  oldSubgroup11_->reset(ResetStreamErrorCode::SESSION_CLOSED);
  loseUpstream(PublishDoneStatusCode::SESSION_CLOSED);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  EXPECT_EQ(switchStats_->lostSwitches.started, 0);
  EXPECT_EQ(switchStats_->upstreamsLost, 1);
  EXPECT_EQ(switchStats_->lostCutoffs.cutoffs, 1);
  EXPECT_EQ(switchStats_->lostCutoffs.unfinishedGroups, 1);
  EXPECT_TRUE(replacementOpens(12));
  EXPECT_TRUE(viewerSubgroups_.contains(12));
  Mock::VerifyAndClearExpectations(consumer_.get());
}

// The session GOAWAY, the request-stream GOAWAY and the loss of the old
// upstream make one switch.
TEST_F(MoQProxyTrackGracefulSwitchTest, SignalsForOneUpstreamMakeOneSwitch) {
  subscribeWithGroup11Open();
  expectReplacementSubscribe();

  receiveSessionGoaway();
  upstreamConsumer_->goaway(Goaway{});
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  loseUpstream(PublishDoneStatusCode::SESSION_CLOSED);

  EXPECT_EQ(switchStats_->upstreamGoaways, 2);
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  EXPECT_EQ(switchStats_->lostSwitches.started, 0);
  EXPECT_EQ(replacementProvider_->calls, 1);
  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
}

// The old upstream never finishes group 11. The tail timer, which the
// replacement's first forwarded object starts, lets go of it: its stream of
// group 11 to the viewer resets, and the replacement serves. The tail is two
// group durations, no less than minTail, and ends at least tailMargin before
// the GOAWAY's timeout.
struct TailCase {
  std::string name;
  uint64_t goawayTimeoutMs;
  // Between the starts of groups 11 and 12.
  std::chrono::milliseconds groupSpacing;
  // Still serving from the old upstream then, and no longer by cutBy.
  std::chrono::milliseconds servesUntil;
  std::chrono::milliseconds cutBy;
};

class MoQProxyTrackTailTest : public MoQProxyTrackGracefulSwitchTest {};

class MoQProxyTrackTailCaseTest : public MoQProxyTrackTailTest,
                                  public WithParamInterface<TailCase> {};

TEST_P(MoQProxyTrackTailCaseTest, TheTailTimerLetsGoOfAnOldUpstreamThatLags) {
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway(Goaway{.timeout = GetParam().goawayTimeoutMs});
  runPending();
  advanceTime(GetParam().groupSpacing);
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_FALSE(oldOpens(12));
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  advanceTime(GetParam().servesUntil);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
  EXPECT_CALL(*oldHandle_, unsubscribe());
  EXPECT_CALL(*viewerSubgroups_[11], reset(ResetStreamErrorCode::CANCELLED));

  advanceTime(GetParam().cutBy - GetParam().servesUntil);

  Mock::VerifyAndClearExpectations(viewerSubgroups_[11].get());
  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 1);
  EXPECT_EQ(switchStats_->tailCutoffs.unfinishedGroups, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  EXPECT_EQ(switchStats_->lostSwitches.started, 0);
  EXPECT_TRUE(
      oldSubgroup11_->object(1, folly::IOBuf::copyBuffer("x"), noExtensions())
          .hasError());
}

INSTANTIATE_TEST_SUITE_P(
    Tail,
    MoQProxyTrackTailCaseTest,
    Values(
        TailCase{
            "TwoGroupDurations",
            0,
            std::chrono::seconds(1),
            std::chrono::milliseconds(1995),
            std::chrono::seconds(2)},
        TailCase{
            "MinTail",
            0,
            std::chrono::milliseconds(100),
            std::chrono::milliseconds(995),
            std::chrono::seconds(1)},
        // The GOAWAY arrived a few milliseconds before the replacement's
        // first object, on the steady clock.
        TailCase{
            "BeforeTheGoawayTimeout",
            1000,
            std::chrono::seconds(1),
            std::chrono::milliseconds(400),
            std::chrono::milliseconds(500)}),
    [](const TestParamInfo<TailCase>& info) { return info.param.name; });

// The old upstream had opened no group, so the replacement's group 12 is the
// first to start and the tail is two default group durations.
TEST_F(
    MoQProxyTrackTailTest,
    BeforeTwoGroupsStartTheTailIsTwoDefaultGroupDurations) {
  const auto tail = 2 * switchOptions_.defaultGroupDuration;
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));

  advanceTime(tail - std::chrono::milliseconds(5));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  advanceTime(std::chrono::milliseconds(5));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 1);
  EXPECT_EQ(switchStats_->tailCutoffs.unfinishedGroups, 0);
}

// Groups 11 to 14 start 50 ms apart, so the tail is 100 ms. A second subgroup
// of group 13 doesn't start a group.
TEST_F(MoQProxyTrackTailTest, TheTailTimerFollowsTheGroupDuration) {
  constexpr std::chrono::milliseconds kSpacing{50};
  switchOptions_.minTail = std::chrono::milliseconds(0);
  makeTrackOnManualTime({provider_, replacementProvider_});
  subscribeWithGroup11Open();
  advanceTime(kSpacing);
  ASSERT_TRUE(oldOpens(12));
  advanceTime(kSpacing);
  ASSERT_TRUE(oldOpens(13));
  ASSERT_TRUE(upstreamConsumer_->beginSubgroup(13, 1, kDefaultPriority));
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  advanceTime(kSpacing);
  ASSERT_TRUE(replacementOpens(14));

  advanceTime(2 * kSpacing - std::chrono::milliseconds(5));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  advanceTime(std::chrono::milliseconds(5));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->tailCutoffs.unfinishedGroups, 3);
}

// The replacement's copy of group 12, which the old upstream has open, was
// refused, so it has forwarded nothing and the old upstream serves past the
// tail. The replacement's group 13 starts the tail timer.
TEST_F(MoQProxyTrackTailTest, TheTailTimerStartsOnceTheReplacementForwards) {
  const auto tail = switchOptions_.minTail;
  subscribeWithGroup11Open();
  ASSERT_TRUE(oldOpens(12));
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  EXPECT_FALSE(replacementOpens(12));
  advanceTime(2 * tail);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  ASSERT_TRUE(replacementOpens(13));

  advanceTime(tail - std::chrono::milliseconds(5));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  advanceTime(std::chrono::milliseconds(5));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->tailCutoffs.unfinishedGroups, 2);
  EXPECT_EQ(switchStats_->lostSwitches.started, 0);
}

// The replacement delivered group 13 while still subscribing, and the deadline
// passed before the track adopted it. The tail timer starts only with an
// adopted replacement, so the old upstream serves on.
TEST_F(MoQProxyTrackTailTest, TheTailTimerWaitsForAnAdoptedReplacement) {
  switchOptions_.minTail = std::chrono::milliseconds(0);
  switchOptions_.defaultGroupDuration = std::chrono::milliseconds(200);
  makeTrackOnManualTime({provider_, replacementProvider_});
  subscribeWithGroup11Open();
  folly::coro::Baton reply;
  auto lateHandle = makeUpstreamHandle();
  EXPECT_CALL(*replacementSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [&reply, lateHandle](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            EXPECT_TRUE(sendFirstObject(*cb, 13));
            co_await reply;
            co_return Publisher::SubscribeResult(lateHandle);
          }));
  receiveSessionGoaway();
  runPending();
  advanceTime(kDeadline);
  ASSERT_EQ(switchStats_->gracefulSwitches.failed, 1);

  advanceTime(std::chrono::milliseconds(200));

  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 0);
  EXPECT_EQ(switchStats_->lostSwitches.started, 0);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  reply.post();
  runPending();
  EXPECT_TRUE(oldOpens(14));
}

// The old upstream catches up half a reorder grace before the tail runs out,
// so the reorder grace hands over, and no cutoff is counted.
TEST_F(
    MoQProxyTrackTailTest,
    ATailRunningOutDuringTheReorderGraceLeavesTheHandoverToIt) {
  const auto caughtUpAt = switchOptions_.minTail - kReorderGrace / 2;
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  advanceTime(caughtUpAt);
  ASSERT_TRUE(finish(*oldSubgroup11_));
  EXPECT_FALSE(oldOpens(12));

  advanceTime(kReorderGrace - std::chrono::milliseconds(5));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  advanceTime(std::chrono::milliseconds(5));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 0);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
}

// The old upstream catches up half a reorder grace before the tail runs out,
// and a late stream of group 11 stops the grace after the tail ran out: a
// reorder grace after it ran out, the tail timer lets go of the old upstream.
TEST_F(
    MoQProxyTrackTailTest,
    ALateStreamThatStopsTheReorderGraceLeavesTheOldUpstreamToTheTail) {
  const auto tail = switchOptions_.minTail;
  const auto caughtUpAt = tail - kReorderGrace / 2;
  const auto lateStreamAt = tail + kReorderGrace / 4;
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  advanceTime(caughtUpAt);
  ASSERT_TRUE(finish(*oldSubgroup11_));
  EXPECT_FALSE(oldOpens(12));
  advanceTime(lateStreamAt - caughtUpAt);
  auto late = upstreamConsumer_->beginSubgroup(11, 1, kDefaultPriority);
  ASSERT_TRUE(late.hasValue());

  advanceTime(
      tail + kReorderGrace - lateStreamAt - std::chrono::milliseconds(5));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  advanceTime(std::chrono::milliseconds(5));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 1);
  EXPECT_EQ(switchStats_->tailCutoffs.unfinishedGroups, 1);
}

// Group 11 is still open, and the old upstream's stream of group 12 was reset
// during the switch: the tail cutoff counts both groups.
TEST_F(
    MoQProxyTrackTailTest,
    ATailCutoffCountsTheGroupsCutShortDuringTheSwitch) {
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(13));
  ASSERT_TRUE(oldOpens(12));
  oldSubgroups_[12]->reset(ResetStreamErrorCode::CANCELLED);

  advanceTime(switchOptions_.minTail);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 1);
  EXPECT_EQ(switchStats_->tailCutoffs.unfinishedGroups, 2);
}

// The viewer left after the replacement finished its stream of group 12, so
// the old upstream's stream of group 11 is the last one open. Cancelling it
// at the tail cutoff ends the track, which unsubscribes from both upstreams.
TEST_F(MoQProxyTrackTailTest, ATailCutoffThatEndsTheLastStreamClosesTheTrack) {
  subscribeWithGroup11Open();
  auto replacementHandle = makeUpstreamHandle();
  expectReplacementSubscribe(replacementHandle);
  receiveSessionGoaway();
  runPending();
  auto subgroup = replacementConsumer_->beginSubgroup(12, 0, kDefaultPriority);
  ASSERT_TRUE(subgroup.hasValue());
  ASSERT_TRUE(subgroup.value()->object(
      0, folly::IOBuf::copyBuffer("x"), noExtensions(), false));
  downstreamHandle_->unsubscribe();
  subgroup.value()->endOfSubgroup();
  ASSERT_EQ(callback_->calls, 0);
  EXPECT_CALL(*oldHandle_, unsubscribe());
  EXPECT_CALL(*replacementHandle, unsubscribe());

  advanceTime(switchOptions_.minTail);

  EXPECT_EQ(callback_->calls, 1);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 1);
}

// The cache has group 11's first object when the tail cutoff cuts the group
// short, so the rest of it is a miss at once, not data that may still arrive.
TEST_F(MoQProxyTrackTailTest, ATailCutoffEndsTheCachedCopyOfTheGroupsItCuts) {
  makeTrackOnManualTime(
      {provider_, replacementProvider_},
      {.maxGroups = 3, .budget = std::make_shared<MoQCacheBudget>(1 << 20)});
  subscribeWithGroup11Open();
  ASSERT_TRUE(oldSubgroup11_->object(
      0, folly::IOBuf::copyBuffer("x"), noExtensions(), false));
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  ASSERT_EQ(
      track_->groupCache()->inspect({11, 1}, {11, 2}),
      MoQGroupCache::Coverage::AwaitIngress);

  advanceTime(switchOptions_.minTail);

  EXPECT_EQ(
      track_->groupCache()->inspect({11, 1}, {11, 2}),
      MoQGroupCache::Coverage::Miss);
}

// The old upstream had caught up when it was lost, during the reorder grace,
// so the loss cuts nothing short.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AnUpstreamLostDuringTheReorderGraceIsntACutoff) {
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  EXPECT_FALSE(oldOpens(12));

  loseUpstream();

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->lostCutoffs.cutoffs, 0);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
}

// The replacement goes away 200 ms after its first object, and the next
// one's first object starts the 2 s tail timer again.
TEST_F(MoQProxyTrackTailTest, ANewSplitRestartsTheTailTimer) {
  auto thirdSession = makeSession();
  auto thirdProvider = std::make_shared<TestUpstreamProvider>(thirdSession);
  switchOptions_.minTail = std::chrono::seconds(2);
  makeTrackOnManualTime({provider_, replacementProvider_, thirdProvider});
  subscribeWithGroup11Open();
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
  runPending();
  ASSERT_TRUE(replacementOpens(13));
  advanceTime(std::chrono::milliseconds(200));
  replacementConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::GOING_AWAY, 0, "going away"});
  runPending();
  ASSERT_NE(thirdConsumer, nullptr);
  ASSERT_TRUE(sendFirstObject(*thirdConsumer, 14));

  advanceTime(std::chrono::milliseconds(1900));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  advanceTime(std::chrono::milliseconds(100));

  EXPECT_EQ(track_->upstreamSession(), thirdSession);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 1);
}

// The old upstream is lost before the adopted replacement forwarded an object,
// so the switch carries on as a lost one with that replacement.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AnUpstreamLostWhileItsReplacementIsSilentCarriesOnAsALostSwitch) {
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);

  loseUpstream();

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->lostSwitches.started, 1);
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_EQ(switchStats_->lostSwitches.completed, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 0);
  EXPECT_EQ(switchStats_->lateReplacementGaps.gaps, 1);
  Mock::VerifyAndClearExpectations(consumer_.get());
}

// The replacement has groups the track doesn't. The deadline passed while it
// waited, and it still forwards nothing once the old upstream is lost: the
// lost switch it carries on as gets a deadline of its own, then ends the
// viewer with GOING_AWAY.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    ASilentReplacementFailsTheSwitchItCarriesOnAs) {
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  subscribeWithGroup11Open();
  expectReplacementSubscribe(makeUpstreamHandle(AbsoluteLocation{13, 0}));
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  advanceTime(kDeadline);
  ASSERT_EQ(switchStats_->gracefulSwitches.failed, 0);

  loseUpstream();
  advanceTime(kDeadline - std::chrono::milliseconds(5));
  EXPECT_EQ(switchStats_->lostSwitches.failed, 0);
  Mock::VerifyAndClearExpectations(consumer_.get());
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  advanceTime(std::chrono::milliseconds(5));

  EXPECT_EQ(switchStats_->lostSwitches.failed, 1);
}

// A quiet track: old has nothing open, and the replacement answered with the
// track's Largest and sends nothing. Once that has lasted the reorder grace,
// old goes, before its GOAWAY's timeout, and the replacement serves the next
// group.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AQuietTrackHandsOverAfterTheReorderGrace) {
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  advanceTime(kReorderGrace - std::chrono::milliseconds(1));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
  EXPECT_CALL(*oldHandle_, unsubscribe());

  advanceTime(std::chrono::milliseconds(1));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  EXPECT_FALSE(oldOpens(11));
  EXPECT_TRUE(replacementOpens(11));
  EXPECT_TRUE(viewerSubgroups_.contains(11));
  Mock::VerifyAndClearExpectations(consumer_.get());
}

// Old may still have objects for a group it has open, so a quiet handover
// waits until it finishes the group.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AQuietHandoverWaitsForOldToFinishItsGroup) {
  subscribeWithGroup11Open();
  expectReplacementSubscribe(makeUpstreamHandle(AbsoluteLocation{11, 0}));
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  advanceTime(2 * kReorderGrace);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
  EXPECT_CALL(*oldHandle_, unsubscribe());

  ASSERT_TRUE(finish(*oldSubgroup11_));
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
}

// Old opening a group during the reorder grace has something to send, so it
// stays.
TEST_F(MoQProxyTrackGracefulSwitchTest, OldOpeningAGroupStopsAQuietHandover) {
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  advanceTime(kReorderGrace / 2);
  ASSERT_TRUE(oldOpens(11));
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);

  advanceTime(2 * kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 0);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
}

// On object streams old leaves no group open, though more of group 10 may
// follow. The replacement answered with 10/4, so old stays until it has sent
// 10/3 and 10/4. A late object of group 9 then starts the reorder grace
// afresh, so old goes only once it has sent nothing for that long.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    OldObjectsOnAQuietTrackRestartTheReorderGrace) {
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  EXPECT_CALL(*consumer_, objectStream(Field(&ObjectHeader::group, 10), _, _))
      .Times(2)
      .WillRepeatedly(
          Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  expectReplacementSubscribe(makeUpstreamHandle(AbsoluteLocation{10, 4}));
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  ASSERT_TRUE(upstreamConsumer_->objectStream(
      ObjectHeader(10, 0, 3, 0, uint64_t(1)), folly::IOBuf::copyBuffer("x")));
  advanceTime(2 * kReorderGrace);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  ASSERT_TRUE(upstreamConsumer_->objectStream(
      ObjectHeader(10, 0, 4, 0, uint64_t(1)), folly::IOBuf::copyBuffer("x")));
  advanceTime(kReorderGrace / 2);
  ASSERT_TRUE(upstreamConsumer_->objectStream(
      ObjectHeader(9, 0, 7, 0, uint64_t(1)), folly::IOBuf::copyBuffer("x")));
  advanceTime(kReorderGrace + kReorderGrace / 2 - std::chrono::milliseconds(1));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
  EXPECT_CALL(*oldHandle_, unsubscribe());

  advanceTime(std::chrono::milliseconds(1));

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  Mock::VerifyAndClearExpectations(consumer_.get());
}

// Both upstreams are at 10/2 and quiet, and old goes. The publisher then adds
// 10/3: the replacement, which subscribed from the object after 10/2, sends it,
// and it reaches the viewer.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AQuietReplacementGoesOnWithTheGroupOfTheTracksLargest) {
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  advanceTime(kReorderGrace);
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_CALL(
      *consumer_,
      objectStream(
          AllOf(Field(&ObjectHeader::group, 10), Field(&ObjectHeader::id, 3)),
          _,
          _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  EXPECT_TRUE(replacementConsumer_
                  ->objectStream(
                      ObjectHeader(10, 0, 3, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasValue());

  Mock::VerifyAndClearExpectations(consumer_.get());
}

// Both upstreams are at 10/2 and old goes. The replacement then goes on with
// subgroup 10/0, on a stream that starts mid-subgroup (draft 18+ says so)
// since viewers have its first objects, and its objects reach the viewer.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AQuietReplacementGoesOnWithASubgroupOfTheTracksLargestGroup) {
  ON_CALL(*replacementSession_, getNegotiatedVersion())
      .WillByDefault(Return(kVersionDraft18));
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  advanceTime(kReorderGrace);
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);

  auto subgroup = replacementConsumer_->beginSubgroup(
      10,
      0,
      kDefaultPriority,
      TrackConsumer::BeginSubgroupOptions{.beginsWithFirstObject = false});

  ASSERT_TRUE(subgroup.hasValue());
  ASSERT_TRUE(viewerSubgroups_.contains(10));
  EXPECT_CALL(*viewerSubgroups_[10], object(3, _, _, false));
  EXPECT_CALL(*viewerSubgroups_[10], object(4, _, _, false));
  EXPECT_TRUE(subgroup.value()
                  ->object(3, folly::IOBuf::copyBuffer("x"), noExtensions())
                  .hasValue());
  EXPECT_TRUE(subgroup.value()
                  ->object(4, folly::IOBuf::copyBuffer("y"), noExtensions())
                  .hasValue());
  Mock::VerifyAndClearExpectations(viewerSubgroups_[10].get());
}

// Old sent 10/3 after the replacement answered with 10/2, so the replacement's
// own 10/3 would be its second copy for the viewer. Old stays, and keeps group
// 10 once the replacement sends.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AQuietReplacementBehindTheTracksLargestLeavesItsGroupToOld) {
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  EXPECT_CALL(*consumer_, objectStream(Field(&ObjectHeader::group, 10), _, _))
      .Times(2)
      .WillRepeatedly(
          Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  ASSERT_TRUE(upstreamConsumer_->objectStream(
      ObjectHeader(10, 0, 3, 0, uint64_t(1)), folly::IOBuf::copyBuffer("x")));
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);

  advanceTime(2 * kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  EXPECT_TRUE(replacementConsumer_
                  ->objectStream(
                      ObjectHeader(10, 0, 3, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasError());
  EXPECT_TRUE(upstreamConsumer_
                  ->objectStream(
                      ObjectHeader(10, 0, 4, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasValue());
  Mock::VerifyAndClearExpectations(consumer_.get());
  Mock::VerifyAndClearExpectations(oldHandle_.get());
}

// Both upstreams are at 10/2 and quiet. During the reorder grace the
// replacement sends 10/3, of the group old still owns: on an object stream, or
// on a stream that starts mid-subgroup (draft 18+ says so). It's refused, and
// the track isn't quiet, so old stays and its own 10/3 reaches the viewer.
class MoQProxyTrackQuietRefusalTest : public MoQProxyTrackGracefulSwitchTest,
                                      public WithParamInterface<bool> {};

TEST_P(MoQProxyTrackQuietRefusalTest, ARefusedReplacementStreamKeepsOld) {
  const bool midSubgroup = GetParam();
  ON_CALL(*replacementSession_, getNegotiatedVersion())
      .WillByDefault(Return(kVersionDraft18));
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  advanceTime(kReorderGrace / 2);
  if (midSubgroup) {
    EXPECT_TRUE(replacementConsumer_
                    ->beginSubgroup(
                        10,
                        0,
                        kDefaultPriority,
                        TrackConsumer::BeginSubgroupOptions{
                            .beginsWithFirstObject = false})
                    .hasError());
  } else {
    EXPECT_TRUE(replacementConsumer_
                    ->objectStream(
                        ObjectHeader(10, 0, 3, 0, uint64_t(1)),
                        folly::IOBuf::copyBuffer("x"))
                    .hasError());
  }
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);

  advanceTime(2 * kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  EXPECT_CALL(
      *consumer_,
      objectStream(
          AllOf(Field(&ObjectHeader::group, 10), Field(&ObjectHeader::id, 3)),
          _,
          _))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  EXPECT_TRUE(upstreamConsumer_
                  ->objectStream(
                      ObjectHeader(10, 0, 3, 0, uint64_t(1)),
                      folly::IOBuf::copyBuffer("x"))
                  .hasValue());
  Mock::VerifyAndClearExpectations(consumer_.get());
  Mock::VerifyAndClearExpectations(oldHandle_.get());
}

INSTANTIATE_TEST_SUITE_P(
    TheReplacementSentOldsGroup,
    MoQProxyTrackQuietRefusalTest,
    Values(false, true),
    [](const TestParamInfo<bool>& info) {
      return info.param ? "OnAStreamStartingMidSubgroup" : "OnAnObjectStream";
    });

// A replacement behind the track's last group may not have it, so old stays
// until it goes. The lost switch that then carries on fails at the deadline,
// and the viewer goes away.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AQuietReplacementBehindFailsTheSwitchOnceOldGoes) {
  subscribeWithGroup11Open();
  ASSERT_TRUE(finish(*oldSubgroup11_));
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  advanceTime(2 * kReorderGrace);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  Mock::VerifyAndClearExpectations(oldHandle_.get());

  loseUpstream();
  runPending();
  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  advanceTime(kDeadline);

  EXPECT_EQ(switchStats_->lostSwitches.completed, 0);
  EXPECT_EQ(switchStats_->lostSwitches.failed, 1);
  Mock::VerifyAndClearExpectations(consumer_.get());
}

// The replacement's stream header alone doesn't let the old upstream go.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    TheOldUpstreamStaysUntilTheReplacementForwardsAnObject) {
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  auto replacement12 =
      replacementConsumer_->beginSubgroup(12, 0, kDefaultPriority);
  ASSERT_TRUE(replacement12.hasValue());
  EXPECT_FALSE(oldOpens(12));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);
  advanceTime(2 * kReorderGrace);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  Mock::VerifyAndClearExpectations(oldHandle_.get());
  EXPECT_CALL(*oldHandle_, unsubscribe());

  ASSERT_TRUE(replacement12.value()->object(
      0, folly::IOBuf::copyBuffer("x"), noExtensions(), false));
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
}

// The replacement goes away after it served group 13 and before the old
// upstream caught up: the old upstream still fills group 12, takes back the
// groups after 13, and the next replacement takes over from it.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AReplacementLostBeforeTakingOverIsReplacedByTheNext) {
  auto thirdSession = makeSession();
  auto thirdProvider = std::make_shared<TestUpstreamProvider>(thirdSession);
  makeTrackOnManualTime({provider_, replacementProvider_, thirdProvider});
  subscribeWithGroup11Open();
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
  runPending();
  ASSERT_TRUE(replacementOpens(13));

  replacementConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::GOING_AWAY, 0, "going away"});
  runPending();

  EXPECT_TRUE(oldOpens(12));
  EXPECT_FALSE(oldOpens(13));
  EXPECT_TRUE(oldOpens(14));
  EXPECT_TRUE(viewerSubgroups_.contains(14));
  ASSERT_NE(thirdConsumer, nullptr);
  EXPECT_EQ(replacementProvider_->calls, 2);
  ASSERT_TRUE(sendFirstObject(*thirdConsumer, 15));
  EXPECT_FALSE(oldOpens(15));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  ASSERT_TRUE(finish(*oldSubgroups_[14]));
  advanceTime(kReorderGrace);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  ASSERT_TRUE(finish(*oldSubgroups_[12]));
  advanceTime(kReorderGrace);
  EXPECT_EQ(track_->upstreamSession(), thirdSession);
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  EXPECT_EQ(switchStats_->refusedStreams, 0);
}

// The old upstream had datagram group 11 open, and a stream of it arrives
// late, after the replacement that took group 13 went away. It also sends
// group 12 as an object stream.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AfterItsReplacementGoesTheOldUpstreamStillFinishesItsGroups) {
  downstreamHandle_ = subscribeThroughUpstream(oldHandle_, consumer_);
  ASSERT_NE(upstreamConsumer_, nullptr);
  ON_CALL(*consumer_, datagram(_, _, _))
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  ON_CALL(*consumer_, objectStream(_, _, _))
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  auto payload = [] { return folly::IOBuf::copyBuffer("x"); };
  ASSERT_TRUE(upstreamConsumer_->datagram(
      ObjectHeader(11, 0, 0, 0, uint64_t(1)), payload(), false));
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(13));

  replacementConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::GOING_AWAY, 0, "going away"});
  runPending();

  EXPECT_TRUE(
      upstreamConsumer_
          ->datagram(ObjectHeader(11, 0, 1, 0, uint64_t(1)), payload(), true)
          .hasValue());
  EXPECT_TRUE(
      upstreamConsumer_->beginSubgroup(11, 1, kDefaultPriority).hasValue());
  EXPECT_TRUE(upstreamConsumer_
                  ->objectStream(
                      ObjectHeader(12, 0, 0, 0, uint64_t(1)), payload(), false)
                  .hasValue());
  EXPECT_FALSE(oldOpens(13));
  EXPECT_TRUE(oldOpens(14));
  EXPECT_EQ(switchStats_->refusedStreams, 0);
  EXPECT_EQ(switchStats_->refusedDatagrams, 0);
}

// The replacement ends the track before it took over: the old upstream still
// serves the viewer, and the track looks for another replacement.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AReplacementEndingTheTrackBeforeTakingOverIsDropped) {
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  subscribeWithGroup11Open();
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);

  replacementConsumer_->publishDone(
      PublishDone{
          RequestID(100), PublishDoneStatusCode::TRACK_ENDED, 0, "ended"});
  runPending();

  EXPECT_TRUE(oldOpens(12));
  EXPECT_TRUE(viewerSubgroups_.contains(12));
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
  EXPECT_GT(provider_->calls, 2);
  Mock::VerifyAndClearExpectations(consumer_.get());
}

// The switch deadline passes while the replacement, which already served
// group 13, is still subscribing. The old upstream takes back the groups after
// 13.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AReplacementCutOffWhileSubscribingGivesItsGroupsBack) {
  subscribeWithGroup11Open();
  auto lateHandle = makeUpstreamHandle();
  auto subscribeReturned = std::make_shared<bool>(false);
  EXPECT_CALL(*replacementSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [lateHandle, subscribeReturned](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            EXPECT_TRUE(sendFirstObject(*cb, 13));
            co_await folly::coro::co_awaitTry(
                folly::coro::sleep(kCancellationWait));
            *subscribeReturned = true;
            co_return Publisher::SubscribeResult(lateHandle);
          }));
  EXPECT_CALL(*lateHandle, unsubscribe());
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(viewerSubgroups_.contains(13));
  EXPECT_FALSE(oldOpens(14));

  advanceTime(kDeadline);
  runUntil([&] { return *subscribeReturned; });
  runPending();

  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 1);
  EXPECT_FALSE(oldOpens(13));
  EXPECT_TRUE(oldOpens(14));
  EXPECT_TRUE(viewerSubgroups_.contains(14));
}

// The replacement of a lost upstream went away before its first object, an
// object stream for group 12, which starts a switch from it at once. The next
// replacement can't take group 12.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    ASwitchStartedByAFirstObjectStreamCountsItsGroup) {
  auto thirdSession = makeSession();
  auto thirdProvider = std::make_shared<TestUpstreamProvider>(thirdSession);
  makeTrackOnManualTime({provider_, replacementProvider_, thirdProvider});
  ON_CALL(*consumer_, objectStream(_, _, _))
      .WillByDefault(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
  subscribeWithGroup11Open();
  provider_->error = "unavailable";
  expectReplacementSubscribe();
  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  std::shared_ptr<TrackConsumer> thirdConsumer;
  EXPECT_CALL(*thirdSession, subscribe(_, _))
      .WillOnce(Invoke(
          [this, &thirdConsumer](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            thirdConsumer = std::move(cb);
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));
  replacementConsumer_->goaway(Goaway{});

  ASSERT_TRUE(replacementConsumer_->objectStream(
      ObjectHeader(12, 0, 0, 0, uint64_t(1)),
      folly::IOBuf::copyBuffer("x"),
      false));
  runPending();

  ASSERT_EQ(switchStats_->gracefulSwitches.started, 1);
  ASSERT_NE(thirdConsumer, nullptr);
  EXPECT_FALSE(sendFirstObject(*thirdConsumer, 12));
  EXPECT_TRUE(sendFirstObject(*thirdConsumer, 13));
}

// The replacement of a lost upstream goes away before it forwards an object.
// The track switches from it once the lost switch completes, not before.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AReplacementThatWentAwayIsSwitchedFromOnceTheLostSwitchCompletes) {
  auto thirdSession = makeSession();
  auto thirdProvider = std::make_shared<TestUpstreamProvider>(thirdSession);
  makeTrackOnManualTime({provider_, replacementProvider_, thirdProvider});
  subscribeWithGroup11Open();
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
  loseUpstream();
  provider_->error = "closed";
  runPending();
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);

  replacementConsumer_->goaway(Goaway{});
  runPending();
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 0);
  EXPECT_EQ(thirdConsumer, nullptr);
  ASSERT_TRUE(replacementOpens(12));
  runPending();

  EXPECT_EQ(switchStats_->lostSwitches.completed, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 1);
  EXPECT_NE(thirdConsumer, nullptr);
}

// The adopted replacement goes away before it took over. Once it takes over,
// the track switches from it too.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AReplacementThatWentAwayIsSwitchedFromOnceItTakesOver) {
  auto thirdSession = makeSession();
  auto thirdProvider = std::make_shared<TestUpstreamProvider>(thirdSession);
  makeTrackOnManualTime({provider_, replacementProvider_, thirdProvider});
  subscribeWithGroup11Open();
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
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  provider_->error = "closed";

  replacementConsumer_->goaway(Goaway{});
  runPending();
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 1);
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_FALSE(oldOpens(12));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 2);
  EXPECT_NE(thirdConsumer, nullptr);
}

TEST_F(MoQProxyTrackGracefulSwitchTest, ClosingDuringASwitchStopsIt) {
  subscribeWithGroup11Open();
  auto replacementHandle = makeUpstreamHandle();
  expectReplacementSubscribe(replacementHandle);
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_CALL(*replacementHandle, unsubscribe());
  EXPECT_CALL(*oldHandle_, unsubscribe());

  track_->close();

  Mock::VerifyAndClearExpectations(replacementHandle.get());
  EXPECT_TRUE(
      replacementConsumer_->beginSubgroup(13, 0, kDefaultPriority).hasError());
  advanceTime(2 * switchOptions_.defaultGroupDuration);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 0);
  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 0);
  EXPECT_EQ(switchStats_->tailCutoffs.cutoffs, 0);
}

// The old upstream ends the track before the replacement took over.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    TheOldUpstreamEndingTheTrackStopsTheSwitch) {
  subscribeWithGroup11Open();
  auto replacementHandle = makeUpstreamHandle();
  expectReplacementSubscribe(replacementHandle);
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  EXPECT_CALL(*replacementHandle, unsubscribe());
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::TRACK_ENDED)));

  loseUpstream(PublishDoneStatusCode::TRACK_ENDED);

  Mock::VerifyAndClearExpectations(replacementHandle.get());
  EXPECT_EQ(switchStats_->upstreamsLost, 0);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 0);
}

// The old upstream finishes group 11 and ends the track while the replacement
// is still sending group 12, the last one, which its stream header alone, or
// with the first object, made the replacement's. The replacement takes over,
// so the rest of group 12 reaches viewers, and its own end of the track ends
// theirs.
class MoQProxyTrackOldUpstreamEndTest : public MoQProxyTrackGracefulSwitchTest,
                                        public WithParamInterface<bool> {};

TEST_P(
    MoQProxyTrackOldUpstreamEndTest,
    TheOldUpstreamEndingTheTrackLeavesItsLastGroupsToTheReplacement) {
  const bool firstObject = GetParam();
  subscribeWithGroup11Open();
  auto replacementHandle = makeUpstreamHandle();
  expectReplacementSubscribe(replacementHandle);
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(replacementConsumer_, nullptr);
  auto group12 = replacementConsumer_->beginSubgroup(12, 0, kDefaultPriority);
  ASSERT_TRUE(group12.hasValue());
  if (firstObject) {
    ASSERT_TRUE(group12.value()
                    ->object(0, folly::IOBuf::copyBuffer("x"), noExtensions())
                    .hasValue());
  }
  EXPECT_FALSE(oldOpens(12));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  EXPECT_CALL(*replacementHandle, unsubscribe()).Times(0);
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);

  loseUpstream(PublishDoneStatusCode::TRACK_ENDED);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  const uint64_t next = firstObject ? 1 : 0;
  EXPECT_TRUE(group12.value()
                  ->object(next, folly::IOBuf::copyBuffer("y"), noExtensions())
                  .hasValue());
  EXPECT_TRUE(finish(*group12.value()));
  ASSERT_TRUE(viewerSubgroups_.contains(12));
  Mock::VerifyAndClearExpectations(consumer_.get());
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::TRACK_ENDED)));

  replacementConsumer_->publishDone(
      PublishDone{
          RequestID(101), PublishDoneStatusCode::TRACK_ENDED, 1, "ended"});

  Mock::VerifyAndClearExpectations(consumer_.get());
  Mock::VerifyAndClearExpectations(replacementHandle.get());
}

// The same, but the old upstream ends the track while the replacement's
// SUBSCRIBE is still in flight. The replacement takes over once adopted.
TEST_P(
    MoQProxyTrackOldUpstreamEndTest,
    TheOldUpstreamEndingTheTrackWaitsForAReplacementStillSubscribing) {
  const bool firstObject = GetParam();
  subscribeWithGroup11Open();
  auto replacementHandle = makeUpstreamHandle();
  folly::coro::Baton subscribeReply;
  std::shared_ptr<SubgroupConsumer> group12;
  EXPECT_CALL(*replacementSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [this, firstObject, replacementHandle, &subscribeReply, &group12](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            replacementConsumer_ = cb;
            auto subgroup = cb->beginSubgroup(12, 0, kDefaultPriority);
            EXPECT_TRUE(subgroup.hasValue());
            if (subgroup.hasValue()) {
              group12 = subgroup.value();
            }
            if (group12 && firstObject) {
              EXPECT_TRUE(
                  group12
                      ->object(0, folly::IOBuf::copyBuffer("x"), noExtensions())
                      .hasValue());
            }
            co_await subscribeReply;
            co_return Publisher::SubscribeResult(replacementHandle);
          }));
  receiveSessionGoaway();
  runPending();
  ASSERT_NE(group12, nullptr);
  EXPECT_FALSE(oldOpens(12));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  EXPECT_CALL(*replacementHandle, unsubscribe()).Times(0);
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);

  loseUpstream(PublishDoneStatusCode::TRACK_ENDED);
  runPending();
  subscribeReply.post();
  runPending();

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 1);
  const uint64_t next = firstObject ? 1 : 0;
  EXPECT_TRUE(
      group12->object(next, folly::IOBuf::copyBuffer("y"), noExtensions())
          .hasValue());
  EXPECT_TRUE(finish(*group12));
  ASSERT_TRUE(viewerSubgroups_.contains(12));
  Mock::VerifyAndClearExpectations(consumer_.get());
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::TRACK_ENDED)));

  replacementConsumer_->publishDone(
      PublishDone{
          RequestID(101), PublishDoneStatusCode::TRACK_ENDED, 1, "ended"});

  Mock::VerifyAndClearExpectations(consumer_.get());
  Mock::VerifyAndClearExpectations(replacementHandle.get());
}

INSTANTIATE_TEST_SUITE_P(
    TheReplacementTookGroup12,
    MoQProxyTrackOldUpstreamEndTest,
    Values(false, true),
    [](const TestParamInfo<bool>& info) {
      return info.param ? "WithItsFirstObject" : "WithItsStreamHeader";
    });

// The old upstream ends the track while the replacement, which already served
// group 13, is still subscribing, and the switch deadline passes before its
// SUBSCRIBE returns. Viewers get the old upstream's end then.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    TheOldUpstreamsEndReachesViewersIfNoReplacementIsAdoptedByTheDeadline) {
  subscribeWithGroup11Open();
  auto lateHandle = makeUpstreamHandle();
  auto subscribeReturned = std::make_shared<bool>(false);
  EXPECT_CALL(*replacementSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [lateHandle, subscribeReturned](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            EXPECT_TRUE(sendFirstObject(*cb, 13));
            co_await folly::coro::co_awaitTry(
                folly::coro::sleep(kCancellationWait));
            *subscribeReturned = true;
            co_return Publisher::SubscribeResult(lateHandle);
          }));
  EXPECT_CALL(*lateHandle, unsubscribe());
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(viewerSubgroups_.contains(13));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  loseUpstream(PublishDoneStatusCode::TRACK_ENDED);
  runPending();
  Mock::VerifyAndClearExpectations(consumer_.get());
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::TRACK_ENDED)));

  advanceTime(kDeadline);
  runUntil([&] { return *subscribeReturned; });
  runPending();

  Mock::VerifyAndClearExpectations(consumer_.get());
  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 1);
  EXPECT_EQ(switchStats_->gracefulSwitches.completed, 0);
}

// The switch deadline passed while the replacement, which already served group
// 13, is still subscribing, and the old upstream then ends the track. Viewers
// get its end at once.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    TheOldUpstreamsEndAfterTheDeadlineReachesViewersAtOnce) {
  subscribeWithGroup11Open();
  auto lateHandle = makeUpstreamHandle();
  folly::coro::Baton subscribeReply;
  EXPECT_CALL(*replacementSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [lateHandle, &subscribeReply](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            EXPECT_TRUE(sendFirstObject(*cb, 13));
            co_await subscribeReply;
            co_return Publisher::SubscribeResult(lateHandle);
          }));
  EXPECT_CALL(*lateHandle, unsubscribe());
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(viewerSubgroups_.contains(13));
  advanceTime(kDeadline);
  ASSERT_EQ(switchStats_->gracefulSwitches.failed, 1);
  ASSERT_TRUE(finish(*oldSubgroup11_));
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::TRACK_ENDED)));

  loseUpstream(PublishDoneStatusCode::TRACK_ENDED);

  Mock::VerifyAndClearExpectations(consumer_.get());
  subscribeReply.post();
  runPending();
  Mock::VerifyAndClearExpectations(lateHandle.get());
}

// The viewer leaves while the old upstream's end waits for the replacement,
// which served group 13. The old upstream's subscription already ended, so it
// isn't unsubscribed.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AnOldUpstreamWhoseEndWaitsIsNotUnsubscribed) {
  subscribeWithGroup11Open();
  auto lateHandle = makeUpstreamHandle();
  auto subscribeReturned = std::make_shared<bool>(false);
  EXPECT_CALL(*replacementSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [lateHandle, subscribeReturned](
              SubscribeRequest, std::shared_ptr<TrackConsumer> cb)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            auto subgroup = cb->beginSubgroup(13, 0, kDefaultPriority);
            EXPECT_TRUE(
                subgroup.hasValue() &&
                subgroup.value()
                    ->object(0, folly::IOBuf::copyBuffer("x"), noExtensions())
                    .hasValue() &&
                subgroup.value()->endOfSubgroup().hasValue());
            co_await folly::coro::co_awaitTry(
                folly::coro::sleep(kCancellationWait));
            *subscribeReturned = true;
            co_return Publisher::SubscribeResult(lateHandle);
          }));
  EXPECT_CALL(*lateHandle, unsubscribe());
  receiveSessionGoaway();
  runPending();
  ASSERT_TRUE(finish(*oldSubgroup11_));
  loseUpstream(PublishDoneStatusCode::TRACK_ENDED);
  runPending();
  EXPECT_CALL(*oldHandle_, unsubscribe()).Times(0);

  downstreamHandle_->unsubscribe();
  runUntil([&] { return *subscribeReturned; });
  runPending();

  Mock::VerifyAndClearExpectations(oldHandle_.get());
  Mock::VerifyAndClearExpectations(lateHandle.get());
}

// The upstream went away while its SUBSCRIBE was in flight, before the track
// adopted it.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AnUpstreamThatWentAwayIsSwitchedFromOnceAdopted) {
  expectReplacementSubscribe();

  subscribeWithGroup11Open([this] { receiveSessionGoaway(); });
  runPending();

  EXPECT_EQ(switchStats_->gracefulSwitches.started, 1);
  EXPECT_NE(replacementConsumer_, nullptr);
  EXPECT_EQ(switchStats_->upstreamGoaways, 1);
}

// The upstream is lost before the switch on the GOAWAY it got before the track
// adopted it finds a replacement, so its gap counts as one after a GOAWAY.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AnUpstreamLostAfterAnEarlierGoawayLeavesALateReplacementGap) {
  switchOptions_.maxStartDelay = std::chrono::seconds(1);
  makeTrackOnManualTime({provider_, replacementProvider_});
  expectReplacementSubscribe();
  subscribeWithGroup11Open([this] { receiveSessionGoaway(); });
  runPending();
  ASSERT_EQ(switchStats_->gracefulSwitches.started, 1);
  // The seed's start delay isn't zero.
  ASSERT_EQ(replacementConsumer_, nullptr);

  loseUpstream();
  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  ASSERT_TRUE(replacementOpens(12));

  EXPECT_EQ(switchStats_->lateReplacementGaps.gaps, 1);
  EXPECT_EQ(switchStats_->lostGaps.gaps, 0);
}

// The GOAWAY the upstream got while the track subscribed expired before the
// SUBSCRIBE returned, so the track waits for the upstream to go and switches
// from it then.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    AGoawayWithNoTimeLeftLeavesTheSwitchToTheLoss) {
  subscribeWithGroup11Open([this] {
    receiveSessionGoaway(Goaway{.timeout = 20});
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
  });
  runPending();
  EXPECT_EQ(switchStats_->gracefulSwitches.started, 0);
  expectReplacementSubscribe();

  loseUpstream();

  ASSERT_TRUE(runUntilAdopted(replacementSession_));
  EXPECT_EQ(switchStats_->lostSwitches.started, 1);
}

// The switch's start delay and deadline run from the GOAWAY's arrival, here
// while the track subscribed.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    TheDeadlineOfAnEarlierGoawayRunsFromItsArrival) {
  static constexpr std::chrono::milliseconds kBeforeAdopting{50};
  makeTrackOnManualTime({provider_});
  subscribeWithGroup11Open([this] {
    receiveSessionGoaway();
    std::this_thread::sleep_for(kBeforeAdopting);
  });
  ASSERT_EQ(switchStats_->gracefulSwitches.started, 1);

  advanceTime(kDeadline - kBeforeAdopting);

  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 1);
}

// Of a request-stream GOAWAY and a session GOAWAY, the one that expires
// first, the session's, bounds the switch.
TEST_F(
    MoQProxyTrackGracefulSwitchTest,
    TheGoawayThatExpiresFirstBoundsTheSwitch) {
  static constexpr std::chrono::milliseconds kSessionTimeout{100};
  makeTrackOnManualTime({provider_});
  subscribeWithGroup11Open([this] {
    receiveSessionGoaway(Goaway{.timeout = uint64_t(kSessionTimeout.count())});
    upstreamConsumer_->goaway(Goaway{});
  });

  advanceTime(kSessionTimeout);

  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 1);
}

// With no replacement, the track stops looking at the switch deadline, or at
// the GOAWAY's timeout if that is sooner. The old upstream serves until it
// goes, and the viewer then gets GOING_AWAY at once.
struct GracefulDeadlineCase {
  std::string name;
  uint64_t goawayTimeoutMs;
  std::chrono::milliseconds stopsLookingAfter;
};

class MoQProxyTrackGracefulDeadlineTest
    : public MoQProxyTrackGracefulSwitchTest,
      public WithParamInterface<GracefulDeadlineCase> {};

TEST_P(
    MoQProxyTrackGracefulDeadlineTest,
    WithNoReplacementTheOldUpstreamServesUntilItGoes) {
  makeTrackOnManualTime({provider_});
  EXPECT_CALL(*consumer_, publishDone(_)).Times(0);
  subscribeWithGroup11Open();

  receiveSessionGoaway(Goaway{.timeout = GetParam().goawayTimeoutMs});
  advanceTime(GetParam().stopsLookingAfter - std::chrono::milliseconds(1));
  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 0);
  advanceTime(std::chrono::milliseconds(1));
  EXPECT_EQ(switchStats_->gracefulSwitches.failed, 1);
  const auto attempts = provider_->calls;
  EXPECT_GT(attempts, 1);
  advanceTime(std::chrono::milliseconds(50));
  EXPECT_EQ(provider_->calls, attempts);
  EXPECT_TRUE(oldOpens(12));
  EXPECT_TRUE(viewerSubgroups_.contains(12));
  Mock::VerifyAndClearExpectations(consumer_.get());
  EXPECT_CALL(
      *consumer_,
      publishDone(
          Field(&PublishDone::statusCode, PublishDoneStatusCode::GOING_AWAY)))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));

  loseUpstream();

  EXPECT_EQ(switchStats_->lostSwitches.started, 0);
}

INSTANTIATE_TEST_SUITE_P(
    GracefulDeadline,
    MoQProxyTrackGracefulDeadlineTest,
    Values(
        GracefulDeadlineCase{"NoTimeout", 0, kSwitchDeadline},
        GracefulDeadlineCase{
            "ShorterTimeout",
            100,
            std::chrono::milliseconds(100)},
        // The largest a varint holds, too long for the clock.
        GracefulDeadlineCase{
            "TimeoutPastTheClock",
            (uint64_t(1) << 62) - 1,
            kSwitchDeadline}),
    [](const TestParamInfo<GracefulDeadlineCase>& info) {
      return info.param.name;
    });

// The tracks of a session that goes away get its one GOAWAY, and their
// replacement SUBSCRIBEs spread over the start delay rather than go out at
// once: up to a quarter of the GOAWAY's timeout, and at most maxStartDelay.
struct SwitchSpreadCase {
  std::string name;
  uint64_t goawayTimeoutMs;
  std::chrono::milliseconds maxStartDelay;
  std::chrono::milliseconds longestDelay;
};

class MoQProxyTrackSwitchSpreadTest
    : public MoQProxyTrackSwitchTest,
      public WithParamInterface<SwitchSpreadCase> {};

TEST_P(
    MoQProxyTrackSwitchSpreadTest,
    TheTracksOfASessionGoingAwaySpreadTheirSwitches) {
  constexpr size_t kTracks = 20;
  constexpr std::chrono::milliseconds kStep{5};
  const auto longestDelay = GetParam().longestDelay;
  timekeeper_ = std::make_shared<folly::ManualTimekeeper>();
  auto goaways = std::make_shared<MoQUpstreamGoaways>();
  auto random = std::make_shared<std::mt19937_64>(1);
  ON_CALL(*upstreamSession_, subscribe(_, _))
      .WillByDefault(Invoke(
          [this](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));
  size_t replacementSubscribes = 0;
  ON_CALL(*replacementSession_, subscribe(_, _))
      .WillByDefault(Invoke(
          [this, &replacementSubscribes](
              SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            ++replacementSubscribes;
            co_return Publisher::SubscribeResult(makeUpstreamHandle());
          }));
  std::vector<std::shared_ptr<MoQProxyTrack>> tracks;
  std::vector<std::shared_ptr<Publisher::SubscriptionHandle>> viewers;
  for (size_t i = 0; i < kTracks; i++) {
    FullTrackName name{kTrackName.trackNamespace, fmt::format("video{}", i)};
    tracks.push_back(
        MoQProxyTrack::create(
            name,
            {provider_, replacementProvider_},
            {},
            MoQProxyTrack::UpstreamConfig{
                .goaways = goaways,
                .switchStats = switchStats_,
                .switchOptions =
                    {.deadline = std::chrono::seconds(1),
                     .maxStartDelay = GetParam().maxStartDelay},
                .timekeeper = timekeeper_,
                .random = random}));
    auto request = makeSubscribeRequest(RequestID(1));
    request.fullTrackName = name;
    auto viewer = folly::coro::blockingWait(
        tracks.back()->subscribe(
            std::move(request), makeConsumer(), makeDownstreamPeer()),
        &eventBase_);
    ASSERT_TRUE(viewer.hasValue());
    viewers.push_back(viewer.value());
  }

  receiveSessionGoaway(Goaway{.timeout = GetParam().goawayTimeoutMs});
  runPending();
  const auto atOnce = replacementSubscribes;
  std::chrono::milliseconds lastAfter{0};
  for (auto elapsed = kStep; elapsed <= longestDelay; elapsed += kStep) {
    const auto before = replacementSubscribes;
    advanceTime(kStep);
    if (replacementSubscribes > before) {
      lastAfter = elapsed;
    }
  }

  EXPECT_EQ(switchStats_->gracefulSwitches.started, kTracks);
  EXPECT_EQ(replacementSubscribes, kTracks);
  EXPECT_LT(atOnce, kTracks / 4);
  EXPECT_GT(lastAfter, longestDelay / 2);
  for (const auto& track : tracks) {
    track->close();
  }
}

INSTANTIATE_TEST_SUITE_P(
    SwitchSpread,
    MoQProxyTrackSwitchSpreadTest,
    Values(
        SwitchSpreadCase{
            "NoTimeout",
            0,
            std::chrono::milliseconds(100),
            std::chrono::milliseconds(100)},
        SwitchSpreadCase{
            "ShortTimeout",
            200,
            std::chrono::seconds(1),
            std::chrono::milliseconds(50)}),
    [](const TestParamInfo<SwitchSpreadCase>& info) {
      return info.param.name;
    });

TEST(MoQGroupDurationEstimateTest, SmoothsTheSpacingOfGroupStarts) {
  const auto start = std::chrono::steady_clock::now();
  MoQGroupDurationEstimate estimate;
  estimate.onGroupStart(start);
  EXPECT_FALSE(estimate.duration().has_value());

  estimate.onGroupStart(start + std::chrono::milliseconds(100));
  EXPECT_EQ(estimate.duration(), std::chrono::milliseconds(100));
  estimate.onGroupStart(start + std::chrono::milliseconds(300));

  EXPECT_EQ(estimate.duration(), std::chrono::milliseconds(112));
}

// The session history goes to, during a switch on a GOAWAY.
class MoQProxyTrackUpstreamWaitTest : public MoQProxyTrackGracefulSwitchTest {
 protected:
  void SetUp() override {
    MoQProxyTrackGracefulSwitchTest::SetUp();
    subscribeWithGroup11Open();
  }

  // Holds the track while it waits, as a joining FETCH does.
  folly::SemiFuture<std::shared_ptr<MoQSession>> awaitUpstream(
      std::vector<std::shared_ptr<MoQSession>> tried) {
    return folly::coro::co_withExecutor(
               executor_.get(),
               folly::coro::co_invoke(
                   [track = track_, tried = std::move(tried)]() mutable
                       -> folly::coro::Task<std::shared_ptr<MoQSession>> {
                     co_return co_await track->awaitUpstreamSession(
                         std::move(tried));
                   }))
        .start();
  }
};

TEST_F(MoQProxyTrackUpstreamWaitTest, TheUpstreamServesUntilItRefuses) {
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();

  auto upstream = awaitUpstream({});
  runPending();

  ASSERT_TRUE(upstream.isReady());
  EXPECT_EQ(std::move(upstream).get(), upstreamSession_);
}

TEST_F(MoQProxyTrackUpstreamWaitTest, ARefusalWaitsForTheHeldReplacement) {
  replacementProvider_->error = "unavailable";
  receiveSessionGoaway();
  runPending();

  auto upstream = awaitUpstream({upstreamSession_});
  runPending();
  EXPECT_FALSE(upstream.isReady());
  replacementProvider_->error.reset();
  expectReplacementSubscribe();
  advanceTime(std::chrono::milliseconds(40));

  ASSERT_TRUE(upstream.isReady());
  EXPECT_EQ(std::move(upstream).get(), replacementSession_);
  EXPECT_EQ(track_->upstreamSession(), upstreamSession_);
}

TEST_F(
    MoQProxyTrackUpstreamWaitTest,
    TheWaitEndsAtTheDeadlineWithNoReplacement) {
  replacementProvider_->error = "unavailable";
  receiveSessionGoaway();
  runPending();

  auto upstream = awaitUpstream({upstreamSession_});
  advanceTime(kDeadline / 2);
  EXPECT_FALSE(upstream.isReady());
  advanceTime(kDeadline);

  ASSERT_TRUE(upstream.isReady());
  EXPECT_EQ(std::move(upstream).get(), nullptr);
}

// The replacement was tried too: the wait ends at the deadline, though the
// track still holds it.
TEST_F(
    MoQProxyTrackUpstreamWaitTest,
    TheWaitEndsAtTheDeadlineThoughATriedReplacementIsHeld) {
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();

  auto upstream = awaitUpstream({upstreamSession_, replacementSession_});
  advanceTime(kDeadline / 2);
  EXPECT_FALSE(upstream.isReady());
  advanceTime(kDeadline);

  ASSERT_TRUE(upstream.isReady());
  EXPECT_EQ(std::move(upstream).get(), nullptr);
}

// The replacement was tried too, and takes over with no switch from it.
TEST_F(
    MoQProxyTrackUpstreamWaitTest,
    TheWaitEndsWhenATriedReplacementTakesOver) {
  expectReplacementSubscribe();
  receiveSessionGoaway();
  runPending();

  auto upstream = awaitUpstream({upstreamSession_, replacementSession_});
  ASSERT_TRUE(replacementOpens(12));
  EXPECT_FALSE(oldOpens(12));
  ASSERT_TRUE(finish(*oldSubgroup11_));
  runPending();
  EXPECT_FALSE(upstream.isReady());
  advanceTime(kReorderGrace);

  EXPECT_EQ(track_->upstreamSession(), replacementSession_);
  ASSERT_TRUE(upstream.isReady());
  EXPECT_EQ(std::move(upstream).get(), nullptr);
}

// A lost switch adopts a replacement that was tried too: the wait lasts until
// it forwards data, which completes the switch.
TEST_F(MoQProxyTrackUpstreamWaitTest, TheWaitLastsUntilALostSwitchCompletes) {
  expectReplacementSubscribe();
  loseUpstream();
  runPending();
  ASSERT_EQ(track_->upstreamSession(), replacementSession_);

  auto upstream = awaitUpstream({replacementSession_});
  runPending();
  EXPECT_FALSE(upstream.isReady());
  ASSERT_TRUE(sendFirstObject(*replacementConsumer_, 12));
  runPending();

  ASSERT_TRUE(upstream.isReady());
  EXPECT_EQ(std::move(upstream).get(), nullptr);
}

}} // namespace moxygen
