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
#include <string>
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

// An upstream PUBLISH_DONE status, and whether it means the upstream was lost
// rather than the track ending.
struct UpstreamEndCase {
  std::string name;
  PublishDoneStatusCode statusCode;
  bool lost;
  // Defaults to statusCode.
  std::optional<PublishDoneStatusCode> viewerStatusCode;
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
      publishDone(Field(
          &PublishDone::statusCode,
          upstreamEnd.viewerStatusCode.value_or(upstreamEnd.statusCode))));

  upstreamConsumer_->publishDone(
      PublishDone{RequestID(100), upstreamEnd.statusCode, 0, "upstream done"});

  EXPECT_EQ(switchStats_->upstreamsLost, upstreamEnd.lost ? 1 : 0);
}

INSTANTIATE_TEST_SUITE_P(
    UpstreamPublishDone,
    MoQProxyTrackUpstreamEndTest,
    Values(
        UpstreamEndCase{"GoingAway", PublishDoneStatusCode::GOING_AWAY, true},
        UpstreamEndCase{
            "SessionClosed",
            PublishDoneStatusCode::SESSION_CLOSED,
            true,
            PublishDoneStatusCode::GOING_AWAY},
        UpstreamEndCase{
            "TrackEnded",
            PublishDoneStatusCode::TRACK_ENDED,
            false},
        UpstreamEndCase{
            "SubscriptionEnded",
            PublishDoneStatusCode::SUBSCRIPTION_ENDED,
            false},
        UpstreamEndCase{
            "Unauthorized",
            PublishDoneStatusCode::UNAUTHORIZED,
            false},
        UpstreamEndCase{"Expired", PublishDoneStatusCode::EXPIRED, false},
        UpstreamEndCase{
            "MalformedTrack",
            PublishDoneStatusCode::MALFORMED_TRACK,
            false},
        UpstreamEndCase{
            "TooFarBehind",
            PublishDoneStatusCode::TOO_FAR_BEHIND,
            false},
        UpstreamEndCase{
            "UpdateFailed",
            PublishDoneStatusCode::UPDATE_FAILED,
            false},
        // Another upstream might serve it, but every proxy on this upstream
        // switching at once would only move the load.
        UpstreamEndCase{
            "ExcessiveLoad",
            PublishDoneStatusCode::EXCESSIVE_LOAD,
            false},
        // Kept as a track end, as before: nothing says another relay would
        // fare better.
        UpstreamEndCase{
            "InternalError",
            PublishDoneStatusCode::INTERNAL_ERROR,
            false}),
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

}} // namespace moxygen
