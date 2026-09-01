/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <memory>
#include <optional>
#include <utility>

#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQProxy.h"
#include "moxygen/test/MockMoQSession.h"
#include "moxygen/test/Mocks.h"

namespace moxygen { namespace {

using namespace testing;

const FullTrackName kTrackName{TrackNamespace{{"live"}}, "video"};
const FullTrackName kOtherTrackName{TrackNamespace{{"live"}}, "audio"};

class TestUpstreamProvider final : public MoQUpstreamProvider {
 public:
  explicit TestUpstreamProvider(std::shared_ptr<MoQSession> session)
      : session_(std::move(session)) {}

  folly::coro::Task<MoQUpstreamSessionResult> getSession(
      const FullTrackName& fullTrackName,
      const TrackRequestParameters&,
      bool fallbackExists) override {
    ++calls;
    lastTrackName = fullTrackName;
    lastFallbackExists = fallbackExists;
    co_return session_;
  }

  size_t calls{0};
  std::optional<FullTrackName> lastTrackName;
  std::optional<bool> lastFallbackExists;

 private:
  std::shared_ptr<MoQSession> session_;
};

class MoQProxyTest : public Test {
 protected:
  void SetUp() override {
    executor_ = std::make_shared<MoQFollyExecutorImpl>(&eventBase_);
    upstreamSession_ =
        std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
    ON_CALL(*upstreamSession_, getNegotiatedVersion())
        .WillByDefault(Return(kVersionDraftCurrent));
    provider_ = std::make_shared<TestUpstreamProvider>(upstreamSession_);
    proxy_ = MoQProxy::create({provider_});
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

  SubscribeRequest makeSubscribeRequest(
      RequestID requestID,
      FullTrackName fullTrackName = kTrackName) {
    auto request = SubscribeRequest::make(fullTrackName);
    request.requestID = requestID;
    return request;
  }

  std::shared_ptr<NiceMock<MockSubscriptionHandle>> makeUpstreamHandle(
      RequestID requestID) {
    SubscribeOk subscribeOk{
        .requestID = requestID,
        .trackAlias = TrackAlias(requestID.value),
        .groupOrder = GroupOrder::OldestFirst,
        .largest = AbsoluteLocation{10, 2}};
    return std::make_shared<NiceMock<MockSubscriptionHandle>>(
        std::move(subscribeOk));
  }

  template <typename Func>
  auto withSessionContext(std::shared_ptr<MoQSession> session, Func&& func)
      -> decltype(func()) {
    folly::RequestContextScopeGuard guard;
    folly::RequestContext::get()->setContextData(
        sessionRequestToken(),
        std::make_unique<MoQSession::MoQSessionRequestData>(
            std::move(session)));
    return func();
  }

  Publisher::SubscribeResult subscribe(
      std::shared_ptr<MoQSession> session,
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer = nullptr) {
    if (!consumer) {
      consumer = makeConsumer();
    }
    return withSessionContext(std::move(session), [&]() {
      return folly::coro::blockingWait(
          proxy_->subscribe(std::move(request), std::move(consumer)),
          &eventBase_);
    });
  }

  static const folly::RequestToken& sessionRequestToken() {
    static folly::RequestToken token("moq_session");
    return token;
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_;
  std::shared_ptr<NiceMock<test::MockMoQSession>> upstreamSession_;
  std::shared_ptr<TestUpstreamProvider> provider_;
  std::shared_ptr<MoQProxy> proxy_;
};

TEST_F(MoQProxyTest, EstablishesUpstreamSubscription) {
  auto upstreamHandle = makeUpstreamHandle(RequestID(100));
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));

  auto result =
      subscribe(makeDownstreamSession(), makeSubscribeRequest(RequestID(1)));

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(provider_->calls, 1);
  EXPECT_EQ(provider_->lastTrackName, kTrackName);
  result.value()->unsubscribe();
}

TEST_F(MoQProxyTest, TriesUpstreamProvidersInOrder) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  ON_CALL(*fallbackSession, getNegotiatedVersion())
      .WillByDefault(Return(kVersionDraftCurrent));
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(fallbackSession);
  proxy_ = MoQProxy::create({provider_, fallbackProvider});

  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return folly::makeUnexpected(
                SubscribeError{
                    RequestID(100),
                    SubscribeErrorCode::INTERNAL_ERROR,
                    "primary unavailable"});
          }));
  auto upstreamHandle = makeUpstreamHandle(RequestID(101));
  EXPECT_CALL(*fallbackSession, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));

  auto result =
      subscribe(makeDownstreamSession(), makeSubscribeRequest(RequestID(1)));

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(provider_->calls, 1);
  EXPECT_EQ(fallbackProvider->calls, 1);
  EXPECT_EQ(provider_->lastFallbackExists, true);
  EXPECT_EQ(fallbackProvider->lastFallbackExists, false);
  result.value()->unsubscribe();
}

TEST_F(MoQProxyTest, ReusesTrackForMatchingSubscriptions) {
  auto upstreamHandle = makeUpstreamHandle(RequestID(100));
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));

  auto first =
      subscribe(makeDownstreamSession(), makeSubscribeRequest(RequestID(1)));
  auto second =
      subscribe(makeDownstreamSession(), makeSubscribeRequest(RequestID(2)));

  ASSERT_TRUE(first.hasValue());
  ASSERT_TRUE(second.hasValue());
  EXPECT_EQ(provider_->calls, 1);
  first.value()->unsubscribe();
  second.value()->unsubscribe();
}

TEST_F(MoQProxyTest, CreatesSeparateTracksForDifferentNames) {
  auto videoHandle = makeUpstreamHandle(RequestID(100));
  auto audioHandle = makeUpstreamHandle(RequestID(101));
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [videoHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(videoHandle);
          }))
      .WillOnce(Invoke(
          [audioHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(audioHandle);
          }));

  auto video =
      subscribe(makeDownstreamSession(), makeSubscribeRequest(RequestID(1)));
  auto audio = subscribe(
      makeDownstreamSession(),
      makeSubscribeRequest(RequestID(2), kOtherTrackName));

  ASSERT_TRUE(video.hasValue());
  ASSERT_TRUE(audio.hasValue());
  EXPECT_EQ(provider_->calls, 2);
  video.value()->unsubscribe();
  audio.value()->unsubscribe();
}

TEST_F(MoQProxyTest, RecreatesTrackAfterLastSubscriberLeaves) {
  auto firstUpstreamHandle = makeUpstreamHandle(RequestID(100));
  auto secondUpstreamHandle = makeUpstreamHandle(RequestID(101));
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [firstUpstreamHandle](
              SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(firstUpstreamHandle);
          }))
      .WillOnce(Invoke(
          [secondUpstreamHandle](
              SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(secondUpstreamHandle);
          }));

  auto first =
      subscribe(makeDownstreamSession(), makeSubscribeRequest(RequestID(1)));
  ASSERT_TRUE(first.hasValue());
  EXPECT_CALL(*firstUpstreamHandle, unsubscribe());
  first.value()->unsubscribe();

  auto second =
      subscribe(makeDownstreamSession(), makeSubscribeRequest(RequestID(2)));
  ASSERT_TRUE(second.hasValue());
  EXPECT_EQ(provider_->calls, 2);
  EXPECT_CALL(*secondUpstreamHandle, unsubscribe());
  second.value()->unsubscribe();
}

TEST_F(MoQProxyTest, CloseStopsTracksAndRejectsNewSubscriptions) {
  auto upstreamHandle = makeUpstreamHandle(RequestID(100));
  auto downstreamConsumer = makeConsumer();
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(Invoke(
          [upstreamHandle](SubscribeRequest, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(upstreamHandle);
          }));
  auto result = subscribe(
      makeDownstreamSession(),
      makeSubscribeRequest(RequestID(1)),
      downstreamConsumer);
  ASSERT_TRUE(result.hasValue());

  EXPECT_CALL(*upstreamHandle, unsubscribe());
  EXPECT_CALL(*downstreamConsumer, publishDone(_));
  proxy_->close();

  auto rejected =
      subscribe(makeDownstreamSession(), makeSubscribeRequest(RequestID(2)));
  ASSERT_TRUE(rejected.hasError());
  EXPECT_EQ(rejected.error().requestID, RequestID(2));
  EXPECT_EQ(rejected.error().errorCode, SubscribeErrorCode::GOING_AWAY);
  EXPECT_EQ(provider_->calls, 1);
}

}} // namespace moxygen
