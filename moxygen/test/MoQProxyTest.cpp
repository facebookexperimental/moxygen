/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Invoke.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>

#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQProxy.h"
#include "moxygen/test/MockMoQSession.h"
#include "moxygen/test/Mocks.h"

namespace moxygen { namespace {

using namespace testing;

const FullTrackName kTrackName{TrackNamespace{{"live"}}, "video"};
const FullTrackName kOtherTrackName{TrackNamespace{{"live"}}, "audio"};

Subscriber::PublishResult acceptedPublish(
    std::shared_ptr<TrackConsumer> consumer,
    RequestID requestID) {
  return Subscriber::PublishConsumerAndReplyTask{
      std::move(consumer),
      folly::coro::makeTask<folly::Expected<PublishOk, PublishError>>(
          PublishOk{.requestID = requestID}),
      /*consumerReady=*/true};
}

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
    if (beforeReturn) {
      beforeReturn();
    }
    if (exception) {
      throw std::runtime_error(*exception);
    }
    co_return session_;
  }

  size_t calls{0};
  std::optional<FullTrackName> lastTrackName;
  std::optional<bool> lastFallbackExists;
  std::function<void()> beforeReturn;
  std::optional<std::string> exception;

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

  Fetch makeFetch(RequestID requestID) {
    return Fetch(
        requestID,
        kTrackName,
        AbsoluteLocation{0, 0},
        AbsoluteLocation{1, 0},
        7,
        GroupOrder::OldestFirst);
  }

  PublishRequest makePublish(RequestID requestID) {
    return PublishRequest{
        .requestID = requestID,
        .fullTrackName = kTrackName,
        .trackAlias = TrackAlias(9),
        .groupOrder = GroupOrder::OldestFirst,
        .largest = AbsoluteLocation{10, 2},
        .forward = true};
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

  std::shared_ptr<NiceMock<MockFetchHandle>> makeFetchHandle(
      RequestID requestID) {
    return std::make_shared<NiceMock<MockFetchHandle>>(FetchOk{
        .requestID = requestID,
        .groupOrder = GroupOrder::OldestFirst,
        .endOfTrack = 1,
        .endLocation = AbsoluteLocation{1, 0}});
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

  Publisher::FetchResult fetch(
      std::shared_ptr<MoQSession> session,
      Fetch request,
      std::shared_ptr<FetchConsumer> consumer = nullptr) {
    if (!consumer) {
      consumer = std::make_shared<NiceMock<MockFetchConsumer>>();
    }
    return withSessionContext(std::move(session), [&]() {
      return folly::coro::blockingWait(
          proxy_->fetch(std::move(request), std::move(consumer)), &eventBase_);
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

TEST_F(MoQProxyTest, ForwardsPublish) {
  auto upstreamConsumer = makeConsumer();
  auto handle = makeUpstreamHandle(RequestID(1));
  PublishRequest upstreamRequest;
  std::shared_ptr<Publisher::SubscriptionHandle> upstreamHandle;
  EXPECT_CALL(*upstreamSession_, publish(_, _))
      .WillOnce(
          [&](PublishRequest request,
              std::shared_ptr<Publisher::SubscriptionHandle> publishHandle) {
            upstreamRequest = std::move(request);
            upstreamHandle = std::move(publishHandle);
            return acceptedPublish(upstreamConsumer, RequestID(100));
          });

  auto result = proxy_->publish(makePublish(RequestID(7)), handle);

  ASSERT_TRUE(result.hasValue());
  EXPECT_FALSE(result->consumerReady);
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);
  ASSERT_TRUE(reply.hasValue());
  EXPECT_EQ(reply->requestID, RequestID(7));
  EXPECT_EQ(provider_->calls, 1);
  EXPECT_EQ(provider_->lastTrackName, kTrackName);
  EXPECT_EQ(upstreamRequest.requestID, RequestID(7));
  EXPECT_EQ(upstreamRequest.fullTrackName, kTrackName);
  EXPECT_EQ(upstreamHandle, handle);

  EXPECT_CALL(*upstreamConsumer, setTrackAlias(_)).Times(0);
  EXPECT_TRUE(result->consumer->setTrackAlias(TrackAlias(7)).hasValue());
  EXPECT_CALL(*upstreamConsumer, datagram(_, _, true))
      .WillOnce(Return(folly::unit));
  EXPECT_TRUE(result->consumer
                  ->datagram(
                      ObjectHeader{},
                      folly::IOBuf::copyBuffer("x"),
                      /*lastInGroup=*/true)
                  .hasValue());
}

TEST_F(MoQProxyTest, PublishProviderExceptionIsReturned) {
  provider_->exception = "provider failed";

  auto result = proxy_->publish(
      makePublish(RequestID(9)), makeUpstreamHandle(RequestID(1)));
  ASSERT_TRUE(result.hasValue());
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);

  ASSERT_TRUE(reply.hasError());
  EXPECT_EQ(reply.error().requestID, RequestID(9));
  EXPECT_EQ(reply.error().errorCode, PublishErrorCode::INTERNAL_ERROR);
  EXPECT_THAT(reply.error().reasonPhrase, HasSubstr("provider failed"));
}

TEST_F(MoQProxyTest, CloseDuringPublishSessionLookupReturnsGoingAway) {
  std::weak_ptr<MoQProxy> proxy = proxy_;
  provider_->beforeReturn = [proxy] {
    if (auto locked = proxy.lock()) {
      locked->close();
    }
  };
  EXPECT_CALL(*upstreamSession_, publish(_, _)).Times(0);

  auto result = proxy_->publish(
      makePublish(RequestID(10)), makeUpstreamHandle(RequestID(1)));
  ASSERT_TRUE(result.hasValue());
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);

  ASSERT_TRUE(reply.hasError());
  EXPECT_EQ(reply.error().requestID, RequestID(10));
  EXPECT_EQ(reply.error().errorCode, PublishErrorCode::GOING_AWAY);
}

TEST_F(MoQProxyTest, CloseDuringPublishReplyReturnsGoingAway) {
  auto upstreamConsumer = makeConsumer();
  std::weak_ptr<MoQProxy> proxy = proxy_;
  EXPECT_CALL(*upstreamSession_, publish(_, _))
      .WillOnce([upstreamConsumer, proxy](const PublishRequest& request, auto) {
        auto reply = folly::coro::co_invoke(
            [proxy, requestID = request.requestID]()
                -> folly::coro::Task<folly::Expected<PublishOk, PublishError>> {
              if (auto locked = proxy.lock()) {
                locked->close();
              }
              co_return PublishOk{.requestID = requestID};
            });
        return Subscriber::PublishConsumerAndReplyTask{
            upstreamConsumer, std::move(reply), /*consumerReady=*/true};
      });

  auto result = proxy_->publish(
      makePublish(RequestID(11)), makeUpstreamHandle(RequestID(1)));
  ASSERT_TRUE(result.hasValue());
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);

  ASSERT_TRUE(reply.hasError());
  EXPECT_EQ(reply.error().requestID, RequestID(11));
  EXPECT_EQ(reply.error().errorCode, PublishErrorCode::GOING_AWAY);
}

TEST_F(MoQProxyTest, CloseRejectsPublish) {
  proxy_->close();

  auto result = proxy_->publish(
      makePublish(RequestID(12)), makeUpstreamHandle(RequestID(1)));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().requestID, RequestID(12));
  EXPECT_EQ(result.error().errorCode, PublishErrorCode::GOING_AWAY);
  EXPECT_EQ(provider_->calls, 0);
}

TEST_F(MoQProxyTest, ForwardsFetchThroughCache) {
  auto consumer = std::make_shared<NiceMock<MockFetchConsumer>>();
  auto upstreamHandle = makeFetchHandle(RequestID(100));
  Fetch upstreamRequest;
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [&](Fetch request, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            upstreamRequest = std::move(request);
            co_return upstreamHandle;
          });

  auto result =
      fetch(makeDownstreamSession(), makeFetch(RequestID(7)), consumer);

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(result.value(), upstreamHandle);
  EXPECT_EQ(provider_->calls, 1);
  EXPECT_EQ(provider_->lastTrackName, kTrackName);
  EXPECT_EQ(upstreamRequest.requestID, RequestID(7));
  EXPECT_EQ(upstreamRequest.fullTrackName, kTrackName);
  EXPECT_EQ(upstreamRequest.priority, 7);
  EXPECT_EQ(upstreamRequest.groupOrder, GroupOrder::OldestFirst);
  const auto* range = std::get_if<StandaloneFetch>(&upstreamRequest.args);
  ASSERT_NE(range, nullptr);
  EXPECT_EQ(range->start, AbsoluteLocation(0, 0));
  EXPECT_EQ(range->end, AbsoluteLocation(1, 0));
}

TEST_F(MoQProxyTest, FetchPopulatesFetchCache) {
  auto firstConsumer = std::make_shared<NiceMock<MockFetchConsumer>>();
  auto upstreamHandle = std::make_shared<NiceMock<MockFetchHandle>>(FetchOk{
      .requestID = RequestID(100),
      .groupOrder = GroupOrder::OldestFirst,
      .endOfTrack = 1,
      .endLocation = AbsoluteLocation{0, 1}});
  EXPECT_CALL(*firstConsumer, object(0, 0, 0, _, _, true, false))
      .WillOnce(Return(folly::unit));
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [upstreamHandle](Fetch, std::shared_ptr<FetchConsumer> consumer)
              -> folly::coro::Task<Publisher::FetchResult> {
            auto result = consumer->object(
                0, 0, 0, folly::IOBuf::copyBuffer("x"), {}, true);
            EXPECT_TRUE(result.hasValue());
            co_return upstreamHandle;
          });

  auto first =
      fetch(makeDownstreamSession(), makeFetch(RequestID(1)), firstConsumer);
  ASSERT_TRUE(first.hasValue());

  auto secondConsumer = std::make_shared<NiceMock<MockFetchConsumer>>();
  EXPECT_CALL(*secondConsumer, object(0, 0, 0, _, _, true, false))
      .WillOnce(Return(folly::unit));
  auto second =
      fetch(makeDownstreamSession(), makeFetch(RequestID(2)), secondConsumer);

  ASSERT_TRUE(second.hasValue());
  EXPECT_EQ(second.value()->fetchOk().requestID, RequestID(2));
}

TEST_F(MoQProxyTest, FetchFallsBackToNextProvider) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(fallbackSession);
  proxy_ = MoQProxy::create({provider_, fallbackProvider});

  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [](Fetch fetch, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            co_return folly::makeUnexpected(
                FetchError{
                    fetch.requestID,
                    FetchErrorCode::INTERNAL_ERROR,
                    "unavailable"});
          });
  auto upstreamHandle = makeFetchHandle(RequestID(101));
  EXPECT_CALL(*fallbackSession, fetch(_, _))
      .WillOnce(
          [upstreamHandle](Fetch, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            co_return upstreamHandle;
          });

  auto result = fetch(makeDownstreamSession(), makeFetch(RequestID(8)));

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(result.value()->fetchOk().requestID, RequestID(8));
  EXPECT_EQ(provider_->lastFallbackExists, true);
  EXPECT_EQ(fallbackProvider->lastFallbackExists, false);
}

TEST_F(MoQProxyTest, RejectsJoiningFetch) {
  auto request = Fetch(
      RequestID(9),
      RequestID(1),
      0,
      FetchType::RELATIVE_JOINING,
      7,
      GroupOrder::OldestFirst);
  request.fullTrackName = kTrackName;

  auto result = fetch(makeDownstreamSession(), std::move(request));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().requestID, RequestID(9));
  EXPECT_EQ(result.error().errorCode, FetchErrorCode::NOT_SUPPORTED);
  EXPECT_EQ(provider_->calls, 0);
}

TEST_F(MoQProxyTest, CloseRejectsFetch) {
  proxy_->close();

  auto result = fetch(makeDownstreamSession(), makeFetch(RequestID(10)));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().requestID, RequestID(10));
  EXPECT_EQ(result.error().errorCode, FetchErrorCode::GOING_AWAY);
  EXPECT_EQ(provider_->calls, 0);
}

}} // namespace moxygen
