/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Invoke.h>
#include <folly/futures/ManualTimekeeper.h>
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
#include "moxygen/proxy/test/RecordingFetchConsumer.h"
#include "moxygen/test/LiveViewerConsumer.h"
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

  Subscriber::PublishResult publish(
      std::shared_ptr<MoQSession> session,
      PublishRequest request,
      std::shared_ptr<Publisher::SubscriptionHandle> handle) {
    return withSessionContext(std::move(session), [&]() {
      return proxy_->publish(std::move(request), std::move(handle));
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

  auto result =
      publish(makeDownstreamSession(), makePublish(RequestID(7)), handle);

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

TEST_F(MoQProxyTest, PublishFallsBackToNextProvider) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(fallbackSession);
  proxy_ = MoQProxy::create({provider_, fallbackProvider});

  EXPECT_CALL(*upstreamSession_, publish(_, _))
      .WillOnce([](const PublishRequest& request, auto) {
        return Subscriber::PublishResult(
            folly::makeUnexpected(
                PublishError{
                    request.requestID,
                    PublishErrorCode::INTERNAL_ERROR,
                    "unavailable"}));
      });
  auto upstreamConsumer = makeConsumer();
  EXPECT_CALL(*fallbackSession, publish(_, _))
      .WillOnce([upstreamConsumer](const PublishRequest&, auto) {
        return acceptedPublish(upstreamConsumer, RequestID(101));
      });

  auto result = publish(
      makeDownstreamSession(),
      makePublish(RequestID(8)),
      makeUpstreamHandle(RequestID(1)));
  ASSERT_TRUE(result.hasValue());
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);

  ASSERT_TRUE(reply.hasValue());
  EXPECT_EQ(reply->requestID, RequestID(8));
  EXPECT_EQ(provider_->lastFallbackExists, true);
  EXPECT_EQ(fallbackProvider->lastFallbackExists, false);
}

TEST_F(MoQProxyTest, PublishReplyErrorFallsBackToNextProvider) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(fallbackSession);
  proxy_ = MoQProxy::create({provider_, fallbackProvider});

  auto rejectedConsumer = makeConsumer();
  EXPECT_CALL(*upstreamSession_, publish(_, _))
      .WillOnce([rejectedConsumer](const PublishRequest& request, auto) {
        return Subscriber::PublishConsumerAndReplyTask{
            rejectedConsumer,
            folly::coro::makeTask<folly::Expected<PublishOk, PublishError>>(
                folly::makeUnexpected(
                    PublishError{
                        request.requestID,
                        PublishErrorCode::INTERNAL_ERROR,
                        "rejected"})),
            /*consumerReady=*/true};
      });
  auto fallbackConsumer = makeConsumer();
  EXPECT_CALL(*fallbackSession, publish(_, _))
      .WillOnce([fallbackConsumer](const PublishRequest&, auto) {
        return acceptedPublish(fallbackConsumer, RequestID(101));
      });

  auto result = publish(
      makeDownstreamSession(),
      makePublish(RequestID(9)),
      makeUpstreamHandle(RequestID(1)));
  ASSERT_TRUE(result.hasValue());
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);

  ASSERT_TRUE(reply.hasValue());
  EXPECT_EQ(reply->requestID, RequestID(9));
  EXPECT_CALL(*rejectedConsumer, datagram(_, _, _)).Times(0);
  EXPECT_CALL(*fallbackConsumer, datagram(_, _, true))
      .WillOnce(Return(folly::unit));
  EXPECT_TRUE(result->consumer
                  ->datagram(
                      ObjectHeader{},
                      folly::IOBuf::copyBuffer("x"),
                      /*lastInGroup=*/true)
                  .hasValue());
}

TEST_F(MoQProxyTest, PublishSkipsDownstreamSession) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(fallbackSession);
  proxy_ = MoQProxy::create({provider_, fallbackProvider});

  EXPECT_CALL(*upstreamSession_, publish(_, _)).Times(0);
  auto fallbackConsumer = makeConsumer();
  EXPECT_CALL(*fallbackSession, publish(_, _))
      .WillOnce([fallbackConsumer](const PublishRequest&, auto) {
        return acceptedPublish(fallbackConsumer, RequestID(101));
      });

  auto result = publish(
      upstreamSession_,
      makePublish(RequestID(10)),
      makeUpstreamHandle(RequestID(1)));
  ASSERT_TRUE(result.hasValue());
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);

  ASSERT_TRUE(reply.hasValue());
  EXPECT_EQ(reply->requestID, RequestID(10));
  EXPECT_EQ(provider_->lastFallbackExists, true);
  EXPECT_EQ(fallbackProvider->lastFallbackExists, false);
}

TEST_F(MoQProxyTest, PublishProviderExceptionIsReturned) {
  provider_->exception = "provider failed";

  auto result = publish(
      makeDownstreamSession(),
      makePublish(RequestID(9)),
      makeUpstreamHandle(RequestID(1)));
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

  auto result = publish(
      makeDownstreamSession(),
      makePublish(RequestID(10)),
      makeUpstreamHandle(RequestID(1)));
  ASSERT_TRUE(result.hasValue());
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);

  ASSERT_TRUE(reply.hasError());
  EXPECT_EQ(reply.error().requestID, RequestID(10));
  EXPECT_EQ(reply.error().errorCode, PublishErrorCode::GOING_AWAY);
}

TEST_F(MoQProxyTest, CloseDuringPublishReplyReturnsGoingAway) {
  auto upstreamConsumer = makeConsumer();
  std::weak_ptr<MoQProxy> proxy = proxy_;
  EXPECT_CALL(*upstreamConsumer, publishDone(_))
      .WillOnce(Return(folly::makeExpected<MoQPublishError>(folly::unit)));
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

  auto result = publish(
      makeDownstreamSession(),
      makePublish(RequestID(11)),
      makeUpstreamHandle(RequestID(1)));
  ASSERT_TRUE(result.hasValue());
  auto reply = folly::coro::blockingWait(std::move(result->reply), &eventBase_);

  ASSERT_TRUE(reply.hasError());
  EXPECT_EQ(reply.error().requestID, RequestID(11));
  EXPECT_EQ(reply.error().errorCode, PublishErrorCode::GOING_AWAY);
}

TEST_F(MoQProxyTest, CloseRejectsPublish) {
  proxy_->close();

  auto result = publish(
      makeDownstreamSession(),
      makePublish(RequestID(12)),
      makeUpstreamHandle(RequestID(1)));

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

// Standalone FETCH is pure pass-through: nothing is cached or served
// locally, so a repeated request reaches upstream again.
TEST_F(MoQProxyTest, StandaloneFetchesAlwaysReachUpstream) {
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .Times(2)
      .WillRepeatedly(
          [this](Fetch request, std::shared_ptr<FetchConsumer> consumer)
              -> folly::coro::Task<Publisher::FetchResult> {
            EXPECT_TRUE(consumer
                            ->object(
                                0,
                                0,
                                0,
                                folly::IOBuf::copyBuffer("x"),
                                noExtensions(),
                                /*finFetch=*/true)
                            .hasValue());
            co_return makeFetchHandle(request.requestID);
          });
  auto firstConsumer = std::make_shared<NiceMock<MockFetchConsumer>>();
  auto secondConsumer = std::make_shared<NiceMock<MockFetchConsumer>>();
  EXPECT_CALL(*firstConsumer, object(0, 0, 0, _, _, true, false))
      .WillOnce(Return(folly::unit));
  EXPECT_CALL(*secondConsumer, object(0, 0, 0, _, _, true, false))
      .WillOnce(Return(folly::unit));

  auto first =
      fetch(makeDownstreamSession(), makeFetch(RequestID(1)), firstConsumer);
  auto second =
      fetch(makeDownstreamSession(), makeFetch(RequestID(2)), secondConsumer);

  EXPECT_TRUE(first.hasValue());
  EXPECT_TRUE(second.hasValue());
  EXPECT_EQ(provider_->calls, 2);
  EXPECT_EQ(proxy_->findTrack(kTrackName), nullptr);
  EXPECT_EQ(proxy_->cacheBudget()->used(), 0);
}

// Once an upstream exposed any part of its response, another provider would
// duplicate or contradict it, so its failure is final.
TEST_F(MoQProxyTest, StandaloneFetchDoesNotRetryAfterExposure) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(fallbackSession);
  proxy_ = MoQProxy::create({provider_, fallbackProvider});
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [](Fetch request, std::shared_ptr<FetchConsumer> consumer)
              -> folly::coro::Task<Publisher::FetchResult> {
            consumer->object(0, 0, 0, folly::IOBuf::copyBuffer("x"));
            co_return folly::makeUnexpected(
                FetchError{
                    request.requestID,
                    FetchErrorCode::INTERNAL_ERROR,
                    "failed midway"});
          });
  EXPECT_CALL(*fallbackSession, fetch(_, _)).Times(0);

  auto result = fetch(makeDownstreamSession(), makeFetch(RequestID(8)));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().requestID, RequestID(8));
  EXPECT_EQ(fallbackProvider->calls, 0);
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
  EXPECT_EQ(result.value(), upstreamHandle);
  EXPECT_EQ(provider_->lastFallbackExists, true);
  EXPECT_EQ(fallbackProvider->lastFallbackExists, false);
}

TEST_F(MoQProxyTest, CloseDuringFetchCancelsUpstream) {
  auto upstreamHandle = makeFetchHandle(RequestID(101));
  std::weak_ptr<MoQProxy> proxy = proxy_;
  EXPECT_CALL(*upstreamHandle, fetchCancel());
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [upstreamHandle, proxy](Fetch, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            if (auto locked = proxy.lock()) {
              locked->close();
            }
            co_return upstreamHandle;
          });

  auto result = fetch(makeDownstreamSession(), makeFetch(RequestID(9)));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().requestID, RequestID(9));
  EXPECT_EQ(result.error().errorCode, FetchErrorCode::GOING_AWAY);
}

TEST_F(MoQProxyTest, JoiningFetchWithoutSubscriptionIsRejected) {
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
  EXPECT_EQ(
      result.error().errorCode, FetchErrorCode::INVALID_JOINING_REQUEST_ID);
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

using namespace std::chrono_literals;

Payload payloadOf(size_t length) {
  return folly::IOBuf::copyBuffer(std::string(length, 'x'));
}

class MoQProxyJoiningTest : public MoQProxyTest {
 protected:
  void SetUp() override {
    MoQProxyTest::SetUp();
    proxy_ = MoQProxy::create({provider_}, makeOptions());
  }

  void TearDown() override {
    proxy_->close();
    drive();
  }

  MoQProxyOptions makeOptions() {
    MoQProxyOptions options;
    options.timekeeper = timekeeper_;
    options.cacheCompletionWait = 100ms;
    options.replayStallTimeout = 5s;
    return options;
  }

  std::shared_ptr<NiceMock<MockSubscriptionHandle>> makeUpstreamHandleAt(
      RequestID requestID,
      std::optional<AbsoluteLocation> largest) {
    SubscribeOk subscribeOk{
        .requestID = requestID,
        .trackAlias = TrackAlias(requestID.value),
        .groupOrder = GroupOrder::OldestFirst,
        .largest = largest};
    if (upstreamGroupOrder_) {
      subscribeOk.extensions.insertMutableExtension(Extension(
          kPublisherGroupOrderExtensionType,
          folly::to_underlying(*upstreamGroupOrder_)));
    }
    return std::make_shared<NiceMock<MockSubscriptionHandle>>(
        std::move(subscribeOk));
  }

  // The first viewer's subscription establishes the upstream one; data then
  // arrives through the consumer the proxy handed upstream.
  void establish(
      std::optional<AbsoluteLocation> upstreamLargest = AbsoluteLocation{
          40,
          0}) {
    EXPECT_CALL(*upstreamSession_, subscribe(_, _))
        .WillOnce(
            [this, upstreamLargest](
                SubscribeRequest request,
                std::shared_ptr<TrackConsumer> consumer)
                -> folly::coro::Task<Publisher::SubscribeResult> {
              upstreamConsumer_ = std::move(consumer);
              co_return Publisher::SubscribeResult(
                  makeUpstreamHandleAt(request.requestID, upstreamLargest));
            });
    auto result = subscribe(
        firstViewer_, makeSubscribeRequest(RequestID(1)), firstViewerLive_);
    ASSERT_TRUE(result.hasValue());
    firstHandle_ = std::move(result.value());
  }

  std::shared_ptr<SubgroupConsumer> openSubgroup(
      uint64_t group,
      bool containsLastInGroup = true) {
    TrackConsumer::BeginSubgroupOptions options;
    options.containsLastInGroup = containsLastInGroup;
    auto res =
        upstreamConsumer_->beginSubgroup(group, 0, kDefaultPriority, options);
    EXPECT_TRUE(res.hasValue());
    return res.hasValue() ? res.value() : nullptr;
  }

  // Objects [0, count) of a group, ended by the last-in-group flag and a
  // clean FIN. Every upstream callback must succeed.
  void publishGroup(uint64_t group, uint64_t count) {
    auto subgroup = openSubgroup(group);
    for (uint64_t objectID = 0; objectID < count; objectID++) {
      EXPECT_TRUE(subgroup
                      ->object(
                          objectID,
                          payloadOf(10),
                          noExtensions(),
                          objectID + 1 == count)
                      .hasValue());
    }
  }

  // A viewer joining the established track: its Joining Location is the
  // track's Largest at that moment.
  std::shared_ptr<NiceMock<test::MockMoQSession>> addViewer(
      RequestID requestID,
      std::shared_ptr<TrackConsumer> live = nullptr,
      bool forward = true) {
    auto viewer = makeDownstreamSession();
    auto request = makeSubscribeRequest(requestID);
    request.forward = forward;
    auto result = subscribe(
        viewer,
        std::move(request),
        live ? std::move(live) : std::make_shared<test::LiveViewerConsumer>());
    EXPECT_TRUE(result.hasValue());
    if (result.hasValue()) {
      viewerHandles_.push_back(std::move(result.value()));
    }
    return viewer;
  }

  static Fetch joiningFetch(
      RequestID fetchID,
      RequestID joiningID,
      uint64_t joiningStart,
      FetchType fetchType = FetchType::ABSOLUTE_JOINING,
      GroupOrder groupOrder = GroupOrder::OldestFirst) {
    Fetch request(fetchID, joiningID, joiningStart, fetchType, 9, groupOrder);
    request.fullTrackName = kTrackName;
    return request;
  }

  folly::SemiFuture<Publisher::FetchResult> startFetch(
      std::shared_ptr<MoQSession> session,
      Fetch request,
      std::shared_ptr<FetchConsumer> consumer) {
    return withSessionContext(std::move(session), [&]() {
      return folly::coro::co_withExecutor(
                 executor_.get(),
                 proxy_->fetch(std::move(request), std::move(consumer)))
          .start();
    });
  }

  // Runs everything ready now. Waiting joins and replays hold keep-alives, so
  // loop() would not return.
  void drive() {
    do {
      eventBase_.loopOnce(EVLOOP_NONBLOCK);
    } while (eventBase_.getNumLoopCallbacks() > 0 ||
             eventBase_.getNotificationQueueSize() > 0);
  }

  std::shared_ptr<MoQGroupCache> cache() {
    auto track = proxy_->findTrack(kTrackName);
    return track ? track->groupCache() : nullptr;
  }

  std::shared_ptr<folly::ManualTimekeeper> timekeeper_{
      std::make_shared<folly::ManualTimekeeper>()};
  // The publisher group order the upstream SUBSCRIBE_OK advertises, if any.
  std::optional<GroupOrder> upstreamGroupOrder_;
  std::shared_ptr<NiceMock<test::MockMoQSession>> firstViewer_{
      makeDownstreamSession()};
  std::shared_ptr<test::LiveViewerConsumer> firstViewerLive_{
      std::make_shared<test::LiveViewerConsumer>()};
  std::shared_ptr<Publisher::SubscriptionHandle> firstHandle_;
  std::vector<std::shared_ptr<Publisher::SubscriptionHandle>> viewerHandles_;
  std::shared_ptr<TrackConsumer> upstreamConsumer_;
};

std::vector<std::string> expectedReplay(
    std::vector<std::string> objects,
    std::string last = "endOfFetch") {
  objects.push_back(std::move(last));
  return objects;
}

TEST_F(MoQProxyJoiningTest, WarmJoinIsServedFromCacheWithoutUpstream) {
  establish();
  publishGroup(41, 2);
  publishGroup(42, 2);
  auto active = openSubgroup(43, /*containsLastInGroup=*/false);
  for (uint64_t objectID = 0; objectID < 3; objectID++) {
    EXPECT_TRUE(active->object(objectID, payloadOf(10), noExtensions(), false)
                    .hasValue());
  }
  auto viewer = addViewer(RequestID(5));
  EXPECT_CALL(*upstreamSession_, fetch(_, _)).Times(0);
  auto consumer = std::make_shared<test::RecordingFetchConsumer>();

  auto result =
      fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 41), consumer);
  drive();

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(result.value()->fetchOk().endLocation, AbsoluteLocation(43, 3));
  EXPECT_EQ(result.value()->fetchOk().groupOrder, GroupOrder::OldestFirst);
  EXPECT_EQ(
      consumer->log,
      expectedReplay(
          {"object 41/0 10",
           "object 41/1 10",
           "object 42/0 10",
           "object 42/1 10",
           "object 43/0 10",
           "object 43/1 10",
           "object 43/2 10"}));
  EXPECT_EQ(proxy_->joinStats().immediateHits, 1);
}

// Viewers that join while the object at their Joining Location is still
// arriving wait for it locally instead of fetching history upstream. Live
// delivery then continues strictly after that location.
TEST_F(MoQProxyJoiningTest, JoinsMidObjectWaitForLocalCompletion) {
  establish();
  publishGroup(41, 2);
  publishGroup(42, 2);
  auto active = openSubgroup(43, /*containsLastInGroup=*/false);
  for (uint64_t objectID = 0; objectID < 2; objectID++) {
    EXPECT_TRUE(active->object(objectID, payloadOf(10), noExtensions(), false)
                    .hasValue());
  }
  EXPECT_TRUE(
      active->beginObject(2, 20, payloadOf(5), noExtensions()).hasValue());
  auto firstLive = std::make_shared<test::LiveViewerConsumer>();
  auto firstViewer = addViewer(RequestID(5), firstLive);
  auto secondViewer = addViewer(RequestID(5));
  EXPECT_CALL(*upstreamSession_, fetch(_, _)).Times(0);
  auto firstConsumer = std::make_shared<test::RecordingFetchConsumer>();
  auto secondConsumer = std::make_shared<test::RecordingFetchConsumer>();

  auto first = startFetch(
      firstViewer, joiningFetch(RequestID(7), RequestID(5), 41), firstConsumer);
  auto second = startFetch(
      secondViewer,
      joiningFetch(RequestID(7), RequestID(5), 41),
      secondConsumer);
  drive();
  EXPECT_FALSE(first.isReady());
  EXPECT_FALSE(second.isReady());
  EXPECT_TRUE(active->objectPayload(payloadOf(15), false).hasValue());
  drive();
  EXPECT_TRUE(
      active->object(3, payloadOf(10), noExtensions(), false).hasValue());
  openSubgroup(44)->object(0, payloadOf(10), noExtensions(), false);
  drive();

  ASSERT_TRUE(first.isReady());
  ASSERT_TRUE(second.isReady());
  EXPECT_TRUE(std::move(first).get().hasValue());
  EXPECT_TRUE(std::move(second).get().hasValue());
  const auto history = expectedReplay(
      {"object 41/0 10",
       "object 41/1 10",
       "object 42/0 10",
       "object 42/1 10",
       "object 43/0 10",
       "object 43/1 10",
       "object 43/2 20"});
  EXPECT_EQ(firstConsumer->log, history);
  EXPECT_EQ(secondConsumer->log, history);
  const std::vector<AbsoluteLocation> live{{43, 3}, {44, 0}};
  EXPECT_EQ(firstLive->received, live);
  EXPECT_EQ(proxy_->joinStats().hitsAfterWait, 2);
  EXPECT_EQ(proxy_->joinStats().fallbacksWaitExpired, 0);
}

TEST_F(MoQProxyJoiningTest, ExpiredWaitFallsBackToOneUpstreamFetch) {
  establish();
  publishGroup(41, 2);
  auto active = openSubgroup(42, /*containsLastInGroup=*/false);
  EXPECT_TRUE(
      active->object(0, payloadOf(10), noExtensions(), false).hasValue());
  EXPECT_TRUE(
      active->beginObject(1, 20, payloadOf(5), noExtensions()).hasValue());
  auto viewer = addViewer(RequestID(5));
  Fetch upstreamRequest;
  auto upstreamHandle = makeFetchHandle(RequestID(200));
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [&](Fetch request, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            upstreamRequest = std::move(request);
            co_return upstreamHandle;
          });
  auto consumer = std::make_shared<test::RecordingFetchConsumer>();

  auto pending = startFetch(
      viewer, joiningFetch(RequestID(7), RequestID(5), 41), consumer);
  drive();
  EXPECT_FALSE(pending.isReady());
  timekeeper_->advance(100ms);
  drive();

  ASSERT_TRUE(pending.isReady());
  auto result = std::move(pending).get();
  ASSERT_TRUE(result.hasValue());
  const auto* range = std::get_if<StandaloneFetch>(&upstreamRequest.args);
  ASSERT_NE(range, nullptr);
  EXPECT_EQ(range->start, AbsoluteLocation(41, 0));
  EXPECT_EQ(range->end, AbsoluteLocation(42, 2));
  EXPECT_TRUE(consumer->log.empty());
  EXPECT_EQ(proxy_->joinStats().fallbacksWaitExpired, 1);
  EXPECT_CALL(*upstreamHandle, fetchCancel());
  result.value()->fetchCancel();
}

// The completion wait is one absolute deadline: an object that needs longer
// to arrive falls back even while it is making progress.
TEST_F(MoQProxyJoiningTest, SlowObjectIsServedLocallyOnlyWithinTheDeadline) {
  for (auto wait : {100ms, 1000ms}) {
    SCOPED_TRACE(wait.count());
    proxy_->close();
    auto options = makeOptions();
    options.cacheCompletionWait = wait;
    proxy_ = MoQProxy::create({provider_}, options);
    establish();
    auto active = openSubgroup(41, /*containsLastInGroup=*/false);
    EXPECT_TRUE(
        active->beginObject(0, 90, payloadOf(10), noExtensions()).hasValue());
    auto viewer = addViewer(RequestID(5));
    size_t upstreamFetches = 0;
    EXPECT_CALL(*upstreamSession_, fetch(_, _))
        .WillRepeatedly(
            [&](const Fetch& request, std::shared_ptr<FetchConsumer>) {
              upstreamFetches++;
              return folly::coro::makeTask(
                  Publisher::FetchResult(makeFetchHandle(request.requestID)));
            });
    auto consumer = std::make_shared<test::RecordingFetchConsumer>();

    auto pending = startFetch(
        viewer, joiningFetch(RequestID(7), RequestID(5), 41), consumer);
    // The object's remaining payload arrives over 800 ms.
    for (int step = 0; step < 8; step++) {
      timekeeper_->advance(100ms);
      drive();
      EXPECT_TRUE(active->objectPayload(payloadOf(10), false).hasValue());
    }
    drive();

    ASSERT_TRUE(pending.isReady());
    EXPECT_TRUE(std::move(pending).get().hasValue());
    if (wait < 800ms) {
      EXPECT_EQ(upstreamFetches, 1);
      EXPECT_TRUE(consumer->log.empty());
      EXPECT_EQ(proxy_->joinStats().fallbacksWaitExpired, 1);
    } else {
      EXPECT_EQ(upstreamFetches, 0);
      EXPECT_EQ(consumer->log, expectedReplay({"object 41/0 90"}));
      EXPECT_EQ(proxy_->joinStats().hitsAfterWait, 1);
    }
    testing::Mock::VerifyAndClearExpectations(upstreamSession_.get());
    viewerHandles_.clear();
    firstHandle_.reset();
    drive();
  }
}

TEST_F(MoQProxyJoiningTest, UncachedHistoryGoesUpstreamWithoutWaiting) {
  establish();
  auto active = openSubgroup(43, /*containsLastInGroup=*/false);
  for (uint64_t objectID = 0; objectID < 3; objectID++) {
    EXPECT_TRUE(active->object(objectID, payloadOf(10), noExtensions(), false)
                    .hasValue());
  }
  auto viewer = addViewer(RequestID(5));
  Fetch upstreamRequest;
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [&](Fetch request, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            upstreamRequest = std::move(request);
            co_return makeFetchHandle(RequestID(200));
          });

  auto result = fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 10));

  ASSERT_TRUE(result.hasValue());
  const auto* range = std::get_if<StandaloneFetch>(&upstreamRequest.args);
  ASSERT_NE(range, nullptr);
  EXPECT_EQ(range->start, AbsoluteLocation(10, 0));
  EXPECT_EQ(range->end, AbsoluteLocation(43, 3));
  EXPECT_EQ(upstreamRequest.priority, 9);
  EXPECT_EQ(upstreamRequest.groupOrder, GroupOrder::OldestFirst);
  EXPECT_EQ(upstreamRequest.fullTrackName, kTrackName);
  EXPECT_EQ(proxy_->joinStats().fallbacksMiss, 1);
  EXPECT_EQ(proxy_->joinStats().completionWaitTime, 0ms);
}

// Nothing has been written when the plan cannot be charged, so the join can
// still use the one upstream source.
TEST_F(MoQProxyJoiningTest, UnchargeablePlanFallsBackBeforeAnyWrite) {
  auto options = makeOptions();
  // Room for one cached group of one small object, not for its replay plan.
  options.maxCacheBytes = 380;
  proxy_ = MoQProxy::create({provider_}, options);
  establish();
  publishGroup(41, 1);
  auto viewer = addViewer(RequestID(5));
  ASSERT_EQ(cache()->inspect({41, 0}, {41, 1}), MoQGroupCache::Coverage::Ready);
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [this](Fetch request, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            co_return makeFetchHandle(request.requestID);
          });
  auto consumer = std::make_shared<test::RecordingFetchConsumer>();

  auto result =
      fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 41), consumer);

  ASSERT_TRUE(result.hasValue());
  EXPECT_TRUE(consumer->log.empty());
  EXPECT_EQ(proxy_->joinStats().fallbacksPlanRejected, 1);
}

// A FETCH without a group order is replayed oldest first even on a
// newest-first track, matching how MoQSession writes and reads it. An
// explicit order is honoured.
TEST_F(MoQProxyJoiningTest, DefaultOrderJoinIsOldestFirstOnNewestFirstTrack) {
  upstreamGroupOrder_ = GroupOrder::NewestFirst;
  establish();
  publishGroup(41, 1);
  publishGroup(42, 1);
  auto viewer = addViewer(RequestID(5));
  EXPECT_CALL(*upstreamSession_, fetch(_, _)).Times(0);
  auto defaultOrder = std::make_shared<test::RecordingFetchConsumer>();
  auto newestFirst = std::make_shared<test::RecordingFetchConsumer>();

  auto defaultResult = fetch(
      viewer,
      joiningFetch(
          RequestID(7),
          RequestID(5),
          41,
          FetchType::ABSOLUTE_JOINING,
          GroupOrder::Default),
      defaultOrder);
  auto newestResult = fetch(
      viewer,
      joiningFetch(
          RequestID(8),
          RequestID(5),
          41,
          FetchType::ABSOLUTE_JOINING,
          GroupOrder::NewestFirst),
      newestFirst);
  drive();

  ASSERT_TRUE(defaultResult.hasValue());
  ASSERT_TRUE(newestResult.hasValue());
  EXPECT_EQ(
      defaultResult.value()->fetchOk().groupOrder, GroupOrder::OldestFirst);
  EXPECT_EQ(
      newestResult.value()->fetchOk().groupOrder, GroupOrder::NewestFirst);
  EXPECT_EQ(
      defaultOrder->log, expectedReplay({"object 41/0 10", "object 42/0 10"}));
  EXPECT_EQ(
      newestFirst->log, expectedReplay({"object 42/0 10", "object 41/0 10"}));
}

TEST_F(MoQProxyJoiningTest, RelativeOffsetPastGroupZeroStartsAtGroupZero) {
  establish();
  openSubgroup(43, false)->object(0, payloadOf(10), noExtensions(), false);
  auto viewer = addViewer(RequestID(5));
  Fetch upstreamRequest;
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [&](Fetch request, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            upstreamRequest = std::move(request);
            co_return makeFetchHandle(RequestID(200));
          });

  auto result = fetch(
      viewer,
      joiningFetch(
          RequestID(7), RequestID(5), 100, FetchType::RELATIVE_JOINING));

  ASSERT_TRUE(result.hasValue());
  const auto* range = std::get_if<StandaloneFetch>(&upstreamRequest.args);
  ASSERT_NE(range, nullptr);
  EXPECT_EQ(range->start, AbsoluteLocation(0, 0));
  EXPECT_EQ(range->end, AbsoluteLocation(43, 1));
}

TEST_F(MoQProxyJoiningTest, InvalidJoinsGetVersionedErrors) {
  establish();
  openSubgroup(43, false)->object(0, payloadOf(10), noExtensions(), false);
  auto viewer = addViewer(RequestID(5));
  auto pausedViewer = addViewer(RequestID(6), nullptr, /*forward=*/false);
  EXPECT_CALL(*upstreamSession_, fetch(_, _)).Times(0);

  auto wrongID = fetch(viewer, joiningFetch(RequestID(7), RequestID(99), 41));
  auto pastJoiningLocation =
      fetch(viewer, joiningFetch(RequestID(8), RequestID(5), 50));
  auto notForwarding =
      fetch(pausedViewer, joiningFetch(RequestID(9), RequestID(6), 41));

  ASSERT_TRUE(wrongID.hasError());
  EXPECT_EQ(
      wrongID.error().errorCode, FetchErrorCode::INVALID_JOINING_REQUEST_ID);
  EXPECT_EQ(wrongID.error().requestID, RequestID(7));
  ASSERT_TRUE(pastJoiningLocation.hasError());
  EXPECT_EQ(
      pastJoiningLocation.error().errorCode, FetchErrorCode::INVALID_RANGE);
  ASSERT_TRUE(notForwarding.hasError());
  EXPECT_EQ(notForwarding.error().errorCode, FetchErrorCode::INVALID_RANGE);
}

TEST_F(MoQProxyJoiningTest, JoinWithoutContentIsInvalidRange) {
  establish(/*upstreamLargest=*/std::nullopt);

  auto result =
      fetch(firstViewer_, joiningFetch(RequestID(7), RequestID(1), 0));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, FetchErrorCode::INVALID_RANGE);
}

// Data can reach the forwarder before the upstream SUBSCRIBE_OK is processed.
// The first viewer still gets the upstream Largest as its Joining Location,
// so history and live delivery stay disjoint.
TEST_F(MoQProxyJoiningTest, FirstViewerKeepsTheUpstreamAnchor) {
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(
          [this](
              SubscribeRequest request, std::shared_ptr<TrackConsumer> consumer)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            upstreamConsumer_ = consumer;
            auto early = consumer->beginSubgroup(11, 0, kDefaultPriority, {});
            EXPECT_TRUE(early.hasValue());
            for (uint64_t objectID = 0; objectID < 2; objectID++) {
              (*early)->object(objectID, payloadOf(10), noExtensions(), false);
            }
            co_return Publisher::SubscribeResult(makeUpstreamHandleAt(
                request.requestID, AbsoluteLocation{10, 2}));
          });
  auto subscribed = subscribe(
      firstViewer_, makeSubscribeRequest(RequestID(1)), firstViewerLive_);
  ASSERT_TRUE(subscribed.hasValue());
  Fetch upstreamRequest;
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [&](Fetch request, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            upstreamRequest = std::move(request);
            co_return makeFetchHandle(RequestID(200));
          });

  auto result =
      fetch(firstViewer_, joiningFetch(RequestID(7), RequestID(1), 10));

  EXPECT_EQ(subscribed.value()->subscribeOk().largest, AbsoluteLocation(10, 2));
  ASSERT_TRUE(result.hasValue());
  const auto* range = std::get_if<StandaloneFetch>(&upstreamRequest.args);
  ASSERT_NE(range, nullptr);
  EXPECT_EQ(range->end, AbsoluteLocation(10, 3));
  const std::vector<AbsoluteLocation> live{{11, 0}, {11, 1}};
  EXPECT_EQ(firstViewerLive_->received, live);
}

TEST_F(MoQProxyJoiningTest, SecondSubscriptionFromSameSessionIsRejected) {
  establish();

  auto duplicate = subscribe(firstViewer_, makeSubscribeRequest(RequestID(2)));
  auto joinDuplicate =
      fetch(firstViewer_, joiningFetch(RequestID(7), RequestID(2), 0));

  ASSERT_TRUE(duplicate.hasError());
  EXPECT_EQ(
      duplicate.error().errorCode, SubscribeErrorCode::DUPLICATE_SUBSCRIPTION);
  // The first subscription was not replaced by the second one.
  ASSERT_TRUE(joinDuplicate.hasError());
  EXPECT_EQ(
      joinDuplicate.error().errorCode,
      FetchErrorCode::INVALID_JOINING_REQUEST_ID);
}

TEST_F(MoQProxyJoiningTest, CancellingLocalReplayStopsFurtherWrites) {
  establish();
  publishGroup(41, 3);
  auto viewer = addViewer(RequestID(5));
  auto consumer = std::make_shared<test::RecordingFetchConsumer>();
  consumer->readyWrites = 1;

  auto result =
      fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 41), consumer);
  drive();
  ASSERT_TRUE(result.hasValue());
  result.value()->fetchCancel();
  drive();
  consumer->makeReady();
  drive();

  const std::vector<std::string> firstOnly{"object 41/0 10"};
  EXPECT_EQ(consumer->log, firstOnly);
  EXPECT_EQ(consumer->resetCode, ResetStreamErrorCode::CANCELLED);
  EXPECT_EQ(proxy_->joinStats().replayStallResets, 0);
}

TEST_F(MoQProxyJoiningTest, StalledReplayIsResetWithoutAffectingLive) {
  establish();
  publishGroup(41, 3);
  auto live = std::make_shared<test::LiveViewerConsumer>();
  auto viewer = addViewer(RequestID(5), live);
  auto consumer = std::make_shared<test::RecordingFetchConsumer>();
  consumer->readyWrites = 1;

  auto result =
      fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 41), consumer);
  drive();
  timekeeper_->advance(5s);
  drive();
  publishGroup(42, 1);

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ(consumer->resetCode, ResetStreamErrorCode::CANCELLED);
  EXPECT_EQ(proxy_->joinStats().replayStallResets, 1);
  const std::vector<AbsoluteLocation> afterJoin{{42, 0}};
  EXPECT_EQ(live->received, afterJoin);
}

TEST_F(MoQProxyJoiningTest, ClosingTheProxyEndsAWaitingJoin) {
  establish();
  auto active = openSubgroup(41, /*containsLastInGroup=*/false);
  EXPECT_TRUE(
      active->beginObject(0, 20, payloadOf(5), noExtensions()).hasValue());
  auto viewer = addViewer(RequestID(5));
  EXPECT_CALL(*upstreamSession_, fetch(_, _)).Times(0);

  auto pending = startFetch(
      viewer,
      joiningFetch(RequestID(7), RequestID(5), 41),
      std::make_shared<test::RecordingFetchConsumer>());
  drive();
  EXPECT_FALSE(pending.isReady());
  proxy_->close();
  drive();

  ASSERT_TRUE(pending.isReady());
  auto result = std::move(pending).get();
  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, FetchErrorCode::GOING_AWAY);
}

// A viewer that stops a subgroup must not stop the cache from consuming it:
// the upstream stream keeps succeeding and the group is still complete.
TEST_F(MoQProxyJoiningTest, CacheKeepsConsumingAfterViewerStopsSubgroup) {
  firstViewerLive_->stoppedObjects = {{41, 1}};
  establish();

  publishGroup(41, 4);

  EXPECT_EQ(cache()->inspect({41, 0}, {42, 0}), MoQGroupCache::Coverage::Ready);
}

TEST_F(MoQProxyJoiningTest, GroupEndIsProvenByEveryCleanFinForm) {
  establish();

  EXPECT_TRUE(openSubgroup(41)
                  ->object(0, payloadOf(10), noExtensions(), true)
                  .hasValue());
  auto byPayload = openSubgroup(42);
  EXPECT_TRUE(
      byPayload->beginObject(0, 4, payloadOf(2), noExtensions()).hasValue());
  EXPECT_TRUE(byPayload->objectPayload(payloadOf(2), true).hasValue());
  auto byEndOfSubgroup = openSubgroup(43);
  EXPECT_TRUE(byEndOfSubgroup->object(0, payloadOf(10), noExtensions(), false)
                  .hasValue());
  EXPECT_TRUE(byEndOfSubgroup->endOfSubgroup().hasValue());

  EXPECT_EQ(cache()->inspect({41, 0}, {44, 0}), MoQGroupCache::Coverage::Ready);
}

// After PUBLISH_DONE, already received history is still served locally, and a
// miss still reaches the upstream session that remains connected.
TEST_F(MoQProxyJoiningTest, DrainingTrackStillServesHistory) {
  establish(AbsoluteLocation{39, 0});
  publishGroup(40, 2);
  publishGroup(41, 2);
  auto active = openSubgroup(42, /*containsLastInGroup=*/false);
  EXPECT_TRUE(
      active->object(0, payloadOf(10), noExtensions(), false).hasValue());
  EXPECT_TRUE(
      active->object(1, payloadOf(10), noExtensions(), false).hasValue());
  auto viewer = addViewer(RequestID(5));
  // Gives the viewer an open subgroup, so it drains instead of leaving.
  EXPECT_TRUE(
      active->object(2, payloadOf(10), noExtensions(), false).hasValue());
  upstreamConsumer_->publishDone(
      PublishDone{
          RequestID(0), PublishDoneStatusCode::TRACK_ENDED, 0, "ended"});
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [this](Fetch request, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            co_return makeFetchHandle(request.requestID);
          });
  auto consumer = std::make_shared<test::RecordingFetchConsumer>();

  auto local =
      fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 41), consumer);
  drive();
  auto miss = fetch(viewer, joiningFetch(RequestID(8), RequestID(5), 10));

  ASSERT_TRUE(local.hasValue());
  EXPECT_EQ(
      consumer->log,
      expectedReplay(
          {"object 41/0 10",
           "object 41/1 10",
           "object 42/0 10",
           "object 42/1 10"}));
  EXPECT_TRUE(miss.hasValue());
}

// A miss whose upstream can no longer serve history is an error, never an
// empty or fabricated response.
TEST_F(MoQProxyJoiningTest, MissOnUnavailableUpstreamIsAnError) {
  establish();
  auto active = openSubgroup(42, /*containsLastInGroup=*/false);
  EXPECT_TRUE(
      active->object(0, payloadOf(10), noExtensions(), false).hasValue());
  auto viewer = addViewer(RequestID(5));
  EXPECT_TRUE(
      active->object(1, payloadOf(10), noExtensions(), false).hasValue());
  upstreamConsumer_->publishDone(
      PublishDone{
          RequestID(0), PublishDoneStatusCode::TRACK_ENDED, 0, "ended"});
  EXPECT_CALL(*upstreamSession_, fetch(_, _))
      .WillOnce(
          [](Fetch request, std::shared_ptr<FetchConsumer>)
              -> folly::coro::Task<Publisher::FetchResult> {
            co_return folly::makeUnexpected(
                FetchError{
                    request.requestID,
                    FetchErrorCode::INTERNAL_ERROR,
                    "session closed"});
          });
  auto consumer = std::make_shared<test::RecordingFetchConsumer>();

  auto result =
      fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 10), consumer);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, FetchErrorCode::INTERNAL_ERROR);
  EXPECT_EQ(result.error().requestID, RequestID(7));
  EXPECT_TRUE(consumer->log.empty());
}

TEST_F(MoQProxyJoiningTest, RetiredSubscriptionCannotBeJoined) {
  establish();
  publishGroup(41, 2);
  auto viewer = addViewer(RequestID(5));
  viewerHandles_.back()->unsubscribe();
  EXPECT_CALL(*upstreamSession_, fetch(_, _)).Times(0);

  auto result = fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 41));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(
      result.error().errorCode, FetchErrorCode::INVALID_JOINING_REQUEST_ID);
}

// Once accepted, the history response is independent of the subscription
// that anchored it.
TEST_F(MoQProxyJoiningTest, AcceptedReplayOutlivesItsSubscription) {
  establish();
  publishGroup(41, 2);
  auto viewer = addViewer(RequestID(5));
  auto consumer = std::make_shared<test::RecordingFetchConsumer>();
  consumer->readyWrites = 1;

  auto result =
      fetch(viewer, joiningFetch(RequestID(7), RequestID(5), 41), consumer);
  drive();
  ASSERT_TRUE(result.hasValue());
  viewerHandles_.back()->unsubscribe();
  drive();
  consumer->makeReady();
  drive();

  EXPECT_EQ(
      consumer->log, expectedReplay({"object 41/0 10", "object 41/1 10"}));
  EXPECT_FALSE(consumer->resetCode.has_value());
}

// A failed upstream attempt's late callbacks, including on a subgroup it
// opened before failing, can neither reach viewers nor populate the cache used
// by the attempt that succeeded.
TEST_F(MoQProxyJoiningTest, FailedAttemptCannotPopulateTheCache) {
  auto fallbackSession =
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_);
  ON_CALL(*fallbackSession, getNegotiatedVersion())
      .WillByDefault(Return(kVersionDraftCurrent));
  auto fallbackProvider =
      std::make_shared<TestUpstreamProvider>(fallbackSession);
  proxy_ = MoQProxy::create({provider_, fallbackProvider}, makeOptions());
  std::shared_ptr<TrackConsumer> failedAttempt;
  std::shared_ptr<SubgroupConsumer> openedBeforeFailure;
  EXPECT_CALL(*upstreamSession_, subscribe(_, _))
      .WillOnce(
          [&](SubscribeRequest request, std::shared_ptr<TrackConsumer> consumer)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            failedAttempt = consumer;
            auto subgroup =
                consumer->beginSubgroup(41, 0, kDefaultPriority, {});
            EXPECT_TRUE(subgroup.hasValue());
            if (subgroup.hasValue()) {
              openedBeforeFailure = std::move(subgroup.value());
            }
            co_return folly::makeUnexpected(
                SubscribeError{
                    request.requestID,
                    SubscribeErrorCode::INTERNAL_ERROR,
                    "upstream failed"});
          });
  EXPECT_CALL(*fallbackSession, subscribe(_, _))
      .WillOnce(
          [this](SubscribeRequest request, std::shared_ptr<TrackConsumer>)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            co_return Publisher::SubscribeResult(makeUpstreamHandleAt(
                request.requestID, AbsoluteLocation{40, 0}));
          });
  auto subscribed = subscribe(
      firstViewer_, makeSubscribeRequest(RequestID(1)), firstViewerLive_);
  ASSERT_TRUE(subscribed.hasValue());

  ASSERT_NE(openedBeforeFailure, nullptr);

  auto lateObject =
      openedBeforeFailure->object(0, payloadOf(10), noExtensions(), false);
  auto late = failedAttempt->beginSubgroup(42, 0, kDefaultPriority, {});

  EXPECT_TRUE(lateObject.hasError());
  EXPECT_TRUE(late.hasError());
  EXPECT_EQ(cache()->residentGroups(), 0);
  EXPECT_TRUE(firstViewerLive_->received.empty());
}

}} // namespace moxygen
