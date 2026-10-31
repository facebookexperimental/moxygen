/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

#include <folly/coro/Collect.h>
#include <folly/coro/Timeout.h>

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

namespace {

using namespace std::chrono_literals;

// Resolves once the current coroutine's cancellation token is cancelled.
folly::coro::Task<void> waitForCancellation() {
  auto token = co_await folly::coro::co_current_cancellation_token;
  folly::coro::Baton cancelled;
  folly::CancellationCallback callback(token, [&] { cancelled.post(); });
  co_await cancelled;
}

// Gives the sessions up to a second to make done() true.
template <typename Predicate>
folly::coro::Task<void> waitUntil(Predicate done) {
  for (int i = 0; i < 100 && !done(); ++i) {
    co_await folly::coro::sleep(10ms);
  }
}

MoQSettings joiningSettings(
    std::chrono::milliseconds timeout,
    size_t maxDeferred = 16) {
  MoQSettings settings;
  settings.joiningDependencyTimeout = timeout;
  settings.maxDeferredJoiningFetches = maxDeferred;
  return settings;
}

} // namespace

class JoiningFetchTestHelper {
 public:
  static folly::SemiFuture<MoQSession::JoinResult> startJoin(
      folly::EventBase& evb,
      const std::shared_ptr<MoQSession>& session,
      SubscribeRequest subscribe,
      std::shared_ptr<TrackConsumer> subscribeCallback,
      std::shared_ptr<FetchConsumer> fetchCallback) {
    return folly::coro::co_withExecutor(
               &evb,
               session->join(
                   std::move(subscribe),
                   std::move(subscribeCallback),
                   /*joiningStart=*/0,
                   /*fetchPri=*/129,
                   GroupOrder::Default,
                   {},
                   std::move(fetchCallback),
                   FetchType::RELATIVE_JOINING))
        .start();
  }
};

// The session dispatches a joining FETCH only after its SUBSCRIBE has been
// answered, and a bound dependency does not inherit the unseen-ID timeout.
CO_TEST_P_X(MoQSessionTest, JoiningFetchWaitsForSlowSubscription) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 22 retires Joining FETCH.
    co_return;
  }
  serverSession_->setMoqSettings(joiningSettings(20ms));
  co_await setupMoQSession();
  folly::coro::Baton releaseSubscribe;
  bool subscribeAnswered = false;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    co_await releaseSubscribe;
    subscribeAnswered = true;
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectFetch([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    EXPECT_TRUE(subscribeAnswered);
    fetchPub->object(0, 0, 0, makeBuf(10), noExtensions(), /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
  });
  expectPublishDone();
  folly::coro::Baton fetchObject;
  EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, true, _)).WillOnce([&] {
    fetchObject.post();
    return folly::unit;
  });
  expectFetchSuccess();

  auto join = JoiningFetchTestHelper::startJoin(
      eventBase_,
      clientSession_,
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      fetchCallback_);
  // Establishment takes longer than the unseen-ID timeout.
  co_await folly::coro::sleep(80ms);
  releaseSubscribe.post();
  auto res = co_await std::move(join);

  EXPECT_FALSE(res.subscribeResult.hasError());
  EXPECT_FALSE(res.fetchResult.hasError());
  co_await publishDone_;
  co_await fetchObject;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Draft 18 request streams can deliver the joining FETCH before the SUBSCRIBE
// it references. The session holds it until that SUBSCRIBE is established.
CO_TEST_P_X(Draft18Test, JoiningFetchArrivesBeforeItsSubscribe) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 22 retires Joining FETCH; FillArrivesBeforeSubscribeOk covers the
    // same ordering.
    co_return;
  }
  co_await setupMoQSession();
  bool subscribeHandled = false;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    subscribeHandled = true;
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectFetch([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    EXPECT_TRUE(subscribeHandled);
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    fetchPub->object(0, 0, 0, makeBuf(10), noExtensions(), /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
  });
  expectPublishDone();
  folly::coro::Baton fetchObject;
  EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, true, _)).WillOnce([&] {
    fetchObject.post();
    return folly::unit;
  });
  expectFetchSuccess();

  clientWt_->setDeferNewStreamData(true);
  auto join = JoiningFetchTestHelper::startJoin(
      eventBase_,
      clientSession_,
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      fetchCallback_);
  co_await rescheduleN(3);
  clientWt_->setDeferNewStreamData(false);
  // The join opened the SUBSCRIBE request stream first, then the FETCH one.
  auto subscribeStream = clientWt_->writeHandles.at(0);
  auto fetchStream = clientWt_->writeHandles.at(4);
  fetchStream->setImmediateDelivery(true);
  fetchStream->deliverInflightData();
  co_await rescheduleN(5);
  EXPECT_FALSE(subscribeHandled);
  subscribeStream->setImmediateDelivery(true);
  subscribeStream->deliverInflightData();
  auto res = co_await std::move(join);

  EXPECT_FALSE(res.subscribeResult.hasError());
  EXPECT_FALSE(res.fetchResult.hasError());
  co_await publishDone_;
  co_await fetchObject;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Once the late SUBSCRIBE binds a deferred FETCH, the unseen-ID timeout no
// longer applies, however long establishment takes.
CO_TEST_P_X(Draft18Test, BoundFetchFirstJoinOutlivesTheUnseenTimeout) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 22 retires Joining FETCH.
    co_return;
  }
  serverSession_->setMoqSettings(joiningSettings(500ms));
  co_await setupMoQSession();
  folly::coro::Baton releaseSubscribe;
  bool subscribeAnswered = false;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    co_await releaseSubscribe;
    subscribeAnswered = true;
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectFetch([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    EXPECT_TRUE(subscribeAnswered);
    fetchPub->object(0, 0, 0, makeBuf(10), noExtensions(), /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
  });
  expectPublishDone();
  folly::coro::Baton fetchObject;
  EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, true, _)).WillOnce([&] {
    fetchObject.post();
    return folly::unit;
  });
  expectFetchSuccess();

  clientWt_->setDeferNewStreamData(true);
  auto join = JoiningFetchTestHelper::startJoin(
      eventBase_,
      clientSession_,
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      fetchCallback_);
  co_await rescheduleN(3);
  clientWt_->setDeferNewStreamData(false);
  auto subscribeStream = clientWt_->writeHandles.at(0);
  auto fetchStream = clientWt_->writeHandles.at(4);
  fetchStream->setImmediateDelivery(true);
  fetchStream->deliverInflightData();
  co_await rescheduleN(5);
  subscribeStream->setImmediateDelivery(true);
  subscribeStream->deliverInflightData();
  co_await folly::coro::sleep(1s);
  releaseSubscribe.post();
  auto res = co_await std::move(join);

  EXPECT_FALSE(res.subscribeResult.hasError());
  EXPECT_FALSE(res.fetchResult.hasError());
  co_await publishDone_;
  co_await fetchObject;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A joining FETCH whose SUBSCRIBE never shows up fails after the unseen-ID
// timeout without reaching the Publisher.
CO_TEST_P_X(Draft18Test, JoiningFetchForUnseenSubscribeTimesOut) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 22 retires Joining FETCH.
    co_return;
  }
  serverSession_->setMoqSettings(joiningSettings(20ms));
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->publishDone(getTrackEndedPublishDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  EXPECT_CALL(*serverPublisher, fetch(_, _)).Times(0);
  expectPublishDone();

  clientWt_->setDeferNewStreamData(true);
  auto join = JoiningFetchTestHelper::startJoin(
      eventBase_,
      clientSession_,
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      fetchCallback_);
  co_await rescheduleN(3);
  clientWt_->setDeferNewStreamData(false);
  auto subscribeStream = clientWt_->writeHandles.at(0);
  auto fetchStream = clientWt_->writeHandles.at(4);
  fetchStream->setImmediateDelivery(true);
  fetchStream->deliverInflightData();
  co_await folly::coro::sleep(80ms);
  subscribeStream->setImmediateDelivery(true);
  subscribeStream->deliverInflightData();
  auto res = co_await std::move(join);

  EXPECT_FALSE(res.subscribeResult.hasError());
  CO_ASSERT_TRUE(res.fetchResult.hasError());
  EXPECT_EQ(
      res.fetchResult.error().errorCode,
      FetchErrorCode::INVALID_JOINING_REQUEST_ID);
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A joining FETCH held for a PUBLISH reaches the Publisher only after the
// PUBLISH reply has been delivered, so a relay has recorded the subscription.
CO_TEST_P_X(Draft18Test, JoiningFetchOnPublishFollowsThePublishReply) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 22 retires Joining FETCH; PublishHandleDefersUpdateUntilPublishOk
    // covers the same ordering.
    co_return;
  }
  co_await setupMoQSessionForPublish(initialMaxRequestID_);
  folly::coro::Baton publishReceived;
  auto releasePublishOk = std::make_shared<folly::coro::Baton>();
  auto trackConsumer = std::make_shared<testing::NiceMock<MockTrackConsumer>>();
  ON_CALL(*trackConsumer, setTrackAlias(_))
      .WillByDefault(testing::Return(folly::unit));
  ON_CALL(*trackConsumer, publishDone(_))
      .WillByDefault(testing::Return(folly::unit));
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [&](const PublishRequest& pub, auto) -> Subscriber::PublishResult {
            publishReceived.post();
            return Subscriber::PublishConsumerAndReplyTask{
                trackConsumer,
                folly::coro::co_invoke(
                    [releasePublishOk, requestID = pub.requestID]()
                        -> folly::coro::Task<
                            folly::Expected<PublishOk, PublishError>> {
                      co_await *releasePublishOk;
                      co_return PublishOk{
                          .requestID = requestID,
                          .subscriberPriority = 128,
                          .groupOrder = GroupOrder::Default};
                    }),
                /*consumerReady=*/true};
          });
  bool publishReplied = false;
  EXPECT_CALL(*clientPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        EXPECT_TRUE(publishReplied);
        fetchPub->endOfFetch();
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
      });

  auto publish = clientSession_->publish(
      PublishRequest{
          .requestID = RequestID(0),
          .fullTrackName = kTestTrackName,
          .trackAlias = TrackAlias(100),
          .groupOrder = GroupOrder::Default,
          .largest = AbsoluteLocation{0, 100},
          .forward = true},
      makePublishHandle());
  CO_ASSERT_TRUE(publish.hasValue());
  auto replied = folly::coro::co_withExecutor(
                     &eventBase_,
                     folly::coro::co_invoke(
                         [&, reply = std::move(publish->reply)]() mutable
                             -> folly::coro::Task<void> {
                           EXPECT_TRUE((co_await std::move(reply)).hasValue());
                           publishReplied = true;
                         }))
                     .start();
  co_await publishReceived;
  Fetch joiningFetch(
      RequestID(0), RequestID(0), 0, FetchType::RELATIVE_JOINING);
  joiningFetch.fullTrackName = kTestTrackName;
  auto joinConsumer = std::make_shared<testing::NiceMock<MockFetchConsumer>>();
  ON_CALL(*joinConsumer, endOfFetch())
      .WillByDefault(testing::Return(folly::unit));
  auto fetch = folly::coro::co_withExecutor(
                   &eventBase_,
                   serverSession_->fetch(std::move(joiningFetch), joinConsumer))
                   .start();
  co_await rescheduleN(5);
  releasePublishOk->post();
  co_await std::move(replied);
  auto res = co_await std::move(fetch);

  EXPECT_FALSE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A subscription that fails takes its pending joining FETCH with it.
CO_TEST_P_X(MoQSessionTest, JoiningFetchFailsWithItsSubscription) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 22 retires Joining FETCH.
    co_return;
  }
  co_await setupMoQSession();
  expectSubscribe(
      [](auto sub, auto) -> TaskSubscribeResult {
        co_await folly::coro::co_reschedule_on_current_executor;
        co_return folly::makeUnexpected(
            SubscribeError{
                sub.requestID,
                SubscribeErrorCode::INTERNAL_ERROR,
                "upstream failed"});
      },
      MoQControlCodec::Direction::SERVER,
      SubscribeErrorCode::INTERNAL_ERROR);
  EXPECT_CALL(*serverPublisher, fetch(_, _)).Times(0);

  auto res = co_await clientSession_->join(
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      0,
      129,
      GroupOrder::Default,
      {},
      fetchCallback_,
      FetchType::RELATIVE_JOINING);

  EXPECT_TRUE(res.subscribeResult.hasError());
  CO_ASSERT_TRUE(res.fetchResult.hasError());
  EXPECT_EQ(
      res.fetchResult.error().errorCode,
      FetchErrorCode::INVALID_JOINING_REQUEST_ID);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Held joining FETCHes are capped per session. Overflow is an overload error;
// a held FETCH that is dispatched frees its slot.
CO_TEST_P_X(MoQSessionTest, DeferredJoiningFetchesAreCapped) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 22 retires Joining FETCH.
    co_return;
  }
  serverSession_->setMoqSettings(joiningSettings(2000ms, /*maxDeferred=*/1));
  // Two joins issue four requests.
  initialMaxRequestID_ = 4 * getRequestIDMultiplier();
  co_await setupMoQSession();
  folly::coro::Baton releaseFirst;
  folly::coro::Baton releaseSecond;
  EXPECT_CALL(*serverPublisher, subscribe(_, _))
      .WillOnce([&](auto sub, auto pub) -> TaskSubscribeResult {
        pub->setTrackAlias(TrackAlias(sub.requestID.value));
        co_await releaseFirst;
        pub->publishDone(getTrackEndedPublishDone(sub.requestID));
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      })
      .WillOnce([&](auto sub, auto pub) -> TaskSubscribeResult {
        pub->setTrackAlias(TrackAlias(sub.requestID.value));
        co_await releaseSecond;
        pub->publishDone(getTrackEndedPublishDone(sub.requestID));
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      });
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        fetchPub->endOfFetch();
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
      });
  EXPECT_CALL(*fetchCallback_, endOfFetch())
      .WillOnce(testing::Return(folly::unit));
  auto secondSubscribeCallback =
      std::make_shared<testing::NiceMock<MockTrackConsumer>>();
  ON_CALL(*secondSubscribeCallback, setTrackAlias(_))
      .WillByDefault(testing::Return(folly::unit));
  ON_CALL(*secondSubscribeCallback, publishDone(_))
      .WillByDefault(testing::Return(folly::unit));
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce(testing::Return(folly::unit));
  auto secondFetchCallback =
      std::make_shared<testing::StrictMock<MockFetchConsumer>>();

  auto first = JoiningFetchTestHelper::startJoin(
      eventBase_,
      clientSession_,
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      fetchCallback_);
  co_await rescheduleN(5);
  auto second = JoiningFetchTestHelper::startJoin(
      eventBase_,
      clientSession_,
      getSubscribe(FullTrackName{kTestTrackName.trackNamespace, "other"}),
      secondSubscribeCallback,
      secondFetchCallback);
  co_await rescheduleN(5);
  releaseFirst.post();
  auto firstRes = co_await std::move(first);
  releaseSecond.post();
  auto secondRes = co_await std::move(second);

  EXPECT_FALSE(firstRes.fetchResult.hasError());
  EXPECT_FALSE(secondRes.subscribeResult.hasError());
  CO_ASSERT_TRUE(secondRes.fetchResult.hasError());
  EXPECT_EQ(
      secondRes.fetchResult.error().errorCode,
      getDraftMajorVersion(getServerSelectedVersion()) >= 18
          ? FetchErrorCode::EXCESSIVE_LOAD
          : FetchErrorCode::INTERNAL_ERROR);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// FETCH_CANCEL for a held joining FETCH releases its slot for later joins.
CO_TEST_P_X(MoQSessionTest, CancellingAHeldJoiningFetchFreesItsSlot) {
  if (useFillFetch(getServerSelectedVersion())) {
    // Draft 22 retires Joining FETCH.
    co_return;
  }
  serverSession_->setMoqSettings(joiningSettings(2000ms, /*maxDeferred=*/1));
  initialMaxRequestID_ = 4 * getRequestIDMultiplier();
  co_await setupMoQSession();
  folly::coro::Baton heldSubscribeEntered;
  folly::coro::Baton releaseHeld;
  folly::coro::Baton releaseSecond;
  RequestID heldSubscribeID{0};
  EXPECT_CALL(*serverPublisher, subscribe(_, _))
      .WillOnce([&](auto sub, auto pub) -> TaskSubscribeResult {
        pub->setTrackAlias(TrackAlias(sub.requestID.value));
        heldSubscribeID = sub.requestID;
        heldSubscribeEntered.post();
        co_await releaseHeld;
        pub->publishDone(getTrackEndedPublishDone(sub.requestID));
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      })
      .WillOnce([&](auto sub, auto pub) -> TaskSubscribeResult {
        pub->setTrackAlias(TrackAlias(sub.requestID.value));
        co_await releaseSecond;
        pub->publishDone(getTrackEndedPublishDone(sub.requestID));
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      });
  // Only the later join's FETCH reaches the Publisher.
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        fetchPub->endOfFetch();
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
      });
  EXPECT_CALL(*fetchCallback_, endOfFetch())
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce(testing::Return(folly::unit));
  auto secondSubscribeCallback =
      std::make_shared<testing::NiceMock<MockTrackConsumer>>();
  ON_CALL(*secondSubscribeCallback, setTrackAlias(_))
      .WillByDefault(testing::Return(folly::unit));
  ON_CALL(*secondSubscribeCallback, publishDone(_))
      .WillByDefault(testing::Return(folly::unit));
  auto heldFetchCallback =
      std::make_shared<testing::NiceMock<MockFetchConsumer>>();

  auto heldSubscribe =
      folly::coro::co_withExecutor(
          &eventBase_,
          clientSession_->subscribe(
              getSubscribe(kTestTrackName), subscribeCallback_))
          .start();
  co_await heldSubscribeEntered;
  Fetch heldFetch(
      RequestID(0),
      heldSubscribeID,
      0,
      FetchType::RELATIVE_JOINING,
      129,
      GroupOrder::Default);
  heldFetch.fullTrackName = kTestTrackName;
  folly::CancellationSource cancelHeld;
  auto held =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelHeld.getToken(),
              clientSession_->fetch(std::move(heldFetch), heldFetchCallback)))
          .start();
  co_await rescheduleN(5);
  cancelHeld.requestCancellation();
  EXPECT_THROW(co_await std::move(held), folly::OperationCancelled);
  co_await rescheduleN(5);
  auto second = JoiningFetchTestHelper::startJoin(
      eventBase_,
      clientSession_,
      getSubscribe(FullTrackName{kTestTrackName.trackNamespace, "other"}),
      secondSubscribeCallback,
      fetchCallback_);
  co_await rescheduleN(5);
  releaseSecond.post();
  auto secondRes = co_await std::move(second);
  releaseHeld.post();
  auto heldSubscribeRes = co_await std::move(heldSubscribe);

  EXPECT_FALSE(secondRes.subscribeResult.hasError());
  EXPECT_FALSE(secondRes.fetchResult.hasError());
  EXPECT_FALSE(heldSubscribeRes.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Draft 16 only defines a joining FETCH for a Largest Object subscription;
// anything else is a session-level protocol violation.
CO_TEST_P_X(PreDraft18Test, JoiningFetchOnNonLargestObjectSubscriptionCloses) {
  co_await setupMoQSession();
  EXPECT_CALL(*serverPublisher, subscribe(_, _)).Times(testing::AtMost(1));
  EXPECT_CALL(*serverPublisher, fetch(_, _)).Times(0);
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .Times(testing::AtMost(1))
      .WillRepeatedly(testing::Return(folly::unit));
  auto subscribe = getSubscribe(kTestTrackName);
  subscribe.filter = LocationFilter::absoluteStart(AbsoluteLocation{0, 0});

  auto res = co_await clientSession_->join(
      std::move(subscribe),
      subscribeCallback_,
      0,
      129,
      GroupOrder::Default,
      {},
      fetchCallback_,
      FetchType::RELATIVE_JOINING);

  EXPECT_TRUE(res.fetchResult.hasError());
  EXPECT_TRUE(serverWt_->isSessionClosed());
}

// FETCH_CANCEL before FETCH_OK interrupts the Publisher; a handle it still
// returns is cancelled rather than dropped. The caller receives cancellation
// and its consumer is reset.
CO_TEST_P_X(MoQSessionTest, FetchCancelledBeforeFetchOk) {
  co_await setupMoQSession();
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
  folly::coro::Baton publisherEntered;
  std::shared_ptr<MockFetchHandle> lateHandle;
  folly::coro::Baton lateHandleCancelled;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto) -> TaskFetchResult {
        publisherEntered.post();
        co_await waitForCancellation();
        lateHandle = makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
        EXPECT_CALL(*lateHandle, fetchCancel()).WillOnce([&] {
          lateHandleCancelled.post();
        });
        co_return lateHandle;
      });

  folly::CancellationSource cancelSource;
  auto fetch =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_)))
          .start();
  co_await publisherEntered;
  cancelSource.requestCancellation();
  EXPECT_THROW(co_await std::move(fetch), folly::OperationCancelled);
  co_await lateHandleCancelled;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A FETCH_ERROR already in flight when the subscriber cancels arrives for an
// ID the subscriber forgot. It must be ignored, not treated as a protocol
// violation.
CO_TEST_P_X(PreDraft18Test, LateFetchErrorAfterCancelIsIgnored) {
  co_await setupMoQSession();
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
  folly::coro::Baton handlerEntered;
  folly::coro::Baton releaseReply;
  folly::coro::Baton replyWritten;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto) -> TaskFetchResult {
        handlerEntered.post();
        co_await releaseReply;
        replyWritten.post();
        co_return folly::makeUnexpected(
            FetchError{
                fetch.requestID, FetchErrorCode::INTERNAL_ERROR, "late"});
      })
      .WillOnce([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        fetchPub->endOfFetch();
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
      });
  EXPECT_CALL(*fetchCallback_, endOfFetch())
      .WillOnce(testing::Return(folly::unit));
  auto serverControl = serverWt_->writeHandles.at(0);

  folly::CancellationSource cancelSource;
  auto cancelled =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_)))
          .start();
  co_await handlerEntered;
  serverControl->setImmediateDelivery(false);
  releaseReply.post();
  co_await replyWritten;
  co_await rescheduleN(3);
  cancelSource.requestCancellation();
  EXPECT_THROW(co_await std::move(cancelled), folly::OperationCancelled);
  serverControl->setImmediateDelivery(true);
  serverControl->deliverInflightData();
  co_await rescheduleN(3);
  // The session survived, so a new FETCH still works.
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);

  EXPECT_FALSE(serverWt_->isSessionClosed());
  EXPECT_FALSE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Data the publisher sent before it learned of the cancellation reaches a
// session that no longer knows the request. It is stopped without touching the
// detached consumer or the session.
CO_TEST_P_X(PreDraft18Test, LateFetchDataAfterCancelIsDropped) {
  co_await setupMoQSession();
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::CANCELLED));
  folly::coro::Baton objectWritten;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        fetchPub->object(0, 0, 0, makeBuf(10), noExtensions());
        objectWritten.post();
        co_await waitForCancellation();
        co_return folly::makeUnexpected(
            FetchError{
                fetch.requestID, FetchErrorCode::CANCELLED, "cancelled"});
      });
  // fetchCallback_ is strict: any data delivery to it fails the test.
  auto clientControl = clientWt_->writeHandles.at(0);

  serverWt_->setDeferNewStreamData(true);
  folly::CancellationSource cancelSource;
  auto fetch =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_)))
          .start();
  co_await objectWritten;
  serverWt_->setDeferNewStreamData(false);
  // Hold the FETCH_CANCEL so the publisher keeps its data stream open.
  clientControl->setImmediateDelivery(false);
  cancelSource.requestCancellation();
  EXPECT_THROW(co_await std::move(fetch), folly::OperationCancelled);
  auto dataStream = serverWt_->writeHandles.at(serverObjectStreamId());
  dataStream->setImmediateDelivery(true);
  dataStream->deliverInflightData();
  co_await rescheduleN(5);
  clientControl->setImmediateDelivery(true);
  clientControl->deliverInflightData();
  co_await rescheduleN(3);

  EXPECT_GT(dataStream->stopSendingCount_, 0);
  EXPECT_FALSE(serverWt_->isSessionClosed());
  EXPECT_FALSE(clientWt_->isSessionClosed());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// With stream credit available, readiness before a FETCH's first write is
// immediate and never waits on the transport, which HTTP/3 WebTransport cannot
// do.
CO_TEST_P_X(
    MoQSessionTest,
    ReadinessBeforeFirstFetchWriteDoesNotWaitForCredit) {
  co_await setupMoQSession();
  const auto creditWaitsBefore = serverWt_->uniCreditWaitCalls();
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        auto ready = fetchPub->awaitReadyToConsume();
        EXPECT_TRUE(ready.hasValue());
        if (ready.hasValue()) {
          EXPECT_TRUE(ready.value().isReady());
          co_await std::move(ready.value());
        }
        EXPECT_TRUE(fetchPub->object(0, 0, 0, makeBuf(10), noExtensions(), true)
                        .hasValue());
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
      });
  folly::coro::Baton objectReceived;
  EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, true, _)).WillOnce([&] {
    objectReceived.post();
    return folly::unit;
  });

  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  co_await objectReceived;

  EXPECT_FALSE(res.hasError());
  EXPECT_EQ(serverWt_->uniCreditWaitCalls(), creditWaitsBefore);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Before the FETCH stream exists, a write is accepted and reports BLOCKED.
// Readiness waits for stream credit; the held object then goes out ahead of
// the next one.
CO_TEST_P_X(MoQSessionTest, FetchReadinessWaitsForStreamCredit) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton waitingForCredit;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        auto blocked =
            fetchPub->object(0, 0, 0, makeBuf(10), noExtensions(), false);
        EXPECT_TRUE(blocked.hasError());
        if (blocked.hasError()) {
          EXPECT_EQ(blocked.error().code, MoQPublishError::BLOCKED);
        }
        auto ready = fetchPub->awaitReadyToConsume();
        EXPECT_TRUE(ready.hasValue());
        waitingForCredit.post();
        if (ready.hasValue()) {
          co_await std::move(ready.value());
        }
        EXPECT_TRUE(fetchPub->object(0, 0, 1, makeBuf(10), noExtensions(), true)
                        .hasValue());
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 2});
      });
  folly::coro::Baton lastObject;
  {
    testing::InSequence inOrder;
    EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, false, _))
        .WillOnce(testing::Return(folly::unit));
    EXPECT_CALL(*fetchCallback_, object(0, 0, 1, _, _, true, _)).WillOnce([&] {
      lastObject.post();
      return folly::unit;
    });
  }

  auto fetch =
      folly::coro::co_withExecutor(
          &eventBase_,
          clientSession_->fetch(getFetch({0, 0}, {0, 2}), fetchCallback_))
          .start();
  co_await waitingForCredit;
  EXPECT_EQ(serverWt_->pendingUniCreditWaiters(), 1);
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  auto res = co_await std::move(fetch);
  co_await lastObject;

  EXPECT_FALSE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Raw QUIC transports allow a single uni stream credit waiter. FETCHes held
// for their first stream share one transport waiter. When credit for one
// stream arrives, the other FETCH waits again and goes out once that stream
// finishes. A second readiness call while one is pending fails.
CO_TEST_P_X(MoQSessionTest, ConcurrentFetchesShareOneStreamCreditWaiter) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton firstWaiting;
  folly::coro::Baton secondWaiting;
  folly::coro::Baton releaseFirst;
  folly::coro::Baton releaseSecond;
  auto holdThenEnd = [](auto fetchPub,
                        folly::coro::Baton& waiting,
                        folly::coro::Baton& release,
                        bool callTwice) -> folly::coro::Task<void> {
    auto blocked =
        fetchPub->object(0, 0, 0, makeBuf(10), noExtensions(), false);
    EXPECT_TRUE(blocked.hasError());
    if (blocked.hasError()) {
      EXPECT_EQ(blocked.error().code, MoQPublishError::BLOCKED);
    }
    auto ready = fetchPub->awaitReadyToConsume();
    EXPECT_TRUE(ready.hasValue());
    if (callTwice) {
      auto again = fetchPub->awaitReadyToConsume();
      EXPECT_TRUE(again.hasError());
      if (again.hasError()) {
        EXPECT_EQ(again.error().code, MoQPublishError::API_ERROR);
      }
    }
    waiting.post();
    if (ready.hasValue()) {
      co_await std::move(ready.value());
    }
    co_await release;
    EXPECT_TRUE(fetchPub->endOfFetch().hasValue());
  };
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        co_await holdThenEnd(fetchPub, firstWaiting, releaseFirst, true);
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
      })
      .WillOnce([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        co_await holdThenEnd(fetchPub, secondWaiting, releaseSecond, false);
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
      });
  auto secondCallback =
      std::make_shared<testing::NiceMock<MockFetchConsumer>>();
  size_t objectsReceived = 0;
  folly::coro::Baton oneObjectReceived;
  auto onObject = [&] {
    if (++objectsReceived == 1) {
      oneObjectReceived.post();
    }
    return folly::unit;
  };
  EXPECT_CALL(*fetchCallback_, object(0, 0, 0, _, _, false, _))
      .WillOnce(onObject);
  EXPECT_CALL(*secondCallback, object(0, 0, 0, _, _, false, _))
      .WillOnce(onObject);
  EXPECT_CALL(*fetchCallback_, endOfFetch())
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*secondCallback, endOfFetch())
      .WillOnce(testing::Return(folly::unit));

  auto first =
      folly::coro::co_withExecutor(
          &eventBase_,
          clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_))
          .start();
  auto second =
      folly::coro::co_withExecutor(
          &eventBase_,
          clientSession_->fetch(getFetch({0, 0}, {0, 1}), secondCallback))
          .start();
  co_await firstWaiting;
  co_await secondWaiting;
  EXPECT_EQ(serverWt_->pendingUniCreditWaiters(), 1);
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await oneObjectReceived;
  co_await rescheduleN(5);
  // One FETCH got the stream; the other is waiting for credit again.
  EXPECT_EQ(objectsReceived, 1);
  EXPECT_EQ(serverWt_->pendingUniCreditWaiters(), 1);
  releaseFirst.post();
  releaseSecond.post();
  auto results = co_await folly::coro::co_awaitTry(
      folly::coro::timeout(
          folly::coro::collectAll(std::move(first), std::move(second)), 5s));

  CO_ASSERT_FALSE(results.hasException());
  EXPECT_FALSE(std::get<0>(*results).hasError());
  EXPECT_FALSE(std::get<1>(*results).hasError());
  EXPECT_EQ(objectsReceived, 2);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A write held for stream credit goes out once credit arrives, even when it
// ended the FETCH and nothing waits for readiness afterwards.
CO_TEST_P_X(MoQSessionTest, HeldEndOfFetchFlushesWhenCreditArrives) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        auto held = fetchPub->endOfFetch();
        EXPECT_TRUE(held.hasError());
        if (held.hasError()) {
          EXPECT_EQ(held.error().code, MoQPublishError::BLOCKED);
        }
        co_return makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
      });
  bool fetchEnded = false;
  EXPECT_CALL(*fetchCallback_, endOfFetch()).WillOnce([&] {
    fetchEnded = true;
    return folly::unit;
  });

  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  CO_ASSERT_FALSE(res.hasError());
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await waitUntil([&] { return fetchEnded; });

  EXPECT_TRUE(fetchEnded);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A FETCH cancelled while its writes wait for stream credit gives up that
// wait instead of staying referenced by it until credit arrives.
CO_TEST_P_X(MoQSessionTest, CancelledFetchReleasesItsStreamCreditWait) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  std::weak_ptr<FetchConsumer> heldFetch;
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce([&](Fetch fetch, auto fetchPub) -> TaskFetchResult {
        auto held =
            fetchPub->object(0, 0, 0, makeBuf(10), noExtensions(), false);
        EXPECT_TRUE(held.hasError());
        if (held.hasError()) {
          EXPECT_EQ(held.error().code, MoQPublishError::BLOCKED);
        }
        heldFetch = fetchPub;
        auto handle = makeFetchOkResult(fetch, AbsoluteLocation{0, 1});
        EXPECT_CALL(*handle, fetchCancel());
        co_return handle;
      });

  auto res = co_await clientSession_->fetch(
      getFetch({0, 0}, {0, 1}),
      std::make_shared<testing::NiceMock<MockFetchConsumer>>());
  CO_ASSERT_FALSE(res.hasError());
  EXPECT_FALSE(heldFetch.expired());
  res.value()->fetchCancel();
  co_await waitUntil([&] { return heldFetch.expired(); });

  EXPECT_TRUE(heldFetch.expired());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
