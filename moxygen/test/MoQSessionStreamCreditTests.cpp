/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// beginSubgroup succeeds without uni stream credit. The subgroup's writes and
// FIN go out once credit arrives.
CO_TEST_P_X(MoQSessionTest, SubgroupWaitsForStreamCredit) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton published;
  std::shared_ptr<TrackConsumer> serverPub;
  RequestID requestID;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    serverPub = pub;
    requestID = sub.requestID;
    eventBase_.add([&, pub] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()->object(0, makeBuf(10)).hasValue());
        EXPECT_TRUE(sgp.value()
                        ->object(1, makeBuf(10), noExtensions(), true)
                        .hasValue());
      }
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton received;
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0, _))
      .WillOnce(testing::Return(sg));
  EXPECT_CALL(*sg, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg, object(1, _, _, true)).WillOnce([&] {
    received.post();
    return folly::unit;
  });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  co_await rescheduleN(5);
  EXPECT_EQ(serverWt_->openLocalUniStreams().size(), openUni);
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await received;
  expectPublishDone();
  serverPub->publishDone(getTrackEndedPublishDone(requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Objects written while a subgroup waits for credit are each reported
// delivered once.
CO_TEST_P_X(MoQSessionTest, WaitingSubgroupReportsEachDeliveryOnce) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  auto deliveryCallback =
      std::make_shared<testing::StrictMock<MockDeliveryCallback>>();
  EXPECT_CALL(*deliveryCallback, onDelivered(_, 0, 0, 0));
  EXPECT_CALL(*deliveryCallback, onDelivered(_, 0, 0, 1));
  folly::coro::Baton published;
  std::shared_ptr<TrackConsumer> serverPub;
  RequestID requestID;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    serverPub = pub;
    requestID = sub.requestID;
    pub->setDeliveryCallback(deliveryCallback);
    eventBase_.add([&, pub] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()->object(0, makeBuf(10)).hasValue());
        EXPECT_TRUE(sgp.value()
                        ->object(1, makeBuf(10), noExtensions(), true)
                        .hasValue());
      }
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton received;
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0, _))
      .WillOnce(testing::Return(sg));
  EXPECT_CALL(*sg, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg, object(1, _, _, true)).WillOnce([&] {
    received.post();
    return folly::unit;
  });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await received;
  expectPublishDone();
  serverPub->publishDone(getTrackEndedPublishDone(requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  EXPECT_TRUE(
      testing::Mock::VerifyAndClearExpectations(deliveryCallback.get()));
}

// awaitDelivery waits for a subgroup held for stream credit.
CO_TEST_P_X(MoQSessionTest, AwaitDeliveryWaitsForStreamCredit) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton published;
  std::shared_ptr<TrackConsumer> serverPub;
  RequestID requestID;
  folly::SemiFuture<folly::Unit> delivery = folly::makeSemiFuture();
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    serverPub = pub;
    requestID = sub.requestID;
    eventBase_.add([&, pub] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()
                        ->object(0, makeBuf(10), noExtensions(), true)
                        .hasValue());
      }
      delivery = pub->awaitDelivery();
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });
  auto sg = std::make_shared<testing::NiceMock<MockSubgroupConsumer>>();
  folly::coro::Baton received;
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0, _))
      .WillOnce(testing::Return(sg));
  EXPECT_CALL(*sg, object(0, _, _, true)).WillOnce([&] {
    received.post();
    return folly::unit;
  });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  co_await rescheduleN(5);
  EXPECT_FALSE(delivery.isReady());
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await received;
  EXPECT_TRUE(delivery.isReady());
  EXPECT_NO_THROW(std::move(delivery).get());
  expectPublishDone();
  serverPub->publishDone(getTrackEndedPublishDone(requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Resetting a subgroup held for stream credit fails awaitDelivery.
CO_TEST_P_X(MoQSessionTest, ResetWaitingSubgroupFailsAwaitDelivery) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([pub, requestID = sub.requestID] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()->object(0, makeBuf(10)).hasValue());
        auto delivery = pub->awaitDelivery();
        EXPECT_FALSE(delivery.isReady());
        sgp.value()->reset(ResetStreamErrorCode::DELIVERY_TIMEOUT);
        ASSERT_TRUE(delivery.isReady());
        EXPECT_THROW(std::move(delivery).get(), std::runtime_error);
      }
      pub->publishDone(getTrackEndedPublishDone(requestID));
    });
    co_return makeSubscribeOkResult(sub);
  });
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(_, _, _, _)).Times(0);
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&](const PublishDone&) {
        publishDone_.post();
        return folly::unit;
      });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Waiting subgroups get streams in priority order.
CO_TEST_P_X(MoQSessionTest, WaitingSubgroupsGetStreamsByPriority) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton published;
  std::shared_ptr<TrackConsumer> serverPub;
  RequestID requestID;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    serverPub = pub;
    requestID = sub.requestID;
    eventBase_.add([&, pub] {
      // Lower publisher priority values are more urgent.
      for (auto [subgroup, priority] : {std::pair{0, 200}, std::pair{1, 10}}) {
        auto sgp = pub->beginSubgroup(0, subgroup, priority);
        EXPECT_TRUE(sgp.hasValue());
        if (sgp.hasValue()) {
          EXPECT_TRUE(sgp.value()
                          ->object(0, makeBuf(10), noExtensions(), true)
                          .hasValue());
        }
      }
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });
  auto urgent = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  auto later = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton received;
  {
    testing::InSequence inOrder;
    EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 1, _, _))
        .WillOnce(testing::Return(urgent));
    EXPECT_CALL(*urgent, object(0, _, _, true))
        .WillOnce(testing::Return(folly::unit));
    EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, _, _))
        .WillOnce(testing::Return(later));
    EXPECT_CALL(*later, object(0, _, _, true)).WillOnce([&] {
      received.post();
      return folly::unit;
    });
  }

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  // The second stream is granted when the first finishes.
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await received;
  expectPublishDone();
  serverPub->publishDone(getTrackEndedPublishDone(requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A subgroup reset while waiting never opens a stream. PUBLISH_DONE doesn't
// count it.
CO_TEST_P_X(MoQSessionTest, ResetWaitingSubgroupOpensNoStream) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([pub, requestID = sub.requestID] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()->object(0, makeBuf(10)).hasValue());
        sgp.value()->reset(ResetStreamErrorCode::DELIVERY_TIMEOUT);
      }
      pub->publishDone(getTrackEndedPublishDone(requestID));
    });
    co_return makeSubscribeOkResult(sub);
  });
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(_, _, _, _)).Times(0);
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&](const PublishDone& pubDone) {
        EXPECT_EQ(pubDone.streamCount, 0);
        publishDone_.post();
        return folly::unit;
      });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await publishDone_;
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await rescheduleN(5);
  EXPECT_EQ(serverWt_->openLocalUniStreams().size(), openUni);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A waiting subgroup whose delivery timeout fires never opens a stream.
CO_TEST_P_X(MoQSessionTest, DeliveryTimeoutWhileWaitingOpensNoStream) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton published;
  std::shared_ptr<TrackConsumer> serverPub;
  RequestID requestID;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    serverPub = pub;
    requestID = sub.requestID;
    eventBase_.add([&, pub] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()
                        ->object(0, makeBuf(10), noExtensions(), true)
                        .hasValue());
      }
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(_, _, _, _)).Times(0);

  auto subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.params.insertParam(Parameter(
      folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 50));
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  co_await folly::coro::sleep(std::chrono::milliseconds(300));
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await rescheduleN(5);
  EXPECT_EQ(serverWt_->openLocalUniStreams().size(), openUni);
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&](const PublishDone& pubDone) {
        EXPECT_EQ(pubDone.streamCount, 0);
        publishDone_.post();
        return folly::unit;
      });
  serverPub->publishDone(getTrackEndedPublishDone(requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// beginSubgroup fails with BLOCKED past maxPendingUniStreams.
CO_TEST_P_X(MoQSessionTest, WaitingSubgroupLimit) {
  co_await setupMoQSession();
  MoQSettings settings;
  settings.maxPendingUniStreams = 1;
  serverSession_->setMoqSettings(settings);
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton published;
  std::shared_ptr<TrackConsumer> serverPub;
  RequestID requestID;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    serverPub = pub;
    requestID = sub.requestID;
    eventBase_.add([&, pub] {
      auto waiting = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(waiting.hasValue());
      auto blocked = pub->beginSubgroup(0, 1, 0);
      EXPECT_TRUE(blocked.hasError());
      if (blocked.hasError()) {
        EXPECT_EQ(blocked.error().code, MoQPublishError::BLOCKED);
      }
      if (waiting.hasValue()) {
        waiting.value()->reset(ResetStreamErrorCode::CANCELLED);
      }
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  expectPublishDone();
  serverPub->publishDone(getTrackEndedPublishDone(requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// After TRACK_ENDED, PUBLISH_DONE waits until the waiting subgroup gets its
// stream, and its count includes that stream.
CO_TEST_P_X(MoQSessionTest, PublishDoneWaitsForWaitingSubgroup) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton published;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([&, pub, requestID = sub.requestID] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()
                        ->object(0, makeBuf(10), noExtensions(), true)
                        .hasValue());
      }
      EXPECT_TRUE(
          pub->publishDone(getTrackEndedPublishDone(requestID)).hasValue());
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0, _))
      .WillOnce(testing::Return(sg));
  EXPECT_CALL(*sg, object(0, _, _, true))
      .WillOnce(testing::Return(folly::unit));
  bool publishDoneReceived = false;
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&](const PublishDone& pubDone) {
        EXPECT_EQ(pubDone.streamCount, 1);
        publishDoneReceived = true;
        publishDone_.post();
        return folly::unit;
      });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  co_await rescheduleN(5);
  EXPECT_FALSE(publishDoneReceived);
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// An error PUBLISH_DONE resets waiting subgroups and goes out at once,
// without counting them.
CO_TEST_P_X(MoQSessionTest, ErrorPublishDoneResetsWaitingSubgroup) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([pub, requestID = sub.requestID] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        EXPECT_TRUE(sgp.value()
                        ->object(0, makeBuf(10), noExtensions(), true)
                        .hasValue());
      }
      EXPECT_TRUE(pub->publishDone(
                         PublishDone{
                             requestID,
                             PublishDoneStatusCode::INTERNAL_ERROR,
                             0,
                             "error"})
                      .hasValue());
    });
    co_return makeSubscribeOkResult(sub);
  });
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(_, _, _, _)).Times(0);
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&](const PublishDone& pubDone) {
        EXPECT_EQ(pubDone.streamCount, 0);
        publishDone_.post();
        return folly::unit;
      });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await publishDone_;
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await rescheduleN(5);
  EXPECT_EQ(serverWt_->openLocalUniStreams().size(), openUni);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Resetting a waiting subgroup fails its readiness wait.
CO_TEST_P_X(MoQSessionTest, ResetFailsReadinessWaitWhileWaiting) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  folly::coro::Baton published;
  std::shared_ptr<TrackConsumer> serverPub;
  RequestID requestID;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    serverPub = pub;
    requestID = sub.requestID;
    eventBase_.add([&, pub] {
      auto sgp = pub->beginSubgroup(0, 0, 0);
      EXPECT_TRUE(sgp.hasValue());
      if (sgp.hasValue()) {
        auto ready = sgp.value()->awaitReadyToConsume();
        EXPECT_TRUE(ready.hasValue());
        if (ready.hasValue()) {
          EXPECT_FALSE(ready.value().isReady());
          sgp.value()->reset(ResetStreamErrorCode::CANCELLED);
          EXPECT_TRUE(ready.value().isReady());
          if (ready.value().isReady()) {
            EXPECT_TRUE(ready.value().hasException());
          }
        }
      }
      published.post();
    });
    co_return makeSubscribeOkResult(sub);
  });

  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await published;
  expectPublishDone();
  serverPub->publishDone(getTrackEndedPublishDone(requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// PUBLISH_ERROR detaches the publisher without cancelling its waiting
// subgroup's request. A grant after the subgroup's reset resets the stream.
CO_TEST_P_X(Draft18Test, WaitingSubgroupAfterPublishErrorReleasesStream) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);
  folly::coro::Baton allowError;
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [&](const PublishRequest& actualPub,
              std::shared_ptr<SubscriptionHandle>)
              -> Subscriber::PublishResult {
            auto consumer =
                std::make_shared<testing::NiceMock<MockTrackConsumer>>();
            ON_CALL(*consumer, setTrackAlias(_))
                .WillByDefault(testing::Return(folly::unit));
            return Subscriber::PublishConsumerAndReplyTask{
                consumer,
                folly::coro::co_invoke(
                    [&allowError, actualPub]()
                        -> folly::coro::Task<
                            folly::Expected<PublishOk, PublishError>> {
                      co_await allowError;
                      co_return folly::makeUnexpected(
                          PublishError{
                              actualPub.requestID,
                              PublishErrorCode::INTERNAL_ERROR,
                              "publish rejected"});
                    })};
          });

  PublishRequest pub;
  pub.fullTrackName =
      FullTrackName{TrackNamespace{{"test"}}, "waiting-publisher-track"};
  pub.forward = true;
  auto result = clientSession_->publish(pub, makePublishHandle());
  CO_ASSERT_TRUE(result.hasValue());
  auto publish = std::move(result.value());
  auto consumer = publish.consumer;
  auto reply = co_withExecutor(&eventBase_, std::move(publish.reply)).start();

  const auto openUni = clientWt_->openLocalUniStreams().size();
  clientWt_->setMaxLocalUniStreams(openUni);
  auto subgroup = consumer->beginSubgroup(0, 0, 0);
  CO_ASSERT_TRUE(subgroup.hasValue());
  EXPECT_TRUE(subgroup.value()->object(0, makeBuf(10)).hasValue());

  allowError.post();
  auto replyResult = co_await std::move(reply).via(&eventBase_);
  CO_ASSERT_TRUE(replyResult.hasError());

  subgroup.value()->reset(ResetStreamErrorCode::CANCELLED);
  const auto handlesBefore = clientWt_->writeHandles.size();
  clientWt_->setMaxLocalUniStreams(openUni + 1);
  co_await rescheduleN(5);
  CO_ASSERT_EQ(clientWt_->writeHandles.size(), handlesBefore + 1);
  EXPECT_TRUE(
      clientWt_->writeHandles.rbegin()->second->getWriteErr().has_value());
  EXPECT_EQ(clientWt_->openLocalUniStreams().size(), openUni);

  consumer.reset();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A fill reset without uni stream credit writes its FETCH_HEADER and resets
// with its own error once credit arrives.
CO_TEST_P_X(Draft22Test, FillResetWaitsForStreamCredit) {
  co_await setupMoQSession();
  const auto openUni = serverWt_->openLocalUniStreams().size();
  serverWt_->setMaxLocalUniStreams(openUni);
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    auto fill = pub->beginFill(sub.requestID);
    EXPECT_TRUE(fill.hasValue());
    if (fill) {
      fill.value()->reset(ResetStreamErrorCode::INTERNAL_ERROR);
    }
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{4, 0});
  });
  expectBeginFill();
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillRepeatedly(testing::Return(folly::unit));
  // The fake transport drops the unread FETCH_HEADER on reset, so this fill
  // does not open on the client.
  EXPECT_CALL(*fetchCallback_, reset(_)).WillRepeatedly(testing::Return());

  auto res = co_await clientSession_->subscribe(
      subscribeWithFill(kTestTrackName, /*joiningStart=*/1),
      subscribeCallback_);
  CO_ASSERT_TRUE(!res.hasError());
  co_await rescheduleN(5);
  const auto handlesBefore = serverWt_->writeHandles.size();
  serverWt_->setMaxLocalUniStreams(openUni + 1);
  co_await rescheduleN(5);
  CO_ASSERT_EQ(serverWt_->writeHandles.size(), handlesBefore + 1);
  auto& fillStream = serverWt_->writeHandles.rbegin()->second;
  EXPECT_GT(fillStream->dataWritten_, 0);
  EXPECT_EQ(
      fillStream->getWriteErr(),
      toWireResetStreamErrorCode(
          ResetStreamErrorCode::INTERNAL_ERROR, getServerSelectedVersion()));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
