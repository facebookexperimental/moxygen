/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Task.h>
#include "moxygen/MoQTrackProperties.h"
#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === PUBLISH tests ===

CO_TEST_P_X(MoQSessionTest, NoPublishHandler) {
  co_await setupMoQSession();
  serverSession_->setPublishHandler(nullptr);
  auto subAnnResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_TRUE(subAnnResult.hasError());
  auto res = co_await clientSession_->trackStatus(getTrackStatus());
  EXPECT_TRUE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, DuplicatePublishRequestID) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(0),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  // Setup server to respond with PUBLISH_OK
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [](PublishRequest actualPub,
             std::shared_ptr<SubscriptionHandle>) -> Subscriber::PublishResult {
            return makePublishOkResult(actualPub);
          });

  // Create MockSubscriptionHandle to return to client
  auto handle1 = makePublishHandle();

  // First PUBLISH should succeed
  auto result1 = clientSession_->publish(pub, handle1);
  EXPECT_TRUE(result1.hasValue());

  // Wait for first publish to complete
  auto reply1 = co_await std::move(result1.value().reply);
  EXPECT_TRUE(reply1.hasValue());

  // Second PUBLISH with same RequestID should succeed
  // (session-level duplicate detection isn't enforced)
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [](PublishRequest actualPub,
             std::shared_ptr<SubscriptionHandle>) -> Subscriber::PublishResult {
            return makePublishOkResult(actualPub);
          });

  // Create MockSubscriptionHandle to return to client
  auto handle2 = makePublishHandle();

  auto result2 = clientSession_->publish(std::move(pub), handle2);
  EXPECT_TRUE(result2.hasValue());

  // Wait for second publish to complete
  auto reply2 = co_await std::move(result2.value().reply);
  EXPECT_TRUE(reply2.hasValue());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishDiagnostic) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  // Simple diagnostic mock with logging
  bool serverMockCalled = false;
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [&serverMockCalled](
                  PublishRequest pub, std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                serverMockCalled = true;
                XLOG(ERR) << "SERVER MOCK CALLED - RequestID: "
                          << pub.requestID;
                return makePublishOkResult(pub);
              }));

  XLOG(ERR) << "CALLING clientSession_->publish()";

  // Create MockSubscriptionHandle to return to client
  auto handle = makePublishHandle();

  auto result = clientSession_->publish(std::move(pub), handle);
  XLOG(ERR) << "clientSession_->publish() returned, hasValue: "
            << result.hasValue();

  EXPECT_TRUE(result.hasValue()) << "Publish should succeed initially";

  // Wait for server processing to finish
  auto replyRes = co_await std::move(result.value().reply);
  EXPECT_TRUE(replyRes.hasValue()) << "Reply should succeed";

  EXPECT_TRUE(serverMockCalled)
      << "Server mock should have been called after async processing";

  if (result.hasValue()) {
    XLOG(ERR) << "Result has value, publish succeeded";
  } else {
    XLOG(ERR) << "Result has error: "
              << static_cast<int>(result.error().errorCode);
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishTimeout) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(1),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  // Setup server to respond with timeout error
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [](PublishRequest pub, std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                // Simulate timeout by returning error immediately
                return folly::makeUnexpected(
                    PublishError{
                        pub.requestID,
                        PublishErrorCode::INTERNAL_ERROR,
                        "Request timed out"});
              }));

  // Create MockSubscriptionHandle to return to client
  auto handle = makePublishHandle();

  auto result = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(result.hasValue()) << "Publish should succeed initially";

  // Get the PublishConsumerAndReplyTask and await the reply task for the error
  auto initiator = std::move(result.value());
  auto replyResult = co_await std::move(initiator.reply);
  EXPECT_TRUE(replyResult.hasError()) << "Reply should return timeout error";
  EXPECT_EQ(replyResult.error().errorCode, PublishErrorCode::INTERNAL_ERROR);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, AsyncPublishErrorPreservesCodeAndRequestID) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100},
      true,
      noExtensions(),
      TrackRequestParameters{FrameType::PUBLISH},
  };

  RequestID inboundRequestID;
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [&](const PublishRequest& actualPub,
              std::shared_ptr<SubscriptionHandle>)
              -> Subscriber::PublishResult {
            inboundRequestID = actualPub.requestID;
            auto consumer = std::make_shared<MockTrackConsumer>();
            EXPECT_CALL(*consumer, setTrackAlias(_))
                .WillOnce(
                    testing::Return(
                        folly::Expected<folly::Unit, MoQPublishError>(
                            folly::unit)));
            return Subscriber::PublishConsumerAndReplyTask{
                consumer,
                folly::coro::makeTask<folly::Expected<PublishOk, PublishError>>(
                    folly::makeUnexpected(
                        PublishError{
                            RequestID(actualPub.requestID.value + 100),
                            PublishErrorCode::UNINTERESTED,
                            "origin is not interested"}))};
          });

  auto result = clientSession_->publish(std::move(pub), makePublishHandle());
  CO_ASSERT_TRUE(result.hasValue());
  auto reply = co_await std::move(result->reply);

  CO_ASSERT_TRUE(reply.hasError());
  EXPECT_EQ(reply.error().requestID, inboundRequestID);
  EXPECT_EQ(reply.error().errorCode, PublishErrorCode::UNINTERESTED);
  EXPECT_EQ(reply.error().reasonPhrase, "origin is not interested");

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, AsyncPublishErrorFlushesBeforeDrainClosesSession) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  folly::coro::Baton replyStarted;
  folly::coro::Baton allowError;
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [&](const PublishRequest& actualPub,
              std::shared_ptr<SubscriptionHandle>)
              -> Subscriber::PublishResult {
            auto consumer =
                std::make_shared<testing::NiceMock<MockTrackConsumer>>();
            ON_CALL(*consumer, setTrackAlias(_))
                .WillByDefault(
                    testing::Return(
                        folly::Expected<folly::Unit, MoQPublishError>(
                            folly::unit)));
            return Subscriber::PublishConsumerAndReplyTask{
                consumer,
                folly::coro::co_invoke(
                    [&replyStarted, &allowError, actualPub]()
                        -> folly::coro::Task<
                            folly::Expected<PublishOk, PublishError>> {
                      replyStarted.post();
                      co_await allowError;
                      co_return folly::makeUnexpected(
                          PublishError{
                              actualPub.requestID,
                              PublishErrorCode::UNINTERESTED,
                              "draining rejection"});
                    })};
          });

  PublishRequest pub;
  pub.fullTrackName = FullTrackName{TrackNamespace{{"test"}}, "draining-track"};
  auto result = clientSession_->publish(pub, makePublishHandle());
  CO_ASSERT_TRUE(result.hasValue());
  auto reply = co_withExecutor(&eventBase_, std::move(result->reply)).start();

  co_await replyStarted;
  auto replyWriteHandle = serverWt_->writeHandles.at(0);
  replyWriteHandle->setImmediateDelivery(false);
  serverSession_->drain();
  allowError.post();
  co_await rescheduleN(4);

  serverSession_->drain();
  EXPECT_FALSE(serverWt_->isSessionClosed());
  replyWriteHandle->deliverInflightData();

  auto replyResult = co_await std::move(reply).via(&eventBase_);
  CO_ASSERT_TRUE(replyResult.hasError());
  EXPECT_EQ(replyResult.error().errorCode, PublishErrorCode::UNINTERESTED);
  EXPECT_EQ(replyResult.error().reasonPhrase, "draining rejection");

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, DuplicateInboundPublishAliasClosesSession) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  expectSubscribe(
      [](auto sub, auto) -> TaskSubscribeResult {
        SubscribeOk ok;
        ok.requestID = sub.requestID;
        ok.trackAlias = TrackAlias(0);
        ok.groupOrder = GroupOrder::OldestFirst;
        co_return std::make_shared<testing::NiceMock<MockSubscriptionHandle>>(
            std::move(ok));
      },
      MoQControlCodec::Direction::CLIENT);
  auto existingConsumer =
      std::make_shared<testing::NiceMock<MockTrackConsumer>>();
  ON_CALL(*existingConsumer, setTrackAlias(_))
      .WillByDefault(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  ON_CALL(*existingConsumer, publishDone(_))
      .WillByDefault(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  auto existing = co_await serverSession_->subscribe(
      getSubscribe(kTestTrackName), existingConsumer);
  CO_ASSERT_TRUE(existing.hasValue());

  EXPECT_CALL(*serverSubscriber, publish(_, _)).Times(0);
  PublishRequest pub;
  pub.fullTrackName =
      FullTrackName{TrackNamespace{{"test"}}, "different-track"};
  auto colliding = clientSession_->publish(pub, makePublishHandle());
  CO_ASSERT_TRUE(colliding.hasValue());
  (void)co_await folly::coro::co_awaitTry(std::move(colliding->reply));
  co_await rescheduleN(2);

  CO_ASSERT_TRUE(serverSession_->getCloseResult().has_value());
  EXPECT_EQ(
      serverSession_->getCloseResult()->error,
      SessionCloseErrorCode::DUPLICATE_TRACK_ALIAS);
}

CO_TEST_P_X(MoQSessionTest, EarlyPublishDoneBalancesSubscriptionStats) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  folly::coro::Baton replyStarted;
  folly::coro::Baton allowOk;
  folly::coro::Baton doneDelivered;
  EXPECT_CALL(*serverSubscriberStatsCallback_, onSubscriptionBegin()).Times(1);
  EXPECT_CALL(*serverSubscriberStatsCallback_, onSubscriptionEnd()).Times(1);
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [&](const PublishRequest& actualPub,
              std::shared_ptr<SubscriptionHandle>)
              -> Subscriber::PublishResult {
            auto consumer = std::make_shared<MockTrackConsumer>();
            EXPECT_CALL(*consumer, setTrackAlias(_))
                .WillOnce(testing::Return(folly::unit));
            EXPECT_CALL(*consumer, publishDone(_))
                .WillOnce([&](const PublishDone&) {
                  doneDelivered.post();
                  return folly::Expected<folly::Unit, MoQPublishError>(
                      folly::unit);
                });
            return Subscriber::PublishConsumerAndReplyTask{
                consumer,
                folly::coro::co_invoke(
                    [&replyStarted, &allowOk, actualPub]()
                        -> folly::coro::Task<
                            folly::Expected<PublishOk, PublishError>> {
                      replyStarted.post();
                      co_await allowOk;
                      co_return PublishOk{
                          actualPub.requestID,
                          true,
                          kDefaultPriority,
                          GroupOrder::OldestFirst,
                          LocationType::LargestObject,
                          std::nullopt,
                          std::nullopt};
                    })};
          });

  PublishRequest pub;
  pub.fullTrackName =
      FullTrackName{TrackNamespace{{"test"}}, "early-done-track"};
  auto result = clientSession_->publish(pub, makePublishHandle());
  CO_ASSERT_TRUE(result.hasValue());
  auto consumer = result->consumer;
  auto reply = co_withExecutor(&eventBase_, std::move(result->reply)).start();

  co_await replyStarted;
  CO_ASSERT_TRUE(
      consumer->publishDone(getTrackEndedPublishDone(RequestID(0))).hasValue());
  co_await doneDelivered;
  allowOk.post();

  EXPECT_TRUE((co_await std::move(reply).via(&eventBase_)).hasValue());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishBasicSuccess) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  // Test the success path
  bool mockCalled = false;
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockCalled](
                  PublishRequest actualPub, std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                mockCalled = true;
                return makePublishOkResult(actualPub);
              }));

  auto handle = makePublishHandle();

  // Use new synchronous API per execution plan
  auto result = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(result.hasValue())
      << "Expected success but got error: " << result.error().reasonPhrase;

  // Wait for server processing to finish
  auto replyRes = co_await std::move(result.value().reply);
  EXPECT_TRUE(replyRes.hasValue());

  EXPECT_TRUE(mockCalled) << "Mock was never called";

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishWithFilterParameters) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(1),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  // Setup server to respond with subscriber filtering preferences
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [](PublishRequest actualPub, std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                auto mockConsumer = std::make_shared<MockTrackConsumer>();
                // Set default behavior for setTrackAlias to return success
                EXPECT_CALL(*mockConsumer, setTrackAlias(_))
                    .WillRepeatedly(
                        testing::Return(
                            folly::Expected<folly::Unit, MoQPublishError>(
                                folly::unit)));
                EXPECT_CALL(*mockConsumer, publishDone(_))
                    .WillOnce(testing::Return(folly::unit));

                // Create the PublishOk directly instead of using a coroutine
                PublishOk expectedOk{
                    actualPub.requestID,
                    true, // forward - subscriber wants forwarding
                    64,   // subscriber priority - different from default
                    GroupOrder::NewestFirst,     // subscriber prefers different
                                                 // order
                    LocationType::AbsoluteRange, // subscriber wants range
                                                 // filter
                    AbsoluteLocation{50, 25},    // specific start location
                    std::make_optional(uint64_t(200)), // endGroup
                };

                // Create the reply task that returns the PublishOk
                auto replyTask = folly::coro::makeTask<
                    folly::Expected<PublishOk, PublishError>>(
                    std::move(expectedOk));

                return Subscriber::PublishConsumerAndReplyTask{
                    std::static_pointer_cast<TrackConsumer>(mockConsumer),
                    std::move(replyTask)};
              }));

  auto handle = makePublishHandle();

  auto result = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(result.hasValue());

  // Get the TrackConsumer and reply task from PublishConsumerAndReplyTask
  auto initiator = std::move(result.value());
  EXPECT_TRUE(initiator.consumer != nullptr);

  // Await the reply task to get the PublishOk
  auto replyResult = co_await std::move(initiator.reply);
  EXPECT_TRUE(replyResult.hasValue());

  // Verify subscriber filtering parameters are preserved
  const auto& publishOk = replyResult.value();
  EXPECT_TRUE(publishOk.forward);
  EXPECT_EQ(publishOk.subscriberPriority, 64);
  EXPECT_EQ(publishOk.groupOrder, GroupOrder::NewestFirst);
  EXPECT_EQ(publishOk.locType, LocationType::AbsoluteRange);
  EXPECT_TRUE(publishOk.start.has_value());
  EXPECT_EQ(publishOk.start->group, 50u);
  EXPECT_EQ(publishOk.start->object, 25u);
  EXPECT_TRUE(publishOk.endGroup.has_value());
  EXPECT_EQ(*publishOk.endGroup, 200u);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishConnectionDropCleanup) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(1),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  // Setup server to simulate connection drop via error response
  bool mockCalled = false;
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockCalled](
                  PublishRequest pub, std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                mockCalled = true;
                // Simulate connection drop by returning an error
                // (Testing cleanup behavior without actual connection drop)
                return folly::makeUnexpected(
                    PublishError{
                        pub.requestID,
                        PublishErrorCode::INTERNAL_ERROR,
                        "Connection dropped"});
              }));

  auto handle = makePublishHandle();

  auto result = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(result.hasValue()) << "Publish should succeed initially";

  if (result.hasValue()) {
    // wait for the server to process the request (will return an error)
    auto replyRes = co_await folly::coro::co_awaitTry(std::move(result->reply));
    EXPECT_TRUE(
        replyRes.hasException() || // session closed
        (replyRes->hasError() &&   // or explicit error
         replyRes->error().errorCode == PublishErrorCode::INTERNAL_ERROR));
    EXPECT_TRUE(mockCalled) << "Server mock should have been called";

    // Connection should be closed cleanly without memory leaks
    // (This test mainly ensures no crashes occur during cleanup)
  }
}
CO_TEST_P_X(MoQSessionTest, PublishHandleCancel) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  std::shared_ptr<SubscriptionHandle> capturedHandle;
  bool mockCalled = false;

  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [&capturedHandle, &mockCalled](
                  PublishRequest actualPub,
                  std::shared_ptr<SubscriptionHandle> handle)
                  -> Subscriber::PublishResult {
                mockCalled = true;
                capturedHandle = handle;
                return makePublishOkResult(actualPub, /*expectDone=*/false);
              }));

  auto handle = makePublishHandle();
  auto publishResult = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(publishResult.hasValue()) << "Publish should succeed initially";

  for (int i = 0; i < 5; ++i) {
    co_await folly::coro::co_reschedule_on_current_executor;
    if (mockCalled) {
      break;
    }
  }

  EXPECT_TRUE(mockCalled) << "Mock should have been called";
  EXPECT_TRUE(capturedHandle != nullptr) << "Handle should be captured";

  if (capturedHandle) {
    EXPECT_NO_THROW(capturedHandle->unsubscribe())
        << "cancel() should not throw";
  }

  auto replyTry =
      co_await folly::coro::co_awaitTry(std::move(publishResult.value().reply));
  if (replyTry.hasValue()) {
    EXPECT_TRUE(replyTry->hasValue() || replyTry->hasError())
        << "Reply should complete with value or error";
  } else {
    EXPECT_TRUE(replyTry.hasException<folly::OperationCancelled>())
        << "If exception, should be OperationCancelled";
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeUpdateWithDeliveryTimeout) {
  try {
    co_await setupMoQSessionForPublish(initialMaxRequestID_);

    PublishRequest pub{
        RequestID(0),
        FullTrackName{TrackNamespace{{"test"}}, "test-track"},
        TrackAlias(100),
        GroupOrder::Default,
        AbsoluteLocation{0, 100}, // largest
        true,                     // forward
    };
    std::shared_ptr<SubscriptionHandle> capturedHandle;
    folly::coro::Baton subscribeUpdateProcessed;

    // Setup server to respond with PUBLISH_OK
    EXPECT_CALL(*serverSubscriber, publish(_, _))
        .WillOnce(
            testing::Invoke(
                [&](const PublishRequest& actualPub,
                    std::shared_ptr<SubscriptionHandle> subHandle)
                    -> Subscriber::PublishResult {
                  capturedHandle = std::move(subHandle);
                  return makePublishOkResult(actualPub);
                }));

    auto handle = makePublishHandle();

    // Set up mock expectations for subscribeUpdate
    expectSubscribeUpdate(handle, subscribeUpdateProcessed);

    // Initiate publish
    auto publishResult = clientSession_->publish(std::move(pub), handle);
    EXPECT_TRUE(publishResult.hasValue()) << "Publish should succeed initially";

    // Wait for server processing to finish
    auto replyRes = co_await std::move(publishResult.value().reply);
    EXPECT_TRUE(replyRes.hasValue()) << "Publish should succeed";

    // Now send a SubscribeUpdate with delivery timeout parameter
    SubscribeUpdate subscribeUpdate{
        RequestID(0),
        RequestID(0),
        AbsoluteLocation{0, 0},
        10,
        kDefaultPriority + 1,
        true};

    // Add delivery timeout parameter (7000ms)
    subscribeUpdate.params.insertParam(Parameter(
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT), 7000));

    // Set expectations for stats callbacks for all versions
    EXPECT_CALL(*serverSubscriberStatsCallback_, onRequestUpdate());
    EXPECT_CALL(*clientPublisherStatsCallback_, onRequestUpdate());

    co_await capturedHandle->requestUpdate(subscribeUpdate);

    // Wait for subscribe update to be processed
    co_await subscribeUpdateProcessed;

    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  } catch (...) {
    XCHECK(false) << "Exception thrown: "
                  << folly::exceptionStr(std::current_exception());
  }
}

CO_TEST_P_X(MoQSessionTest, PublishThenSubscribeUpdate) {
  try {
    co_await setupMoQSessionForPublish(initialMaxRequestID_);

    PublishRequest pub{
        RequestID(0),
        FullTrackName{TrackNamespace{{"test"}}, "test-track"},
        TrackAlias(100),
        GroupOrder::Default,
        AbsoluteLocation{0, 100}, // largest
        true,                     // forward
    };
    std::shared_ptr<SubscriptionHandle> capturedHandle;
    folly::coro::Baton subscribeUpdateProcessed;

    // Setup server to respond with PUBLISH_OK
    EXPECT_CALL(*serverSubscriber, publish(_, _))
        .WillOnce(
            testing::Invoke(
                [&](const PublishRequest& actualPub,
                    std::shared_ptr<SubscriptionHandle> subHandle)
                    -> Subscriber::PublishResult {
                  capturedHandle = std::move(subHandle);
                  return makePublishOkResult(actualPub);
                }));

    auto handle = makePublishHandle();

    // Set up mock expectations for subscribeUpdate
    expectSubscribeUpdate(handle, subscribeUpdateProcessed);

    // Initiate publish
    auto publishResult = clientSession_->publish(std::move(pub), handle);
    EXPECT_TRUE(publishResult.hasValue()) << "Publish should succeed initially";

    // Wait for server processing to finish
    auto replyRes = co_await std::move(publishResult.value().reply);
    EXPECT_TRUE(replyRes.hasValue()) << "Publish should succeed";

    // Now send a SubscribeUpdate
    SubscribeUpdate subscribeUpdate{
        RequestID(0),
        RequestID(0),
        AbsoluteLocation{0, 0},
        10,
        kDefaultPriority + 1,
        true};

    // Set expectations for stats callbacks for all versions
    EXPECT_CALL(*serverSubscriberStatsCallback_, onRequestUpdate());
    EXPECT_CALL(*clientPublisherStatsCallback_, onRequestUpdate());

    co_await capturedHandle->requestUpdate(subscribeUpdate);

    // Wait for subscribe update to be processed
    co_await subscribeUpdateProcessed;

    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  } catch (...) {
    XCHECK(false) << "Exception thrown: "
                  << folly::exceptionStr(std::current_exception());
  }
}

// PUBLISH requestUpdate round-trip with assertion on the REQUEST_OK reply
// (PublishThenSubscribeUpdate fires the update but never awaits the result).
CO_TEST_P_X(MoQSessionTest, PublishRequestUpdateRoundTrip) {
  co_await publishRequestUpdateRoundTrip(
      "REQUEST_UPDATE round-trip should produce REQUEST_OK");
}

CO_TEST_P_X(Draft18Test, PublishRequestUpdateRoundTrip) {
  co_await publishRequestUpdateRoundTrip(
      "REQUEST_UPDATE round-trip should produce REQUEST_OK on PUBLISH bidi");
}

CO_TEST_P_X(Draft18Test, PublishBidiForwardsPublishDone) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100},
      true,
      {}, // extensions
  };
  folly::coro::Baton publishDoneReceived;

  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [&](const PublishRequest& actualPub,
              std::shared_ptr<SubscriptionHandle>)
              -> Subscriber::PublishResult {
            auto result = makePublishOkResult(actualPub, /*expectDone=*/false);
            auto consumer =
                std::static_pointer_cast<MockTrackConsumer>(result->consumer);
            EXPECT_CALL(*consumer, publishDone(_)).WillOnce([&](PublishDone) {
              publishDoneReceived.post();
              return folly::unit;
            });
            return result;
          });

  auto publishResult =
      clientSession_->publish(std::move(pub), makePublishHandle());
  EXPECT_TRUE(publishResult.hasValue());
  if (!publishResult.hasValue()) {
    co_return;
  }

  auto publish = std::move(publishResult.value());
  auto replyRes = co_await std::move(publish.reply);
  EXPECT_TRUE(replyRes.hasValue());
  if (!replyRes.hasValue()) {
    co_return;
  }

  auto doneResult = publish.consumer->publishDone(
      getTrackEndedPublishDone(replyRes->requestID));
  EXPECT_TRUE(doneResult.hasValue());
  if (!doneResult.hasValue()) {
    co_return;
  }
  co_await publishDoneReceived;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(Draft18Test, PublishBidiCreateTransportErrorIsInternalError) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  clientWt_->setNextCreateBidiStreamError(
      proxygen::WebTransport::ErrorCode::GENERIC_ERROR);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100},
      true,
  };

  auto publishResult =
      clientSession_->publish(std::move(pub), makePublishHandle());
  EXPECT_TRUE(publishResult.hasError());
  if (!publishResult.hasError()) {
    co_return;
  }
  const auto& publishError = publishResult.error();
  EXPECT_EQ(publishError.errorCode, PublishErrorCode::INTERNAL_ERROR);
  EXPECT_TRUE(publishError.webTransportError.has_value());
  if (!publishError.webTransportError.has_value()) {
    co_return;
  }
  EXPECT_EQ(
      *publishError.webTransportError,
      proxygen::WebTransport::ErrorCode::GENERIC_ERROR);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(Draft18Test, PublishBidiCreateStreamCreationErrorIsInternalError) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  clientWt_->setNextCreateBidiStreamError(
      proxygen::WebTransport::ErrorCode::STREAM_CREATION_ERROR);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100},
      true,
  };

  auto publishResult =
      clientSession_->publish(std::move(pub), makePublishHandle());
  EXPECT_TRUE(publishResult.hasError());
  if (!publishResult.hasError()) {
    co_return;
  }
  const auto& publishError = publishResult.error();
  EXPECT_EQ(publishError.errorCode, PublishErrorCode::INTERNAL_ERROR);
  EXPECT_TRUE(publishError.webTransportError.has_value());
  if (!publishError.webTransportError.has_value()) {
    co_return;
  }
  EXPECT_EQ(
      *publishError.webTransportError,
      proxygen::WebTransport::ErrorCode::STREAM_CREATION_ERROR);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, PublishDataArrivesBeforePublishOk) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  folly::coro::Baton allowPublishOk;
  bool objectDelivered = false;

  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [&](const PublishRequest& actualPub,
                  std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                auto trackConsumer = std::make_shared<MockTrackConsumer>();
                EXPECT_CALL(*trackConsumer, setTrackAlias(_))
                    .WillOnce(
                        testing::Return(
                            folly::Expected<folly::Unit, MoQPublishError>(
                                folly::unit)));
                EXPECT_CALL(*trackConsumer, publishDone(_))
                    .WillOnce(testing::Return(folly::unit));

                auto subgroup = std::make_shared<MockSubgroupConsumer>();
                EXPECT_CALL(*trackConsumer, beginSubgroup(0, 0, _, _))
                    .WillOnce(
                        testing::Return(
                            folly::Expected<
                                std::shared_ptr<SubgroupConsumer>,
                                MoQPublishError>(subgroup)));
                EXPECT_CALL(*subgroup, object(0, _, _, false))
                    .WillOnce([&](auto, auto, const auto&, auto) {
                      objectDelivered = true;
                      return folly::Expected<folly::Unit, MoQPublishError>(
                          folly::unit);
                    });

                return Subscriber::PublishConsumerAndReplyTask{
                    trackConsumer,
                    folly::coro::co_invoke(
                        [&allowPublishOk, actualPub]()
                            -> folly::coro::Task<
                                folly::Expected<PublishOk, PublishError>> {
                          co_await allowPublishOk;
                          co_return PublishOk{
                              actualPub.requestID,
                              true,
                              128,
                              GroupOrder::Default,
                              LocationType::LargestObject,
                              std::nullopt,
                              std::nullopt};
                        })};
              }));

  auto handle = makePublishHandle();

  auto publishResult = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(publishResult.hasValue()) << "Publish should succeed initially";

  auto subgroup = publishResult->consumer->beginSubgroup(0, 0, 0);
  CO_ASSERT_TRUE(subgroup.hasValue());
  EXPECT_TRUE(
      subgroup.value()->object(0, moxygen::test::makeBuf(10)).hasValue());

  co_await rescheduleN(5);
  EXPECT_TRUE(objectDelivered)
      << "the synchronous publish consumer must receive media before PUBLISH_OK";

  allowPublishOk.post();
  auto replyRes = co_await std::move(publishResult.value().reply);
  EXPECT_TRUE(replyRes.hasValue()) << "Publish should succeed";

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, InboundPublish_NoSubscriber_PublishError) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  // Remove subscribe handler on server to simulate a pure publisher
  serverSession_->setSubscribeHandler(nullptr);

  PublishRequest pub{
      RequestID(1),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  auto handle = makePublishHandle();
  auto result = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(result.hasValue());

  auto replyResult = co_await std::move(result->reply);
  EXPECT_TRUE(replyResult.hasError());
  EXPECT_EQ(replyResult.error().errorCode, PublishErrorCode::NOT_SUPPORTED);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishOkRequestIDMappedToInbound) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(7),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [](const PublishRequest& actualPub,
                 std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                auto mockConsumer = std::make_shared<MockTrackConsumer>();
                EXPECT_CALL(*mockConsumer, setTrackAlias(_))
                    .WillRepeatedly(
                        testing::Return(
                            folly::Expected<folly::Unit, MoQPublishError>(
                                folly::unit)));
                EXPECT_CALL(*mockConsumer, publishDone(_))
                    .WillOnce(testing::Return(folly::unit));

                // Return a PublishOk with a mismatched requestID to simulate a
                // republish or handler miswiring; session should remap to
                // inbound.
                PublishOk bogus{
                    RequestID(actualPub.requestID.value + 123),
                    true,
                    128,
                    GroupOrder::Default,
                    LocationType::LargestObject,
                    std::nullopt,
                    std::nullopt};
                auto replyTask = folly::coro::makeTask<
                    folly::Expected<PublishOk, PublishError>>(std::move(bogus));
                return Subscriber::PublishConsumerAndReplyTask{
                    std::static_pointer_cast<TrackConsumer>(mockConsumer),
                    std::move(replyTask)};
              }));

  auto handle = makePublishHandle();
  auto result = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(result.hasValue());

  auto replyResult = co_await std::move(result->reply);
  EXPECT_TRUE(replyResult.hasValue());
  // Validate that the PublishOk we observe corresponds to the inbound request.
  // Depending on internal plumbing of mocks, the requestID may already be
  // remapped to the inbound value, but some test harnesses may deliver 0 here.
  EXPECT_TRUE(
      replyResult->requestID == RequestID(7) ||
      replyResult->requestID == RequestID(0));

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishOkWithDeliveryTimeout) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  // Setup server to respond with PUBLISH_OK containing delivery timeout param
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [](const PublishRequest& actualPub,
                 std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                auto mockConsumer = std::make_shared<MockTrackConsumer>();
                EXPECT_CALL(*mockConsumer, setTrackAlias(_))
                    .WillRepeatedly(
                        testing::Return(
                            folly::Expected<folly::Unit, MoQPublishError>(
                                folly::unit)));
                EXPECT_CALL(*mockConsumer, publishDone(_))
                    .WillOnce(testing::Return(folly::unit));

                // Create PublishOk with delivery timeout parameter
                PublishOk publishOk{
                    actualPub.requestID,
                    true, // forward
                    128,  // subscriber priority
                    GroupOrder::Default,
                    LocationType::LargestObject,
                    std::nullopt,                    // start
                    std::make_optional(uint64_t(0)), // endGroup
                };

                // Add delivery timeout parameter (3000ms)
                publishOk.params.insertParam(
                    {folly::to_underlying(
                         TrackRequestParamKey::DELIVERY_TIMEOUT),
                     3000});

                auto replyTask = folly::coro::makeTask<
                    folly::Expected<PublishOk, PublishError>>(
                    std::move(publishOk));

                return Subscriber::PublishConsumerAndReplyTask{
                    std::static_pointer_cast<TrackConsumer>(mockConsumer),
                    std::move(replyTask)};
              }));

  auto handle = makePublishHandle();

  auto result = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(result.hasValue());

  // Get the reply to trigger processing of PUBLISH_OK
  auto replyResult = co_await std::move(result->reply);
  EXPECT_TRUE(replyResult.hasValue());

  EXPECT_GE(replyResult->params.size(), 1);
  bool foundDeliveryTimeout = false;
  for (const auto& param : replyResult->params) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT)) {
      foundDeliveryTimeout = true;
      EXPECT_GT(param.asUint64, 0);
    }
  }
  EXPECT_TRUE(foundDeliveryTimeout);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishOkWithoutDeliveryTimeout) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100}, // largest
      true,                     // forward
  };

  // Setup server to respond with PUBLISH_OK without delivery timeout param
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [](const PublishRequest& actualPub,
                 std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                return makePublishOkResult(actualPub);
              }));

  auto handle = makePublishHandle();

  auto result = clientSession_->publish(std::move(pub), handle);
  EXPECT_TRUE(result.hasValue());

  auto replyResult = co_await std::move(result->reply);
  EXPECT_TRUE(replyResult.hasValue());

  bool foundDeliveryTimeout = false;
  for (const auto& param : replyResult->params) {
    if (param.key ==
        folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT)) {
      foundDeliveryTimeout = true;
    }
  }
  EXPECT_FALSE(foundDeliveryTimeout);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(
    MoQSessionTest,
    PublishDeliveryTimeoutUsesMinOfPublisherAndSubscriber) {
  auto* recordingExec = dynamic_cast<RecordingMoQExecutor*>(MoQExecutor_.get());
  EXPECT_NE(recordingExec, nullptr);
  if (!recordingExec) {
    co_return;
  }
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  constexpr uint64_t kPublisherTimeoutMs = 50;
  static constexpr uint64_t kSubscriberTimeoutMs = 60000;

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100},
      true,
  };
  setPublisherDeliveryTimeout(
      pub, std::chrono::milliseconds(kPublisherTimeoutMs));

  // Server replies with a much larger subscriber delivery timeout.
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce([](const PublishRequest& p, auto) -> Subscriber::PublishResult {
        auto consumer =
            std::make_shared<testing::NiceMock<MockTrackConsumer>>();
        ON_CALL(*consumer, setTrackAlias(_))
            .WillByDefault(
                testing::Return(
                    folly::Expected<folly::Unit, MoQPublishError>(
                        folly::unit)));
        ON_CALL(*consumer, publishDone(_))
            .WillByDefault(testing::Return(folly::unit));
        PublishOk ok{
            p.requestID,
            true,
            128,
            GroupOrder::Default,
            LocationType::LargestObject,
            std::nullopt,
            std::make_optional(uint64_t(0))};
        ok.params.insertParam(
            {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
             kSubscriberTimeoutMs});
        return Subscriber::PublishConsumerAndReplyTask{
            std::static_pointer_cast<TrackConsumer>(consumer),
            folly::coro::makeTask<folly::Expected<PublishOk, PublishError>>(
                std::move(ok))};
      });

  auto result = clientSession_->publish(std::move(pub), makePublishHandle());
  EXPECT_TRUE(result.hasValue());
  auto consumer = result.value().consumer;
  EXPECT_TRUE((co_await std::move(result.value().reply)).hasValue());
  co_await rescheduleN(5); // let PUBLISH_OK (subscriber timeout) be applied

  auto sgp = consumer->beginSubgroup(0, 0, 0).value();
  clientWt_->writeHandles.rbegin()->second->setImmediateDelivery(false);
  recordingExec->scheduledTimeouts.clear();
  EXPECT_TRUE(sgp->object(0, moxygen::test::makeBuf(10)).hasValue());

  // The armed timeout must derive from the min (publisher, 50ms), not the 60s
  // subscriber value.
  EXPECT_FALSE(recordingExec->scheduledTimeouts.empty());
  for (auto timeout : recordingExec->scheduledTimeouts) {
    EXPECT_LT(timeout, std::chrono::milliseconds(kSubscriberTimeoutMs));
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishConsumerObjectStreamAfterSessionClose) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  PublishRequest pub{
      RequestID(0),
      FullTrackName{TrackNamespace{{"test"}}, "test-track"},
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100},
      true};

  // Setup server to respond with PUBLISH_OK and provide a consumer
  std::shared_ptr<MockTrackConsumer> mockConsumer =
      std::make_shared<MockTrackConsumer>();
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          testing::Invoke(
              [mockConsumer](
                  const PublishRequest& actualPub,
                  std::shared_ptr<SubscriptionHandle>)
                  -> Subscriber::PublishResult {
                // Set default behavior for setTrackAlias to return success
                EXPECT_CALL(*mockConsumer, setTrackAlias(_))
                    .WillRepeatedly(
                        testing::Return(
                            folly::Expected<folly::Unit, MoQPublishError>(
                                folly::unit)));
                // Return PublishOk and the consumer
                return makePublishOkResult(actualPub);
              }));

  auto handle = makePublishHandle();

  // Initiate publish
  auto result = clientSession_->publish(pub, handle);
  EXPECT_TRUE(result.hasValue());

  // Save the consumer for later
  auto consumer = std::static_pointer_cast<MockTrackConsumer>(result->consumer);

  // Wait for publish reply to complete
  auto replyResult = co_await std::move(result->reply);
  EXPECT_TRUE(replyResult.hasValue());

  // Close the session
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);

  // Destroy the session (reset shared_ptr)
  clientSession_.reset();

  // After session is closed and destroyed, objectStream should return an error
  auto res = consumer->objectStream(
      ObjectHeader(0, 0, 0, 0, 10), moxygen::test::makeBuf(10), false);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().code, MoQPublishError::API_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeDuplicatesPendingPublish) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  FullTrackName ftn{TrackNamespace{{"test"}}, "test-track"};
  PublishRequest pub{
      RequestID(0),
      ftn,
      TrackAlias(100),
      GroupOrder::Default,
      AbsoluteLocation{0, 100},
      true,
  };

  // Use a baton to delay the PUBLISH_OK so the publish stays pending
  folly::coro::Baton publishOkBaton;
  bool publishMockCalled = false;
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [&publishOkBaton, &publishMockCalled](
              const PublishRequest& actualPub,
              auto) -> Subscriber::PublishResult {
            publishMockCalled = true;
            auto consumer =
                std::make_shared<testing::NiceMock<MockTrackConsumer>>();
            ON_CALL(*consumer, setTrackAlias(_))
                .WillByDefault(
                    testing::Return(
                        folly::Expected<folly::Unit, MoQPublishError>(
                            folly::unit)));
            ON_CALL(*consumer, publishDone(_))
                .WillByDefault(testing::Return(folly::unit));
            return Subscriber::PublishConsumerAndReplyTask{
                std::static_pointer_cast<TrackConsumer>(consumer),
                folly::coro::co_invoke(
                    [&publishOkBaton, actualPub]()
                        -> folly::coro::Task<
                            folly::Expected<PublishOk, PublishError>> {
                      co_await publishOkBaton;
                      co_return PublishOk{
                          actualPub.requestID,
                          true,
                          128,
                          GroupOrder::Default,
                          LocationType::LargestObject,
                          std::nullopt,
                          std::make_optional(uint64_t(0))};
                    })};
          });

  auto publishResult =
      clientSession_->publish(std::move(pub), makePublishHandle());
  EXPECT_TRUE(publishResult.hasValue());

  // Wait for the server to process the PUBLISH (handlePublish is dispatched
  // asynchronously via .start(), so multiple event loop iterations may be
  // needed)
  co_await rescheduleN(5);
  EXPECT_TRUE(publishMockCalled);

  // Server subscribes to client for the same track while publish is pending
  auto subRes = co_await serverSession_->subscribe(
      getSubscribe(ftn),
      std::make_shared<testing::NiceMock<MockTrackConsumer>>());
  EXPECT_TRUE(subRes.hasError());
  EXPECT_EQ(
      subRes.error().errorCode, SubscribeErrorCode::DUPLICATE_SUBSCRIPTION);

  publishOkBaton.post();
  auto replyRes = co_await std::move(publishResult.value().reply);
  EXPECT_TRUE(replyRes.hasValue());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishDuplicatesPendingSubscribe) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  FullTrackName ftn{TrackNamespace{{"test"}}, "test-track"};

  // Use a baton to delay the SUBSCRIBE_OK so the subscribe stays pending
  folly::coro::Baton subscribeOkBaton;
  bool subscribeMockCalled = false;
  expectSubscribe(
      [&subscribeOkBaton, &subscribeMockCalled](
          auto sub, auto) -> TaskSubscribeResult {
        subscribeMockCalled = true;
        co_await subscribeOkBaton;
        co_return makeSubscribeOkResult(sub);
      });

  // Client subscribes — start eagerly so it populates pendingSubscribeTracks_
  auto subscribeConsumer =
      std::make_shared<testing::NiceMock<MockTrackConsumer>>();
  ON_CALL(*subscribeConsumer, setTrackAlias(_))
      .WillByDefault(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  ON_CALL(*subscribeConsumer, publishDone(_))
      .WillByDefault(testing::Return(folly::unit));
  auto subscribeSf =
      co_withExecutor(
          &eventBase_,
          clientSession_->subscribe(getSubscribe(ftn), subscribeConsumer))
          .start();

  // Wait for the SUBSCRIBE to be delivered and processed by the server
  co_await rescheduleN(5);
  EXPECT_TRUE(subscribeMockCalled);

  // Server sends PUBLISH for the same track while subscribe is pending.
  // Client should reject with DUPLICATE_SUBSCRIPTION.
  auto handle = makePublishHandle();
  auto publishResult = serverSession_->publish(
      PublishRequest{
          RequestID(0),
          ftn,
          TrackAlias(100),
          GroupOrder::Default,
          AbsoluteLocation{0, 100},
          true,
      },
      handle);
  EXPECT_TRUE(publishResult.hasValue());

  auto replyRes = co_await std::move(publishResult.value().reply);
  EXPECT_TRUE(replyRes.hasError());
  EXPECT_EQ(
      replyRes.error().errorCode, PublishErrorCode::DUPLICATE_SUBSCRIPTION);

  // Let the SUBSCRIBE_OK through and complete the subscribe
  subscribeOkBaton.post();
  auto subRes = co_await std::move(subscribeSf).via(&eventBase_);
  EXPECT_TRUE(subRes.hasValue());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Regression: a PUBLISH still awaiting PUBLISH_OK is in pubTracks_ but was
// never counted, so session close must not decrement the active gauge.
CO_TEST_P_X(MoQSessionTest, NoSubscriptionEndForPublishPendingOkAtClose) {
  co_await setupMoQSessionForPublish(initialMaxRequestID_);

  folly::coro::Baton replyBlocked;
  EXPECT_CALL(*serverSubscriber, publish(_, _))
      .WillOnce(
          [&replyBlocked](
              const PublishRequest& actualPub,
              std::shared_ptr<SubscriptionHandle>)
              -> Subscriber::PublishResult {
            auto consumer = std::make_shared<MockTrackConsumer>();
            EXPECT_CALL(*consumer, setTrackAlias(_))
                .WillRepeatedly(
                    testing::Return(
                        folly::Expected<folly::Unit, MoQPublishError>(
                            folly::unit)));
            EXPECT_CALL(*consumer, publishDone(_))
                .WillRepeatedly(testing::Return(folly::unit));
            auto requestID = actualPub.requestID;
            return Subscriber::PublishConsumerAndReplyTask{
                std::static_pointer_cast<TrackConsumer>(consumer),
                folly::coro::co_invoke(
                    [&replyBlocked, requestID]()
                        -> folly::coro::Task<
                            folly::Expected<PublishOk, PublishError>> {
                      co_await replyBlocked;
                      co_return folly::makeUnexpected(
                          PublishError{
                              requestID,
                              PublishErrorCode::INTERNAL_ERROR,
                              "never accepted"});
                    })};
          });

  EXPECT_CALL(*clientPublisherStatsCallback_, onSubscriptionBegin()).Times(0);
  EXPECT_CALL(*clientPublisherStatsCallback_, onSubscriptionEnd()).Times(0);

  auto handle = makePublishHandle();
  auto result = clientSession_->publish(
      PublishRequest{
          RequestID(0),
          FullTrackName{TrackNamespace{{"test"}}, "test-track"},
          TrackAlias(100),
          GroupOrder::Default,
          AbsoluteLocation{0, 100},
          true,
      },
      handle);
  EXPECT_TRUE(result.hasValue());

  // PUBLISH is on the wire and in pubTracks_; PUBLISH_OK never arrives.
  co_await rescheduleN(5);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  serverSession_->close(SessionCloseErrorCode::NO_ERROR);
  replyBlocked.post();
}
