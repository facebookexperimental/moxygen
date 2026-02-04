/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;

namespace {
using testing::_;

// SUBSCRIPTION FILTER tests

// Test that subscription filter validation fails when start location decreases
CO_TEST_P_X(MoQSessionTest, SubscribeUpdateFilterStartDecreases) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        // Initialize with start location at {10, 5}
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{10, 5});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.start = AbsoluteLocation{10, 5};
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  // Attempt to update with a start location that decreases (5, 3 < 10, 5)
  SubscribeUpdate subscribeUpdate{
      subscribeRequest.requestID,
      RequestID(0),
      AbsoluteLocation{5, 3}, // Start decreased - should fail
      20,
      kDefaultPriority,
      true};

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeUpdate());
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdateCalled)
      .WillOnce([&subscribeUpdateInvoked](const auto& actualUpdate) {
        // Verify that start location decreased
        EXPECT_TRUE(actualUpdate.start.has_value());
        EXPECT_LT(actualUpdate.start.value(), (AbsoluteLocation{10, 5}))
            << "Start should have decreased";
        subscribeUpdateInvoked.post();
      });
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdateResult)
      .WillOnce(
          testing::Return(
              SubscribeUpdateOk{
                  .requestID = subscribeUpdate.existingRequestID}));
  co_await subscribeHandler->subscribeUpdate(subscribeUpdate);
  co_await subscribeUpdateInvoked;
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Test that subscription filter validation fails when endGroup < start.group
CO_TEST_P_X(MoQSessionTest, SubscribeUpdateFilterEndLessThanStart) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{5, 10});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.start = AbsoluteLocation{5, 10};
  subscribeRequest.endGroup = 20;
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  // Attempt to update with endGroup < start.group (3 < 5)
  SubscribeUpdate subscribeUpdate{
      subscribeRequest.requestID,
      RequestID(0),
      AbsoluteLocation{5, 10},
      3, // endGroup < start.group - should fail
      kDefaultPriority,
      true};

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeUpdate());
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdateCalled)
      .WillOnce([&subscribeUpdateInvoked](const auto& actualUpdate) {
        // Verify that endGroup is less than start.group
        EXPECT_TRUE(actualUpdate.endGroup.has_value());
        EXPECT_TRUE(actualUpdate.start.has_value());
        EXPECT_LT(
            actualUpdate.endGroup.value(), actualUpdate.start.value().group)
            << "End group should be less than start group";
        subscribeUpdateInvoked.post();
      });
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdateResult)
      .WillOnce(
          testing::Return(
              SubscribeUpdateOk{
                  .requestID = subscribeUpdate.existingRequestID}));
  co_await subscribeHandler->subscribeUpdate(subscribeUpdate);
  co_await subscribeUpdateInvoked;
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Test successful filter update via SubscribeUpdate with correct largest object
CO_TEST_P_X(MoQSessionTest, SubscribeUpdateFilterSuccess) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        // Initialize with start at {10, 5} and endGroup at 50
        mockSubscriptionHandle =
            std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.requestID,
                TrackAlias(sub.requestID.value),
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                AbsoluteLocation{15, 20}, // largest object
            });
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.start = AbsoluteLocation{10, 5};
  subscribeRequest.endGroup = 50;
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  // Successfully update filter with valid new range
  SubscribeUpdate subscribeUpdate{
      subscribeRequest.requestID,
      RequestID(0),
      AbsoluteLocation{20, 10}, // Start advanced (20, 10 > 10, 5)
      100,                      // endGroup increased
      kDefaultPriority + 1,
      true};

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeUpdate());
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdateCalled)
      .WillOnce([&subscribeUpdateInvoked](const auto& actualUpdate) {
        // Verify the filter was updated correctly
        EXPECT_TRUE(actualUpdate.start.has_value());
        EXPECT_TRUE(actualUpdate.endGroup.has_value());
        EXPECT_EQ(actualUpdate.start.value(), (AbsoluteLocation(20, 10)))
            << "Start should be updated to {20, 10}";
        EXPECT_EQ(actualUpdate.endGroup.value(), 100)
            << "End group should be 100";
        EXPECT_EQ(actualUpdate.priority, kDefaultPriority + 1)
            << "Priority should be updated";
        EXPECT_TRUE(actualUpdate.forward.value()) << "Forward should be true";
        // Verify start is valid (doesn't decrease and endGroup >=
        // start.group)
        EXPECT_GE(actualUpdate.start.value(), (AbsoluteLocation(10, 5)))
            << "Start should not decrease";
        EXPECT_GE(
            actualUpdate.endGroup.value(), actualUpdate.start.value().group)
            << "End group should be >= start.group";
        subscribeUpdateInvoked.post();
      });
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdateResult)
      .WillOnce(
          testing::Return(
              SubscribeUpdateOk{
                  .requestID = subscribeUpdate.existingRequestID}));
  co_await subscribeHandler->subscribeUpdate(subscribeUpdate);
  co_await subscribeUpdateInvoked;
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Test that missing filter fields in SubscribeUpdate preserve existing values
CO_TEST_P_X(MoQSessionTest, SubscribeUpdateFilterMissingFieldsPreserved) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();

  // Initial values
  const AbsoluteLocation initialStart{10, 5};
  const uint64_t initialEndGroup = 50;
  const uint8_t initialPriority = kDefaultPriority + 2;
  const bool initialForward = true;

  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer, initialStart](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle = makeSubscribeOkResult(sub, initialStart);
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.start = initialStart;
  subscribeRequest.endGroup = initialEndGroup;
  subscribeRequest.priority = initialPriority;
  subscribeRequest.forward = initialForward;
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  // Send SubscribeUpdate with only priority changed
  // Other fields should preserve their initial values
  SubscribeUpdate subscribeUpdate{
      subscribeRequest.requestID,
      RequestID(0),
      initialStart,     // Keep start the same
      initialEndGroup,  // Keep endGroup the same
      kDefaultPriority, // Change only priority
      initialForward,   // Keep forward the same
  };

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeUpdate());
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdateCalled)
      .WillOnce([&subscribeUpdateInvoked,
                 initialStart,
                 initialEndGroup,
                 initialForward](const auto& actualUpdate) {
        // Verify that unchanged fields are preserved
        EXPECT_EQ(actualUpdate.start, initialStart)
            << "Start location should be preserved";
        EXPECT_EQ(actualUpdate.endGroup, initialEndGroup)
            << "End group should be preserved";
        EXPECT_EQ(actualUpdate.forward, initialForward)
            << "Forward should be preserved";
        // Verify only priority changed
        EXPECT_EQ(actualUpdate.priority, kDefaultPriority)
            << "Priority should be updated";
        subscribeUpdateInvoked.post();
      });
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdateResult)
      .WillOnce(
          testing::Return(
              SubscribeUpdateOk{
                  .requestID = subscribeUpdate.existingRequestID}));
  co_await subscribeHandler->subscribeUpdate(subscribeUpdate);
  co_await subscribeUpdateInvoked;
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

} // namespace
