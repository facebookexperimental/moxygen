/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <array>
#include <stdexcept>

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;

namespace {
using testing::_;

class Draft18RequestUpdateGoawayTest : public MoQSessionTest {
 protected:
  struct OpenSubscription {
    SubscribeRequest subscribeRequest;
    std::shared_ptr<Publisher::SubscriptionHandle> clientHandle;
    std::shared_ptr<MockSubscriptionHandle> serverHandle;
    std::shared_ptr<TrackConsumer> serverConsumer;
  };

  folly::coro::Task<OpenSubscription> openSubscription() {
    co_await setupMoQSession();

    OpenSubscription subscription;
    expectSubscribe([&subscription](auto sub, auto pub) -> TaskSubscribeResult {
      subscription.serverConsumer = pub;
      subscription.serverHandle =
          makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      co_return subscription.serverHandle;
    });

    subscription.subscribeRequest = getSubscribe(kTestTrackName);
    auto result = co_await clientSession_->subscribe(
        subscription.subscribeRequest, subscribeCallback_);
    EXPECT_FALSE(result.hasError());
    if (!result.hasError()) {
      subscription.clientHandle = result.value();
    }
    co_return subscription;
  }

  std::shared_ptr<testing::NiceMock<MockTrackConsumer>>
  makeKeepAliveConsumer() {
    auto keepAliveConsumer =
        std::make_shared<testing::NiceMock<MockTrackConsumer>>();
    ON_CALL(*keepAliveConsumer, setTrackAlias(_))
        .WillByDefault(
            testing::Return(
                folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    ON_CALL(*keepAliveConsumer, publishDone(_))
        .WillByDefault(
            testing::Return(
                folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    return keepAliveConsumer;
  }

  folly::coro::Task<std::shared_ptr<Publisher::SubscriptionHandle>>
  openServerSubscriptionToKeepDrainOpen() {
    expectSubscribe(
        [](auto sub, auto /* pub */) -> TaskSubscribeResult {
          co_return makeSubscribeOkResult(sub);
        },
        MoQControlCodec::Direction::CLIENT);
    auto keepAlive = co_await serverSession_->subscribe(
        getSubscribe(kTestTrackName), makeKeepAliveConsumer());
    EXPECT_FALSE(keepAlive.hasError());
    if (keepAlive.hasError()) {
      co_return nullptr;
    }
    co_return keepAlive.value();
  }

  void expectTrackEndedPublishDone(
      std::optional<PublishDoneStatusCode>& statusCode,
      folly::coro::Baton& publishDone) {
    EXPECT_CALL(*subscribeCallback_, publishDone(_))
        .WillOnce([&statusCode, &publishDone](const PublishDone& done) {
          statusCode = done.statusCode;
          publishDone.post();
          return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
        });
  }
};

INSTANTIATE_TEST_SUITE_P(
    Draft18RequestUpdateGoawayTest,
    Draft18RequestUpdateGoawayTest,
    testing::Values(VersionParams{{kVersionDraft18}, kVersionDraft18}));

CO_TEST_P_X(
    Draft18RequestUpdateGoawayTest,
    DrainingRejectsInboundRequestUpdate) {
  auto subscription = co_await openSubscription();
  if (!subscription.clientHandle || !subscription.serverConsumer) {
    co_return;
  }
  auto keepAlive = co_await openServerSubscriptionToKeepDrainOpen();
  if (!keepAlive) {
    co_return;
  }

  // A drain-rejected REQUEST_UPDATE is unsuccessful, so per spec the publisher
  // MUST also terminate the subscription with PUBLISH_DONE(UPDATE_FAILED).
  std::optional<PublishDoneStatusCode> publishDoneStatusCode;
  folly::coro::Baton publishDone;
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce(
          [&publishDoneStatusCode, &publishDone](const PublishDone& done) {
            publishDoneStatusCode = done.statusCode;
            publishDone.post();
            return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
          });

  // GOAWAY sets draining_; use drain() directly so the client can still send
  // REQUEST_UPDATE and exercise the peer-side rejection path.
  serverSession_->drain();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*subscription.serverHandle, requestUpdateCalled).Times(0);
  EXPECT_CALL(*subscription.serverHandle, requestUpdateResult()).Times(0);

  SubscribeUpdate update{
      subscription.subscribeRequest.requestID,
      RequestID(0),
      AbsoluteLocation{1, 0},
      2,
      kDefaultPriority + 1,
      true};

  auto result = co_await subscription.clientHandle->requestUpdate(update);
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(result.error().requestID, RequestID(getRequestIDMultiplier()));
    EXPECT_EQ(result.error().errorCode, RequestErrorCode::GOING_AWAY);
  }
  co_await publishDone;
  EXPECT_EQ(publishDoneStatusCode, PublishDoneStatusCode::UPDATE_FAILED);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(
    Draft18RequestUpdateGoawayTest,
    ReceivedGoawayRejectsLocalRequestUpdate) {
  auto subscription = co_await openSubscription();
  if (!subscription.clientHandle || !subscription.serverConsumer) {
    co_return;
  }
  auto keepAlive = co_await openServerSubscriptionToKeepDrainOpen();
  if (!keepAlive) {
    co_return;
  }

  folly::coro::Baton goawayReceived;
  EXPECT_CALL(*clientPublisher, goaway(_))
      .WillOnce(testing::Invoke([&goawayReceived](Goaway /* goaway */) {
        goawayReceived.post();
      }));
  serverSession_->goaway(Goaway{});
  co_await goawayReceived;

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate()).Times(0);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(0);
  EXPECT_CALL(*subscription.serverHandle, requestUpdateCalled).Times(0);
  EXPECT_CALL(*subscription.serverHandle, requestUpdateResult()).Times(0);

  SubscribeUpdate update{
      subscription.subscribeRequest.requestID,
      RequestID(0),
      AbsoluteLocation{1, 0},
      2,
      kDefaultPriority + 1,
      true};

  auto result = co_await subscription.clientHandle->requestUpdate(update);
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(result.error().requestID, RequestID(getRequestIDMultiplier()));
    EXPECT_EQ(result.error().errorCode, RequestErrorCode::GOING_AWAY);
  }
  co_await rescheduleN(4);

  std::optional<PublishDoneStatusCode> publishDoneStatusCode;
  folly::coro::Baton publishDone;
  expectTrackEndedPublishDone(publishDoneStatusCode, publishDone);
  subscription.serverConsumer->publishDone(
      getTrackEndedPublishDone(subscription.subscribeRequest.requestID));
  co_await publishDone;
  EXPECT_EQ(publishDoneStatusCode, PublishDoneStatusCode::TRACK_ENDED);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// =============================================================================
// SUBSCRIBE REQUEST_UPDATE tests
// =============================================================================

// Test that subscription filter validation fails when start location decreases
CO_TEST_P_X(MoQSessionTest, SubscribeRequestUpdateFilterStartDecreases) {
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

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
      .WillOnce([&subscribeUpdateInvoked](const auto& actualUpdate) {
        // Verify that start location decreased
        EXPECT_TRUE(actualUpdate.start.has_value());
        EXPECT_LT(actualUpdate.start.value(), (AbsoluteLocation{10, 5}))
            << "Start should have decreased";
        subscribeUpdateInvoked.post();
      });
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(
              RequestOk{.requestID = subscribeUpdate.existingRequestID}));
  co_await subscribeHandler->requestUpdate(subscribeUpdate);
  co_await subscribeUpdateInvoked;
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Test that subscription filter validation fails when endGroup < start.group.
// Draft-18+ encodes EndGroup as an unsigned delta from StartLocation.group, so
// endGroup < start.group cannot be represented on the wire; skip in that case.
CO_TEST_P_X(MoQSessionTest, SubscribeRequestUpdateFilterEndLessThanStart) {
  if (getDraftMajorVersion(GetParam().serverVersion) >= 18) {
    co_return;
  }
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

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
      .WillOnce([&subscribeUpdateInvoked](const auto& actualUpdate) {
        // Verify that endGroup is less than start.group
        EXPECT_TRUE(actualUpdate.endGroup.has_value());
        EXPECT_TRUE(actualUpdate.start.has_value());
        EXPECT_LT(
            actualUpdate.endGroup.value(), actualUpdate.start.value().group)
            << "End group should be less than start group";
        subscribeUpdateInvoked.post();
      });
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(
              RequestOk{.requestID = subscribeUpdate.existingRequestID}));
  co_await subscribeHandler->requestUpdate(subscribeUpdate);
  co_await subscribeUpdateInvoked;
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Test successful filter update via SubscribeUpdate with correct largest object
CO_TEST_P_X(MoQSessionTest, SubscribeRequestUpdateFilterSuccess) {
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

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
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
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(
              RequestOk{.requestID = subscribeUpdate.existingRequestID}));
  co_await subscribeHandler->requestUpdate(subscribeUpdate);
  co_await subscribeUpdateInvoked;
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Test that missing filter fields in SubscribeUpdate preserve existing values
CO_TEST_P_X(
    MoQSessionTest,
    SubscribeRequestUpdateFilterMissingFieldsPreserved) {
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
      initialStart,         // Keep start the same
      initialEndGroup,      // Keep endGroup the same
      kDefaultPriority + 1, // Change only priority (non-default so it is sent)
      initialForward,       // Keep forward the same
  };

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
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
        EXPECT_EQ(actualUpdate.priority, kDefaultPriority + 1)
            << "Priority should be updated";
        subscribeUpdateInvoked.post();
      });
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(
              RequestOk{.requestID = subscribeUpdate.existingRequestID}));
  co_await subscribeHandler->requestUpdate(subscribeUpdate);
  co_await subscribeUpdateInvoked;
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A sequence of REQUEST_UPDATEs threads priority through correctly: a
// non-default value changes it (128 -> 129); an update that omits priority
// leaves it unchanged (129 -> 129); and explicitly setting the default changes
// it (129 -> 128) rather than being dropped as if omitted.
CO_TEST_P_X(Draft18Test, SubscribeRequestUpdatePriorityTransitions) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate()).Times(3);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(3);

  // Capture the priority the application observes for each update, in order.
  std::vector<std::optional<uint8_t>> observed;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
      .WillRepeatedly([&observed](const RequestUpdate& u) {
        observed.push_back(u.priority);
      });
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillRepeatedly(
          testing::Return(RequestOk{.requestID = subscribeRequest.requestID}));

  const auto majorVersion = getDraftMajorVersion(getServerSelectedVersion());
  auto sendUpdate =
      [&](std::optional<uint8_t> priority) -> folly::coro::Task<void> {
    RequestUpdate update;
    update.priority = priority;
    update.forward = true;
    update.params.setMajorVersion(majorVersion);
    auto result = co_await subscribeHandler->requestUpdate(std::move(update));
    EXPECT_TRUE(result.hasValue());
  };

  co_await sendUpdate(kDefaultPriority + 1); // 128 -> 129
  co_await sendUpdate(std::nullopt);         // 129 -> 129 (omitted, unchanged)
  co_await sendUpdate(kDefaultPriority);     // 129 -> 128 (explicit default)

  EXPECT_EQ(observed.size(), 3u);
  if (observed.size() == 3) {
    EXPECT_EQ(
        observed[0], kDefaultPriority + 1);   // explicit non-default applied
    EXPECT_EQ(observed[1], std::nullopt);     // omitted => leave unchanged
    EXPECT_EQ(observed[2], kDefaultPriority); // explicit default applied
  }

  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// =============================================================================
// SUBSCRIBE REQUEST_UPDATE coalescing tests (draft 18+)
//
// Draft: "A receiver of multiple REQUEST_UPDATE messages on the same stream MAY
// coalesce their processing by applying only the cumulative result [...] The
// receiver MUST still send a REQUEST_OK for each successful update, but it is
// not required to process intermediate states individually."
// =============================================================================

// Fire kNumCoalescedUpdates REQUEST_UPDATEs back-to-back (without awaiting
// each) so they all reach the server before the first queued handler runs.
namespace {
constexpr int kNumCoalescedUpdates = 3;

template <typename HandleT>
folly::coro::Task<void> fireCoalescedUpdates(
    folly::Executor* executor,
    const std::shared_ptr<HandleT>& handle,
    uint64_t majorVersion,
    std::array<folly::coro::Baton, kNumCoalescedUpdates>& updateDone,
    std::array<
        std::optional<folly::Expected<RequestOk, RequestError>>,
        kNumCoalescedUpdates>& results) {
  for (int i = 0; i < kNumCoalescedUpdates; ++i) {
    folly::coro::co_withExecutor(
        executor, folly::coro::co_invoke([&, i]() -> folly::coro::Task<void> {
          RequestUpdate update;
          // Distinct priority per update; the last fired has the highest
          // requestID, so it is the newest.
          update.priority = static_cast<uint8_t>(kDefaultPriority + i);
          update.forward = true;
          update.params.setMajorVersion(majorVersion);
          results[i] = co_await handle->requestUpdate(std::move(update));
          updateDone[i].post();
        }))
        .start();
  }
  for (int i = 0; i < kNumCoalescedUpdates; ++i) {
    co_await updateDone[i];
  }
}
} // namespace

// Several REQUEST_UPDATEs queued before any is processed collapse to a single
// application call for the newest update, yet every update is still
// acknowledged with a REQUEST_OK. Because later values override earlier ones,
// that single call observes the newest update's parameters.
CO_TEST_P_X(Draft18Test, SubscribeRequestUpdateCoalescesToNewest) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  // Every update is sent and received, so the stat fires once per update on
  // both peers even though only one is applied.
  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate())
      .Times(kNumCoalescedUpdates);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate())
      .Times(kNumCoalescedUpdates);

  // The application handler runs exactly once, for the newest update.
  int observedPriority = -1;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
      .WillOnce([&observedPriority](const RequestUpdate& u) {
        observedPriority = u.priority.value_or(-1);
      });
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(RequestOk{.requestID = subscribeRequest.requestID}));

  std::array<folly::coro::Baton, kNumCoalescedUpdates> updateDone;
  std::array<
      std::optional<folly::Expected<RequestOk, RequestError>>,
      kNumCoalescedUpdates>
      results;
  co_await fireCoalescedUpdates(
      MoQExecutor_.get(),
      subscribeHandler,
      getDraftMajorVersion(getServerSelectedVersion()),
      updateDone,
      results);

  // The newest update (highest request ID = last fired) is the one applied.
  EXPECT_EQ(observedPriority, kDefaultPriority + kNumCoalescedUpdates - 1);
  // Every update — superseded or not — was acknowledged with a REQUEST_OK.
  for (int i = 0; i < kNumCoalescedUpdates; ++i) {
    EXPECT_TRUE(results[i].has_value() && results[i]->hasValue())
        << "update " << i << " should have been acknowledged with REQUEST_OK";
  }

  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Coalescing applies the CUMULATIVE result, not just the newest update's own
// fields: a field set by an earlier (superseded) update survives when the
// newest update is silent about it. Here the first update turns forward off and
// the newest changes only priority, so the single applied update must carry
// both.
CO_TEST_P_X(Draft18Test, SubscribeRequestUpdateCoalescesCumulatively) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate()).Times(2);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(2);

  // Only the newest update is applied; it must carry forward=false (from the
  // first update) and the priority from the second.
  std::optional<bool> observedForward;
  int observedPriority = -1;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
      .WillOnce([&observedForward, &observedPriority](const RequestUpdate& u) {
        observedForward = u.forward;
        observedPriority = u.priority.value_or(-1);
      });
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(RequestOk{.requestID = subscribeRequest.requestID}));

  const auto majorVersion = getDraftMajorVersion(getServerSelectedVersion());
  std::array<folly::coro::Baton, 2> updateDone;
  std::array<std::optional<folly::Expected<RequestOk, RequestError>>, 2>
      results;

  // Fire both updates back-to-back so both reach the server before the first
  // queued handler runs (the last fired gets the higher requestID = newest).
  auto fire = [&](int i, std::optional<bool> forward, uint8_t priority) {
    folly::coro::co_withExecutor(
        MoQExecutor_.get(),
        folly::coro::co_invoke(
            [&, i, forward, priority]() -> folly::coro::Task<void> {
              RequestUpdate update;
              update.priority = priority;
              update.forward = forward;
              update.params.setMajorVersion(majorVersion);
              results[i] =
                  co_await subscribeHandler->requestUpdate(std::move(update));
              updateDone[i].post();
            }))
        .start();
  };
  fire(0, /*forward=*/false, kDefaultPriority + 1);
  fire(1, /*forward=*/std::nullopt, kDefaultPriority + 7);
  co_await updateDone[0];
  co_await updateDone[1];

  // Cumulative: forward from update 0, priority from the newest update 1.
  EXPECT_EQ(observedForward, false);
  EXPECT_EQ(observedPriority, kDefaultPriority + 7);
  EXPECT_TRUE(results[0].has_value() && results[0]->hasValue());
  EXPECT_TRUE(results[1].has_value() && results[1]->hasValue());

  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Regression: an explicit priority equal to the default must still override an
// earlier update's priority through coalescing — the default is a real value on
// the wire, not treated as "omitted". Earlier update sets 129; the newest sets
// the default explicitly, and the default must win.
CO_TEST_P_X(
    Draft18Test,
    SubscribeRequestUpdateCoalescesExplicitDefaultPriority) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate()).Times(2);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(2);

  int observedPriority = -1;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
      .WillOnce([&observedPriority](const RequestUpdate& u) {
        observedPriority = u.priority.value_or(-1);
      });
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(RequestOk{.requestID = subscribeRequest.requestID}));

  const auto majorVersion = getDraftMajorVersion(getServerSelectedVersion());
  std::array<folly::coro::Baton, 2> updateDone;
  std::array<std::optional<folly::Expected<RequestOk, RequestError>>, 2>
      results;

  // Update 0 sets priority 129; update 1 (newest) explicitly sets the default.
  auto fire = [&](int i, uint8_t priority) {
    folly::coro::co_withExecutor(
        MoQExecutor_.get(),
        folly::coro::co_invoke([&, i, priority]() -> folly::coro::Task<void> {
          RequestUpdate update;
          update.priority = priority;
          update.forward = true;
          update.params.setMajorVersion(majorVersion);
          results[i] =
              co_await subscribeHandler->requestUpdate(std::move(update));
          updateDone[i].post();
        }))
        .start();
  };
  fire(0, kDefaultPriority + 1);
  fire(1, kDefaultPriority);
  co_await updateDone[0];
  co_await updateDone[1];

  // The newest update's explicit default wins over the earlier non-default.
  EXPECT_EQ(observedPriority, kDefaultPriority);
  EXPECT_TRUE(results[0].has_value() && results[0]->hasValue());
  EXPECT_TRUE(results[1].has_value() && results[1]->hasValue());

  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Regression: the coalescing accumulator must not persist across bursts. Once
// an update applies, a later update that is silent about a field must NOT
// inherit that field from the earlier (already-applied) update.
CO_TEST_P_X(Draft18Test, SubscribeRequestUpdateCoalescingResetsBetweenBursts) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate()).Times(2);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(2);

  // One handler call per burst; capture the forward each observed.
  std::vector<std::optional<bool>> observedForward;
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled)
      .WillRepeatedly([&observedForward](const RequestUpdate& u) {
        observedForward.push_back(u.forward);
      });
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillRepeatedly(
          testing::Return(RequestOk{.requestID = subscribeRequest.requestID}));

  const auto majorVersion = getDraftMajorVersion(getServerSelectedVersion());

  // Burst 1 (awaited to completion) turns forward off.
  RequestUpdate first;
  first.forward = false;
  first.params.setMajorVersion(majorVersion);
  auto r1 = co_await subscribeHandler->requestUpdate(std::move(first));
  EXPECT_TRUE(r1.hasValue());

  // Burst 2 changes only priority and says nothing about forward.
  RequestUpdate second;
  second.priority = kDefaultPriority + 3;
  second.params.setMajorVersion(majorVersion);
  auto r2 = co_await subscribeHandler->requestUpdate(std::move(second));
  EXPECT_TRUE(r2.hasValue());

  // Burst 2 must not inherit burst 1's forward=false.
  EXPECT_EQ(observedForward.size(), 2u);
  if (observedForward.size() == 2) {
    EXPECT_EQ(observedForward[0], false);
    EXPECT_EQ(observedForward[1], std::nullopt);
  }

  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// REQUEST_UPDATE_OK must report the CURRENT largest object, which advances as
// objects publish, rather than the value frozen at SUBSCRIBE_OK time.
CO_TEST_P_X(Draft18Test, SubscribeRequestUpdateReportsCurrentLargest) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        // Subscribe-time largest is {0, 0}.
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  // Publish an object past the subscribe-time largest, advancing the tracker.
  EXPECT_CALL(*subscribeCallback_, datagram(_, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  auto pubRes = trackConsumer->datagram(
      ObjectHeader{5, 0, 3}, moxygen::test::makeBuf(10), /*lastInGroup=*/false);
  EXPECT_FALSE(pubRes.hasError());

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled).Times(1);
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(RequestOk{.requestID = subscribeRequest.requestID}));

  RequestUpdate update;
  update.priority = kDefaultPriority + 1;
  update.forward = true;
  update.params.setMajorVersion(
      getDraftMajorVersion(getServerSelectedVersion()));
  auto result = co_await subscribeHandler->requestUpdate(std::move(update));
  EXPECT_TRUE(result.hasValue());
  if (result.hasValue()) {
    std::optional<AbsoluteLocation> reportedLargest;
    for (const auto& param : result.value().requestSpecificParams) {
      if (param.key ==
          folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT)) {
        reportedLargest = param.largestObject;
        break;
      }
    }
    // The current largest ({5,3}), not the subscribe-time {0,0}.
    EXPECT_EQ(reportedLargest, (AbsoluteLocation{5, 3}));
  }

  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Per draft, a failed coalesced update yields a single REQUEST_ERROR and no
// REQUEST_OK. The held acks for the superseded updates are dropped rather than
// sent — acking them before the coalesced result was known was the bug. The
// applied update surfaces the error and the subscription tears down with
// PUBLISH_DONE(UPDATE_FAILED), so every coalesced update resolves as an error.
CO_TEST_P_X(Draft18Test, SubscribeRequestUpdateCoalescedFailureReportedOnce) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  expectSubscribe(
      [&mockSubscriptionHandle](auto sub, auto /*pub*/) -> TaskSubscribeResult {
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate())
      .Times(kNumCoalescedUpdates);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate())
      .Times(kNumCoalescedUpdates);

  // Only the newest update is applied, and the application rejects it.
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled).Times(1);
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(
          testing::Return(
              folly::makeUnexpected(
                  RequestError{
                      subscribeRequest.requestID,
                      RequestErrorCode::NOT_SUPPORTED,
                      "rejected"})));

  // A failed REQUEST_UPDATE terminates the subscription.
  std::optional<PublishDoneStatusCode> pubDoneCode;
  folly::coro::Baton pubDone;
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&pubDoneCode, &pubDone](const PublishDone& done) {
        pubDoneCode = done.statusCode;
        pubDone.post();
        return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
      });

  std::array<folly::coro::Baton, kNumCoalescedUpdates> updateDone;
  std::array<
      std::optional<folly::Expected<RequestOk, RequestError>>,
      kNumCoalescedUpdates>
      results;
  co_await fireCoalescedUpdates(
      MoQExecutor_.get(),
      subscribeHandler,
      getDraftMajorVersion(getServerSelectedVersion()),
      updateDone,
      results);
  co_await pubDone;

  // No update is acked with a REQUEST_OK: the held OKs are dropped, not sent,
  // so nothing is acknowledged before the coalesced result was known. The
  // applied update surfaces the application's error; the rest resolve as errors
  // when the subscription tears down.
  for (int i = 0; i < kNumCoalescedUpdates; ++i) {
    EXPECT_TRUE(results[i].has_value() && results[i]->hasError())
        << "update " << i << " must not be acked with REQUEST_OK on failure";
  }
  EXPECT_EQ(pubDoneCode, PublishDoneStatusCode::UPDATE_FAILED);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Regression: when the application handler throws, handleRequestUpdate must
// convert the exception into a single REQUEST_ERROR and stop, rather than
// falling through and dereferencing the exception-holding folly::Try (which
// rethrows and would crash the detached handler).
CO_TEST_P_X(Draft18Test, SubscribeRequestUpdateHandlerThrowsReportsError) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  expectSubscribe(
      [&mockSubscriptionHandle](auto sub, auto /*pub*/) -> TaskSubscribeResult {
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled).Times(1);
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(testing::Throw(std::runtime_error("boom")));

  std::optional<PublishDoneStatusCode> pubDoneCode;
  folly::coro::Baton pubDone;
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&pubDoneCode, &pubDone](const PublishDone& done) {
        pubDoneCode = done.statusCode;
        pubDone.post();
        return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
      });

  RequestUpdate update;
  update.priority = kDefaultPriority + 1;
  update.forward = true;
  update.params.setMajorVersion(
      getDraftMajorVersion(getServerSelectedVersion()));
  auto result = co_await subscribeHandler->requestUpdate(std::move(update));
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(result.error().errorCode, RequestErrorCode::INTERNAL_ERROR);
  }
  co_await pubDone;
  EXPECT_EQ(pubDoneCode, PublishDoneStatusCode::UPDATE_FAILED);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Coalesced-burst counterpart to the throw case: when the newest (merged)
// handler throws, the held OKs for the superseded updates must be dropped, not
// left buffered, so a single REQUEST_ERROR is sent and no update is acked OK.
CO_TEST_P_X(
    Draft18Test,
    SubscribeRequestUpdateCoalescedHandlerThrowsDropsHeldOks) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  expectSubscribe(
      [&mockSubscriptionHandle](auto sub, auto /*pub*/) -> TaskSubscribeResult {
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate())
      .Times(kNumCoalescedUpdates);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate())
      .Times(kNumCoalescedUpdates);

  // Only the newest (merged) update is applied, and its handler throws.
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled).Times(1);
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateResult)
      .WillOnce(testing::Throw(std::runtime_error("boom")));

  std::optional<PublishDoneStatusCode> pubDoneCode;
  folly::coro::Baton pubDone;
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([&pubDoneCode, &pubDone](const PublishDone& done) {
        pubDoneCode = done.statusCode;
        pubDone.post();
        return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
      });

  std::array<folly::coro::Baton, kNumCoalescedUpdates> updateDone;
  std::array<
      std::optional<folly::Expected<RequestOk, RequestError>>,
      kNumCoalescedUpdates>
      results;
  co_await fireCoalescedUpdates(
      MoQExecutor_.get(),
      subscribeHandler,
      getDraftMajorVersion(getServerSelectedVersion()),
      updateDone,
      results);
  co_await pubDone;

  // The thrown exception becomes a single REQUEST_ERROR; the held OKs are
  // dropped, so no coalesced update is acked with a REQUEST_OK.
  for (int i = 0; i < kNumCoalescedUpdates; ++i) {
    EXPECT_TRUE(results[i].has_value() && results[i]->hasError())
        << "update " << i << " must not be acked with REQUEST_OK when the "
        << "coalesced handler throws";
  }
  EXPECT_EQ(pubDoneCode, PublishDoneStatusCode::UPDATE_FAILED);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A SubscriptionHandle whose async requestUpdate blocks on a per-invocation
// baton, so a test can hold the application handler mid-flight and control
// exactly when it completes. Invocations are numbered in call order.
namespace {
class BlockingRequestUpdateHandle : public MockSubscriptionHandle {
 public:
  using MockSubscriptionHandle::MockSubscriptionHandle;

  folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
      RequestUpdate update) override {
    const int index = nextInvocation_++;
    requestUpdateCalled(update);
    invoked[index].post();
    co_await release[index];
    co_return requestUpdateResult();
  }

  std::array<folly::coro::Baton, 3> invoked;
  std::array<folly::coro::Baton, 3> release;

 private:
  int nextInvocation_{0};
};
} // namespace

// An in-flight update must not flush a later, still-processing coalesced
// burst's held REQUEST_OKs. Update A is applied on its own; while its
// application handler is suspended, B and C arrive and coalesce (C is newest, B
// is superseded so B's REQUEST_OK is held on requestOks_). When A completes it
// must send only A's REQUEST_OK — B's held OK belongs to the B+C burst and must
// not be flushed until that burst's handler completes. The newest-burst handler
// snapshots requestOks_ before awaiting, so A's completion sees only its own
// OK.
CO_TEST_P_X(Draft18Test, SubscribeRequestUpdateInFlightDoesNotFlushLaterBurst) {
  co_await setupMoQSession();
  std::shared_ptr<BlockingRequestUpdateHandle> handle;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectPublishDone();
  expectSubscribe(
      [&handle, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        SubscribeOk ok;
        ok.requestID = sub.requestID;
        ok.trackAlias = TrackAlias(sub.requestID.value);
        ok.expires = std::chrono::milliseconds(0);
        ok.groupOrder = GroupOrder::OldestFirst;
        ok.largest = AbsoluteLocation{0, 0};
        handle = std::make_shared<BlockingRequestUpdateHandle>(std::move(ok));
        co_return handle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  if (res.hasError()) {
    co_return;
  }
  auto subscribeHandler = res.value();

  // Three updates are received (A, then B and C), so the stat fires three
  // times.
  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate()).Times(3);
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(3);

  // Exactly two application-handler invocations: one for A, one for the merged
  // B+C burst (B is superseded and never reaches the handler).
  EXPECT_CALL(*handle, requestUpdateCalled).Times(2);
  EXPECT_CALL(*handle, requestUpdateResult)
      .WillRepeatedly(
          testing::Return(RequestOk{.requestID = subscribeRequest.requestID}));

  const auto majorVersion = getDraftMajorVersion(getServerSelectedVersion());

  // Fire an update without awaiting; results[i] captures its REQUEST_OK/ERROR
  // once the peer acknowledges it.
  std::array<std::optional<folly::Expected<RequestOk, RequestError>>, 3>
      results;
  std::array<folly::coro::Baton, 3> done;
  auto fire = [&](int i, uint8_t priority) {
    folly::coro::co_withExecutor(
        MoQExecutor_.get(),
        folly::coro::co_invoke([&, i, priority]() -> folly::coro::Task<void> {
          RequestUpdate update;
          update.priority = priority;
          update.forward = true;
          update.params.setMajorVersion(majorVersion);
          results[i] =
              co_await subscribeHandler->requestUpdate(std::move(update));
          done[i].post();
        }))
        .start();
  };

  // A is its own burst. Wait until its application handler is running (which
  // means it has already reset the accumulator and snapshotted the held OKs).
  fire(0, kDefaultPriority + 1);
  co_await handle->invoked[0];

  // B then C arrive while A is in flight; they coalesce, so B's REQUEST_OK is
  // held on requestOks_. Wait until the merged burst's handler is running.
  fire(1, kDefaultPriority + 2);
  fire(2, kDefaultPriority + 3);
  co_await handle->invoked[1];

  // Release A. It resolves on its own and must flush only A's REQUEST_OK.
  handle->release[0].post();
  co_await done[0];
  EXPECT_TRUE(results[0].has_value() && results[0]->hasValue());

  // Give A's completion room to (incorrectly) flush the held OKs. The B+C burst
  // is still suspended, so neither B nor C may be acknowledged yet.
  co_await rescheduleN(4);
  EXPECT_FALSE(results[1].has_value())
      << "B was acked when A completed, before the B+C burst finished";
  EXPECT_FALSE(results[2].has_value())
      << "C was acked when A completed, before the B+C burst finished";

  // Release the B+C burst; now B and C are both acknowledged.
  handle->release[1].post();
  co_await done[1];
  co_await done[2];
  EXPECT_TRUE(results[1].has_value() && results[1]->hasValue());
  EXPECT_TRUE(results[2].has_value() && results[2]->hasValue());

  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// =============================================================================
// FETCH REQUEST_UPDATE tests
//
// ReceiverFetchHandle::requestUpdate answers NOT_SUPPORTED locally, so a client
// fetch handle never puts a REQUEST_UPDATE on the wire. These tests inject the
// updates into the server's control callback instead, which leaves the client
// with no outstanding update to correlate the responder's REQUEST_UPDATE
// responses against — it rejects them as a PROTOCOL_VIOLATION and tears the
// session down. Any test that lets a response be written therefore holds it on
// the fetch bidi (first client-initiated bidi = id 0) so the real client never
// consumes it. The fetch is also still open when these tests close the session,
// which resets the fetch consumer.
// =============================================================================

// FETCH REQUEST_UPDATE coalescing (draft 18+). The client fetch handle does not
// send updates, so inject them server-side: several queued updates collapse to
// a single application call for the newest.
CO_TEST_P_X(Draft18Test, FetchRequestUpdateCoalescesToNewest) {
  co_await setupMoQSession();
  std::shared_ptr<MockFetchHandle> mockFetchHandle = nullptr;
  std::shared_ptr<FetchConsumer> heldFetchConsumer = nullptr;

  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce(
          [&](Fetch fetch,
              std::shared_ptr<FetchConsumer> consumer) -> TaskFetchResult {
            heldFetchConsumer = std::move(consumer);
            mockFetchHandle = makeFetchOkResult(fetch, AbsoluteLocation{0, 10});
            co_return mockFetchHandle;
          });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));

  auto fetchRes =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 10}), fetchCallback_);
  EXPECT_FALSE(fetchRes.hasError());
  if (fetchRes.hasError()) {
    co_return;
  }
  auto fetchRequestID = fetchRes.value()->fetchOk().requestID;

  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate())
      .Times(kNumCoalescedUpdates);
  int observedPriority = -1;
  EXPECT_CALL(*mockFetchHandle, requestUpdateCalled)
      .WillOnce([&observedPriority](const RequestUpdate& u) {
        observedPriority = u.priority.value_or(-1);
      });
  EXPECT_CALL(*mockFetchHandle, requestUpdateResult)
      .WillOnce(testing::Return(RequestOk{.requestID = fetchRequestID}));

  serverWt_->writeHandles.at(0)->setImmediateDelivery(false);

  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());
  for (int i = 0; i < kNumCoalescedUpdates; ++i) {
    RequestUpdate update;
    update.existingRequestID = fetchRequestID;
    update.requestID = RequestID(getRequestIDMultiplier() * (i + 1));
    update.priority = static_cast<uint8_t>(kDefaultPriority + i);
    cb->onRequestUpdate(std::move(update));
  }
  co_await rescheduleN(5);

  // Only the newest update is applied to the application handler.
  EXPECT_EQ(observedPriority, kDefaultPriority + kNumCoalescedUpdates - 1);

  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// FETCH coalescing applies the CUMULATIVE result: a field set by an earlier
// (superseded) update survives when the newest update is silent about it. The
// first update sets a new start; the newest changes only priority (draft 18+).
CO_TEST_P_X(Draft18Test, FetchRequestUpdateCoalescesCumulatively) {
  co_await setupMoQSession();
  std::shared_ptr<MockFetchHandle> mockFetchHandle = nullptr;
  std::shared_ptr<FetchConsumer> heldFetchConsumer = nullptr;

  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce(
          [&](Fetch fetch,
              std::shared_ptr<FetchConsumer> consumer) -> TaskFetchResult {
            heldFetchConsumer = std::move(consumer);
            mockFetchHandle = makeFetchOkResult(fetch, AbsoluteLocation{0, 10});
            co_return mockFetchHandle;
          });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));

  auto fetchRes =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 10}), fetchCallback_);
  EXPECT_FALSE(fetchRes.hasError());
  if (fetchRes.hasError()) {
    co_return;
  }
  auto fetchRequestID = fetchRes.value()->fetchOk().requestID;

  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(2);

  // Only the newest update is applied; it must carry the start from the first
  // update and the priority from the second.
  std::optional<AbsoluteLocation> observedStart;
  int observedPriority = -1;
  EXPECT_CALL(*mockFetchHandle, requestUpdateCalled)
      .WillOnce([&observedStart, &observedPriority](const RequestUpdate& u) {
        observedStart = u.start;
        observedPriority = u.priority.value_or(-1);
      });
  EXPECT_CALL(*mockFetchHandle, requestUpdateResult)
      .WillOnce(testing::Return(RequestOk{.requestID = fetchRequestID}));

  serverWt_->writeHandles.at(0)->setImmediateDelivery(false);

  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());
  // Update 0 sets a new start; update 1 (newest) changes only priority.
  RequestUpdate first;
  first.existingRequestID = fetchRequestID;
  first.requestID = RequestID(getRequestIDMultiplier());
  first.priority = kDefaultPriority + 1;
  first.start = AbsoluteLocation{5, 0};
  cb->onRequestUpdate(std::move(first));

  RequestUpdate second;
  second.existingRequestID = fetchRequestID;
  second.requestID = RequestID(getRequestIDMultiplier() * 2);
  second.priority = kDefaultPriority + 7;
  cb->onRequestUpdate(std::move(second));

  co_await rescheduleN(5);

  // Cumulative: start from update 0, priority from the newest update 1.
  EXPECT_EQ(observedStart, (AbsoluteLocation{5, 0}));
  EXPECT_EQ(observedPriority, kDefaultPriority + 7);

  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, FetchRequestUpdateNotSupported) {
  co_await setupMoQSession();
  std::shared_ptr<MockFetchHandle> mockFetchHandle = nullptr;
  std::shared_ptr<FetchConsumer> fetchPubCaptured = nullptr;

  expectFetch(
      [&mockFetchHandle, &fetchPubCaptured](
          Fetch fetch, auto fetchPub) -> TaskFetchResult {
        auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
        EXPECT_NE(standalone, nullptr);
        mockFetchHandle = makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
        fetchPubCaptured = fetchPub;
        fetchPub->object(
            standalone->start.group,
            /*subgroupID=*/0,
            standalone->start.object,
            moxygen::test::makeBuf(100),
            noExtensions(),
            /*finFetch=*/false,
            /*forwardingPreferenceIsDatagram=*/false);
        co_return mockFetchHandle;
      });

  folly::coro::Baton objectReceived;
  EXPECT_CALL(
      *fetchCallback_,
      object(0, 0, 0, HasChainDataLengthOf(100), _, false, false))
      .WillOnce([&] {
        objectReceived.post();
        return folly::unit;
      });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));

  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await objectReceived;

  // Send REQUEST_UPDATE for the fetch - should return NOT_SUPPORTED
  RequestUpdate requestUpdate{
      RequestID(0),                     // Will be assigned by client
      res.value()->fetchOk().requestID, // Existing fetch request ID
      AbsoluteLocation{50, 0},
      200,
      kDefaultPriority + 1,
      true};

  // FETCH REQUEST_UPDATE is not yet supported - returns NOT_SUPPORTED
  auto updateResult = co_await res.value()->requestUpdate(requestUpdate);
  EXPECT_TRUE(updateResult.hasError());
  EXPECT_EQ(updateResult.error().errorCode, RequestErrorCode::NOT_SUPPORTED);

  // Complete the fetch
  folly::coro::Baton fetchComplete;
  EXPECT_CALL(*fetchCallback_, endOfFetch()).WillOnce([&] {
    fetchComplete.post();
    return folly::unit;
  });
  fetchPubCaptured->endOfFetch();
  co_await fetchComplete;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Regression test: when a REQUEST_UPDATE is queued on the executor but the
// session closes before it runs, the update should not be delivered to the
// application handler.  cleanup() calls unsubscribe() on the handle; the
// subsequent requestUpdate should be suppressed.
CO_TEST_P_X(MoQSessionTest, RequestUpdateAfterClose) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;

  // Cleanup will deliver a publishDone to the client's subscribeCallback_
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce(testing::Return(folly::unit));
  expectSubscribe(
      [&mockSubscriptionHandle](auto sub, auto /*pub*/) -> TaskSubscribeResult {
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());

  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());

  // Simulate a REQUEST_UPDATE arriving at the server for the existing
  // subscribe (requestID 0).  This queues handleRequestUpdate on the
  // executor but it won't run until we yield.
  RequestUpdate update;
  update.existingRequestID = subscribeRequest.requestID;
  update.requestID = RequestID(getRequestIDMultiplier());
  update.priority = kDefaultPriority + 1;
  update.forward = true;
  cb->onRequestUpdate(std::move(update));

  // requestUpdateCalled must NOT be invoked — the session is about to close
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled).Times(0);

  // Close the session before handleRequestUpdate runs.  Use a PUBLISH with
  // wrong requestID parity to trigger closeSessionIfRequestIDInvalid.
  cb->onPublish(PublishRequest{.requestID = RequestID(1), .fullTrackName = {}});

  // Yield to let queued coroutines run
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;
}

// Regression test: when a REQUEST_UPDATE is queued on the executor but
// publishDone resets subscriptionHandle_ before it runs, handleRequestUpdate
// must not dereference the null handle.  Unlike RequestUpdateAfterClose, the
// session stays open — the cancellation token is NOT cancelled — so
// co_safe_point alone does not protect against this.
CO_TEST_P_X(MoQSessionTest, RequestUpdateAfterPublishDone) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;

  expectPublishDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());

  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());

  // Queue a REQUEST_UPDATE — the coroutine is enqueued via .start() but
  // won't run until we yield.
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  RequestUpdate update;
  update.existingRequestID = subscribeRequest.requestID;
  update.requestID = RequestID(getRequestIDMultiplier());
  update.priority = kDefaultPriority + 1;
  update.forward = true;
  cb->onRequestUpdate(std::move(update));

  // Now send publishDone from the server's publisher.  This synchronously
  // resets subscriptionHandle_ (MoQSession.cpp TrackPublisherImpl::publishDone)
  // WITHOUT cancelling the session's cancellation token.
  trackConsumer->publishDone(
      getTrackEndedPublishDone(subscribeRequest.requestID));
  co_await publishDone_;

  // requestUpdateCalled must NOT be invoked — subscriptionHandle_ is null.
  // Without the null-check fix, this crashes with SIGSEGV.
  EXPECT_CALL(*mockSubscriptionHandle, requestUpdateCalled).Times(0);

  // Yield to let the queued handleRequestUpdate coroutine drain
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;

  // Session is still alive (not closed) — verify we can still close cleanly
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// =============================================================================
// FETCH REQUEST_UPDATE tests
// =============================================================================

// A FetchHandle whose requestUpdate suspends on a baton, allowing the test to
// interleave session teardown between the co_await and resume.
class SuspendingFetchHandle : public MockFetchHandle {
 public:
  using MockFetchHandle::MockFetchHandle;

  folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
      RequestUpdate update) override {
    co_await baton_;
    co_return RequestOk{.requestID = update.requestID};
  }

  folly::coro::Baton baton_;
};

// A FetchHandle whose requestUpdate blocks on a per-invocation baton, so a test
// can hold the application handler mid-flight and control exactly when it
// completes. Invocations are numbered in call order (A is 0, the merged B+C
// burst is 1; a superseded update never reaches the handler).
class BlockingFetchRequestUpdateHandle : public MockFetchHandle {
 public:
  using MockFetchHandle::MockFetchHandle;

  folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
      RequestUpdate update) override {
    const int index = nextInvocation_++;
    requestUpdateCalled(update);
    invoked[index].post();
    co_await release[index];
    co_return requestUpdateResult();
  }

  std::array<folly::coro::Baton, 2> invoked;
  std::array<folly::coro::Baton, 2> release;

 private:
  int nextInvocation_{0};
};

// Like BlockingFetchRequestUpdateHandle, but each invocation returns its own
// result, so a test can complete the invocations out of order and give each a
// distinct outcome. Invocations are numbered in call order (A is 0, the merged
// B+C burst is 1).
class OutOfOrderFetchRequestUpdateHandle : public MockFetchHandle {
 public:
  using MockFetchHandle::MockFetchHandle;

  folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
      RequestUpdate update) override {
    const int index = nextInvocation_++;
    requestUpdateCalled(update);
    invoked[index].post();
    co_await release[index];
    co_return results[index].value();
  }

  std::array<folly::coro::Baton, 2> invoked;
  std::array<folly::coro::Baton, 2> release;
  std::array<std::optional<folly::Expected<RequestOk, RequestError>>, 2>
      results;

 private:
  int nextInvocation_{0};
};

// Regression test: FetchPublisherImpl::onRequestUpdate dereferences session_
// after co_await without a null check.  If terminatePublish (via cleanup)
// runs while requestUpdate is suspended, session_ is nulled but handle_ is
// not, so there is no existing guard.
CO_TEST_P_X(MoQSessionTest, FetchRequestUpdateNullSessionAfterAwait) {
  // This test only applies to v16+ which supports fetch REQUEST_UPDATE.
  if (getDraftMajorVersion(getServerSelectedVersion()) < 16) {
    co_return;
  }

  co_await setupMoQSession();
  std::shared_ptr<SuspendingFetchHandle> suspendingHandle;
  std::shared_ptr<FetchConsumer> heldFetchConsumer;

  // Server accepts the fetch with a SuspendingFetchHandle.
  // Do NOT end the fetch — keep the FetchPublisherImpl alive in pubTracks_.
  EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess())
      .RetiresOnSaturation();
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce(
          [&](Fetch fetch,
              std::shared_ptr<FetchConsumer> consumer) -> TaskFetchResult {
            heldFetchConsumer = std::move(consumer);
            suspendingHandle = std::make_shared<SuspendingFetchHandle>(FetchOk{
                fetch.requestID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/false,
                AbsoluteLocation{0, 10}});
            co_return suspendingHandle;
          })
      .RetiresOnSaturation();

  // Client sends fetch.
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto fetchRes =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 10}), fetchCallback_);
  EXPECT_FALSE(fetchRes.hasError());

  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());

  // Send a REQUEST_UPDATE for the fetch.  handleFetchRequestUpdate starts a
  // detached coroutine that calls handle_->requestUpdate(), which suspends
  // on baton_.
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate());
  RequestUpdate update;
  update.existingRequestID = fetchRes.value()->fetchOk().requestID;
  update.requestID = RequestID(getRequestIDMultiplier());
  update.priority = kDefaultPriority + 1;
  cb->onRequestUpdate(std::move(update));

  // Yield so the detached coroutine reaches the co_await inside
  // SuspendingFetchHandle::requestUpdate.
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;

  // Close the server session while requestUpdate is suspended.  cleanup()
  // erases from pubTracks_, then calls terminatePublish on FetchPublisherImpl
  // which does reset() -> streamPublisher_->reset() -> onStreamComplete ->
  // fetchComplete -> session_ = null.  The cancellation token is also
  // triggered, but co_awaitTry catches the OperationCancelled — execution
  // continues to the dereference of session_ without a null check.
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED));
  serverSession_->close(SessionCloseErrorCode::NO_ERROR);

  // Post the baton — the coroutine resumes.  Without the fix, this crashes
  // with SIGSEGV on session_->getNegotiatedVersion().
  suspendingHandle->baton_.post();

  // Yield to let the resumed coroutine drain.
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;

  heldFetchConsumer.reset();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// FETCH counterpart to SubscribeRequestUpdateInFlightDoesNotFlushLaterBurst.
// While update A's application handler is in flight, updates B and C arrive and
// coalesce (C newest; B superseded, so B's REQUEST_OK is held in requestOks_).
// A's completion must flush ONLY A's REQUEST_OK — B's held ack belongs to the
// still-processing B+C burst and must not go out with A. FetchPublisherImpl
// snapshots the held acks before awaiting, so A's completion sees only its own.
CO_TEST_P_X(Draft18Test, FetchRequestUpdateInFlightDoesNotFlushLaterBurst) {
  co_await setupMoQSession();
  std::shared_ptr<BlockingFetchRequestUpdateHandle> handle;
  std::shared_ptr<FetchConsumer> heldFetchConsumer = nullptr;

  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce(
          [&](Fetch fetch,
              std::shared_ptr<FetchConsumer> consumer) -> TaskFetchResult {
            heldFetchConsumer = std::move(consumer);
            handle = std::make_shared<BlockingFetchRequestUpdateHandle>(FetchOk{
                fetch.requestID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/false,
                AbsoluteLocation{0, 10}});
            co_return handle;
          });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));

  auto fetchRes =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 10}), fetchCallback_);
  EXPECT_FALSE(fetchRes.hasError());
  if (fetchRes.hasError()) {
    co_return;
  }
  auto fetchRequestID = fetchRes.value()->fetchOk().requestID;

  // A, B, C are all received (stat fires 3x); B is superseded, so only A and
  // the merged B+C burst reach the application handler.
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(3);
  EXPECT_CALL(*handle, requestUpdateCalled).Times(2);
  EXPECT_CALL(*handle, requestUpdateResult)
      .WillRepeatedly(testing::Return(RequestOk{.requestID = fetchRequestID}));

  // Hold the server->client acks on the fetch bidi (first client-initiated bidi
  // = id 0) so the real client never consumes them. FETCH_OK was already
  // delivered above, so subsequent bytes on this handle are REQUEST_OK acks.
  auto fetchBidi = serverWt_->writeHandles.at(0);
  fetchBidi->setImmediateDelivery(false);
  const uint32_t bytesAfterFetchOk = fetchBidi->dataWritten_;

  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());
  auto inject = [&](uint64_t reqIdMultiple, uint8_t priority) {
    RequestUpdate update;
    update.existingRequestID = fetchRequestID;
    update.requestID = RequestID(getRequestIDMultiplier() * reqIdMultiple);
    update.priority = priority;
    cb->onRequestUpdate(std::move(update));
  };

  // A is its own burst; wait until its handler is running (accumulator reset
  // and held acks snapshotted, now suspended before it can flush).
  inject(1, kDefaultPriority + 1);
  co_await handle->invoked[0];

  // B then C arrive while A is in flight and coalesce; B's REQUEST_OK is held.
  // Wait until the merged burst's handler is running (and suspended).
  inject(2, kDefaultPriority + 2);
  inject(3, kDefaultPriority + 3);
  co_await handle->invoked[1];

  // Release A. It must flush only its own REQUEST_OK, not B's held ack.
  handle->release[0].post();
  co_await rescheduleN(4);
  const uint32_t bytesFromA = fetchBidi->dataWritten_ - bytesAfterFetchOk;
  EXPECT_GT(bytesFromA, 0u);

  // Release the B+C burst; it flushes B's held ack plus C's own — two acks.
  handle->release[1].post();
  co_await rescheduleN(4);
  const uint32_t bytesFromBurst =
      fetchBidi->dataWritten_ - bytesAfterFetchOk - bytesFromA;
  EXPECT_EQ(bytesFromBurst, 2 * bytesFromA)
      << "A's completion flushed B's held REQUEST_OK early (expected A to flush "
         "one ack and the B+C burst to flush two)";

  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A arrives and its handler suspends. B and C then arrive and coalesce (C
// newest, B superseded). The B+C burst succeeds *before* A, and A then fails.
// If the burst's REQUEST_OKs go out as soon as they are ready, the wire carries
// REQUEST_OK, REQUEST_OK, REQUEST_ERROR — and the peer FIFO-maps them to A=OK,
// B=OK, C=ERROR, concluding that the failed update A succeeded. A's response
// must go out first.
CO_TEST_P_X(Draft18Test, FetchRequestUpdateResponsesFollowArrivalOrder) {
  co_await setupMoQSession();
  std::shared_ptr<OutOfOrderFetchRequestUpdateHandle> handle;
  std::shared_ptr<FetchConsumer> heldFetchConsumer = nullptr;

  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce(
          [&](Fetch fetch,
              std::shared_ptr<FetchConsumer> consumer) -> TaskFetchResult {
            heldFetchConsumer = std::move(consumer);
            handle =
                std::make_shared<OutOfOrderFetchRequestUpdateHandle>(FetchOk{
                    fetch.requestID,
                    GroupOrder::OldestFirst,
                    /*endOfTrack=*/false,
                    AbsoluteLocation{0, 10}});
            co_return handle;
          });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));

  auto fetchRes =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 10}), fetchCallback_);
  EXPECT_FALSE(fetchRes.hasError());
  if (fetchRes.hasError()) {
    co_return;
  }
  auto fetchRequestID = fetchRes.value()->fetchOk().requestID;

  // A, B, C are all received (stat fires 3x); B is superseded, so only A and
  // the merged B+C burst reach the application handler.
  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(3);
  EXPECT_CALL(*handle, requestUpdateCalled).Times(2);

  // A fails; the merged B+C burst succeeds.
  handle->results[0] = folly::makeUnexpected(
      RequestError{
          RequestID(getRequestIDMultiplier()),
          RequestErrorCode::INTERNAL_ERROR,
          "A failed"});
  handle->results[1] = RequestOk{.requestID = fetchRequestID};

  // Hold the server->client responses on the fetch bidi (first client-initiated
  // bidi = id 0) so the real client never consumes them. FETCH_OK was already
  // delivered above, so subsequent bytes on this handle are REQUEST_UPDATE
  // responses.
  auto fetchBidi = serverWt_->writeHandles.at(0);
  fetchBidi->setImmediateDelivery(false);
  const uint32_t bytesAfterFetchOk = fetchBidi->dataWritten_;

  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());
  auto inject = [&](uint64_t reqIdMultiple, uint8_t priority) {
    RequestUpdate update;
    update.existingRequestID = fetchRequestID;
    update.requestID = RequestID(getRequestIDMultiplier() * reqIdMultiple);
    update.priority = priority;
    cb->onRequestUpdate(std::move(update));
  };

  // A is its own burst; wait until its handler is running (and suspended).
  inject(1, kDefaultPriority + 1);
  co_await handle->invoked[0];

  // B then C arrive while A is in flight and coalesce. Wait until the merged
  // burst's handler is running (and suspended).
  inject(2, kDefaultPriority + 2);
  inject(3, kDefaultPriority + 3);
  co_await handle->invoked[1];

  // The B+C burst finishes first, but A arrived first and has not responded
  // yet, so the burst's REQUEST_OKs must be held: nothing may go out ahead of
  // A's response.
  handle->release[1].post();
  co_await rescheduleN(4);
  EXPECT_EQ(fetchBidi->dataWritten_, bytesAfterFetchOk)
      << "B/C were acked before A responded; the peer correlates responses in "
         "arrival order and would read B's REQUEST_OK as A's response";

  // Now let A fail. Its REQUEST_ERROR must be the first response on the wire.
  handle->release[0].post();
  co_await rescheduleN(4);
  EXPECT_GT(fetchBidi->dataWritten_, bytesAfterFetchOk);
  const auto* responses = fetchBidi->inflightBuf_.front();
  EXPECT_NE(responses, nullptr);
  if (responses != nullptr && responses->computeChainDataLength() > 0) {
    EXPECT_EQ(
        responses->cloneCoalesced()->data()[0],
        static_cast<uint8_t>(FrameType::REQUEST_ERROR))
        << "the first REQUEST_UPDATE response on the wire must be A's "
           "REQUEST_ERROR";
  }

  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Ordering responses makes each burst wait on the one before it, so a burst
// that bails out early must still release its place in that order.
//
// A and B are separate bursts, so B waits on A. The session closes while both
// application handlers are suspended, which nulls session_ and sends both
// coroutines down the early return after their await — A responds to nobody.
// If A leaves without releasing its turn, B waits forever, and because B's
// coroutine holds a shared_ptr to the FetchPublisherImpl, the publisher (and
// the fetch handle it owns) leaks along with the stranded coroutine. The
// symptom is a leak rather than a bad byte on the wire, so assert on
// teardown: once both coroutines finish, nothing holds the handle.
CO_TEST_P_X(Draft18Test, FetchRequestUpdateEarlyReturnReleasesResponseOrder) {
  co_await setupMoQSession();
  std::shared_ptr<BlockingFetchRequestUpdateHandle> handle;
  std::shared_ptr<FetchConsumer> heldFetchConsumer = nullptr;

  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce(
          [&](Fetch fetch,
              std::shared_ptr<FetchConsumer> consumer) -> TaskFetchResult {
            heldFetchConsumer = std::move(consumer);
            handle = std::make_shared<BlockingFetchRequestUpdateHandle>(FetchOk{
                fetch.requestID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/false,
                AbsoluteLocation{0, 10}});
            co_return handle;
          });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));

  auto fetchRes =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 10}), fetchCallback_);
  EXPECT_FALSE(fetchRes.hasError());
  if (fetchRes.hasError()) {
    co_return;
  }
  auto fetchRequestID = fetchRes.value()->fetchOk().requestID;

  EXPECT_CALL(*serverPublisherStatsCallback_, onRequestUpdate()).Times(2);
  EXPECT_CALL(*handle, requestUpdateCalled).Times(2);
  EXPECT_CALL(*handle, requestUpdateResult)
      .WillRepeatedly(testing::Return(RequestOk{.requestID = fetchRequestID}));

  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());
  auto inject = [&](uint64_t reqIdMultiple, uint8_t priority) {
    RequestUpdate update;
    update.existingRequestID = fetchRequestID;
    update.requestID = RequestID(getRequestIDMultiplier() * reqIdMultiple);
    update.priority = priority;
    cb->onRequestUpdate(std::move(update));
  };

  // Two separate bursts: A claims and resets the accumulator before B arrives,
  // so B queues behind A rather than coalescing with it.
  inject(1, kDefaultPriority + 1);
  co_await handle->invoked[0];
  inject(2, kDefaultPriority + 2);
  co_await handle->invoked[1];

  // Close while both handlers are suspended, so both coroutines take the
  // early return once they resume.
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::SESSION_CLOSED));
  serverSession_->close(SessionCloseErrorCode::NO_ERROR);
  handle->release[0].post();
  handle->release[1].post();
  co_await rescheduleN(6);

  std::weak_ptr<BlockingFetchRequestUpdateHandle> weakHandle = handle;
  handle.reset();
  heldFetchConsumer.reset();
  EXPECT_TRUE(weakHandle.expired())
      << "a burst is still suspended waiting on a turn that was never "
         "released, keeping the FetchPublisherImpl alive";

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// =============================================================================
// Namespace REQUEST_UPDATE tests
// =============================================================================

// A failed REQUEST_UPDATE for a SUBSCRIBE_NAMESPACE must close the request's
// bidi stream. The responder sends REQUEST_ERROR, FINs its write half, and
// tears down the subscription (draft 18+).
CO_TEST_P_X(Draft18Test, SubscribeNamespaceRequestUpdateFailureClosesBidi) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeNamespaceHandle> serverHandle;
  RequestID serverRequestID{0};
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          [&](auto subAnn, auto /*handler*/)
              -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
            serverRequestID = subAnn.requestID;
            serverHandle = std::make_shared<MockSubscribeNamespaceHandle>(
                SubscribeNamespaceOk(
                    {.requestID = subAnn.requestID,
                     .requestSpecificParams = {}}));
            co_return serverHandle;
          });

  auto subNsResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_FALSE(subNsResult.hasError());

  // The SUBSCRIBE_NAMESPACE bidi is the first client-initiated bidi (id 0).
  // Capture the responder's write half up front: the fake client cannot send
  // namespace updates, so it will tear the stream down once it sees the
  // unsolicited REQUEST_ERROR — we assert on the responder's FIN, which happens
  // first, via the retained handle.
  auto serverBidi = serverWt_->writeHandles.at(0);

  // The application rejects the update, so the responder must tear it down.
  folly::coro::Baton updateHandled;
  EXPECT_CALL(*serverHandle, requestUpdateCalled(_))
      .WillOnce([&](const RequestUpdate&) { updateHandled.post(); });
  EXPECT_CALL(*serverHandle, requestUpdateResult())
      .WillOnce(
          testing::Return(
              folly::makeUnexpected(
                  RequestError{
                      serverRequestID,
                      RequestErrorCode::NOT_SUPPORTED,
                      "namespace updates unsupported"})));
  folly::coro::Baton unsubscribed;
  EXPECT_CALL(*serverHandle, unsubscribeNamespace()).WillOnce([&] {
    unsubscribed.post();
  });

  RequestUpdate update;
  update.existingRequestID = serverRequestID;
  update.requestID = RequestID(getRequestIDMultiplier());
  update.priority = kDefaultPriority + 1;
  update.forward = true;
  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());
  cb->onRequestUpdate(std::move(update));

  co_await updateHandled;
  co_await unsubscribed;

  // The responder FINs its write half to close the bidi.
  EXPECT_TRUE(serverBidi->fin_);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Draft section 10.9.2: a subscriber moves an established SUBSCRIBE_NAMESPACE
// to a new Track Namespace Prefix by sending a REQUEST_UPDATE carrying the
// TRACK_NAMESPACE_PREFIX parameter. This drives the update end-to-end over the
// wire: the client handle sends it, the responder decodes the new prefix, and
// the REQUEST_OK round-trips back to the client (draft 18+).
CO_TEST_P_X(Draft18Test, SubscribeNamespaceRequestUpdatePrefixRoundTrip) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeNamespaceHandle> serverHandle;
  RequestID serverRequestID{0};
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          [&](auto subAnn, auto /*handler*/)
              -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
            serverRequestID = subAnn.requestID;
            serverHandle = std::make_shared<MockSubscribeNamespaceHandle>(
                SubscribeNamespaceOk(
                    {.requestID = subAnn.requestID,
                     .requestSpecificParams = {}}));
            co_return serverHandle;
          });

  auto subNsResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_FALSE(subNsResult.hasError());
  if (subNsResult.hasError()) {
    co_return;
  }
  auto handle = subNsResult.value();

  const TrackNamespace newPrefix{{"foo", "bar"}};

  // The client send path records the subscriber-side REQUEST_UPDATE stat.
  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());

  folly::coro::Baton updateHandled;
  EXPECT_CALL(*serverHandle, requestUpdateCalled(_))
      .WillOnce([&](const RequestUpdate& update) {
        // The responder receives the new prefix as the TRACK_NAMESPACE_PREFIX
        // parameter and can decode it back to newPrefix.
        bool foundPrefix = false;
        TrackNamespace decodedPrefix;
        for (const auto& param : update.params) {
          if (param.key ==
              folly::to_underlying(
                  TrackRequestParamKey::TRACK_NAMESPACE_PREFIX)) {
            auto decoded = MoQFrameParser::parseTrackNamespacePrefixParam(
                param.asString, kVersionDraft18);
            EXPECT_FALSE(decoded.hasError());
            if (!decoded.hasError()) {
              decodedPrefix = decoded.value();
              foundPrefix = true;
            }
            break;
          }
        }
        EXPECT_TRUE(foundPrefix);
        EXPECT_EQ(decodedPrefix, newPrefix);
        updateHandled.post();
      });
  EXPECT_CALL(*serverHandle, requestUpdateResult())
      .WillOnce(testing::Return(RequestOk{.requestID = serverRequestID}));

  RequestUpdate update;
  update.params.setMajorVersion(getDraftMajorVersion(kVersionDraft18));
  update.params.insertParam(
      MoQFrameWriter::encodeTrackNamespacePrefixParam(
          newPrefix, kVersionDraft18));
  auto updateResult = co_await handle->requestUpdate(std::move(update));
  co_await updateHandled;
  EXPECT_TRUE(updateResult.hasValue());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A SUBSCRIBE_NAMESPACE REQUEST_UPDATE must also round-trip at draft 16/17
// (the TRACK_NAMESPACE_PREFIX parameter is draft-18-only, so this drives a
// plain update). Pre-draft-18 there are no per-request bidi streams, so the
// update and its REQUEST_OK both ride the shared control stream and correlate
// by the on-wire requestID. This guards that pre-18 control-stream path (draft
// 18+ instead rides the subscription's own bidi).
CO_TEST_P_X(PreDraft18Test, SubscribeNamespaceRequestUpdateRoundTrip) {
  co_await setupMoQSession();
  const auto version = *clientSession_->getNegotiatedVersion();
  if (getDraftMajorVersion(version) < 16) {
    // REQUEST_UPDATE for SUBSCRIBE_NAMESPACE only exists at draft 16+.
    co_return;
  }

  std::shared_ptr<MockSubscribeNamespaceHandle> serverHandle;
  RequestID serverRequestID{0};
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          [&](auto subAnn, auto /*handler*/)
              -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
            serverRequestID = subAnn.requestID;
            serverHandle = std::make_shared<MockSubscribeNamespaceHandle>(
                SubscribeNamespaceOk(
                    {.requestID = subAnn.requestID,
                     .requestSpecificParams = {}}));
            co_return serverHandle;
          });

  auto subNsResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_FALSE(subNsResult.hasError());
  if (subNsResult.hasError()) {
    co_return;
  }
  auto handle = subNsResult.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());

  folly::coro::Baton updateHandled;
  EXPECT_CALL(*serverHandle, requestUpdateCalled(_))
      .WillOnce([&](const RequestUpdate& update) {
        EXPECT_EQ(update.priority, kDefaultPriority + 1);
        updateHandled.post();
      });
  EXPECT_CALL(*serverHandle, requestUpdateResult())
      .WillOnce(testing::Return(RequestOk{.requestID = serverRequestID}));

  RequestUpdate update;
  update.params.setMajorVersion(getDraftMajorVersion(version));
  update.priority = kDefaultPriority + 1;
  update.forward = true;
  auto updateResult = co_await handle->requestUpdate(std::move(update));
  co_await updateHandled;
  EXPECT_TRUE(updateResult.hasValue());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A SUBSCRIBE_NAMESPACE REQUEST_UPDATE rides the subscription's own bidi under
// its own request ID (not the stream's SUBSCRIBE_NAMESPACE id). If the peer
// closes that bidi before replying, the update's awaiting coroutine must fail
// rather than hang — the stream-close path has to fail every in-flight update
// queued on the stream, not just its primary request (draft 18+).
CO_TEST_P_X(
    Draft18Test,
    SubscribeNamespaceRequestUpdateFailsOnPeerResetWithoutReply) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          [&](auto subAnn, auto /*handler*/)
              -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
            co_return std::make_shared<MockSubscribeNamespaceHandle>(
                SubscribeNamespaceOk(
                    {.requestID = subAnn.requestID,
                     .requestSpecificParams = {}}));
          });

  auto subNsResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_FALSE(subNsResult.hasError());
  if (subNsResult.hasError()) {
    co_return;
  }
  auto handle = subNsResult.value();

  // Hold the update on the wire so the peer never replies to it. The
  // SUBSCRIBE_NAMESPACE bidi is the first client-initiated bidi (id 0), and
  // this is the client's write half.
  clientWt_->writeHandles.at(0)->setImmediateDelivery(false);

  // onRequestUpdate fires in the send path just before the coroutine suspends
  // awaiting the reply, so it is a stable point at which to close the stream.
  folly::coro::Baton updateSent;
  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate()).WillOnce([&] {
    updateSent.post();
  });

  std::optional<RequestErrorCode> errorCode;
  folly::coro::Baton done;
  folly::coro::co_withExecutor(
      MoQExecutor_.get(),
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        RequestUpdate update;
        update.params.setMajorVersion(getDraftMajorVersion(kVersionDraft18));
        update.params.insertParam(
            MoQFrameWriter::encodeTrackNamespacePrefixParam(
                TrackNamespace{{"foo", "bar"}}, kVersionDraft18));
        auto result = co_await handle->requestUpdate(std::move(update));
        if (result.hasError()) {
          errorCode = result.error().errorCode;
        }
        done.post();
      }))
      .start();

  co_await updateSent;
  // Peer RESETs the SUBSCRIBE_NAMESPACE bidi (the client's read half) without
  // ever sending REQUEST_OK / REQUEST_ERROR for the update.
  serverWt_->writeHandles.at(0)->resetStream(
      folly::to_underlying(ResetStreamErrorCode::CANCELLED));

  co_await done;
  EXPECT_TRUE(errorCode.has_value());
  if (errorCode.has_value()) {
    EXPECT_EQ(*errorCode, RequestErrorCode::INTERNAL_ERROR);
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Same hang guard as above, opposite ordering: the peer RESETs the
// SUBSCRIBE_NAMESPACE bidi *before* any REQUEST_UPDATE is sent, so the stream's
// read loop has already exited by the time requestUpdate() runs. Nothing would
// ever drain a pending registered on the dead stream, so the send path must
// fail fast rather than register a doomed pending and hang (draft 18+).
CO_TEST_P_X(
    Draft18Test,
    SubscribeNamespaceRequestUpdateFailsWhenStreamAlreadyReset) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          [&](auto subAnn, auto /*handler*/)
              -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
            co_return std::make_shared<MockSubscribeNamespaceHandle>(
                SubscribeNamespaceOk(
                    {.requestID = subAnn.requestID,
                     .requestSpecificParams = {}}));
          });

  auto subNsResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_FALSE(subNsResult.hasError());
  if (subNsResult.hasError()) {
    co_return;
  }
  auto handle = subNsResult.value();

  // Peer RESETs the SUBSCRIBE_NAMESPACE bidi (the client's read half) with no
  // update in flight, then let the client's read loop resume, process the RST,
  // and exit. After this the stream can no longer carry a REQUEST_OK /
  // REQUEST_ERROR reply.
  serverWt_->writeHandles.at(0)->resetStream(
      folly::to_underlying(ResetStreamErrorCode::CANCELLED));
  co_await rescheduleN(4);

  // The send path bails out before recording the stat or writing to the wire,
  // since the stream is already dead.
  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate()).Times(0);

  RequestUpdate update;
  update.params.setMajorVersion(getDraftMajorVersion(kVersionDraft18));
  update.params.insertParam(
      MoQFrameWriter::encodeTrackNamespacePrefixParam(
          TrackNamespace{{"foo", "bar"}}, kVersionDraft18));
  auto result = co_await handle->requestUpdate(std::move(update));
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(result.error().errorCode, RequestErrorCode::INTERNAL_ERROR);
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A failed REQUEST_UPDATE for a PUBLISH_NAMESPACE must close the request's bidi
// stream. The responder sends REQUEST_ERROR, FINs its write half, and tears
// down the announcement (draft 18+).
CO_TEST_P_X(Draft18Test, PublishNamespaceRequestUpdateFailureClosesBidi) {
  co_await setupMoQSession();

  std::shared_ptr<MockPublishNamespaceHandle> serverHandle;
  RequestID serverRequestID{0};
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          [&](auto ann, auto /*cb*/)
              -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
            serverRequestID = ann.requestID;
            serverHandle =
                std::make_shared<MockPublishNamespaceHandle>(PublishNamespaceOk(
                    {.requestID = ann.requestID, .requestSpecificParams = {}}));
            co_return Subscriber::PublishNamespaceResult(serverHandle);
          });

  auto annResult =
      co_await clientSession_->publishNamespace(getPublishNamespace());
  EXPECT_FALSE(annResult.hasError());

  // The PUBLISH_NAMESPACE bidi is the first client-initiated bidi (id 0).
  // Capture the responder's write half up front (see the SUBSCRIBE_NAMESPACE
  // test for why).
  auto serverBidi = serverWt_->writeHandles.at(0);

  folly::coro::Baton updateHandled;
  EXPECT_CALL(*serverHandle, requestUpdateCalled(_))
      .WillOnce([&](const RequestUpdate&) { updateHandled.post(); });
  EXPECT_CALL(*serverHandle, requestUpdateResult())
      .WillOnce(
          testing::Return(
              folly::makeUnexpected(
                  RequestError{
                      serverRequestID,
                      RequestErrorCode::NOT_SUPPORTED,
                      "namespace updates unsupported"})));
  folly::coro::Baton done;
  EXPECT_CALL(*serverHandle, publishNamespaceDone()).WillOnce([&] {
    done.post();
  });

  RequestUpdate update;
  update.existingRequestID = serverRequestID;
  update.requestID = RequestID(getRequestIDMultiplier());
  update.priority = kDefaultPriority + 1;
  update.forward = true;
  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());
  cb->onRequestUpdate(std::move(update));

  co_await updateHandled;
  co_await done;

  // The responder FINs its write half to close the bidi.
  EXPECT_TRUE(serverBidi->fin_);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Draft section 10.9.2: a subscriber moves an established SUBSCRIBE_TRACKS to a
// new Track Namespace Prefix by sending a REQUEST_UPDATE carrying the
// TRACK_NAMESPACE_PREFIX parameter. This drives the update end-to-end over the
// wire: the client handle sends it, the responder decodes the new prefix, and
// the REQUEST_OK round-trips back to the client (draft 18+).
CO_TEST_P_X(Draft18Test, SubscribeTracksRequestUpdatePrefixRoundTrip) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeTracksHandle> serverHandle;
  RequestID serverRequestID{0};
  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          [&](auto subTracks, auto /*publishBlockedHandle*/)
              -> folly::coro::Task<Publisher::SubscribeTracksResult> {
            serverRequestID = subTracks.requestID;
            serverHandle =
                std::make_shared<MockSubscribeTracksHandle>(SubscribeTracksOk(
                    {.requestID = subTracks.requestID,
                     .requestSpecificParams = {}}));
            co_return serverHandle;
          });

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_FALSE(result.hasError());
  if (result.hasError()) {
    co_return;
  }
  auto handle = result.value();

  const TrackNamespace newPrefix{{"foo", "bar"}};

  // The client send path records the subscriber-side REQUEST_UPDATE stat.
  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());

  folly::coro::Baton updateHandled;
  EXPECT_CALL(*serverHandle, requestUpdateCalled(_))
      .WillOnce([&](const RequestUpdate& update) {
        // The responder receives the new prefix as the TRACK_NAMESPACE_PREFIX
        // parameter and can decode it back to newPrefix.
        bool foundPrefix = false;
        TrackNamespace decodedPrefix;
        for (const auto& param : update.params) {
          if (param.key ==
              folly::to_underlying(
                  TrackRequestParamKey::TRACK_NAMESPACE_PREFIX)) {
            auto decoded = MoQFrameParser::parseTrackNamespacePrefixParam(
                param.asString, kVersionDraft18);
            EXPECT_FALSE(decoded.hasError());
            if (!decoded.hasError()) {
              decodedPrefix = decoded.value();
              foundPrefix = true;
            }
            break;
          }
        }
        EXPECT_TRUE(foundPrefix);
        EXPECT_EQ(decodedPrefix, newPrefix);
        updateHandled.post();
      });
  EXPECT_CALL(*serverHandle, requestUpdateResult())
      .WillOnce(testing::Return(RequestOk{.requestID = serverRequestID}));

  RequestUpdate update;
  update.params.setMajorVersion(getDraftMajorVersion(kVersionDraft18));
  update.params.insertParam(
      MoQFrameWriter::encodeTrackNamespacePrefixParam(
          newPrefix, kVersionDraft18));
  auto updateResult = co_await handle->requestUpdate(std::move(update));
  co_await updateHandled;
  EXPECT_TRUE(updateResult.hasValue());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A subscriber can update the SUBSCRIBE_TRACKS Forwarding State end-to-end by
// sending a REQUEST_UPDATE carrying a FORWARD value; the responder receives it
// and the REQUEST_OK round-trips back (draft 18+).
CO_TEST_P_X(Draft18Test, SubscribeTracksRequestUpdateForwardRoundTrip) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeTracksHandle> serverHandle;
  RequestID serverRequestID{0};
  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          [&](auto subTracks, auto /*publishBlockedHandle*/)
              -> folly::coro::Task<Publisher::SubscribeTracksResult> {
            serverRequestID = subTracks.requestID;
            serverHandle =
                std::make_shared<MockSubscribeTracksHandle>(SubscribeTracksOk(
                    {.requestID = subTracks.requestID,
                     .requestSpecificParams = {}}));
            co_return serverHandle;
          });

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_FALSE(result.hasError());
  if (result.hasError()) {
    co_return;
  }
  auto handle = result.value();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onRequestUpdate());

  folly::coro::Baton updateHandled;
  EXPECT_CALL(*serverHandle, requestUpdateCalled(_))
      .WillOnce([&](const RequestUpdate& update) {
        // The FORWARD value round-trips as the update's forward field.
        EXPECT_TRUE(update.forward.has_value());
        if (update.forward.has_value()) {
          EXPECT_FALSE(*update.forward);
        }
        updateHandled.post();
      });
  EXPECT_CALL(*serverHandle, requestUpdateResult())
      .WillOnce(testing::Return(RequestOk{.requestID = serverRequestID}));

  RequestUpdate update;
  update.params.setMajorVersion(getDraftMajorVersion(kVersionDraft18));
  update.forward = false;
  auto updateResult = co_await handle->requestUpdate(std::move(update));
  co_await updateHandled;
  EXPECT_TRUE(updateResult.hasValue());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A failed REQUEST_UPDATE for a SUBSCRIBE_TRACKS must close the request's bidi
// stream: the responder rejects it with REQUEST_ERROR, FINs its write half, and
// tears down the handle (draft 18+).
CO_TEST_P_X(Draft18Test, SubscribeTracksRequestUpdateFailureClosesBidi) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeTracksHandle> serverHandle;
  RequestID serverRequestID{0};
  EXPECT_CALL(*serverPublisher, subscribeTracks(_, _))
      .WillOnce(
          [&](auto subTracks, auto /*publishBlockedHandle*/)
              -> folly::coro::Task<Publisher::SubscribeTracksResult> {
            serverRequestID = subTracks.requestID;
            serverHandle =
                std::make_shared<MockSubscribeTracksHandle>(SubscribeTracksOk(
                    {.requestID = subTracks.requestID,
                     .requestSpecificParams = {}}));
            co_return serverHandle;
          });

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_FALSE(result.hasError());

  // The SUBSCRIBE_TRACKS bidi is the first client-initiated bidi (id 0).
  // Capture the responder's write half up front (see the namespace tests).
  auto serverBidi = serverWt_->writeHandles.at(0);

  // No updatable state → the responder rejects the update and tears down.
  folly::coro::Baton unsubscribed;
  EXPECT_CALL(*serverHandle, unsubscribeTracks()).WillOnce([&] {
    unsubscribed.post();
  });

  RequestUpdate update;
  update.existingRequestID = serverRequestID;
  update.requestID = RequestID(getRequestIDMultiplier());
  update.priority = kDefaultPriority + 1;
  update.forward = true;
  auto* cb =
      static_cast<MoQControlCodec::ControlCallback*>(serverSession_.get());
  cb->onRequestUpdate(std::move(update));

  co_await unsubscribed;

  // The responder FINs its write half to close the bidi.
  EXPECT_TRUE(serverBidi->fin_);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

} // namespace
