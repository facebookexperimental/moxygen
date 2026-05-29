/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

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

  std::optional<PublishDoneStatusCode> publishDoneStatusCode;
  folly::coro::Baton publishDone;
  expectTrackEndedPublishDone(publishDoneStatusCode, publishDone);

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
  EXPECT_FALSE(publishDoneStatusCode.has_value());
  if (publishDoneStatusCode.has_value()) {
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
    co_return;
  }

  subscription.serverConsumer->publishDone(
      getTrackEndedPublishDone(subscription.subscribeRequest.requestID));
  co_await publishDone;
  EXPECT_EQ(publishDoneStatusCode, PublishDoneStatusCode::TRACK_ENDED);
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
      initialStart,     // Keep start the same
      initialEndGroup,  // Keep endGroup the same
      kDefaultPriority, // Change only priority
      initialForward,   // Keep forward the same
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
        EXPECT_EQ(actualUpdate.priority, kDefaultPriority)
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

// =============================================================================
// FETCH REQUEST_UPDATE tests
// =============================================================================

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
  EXPECT_CALL(*fetchCallback_, endOfFetch());
  fetchPubCaptured->endOfFetch();

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

} // namespace
