/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

namespace moxygen::test {

// Test suite to demonstrate and verify the UAF fix in MoQSession
// when application callbacks delete the session during
// controlCodec_.onIngress()
class MoQSessionDeleteFromCallbackTest : public MoQSessionTest {};

// Test that demonstrates UAF when application deletes session during
// subscribeDone callback
CO_TEST_P_X(MoQSessionDeleteFromCallbackTest, DeleteFromSubscribeDoneCallback) {
  std::weak_ptr<MoQSession> weakSession;

  // Setup the session first
  co_await setupMoQSession();

  // Create a track consumer that we'll use to send subscribeDone
  std::shared_ptr<TrackConsumer> serverTrackConsumer;

  // Setup server publisher to accept subscription and store the TrackConsumer
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    pub->setTrackAlias(TrackAlias(sub.requestID.value));
    // Store the TrackConsumer so we can call subscribeDone on it later
    serverTrackConsumer = pub;
    co_return makeSubscribeOkResult(sub);
  });

  // Subscribe to a track using our suicidal consumer
  weakSession = clientSession_;
  // Create a mock TrackConsumer that deletes the session in subscribeDone
  auto consumer = std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  EXPECT_CALL(*consumer, setTrackAlias(testing::_))
      .WillOnce(testing::Return(folly::unit));
  // Subscribe to track - co_await ensures the mock lambda executes
  auto subResult = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), consumer);
  EXPECT_CALL(*consumer, subscribeDone(testing::_))
      .WillOnce([&](const SubscribeDone&) {
        // Delete the session during the callback
        clientSession_.reset();
        subResult->reset();
        return folly::unit;
      });

  // Verify the mock executed and set serverTrackConsumer
  EXPECT_NE(serverTrackConsumer, nullptr) << "Server mock should have executed";
  if (!serverTrackConsumer) {
    co_return; // Skip rest of test if setup failed
  }

  // Now server sends SUBSCRIBE_DONE - this will trigger the chain:
  // controlCodec_.onIngress() -> onSubscribeDone -> processSubscribeDone
  // -> callback_->subscribeDone() -> delete session -> return to onIngress ->
  // UAF!
  SubscribeDone subDone;
  subDone.requestID = 1;
  subDone.statusCode = SubscribeDoneStatusCode::SUBSCRIPTION_ENDED;
  subDone.reasonPhrase = "test";
  subDone.streamCount = 0; // No streams in flight

  // This should trigger the UAF if the guard is not in place
  // serverTrackConsumer is the client's TrackConsumer on the server side
  serverTrackConsumer->subscribeDone(subDone);

  // Process just this one event - with the fix, session stays alive during
  // callback Without the fix, we'd have UAF here
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;

  // If we get here without ASAN detecting UAF, the guard is working
  EXPECT_FALSE(clientSession_);
  EXPECT_TRUE(weakSession.expired()) << "Session should have been deleted";

  // Clean up: close server session
  if (serverSession_) {
    serverSession_->close(SessionCloseErrorCode::NO_ERROR);
  }
}

INSTANTIATE_TEST_SUITE_P(
    MoQSessionDeleteFromCallbackTests,
    MoQSessionDeleteFromCallbackTest,
    testing::Values(
        VersionParams{{kVersionDraftCurrent}, kVersionDraftCurrent}));

} // namespace moxygen::test
