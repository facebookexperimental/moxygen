/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQVersions.h"
#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// Draft 18+ only: SUBSCRIBE_TRACKS is a new message split out of
// SUBSCRIBE_NAMESPACE in draft 18.
using V18PlusSubscribeTracksTest = MoQSessionTest;

CO_TEST_P_X(V18PlusSubscribeTracksTest, SubscribeAndUnsubscribeTracks) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeTracksHandle> mockSubscribeTracksHandle;
  EXPECT_CALL(*serverPublisher, subscribeTracks(_))
      .WillOnce(
          testing::Invoke(
              [&mockSubscribeTracksHandle](auto subTracks)
                  -> folly::coro::Task<Publisher::SubscribeTracksResult> {
                mockSubscribeTracksHandle =
                    std::make_shared<MockSubscribeTracksHandle>(
                        SubscribeTracksOk(
                            {.requestID = subTracks.requestID,
                             .requestSpecificParams = {}}));
                co_return mockSubscribeTracksHandle;
              }));

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_FALSE(result.hasError());

  folly::coro::Baton barricade;
  EXPECT_CALL(*mockSubscribeTracksHandle, unsubscribeTracks())
      .WillOnce(testing::Invoke([&barricade]() { barricade.post(); }));
  result.value()->unsubscribeTracks();
  co_await barricade;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(
    V18PlusSubscribeTracksTest,
    SubscribeTracksNotSupportedNoPublisher) {
  // Don't install a publish handler on the server side — the relay should
  // reply with NOT_SUPPORTED.
  co_await setupMoQSession();
  serverSession_->setPublishHandler(nullptr);

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(
        result.error().errorCode, SubscribeTracksErrorCode::NOT_SUPPORTED);
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(V18PlusSubscribeTracksTest, SubscribeTracksAppReturnsError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeTracks(_))
      .WillOnce(
          testing::Invoke(
              [](auto subTracks)
                  -> folly::coro::Task<Publisher::SubscribeTracksResult> {
                co_return folly::makeUnexpected(
                    SubscribeTracksError{
                        subTracks.requestID,
                        SubscribeTracksErrorCode::INTERNAL_ERROR,
                        "boom"});
              }));

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(
        result.error().errorCode, SubscribeTracksErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(result.error().reasonPhrase, "boom");
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(V18PlusSubscribeTracksTest, SubscribeTracksAppThrows) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeTracks(_))
      .WillOnce(
          testing::Invoke(
              [](auto /*subTracks*/)
                  -> folly::coro::Task<Publisher::SubscribeTracksResult> {
                throw std::runtime_error("kaboom");
                co_return folly::makeUnexpected(SubscribeTracksError{});
              }));

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_TRUE(result.hasError());
  if (result.hasError()) {
    EXPECT_EQ(
        result.error().errorCode, SubscribeTracksErrorCode::INTERNAL_ERROR);
  }

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(V18PlusSubscribeTracksTest, UnsubscribeTracksAfterSessionClosed) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeTracks(_))
      .WillOnce(
          testing::Invoke(
              [](auto subTracks)
                  -> folly::coro::Task<Publisher::SubscribeTracksResult> {
                co_return std::make_shared<MockSubscribeTracksHandle>(
                    SubscribeTracksOk(
                        {.requestID = subTracks.requestID,
                         .requestSpecificParams = {}}));
              }));

  auto result = co_await clientSession_->subscribeTracks(getSubscribeTracks());
  EXPECT_FALSE(result.hasError());

  // Closing the session first then unsubscribing should be a no-op (not a
  // crash).
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  EXPECT_NO_THROW(result.value()->unsubscribeTracks());
}

INSTANTIATE_TEST_SUITE_P(
    V18PlusSubscribeTracksTest,
    V18PlusSubscribeTracksTest,
    testing::Values(VersionParams{{kVersionDraft18}, kVersionDraft18}));
