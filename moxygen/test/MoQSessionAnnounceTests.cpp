/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === ANNOUNCE tests ===

CO_TEST_P_X(MoQSessionTest, Announce) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverSubscriber, announce(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto ann, auto /* announceCallback */)
                  -> folly::coro::Task<Subscriber::AnnounceResult> {
                co_return makeAnnounceOkResult(ann);
              }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onAnnounceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onAnnounceSuccess());
  EXPECT_CALL(*clientPublisherStatsCallback_, recordAnnounceLatency(_));
  auto announceResult = co_await clientSession_->announce(getAnnounce());
  EXPECT_FALSE(announceResult.hasError());
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, Unannounce) {
  co_await setupMoQSession();

  std::shared_ptr<MockAnnounceHandle> mockAnnounceHandle;
  EXPECT_CALL(*serverSubscriber, announce(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockAnnounceHandle](auto ann, auto /* announceCallback */)
                  -> folly::coro::Task<Subscriber::AnnounceResult> {
                mockAnnounceHandle = std::make_shared<MockAnnounceHandle>(
                    AnnounceOk({ann.requestID}));
                Subscriber::AnnounceResult announceResult(mockAnnounceHandle);
                co_return announceResult;
              }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onAnnounceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onAnnounceSuccess());
  auto announceResult = co_await clientSession_->announce(getAnnounce());
  EXPECT_FALSE(announceResult.hasError());
  auto announceHandle = announceResult.value();
  EXPECT_CALL(*clientPublisherStatsCallback_, onUnannounce());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onUnannounce());
  EXPECT_CALL(*mockAnnounceHandle, unannounce());
  announceHandle->unannounce();
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, AnnounceCancel) {
  co_await setupMoQSession();

  std::shared_ptr<MockAnnounceHandle> mockAnnounceHandle;
  std::shared_ptr<moxygen::Subscriber::AnnounceCallback> announceCallback;
  EXPECT_CALL(*serverSubscriber, announce(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockAnnounceHandle, &announceCallback](
                  auto ann, auto announceCallbackIn)
                  -> folly::coro::Task<Subscriber::AnnounceResult> {
                announceCallback = announceCallbackIn;
                mockAnnounceHandle = std::make_shared<MockAnnounceHandle>(
                    AnnounceOk({ann.requestID}));
                Subscriber::AnnounceResult announceResult(mockAnnounceHandle);
                co_return announceResult;
              }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onAnnounceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onAnnounceSuccess());
  auto mockAnnounceCallback = std::make_shared<MockAnnounceCallback>();
  auto announceResult =
      co_await clientSession_->announce(getAnnounce(), mockAnnounceCallback);
  EXPECT_FALSE(announceResult.hasError());
  EXPECT_CALL(*clientPublisherStatsCallback_, onAnnounceCancel());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onAnnounceCancel());

  folly::coro::Baton barricade;
  EXPECT_CALL(*mockAnnounceCallback, announceCancel(_, _))
      .WillOnce(
          testing::Invoke(
              [&barricade](moxygen::AnnounceErrorCode, std::string) {
                barricade.post();
                return;
              }));
  announceCallback->announceCancel(
      AnnounceErrorCode::UNINTERESTED, "Not interested!");

  co_await barricade;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, AnnounceError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverSubscriber, announce(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto ann, auto /* announceCallback */)
                  -> folly::coro::Task<Subscriber::AnnounceResult> {
                co_return folly::makeUnexpected(
                    AnnounceError{
                        ann.requestID,
                        AnnounceErrorCode::UNAUTHORIZED,
                        "Unauthorized"});
              }));

  EXPECT_CALL(
      *clientPublisherStatsCallback_,
      onAnnounceError(AnnounceErrorCode::UNAUTHORIZED));
  EXPECT_CALL(
      *serverSubscriberStatsCallback_,
      onAnnounceError(AnnounceErrorCode::UNAUTHORIZED));

  auto announceResult = co_await clientSession_->announce(getAnnounce());
  EXPECT_TRUE(announceResult.hasError());
  EXPECT_EQ(announceResult.error().errorCode, AnnounceErrorCode::UNAUTHORIZED);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
