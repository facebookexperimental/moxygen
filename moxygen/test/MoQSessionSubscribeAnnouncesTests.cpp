/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === SUBSCRIBE ANNOUNCES tests ===

CO_TEST_P_X(MoQSessionTest, SubscribeAndUnsubscribeAnnounces) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeAnnouncesHandle> mockSubscribeAnnouncesHandle;
  EXPECT_CALL(*serverPublisher, subscribeAnnounces(_))
      .WillOnce(
          testing::Invoke(
              [&mockSubscribeAnnouncesHandle](auto subAnn)
                  -> folly::coro::Task<Publisher::SubscribeAnnouncesResult> {
                mockSubscribeAnnouncesHandle =
                    std::make_shared<MockSubscribeAnnouncesHandle>(
                        SubscribeAnnouncesOk({RequestID(0), {}}));
                co_return mockSubscribeAnnouncesHandle;
              }));

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeAnnouncesSuccess());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeAnnouncesSuccess());
  auto announceResult =
      co_await clientSession_->subscribeAnnounces(getSubscribeAnnounces());
  EXPECT_FALSE(announceResult.hasError());

  EXPECT_CALL(*clientSubscriberStatsCallback_, onUnsubscribeAnnounces());
  EXPECT_CALL(*serverPublisherStatsCallback_, onUnsubscribeAnnounces());
  EXPECT_CALL(*mockSubscribeAnnouncesHandle, unsubscribeAnnounces());
  announceResult.value()->unsubscribeAnnounces();
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeAnnouncesError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeAnnounces(_))
      .WillOnce(
          testing::Invoke(
              [](auto subAnn)
                  -> folly::coro::Task<Publisher::SubscribeAnnouncesResult> {
                SubscribeAnnouncesError subAnnError{
                    subAnn.requestID,
                    SubscribeAnnouncesErrorCode::NOT_SUPPORTED,
                    "not supported"};
                co_return folly::makeUnexpected(subAnnError);
              }));

  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeAnnouncesError(SubscribeAnnouncesErrorCode::NOT_SUPPORTED));
  EXPECT_CALL(
      *serverPublisherStatsCallback_,
      onSubscribeAnnouncesError(SubscribeAnnouncesErrorCode::NOT_SUPPORTED));
  auto subAnnResult =
      co_await clientSession_->subscribeAnnounces(getSubscribeAnnounces());
  EXPECT_TRUE(subAnnResult.hasError());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
