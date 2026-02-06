/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === SUBSCRIBE PUBLISH_NAMESPACES tests ===

CO_TEST_P_X(MoQSessionTest, SubscribeAndUnsubscribeNamespace) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeNamespaceHandle> mockSubscribeNamespaceHandle;
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockSubscribeNamespaceHandle](auto subAnn, auto handler)
                  -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
                mockSubscribeNamespaceHandle =
                    std::make_shared<MockSubscribeNamespaceHandle>(
                        SubscribeNamespaceOk(
                            {.requestID = RequestID(0),
                             .requestSpecificParams = {}}));
                co_return mockSubscribeNamespaceHandle;
              }));

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeNamespaceSuccess());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeNamespaceSuccess());
  auto publishNamespaceResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_FALSE(publishNamespaceResult.hasError());

  EXPECT_CALL(*clientSubscriberStatsCallback_, onUnsubscribeNamespace());
  EXPECT_CALL(*serverPublisherStatsCallback_, onUnsubscribeNamespace());

  folly::coro::Baton barricade;
  EXPECT_CALL(*mockSubscribeNamespaceHandle, unsubscribeNamespace())
      .WillOnce(testing::Invoke([&barricade]() { barricade.post(); }));
  publishNamespaceResult.value()->unsubscribeNamespace();
  co_await barricade;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, SubscribeNamespaceError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto subAnn, auto handler)
                  -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
                SubscribeNamespaceError subAnnError{
                    subAnn.requestID,
                    SubscribeNamespaceErrorCode::NOT_SUPPORTED,
                    "not supported"};
                co_return folly::makeUnexpected(subAnnError);
              }));

  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeNamespaceError(SubscribeNamespaceErrorCode::NOT_SUPPORTED));
  EXPECT_CALL(
      *serverPublisherStatsCallback_,
      onSubscribeNamespaceError(SubscribeNamespaceErrorCode::NOT_SUPPORTED));
  auto subAnnResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_TRUE(subAnnResult.hasError());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
