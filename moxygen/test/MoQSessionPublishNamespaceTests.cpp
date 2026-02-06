/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// === PUBLISH_NAMESPACE tests ===

CO_TEST_P_X(MoQSessionTest, PublishNamespace) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto ann, auto /* publishNamespaceCallback */)
                  -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
                co_return makePublishNamespaceOkResult(ann);
              }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceSuccess());
  EXPECT_CALL(*clientPublisherStatsCallback_, recordPublishNamespaceLatency(_));
  auto publishNamespaceResult =
      co_await clientSession_->publishNamespace(getPublishNamespace());
  EXPECT_FALSE(publishNamespaceResult.hasError());
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishNamespaceDone) {
  co_await setupMoQSession();

  std::shared_ptr<MockPublishNamespaceHandle> mockPublishNamespaceHandle;
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockPublishNamespaceHandle](
                  auto ann, auto /* publishNamespaceCallback */)
                  -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
                mockPublishNamespaceHandle =
                    std::make_shared<MockPublishNamespaceHandle>(
                        PublishNamespaceOk(
                            {.requestID = ann.requestID,
                             .requestSpecificParams = {}}));
                Subscriber::PublishNamespaceResult publishNamespaceResult(
                    mockPublishNamespaceHandle);
                co_return publishNamespaceResult;
              }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceSuccess());
  auto publishNamespaceResult =
      co_await clientSession_->publishNamespace(getPublishNamespace());
  EXPECT_FALSE(publishNamespaceResult.hasError());
  auto publishNamespaceHandle = publishNamespaceResult.value();
  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceDone());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceDone());

  folly::coro::Baton barricade;
  EXPECT_CALL(*mockPublishNamespaceHandle, publishNamespaceDone())
      .WillOnce(testing::Invoke([&barricade]() { barricade.post(); }));
  publishNamespaceHandle->publishNamespaceDone();
  co_await barricade;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishNamespaceCancel) {
  co_await setupMoQSession();

  std::shared_ptr<MockPublishNamespaceHandle> mockPublishNamespaceHandle;
  std::shared_ptr<moxygen::Subscriber::PublishNamespaceCallback>
      publishNamespaceCallback;
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockPublishNamespaceHandle, &publishNamespaceCallback](
                  auto ann, auto publishNamespaceCallbackIn)
                  -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
                publishNamespaceCallback = publishNamespaceCallbackIn;
                mockPublishNamespaceHandle =
                    std::make_shared<MockPublishNamespaceHandle>(
                        PublishNamespaceOk(
                            {.requestID = ann.requestID,
                             .requestSpecificParams = {}}));
                Subscriber::PublishNamespaceResult publishNamespaceResult(
                    mockPublishNamespaceHandle);
                co_return publishNamespaceResult;
              }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceSuccess());
  auto mockPublishNamespaceCallback =
      std::make_shared<MockPublishNamespaceCallback>();
  auto publishNamespaceResult = co_await clientSession_->publishNamespace(
      getPublishNamespace(), mockPublishNamespaceCallback);
  EXPECT_FALSE(publishNamespaceResult.hasError());
  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceCancel());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceCancel());

  folly::coro::Baton barricade;
  EXPECT_CALL(*mockPublishNamespaceCallback, publishNamespaceCancel(_, _))
      .WillOnce(
          testing::Invoke(
              [&barricade](moxygen::PublishNamespaceErrorCode, std::string) {
                barricade.post();
                return;
              }));
  publishNamespaceCallback->publishNamespaceCancel(
      PublishNamespaceErrorCode::UNINTERESTED, "Not interested!");

  co_await barricade;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, PublishNamespaceError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto ann, auto /* publishNamespaceCallback */)
                  -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
                co_return folly::makeUnexpected(
                    PublishNamespaceError{
                        ann.requestID,
                        PublishNamespaceErrorCode::UNAUTHORIZED,
                        "Unauthorized"});
              }));

  EXPECT_CALL(
      *clientPublisherStatsCallback_,
      onPublishNamespaceError(PublishNamespaceErrorCode::UNAUTHORIZED));
  EXPECT_CALL(
      *serverSubscriberStatsCallback_,
      onPublishNamespaceError(PublishNamespaceErrorCode::UNAUTHORIZED));

  auto publishNamespaceResult =
      co_await clientSession_->publishNamespace(getPublishNamespace());
  EXPECT_TRUE(publishNamespaceResult.hasError());
  EXPECT_EQ(
      publishNamespaceResult.error().errorCode,
      PublishNamespaceErrorCode::UNAUTHORIZED);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
