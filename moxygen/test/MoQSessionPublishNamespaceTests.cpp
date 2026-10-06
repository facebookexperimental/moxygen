/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
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
              [](auto pubNs, auto /* publishNamespaceCallback */)
                  -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
                co_return makePublishNamespaceOkResult(pubNs);
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
                  auto pubNs, auto /* publishNamespaceCallback */)
                  -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
                mockPublishNamespaceHandle =
                    std::make_shared<MockPublishNamespaceHandle>(
                        PublishNamespaceOk(
                            {.requestID = pubNs.requestID,
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
                  auto pubNs, auto publishNamespaceCallbackIn)
                  -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
                publishNamespaceCallback = publishNamespaceCallbackIn;
                mockPublishNamespaceHandle =
                    std::make_shared<MockPublishNamespaceHandle>(
                        PublishNamespaceOk(
                            {.requestID = pubNs.requestID,
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
// Draft 18+ subscriber-initiated withdrawal: subscriber tears down the
// PUBLISH_NAMESPACE bidi, the peer's read loop synthesizes
// onPublishNamespaceDone. Mirror of the publisher-initiated path above.
CO_TEST_P_X(Draft18Test, SubscriberCancelsPublishNamespace) {
  co_await setupMoQSession();

  std::shared_ptr<MockPublishNamespaceHandle> mockPublishNamespaceHandle;
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          [&mockPublishNamespaceHandle](
              auto pubNs, auto /* publishNamespaceCallback */)
              -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
            mockPublishNamespaceHandle =
                std::make_shared<MockPublishNamespaceHandle>(PublishNamespaceOk(
                    {.requestID = pubNs.requestID,
                     .requestSpecificParams = {}}));
            co_return Subscriber::PublishNamespaceResult(
                mockPublishNamespaceHandle);
          });

  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceSuccess());
  auto publishNamespaceResult =
      co_await clientSession_->publishNamespace(getPublishNamespace());
  EXPECT_FALSE(publishNamespaceResult.hasError());

  // STOP_SENDING the bidi read half → server fires its close callback,
  // synthesizing onPublishNamespaceDone.
  folly::coro::Baton doneBaton;
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceDone());
  EXPECT_CALL(*mockPublishNamespaceHandle, publishNamespaceDone())
      .WillOnce([&] { doneBaton.post(); });
  serverWt_->readHandles.at(0)->stopSending(
      folly::to_underlying(ResetStreamErrorCode::CANCELLED));
  co_await doneBaton;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A bare FIN says the publisher will send no more REQUEST_UPDATEs. Only a
// cancel withdraws the announcement.
CO_TEST_P_X(Draft18Test, PublishNamespaceSurvivesPeerFin) {
  co_await setupMoQSession();

  std::shared_ptr<MockPublishNamespaceHandle> mockPublishNamespaceHandle;
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          [&mockPublishNamespaceHandle](
              auto ann, auto /* publishNamespaceCallback */)
              -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
            mockPublishNamespaceHandle =
                std::make_shared<MockPublishNamespaceHandle>(PublishNamespaceOk(
                    {.requestID = ann.requestID, .requestSpecificParams = {}}));
            co_return Subscriber::PublishNamespaceResult(
                mockPublishNamespaceHandle);
          });

  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishNamespaceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceSuccess());
  auto publishNamespaceResult =
      co_await clientSession_->publishNamespace(getPublishNamespace());
  EXPECT_FALSE(publishNamespaceResult.hasError());

  bool doneCalled = false;
  folly::coro::Baton doneBaton;
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishNamespaceDone());
  EXPECT_CALL(*mockPublishNamespaceHandle, publishNamespaceDone())
      .WillOnce([&] {
        doneCalled = true;
        doneBaton.post();
      });

  // PUBLISH_NAMESPACE bidi is the client-initiated stream id 0.
  clientWt_->writeHandles.at(0)->writeStreamData(
      nullptr, /*fin=*/true, nullptr);
  for (int i = 0; i < 5; i++) {
    co_await folly::coro::co_reschedule_on_current_executor;
  }
  EXPECT_FALSE(doneCalled);

  // The FIN closed the request direction, so the withdrawal has to arrive as
  // STOP_SENDING on the response direction.
  clientWt_->readHandles.at(0)->stopSending(
      folly::to_underlying(ResetStreamErrorCode::CANCELLED));
  co_await doneBaton;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, PublishNamespaceError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto pubNs, auto /* publishNamespaceCallback */)
                  -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
                co_return folly::makeUnexpected(
                    PublishNamespaceError{
                        pubNs.requestID,
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

// Sender: peer FINs the PUBLISH_NAMESPACE bidi before REQUEST_OK/ERROR —
// publishNamespace must fail rather than strand.
CO_TEST_P_X(Draft18Test, PublishNamespaceFailsOnPeerFinWithoutReply) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawPubNs;
  folly::coro::Baton releaseHandler;
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          [&](auto pubNs, auto /*cb*/)
              -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
            serverSawPubNs.post();
            co_await releaseHandler;
            co_return makePublishNamespaceOkResult(pubNs);
          });

  std::optional<PublishNamespaceErrorCode> errorCode;
  folly::coro::Baton done;
  folly::coro::co_withExecutor(
      MoQExecutor_.get(),
      folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
        auto result =
            co_await clientSession_->publishNamespace(getPublishNamespace());
        if (result.hasError()) {
          errorCode = result.error().errorCode;
        }
        done.post();
      }))
      .start();

  co_await serverSawPubNs;
  // PUBLISH_NAMESPACE bidi is the client-initiated stream id 0.
  serverWt_->writeHandles.at(0)->writeStreamData(
      nullptr, /*fin=*/true, nullptr);

  co_await done;
  EXPECT_TRUE(errorCode.has_value());

  releaseHandler.post();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A publishNamespace whose caller is cancelled before the reply must release
// the caller's callback and withdraw the namespace at the peer.
CO_TEST_P_X(MoQSessionTest, PublishNamespaceCallerCancelledBeforeOk) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawPublishNamespace;
  folly::coro::Baton releaseHandler;
  bool serverSawDone = false;
  std::shared_ptr<MockPublishNamespaceHandle> serverHandle;
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          [&](auto pubNs, auto /* publishNamespaceCallback */)
              -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
            serverSawPublishNamespace.post();
            co_await releaseHandler;
            serverHandle =
                std::make_shared<MockPublishNamespaceHandle>(PublishNamespaceOk(
                    {.requestID = pubNs.requestID,
                     .requestSpecificParams = {}}));
            EXPECT_CALL(*serverHandle, publishNamespaceDone())
                .WillRepeatedly([&] { serverSawDone = true; });
            co_return serverHandle;
          });

  auto callback =
      std::make_shared<testing::StrictMock<MockPublishNamespaceCallback>>();
  std::weak_ptr<Subscriber::PublishNamespaceCallback> weakCallback = callback;
  folly::CancellationSource cancelSource;
  auto publishNamespaceFut =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->publishNamespace(
                  getPublishNamespace(), std::move(callback))))
          .start()
          .via(&eventBase_);
  co_await serverSawPublishNamespace;

  cancelSource.requestCancellation();
  EXPECT_THROW(
      co_await std::move(publishNamespaceFut), folly::OperationCancelled);
  releaseHandler.post();
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakCallback.expired());
  // Before draft 16, PUBLISH_NAMESPACE_DONE has no request ID to match.
  if (getDraftMajorVersion(getServerSelectedVersion()) >= 16) {
    EXPECT_TRUE(serverSawDone);
  }
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// The PUBLISH_NAMESPACE reply is read in the same event-loop turn in which the
// caller is cancelled, so it is processed before the caller resumes. The
// cancellation must still release the caller's callback and withdraw the
// namespace at the peer.
CO_TEST_P_X(MoQSessionTest, PublishNamespaceCancelledWhileOkQueued) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawPublishNamespace;
  folly::coro::Baton releaseHandler;
  bool serverSawDone = false;
  std::shared_ptr<MockPublishNamespaceHandle> serverHandle;
  EXPECT_CALL(*serverSubscriber, publishNamespace(_, _))
      .WillOnce(
          [&](auto pubNs, auto /* publishNamespaceCallback */)
              -> folly::coro::Task<Subscriber::PublishNamespaceResult> {
            serverSawPublishNamespace.post();
            co_await releaseHandler;
            serverHandle =
                std::make_shared<MockPublishNamespaceHandle>(PublishNamespaceOk(
                    {.requestID = pubNs.requestID,
                     .requestSpecificParams = {}}));
            EXPECT_CALL(*serverHandle, publishNamespaceDone())
                .WillRepeatedly([&] { serverSawDone = true; });
            co_return serverHandle;
          });

  auto callback =
      std::make_shared<testing::StrictMock<MockPublishNamespaceCallback>>();
  std::weak_ptr<Subscriber::PublishNamespaceCallback> weakCallback = callback;
  folly::CancellationSource cancelSource;
  auto publishNamespaceFut =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->publishNamespace(
                  getPublishNamespace(), std::move(callback))))
          .start()
          .via(&eventBase_);
  co_await serverSawPublishNamespace;

  // The reply arrives on the newest client-initiated bidi: the control stream
  // before draft 18, the request's own stream from draft 18.
  std::shared_ptr<proxygen::test::FakeStreamHandle> replyStream;
  for (const auto& [id, handle] : serverWt_->writeHandles) {
    if (id % 4 == 0) {
      replyStream = handle;
    }
  }
  EXPECT_NE(replyStream, nullptr);
  if (!replyStream) {
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
    co_return;
  }
  replyStream->setImmediateDelivery(false);
  releaseHandler.post();
  for (int i = 0; i < 10 && replyStream->inflightBuf_.empty(); ++i) {
    co_await folly::coro::co_reschedule_on_current_executor;
  }
  EXPECT_FALSE(replyStream->inflightBuf_.empty());
  replyStream->setImmediateDelivery(true);
  replyStream->deliverInflightData();
  cancelSource.requestCancellation();
  EXPECT_THROW(
      co_await std::move(publishNamespaceFut), folly::OperationCancelled);
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakCallback.expired());
  // Before draft 16, PUBLISH_NAMESPACE_DONE does not carry a request ID.
  if (getDraftMajorVersion(getServerSelectedVersion()) >= 16) {
    EXPECT_TRUE(serverSawDone);
  }
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
