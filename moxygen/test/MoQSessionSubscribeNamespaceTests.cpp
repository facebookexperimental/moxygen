/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
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
              [&mockSubscribeNamespaceHandle](auto subNs, auto handler)
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
// Unsubscribing must release the caller's NamespacePublishHandle without
// waiting for the peer to close its half of the stream.
CO_TEST_P_X(MoQSessionTest, UnsubscribeNamespaceReleasesPublishHandle) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawUnsubscribe;
  std::shared_ptr<MockSubscribeNamespaceHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          [&](auto subNs, auto /* handler */)
              -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
            serverHandle = std::make_shared<MockSubscribeNamespaceHandle>(
                SubscribeNamespaceOk(
                    {.requestID = subNs.requestID,
                     .requestSpecificParams = {}}));
            EXPECT_CALL(*serverHandle, unsubscribeNamespace()).WillOnce([&] {
              serverSawUnsubscribe.post();
            });
            co_return serverHandle;
          });

  auto clientHandle =
      std::make_shared<testing::StrictMock<MockNamespacePublishHandle>>();
  std::weak_ptr<Publisher::NamespacePublishHandle> weakClientHandle =
      clientHandle;
  auto result = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), std::move(clientHandle));
  EXPECT_FALSE(result.hasError());
  if (result.hasValue()) {
    result.value()->unsubscribeNamespace();
  }
  co_await serverSawUnsubscribe;
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakClientHandle.expired());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(MoQSessionTest, UnsubscribeNamespaceAfterSessionClosed) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto /*subNs*/, auto /*handler*/)
                  -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
                co_return std::make_shared<MockSubscribeNamespaceHandle>(
                    SubscribeNamespaceOk(
                        {.requestID = RequestID(0),
                         .requestSpecificParams = {}}));
              }));

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeNamespaceSuccess());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeNamespaceSuccess());
  auto subscribeNamespaceResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_FALSE(subscribeNamespaceResult.hasError());

  // Close the session first, then unsubscribe - should not crash
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  EXPECT_NO_THROW(subscribeNamespaceResult.value()->unsubscribeNamespace());
}

using V16PlusSubscribeNamespaceTest = MoQSessionTest;

// Verifies that after NAMESPACE + NAMESPACE_DONE, a second NAMESPACE
// can still be sent on the same stream.
folly::coro::Task<void> verifyNamespaceDoneDoesNotCloseStream(
    std::shared_ptr<Publisher::NamespacePublishHandle>& serverPublishHandle,
    std::shared_ptr<MockNamespacePublishHandle>& clientNamespacePublishHandle) {
  TrackNamespace ns1{{"bar"}};
  TrackNamespace ns2{{"baz"}};

  folly::coro::Baton namespaceBaton;
  folly::coro::Baton namespaceDoneBaton;
  folly::coro::Baton namespace2Baton;

  testing::InSequence seq;
  EXPECT_CALL(*clientNamespacePublishHandle, namespaceMsg(ns1))
      .WillOnce(testing::Invoke([&namespaceBaton](const TrackNamespace&) {
        namespaceBaton.post();
      }));
  EXPECT_CALL(*clientNamespacePublishHandle, namespaceDoneMsg(ns1))
      .WillOnce(testing::Invoke([&namespaceDoneBaton](const TrackNamespace&) {
        namespaceDoneBaton.post();
      }));
  EXPECT_CALL(*clientNamespacePublishHandle, namespaceMsg(ns2))
      .WillOnce(testing::Invoke([&namespace2Baton](const TrackNamespace&) {
        namespace2Baton.post();
      }));

  serverPublishHandle->namespaceMsg(ns1);
  co_await namespaceBaton;

  serverPublishHandle->namespaceDoneMsg(ns1);
  co_await namespaceDoneBaton;

  serverPublishHandle->namespaceMsg(ns2);
  co_await namespace2Baton;
}

CO_TEST_P_X(V16PlusSubscribeNamespaceTest, NamespaceDoneDoesNotCloseStream) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeNamespaceHandle> mockSubscribeNamespaceHandle;
  std::shared_ptr<Publisher::NamespacePublishHandle> serverPublishHandle;
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockSubscribeNamespaceHandle, &serverPublishHandle](
                  auto subNs, auto handler)
                  -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
                serverPublishHandle = handler;
                mockSubscribeNamespaceHandle =
                    std::make_shared<MockSubscribeNamespaceHandle>(
                        SubscribeNamespaceOk(
                            {.requestID = RequestID(0),
                             .requestSpecificParams = {}}));
                co_return mockSubscribeNamespaceHandle;
              }));

  auto clientNamespacePublishHandle =
      std::make_shared<MockNamespacePublishHandle>();

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeNamespaceSuccess());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeNamespaceSuccess());
  auto publishNamespaceResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), clientNamespacePublishHandle);
  EXPECT_FALSE(publishNamespaceResult.hasError());

  // Server sends NAMESPACE, then NAMESPACE_DONE, then another NAMESPACE.
  // Before the fix, NAMESPACE_DONE sent fin on the stream, so the second
  // NAMESPACE would not be received.
  co_await verifyNamespaceDoneDoesNotCloseStream(
      serverPublishHandle, clientNamespacePublishHandle);

  EXPECT_CALL(*clientSubscriberStatsCallback_, onUnsubscribeNamespace());
  EXPECT_CALL(*serverPublisherStatsCallback_, onUnsubscribeNamespace());

  folly::coro::Baton unsubBaton;
  EXPECT_CALL(*mockSubscribeNamespaceHandle, unsubscribeNamespace())
      .WillOnce(testing::Invoke([&unsubBaton]() { unsubBaton.post(); }));
  publishNamespaceResult.value()->unsubscribeNamespace();
  co_await unsubBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(
    V16PlusSubscribeNamespaceTest,
    NamespaceDoneBeforeOkDoesNotCloseStream) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeNamespaceHandle> mockSubscribeNamespaceHandle;
  std::shared_ptr<Publisher::NamespacePublishHandle> serverPublishHandle;
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockSubscribeNamespaceHandle, &serverPublishHandle](
                  auto subNs, auto handler)
                  -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
                serverPublishHandle = handler;
                // Send NAMESPACE + NAMESPACE_DONE before returning OK.
                // These get buffered and flushed when OK is sent.
                TrackNamespace ns1{{"bar"}};
                handler->namespaceMsg(ns1);
                handler->namespaceDoneMsg(ns1);
                mockSubscribeNamespaceHandle =
                    std::make_shared<MockSubscribeNamespaceHandle>(
                        SubscribeNamespaceOk(
                            {.requestID = RequestID(0),
                             .requestSpecificParams = {}}));
                co_return mockSubscribeNamespaceHandle;
              }));

  auto clientNamespacePublishHandle =
      std::make_shared<MockNamespacePublishHandle>();

  // The first NAMESPACE + NAMESPACE_DONE were sent before OK, so set up
  // expectations before the subscribeNamespace call triggers OK + flush.
  TrackNamespace ns1{{"bar"}};
  TrackNamespace ns2{{"baz"}};

  folly::coro::Baton namespaceBaton;
  folly::coro::Baton namespaceDoneBaton;

  {
    testing::InSequence seq;
    EXPECT_CALL(*clientNamespacePublishHandle, namespaceMsg(ns1))
        .WillOnce(testing::Invoke([&namespaceBaton](const TrackNamespace&) {
          namespaceBaton.post();
        }));
    EXPECT_CALL(*clientNamespacePublishHandle, namespaceDoneMsg(ns1))
        .WillOnce(testing::Invoke([&namespaceDoneBaton](const TrackNamespace&) {
          namespaceDoneBaton.post();
        }));
  }

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeNamespaceSuccess());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeNamespaceSuccess());
  auto publishNamespaceResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), clientNamespacePublishHandle);
  EXPECT_FALSE(publishNamespaceResult.hasError());

  co_await namespaceBaton;
  co_await namespaceDoneBaton;

  // Send another NAMESPACE after the buffered NAMESPACE_DONE was flushed.
  // Before the fix, pendingFin_ was true, so flushPendingMessages sent fin
  // and this second message would not be received.
  folly::coro::Baton namespace2Baton;
  EXPECT_CALL(*clientNamespacePublishHandle, namespaceMsg(ns2))
      .WillOnce(testing::Invoke([&namespace2Baton](const TrackNamespace&) {
        namespace2Baton.post();
      }));

  serverPublishHandle->namespaceMsg(ns2);
  co_await namespace2Baton;

  EXPECT_CALL(*clientSubscriberStatsCallback_, onUnsubscribeNamespace());
  EXPECT_CALL(*serverPublisherStatsCallback_, onUnsubscribeNamespace());

  folly::coro::Baton unsubBaton;
  EXPECT_CALL(*mockSubscribeNamespaceHandle, unsubscribeNamespace())
      .WillOnce(testing::Invoke([&unsubBaton]() { unsubBaton.post(); }));
  publishNamespaceResult.value()->unsubscribeNamespace();
  co_await unsubBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

INSTANTIATE_TEST_SUITE_P(
    V16PlusSubscribeNamespaceTest,
    V16PlusSubscribeNamespaceTest,
    testing::Values(
        VersionParams{{kVersionDraft16}, kVersionDraft16},
        VersionParams{{kVersionDraft18}, kVersionDraft18}));

using V16SubscribeNamespaceTest = MoQSessionTest;

// Drafts 16-17 accept a FIN as the unsubscribe signal. Draft 18 does not --
// see Draft18Test.PublishNamespaceSurvivesPeerFin for the other half.
CO_TEST_P_X(V16SubscribeNamespaceTest, PeerFinUnsubscribesNamespace) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeNamespaceHandle> mockSubscribeNamespaceHandle;
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [&mockSubscribeNamespaceHandle](auto /*subAnn*/, auto /*handler*/)
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
  auto subscribeNamespaceResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_FALSE(subscribeNamespaceResult.hasError());

  folly::coro::Baton unsubBaton;
  EXPECT_CALL(*serverPublisherStatsCallback_, onUnsubscribeNamespace());
  EXPECT_CALL(*mockSubscribeNamespaceHandle, unsubscribeNamespace())
      .WillOnce(testing::Invoke([&unsubBaton]() { unsubBaton.post(); }));

  // Control stream is the client's first bidi, so SUBSCRIBE_NAMESPACE is id 4.
  clientWt_->writeHandles.at(4)->writeStreamData(
      nullptr, /*fin=*/true, nullptr);
  co_await unsubBaton;

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

INSTANTIATE_TEST_SUITE_P(
    V16SubscribeNamespaceTest,
    V16SubscribeNamespaceTest,
    testing::Values(VersionParams{{kVersionDraft16}, kVersionDraft16}));

CO_TEST_P_X(MoQSessionTest, SubscribeNamespaceError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          testing::Invoke(
              [](auto subNs, auto /*handler*/)
                  -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
                SubscribeNamespaceError subNsError{
                    subNs.requestID,
                    SubscribeNamespaceErrorCode::NOT_SUPPORTED,
                    "not supported"};
                co_return folly::makeUnexpected(subNsError);
              }));

  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeNamespaceError(SubscribeNamespaceErrorCode::NOT_SUPPORTED));
  EXPECT_CALL(
      *serverPublisherStatsCallback_,
      onSubscribeNamespaceError(SubscribeNamespaceErrorCode::NOT_SUPPORTED));
  auto subNsResult = co_await clientSession_->subscribeNamespace(
      getSubscribeNamespace(), nullptr);
  EXPECT_TRUE(subNsResult.hasError());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// A subscribeNamespace whose caller is cancelled before the reply must release
// the caller's handle and end the subscription at the peer.
CO_TEST_P_X(MoQSessionTest, SubscribeNamespaceCallerCancelledBeforeOk) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawSubscribeNamespace;
  folly::coro::Baton releaseHandler;
  bool serverSawUnsubscribe = false;
  std::shared_ptr<MockSubscribeNamespaceHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          [&](auto subNs, auto /* handler */)
              -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
            serverSawSubscribeNamespace.post();
            co_await releaseHandler;
            serverHandle = std::make_shared<MockSubscribeNamespaceHandle>(
                SubscribeNamespaceOk(
                    {.requestID = subNs.requestID,
                     .requestSpecificParams = {}}));
            EXPECT_CALL(*serverHandle, unsubscribeNamespace())
                .WillRepeatedly([&] { serverSawUnsubscribe = true; });
            co_return serverHandle;
          });

  auto clientHandle =
      std::make_shared<testing::StrictMock<MockNamespacePublishHandle>>();
  std::weak_ptr<Publisher::NamespacePublishHandle> weakClientHandle =
      clientHandle;
  folly::CancellationSource cancelSource;
  auto subscribeNamespaceFut =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->subscribeNamespace(
                  getSubscribeNamespace(), std::move(clientHandle))))
          .start()
          .via(&eventBase_);
  co_await serverSawSubscribeNamespace;

  cancelSource.requestCancellation();
  EXPECT_THROW(
      co_await std::move(subscribeNamespaceFut), folly::OperationCancelled);
  releaseHandler.post();
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakClientHandle.expired());
  // Before draft 16 the request has no stream of its own to cancel.
  if (getDraftMajorVersion(getServerSelectedVersion()) >= 16) {
    EXPECT_TRUE(serverSawUnsubscribe);
  }
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// The SUBSCRIBE_NAMESPACE reply is read in the same event-loop turn in which
// the caller is cancelled, so it is processed before the caller resumes. The
// cancellation must still release the caller's handle and end the
// subscription at the peer.
CO_TEST_P_X(MoQSessionTest, SubscribeNamespaceCancelledWhileOkQueued) {
  co_await setupMoQSession();

  folly::coro::Baton serverSawSubscribeNamespace;
  folly::coro::Baton releaseHandler;
  bool serverSawUnsubscribe = false;
  std::shared_ptr<MockSubscribeNamespaceHandle> serverHandle;
  EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _))
      .WillOnce(
          [&](auto subNs, auto /* handler */)
              -> folly::coro::Task<Publisher::SubscribeNamespaceResult> {
            serverSawSubscribeNamespace.post();
            co_await releaseHandler;
            serverHandle = std::make_shared<MockSubscribeNamespaceHandle>(
                SubscribeNamespaceOk(
                    {.requestID = subNs.requestID,
                     .requestSpecificParams = {}}));
            EXPECT_CALL(*serverHandle, unsubscribeNamespace())
                .WillRepeatedly([&] { serverSawUnsubscribe = true; });
            co_return serverHandle;
          });

  auto clientHandle =
      std::make_shared<testing::StrictMock<MockNamespacePublishHandle>>();
  std::weak_ptr<Publisher::NamespacePublishHandle> weakClientHandle =
      clientHandle;
  folly::CancellationSource cancelSource;
  auto subscribeNamespaceFut =
      folly::coro::co_withExecutor(
          &eventBase_,
          folly::coro::co_withCancellation(
              cancelSource.getToken(),
              clientSession_->subscribeNamespace(
                  getSubscribeNamespace(), std::move(clientHandle))))
          .start()
          .via(&eventBase_);
  co_await serverSawSubscribeNamespace;

  // The reply arrives on the newest client-initiated bidi: the request's own
  // stream, or the control stream before draft 16.
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
      co_await std::move(subscribeNamespaceFut), folly::OperationCancelled);
  co_await folly::coro::sleep(std::chrono::milliseconds(200));

  EXPECT_TRUE(weakClientHandle.expired());
  // Before draft 16 the request does not have its own stream to cancel.
  if (getDraftMajorVersion(getServerSelectedVersion()) >= 16) {
    EXPECT_TRUE(serverSawUnsubscribe);
  }
  if (serverHandle) {
    testing::Mock::VerifyAndClearExpectations(serverHandle.get());
  }
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
