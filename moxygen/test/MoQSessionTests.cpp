/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

#include <quic/api/test/MockQuicSocket.h>

using namespace moxygen;
using namespace moxygen::test;
using testing::_;

// Missing Test Cases
// ===
// getTrack by alias (subscribe with stream)
// getTrack with invalid alias and subscribe ID
// receive non-normal object
// onObjectPayload maps to non-existent object in TrackHandle
// onSubscribeOk/Error/Done with unknown ID
// onMaxRequestID with ID == 0 {no setup param}
// onFetchCancel with no publish data
// onConnectionError
// control message write failures
// order on invalid pub track
// publishStreamPerObject
// publish with payloadOffset > 0
// createUniStream fails
// publish invalid group/object per forward pref
// publish without length
// publish object larger than length
// datagrams
// write stream data fails for object
// publish with stream EOM

// === SETUP and GENERAL tests ===

using MoQVersionNegotiationTest = MoQSessionTest;

INSTANTIATE_TEST_SUITE_P(
    MoQVersionNegotiationTest,
    MoQVersionNegotiationTest,
    testing::ValuesIn(getSupportedVersionParams()));
TEST_P(MoQVersionNegotiationTest, Setup) {
  folly::coro::blockingWait(setupMoQSession(), getExecutor());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
using CurrentVersionOnly = MoQSessionTest;

CO_TEST_P_X(CurrentVersionOnly, SetupTimeout) {
  MoQSettings moqSettings;
  moqSettings.setupTimeout = std::chrono::milliseconds(500);
  clientSession_->setMoqSettings(moqSettings);
  moxygen::Setup setup;
  auto serverSetup = co_await co_awaitTry(clientSession_->setup(setup));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_P_X(CurrentVersionOnly, ServerSetupFail) {
  failServerSetup_ = true;
  clientSession_->start();
  auto serverSetup = co_await co_awaitTry(
      clientSession_->setup(getClientSetup(initialMaxRequestID_)));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

INSTANTIATE_TEST_SUITE_P(
    CurrentVersionOnly,
    CurrentVersionOnly,
    testing::Values(
        VersionParams{{kVersionDraftCurrent}, kVersionDraftCurrent}));
TEST(MoQSessionTest, SetVersionFromAlpnLegacy) {
  folly::EventBase eventBase;
  auto MoQExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  auto session = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()),
      MoQExecutor);

  session->validateAndSetVersionFromAlpn("moq-00");
  EXPECT_FALSE(session->getNegotiatedVersion().has_value());
}
TEST(MoQSessionTest, SetVersionFromAlpnDraft15) {
  folly::EventBase eventBase;
  auto MoQExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  auto session = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()),
      MoQExecutor);

  session->validateAndSetVersionFromAlpn("moqt-15");
  EXPECT_EQ(session->getNegotiatedVersion(), 0xff00000f);
}
TEST(MoQSessionTest, SetVersionFromAlpnDraft16) {
  folly::EventBase eventBase;
  auto MoQExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  auto session = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()),
      MoQExecutor);

  session->validateAndSetVersionFromAlpn("moqt-16");
  EXPECT_EQ(session->getNegotiatedVersion(), 0xff000010);
}
TEST(MoQSessionTest, SetVersionFromAlpnInvalidAlpn) {
  folly::EventBase eventBase;
  auto MoQExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  auto session = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()),
      MoQExecutor);

  session->validateAndSetVersionFromAlpn("invalid-alpn");
  EXPECT_FALSE(session->getNegotiatedVersion().has_value());
}
TEST(MoQSessionTest, WithoutAlpnUsesDraft14) {
  // Test that when no ALPN negotiation happens, the session uses draft-14

  folly::EventBase eventBase;
  auto moqExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();

  class TestServerSetupCallback : public MoQSession::ServerSetupCallback {
   public:
    folly::Try<moxygen::Setup> onClientSetup(
        moxygen::Setup /*clientSetup*/,
        const std::shared_ptr<MoQSession>& /*session*/) override {
      return folly::Try<moxygen::Setup>(moxygen::Setup{});
    }

    folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
        const moxygen::Setup& /*clientSetup*/,
        uint64_t /*negotiatedVersion*/,
        std::shared_ptr<MoQSession> /*session*/) override {
      return folly::unit;
    }
  };

  TestServerSetupCallback serverSetupCallback;
  auto serverSession = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(serverWt.get()),
      serverSetupCallback,
      moqExecutor);

  auto clientSession = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()),
      moqExecutor);

  // Setup peer handlers before starting
  clientWt->setPeerHandler(serverSession.get());
  serverWt->setPeerHandler(clientSession.get());

  // Start both sessions
  serverSession->start();
  clientSession->start();

  // Client sends setup without ALPN negotiation.
  // MoQSession::onClientSetup will call
  // initializeNegotiatedVersion(kVersionDraft14) since no ALPN version was set
  // on the session.
  moxygen::Setup clientSetup;

  folly::coro::co_withExecutor(
      moqExecutor.get(), clientSession->setup(clientSetup))
      .start();
  eventBase.loop();

  // Without ALPN, version should be draft-14
  EXPECT_TRUE(serverSession->getNegotiatedVersion().has_value());
  EXPECT_EQ(getDraftMajorVersion(*serverSession->getNegotiatedVersion()), 14);

  // Cleanup
  if (!clientWt->isSessionClosed()) {
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  }
  if (!serverWt->isSessionClosed()) {
    serverSession->close(SessionCloseErrorCode::NO_ERROR);
  }
}
// === AUTHORITY / PATH tests ===

using MoQAuthorityPathTest = MoQSessionTest;
INSTANTIATE_TEST_SUITE_P(
    MoQAuthorityPathTest,
    MoQAuthorityPathTest,
    testing::ValuesIn(getSupportedVersionParams()));

// After a normal setup the server session's path should be populated from the
// PATH parameter in CLIENT_SETUP, and authority should remain empty (no
// AUTHORITY param is sent in the test ClientSetup).
CO_TEST_P_X(MoQAuthorityPathTest, ServerSessionPathFromClientSetup) {
  co_await setupMoQSession();
  EXPECT_EQ(serverSession_->getPath(), "/foo");
  EXPECT_EQ(serverSession_->getAuthority(), "");
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// If authority/path are already set on the server session before CLIENT_SETUP
// arrives (e.g. populated from HTTP Host/request-path in the WT case), a
// CLIENT_SETUP that also carries PATH is a protocol violation and must close
// the session.
CO_TEST_P_X(MoQAuthorityPathTest, PathInClientSetupConflictsWithPreSetPath) {
  serverSession_->setPath("/pre-set-path");

  clientSession_->setPublishHandler(clientPublisher);
  clientSession_->setSubscribeHandler(clientSubscriber);
  clientSession_->start();
  serverSession_->setPublishHandler(serverPublisher);
  serverSession_->setSubscribeHandler(serverSubscriber);
  serverSession_->start();

  auto result = co_await folly::coro::co_awaitTry(
      clientSession_->setup(getClientSetup(initialMaxRequestID_)));
  EXPECT_TRUE(result.hasException() || serverWt_->isSessionClosed());
}

class Draft18GoawayRequestRejectionTest : public MoQSessionTest {
 protected:
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
  openServerSubscriptionToKeepDrainOpen(
      std::shared_ptr<TrackConsumer> keepAliveConsumer) {
    expectSubscribe(
        [](SubscribeRequest sub, auto /* pub */) -> TaskSubscribeResult {
          co_return makeSubscribeOkResult(sub);
        },
        MoQControlCodec::Direction::CLIENT);
    auto keepAlive = co_await serverSession_->subscribe(
        getSubscribe(kTestTrackName), keepAliveConsumer);
    EXPECT_FALSE(keepAlive.hasError());
    if (keepAlive.hasError()) {
      co_return nullptr;
    }
    co_return keepAlive.value();
  }

  void expectNoServerPeerRequests() {
    EXPECT_CALL(*serverPublisher, subscribe(_, _)).Times(0);
    EXPECT_CALL(*serverPublisher, fetch(_, _)).Times(0);
    EXPECT_CALL(*serverPublisher, trackStatus(_)).Times(0);
    EXPECT_CALL(*serverPublisher, subscribeNamespace(_, _)).Times(0);
    EXPECT_CALL(*serverSubscriber, publish(_, _)).Times(0);
    EXPECT_CALL(*serverSubscriber, publishNamespace(_, _)).Times(0);
  }

  template <typename StartRequest>
  folly::coro::Task<void> expectInboundRequestRejectedAfterGoaway(
      const char* /* requestName */,
      StartRequest startRequest) {
    co_await setupMoQSession();

    auto keepAliveConsumer = makeKeepAliveConsumer();
    auto keepAlive =
        co_await openServerSubscriptionToKeepDrainOpen(keepAliveConsumer);
    if (!keepAlive) {
      co_return;
    }

    // Server's first uni stream id is 3 (QUIC-style: server uses odd uni
    // stream id type = 3). The session's outgoing uni control stream is the
    // first uni stream the server creates.
    auto serverControl = serverWt_->writeHandles[3];
    CHECK(serverControl != nullptr);
    serverControl->setImmediateDelivery(false);
    serverSession_->goaway(Goaway{});
    co_await rescheduleN(2);

    std::optional<RequestErrorCode> errorCode;
    folly::coro::Baton done;
    folly::coro::co_withExecutor(
        MoQExecutor_.get(),
        folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
          errorCode = co_await startRequest();
          done.post();
        }))
        .start();

    co_await rescheduleN(4);
    serverControl->deliverInflightData();
    co_await rescheduleN(4);
    serverControl->deliverInflightData();
    co_await done;

    EXPECT_TRUE(errorCode.has_value());
    if (!errorCode.has_value()) {
      co_return;
    }
    EXPECT_EQ(*errorCode, RequestErrorCode::GOING_AWAY);
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  }

  template <typename StartRequest>
  folly::coro::Task<void> expectLocalRequestRejectedAfterGoaway(
      const char* requestName,
      StartRequest startRequest) {
    co_await setupMoQSession();

    auto keepAliveConsumer = makeKeepAliveConsumer();
    auto keepAlive =
        co_await openServerSubscriptionToKeepDrainOpen(keepAliveConsumer);
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

    expectNoServerPeerRequests();
    auto errorCode = co_await startRequest();
    co_await rescheduleN(4);

    EXPECT_EQ(errorCode, RequestErrorCode::GOING_AWAY);
    clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  }
};

INSTANTIATE_TEST_SUITE_P(
    Draft18GoawayRequestRejectionTest,
    Draft18GoawayRequestRejectionTest,
    testing::Values(VersionParams{{kVersionDraft18}, kVersionDraft18}));

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    SentGoawayRejectsInboundSubscribe) {
  co_await expectInboundRequestRejectedAfterGoaway(
      "SUBSCRIBE", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result = co_await clientSession_->subscribe(
            getSubscribe(kTestTrackName), subscribeCallback_);
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(Draft18GoawayRequestRejectionTest, SentGoawayRejectsInboundFetch) {
  co_await expectInboundRequestRejectedAfterGoaway(
      "FETCH", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result = co_await clientSession_->fetch(
            getFetch({0, 0}, {0, 1}), fetchCallback_);
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    SentGoawayRejectsInboundPublish) {
  co_await expectInboundRequestRejectedAfterGoaway(
      "PUBLISH", [this]() -> folly::coro::Task<RequestErrorCode> {
        PublishRequest pub{
            RequestID(0),
            FullTrackName{TrackNamespace{{"test"}}, "test-track"},
            TrackAlias(100),
            GroupOrder::Default,
            AbsoluteLocation{0, 100},
            true,
        };
        auto result =
            clientSession_->publish(std::move(pub), makePublishHandle());
        if (result.hasError()) {
          co_return result.error().errorCode;
        }
        auto reply = co_await std::move(result.value().reply);
        EXPECT_TRUE(reply.hasError());
        if (!reply.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return reply.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    SentGoawayRejectsInboundTrackStatus) {
  co_await expectInboundRequestRejectedAfterGoaway(
      "TRACK_STATUS", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result = co_await clientSession_->trackStatus(getTrackStatus());
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    SentGoawayRejectsInboundPublishNamespace) {
  co_await expectInboundRequestRejectedAfterGoaway(
      "PUBLISH_NAMESPACE", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result =
            co_await clientSession_->publishNamespace(getPublishNamespace());
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    SentGoawayRejectsInboundSubscribeNamespace) {
  co_await expectInboundRequestRejectedAfterGoaway(
      "SUBSCRIBE_NAMESPACE", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result = co_await clientSession_->subscribeNamespace(
            getSubscribeNamespace(), nullptr);
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    ReceivedGoawayRejectsLocalSubscribe) {
  co_await expectLocalRequestRejectedAfterGoaway(
      "SUBSCRIBE", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result = co_await clientSession_->subscribe(
            getSubscribe(kTestTrackName), subscribeCallback_);
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    ReceivedGoawayRejectsLocalFetch) {
  co_await expectLocalRequestRejectedAfterGoaway(
      "FETCH", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result = co_await clientSession_->fetch(
            getFetch({0, 0}, {0, 1}), fetchCallback_);
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    ReceivedGoawayRejectsLocalPublish) {
  co_await expectLocalRequestRejectedAfterGoaway(
      "PUBLISH", [this]() -> folly::coro::Task<RequestErrorCode> {
        PublishRequest pub{
            RequestID(0),
            FullTrackName{TrackNamespace{{"test"}}, "test-track"},
            TrackAlias(100),
            GroupOrder::Default,
            AbsoluteLocation{0, 100},
            true,
        };
        auto result =
            clientSession_->publish(std::move(pub), makePublishHandle());
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    ReceivedGoawayRejectsLocalTrackStatus) {
  co_await expectLocalRequestRejectedAfterGoaway(
      "TRACK_STATUS", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result = co_await clientSession_->trackStatus(getTrackStatus());
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    ReceivedGoawayRejectsLocalPublishNamespace) {
  co_await expectLocalRequestRejectedAfterGoaway(
      "PUBLISH_NAMESPACE", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result =
            co_await clientSession_->publishNamespace(getPublishNamespace());
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

CO_TEST_P_X(
    Draft18GoawayRequestRejectionTest,
    ReceivedGoawayRejectsLocalSubscribeNamespace) {
  co_await expectLocalRequestRejectedAfterGoaway(
      "SUBSCRIBE_NAMESPACE", [this]() -> folly::coro::Task<RequestErrorCode> {
        auto result = co_await clientSession_->subscribeNamespace(
            getSubscribeNamespace(), nullptr);
        EXPECT_TRUE(result.hasError());
        if (!result.hasError()) {
          co_return RequestErrorCode::INTERNAL_ERROR;
        }
        co_return result.error().errorCode;
      });
}

class RecordingSessionCloseCallback
    : public MoQSession::MoQSessionCloseCallback {
 public:
  void onMoQSessionClosed(
      SessionCloseErrorCode error,
      folly::Optional<uint32_t> /* wtError */) override {
    errorCode = error;
    closed.post();
  }

  std::optional<SessionCloseErrorCode> errorCode;
  folly::coro::Baton closed;
};

class Draft18GoawayTimeoutTest : public MoQSessionTest {
 protected:
  folly::coro::Task<std::shared_ptr<Publisher::SubscriptionHandle>>
  openPeerSubscription() {
    co_await setupMoQSession();
    expectSubscribe([](auto sub, auto /* pub */) -> TaskSubscribeResult {
      co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
    });

    auto result = co_await clientSession_->subscribe(
        getSubscribe(kTestTrackName), subscribeCallback_);
    EXPECT_FALSE(result.hasError());
    if (result.hasError()) {
      co_return nullptr;
    }
    co_return result.value();
  }
};

INSTANTIATE_TEST_SUITE_P(
    Draft18GoawayTimeoutTest,
    Draft18GoawayTimeoutTest,
    testing::Values(VersionParams{{kVersionDraft18}, kVersionDraft18}));

CO_TEST_P_X(Draft18GoawayTimeoutTest, TimeoutClosesOpenPeerSubscription) {
  auto subscription = co_await openPeerSubscription();
  if (!subscription) {
    co_return;
  }

  RecordingSessionCloseCallback closeCallback;
  serverSession_->setSessionCloseCallback(&closeCallback);
  EXPECT_CALL(*subscribeCallback_, publishDone(_))
      .WillOnce([](const PublishDone& done) {
        EXPECT_EQ(done.statusCode, PublishDoneStatusCode::SESSION_CLOSED);
        return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
      });

  Goaway goaway;
  goaway.timeout = 1;
  serverSession_->goaway(goaway);

  co_await closeCallback.closed;
  EXPECT_EQ(closeCallback.errorCode, SessionCloseErrorCode::GOAWAY_TIMEOUT);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(Draft18GoawayTimeoutTest, ClosingPeerSubscriptionCancelsTimeout) {
  auto subscription = co_await openPeerSubscription();
  if (!subscription) {
    co_return;
  }

  RecordingSessionCloseCallback closeCallback;
  serverSession_->setSessionCloseCallback(&closeCallback);

  Goaway goaway;
  goaway.timeout = 1000;
  serverSession_->goaway(goaway);
  EXPECT_FALSE(closeCallback.errorCode.has_value());

  subscription->unsubscribe();
  co_await closeCallback.closed;
  EXPECT_EQ(closeCallback.errorCode, SessionCloseErrorCode::NO_ERROR);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(
    Draft18GoawayTimeoutTest,
    ZeroTimeoutDoesNotCloseOpenPeerSubscription) {
  auto subscription = co_await openPeerSubscription();
  if (!subscription) {
    co_return;
  }

  RecordingSessionCloseCallback closeCallback;
  serverSession_->setSessionCloseCallback(&closeCallback);

  Goaway goaway;
  goaway.timeout = 0;
  serverSession_->goaway(goaway);
  co_await rescheduleN(4);
  EXPECT_FALSE(closeCallback.errorCode.has_value());

  subscription->unsubscribe();
  co_await closeCallback.closed;
  EXPECT_EQ(closeCallback.errorCode, SessionCloseErrorCode::NO_ERROR);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, Goaway) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    auto pubResult = pub->beginSubgroup(0, 0, 0);
    EXPECT_FALSE(pubResult.hasError());
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  Goaway goaway{};
  clientSession_->goaway(goaway);
  folly::coro::Baton goawayBaton;
  EXPECT_CALL(*serverPublisher, goaway(_))
      .WillOnce(testing::Invoke([&](auto receivedGoaway) -> void {
        if (getDraftMajorVersion(getServerSelectedVersion()) >= 18) {
          ASSERT_TRUE(receivedGoaway.requestID.has_value());
          EXPECT_EQ(*receivedGoaway.requestID, RequestID(1));
        } else {
          EXPECT_FALSE(receivedGoaway.requestID.has_value());
        }
        goawayBaton.post();
        return;
      }));
  co_await goawayBaton;

  subscribeHandler->unsubscribe();
}
CO_TEST_P_X(MoQSessionTest, UniStreamBeforeSetup) {
  if (useUniControlStreams(getServerSelectedVersion())) {
    // Draft 18+ uses uni streams for control, so receiving one before
    // setup is expected behavior, not an error.
    co_return;
  }
  EXPECT_FALSE(clientWt_->isSessionClosed());
  serverWt_->createUniStream();
  // Check that the client closed the session
  EXPECT_TRUE(clientWt_->isSessionClosed());
  co_return;
}
CO_TEST_P_X(MoQSessionTest, DatagramBeforeSetup) {
  EXPECT_FALSE(clientWt_->isSessionClosed());
  clientSession_->onDatagram(folly::IOBuf::copyBuffer("hello world"));
  EXPECT_TRUE(clientWt_->isSessionClosed());
  co_return;
}
CO_TEST_P_X(Draft18Test, PaddingDatagramIsDiscarded) {
  co_await setupMoQSession();
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer.writePaddingDatagram(writeBuf, 8);
  EXPECT_TRUE(result.hasValue());
  if (result.hasError()) {
    co_return;
  }

  clientSession_->onDatagram(writeBuf.move());

  EXPECT_FALSE(clientWt_->isSessionClosed());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
  co_return;
}
CO_TEST_P_X(Draft18Test, PaddingDatagramWithNonZeroByteClosesSession) {
  co_await setupMoQSession();
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  auto result = writer.writePaddingDatagram(writeBuf, 0);
  EXPECT_TRUE(result.hasValue());
  if (result.hasError()) {
    co_return;
  }
  const uint8_t invalidPadding = 0x01;
  writeBuf.append(&invalidPadding, sizeof(invalidPadding));

  clientSession_->onDatagram(writeBuf.move());

  EXPECT_TRUE(clientWt_->isSessionClosed());
  co_return;
}
CO_TEST_P_X(MoQSessionTest, EmptyUnidirectionalStream) {
  co_await setupMoQSession();

  auto wh = CHECK_NOTNULL(serverWt_->createUniStream().value_or(nullptr));
  wh->writeStreamData(
      /*data=*/nullptr, /*fin=*/true, /*byteEventCallback=*/nullptr);

  co_await folly::coro::sleep(std::chrono::milliseconds(50));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// === Uni Control Stream tests (draft-18-meta-00) ===

class MoQUniControlTest : public MoQSessionTest {};

INSTANTIATE_TEST_SUITE_P(
    MoQUniControlTest,
    MoQUniControlTest,
    testing::Values(VersionParams{{kVersionDraft18}, kVersionDraft18}));

CO_TEST_P_X(MoQUniControlTest, UniControlSetup) {
  co_await setupMoQSession();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQUniControlTest, UniControlSubscribeAfterSetup) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    auto pubResult = pub->beginSubgroup(0, 0, 0);
    EXPECT_FALSE(pubResult.hasError());
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_TRUE(res.hasValue());
  res.value()->unsubscribe();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQUniControlTest, UniControlFetchAfterSetup) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
    EXPECT_NE(standalone, nullptr);
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    fetchPub->object(
        standalone->start.group,
        /*subgroupID=*/0,
        standalone->start.object,
        moxygen::test::makeBuf(100),
        noExtensions(),
        /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  folly::coro::Baton baton;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true, _))
      .WillOnce([&] {
        baton.post();
        return folly::unit;
      });
  expectFetchSuccess();
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await baton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQUniControlTest, UniControlDataStreamBeforeSetup) {
  // Verify that a data uni stream arriving before setup completes is
  // buffered and replayed with its initial data after setup.
  clientSession_->setPublishHandler(clientPublisher);
  clientSession_->setSubscribeHandler(clientSubscriber);
  serverSession_->setPublishHandler(serverPublisher);
  serverSession_->setSubscribeHandler(serverSubscriber);

  clientSession_->start();
  serverSession_->start();

  // Server sends SERVER_SETUP
  moxygen::Setup serverSetup;
  serverSetup.params.insertParam(
      SetupParameter{
          folly::to_underlying(SetupKey::MAX_REQUEST_ID),
          initialMaxRequestID_});
  serverSetup.params.insertParam(
      SetupParameter{
          folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE), 16});
  serverSession_->sendSetup(std::move(serverSetup));

  // Before client completes setup, server opens a data uni stream with
  // a subgroup header + object. This exercises handlePreSetupUniStream's
  // buffering path and verifies initialData is preserved on replay.
  auto dataWh = CHECK_NOTNULL(serverWt_->createUniStream().value_or(nullptr));
  folly::IOBufQueue dataBuf{folly::IOBufQueue::cacheChainLength()};
  MoQFrameWriter writer;
  writer.initializeVersion(kVersionDraft18);
  TrackAlias trackAlias(0); // will match requestID 0
  ObjectHeader objHeader(0, 0, 0, 0, ObjectStatus::NORMAL);
  objHeader.length = 5;
  writer.writeSubgroupHeader(dataBuf, trackAlias, objHeader);
  writer.writeStreamObject(
      dataBuf,
      getSubgroupStreamType(
          kVersionDraft18, SubgroupIDFormat::Present, true, false),
      objHeader,
      makeBuf(5));
  // Don't FIN yet — the stream must stay open until after subscribe
  // so the read loop can wait for the alias baton
  dataWh->writeStreamData(dataBuf.move(), false, nullptr);

  // Now complete client setup
  clientSession_->setServerMaxTokenCacheSizeGuess(1024);
  auto peerSetup =
      co_await clientSession_->setup(getClientSetup(initialMaxRequestID_));

  // Yield so the replayed data stream's read loop can execute and
  // register its alias baton in bufferedSubgroups_
  co_await folly::coro::co_reschedule_on_current_executor;

  // Subscribe so the client registers trackAlias=0. The buffered data
  // stream's read loop will resolve once the alias is known.
  expectSubscribe([](auto sub, auto /*pub*/) -> TaskSubscribeResult {
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });

  auto sgConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton baton;
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, _, _))
      .WillOnce(testing::Return(sgConsumer));
  EXPECT_CALL(*sgConsumer, object(0, HasChainDataLengthOf(5), _, _))
      .WillOnce([&](auto, auto, const auto&, auto) {
        baton.post();
        return folly::unit;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_TRUE(res.hasValue());

  co_await baton;

  res.value()->unsubscribe();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQUniControlTest, UniControlDuplicateSetupStream) {
  co_await setupMoQSession();

  // Now open a second uni stream with SERVER_SETUP frame type - should be
  // rejected as duplicate
  auto wh = CHECK_NOTNULL(serverWt_->createUniStream().value_or(nullptr));
  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moxygen::Setup dupSetup;
  writeServerSetup(writeBuf, dupSetup, kVersionDraft18);
  wh->writeStreamData(writeBuf.move(), false, nullptr);

  for (int i = 0; i < 50 && !clientWt_->isSessionClosed(); ++i) {
    co_await folly::coro::co_reschedule_on_current_executor;
  }
  // The client should have closed the session due to duplicate control stream
  EXPECT_TRUE(clientWt_->isSessionClosed());
}

CO_TEST_P_X(MoQUniControlTest, BidiSetupRejectedInUniControlMode) {
  co_await setupMoQSession();

  auto bidiResult = clientWt_->createBidiStream();
  EXPECT_TRUE(bidiResult.hasValue());
  if (!bidiResult.hasValue()) {
    co_return;
  }
  auto writeHandle = bidiResult->writeHandle;

  folly::IOBufQueue writeBuf{folly::IOBufQueue::cacheChainLength()};
  moxygen::Setup setup;
  writeClientSetup(writeBuf, setup, kVersionDraft18);
  writeHandle->writeStreamData(writeBuf.move(), false, nullptr);

  for (int i = 0; i < 50 && !serverWt_->isSessionClosed(); ++i) {
    co_await folly::coro::co_reschedule_on_current_executor;
  }
  EXPECT_TRUE(serverWt_->isSessionClosed());
}

CO_TEST_P_X(MoQUniControlTest, MaxBufferedPreSetupUniStreams) {
  clientSession_->setPublishHandler(clientPublisher);
  clientSession_->setSubscribeHandler(clientSubscriber);
  serverSession_->setPublishHandler(serverPublisher);
  serverSession_->setSubscribeHandler(serverSubscriber);

  clientSession_->start();
  serverSession_->start();

  // Server proactively sends SERVER_SETUP
  moxygen::Setup serverSetup;
  serverSetup.params.insertParam(
      SetupParameter{
          folly::to_underlying(SetupKey::MAX_REQUEST_ID),
          initialMaxRequestID_});
  serverSetup.params.insertParam(
      SetupParameter{
          folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE), 16});
  serverSession_->sendSetup(std::move(serverSetup));

  // Before client completes setup, open 101 data uni streams from server.
  // Each writes a non-SETUP frame type byte so handlePreSetupUniStream
  // buffers them.
  static constexpr uint8_t kSubgroupHeaderByte =
      folly::to_underlying(StreamType::SUBGROUP_HEADER_SG_ZERO);
  for (int i = 0; i <= 100; i++) {
    auto wh = CHECK_NOTNULL(serverWt_->createUniStream().value_or(nullptr));
    auto buf = folly::IOBuf::create(1);
    buf->append(1);
    buf->writableData()[0] = kSubgroupHeaderByte;
    wh->writeStreamData(std::move(buf), false, nullptr);
  }

  for (int i = 0; i < 50 && !clientWt_->isSessionClosed(); ++i) {
    co_await folly::coro::co_reschedule_on_current_executor;
  }
  EXPECT_TRUE(clientWt_->isSessionClosed());
}

// New tests for MoQClientBase guarding WT callbacks after session reset
class DummyMoQClientBase : public MoQClientBase {
 public:
  using MoQClientBase::MoQClientBase;

  void test_onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bidi) {
    if (moqSession_) {
      moqSession_->onNewBidiStream(std::move(bidi));
    }
  }
  void test_onNewUniStream(proxygen::WebTransport::StreamReadHandle* handle) {
    if (moqSession_) {
      moqSession_->onNewUniStream(handle);
    }
  }
  void test_onDatagram(std::unique_ptr<folly::IOBuf> datagram) {
    if (moqSession_) {
      moqSession_->onDatagram(std::move(datagram));
    }
  }
  void test_goaway(const Goaway& goaway) {
    MoQClientBase::goaway(goaway);
  }

 protected:
  folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>> connectQuic(
      folly::SocketAddress /*connectAddr*/,
      std::chrono::milliseconds /*timeoutMs*/,
      std::shared_ptr<fizz::CertificateVerifier> /*verifier*/,
      const std::vector<std::string>& /*alpns*/,
      const quic::TransportSettings& /*transportSettings*/) override {
    co_return nullptr;
  }
};

class TestMoQClient : public MoQClient {
 public:
  using MoQClient::MoQClient;

  void setQuicWebTransport(std::shared_ptr<proxygen::QuicWebTransport> wt) {
    quicWebTransport_ = std::move(wt);
  }
};

std::shared_ptr<proxygen::QuicWebTransport> makeTestQuicWebTransport() {
  auto quicSocket = std::make_shared<testing::NiceMock<quic::MockQuicSocket>>();
  ON_CALL(*quicSocket, setDatagramCallback(_))
      .WillByDefault(
          [](quic::QuicSocket::DatagramCallback*)
              -> quic::Expected<void, quic::LocalErrorCode> { return {}; });
  return std::make_shared<proxygen::QuicWebTransport>(std::move(quicSocket));
}

TEST(MoQClientBaseTest, CallbacksIgnoredWhenSessionNull) {
  folly::EventBase evb;
  auto exec = std::make_shared<MoQFollyExecutorImpl>(&evb);
  proxygen::URL url("https://example.com:443/");

  DummyMoQClientBase client(exec, std::move(url));

  // Ensure no session is set (simulating post-reset state)
  client.moqSession_.reset();

  // Prepare fake WT stream handles
  auto readH = std::make_unique<proxygen::test::FakeStreamHandle>(1);
  auto writeH = std::make_unique<proxygen::test::FakeStreamHandle>(1);
  proxygen::WebTransport::BidiStreamHandle bidi{readH.get(), writeH.get()};

  // These should be safely ignored and must not crash when moqSession_ is null
  client.test_onNewBidiStream(bidi);
  client.test_onNewUniStream(readH.get());
  client.test_onDatagram(folly::IOBuf::copyBuffer("hi"));
  client.test_goaway(Goaway{"/newSession"});
}

TEST(MoQClientTest, TransportStatsUnavailableAfterSessionClose) {
  folly::EventBase evb;
  auto exec = std::make_shared<MoQFollyExecutorImpl>(&evb);

  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  (void)serverWt;
  auto session = std::make_shared<MoQSession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()), exec);

  auto quicWebTransport = makeTestQuicWebTransport();
  auto retainedQuicWebTransport = quicWebTransport;

  TestMoQClient client(exec, proxygen::URL("https://example.com:443/"));
  client.moqSession_ = session;
  client.setQuicWebTransport(std::move(quicWebTransport));

  session->close(SessionCloseErrorCode::NO_ERROR);
  static_cast<proxygen::WebTransport*>(retainedQuicWebTransport.get())
      ->closeSession();

  EXPECT_FALSE(client.getTransportInfo().has_value());

  auto flowControl = client.getConnectionFlowControl();
  ASSERT_FALSE(flowControl.has_value());
  EXPECT_EQ(flowControl.error(), quic::LocalErrorCode::CONNECTION_CLOSED);
}

TEST(MoQClientTest, TransportStatsUnavailableAfterSocketClose) {
  folly::EventBase evb;
  auto exec = std::make_shared<MoQFollyExecutorImpl>(&evb);

  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  (void)serverWt;
  auto session = std::make_shared<MoQSession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()), exec);

  auto quicWebTransport = makeTestQuicWebTransport();
  auto retainedQuicWebTransport = quicWebTransport;

  TestMoQClient client(exec, proxygen::URL("https://example.com:443/"));
  client.moqSession_ = session;
  client.setQuicWebTransport(std::move(quicWebTransport));

  static_cast<proxygen::WebTransport*>(retainedQuicWebTransport.get())
      ->closeSession();

  EXPECT_FALSE(session->isClosed());
  EXPECT_FALSE(client.getTransportInfo().has_value());

  auto flowControl = client.getConnectionFlowControl();
  ASSERT_FALSE(flowControl.has_value());
  EXPECT_EQ(flowControl.error(), quic::LocalErrorCode::CONNECTION_CLOSED);
}

TEST(MoQRelayClientTest, ShutdownClearsHandlersAndResetsSession) {
  folly::EventBase evb;
  auto exec = std::make_shared<MoQFollyExecutorImpl>(&evb);

  // Create a session and wire it into a MoQClient used by MoQRelayClient
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  auto session = std::make_shared<MoQSession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()), exec);

  auto moqClient = std::make_unique<MoQClient>(
      exec, proxygen::URL("https://example.com:443/"));
  moqClient->moqSession_ = session;

  MoQRelayClient relay(std::move(moqClient));
  ASSERT_NE(relay.getSession(), nullptr);

  // After shutdown, the client session pointer must be reset to prevent any
  // further usage after the relay stops.
  relay.shutdown();
  EXPECT_EQ(relay.getSession(), nullptr);
}

// Test suite to demonstrate and verify the UAF fix in MoQSession
// when application callbacks delete the session during
// controlCodec_.onIngress()
class MoQSessionDeleteFromCallbackTest : public MoQSessionTest {};

// Test that demonstrates UAF when application deletes session during
// publishDone callback
CO_TEST_P_X(MoQSessionDeleteFromCallbackTest, DeleteFromPublishDoneCallback) {
  std::weak_ptr<MoQSession> weakSession;

  // Setup the session first
  co_await setupMoQSession();

  // Create a track consumer that we'll use to send publishDone
  std::shared_ptr<TrackConsumer> serverTrackConsumer;

  // Setup server publisher to accept subscription and store the TrackConsumer
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    pub->setTrackAlias(TrackAlias(sub.requestID.value));
    // Store the TrackConsumer so we can call publishDone on it later
    serverTrackConsumer = pub;
    co_return makeSubscribeOkResult(sub);
  });

  // Subscribe to a track using our suicidal consumer
  weakSession = clientSession_;
  // Create a mock TrackConsumer that deletes the session in publishDone
  auto consumer = std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  EXPECT_CALL(*consumer, setTrackAlias(testing::_))
      .WillOnce(testing::Return(folly::unit));
  // Subscribe to track - co_await ensures the mock lambda executes
  auto subResult = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), consumer);
  EXPECT_CALL(*consumer, publishDone(testing::_))
      .WillOnce([&](const PublishDone&) {
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

  // Now server sends PUBLISH_DONE - this will trigger the chain:
  // controlCodec_.onIngress() -> onPublishDone -> processPublishDone
  // -> callback_->publishDone() -> delete session -> return to onIngress ->
  // UAF!
  PublishDone pubDone;
  pubDone.requestID = 1;
  pubDone.statusCode = PublishDoneStatusCode::SUBSCRIPTION_ENDED;
  pubDone.reasonPhrase = "test";
  pubDone.streamCount = 0; // No streams in flight

  // This should trigger the UAF if the guard is not in place
  // serverTrackConsumer is the client's TrackConsumer on the server side
  serverTrackConsumer->publishDone(pubDone);

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

// Test that verifies fix for bad_weak_ptr crash
// Scenario: Session is destroyed while controlReadLoop coroutine is waiting
// for data. When data arrives, the coroutine should exit gracefully instead
// of crashing with bad_weak_ptr.
// See: T254245035
CO_TEST_P_X(
    MoQSessionDeleteFromCallbackTest,
    WeakFromThisGracefulExitOnSessionDestroy) {
  // Setup the session
  co_await setupMoQSession();

  // Get weak_ptr to track session lifetime
  std::weak_ptr<MoQSession> weakClient = clientSession_;

  // The control stream is stream ID 0. We need to:
  // 1. Pause data delivery so controlReadLoop blocks on co_await
  // 2. Release the session shared_ptr
  // 3. Deliver data to trigger the coroutine resume
  // 4. Verify it exits gracefully (with our fix) instead of crashing

  // Pause data delivery on the control stream (ID 0)
  // This ensures controlReadLoop is blocked waiting for data
  serverWt_->writeHandles[0]->setImmediateDelivery(false);

  // Write some data that will be held in the inflight buffer
  auto data = folly::IOBuf::copyBuffer("test");
  serverWt_->writeHandles[0]->writeStreamData(std::move(data), false, nullptr);

  // Now release the client session shared_ptr WITHOUT calling close()
  // This simulates the scenario where all external references are released
  // but the coroutine is still running
  clientSession_.reset();

  // Verify session weak_ptr is expired (no more shared_ptr holders)
  EXPECT_TRUE(weakClient.expired())
      << "Session should have no shared_ptr holders";

  // Now deliver the data to wake up the controlReadLoop coroutine
  // Before the fix: shared_from_this() would throw bad_weak_ptr
  // After the fix: weak_from_this().lock() returns nullptr, coroutine exits
  // gracefully
  serverWt_->writeHandles[0]->deliverInflightData();

  // Drive the event loop to let the coroutine process
  co_await folly::coro::co_reschedule_on_current_executor;
  co_await folly::coro::co_reschedule_on_current_executor;

  // If we get here without crashing, the fix is working

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

// Demonstrate and verify that cleanup() breaks the shared_ptr cycle between
// a session and a handler that holds a back-reference to that session.
//
// The cycle:
//   MoQSession --[publishHandler_]--> CyclingHandler
//   CyclingHandler --[session_]-----> MoQSession
//
// Without the reset in cleanup() both objects keep each other alive after all
// external shared_ptrs are released.
TEST(MoQSessionTest, SharedPtrCycleBreaksOnCleanup) {
  folly::EventBase eventBase;
  auto moqExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();

  auto session = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt.get()),
      moqExecutor);

  // A minimal Publisher whose only purpose is to hold a strong ref back to the
  // session, forming a cycle when the session holds it as publishHandler_.
  struct CyclingPublisher : public Publisher {
    explicit CyclingPublisher(std::shared_ptr<MoQSession> s)
        : session_(std::move(s)) {}
    std::shared_ptr<MoQSession> session_;
  };

  auto handler = std::make_shared<CyclingPublisher>(session);
  std::weak_ptr<CyclingPublisher> weakHandler = handler;
  std::weak_ptr<MoQSession> weakSession = session;

  // Establish the cycle: session holds handler, handler holds session.
  session->setPublishHandler(handler);

  // Drop the local strong references — only the cycle keeps both alive.
  handler.reset();
  session.reset();

  // Both objects are still alive because they reference each other.
  EXPECT_FALSE(weakSession.expired()) << "session kept alive by handler";
  EXPECT_FALSE(weakHandler.expired()) << "handler kept alive by session";

  // close() calls cleanup(), which must reset publishHandler_ (and
  // subscribeHandler_) to break the cycle and allow both objects to be
  // destroyed.
  auto locked = weakSession.lock();
  locked->close(SessionCloseErrorCode::NO_ERROR);
  locked.reset();

  EXPECT_TRUE(weakHandler.expired()) << "handler freed after close()";
  EXPECT_TRUE(weakSession.expired()) << "session freed after close()";
}

TEST(MoQSessionTest, BidiStreamRejectsUnexpectedFrameType) {
  folly::EventBase eventBase;
  auto moqExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();

  class TestServerSetupCallback : public MoQSession::ServerSetupCallback {
   public:
    folly::Try<ServerSetup> onClientSetup(
        ClientSetup,
        const std::shared_ptr<MoQSession>&) override {
      return folly::Try<ServerSetup>(ServerSetup{});
    }
    folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
        const ClientSetup&,
        uint64_t,
        std::shared_ptr<MoQSession>) override {
      return folly::unit;
    }
  };

  TestServerSetupCallback serverSetupCallback;
  auto serverSession = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(serverWt.get()),
      serverSetupCallback,
      moqExecutor);

  // Set version to moqt-16 via ALPN so bidiStreamDemuxer is used
  serverSession->validateAndSetVersionFromAlpn("moqt-16");

  // Set the peer handler so createBidiStream triggers onNewBidiStream
  clientWt->setPeerHandler(serverSession.get());

  // Create a bidi stream from the client side
  auto bidiResult = clientWt->createBidiStream();
  ASSERT_TRUE(bidiResult.hasValue());
  auto writeHandle = bidiResult->writeHandle;

  // Write LEGACY_CLIENT_SETUP frame type (0x40) which is invalid for moqt-16.
  // 0x40 as a QUIC varint encodes as two bytes: 0x40 0x40
  auto buf = folly::IOBuf::create(4);
  buf->append(4);
  buf->writableData()[0] = 0x40;
  buf->writableData()[1] = 0x40;
  buf->writableData()[2] = 0x00;
  buf->writableData()[3] = 0x00;
  writeHandle->writeStreamData(std::move(buf), false, nullptr);

  eventBase.loop();

  // The server should have closed the session due to unexpected frame type
  EXPECT_TRUE(serverWt->isSessionClosed());
}

TEST(MoQSessionTest, BidiStreamDemuxerHonorsStreamCancellation) {
  folly::EventBase eventBase;
  auto moqExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();

  class TestServerSetupCallback : public MoQSession::ServerSetupCallback {
   public:
    folly::Try<ServerSetup> onClientSetup(
        ClientSetup,
        const std::shared_ptr<MoQSession>&) override {
      return folly::Try<ServerSetup>(ServerSetup{});
    }
    folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
        const ClientSetup&,
        uint64_t,
        std::shared_ptr<MoQSession>) override {
      return folly::unit;
    }
  };

  TestServerSetupCallback serverSetupCallback;
  auto serverSession = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(serverWt.get()),
      serverSetupCallback,
      moqExecutor);
  serverSession->validateAndSetVersionFromAlpn("moqt-16");
  clientWt->setPeerHandler(serverSession.get());

  auto bidiResult = clientWt->createBidiStream();
  ASSERT_TRUE(bidiResult.hasValue());
  auto streamId = bidiResult->writeHandle->getID();

  eventBase.loopOnce();

  bidiResult->readHandle->stopSending(0);

  auto buf = folly::IOBuf::create(1);
  buf->append(1);
  buf->writableData()[0] = 0x20;
  bidiResult->writeHandle->writeStreamData(std::move(buf), false, nullptr);

  eventBase.loop();

  EXPECT_FALSE(serverWt->writeHandles[streamId]->pri.has_value());
}

INSTANTIATE_TEST_SUITE_P(
    MoQSessionTest,
    MoQSessionTest,
    testing::ValuesIn(getSupportedVersionParams()));
