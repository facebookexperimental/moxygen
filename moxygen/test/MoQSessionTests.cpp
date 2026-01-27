/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

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
  ClientSetup setup;
  setup.supportedVersions.push_back(kVersionDraftCurrent);
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
TEST(MoQSessionTest, ServerSetupVersion15WithoutAlpnShouldFail) {
  // Test that when version >= 15 is present in SERVER_SETUP but not
  // pre-negotiated via ALPN, the server should close with
  // VERSION_NEGOTIATION_FAILED

  folly::EventBase eventBase;
  auto moqExecutor = std::make_shared<MoQFollyExecutorImpl>(&eventBase);
  auto [clientWt, serverWt] =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();

  class TestServerSetupCallback : public MoQSession::ServerSetupCallback {
   public:
    folly::Try<ServerSetup> onClientSetup(
        ClientSetup /*clientSetup*/,
        const std::shared_ptr<MoQSession>& /*session*/) override {
      // Server tries to select version >= 15 without ALPN negotiation
      ServerSetup serverSetup;
      serverSetup.selectedVersion = 0xff00000f;
      return folly::Try<ServerSetup>(serverSetup);
    }

    folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
        const ClientSetup& /*clientSetup*/,
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

  // Client sends setup with version < 15 (no ALPN negotiation happened)
  ClientSetup clientSetup;
  clientSetup.supportedVersions.push_back(kVersionDraft14);

  folly::coro::co_withExecutor(
      moqExecutor.get(), clientSession->setup(clientSetup))
      .start();
  eventBase.loop();

  // Server should have closed the session with VERSION_NEGOTIATION_FAILED
  EXPECT_TRUE(serverWt->isSessionClosed())
      << "Server should close when version >= 15 is selected without ALPN";

  // Cleanup
  if (!clientWt->isSessionClosed()) {
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  }
  if (!serverWt->isSessionClosed()) {
    serverSession->close(SessionCloseErrorCode::NO_ERROR);
  }
}
CO_TEST_P_X(MoQSessionTest, ClientReceivesBidiStream) {
  serverWt_->createBidiStream();
  // Check that the client called stopSending and resetStream on the newly
  // created stream.
  EXPECT_TRUE(clientWt_->readHandles.begin()->second->writeException());
  EXPECT_TRUE(
      clientWt_->writeHandles.begin()->second->getWriteErr().hasValue());
  co_return;
}
CO_TEST_P_X(MoQSessionTest, Goaway) {
  co_await setupMoQSession();

  // Make a SUBSCRIBE request so that we don't immediately close when goaway()
  // is called.
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
      .WillOnce(testing::Invoke([&goawayBaton](auto /* goaway */) -> void {
        goawayBaton.post();
        return;
      }));
  co_await goawayBaton;

  subscribeHandler->unsubscribe();
}
CO_TEST_P_X(MoQSessionTest, UniStreamBeforeSetup) {
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
CO_TEST_P_X(MoQSessionTest, EmptyUnidirectionalStream) {
  co_await setupMoQSession();

  auto wh = CHECK_NOTNULL(serverWt_->createUniStream().value_or(nullptr));
  wh->writeStreamData(
      /*data=*/nullptr, /*fin=*/true, /*byteEventCallback=*/nullptr);

  co_await folly::coro::sleep(std::chrono::milliseconds(50));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// New tests for MoQClientBase guarding WT callbacks after session reset
class DummyMoQClientBase : public MoQClientBase {
 public:
  using MoQClientBase::MoQClientBase;

  void test_onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bidi) {
    MoQClientBase::onNewBidiStream(std::move(bidi));
  }
  void test_onNewUniStream(proxygen::WebTransport::StreamReadHandle* handle) {
    MoQClientBase::onNewUniStream(handle);
  }
  void test_onDatagram(std::unique_ptr<folly::IOBuf> datagram) {
    MoQClientBase::onDatagram(std::move(datagram));
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

INSTANTIATE_TEST_SUITE_P(
    MoQSessionTest,
    MoQSessionTest,
    testing::ValuesIn(getSupportedVersionParams()));
