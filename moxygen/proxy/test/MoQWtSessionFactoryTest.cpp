/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQWtSessionFactory.h"

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>
#include <proxygen/lib/http/webtransport/test/Mocks.h>

#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "moxygen/MoQVersions.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/test/Mocks.h"

namespace moxygen::test {

namespace {

class AcceptingServerSetupCallback : public MoQSession::ServerSetupCallback {
 public:
  folly::Try<Setup> onClientSetup(
      Setup /* clientSetup */,
      const std::shared_ptr<MoQSession>& /* session */) override {
    return folly::Try<Setup>(makeServerSetup());
  }

  folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
      const Setup& /* clientSetup */,
      uint64_t /* negotiatedVersion */,
      std::shared_ptr<MoQSession> /* session */) override {
    return folly::unit;
  }

  static Setup makeServerSetup() {
    Setup setup;
    setup.params.insertParam(Parameter(
        folly::to_underlying(SetupKey::MAX_REQUEST_ID), kDefaultMaxRequestID));
    return setup;
  }
};

class MoQWtSessionFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto [clientTransport, serverTransport] =
        proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
    clientTransport_ = std::move(clientTransport);
    serverTransport_ = std::move(serverTransport);
    clientTransport_->setDeliveryExecutor(&eventBase_);
    serverTransport_->setDeliveryExecutor(&eventBase_);
  }

  void TearDown() override {
    if (serverSession_) {
      serverSession_->close(SessionCloseErrorCode::NO_ERROR);
    }
  }

  void makeServerSession(std::string_view alpn) {
    serverSession_ = std::make_shared<MoQSession>(
        folly::MaybeManagedPtr<proxygen::WebTransport>(serverTransport_.get()),
        serverSetupCallback_,
        executor_);
    ASSERT_TRUE(
        serverSession_->validateAndSetVersionFromAlpn(std::string(alpn)));
    clientTransport_->setPeerHandler(serverSession_.get());
  }

  // Stands in for sendWtReq, which starts the WebTransport with the handler.
  proxygen::WebTransportHandler::Ptr startWebTransport(
      MoQWtSessionFactory& factory,
      std::string_view alpn) {
    auto handler = factory.makeHandler(std::string(alpn), "", "");
    serverTransport_->setPeerHandler(handler.get());
    handler->onWebTransportSession(clientTransport_);
    return handler;
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQExecutor> executor_ =
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_);
  std::shared_ptr<proxygen::test::FakeSharedWebTransport> clientTransport_;
  std::shared_ptr<proxygen::test::FakeSharedWebTransport> serverTransport_;
  AcceptingServerSetupCallback serverSetupCallback_;
  std::shared_ptr<MoQSession> serverSession_;
};

} // namespace

TEST_F(MoQWtSessionFactoryTest, CreatesSessionAndOwnsTransport) {
  auto keepalive = std::make_shared<int>();
  std::weak_ptr<void> keepaliveObserver = keepalive;
  MoQWtSessionFactory factory(executor_, keepalive);
  keepalive.reset();
  makeServerSession(kAlpnMoqtDraft16Latest);
  serverSession_->start();
  auto handler = startWebTransport(factory, kAlpnMoqtDraft16Latest);

  auto session = folly::coro::blockingWait(factory.establish(), &eventBase_);

  ASSERT_NE(session, nullptr);
  EXPECT_EQ(session->getNegotiatedVersion(), kVersionDraft16);
  EXPECT_FALSE(keepaliveObserver.expired());
  EXPECT_FALSE(clientTransport_->isSessionClosed());
  session.reset();
  EXPECT_TRUE(keepaliveObserver.expired());
  EXPECT_TRUE(clientTransport_->isSessionClosed());
}

// On draft 18+ the server sends SERVER_SETUP on its own control stream without
// waiting for CLIENT_SETUP.
TEST_F(MoQWtSessionFactoryTest, ServerSetupBeforeClientSetup) {
  MoQWtSessionFactory factory(executor_);
  makeServerSession(kAlpnMoqtDraft18Latest);
  auto handler = startWebTransport(factory, kAlpnMoqtDraft18Latest);
  serverSession_->start();
  ASSERT_FALSE(
      serverSession_->sendSetup(AcceptingServerSetupCallback::makeServerSetup())
          .hasError());
  eventBase_.loopOnce(EVLOOP_NONBLOCK);

  auto session = folly::coro::blockingWait(factory.establish(), &eventBase_);

  ASSERT_NE(session, nullptr);
  EXPECT_EQ(session->getNegotiatedVersion(), kVersionDraft18);
}

TEST_F(MoQWtSessionFactoryTest, StatsCallbacksOutliveFactoryAndRecordRequests) {
  auto publisher = std::make_shared<testing::StrictMock<MockPublisherStats>>();
  auto subscriber =
      std::make_shared<testing::StrictMock<MockSubscriberStats>>();
  std::weak_ptr<MoQPublisherStatsCallback> publisherObserver = publisher;
  std::weak_ptr<MoQSubscriberStatsCallback> subscriberObserver = subscriber;
  EXPECT_CALL(*publisher, getObjectAckStatsCallback())
      .WillRepeatedly(testing::Return(nullptr));
  EXPECT_CALL(*publisher, onSubscribeError(SubscribeErrorCode::INTERNAL_ERROR));
  EXPECT_CALL(*publisher, onFetchError(FetchErrorCode::INTERNAL_ERROR));
  EXPECT_CALL(
      *subscriber, onSubscribeError(SubscribeErrorCode::INTERNAL_ERROR));
  EXPECT_CALL(*subscriber, recordSubscribeLatency(testing::_));
  EXPECT_CALL(*subscriber, onFetchError(FetchErrorCode::INTERNAL_ERROR));
  EXPECT_CALL(*subscriber, recordFetchLatency(testing::_));

  makeServerSession(kAlpnMoqtDraft16Latest);
  serverSession_->start();
  proxygen::WebTransportHandler::Ptr handler;
  std::shared_ptr<MoQSession> session;
  {
    MoQWtSessionFactory factory(
        executor_,
        nullptr,
        {.publisher = std::move(publisher),
         .subscriber = std::move(subscriber)});
    handler = startWebTransport(factory, kAlpnMoqtDraft16Latest);
    session = folly::coro::blockingWait(factory.establish(), &eventBase_);
  }
  ASSERT_FALSE(publisherObserver.expired());
  ASSERT_FALSE(subscriberObserver.expired());
  EXPECT_EQ(publisherObserver.lock()->getObjectAckStatsCallback(), nullptr);

  const FullTrackName track{TrackNamespace({"test"}), "track"};
  auto subscribeRequest = [&] {
    return SubscribeRequest::make(
        track, 0, GroupOrder::OldestFirst, true, LocationFilter::nextObject());
  };
  auto fetchRequest = [&] {
    return Fetch(
        RequestID(0),
        track,
        AbsoluteLocation{0, 0},
        AbsoluteLocation{1, 0},
        0,
        GroupOrder::OldestFirst);
  };

  auto clientSubscribe = folly::coro::blockingWait(
      session->subscribe(
          subscribeRequest(), std::make_shared<MockTrackConsumer>()),
      &eventBase_);
  EXPECT_TRUE(clientSubscribe.hasError());
  auto serverSubscribe = folly::coro::blockingWait(
      serverSession_->subscribe(
          subscribeRequest(), std::make_shared<MockTrackConsumer>()),
      &eventBase_);
  EXPECT_TRUE(serverSubscribe.hasError());
  auto clientFetch = folly::coro::blockingWait(
      session->fetch(fetchRequest(), std::make_shared<MockFetchConsumer>()),
      &eventBase_);
  EXPECT_TRUE(clientFetch.hasError());
  auto serverFetch = folly::coro::blockingWait(
      serverSession_->fetch(
          fetchRequest(), std::make_shared<MockFetchConsumer>()),
      &eventBase_);
  EXPECT_TRUE(serverFetch.hasError());
}

TEST_F(MoQWtSessionFactoryTest, EstablishFailsAtTheGivenSetupTimeout) {
  MoQWtSessionFactory factory(executor_);
  // takes the client's control stream but never sends SETUP
  testing::NiceMock<proxygen::test::MockWebTransportHandler> peer;
  EXPECT_CALL(
      peer,
      onSessionEnd(
          folly::Optional<uint32_t>(folly::to_underlying(
              SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED))));
  clientTransport_->setPeerHandler(&peer);
  auto handler =
      factory.makeHandler(std::string(kAlpnMoqtDraft18Latest), "", "");
  handler->onWebTransportSession(clientTransport_);
  constexpr std::chrono::milliseconds kSetupTimeout{50};

  const auto start = std::chrono::steady_clock::now();
  auto result = folly::coro::blockingWait(
      folly::coro::co_awaitTry(factory.establish(kSetupTimeout)), &eventBase_);
  const auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_TRUE(result.hasException<folly::FutureTimeout>());
  EXPECT_GE(elapsed, kSetupTimeout);
  EXPECT_LT(elapsed, std::chrono::milliseconds(500));
}

TEST_F(MoQWtSessionFactoryTest, EstablishWithoutWebTransportThrows) {
  MoQWtSessionFactory factory(executor_);
  auto handler =
      factory.makeHandler(std::string(kAlpnMoqtDraft16Latest), "", "");

  EXPECT_THROW(
      folly::coro::blockingWait(factory.establish(), &eventBase_),
      std::runtime_error);
}

TEST_F(MoQWtSessionFactoryTest, RejectsNullExecutor) {
  EXPECT_THROW(MoQWtSessionFactory(nullptr), std::invalid_argument);
}

} // namespace moxygen::test
