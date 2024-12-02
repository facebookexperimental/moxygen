/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"
#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>

namespace {
using namespace moxygen;

const size_t kTestMaxSubscribeId = 100;

class MockControlVisitorBase {
 public:
  virtual ~MockControlVisitorBase() = default;
  virtual void onSubscribe(SubscribeRequest subscribeRequest) const = 0;
  virtual void onSubscribeUpdate(SubscribeUpdate subscribeUpdate) const = 0;
  virtual void onSubscribeDone(SubscribeDone subscribeDone) const = 0;
  virtual void onMaxSubscribeId(MaxSubscribeId maxSubscribeId) const = 0;
  virtual void onUnsubscribe(Unsubscribe unsubscribe) const = 0;
  virtual void onAnnounce(Announce announce) const = 0;
  virtual void onUnannounce(Unannounce unannounce) const = 0;
  virtual void onAnnounceCancel(AnnounceCancel announceCancel) const = 0;
  virtual void onSubscribeAnnounces(
      SubscribeAnnounces subscribeAnnounces) const = 0;
  virtual void onUnsubscribeAnnounces(
      UnsubscribeAnnounces subscribeAnnounces) const = 0;
  virtual void onTrackStatusRequest(
      TrackStatusRequest trackStatusRequest) const = 0;
  virtual void onTrackStatus(TrackStatus trackStatus) const = 0;
  virtual void onGoaway(Goaway goaway) const = 0;
};

class MockControlVisitor : public MoQSession::ControlVisitor,
                           MockControlVisitorBase {
 public:
  MockControlVisitor() = default;
  ~MockControlVisitor() override = default;

  MOCK_METHOD(void, onAnnounce, (Announce), (const));
  void operator()(Announce announce) const override {
    onAnnounce(announce);
  }

  MOCK_METHOD(void, onUnannounce, (Unannounce), (const));
  void operator()(Unannounce unannounce) const override {
    onUnannounce(unannounce);
  }

  MOCK_METHOD(void, onAnnounceCancel, (AnnounceCancel), (const));
  void operator()(AnnounceCancel announceCancel) const override {
    onAnnounceCancel(announceCancel);
  }

  MOCK_METHOD(void, onSubscribeAnnounces, (SubscribeAnnounces), (const));
  void operator()(SubscribeAnnounces subscribeAnnounces) const override {
    onSubscribeAnnounces(subscribeAnnounces);
  }

  MOCK_METHOD(void, onUnsubscribeAnnounces, (UnsubscribeAnnounces), (const));
  void operator()(UnsubscribeAnnounces unsubscribeAnnounces) const override {
    onUnsubscribeAnnounces(unsubscribeAnnounces);
  }

  MOCK_METHOD(void, onSubscribe, (SubscribeRequest), (const));
  void operator()(SubscribeRequest subscribe) const override {
    onSubscribe(subscribe);
  }
  MOCK_METHOD(void, onSubscribeUpdate, (SubscribeUpdate), (const));
  void operator()(SubscribeUpdate subscribeUpdate) const override {
    onSubscribeUpdate(subscribeUpdate);
  }

  MOCK_METHOD(void, onSubscribeDone, (SubscribeDone), (const));
  void operator()(SubscribeDone subscribeDone) const override {
    onSubscribeDone(subscribeDone);
  }

  MOCK_METHOD(void, onUnsubscribe, (Unsubscribe), (const));
  void operator()(Unsubscribe unsubscribe) const override {
    onUnsubscribe(unsubscribe);
  }

  MOCK_METHOD(void, onMaxSubscribeId, (MaxSubscribeId), (const));
  void operator()(MaxSubscribeId maxSubscribeId) const override {
    onMaxSubscribeId(maxSubscribeId);
  }

  MOCK_METHOD(void, onTrackStatusRequest, (TrackStatusRequest), (const));
  void operator()(TrackStatusRequest trackStatusRequest) const override {
    onTrackStatusRequest(trackStatusRequest);
  }
  MOCK_METHOD(void, onTrackStatus, (TrackStatus), (const));
  void operator()(TrackStatus trackStatus) const override {
    onTrackStatus(trackStatus);
  }
  MOCK_METHOD(void, onGoaway, (Goaway), (const));
  void operator()(Goaway goaway) const override {
    onGoaway(goaway);
  }

 private:
};

class MoQSessionTest : public testing::Test,
                       public MoQSession::ServerSetupCallback {
 public:
  MoQSessionTest() {
    std::tie(clientWt_, serverWt_) =
        proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
    clientSession_ = std::make_shared<MoQSession>(clientWt_.get(), &eventBase_);
    serverWt_->setPeerHandler(clientSession_.get());

    serverSession_ =
        std::make_shared<MoQSession>(serverWt_.get(), *this, &eventBase_);
    clientWt_->setPeerHandler(serverSession_.get());
  }

  void SetUp() override {}

  folly::coro::Task<void> controlLoop(
      MoQSession& session,
      MockControlVisitor& control) {
    while (auto msg = co_await session.controlMessages().next()) {
      boost::apply_visitor(control, msg.value());
    }
  }

  folly::Try<ServerSetup> onClientSetup(ClientSetup setup) override {
    EXPECT_EQ(setup.supportedVersions[0], kVersionDraftCurrent);
    if (!setup.params.empty()) {
      EXPECT_EQ(
          setup.params.at(0).key,
          folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID));
      EXPECT_EQ(setup.params.at(0).asUint64, kTestMaxSubscribeId);
    }
    if (failServerSetup_) {
      return folly::makeTryWith(
          []() -> ServerSetup { throw std::runtime_error("failed"); });
    }
    return folly::Try<ServerSetup>(ServerSetup{
        .selectedVersion = negotiatedVersion_,
        .params = {
            {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
              .asUint64 = kTestMaxSubscribeId}}}});
  }

 protected:
  folly::EventBase eventBase_;
  std::unique_ptr<proxygen::test::FakeSharedWebTransport> clientWt_;
  std::unique_ptr<proxygen::test::FakeSharedWebTransport> serverWt_;
  std::shared_ptr<MoQSession> clientSession_;
  std::shared_ptr<MoQSession> serverSession_;
  MockControlVisitor clientControl;
  MockControlVisitor serverControl;
  uint64_t negotiatedVersion_ = kVersionDraftCurrent;
  bool failServerSetup_{false};
};
} // namespace

TEST_F(MoQSessionTest, Setup) {
  clientSession_->start();
  serverSession_->start();
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession) -> folly::coro::Task<void> {
    auto serverSetup = co_await clientSession->setup(ClientSetup{
        .supportedVersions = {kVersionDraftCurrent},
        .params = {
            {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
              .asUint64 = kTestMaxSubscribeId}}}});

    EXPECT_EQ(serverSetup.selectedVersion, kVersionDraftCurrent);
    EXPECT_EQ(
        serverSetup.params.at(0).key,
        folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID));
    EXPECT_EQ(serverSetup.params.at(0).asUint64, kTestMaxSubscribeId);
    clientSession->close();
  }(clientSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  this->controlLoop(*serverSession_, serverControl)
      .scheduleOn(&eventBase_)
      .start();
  this->controlLoop(*clientSession_, clientControl)
      .scheduleOn(&eventBase_)
      .start();
  eventBase_.loop();
}

// receive bidi stream on client

TEST_F(MoQSessionTest, SetupTimeout) {
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession) -> folly::coro::Task<void> {
    auto serverSetup = co_await co_awaitTry(clientSession->setup(ClientSetup{
        .supportedVersions = {kVersionDraftCurrent},
        .params = {
            {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
              .asUint64 = kTestMaxSubscribeId}}}}));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close();
  }(clientSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, InvalidVersion) {
  clientSession_->start();
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession) -> folly::coro::Task<void> {
    auto serverSetup = co_await co_awaitTry(clientSession->setup(ClientSetup{
        .supportedVersions = {0xfaceb001},
        .params = {
            {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
              .asUint64 = kTestMaxSubscribeId}}}}));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close();
  }(clientSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, InvalidServerVersion) {
  negotiatedVersion_ = 0xfaceb001;
  clientSession_->start();
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession) -> folly::coro::Task<void> {
    auto serverSetup = co_await co_awaitTry(clientSession->setup(ClientSetup{
        .supportedVersions = {kVersionDraftCurrent}, .params = {}}));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close();
  }(clientSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, ServerSetupFail) {
  failServerSetup_ = true;
  clientSession_->start();
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession) -> folly::coro::Task<void> {
    auto serverSetup = co_await co_awaitTry(clientSession->setup(ClientSetup{
        .supportedVersions = {kVersionDraftCurrent},
        .params = {
            {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
              .asUint64 = kTestMaxSubscribeId}}}}));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close();
  }(clientSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  eventBase_.loop();
}
