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

class MockControlVisitorBase {
 public:
  virtual ~MockControlVisitorBase() = default;
  virtual void onClientSetup(ClientSetup clientSetup) const = 0;
  virtual void onServerSetup(ServerSetup serverSetup) const = 0;
  virtual void onSubscribe(SubscribeRequest subscribeRequest) const = 0;
  virtual void onSubscribeUpdate(SubscribeUpdate subscribeUpdate) const = 0;
  virtual void onSubscribeDone(SubscribeDone subscribeDone) const = 0;
  virtual void onMaxSubscribeId(MaxSubscribeId maxSubscribeId) const = 0;
  virtual void onUnsubscribe(Unsubscribe unsubscribe) const = 0;
  virtual void onFetch(Fetch fetch) const = 0;
  virtual void onFetchCancel(FetchCancel fetchCancel) const = 0;
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
  MOCK_METHOD(void, onClientSetup, (ClientSetup), (const));
  void operator()(ClientSetup setup) const override {
    onClientSetup(setup);
  }
  MOCK_METHOD(void, onServerSetup, (ServerSetup), (const));
  void operator()(ServerSetup setup) const override {
    onServerSetup(setup);
  }

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

  MOCK_METHOD(void, onFetch, (Fetch), (const));
  void operator()(Fetch fetch) const override {
    onFetch(fetch);
  }

  MOCK_METHOD(void, onFetchCancel, (FetchCancel), (const));
  void operator()(FetchCancel fetchCancel) const override {
    onFetchCancel(fetchCancel);
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

class MoQSessionTest : public testing::Test {
 public:
  MoQSessionTest() {
    std::tie(clientWt_, serverWt_) =
        proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
    clientSession_ = std::make_shared<MoQSession>(
        MoQControlCodec::Direction::CLIENT, clientWt_.get(), &eventBase_);
    serverWt_->setPeerHandler(clientSession_.get());

    serverSession_ = std::make_shared<MoQSession>(
        MoQControlCodec::Direction::SERVER, serverWt_.get(), &eventBase_);
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

 protected:
  folly::EventBase eventBase_;
  std::unique_ptr<proxygen::test::FakeSharedWebTransport> clientWt_;
  std::unique_ptr<proxygen::test::FakeSharedWebTransport> serverWt_;
  std::shared_ptr<MoQSession> clientSession_;
  std::shared_ptr<MoQSession> serverSession_;
  MockControlVisitor clientControl;
  MockControlVisitor serverControl;
};
} // namespace

TEST_F(MoQSessionTest, Setup) {
  clientSession_->start();
  serverSession_->start();
  eventBase_.loopOnce();
  const size_t kTestMaxSubscribeId = 100;
  clientSession_->setup(ClientSetup{
      .supportedVersions = {kVersionDraftCurrent},
      .params = {
          {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
            .asUint64 = kTestMaxSubscribeId}}}});

  EXPECT_CALL(serverControl, onClientSetup(testing::_))
      .WillOnce(testing::Invoke([kTestMaxSubscribeId, this](ClientSetup setup) {
        EXPECT_EQ(setup.supportedVersions[0], kVersionDraftCurrent);
        EXPECT_EQ(setup.params.size(), 1);
        EXPECT_EQ(
            setup.params.at(0).key,
            folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID));
        EXPECT_EQ(setup.params.at(0).asUint64, kTestMaxSubscribeId);
        serverSession_->setup(ServerSetup{
            .selectedVersion = kVersionDraftCurrent,
            .params = {
                {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
                  .asUint64 = kTestMaxSubscribeId}}}});
      }));
  EXPECT_CALL(clientControl, onServerSetup(testing::_))
      .WillOnce(testing::Invoke([kTestMaxSubscribeId, this](ServerSetup setup) {
        EXPECT_EQ(setup.selectedVersion, kVersionDraftCurrent);
        EXPECT_EQ(
            setup.params.at(0).key,
            folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID));
        EXPECT_EQ(setup.params.at(0).asUint64, kTestMaxSubscribeId);
        clientSession_->close();
      }));
  this->controlLoop(*serverSession_, serverControl)
      .scheduleOn(&eventBase_)
      .start();
  this->controlLoop(*clientSession_, clientControl)
      .scheduleOn(&eventBase_)
      .start();
  eventBase_.loop();
}

// receive bidi stream on client
