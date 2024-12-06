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
#include <moxygen/test/TestUtils.h>

using namespace moxygen;

namespace {
using namespace moxygen;

const size_t kTestMaxSubscribeId = 2;

class MockControlVisitorBase {
 public:
  virtual ~MockControlVisitorBase() = default;
  virtual void onSubscribe(SubscribeRequest subscribeRequest) const = 0;
  virtual void onSubscribeUpdate(SubscribeUpdate subscribeUpdate) const = 0;
  virtual void onSubscribeDone(SubscribeDone subscribeDone) const = 0;
  virtual void onUnsubscribe(Unsubscribe unsubscribe) const = 0;
  virtual void onFetch(Fetch fetch) const = 0;
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

  MOCK_METHOD(void, onFetch, (Fetch), (const));
  void operator()(Fetch fetch) const override {
    onFetch(fetch);
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
      EXPECT_EQ(setup.params.at(0).asUint64, initialMaxSubscribeId_);
    }
    if (failServerSetup_) {
      return folly::makeTryWith(
          []() -> ServerSetup { throw std::runtime_error("failed"); });
    }
    return folly::Try<ServerSetup>(ServerSetup{
        .selectedVersion = negotiatedVersion_,
        .params = {
            {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
              .asUint64 = initialMaxSubscribeId_}}}});
  }

  void setupMoQSession();

 protected:
  folly::EventBase eventBase_;
  std::unique_ptr<proxygen::test::FakeSharedWebTransport> clientWt_;
  std::unique_ptr<proxygen::test::FakeSharedWebTransport> serverWt_;
  std::shared_ptr<MoQSession> clientSession_;
  std::shared_ptr<MoQSession> serverSession_;
  MockControlVisitor clientControl;
  MockControlVisitor serverControl;
  uint64_t negotiatedVersion_ = kVersionDraftCurrent;
  uint64_t initialMaxSubscribeId_{kTestMaxSubscribeId};
  bool failServerSetup_{false};
};

void MoQSessionTest::setupMoQSession() {
  clientSession_->start();
  serverSession_->start();
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession,
     uint64_t initialMaxSubscribeId) -> folly::coro::Task<void> {
    auto serverSetup = co_await clientSession->setup(ClientSetup{
        .supportedVersions = {kVersionDraftCurrent},
        .params = {
            {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
              .asUint64 = initialMaxSubscribeId}}}});

    EXPECT_EQ(serverSetup.selectedVersion, kVersionDraftCurrent);
    EXPECT_EQ(
        serverSetup.params.at(0).key,
        folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID));
    EXPECT_EQ(serverSetup.params.at(0).asUint64, initialMaxSubscribeId);
    clientSession->getEventBase()->terminateLoopSoon();
  }(clientSession_, initialMaxSubscribeId_)
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
} // namespace

TEST_F(MoQSessionTest, Setup) {
  setupMoQSession();
  clientSession_->close();
}

TEST_F(MoQSessionTest, Fetch) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto handle = co_await session->fetch(
        {SubscribeID(0),
         FullTrackName{TrackNamespace{{"foo"}}, "bar"},
         0,
         GroupOrder::OldestFirst,
         AbsoluteLocation{0, 0},
         AbsoluteLocation{0, 1},
         {}});
    EXPECT_TRUE(handle.hasValue());
    auto obj = co_await handle.value()->objects().next();
    EXPECT_NE(obj.value(), nullptr);
    EXPECT_EQ(
        *std::get_if<SubscribeID>(&obj.value()->header.trackIdentifier),
        SubscribeID(0));
    auto payload = co_await obj.value()->payload();
    EXPECT_EQ(payload->computeChainDataLength(), 100);
    obj = co_await handle.value()->objects().next();
    EXPECT_FALSE(obj.has_value());
    session->close();
  };
  EXPECT_CALL(serverControl, onFetch(testing::_))
      .WillOnce(testing::Invoke([this](Fetch fetch) {
        EXPECT_EQ(
            fetch.fullTrackName,
            FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
        serverSession_->fetchOk(
            {fetch.subscribeID,
             GroupOrder::OldestFirst,
             /*endOfTrack=*/0,
             AbsoluteLocation{100, 100},
             {}});
        serverSession_->publish(
            {fetch.subscribeID,
             fetch.start.group,
             /*subgroup=*/0,
             fetch.start.object,
             /*priority=*/0,
             ForwardPreference::Fetch,
             ObjectStatus::NORMAL,
             100},
            fetch.subscribeID,
            0,
            moxygen::test::makeBuf(100),
            true);
        serverSession_->closeFetchStream(fetch.subscribeID);
      }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchCleanupFromStreamFin) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session,
              std::shared_ptr<MoQSession> serverSession) mutable
      -> folly::coro::Task<void> {
    auto handle = co_await session->fetch(
        {SubscribeID(0),
         FullTrackName{TrackNamespace{{"foo"}}, "bar"},
         0,
         GroupOrder::OldestFirst,
         AbsoluteLocation{0, 0},
         AbsoluteLocation{0, 1},
         {}});
    EXPECT_TRUE(handle.hasValue());
    // publish here now we know FETCH_OK has been received at client
    serverSession->publish(
        {handle.value()->subscribeID(),
         /*group=*/0,
         /*subgroup=*/0,
         /*object=*/0,
         /*priority=*/0,
         ForwardPreference::Fetch,
         ObjectStatus::NORMAL,
         100},
        handle.value()->subscribeID(),
        0,
        moxygen::test::makeBuf(100),
        true);
    serverSession->closeFetchStream(handle.value()->subscribeID());

    auto obj = co_await handle.value()->objects().next();
    EXPECT_NE(obj.value(), nullptr);
    EXPECT_EQ(
        *std::get_if<SubscribeID>(&obj.value()->header.trackIdentifier),
        SubscribeID(0));
    auto payload = co_await obj.value()->payload();
    EXPECT_EQ(payload->computeChainDataLength(), 100);
    obj = co_await handle.value()->objects().next();
    EXPECT_FALSE(obj.has_value());
    session->close();
  };
  EXPECT_CALL(serverControl, onFetch(testing::_))
      .WillOnce(testing::Invoke([this](Fetch fetch) {
        EXPECT_EQ(
            fetch.fullTrackName,
            FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
        serverSession_->fetchOk(
            {fetch.subscribeID,
             GroupOrder::OldestFirst,
             /*endOfTrack=*/0,
             AbsoluteLocation{100, 100},
             {}});
      }));
  f(clientSession_, serverSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchError) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto handle = co_await session->fetch(
        {SubscribeID(0),
         FullTrackName{TrackNamespace{{"foo"}}, "bar"},
         0,
         GroupOrder::OldestFirst,
         AbsoluteLocation{0, 1},
         AbsoluteLocation{0, 0},
         {}});
    EXPECT_TRUE(handle.hasError());
    EXPECT_EQ(
        handle.error().errorCode,
        folly::to_underlying(FetchErrorCode::INVALID_RANGE));
    session->close();
  };
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchCancel) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> clientSession,
              std::shared_ptr<MoQSession> serverSession) mutable
      -> folly::coro::Task<void> {
    auto handle = co_await clientSession->fetch(
        {SubscribeID(0),
         FullTrackName{TrackNamespace{{"foo"}}, "bar"},
         0,
         GroupOrder::OldestFirst,
         AbsoluteLocation{0, 0},
         AbsoluteLocation{0, 2},
         {}});
    EXPECT_TRUE(handle.hasValue());
    auto subscribeID = handle.value()->subscribeID();
    clientSession->fetchCancel({subscribeID});
    co_await folly::coro::co_reschedule_on_current_executor;
    co_await folly::coro::co_reschedule_on_current_executor;
    co_await folly::coro::co_reschedule_on_current_executor;
    auto res = co_await folly::coro::co_awaitTry(serverSession->publish(
        {subscribeID,
         /*group=*/0,
         /*subgroup=*/0,
         /*object=*/1,
         /*priority=*/0,
         ForwardPreference::Fetch,
         ObjectStatus::NORMAL,
         100},
        subscribeID,
        0,
        moxygen::test::makeBuf(100),
        true));
    // publish after fetchCancel fails
    EXPECT_TRUE(res.hasException());
    clientSession->close();
  };
  EXPECT_CALL(serverControl, onFetch(testing::_))
      .WillOnce(testing::Invoke([this](Fetch fetch) {
        EXPECT_EQ(
            fetch.fullTrackName,
            FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
        serverSession_->fetchOk(
            {fetch.subscribeID,
             GroupOrder::OldestFirst,
             /*endOfTrack=*/0,
             AbsoluteLocation{100, 100},
             {}});
        serverSession_->publish(
            {fetch.subscribeID,
             fetch.start.group,
             /*subgroup=*/0,
             fetch.start.object,
             /*priority=*/0,
             ForwardPreference::Fetch,
             ObjectStatus::NORMAL,
             100},
            fetch.subscribeID,
            0,
            moxygen::test::makeBuf(100),
            true);
        // published 1 object
      }));
  f(clientSession_, serverSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchEarlyCancel) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> clientSession) mutable
      -> folly::coro::Task<void> {
    auto handle = co_await clientSession->fetch(
        {SubscribeID(0),
         FullTrackName{TrackNamespace{{"foo"}}, "bar"},
         0,
         GroupOrder::OldestFirst,
         AbsoluteLocation{0, 0},
         AbsoluteLocation{0, 2},
         {}});
    EXPECT_TRUE(handle.hasValue());
    auto subscribeID = handle.value()->subscribeID();
    // TODO: this no-ops right now so there's nothing to verify
    clientSession->fetchCancel({subscribeID});
    clientSession->close();
  };
  EXPECT_CALL(serverControl, onFetch(testing::_))
      .WillOnce(testing::Invoke([this](Fetch fetch) {
        EXPECT_EQ(
            fetch.fullTrackName,
            FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
        serverSession_->fetchOk(
            {fetch.subscribeID,
             GroupOrder::OldestFirst,
             /*endOfTrack=*/0,
             AbsoluteLocation{100, 100},
             {}});
      }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchBadLength) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto handle = co_await session->fetch(
        {SubscribeID(0),
         FullTrackName{TrackNamespace{{"foo"}}, "bar"},
         0,
         GroupOrder::OldestFirst,
         AbsoluteLocation{0, 0},
         AbsoluteLocation{0, 1},
         {}});
    // onSessionEnd races fetchOk, and the client gets a fetchError
    EXPECT_FALSE(handle.hasValue());
  };
  EXPECT_CALL(serverControl, onFetch(testing::_))
      .WillOnce(testing::Invoke([this](Fetch fetch) {
        EXPECT_EQ(
            fetch.fullTrackName,
            FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
        serverSession_->fetchOk(
            {fetch.subscribeID,
             GroupOrder::OldestFirst,
             /*endOfTrack=*/0,
             AbsoluteLocation{100, 100},
             {}});
        serverSession_->publish(
            {fetch.subscribeID,
             fetch.start.group,
             /*subgroup=*/0,
             fetch.start.object,
             /*priority=*/0,
             ForwardPreference::Fetch,
             ObjectStatus::NORMAL,
             100},
            fetch.subscribeID,
            0,
            moxygen::test::makeBuf(10),
            false);
        auto res = serverSession_->closeFetchStream(fetch.subscribeID);
        EXPECT_TRUE(res.hasException());
        // this should close the session too
      }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, BadFetchClose) {
  setupMoQSession();
  auto res = clientSession_->closeFetchStream(SubscribeID(1000));
  EXPECT_TRUE(res.hasException());
}

TEST_F(MoQSessionTest, FetchOverLimit) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    Fetch fetch{
        SubscribeID(0),
        FullTrackName{TrackNamespace{{"foo"}}, "bar"},
        0,
        GroupOrder::OldestFirst,
        AbsoluteLocation{0, 0},
        AbsoluteLocation{0, 1},
        {}};
    auto handle = co_await session->fetch(fetch);
    handle = co_await session->fetch(fetch);
    handle = co_await session->fetch(fetch);
    EXPECT_TRUE(handle.hasError());
  };
  EXPECT_CALL(serverControl, onFetch(testing::_))
      .WillOnce(testing::Invoke([this](Fetch fetch) {
        EXPECT_EQ(
            fetch.fullTrackName,
            FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
        serverSession_->fetchOk(
            {fetch.subscribeID,
             GroupOrder::OldestFirst,
             /*endOfTrack=*/0,
             AbsoluteLocation{100, 100},
             {}});
      }))
      .WillOnce(testing::Invoke([this](Fetch fetch) {
        EXPECT_EQ(
            fetch.fullTrackName,
            FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
        serverSession_->fetchOk(
            {fetch.subscribeID,
             GroupOrder::OldestFirst,
             /*endOfTrack=*/0,
             AbsoluteLocation{100, 100},
             {}});
      }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchBadID) {
  setupMoQSession();
  serverSession_->fetchOk(
      {SubscribeID(1000),
       GroupOrder::OldestFirst,
       /*endOfTrack=*/0,
       AbsoluteLocation{100, 100},
       {}});
  eventBase_.loopOnce();
  serverSession_->fetchError({SubscribeID(2000), 500, "local write failed"});
  eventBase_.loopOnce();
  // These are no-ops
}

// Missing Test Cases
// ===
// receive bidi stream on client
// getTrack by alias (subscribe with stream)
// getTrack with invalid alias and subscribe ID
// receive non-normal object
// onObjectPayload maps to non-existent object in TrackHandle
// subscribeUpdate/onSubscribeUpdate
// unsubscribe/onUnsubscribe
// onSubscribeOk/Error/Done with unknown ID
// onMaxSubscribeID with ID == 0 {no setup param}
// onFetchCancel with no publish data
// announce/unannounce/announceCancel/announceError/announceOk
// subscribeAnnounces/unsubscribeAnnounces
// GOAWAY
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
// uni stream or datagram before setup complete

TEST_F(MoQSessionTest, SetupTimeout) {
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

TEST_F(MoQSessionTest, InvalidVersion) {
  clientSession_->start();
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession) -> folly::coro::Task<void> {
    auto serverSetup = co_await co_awaitTry(clientSession->setup(
        ClientSetup{.supportedVersions = {0xfaceb001}, .params = {}}));
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
        .supportedVersions = {kVersionDraftCurrent}, .params = {}}));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close();
  }(clientSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, MaxSubscribeID) {
  setupMoQSession();
  [](std::shared_ptr<MoQSession> clientSession,
     std::shared_ptr<MoQSession> serverSession) -> folly::coro::Task<void> {
    SubscribeRequest sub{
        SubscribeID(0),
        TrackAlias(0),
        FullTrackName{TrackNamespace{{"foo"}}, "bar"},
        0,
        GroupOrder::OldestFirst,
        LocationType::LatestObject,
        folly::none,
        folly::none,
        {}};
    auto res = co_await clientSession->subscribe(sub);
    co_await folly::coro::co_reschedule_on_current_executor;
    // This is true because initial is 2 in this test case and we grant credit
    // every 50%.
    auto expectedSubId = 3;
    EXPECT_EQ(serverSession->maxSubscribeID(), expectedSubId);

    // subscribe again but this time we get a DONE
    res = co_await clientSession->subscribe(sub);
    co_await folly::coro::co_reschedule_on_current_executor;
    expectedSubId++;
    EXPECT_EQ(serverSession->maxSubscribeID(), expectedSubId);

    // subscribe three more times, last one should fail
    res = co_await clientSession->subscribe(sub);
    res = co_await clientSession->subscribe(sub);
    res = co_await clientSession->subscribe(sub);
    EXPECT_TRUE(res.hasError());
  }(clientSession_, serverSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  EXPECT_CALL(serverControl, onSubscribe(testing::_))
      .WillOnce(testing::Invoke([this](auto sub) {
        serverSession_->subscribeError(
            {sub.subscribeID, 400, "bad", folly::none});
      }))
      .WillOnce(testing::Invoke([this](auto sub) {
        serverSession_->subscribeOk(
            {sub.subscribeID,
             std::chrono::milliseconds(0),
             GroupOrder::OldestFirst,
             folly::none,
             {}});
        serverSession_->subscribeDone(
            {sub.subscribeID,
             SubscribeDoneStatusCode::TRACK_ENDED,
             "end of track",
             folly::none});
      }))
      .WillOnce(testing::Invoke([this](auto sub) {
        serverSession_->subscribeOk(
            {sub.subscribeID,
             std::chrono::milliseconds(0),
             GroupOrder::OldestFirst,
             folly::none,
             {}});
      }))
      .WillOnce(testing::Invoke([this](auto sub) {
        serverSession_->subscribeOk(
            {sub.subscribeID,
             std::chrono::milliseconds(0),
             GroupOrder::OldestFirst,
             folly::none,
             {}});
      }));

  EXPECT_CALL(clientControl, onSubscribeDone(testing::_));
  eventBase_.loop();
}
