/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQSession.h"
#include <folly/coro/BlockingWait.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>
#include <moxygen/test/Mocks.h>
#include <moxygen/test/TestUtils.h>

using namespace moxygen;

namespace {
using namespace moxygen;

const size_t kTestMaxSubscribeId = 2;

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

    clientSubscriberStatsCallback_ = std::make_shared<MockSubscriberStats>();
    clientSession_->setSubscriberStatsCallback(clientSubscriberStatsCallback_);

    clientPublisherStatsCallback_ = std::make_shared<MockPublisherStats>();
    clientSession_->setPublisherStatsCallback(clientPublisherStatsCallback_);

    serverSubscriberStatsCallback_ = std::make_shared<MockSubscriberStats>();
    serverSession_->setSubscriberStatsCallback(serverSubscriberStatsCallback_);

    serverPublisherStatsCallback_ = std::make_shared<MockPublisherStats>();
    serverSession_->setPublisherStatsCallback(serverPublisherStatsCallback_);
  }

  void SetUp() override {}

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
  std::shared_ptr<MockPublisher> clientPublisher{
      std::make_shared<MockPublisher>()};
  std::shared_ptr<MockPublisher> serverPublisher{
      std::make_shared<MockPublisher>()};
  uint64_t negotiatedVersion_ = kVersionDraftCurrent;
  uint64_t initialMaxSubscribeId_{kTestMaxSubscribeId};
  bool failServerSetup_{false};
  std::shared_ptr<MockSubscriberStats> clientSubscriberStatsCallback_;
  std::shared_ptr<MockPublisherStats> clientPublisherStatsCallback_;
  std::shared_ptr<MockSubscriberStats> serverSubscriberStatsCallback_;
  std::shared_ptr<MockPublisherStats> serverPublisherStatsCallback_;
};

namespace {
// GCC barfs when using struct brace initializers inside a coroutine?
// Helper function to make ClientSetup with MAX_SUBSCRIBE_ID
moxygen::ClientSetup getClientSetup(uint64_t initialMaxSubscribeId) {
  return ClientSetup{
      .supportedVersions = {kVersionDraftCurrent},
      .params = {
          {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
            .asUint64 = initialMaxSubscribeId}}}};
}
} // namespace

void MoQSessionTest::setupMoQSession() {
  clientSession_->setPublishHandler(clientPublisher);
  clientSession_->start();
  serverSession_->setPublishHandler(serverPublisher);
  serverSession_->start();
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession,
     uint64_t initialMaxSubscribeId) -> folly::coro::Task<void> {
    auto serverSetup =
        co_await clientSession->setup(getClientSetup(initialMaxSubscribeId));

    EXPECT_EQ(serverSetup.selectedVersion, kVersionDraftCurrent);
    EXPECT_EQ(
        serverSetup.params.at(0).key,
        folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID));
    EXPECT_EQ(serverSetup.params.at(0).asUint64, initialMaxSubscribeId);
    clientSession->getEventBase()->terminateLoopSoon();
  }(clientSession_, initialMaxSubscribeId_)
                                            .scheduleOn(&eventBase_)
                                            .start();
  eventBase_.loop();
}
} // namespace

TEST_F(MoQSessionTest, Setup) {
  setupMoQSession();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

MATCHER_P(HasChainDataLengthOf, n, "") {
  return arg->computeChainDataLength() == uint64_t(n);
}

namespace {
// GCC barfs when using struct brace initializers inside a coroutine?
// Helper function to make a Fetch request
Fetch getFetch(AbsoluteLocation start, AbsoluteLocation end) {
  return Fetch(
      SubscribeID(0),
      FullTrackName{TrackNamespace{{"foo"}}, "bar"},
      0,
      GroupOrder::OldestFirst,
      start,
      end);
}
} // namespace

TEST_F(MoQSessionTest, Fetch) {
  setupMoQSession();
  auto f = [this](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    folly::coro::Baton baton;
    EXPECT_CALL(
        *fetchCallback, object(0, 0, 0, HasChainDataLengthOf(100), true))
        .WillOnce(testing::Invoke([&] {
          baton.post();
          return folly::unit;
        }));
    EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess());
    auto res = co_await session->fetch(getFetch({0, 0}, {0, 1}), fetchCallback);
    EXPECT_FALSE(res.hasError());
    co_await baton;
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [this](Fetch fetch, auto fetchPub)
              -> folly::coro::Task<Publisher::FetchResult> {
            auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
            EXPECT_NE(standalone, nullptr);
            EXPECT_EQ(
                fetch.fullTrackName,
                FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
            fetchPub->object(
                standalone->start.group,
                /*subgroupID=*/0,
                standalone->start.object,
                moxygen::test::makeBuf(100),
                /*finFetch=*/true);
            EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess());
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/0,
                AbsoluteLocation{100, 100},
                {}});
          }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, JoiningFetch) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    SubscribeRequest sub{
        SubscribeID(0),
        TrackAlias(0),
        FullTrackName{TrackNamespace{{"foo"}}, "bar"},
        0,
        GroupOrder::OldestFirst,
        LocationType::LatestObject,
        folly::none,
        0,
        {}};
    auto trackPublisher1 =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();
    folly::coro::Baton subBaton;
    EXPECT_CALL(*trackPublisher1, datagram(testing::_, testing::_))
        .WillOnce(testing::Invoke([&](auto header, auto) {
          EXPECT_EQ(header.length, 11);
          return folly::unit;
        }));
    EXPECT_CALL(*trackPublisher1, subscribeDone(testing::_))
        .WillOnce(testing::Invoke([&] {
          subBaton.post();
          return folly::unit;
        }));
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    folly::coro::Baton fetchBaton;
    EXPECT_CALL(
        *fetchCallback, object(0, 0, 0, HasChainDataLengthOf(100), true))
        .WillOnce(testing::Invoke([&] {
          fetchBaton.post();
          return folly::unit;
        }));
    auto res = co_await session->join(
        sub, trackPublisher1, 1, 129, GroupOrder::Default, {}, fetchCallback);
    EXPECT_FALSE(res.subscribeResult.hasError());
    EXPECT_FALSE(res.fetchResult.hasError());
    co_await subBaton;
    co_await fetchBaton;
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, subscribe(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](auto sub,
             auto pub) -> folly::coro::Task<Publisher::SubscribeResult> {
            pub->datagram(
                {sub.trackAlias, 0, 0, 1, 0, ObjectStatus::NORMAL, 11},
                folly::IOBuf::copyBuffer("hello world"));
            pub->subscribeDone(
                {sub.subscribeID,
                 SubscribeDoneStatusCode::TRACK_ENDED,
                 0,
                 "end of track",
                 AbsoluteLocation{0, 1}});
            co_return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.subscribeID,
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                AbsoluteLocation{0, 0},
                {}});
          }));
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](Fetch fetch,
             auto fetchPub) -> folly::coro::Task<Publisher::FetchResult> {
            auto joining = std::get_if<JoiningFetch>(&fetch.args);
            EXPECT_NE(joining, nullptr);
            EXPECT_EQ(
                fetch.fullTrackName,
                FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
            fetchPub->object(
                /*groupID=*/0,
                /*subgroupID=*/0,
                /*objectID=*/0,
                moxygen::test::makeBuf(100),
                /*finFetch=*/true);
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/0,
                AbsoluteLocation{0, 0},
                {}});
          }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, BadJoiningFetch) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    auto res = co_await session->fetch(
        Fetch(SubscribeID(0), SubscribeID(17), 1, 128, GroupOrder::Default),
        fetchCallback);
    EXPECT_TRUE(res.hasError());
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchCleanupFromStreamFin) {
  setupMoQSession();
  std::shared_ptr<FetchConsumer> fetchPub;
  auto f = [this](
               std::shared_ptr<MoQSession> session,
               std::shared_ptr<MoQSession> serverSession,
               std::shared_ptr<FetchConsumer>& fetchPub) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess());
    auto res = co_await session->fetch(getFetch({0, 0}, {0, 1}), fetchCallback);
    EXPECT_FALSE(res.hasError());
    // publish here now we know FETCH_OK has been received at client
    XCHECK(fetchPub);
    fetchPub->object(
        /*groupID=*/0,
        /*subgroupID=*/0,
        /*objectID=*/0,
        moxygen::test::makeBuf(100),
        /*finFetch=*/true);
    folly::coro::Baton baton;
    EXPECT_CALL(
        *fetchCallback, object(0, 0, 0, HasChainDataLengthOf(100), true))
        .WillOnce(testing::Invoke([&] {
          baton.post();
          return folly::unit;
        }));
    co_await baton;
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [&fetchPub, this](Fetch fetch, auto inFetchPub)
              -> folly::coro::Task<Publisher::FetchResult> {
            EXPECT_EQ(
                fetch.fullTrackName,
                FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
            fetchPub = std::move(inFetchPub);
            EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess());
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/0,
                AbsoluteLocation{100, 100},
                {}});
          }));
  f(clientSession_, serverSession_, fetchPub).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchError) {
  setupMoQSession();
  auto f = [this](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    EXPECT_CALL(
        *clientSubscriberStatsCallback_,
        onFetchError(folly::to_underlying(FetchErrorCode::INVALID_RANGE)));
    auto res = co_await session->fetch(getFetch({0, 2}, {0, 1}), fetchCallback);
    EXPECT_TRUE(res.hasError());
    EXPECT_EQ(
        res.error().errorCode,
        folly::to_underlying(FetchErrorCode::INVALID_RANGE));
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(
      *serverPublisherStatsCallback_,
      onFetchError(folly::to_underlying(FetchErrorCode::INVALID_RANGE)));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchPublisherError) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    auto res = co_await session->fetch(getFetch({0, 0}, {0, 1}), fetchCallback);
    EXPECT_TRUE(res.hasError());
    EXPECT_EQ(
        res.error().errorCode,
        folly::to_underlying(FetchErrorCode::TRACK_NOT_EXIST));
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](Fetch fetch, auto) -> folly::coro::Task<Publisher::FetchResult> {
            co_return folly::makeUnexpected(FetchError{
                fetch.subscribeID,
                folly::to_underlying(FetchErrorCode::TRACK_NOT_EXIST),
                "Bad trackname"});
          }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchPublisherThrow) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    auto res = co_await session->fetch(getFetch({0, 0}, {0, 1}), fetchCallback);
    EXPECT_TRUE(res.hasError());
    EXPECT_EQ(res.error().errorCode, 500);
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](Fetch fetch, auto) -> folly::coro::Task<Publisher::FetchResult> {
            throw std::runtime_error("panic!");
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/0,
                AbsoluteLocation{100, 100},
                {}});
          }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchCancel) {
  setupMoQSession();
  std::shared_ptr<FetchConsumer> fetchPub;
  auto f = [this](
               std::shared_ptr<MoQSession> clientSession,
               std::shared_ptr<MoQSession> serverSession,
               std::shared_ptr<FetchConsumer>& fetchPub) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    SubscribeID subscribeID(0);
    EXPECT_CALL(
        *fetchCallback, object(0, 0, 0, HasChainDataLengthOf(100), false))
        .WillOnce(testing::Return(folly::unit));
    // TODO: fetchCancel removes the callback - should it also deliver a
    // reset() call to the callback?
    // EXPECT_CALL(*fetchCallback, reset(ResetStreamErrorCode::CANCELLED));
    EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess());
    auto res =
        co_await clientSession->fetch(getFetch({0, 0}, {0, 2}), fetchCallback);
    EXPECT_FALSE(res.hasError());
    res.value()->fetchCancel();
    co_await folly::coro::co_reschedule_on_current_executor;
    co_await folly::coro::co_reschedule_on_current_executor;
    co_await folly::coro::co_reschedule_on_current_executor;
    XCHECK(fetchPub);
    auto res2 = fetchPub->object(
        /*groupID=*/0,
        /*subgroupID=*/0,
        /*objectID=*/1,
        moxygen::test::makeBuf(100),
        /*finFetch=*/true);
    // publish after fetchCancel fails
    EXPECT_TRUE(res2.hasError());
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [&fetchPub, this](Fetch fetch, auto inFetchPub)
              -> folly::coro::Task<Publisher::FetchResult> {
            auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
            EXPECT_NE(standalone, nullptr);
            EXPECT_EQ(
                fetch.fullTrackName,
                FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
            fetchPub = std::move(inFetchPub);
            fetchPub->object(
                standalone->start.group,
                /*subgroupID=*/0,
                standalone->start.object,
                moxygen::test::makeBuf(100),
                false);
            // published 1 object
            EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess());
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/0,
                AbsoluteLocation{100, 100},
                {}});
          }));
  f(clientSession_, serverSession_, fetchPub).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchEarlyCancel) {
  setupMoQSession();
  auto f = [this](std::shared_ptr<MoQSession> clientSession) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    SubscribeID subscribeID(0);
    EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess());
    auto res =
        co_await clientSession->fetch(getFetch({0, 0}, {0, 2}), fetchCallback);
    EXPECT_FALSE(res.hasError());
    // TODO: this no-ops right now so there's nothing to verify
    res.value()->fetchCancel();
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [this](
              Fetch fetch, auto) -> folly::coro::Task<Publisher::FetchResult> {
            EXPECT_EQ(
                fetch.fullTrackName,
                FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
            EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess());
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
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
  auto f = [this](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback =
        std::make_shared<testing::NiceMock<MockFetchConsumer>>();
    EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess());
    auto res = co_await session->fetch(getFetch({0, 0}, {0, 1}), fetchCallback);
    EXPECT_FALSE(res.hasError());
    // FETCH_OK comes but the FETCH stream is reset and we timeout waiting
    // for a new object.
    auto contract = folly::coro::makePromiseContract<folly::Unit>();
    ON_CALL(
        *fetchCallback,
        beginObject(testing::_, testing::_, testing::_, testing::_, testing::_))
        .WillByDefault([&] {
          contract.first.setValue();
          return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
        });
    folly::EventBaseThreadTimekeeper tk(*session->getEventBase());
    EXPECT_THROW(
        co_await folly::coro::timeout(
            std::move(contract.second), std::chrono::milliseconds(100), &tk),
        folly::FutureTimeout);
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [this](Fetch fetch, auto fetchPub)
              -> folly::coro::Task<Publisher::FetchResult> {
            auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
            EXPECT_NE(standalone, nullptr);
            EXPECT_EQ(
                fetch.fullTrackName,
                FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
            auto objPub = fetchPub->beginObject(
                standalone->start.group,
                /*subgroupID=*/0,
                standalone->start.object,
                100,
                moxygen::test::makeBuf(10));
            // this should close the session too
            fetchPub->endOfFetch();
            EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess());
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/0,
                AbsoluteLocation{100, 100},
                {}});
          }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, FetchOverLimit) {
  setupMoQSession();
  auto f = [this](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto fetchCallback1 =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    auto fetchCallback2 =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    auto fetchCallback3 =
        std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    Fetch fetch = getFetch({0, 0}, {0, 1});
    EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess()).Times(2);
    auto res = co_await session->fetch(fetch, fetchCallback1);
    res = co_await session->fetch(fetch, fetchCallback2);
    EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchError(500));
    res = co_await session->fetch(fetch, fetchCallback3);
    EXPECT_TRUE(res.hasError());
  };
  EXPECT_CALL(*serverPublisher, fetch(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [this](
              Fetch fetch, auto) -> folly::coro::Task<Publisher::FetchResult> {
            EXPECT_EQ(
                fetch.fullTrackName,
                FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
            EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess());
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/0,
                AbsoluteLocation{100, 100},
                {}});
          }))
      .WillOnce(testing::Invoke(
          [this](
              Fetch fetch, auto) -> folly::coro::Task<Publisher::FetchResult> {
            EXPECT_EQ(
                fetch.fullTrackName,
                FullTrackName({TrackNamespace{{"foo"}}, "bar"}));
            EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess());
            co_return std::make_shared<MockFetchHandle>(FetchOk{
                fetch.subscribeID,
                GroupOrder::OldestFirst,
                /*endOfTrack=*/0,
                AbsoluteLocation{100, 100},
                {}});
          }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, TrackStatus) {
  setupMoQSession();
  eventBase_.loopOnce();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    auto res = co_await session->trackStatus(
        {FullTrackName{TrackNamespace{{"foo"}}, "bar"}});
    EXPECT_EQ(res.statusCode, TrackStatusCode::IN_PROGRESS);
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, trackStatus(testing::_))
      .WillOnce(testing::Invoke(
          [](TrackStatusRequest request)
              -> folly::coro::Task<Publisher::TrackStatusResult> {
            co_return Publisher::TrackStatusResult{
                request.fullTrackName,
                TrackStatusCode::IN_PROGRESS,
                AbsoluteLocation{}};
          }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
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
    ClientSetup setup;
    setup.supportedVersions.push_back(kVersionDraftCurrent);
    auto serverSetup = co_await co_awaitTry(clientSession->setup(setup));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  }(clientSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, InvalidVersion) {
  clientSession_->start();
  eventBase_.loopOnce();
  [](std::shared_ptr<MoQSession> clientSession) -> folly::coro::Task<void> {
    ClientSetup setup;
    setup.supportedVersions.push_back(0xfaceb001);
    auto serverSetup = co_await co_awaitTry(clientSession->setup(setup));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
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
    ClientSetup setup;
    setup.supportedVersions.push_back(kVersionDraftCurrent);
    auto serverSetup = co_await co_awaitTry(clientSession->setup(setup));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
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
    ClientSetup setup;
    setup.supportedVersions.push_back(kVersionDraftCurrent);
    auto serverSetup = co_await co_awaitTry(clientSession->setup(setup));
    EXPECT_TRUE(serverSetup.hasException());
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  }(clientSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  eventBase_.loop();
}

TEST_F(MoQSessionTest, MaxSubscribeID) {
  setupMoQSession();
  [](std::shared_ptr<MoQSession> clientSession,
     std::shared_ptr<MoQSession> serverSession,
     std::shared_ptr<MockSubscriberStats> clientSubscriberStatsCallback)
      -> folly::coro::Task<void> {
    SubscribeRequest sub{
        SubscribeID(0),
        TrackAlias(0),
        FullTrackName{TrackNamespace{{"foo"}}, "bar"},
        0,
        GroupOrder::OldestFirst,
        LocationType::LatestObject,
        folly::none,
        0,
        {}};
    auto trackPublisher1 =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();
    auto trackPublisher2 =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();
    auto trackPublisher3 =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();
    EXPECT_CALL(*clientSubscriberStatsCallback, onSubscribeError(400));
    auto res = co_await clientSession->subscribe(sub, trackPublisher1);
    co_await folly::coro::co_reschedule_on_current_executor;
    // This is true because initial is 2 in this test case and we grant credit
    // every 50%.
    auto expectedSubId = 3;
    EXPECT_EQ(serverSession->maxSubscribeID(), expectedSubId);

    // subscribe again but this time we get a DONE
    EXPECT_CALL(*trackPublisher2, subscribeDone(testing::_))
        .WillOnce(testing::Return(folly::unit));
    EXPECT_CALL(*clientSubscriberStatsCallback, onSubscribeSuccess());
    res = co_await clientSession->subscribe(sub, trackPublisher2);
    co_await folly::coro::co_reschedule_on_current_executor;
    expectedSubId++;
    EXPECT_EQ(serverSession->maxSubscribeID(), expectedSubId);

    // subscribe three more times, last one should fail, the first two will get
    // subscribeDone via the session closure
    EXPECT_CALL(*trackPublisher3, subscribeDone(testing::_))
        .WillOnce(testing::Return(folly::unit))
        .WillOnce(testing::Return(folly::unit));

    EXPECT_CALL(*clientSubscriberStatsCallback, onSubscribeSuccess()).Times(2);
    res = co_await clientSession->subscribe(sub, trackPublisher3);
    res = co_await clientSession->subscribe(sub, trackPublisher3);
    EXPECT_CALL(*clientSubscriberStatsCallback, onSubscribeError(500));
    res = co_await clientSession->subscribe(sub, trackPublisher3);
    EXPECT_TRUE(res.hasError());
  }(clientSession_, serverSession_, clientSubscriberStatsCallback_)
             .scheduleOn(&eventBase_)
             .start();
  EXPECT_CALL(*serverPublisher, subscribe(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [this](
              auto sub, auto) -> folly::coro::Task<Publisher::SubscribeResult> {
            EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeError(400));
            co_return folly::makeUnexpected(
                SubscribeError{sub.subscribeID, 400, "bad", folly::none});
          }))
      .WillOnce(testing::Invoke(
          [this](auto sub, auto pub)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeSuccess());
            eventBase_.add([pub, sub] {
              pub->subscribeDone(
                  {sub.subscribeID,
                   SubscribeDoneStatusCode::TRACK_ENDED,
                   0,
                   "end of track",
                   folly::none});
            });
            co_return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.subscribeID,
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                folly::none,
                {}});
          }))
      .WillOnce(testing::Invoke(
          [this](
              auto sub, auto) -> folly::coro::Task<Publisher::SubscribeResult> {
            EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeSuccess());
            co_return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.subscribeID,
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                folly::none,
                {}});
          }))
      .WillOnce(testing::Invoke(
          [this](
              auto sub, auto) -> folly::coro::Task<Publisher::SubscribeResult> {
            EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeSuccess());
            co_return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.subscribeID,
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                folly::none,
                {}});
          }));

  eventBase_.loop();
}

TEST_F(MoQSessionTest, SubscribeDoneStreamCount) {
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
        0,
        {}};
    auto trackPublisher1 =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();
    auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
    auto sg2 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
    EXPECT_CALL(*trackPublisher1, beginSubgroup(0, 0, 0))
        .WillOnce(testing::Return(sg1));
    EXPECT_CALL(*trackPublisher1, beginSubgroup(0, 1, 0))
        .WillOnce(testing::Return(sg2));
    EXPECT_CALL(*sg1, object(0, testing::_, true))
        .WillOnce(testing::Return(folly::unit));
    EXPECT_CALL(*sg2, object(1, testing::_, false))
        .WillOnce(testing::Return(folly::unit));
    EXPECT_CALL(*sg2, object(2, testing::_, true))
        .WillOnce(testing::Return(folly::unit));
    folly::coro::Baton baton;
    EXPECT_CALL(*trackPublisher1, subscribeDone(testing::_))
        .WillOnce(testing::Invoke([&] {
          baton.post();
          return folly::unit;
        }));
    auto res = co_await clientSession->subscribe(sub, trackPublisher1);
    co_await baton;
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  }(clientSession_, serverSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  EXPECT_CALL(*serverPublisher, subscribe(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [this](auto sub, auto pub)
              -> folly::coro::Task<Publisher::SubscribeResult> {
            eventBase_.add([pub, sub] {
              pub->objectStream(
                  {sub.trackAlias,
                   0,
                   0,
                   0,
                   0,
                   ObjectStatus::NORMAL,
                   folly::none},
                  moxygen::test::makeBuf(10));
              auto sgp = pub->beginSubgroup(0, 1, 0).value();
              sgp->object(1, moxygen::test::makeBuf(10));
              sgp->object(2, moxygen::test::makeBuf(10), true);
              pub->subscribeDone(
                  {sub.subscribeID,
                   SubscribeDoneStatusCode::TRACK_ENDED,
                   2, // it's set by the session anyways
                   "end of track",
                   folly::none});
            });
            co_return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.subscribeID,
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                folly::none,
                {}});
          }));

  eventBase_.loop();
}

TEST_F(MoQSessionTest, SubscribeDoneFromSubscribe) {
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
        0,
        {}};
    auto trackPublisher1 =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();
    folly::coro::Baton baton;
    EXPECT_CALL(*trackPublisher1, subscribeDone(testing::_))
        .WillOnce(testing::Invoke([&] {
          baton.post();
          return folly::unit;
        }));
    auto res = co_await clientSession->subscribe(sub, trackPublisher1);
    co_await baton;
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  }(clientSession_, serverSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  EXPECT_CALL(*serverPublisher, subscribe(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](auto sub,
             auto pub) -> folly::coro::Task<Publisher::SubscribeResult> {
            pub->subscribeDone(
                {sub.subscribeID,
                 SubscribeDoneStatusCode::TRACK_ENDED,
                 0, // it's set by the session anyways
                 "end of track",
                 folly::none});
            co_return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.subscribeID,
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                folly::none,
                {}});
          }));

  eventBase_.loop();
}

TEST_F(MoQSessionTest, SubscribeDoneAPIErrors) {
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
        0,
        {}};
    auto trackPublisher1 =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();
    folly::coro::Baton baton;
    EXPECT_CALL(*trackPublisher1, subscribeDone(testing::_))
        .WillOnce(testing::Invoke([&] {
          baton.post();
          return folly::unit;
        }));
    auto res = co_await clientSession->subscribe(sub, trackPublisher1);
    co_await baton;
    clientSession->close(SessionCloseErrorCode::NO_ERROR);
  }(clientSession_, serverSession_)
                                                       .scheduleOn(&eventBase_)
                                                       .start();
  EXPECT_CALL(*serverPublisher, subscribe(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](auto sub,
             auto pub) -> folly::coro::Task<Publisher::SubscribeResult> {
            pub->subscribeDone(
                {sub.subscribeID,
                 SubscribeDoneStatusCode::TRACK_ENDED,
                 0, // it's set by the session anyways
                 "end of track",
                 folly::none});
            // All these APIs fail after SUBSCRIBE_DONE
            EXPECT_EQ(
                pub->beginSubgroup(1, 1, 1).error().code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                pub->awaitStreamCredit().error().code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                pub->datagram(
                       {sub.trackAlias,
                        2,
                        2,
                        2,
                        2,
                        ObjectStatus::NORMAL,
                        folly::none},
                       moxygen::test::makeBuf(10))
                    .error()
                    .code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                pub->subscribeDone({sub.subscribeID,
                                    SubscribeDoneStatusCode::TRACK_ENDED,
                                    0, // it's set by the session anyways
                                    "end of track",
                                    folly::none})
                    .error()
                    .code,
                MoQPublishError::API_ERROR);
            co_return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.subscribeID,
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                folly::none,
                {}});
          }));

  eventBase_.loop();
}

TEST_F(MoQSessionTest, Datagrams) {
  setupMoQSession();
  auto f = [](std::shared_ptr<MoQSession> session) mutable
      -> folly::coro::Task<void> {
    SubscribeRequest sub{
        SubscribeID(0),
        TrackAlias(0),
        FullTrackName{TrackNamespace{{"foo"}}, "bar"},
        0,
        GroupOrder::OldestFirst,
        LocationType::LatestObject,
        folly::none,
        0,
        {}};
    auto trackPublisher1 =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();
    folly::coro::Baton subBaton;
    {
      testing::InSequence enforceOrder;
      EXPECT_CALL(*trackPublisher1, datagram(testing::_, testing::_))
          .WillOnce(testing::Invoke([&](auto header, auto) {
            EXPECT_EQ(header.length, 11);
            return folly::unit;
          }));
      EXPECT_CALL(*trackPublisher1, datagram(testing::_, testing::_))
          .WillOnce(testing::Invoke([&](auto header, auto) {
            EXPECT_EQ(header.status, ObjectStatus::OBJECT_NOT_EXIST);
            return folly::unit;
          }));
    }
    EXPECT_CALL(*trackPublisher1, subscribeDone(testing::_))
        .WillOnce(testing::Invoke([&] {
          subBaton.post();
          return folly::unit;
        }));
    auto res = co_await session->subscribe(sub, trackPublisher1);
    EXPECT_FALSE(res.hasError());
    co_await subBaton;
    session->close(SessionCloseErrorCode::NO_ERROR);
  };
  EXPECT_CALL(*serverPublisher, subscribe(testing::_, testing::_))
      .WillOnce(testing::Invoke(
          [](auto sub,
             auto pub) -> folly::coro::Task<Publisher::SubscribeResult> {
            pub->datagram(
                {sub.trackAlias, 0, 0, 1, 0, ObjectStatus::NORMAL, 11},
                folly::IOBuf::copyBuffer("hello world"));
            pub->datagram(
                {sub.trackAlias,
                 0,
                 0,
                 2,
                 0,
                 ObjectStatus::OBJECT_NOT_EXIST,
                 folly::none},
                nullptr);
            pub->subscribeDone(
                {sub.subscribeID,
                 SubscribeDoneStatusCode::TRACK_ENDED,
                 0,
                 "end of track",
                 AbsoluteLocation{0, 2}});
            co_return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
                sub.subscribeID,
                std::chrono::milliseconds(0),
                GroupOrder::OldestFirst,
                AbsoluteLocation{0, 0},
                {}});
          }));
  f(clientSession_).scheduleOn(&eventBase_).start();
  eventBase_.loop();
}
