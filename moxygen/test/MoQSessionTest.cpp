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
#include <moxygen/test/TestHelpers.h>
#include <moxygen/test/TestUtils.h>

using namespace moxygen;

namespace {
using namespace moxygen;
using testing::_;

const size_t kTestMaxSubscribeId = 2;
const FullTrackName kTestTrackName{TrackNamespace{{"foo"}}, "bar"};

MATCHER_P(HasChainDataLengthOf, n, "") {
  return arg->computeChainDataLength() == uint64_t(n);
}

std::shared_ptr<MockFetchHandle> makeFetchOkResult(
    const Fetch& fetch,
    const AbsoluteLocation& location) {
  return std::make_shared<MockFetchHandle>(FetchOk{
      fetch.subscribeID,
      GroupOrder::OldestFirst,
      /*endOfTrack=*/0,
      location,
      {}});
}

auto makeSubscribeOkResult(
    const SubscribeRequest& sub,
    const folly::Optional<AbsoluteLocation>& latest = folly::none) {
  return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
      sub.subscribeID,
      std::chrono::milliseconds(0),
      GroupOrder::OldestFirst,
      latest,
      {}});
}

Publisher::SubscribeAnnouncesResult makeSubscribeAnnouncesOkResult(
    const auto& subAnn) {
  return std::make_shared<MockSubscribeAnnouncesHandle>(
      SubscribeAnnouncesOk({subAnn.trackNamespacePrefix}));
}

class MoQSessionTest : public testing::Test,
                       public MoQSession::ServerSetupCallback {
 public:
  void SetUp() override {
    std::tie(clientWt_, serverWt_) =
        proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
    clientSession_ = std::make_shared<MoQSession>(clientWt_.get(), &eventBase_);
    serverWt_->setPeerHandler(clientSession_.get());

    serverSession_ =
        std::make_shared<MoQSession>(serverWt_.get(), *this, &eventBase_);
    clientWt_->setPeerHandler(serverSession_.get());

    fetchCallback_ = std::make_shared<testing::StrictMock<MockFetchConsumer>>();
    subscribeCallback_ =
        std::make_shared<testing::StrictMock<MockTrackConsumer>>();

    clientSubscriberStatsCallback_ = std::make_shared<MockSubscriberStats>();
    clientSession_->setSubscriberStatsCallback(clientSubscriberStatsCallback_);

    clientPublisherStatsCallback_ = std::make_shared<MockPublisherStats>();
    clientSession_->setPublisherStatsCallback(clientPublisherStatsCallback_);

    serverSubscriberStatsCallback_ = std::make_shared<MockSubscriberStats>();
    serverSession_->setSubscriberStatsCallback(serverSubscriberStatsCallback_);

    serverPublisherStatsCallback_ = std::make_shared<MockPublisherStats>();
    serverSession_->setPublisherStatsCallback(serverPublisherStatsCallback_);
  }

  folly::Try<ServerSetup> onClientSetup(ClientSetup setup) override {
    if (invalidVersion_) {
      return folly::Try<ServerSetup>(std::runtime_error("invalid version"));
    }

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

  virtual folly::coro::Task<void> setupMoQSession();

  folly::DrivableExecutor* getExecutor() {
    return &eventBase_;
  }

 protected:
  using TaskFetchResult = folly::coro::Task<Publisher::FetchResult>;
  void expectFetch(
      std::function<TaskFetchResult(Fetch, std::shared_ptr<FetchConsumer>)>
          lambda,
      folly::Optional<FetchErrorCode> error = folly::none) {
    if (error) {
      EXPECT_CALL(*serverPublisherStatsCallback_, onFetchError(*error))
          .RetiresOnSaturation();
    } else {
      EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess())
          .RetiresOnSaturation();
    }
    EXPECT_CALL(*serverPublisher, fetch(_, _))
        .WillOnce(testing::Invoke(lambda))
        .RetiresOnSaturation();
  }

  void expectFetchSuccess() {
    EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess());
  }

  using TaskSubscribeResult = folly::coro::Task<Publisher::SubscribeResult>;

  void expectSubscribe(
      std::function<TaskSubscribeResult(
          const SubscribeRequest&,
          std::shared_ptr<TrackConsumer>)> lambda,
      const folly::Optional<SubscribeErrorCode>& error = folly::none) {
    EXPECT_CALL(*serverPublisher, subscribe(_, _))
        .WillOnce(testing::Invoke(
            [this, lambda = std::move(lambda), error](
                auto sub, auto pub) -> TaskSubscribeResult {
              if (error) {
                EXPECT_CALL(
                    *serverPublisherStatsCallback_, onSubscribeError(*error))
                    .RetiresOnSaturation();
              } else {
                EXPECT_CALL(
                    *serverPublisherStatsCallback_, onSubscribeSuccess())
                    .RetiresOnSaturation();
              }
              return lambda(sub, pub);
            }))
        .RetiresOnSaturation();
  }

  void expectSubscribeDone() {
    EXPECT_CALL(*subscribeCallback_, subscribeDone(_))
        .WillOnce(testing::Invoke([&] {
          subscribeDone_.post();
          return folly::unit;
        }));
  }

  using TestLogicFn = std::function<void(
      const SubscribeRequest& sub,
      std::shared_ptr<TrackConsumer> pub,
      std::shared_ptr<SubgroupConsumer> sgp,
      std::shared_ptr<MockSubgroupConsumer> sgc)>;
  folly::coro::Task<void> publishValidationTest(TestLogicFn testLogic);

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
  bool invalidVersion_{false};
  std::shared_ptr<testing::StrictMock<MockFetchConsumer>> fetchCallback_;
  std::shared_ptr<testing::StrictMock<MockTrackConsumer>> subscribeCallback_;
  folly::coro::Baton subscribeDone_;
  std::shared_ptr<MockSubscriberStats> clientSubscriberStatsCallback_;
  std::shared_ptr<MockPublisherStats> clientPublisherStatsCallback_;
  std::shared_ptr<MockSubscriberStats> serverSubscriberStatsCallback_;
  std::shared_ptr<MockPublisherStats> serverPublisherStatsCallback_;
};

// GCC barfs when using struct brace initializers inside a coroutine?
// Helper function to make ClientSetup with MAX_SUBSCRIBE_ID
ClientSetup getClientSetup(uint64_t initialMaxSubscribeId) {
  return ClientSetup{
      .supportedVersions = {kVersionDraftCurrent},
      .params = {
          {{.key = folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
            .asUint64 = initialMaxSubscribeId}}}};
}

// Helper function to make a Fetch request
Fetch getFetch(AbsoluteLocation start, AbsoluteLocation end) {
  return Fetch(
      SubscribeID(0), kTestTrackName, start, end, 0, GroupOrder::OldestFirst);
}

SubscribeRequest getSubscribe(const FullTrackName& ftn) {
  return SubscribeRequest{
      SubscribeID(0),
      TrackAlias(0),
      ftn,
      0,
      GroupOrder::OldestFirst,
      LocationType::LatestObject,
      folly::none,
      0,
      {}};
}

SubscribeDone getTrackEndedSubscribeDone(SubscribeID id) {
  return {id, SubscribeDoneStatusCode::TRACK_ENDED, 0, "end of track"};
}

TrackStatusRequest getTrackStatusRequest() {
  return TrackStatusRequest{kTestTrackName};
}

moxygen::SubscribeAnnounces getSubscribeAnnounces() {
  return SubscribeAnnounces{TrackNamespace{{"foo"}}, {}};
}

folly::coro::Task<void> MoQSessionTest::setupMoQSession() {
  clientSession_->setPublishHandler(clientPublisher);
  clientSession_->start();
  serverSession_->setPublishHandler(serverPublisher);
  serverSession_->start();
  auto serverSetup =
      co_await clientSession_->setup(getClientSetup(initialMaxSubscribeId_));

  EXPECT_EQ(serverSetup.selectedVersion, kVersionDraftCurrent);
  EXPECT_EQ(
      serverSetup.params.at(0).key,
      folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID));
  EXPECT_EQ(serverSetup.params.at(0).asUint64, initialMaxSubscribeId_);
}
} // namespace

// === SETUP tests ===

TEST_F(MoQSessionTest, Setup) {
  folly::coro::blockingWait(setupMoQSession(), getExecutor());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, SetupTimeout) {
  ClientSetup setup;
  setup.supportedVersions.push_back(kVersionDraftCurrent);
  auto serverSetup = co_await co_awaitTry(clientSession_->setup(setup));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, InvalidVersion) {
  invalidVersion_ = true;
  clientSession_->start();
  co_await folly::coro::co_reschedule_on_current_executor;
  ClientSetup setup;
  setup.supportedVersions.push_back(0xfaceb001);
  auto serverSetup = co_await co_awaitTry(clientSession_->setup(setup));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, InvalidServerVersion) {
  negotiatedVersion_ = 0xfaceb001;
  clientSession_->start();
  co_await folly::coro::co_reschedule_on_current_executor;
  ClientSetup setup;
  setup.supportedVersions.push_back(kVersionDraftCurrent);
  auto serverSetup = co_await co_awaitTry(clientSession_->setup(setup));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, ServerSetupFail) {
  failServerSetup_ = true;
  clientSession_->start();
  ClientSetup setup;
  setup.supportedVersions.push_back(kVersionDraftCurrent);
  auto serverSetup = co_await co_awaitTry(clientSession_->setup(setup));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, ServerSetupUnsupportedVersion) {
  negotiatedVersion_ = kVersionDraftCurrent - 1;
  clientSession_->start();
  ClientSetup setup;
  setup.supportedVersions.push_back(kVersionDraftCurrent);
  auto serverSetup = co_await co_awaitTry(clientSession_->setup(setup));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// === FETCH tests ===

CO_TEST_F_X(MoQSessionTest, Fetch) {
  co_await setupMoQSession();
  // Usage
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
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true))
      .WillOnce(testing::Invoke([&] {
        baton.post();
        return folly::unit;
      }));
  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await baton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, JoiningFetch) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->datagram(
        ObjectHeader(sub.trackAlias, 0, 0, 1, 0, 11),
        folly::IOBuf::copyBuffer("hello world"));
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto joining = std::get_if<JoiningFetch>(&fetch.args);
    EXPECT_NE(joining, nullptr);
    EXPECT_EQ(fetch.fullTrackName, FullTrackName(kTestTrackName));
    fetchPub->object(
        /*groupID=*/0,
        /*subgroupID=*/0,
        /*objectID=*/0,
        moxygen::test::makeBuf(100),
        noExtensions(),
        /*finFetch=*/true);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  EXPECT_CALL(*subscribeCallback_, datagram(_, _))
      .WillOnce(testing::Invoke([&](const auto& header, auto) {
        EXPECT_EQ(header.length, 11);
        return folly::unit;
      }));
  expectSubscribeDone();
  folly::coro::Baton fetchBaton;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true))
      .WillOnce(testing::Invoke([&] {
        fetchBaton.post();
        return folly::unit;
      }));
  auto res = co_await clientSession_->join(
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      1,
      129,
      GroupOrder::Default,
      {},
      fetchCallback_);
  EXPECT_FALSE(res.subscribeResult.hasError());
  EXPECT_FALSE(res.fetchResult.hasError());
  co_await subscribeDone_;
  co_await fetchBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, BadJoiningFetch) {
  co_await setupMoQSession();
  auto res = co_await clientSession_->fetch(
      Fetch(SubscribeID(0), SubscribeID(17), 1, 128, GroupOrder::Default),
      fetchCallback_);
  EXPECT_TRUE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FetchCleanupFromStreamFin) {
  co_await setupMoQSession();

  std::shared_ptr<FetchConsumer> fetchPub;
  expectFetch([&fetchPub](Fetch fetch, auto inFetchPub) -> TaskFetchResult {
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    fetchPub = std::move(inFetchPub);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  // publish here now we know FETCH_OK has been received at client
  XCHECK(fetchPub);
  fetchPub->object(
      /*groupID=*/0,
      /*subgroupID=*/0,
      /*objectID=*/0,
      moxygen::test::makeBuf(100),
      noExtensions(),
      /*finFetch=*/true);
  folly::coro::Baton baton;
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true))
      .WillOnce(testing::Invoke([&] {
        baton.post();
        return folly::unit;
      }));
  co_await baton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FetchError) {
  co_await setupMoQSession();
  EXPECT_CALL(
      *serverPublisherStatsCallback_,
      onFetchError(FetchErrorCode::INVALID_RANGE));
  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onFetchError(FetchErrorCode::INVALID_RANGE));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 2}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::INVALID_RANGE);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FetchPublisherError) {
  co_await setupMoQSession();
  expectFetch(
      [](Fetch fetch, auto) -> TaskFetchResult {
        co_return folly::makeUnexpected(FetchError{
            fetch.subscribeID,
            FetchErrorCode::TRACK_NOT_EXIST,
            "Bad trackname"});
      },
      FetchErrorCode::TRACK_NOT_EXIST);
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::TRACK_NOT_EXIST);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FetchPublisherThrow) {
  co_await setupMoQSession();
  expectFetch(
      [](Fetch fetch, auto) -> TaskFetchResult {
        throw std::runtime_error("panic!");
        co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
      },
      FetchErrorCode::INTERNAL_ERROR);
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::INTERNAL_ERROR);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FetchCancel) {
  co_await setupMoQSession();
  std::shared_ptr<FetchConsumer> fetchPub;
  expectFetch([&fetchPub](Fetch fetch, auto inFetchPub) -> TaskFetchResult {
    auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
    EXPECT_NE(standalone, nullptr);
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    fetchPub = std::move(inFetchPub);
    fetchPub->object(
        standalone->start.group,
        /*subgroupID=*/0,
        standalone->start.object,
        moxygen::test::makeBuf(100));
    // published 1 object
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, false))
      .WillOnce(testing::Return(folly::unit));
  // TODO: fetchCancel removes the callback - should it also deliver a
  // reset() call to the callback?
  // EXPECT_CALL(*fetchCallback, reset(ResetStreamErrorCode::CANCELLED));
  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 2}), fetchCallback_);
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
      noExtensions(),
      /*finFetch=*/true);
  // publish after fetchCancel fails
  EXPECT_TRUE(res2.hasError());
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FetchEarlyCancel) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 2}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  // TODO: this no-ops right now so there's nothing to verify
  res.value()->fetchCancel();
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FetchBadLength) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
    EXPECT_NE(standalone, nullptr);
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    auto objPub = fetchPub->beginObject(
        standalone->start.group,
        /*subgroupID=*/0,
        standalone->start.object,
        100,
        moxygen::test::makeBuf(10));
    // this should close the session too
    fetchPub->endOfFetch();
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetchSuccess();
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  // FETCH_OK comes but the FETCH stream is reset and we timeout waiting
  // for a new object.
  auto contract = folly::coro::makePromiseContract<folly::Unit>();
  ON_CALL(*fetchCallback_, beginObject(_, _, _, _, _, _)).WillByDefault([&] {
    contract.first.setValue();
    return folly::Expected<folly::Unit, MoQPublishError>(folly::unit);
  });
  folly::EventBaseThreadTimekeeper tk(*clientSession_->getEventBase());
  EXPECT_THROW(
      co_await folly::coro::timeout(
          std::move(contract.second), std::chrono::milliseconds(100), &tk),
      folly::FutureTimeout);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FetchOverLimit) {
  co_await setupMoQSession();
  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  expectFetch([](Fetch fetch, auto) -> TaskFetchResult {
    EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });

  auto fetchCallback1 =
      std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  auto fetchCallback2 =
      std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  auto fetchCallback3 =
      std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  Fetch fetch = getFetch({0, 0}, {0, 1});
  EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess()).Times(2);
  auto res = co_await clientSession_->fetch(fetch, fetchCallback1);
  res = co_await clientSession_->fetch(fetch, fetchCallback2);
  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onFetchError(FetchErrorCode::INTERNAL_ERROR));
  res = co_await clientSession_->fetch(fetch, fetchCallback3);
  EXPECT_TRUE(res.hasError());
}

CO_TEST_F_X(MoQSessionTest, FetchOutOfOrder) {
  co_await setupMoQSession();
  std::shared_ptr<FetchConsumer> fetchPub;
  expectFetch(
      [&fetchPub, this](Fetch fetch, auto inFetchPub) -> TaskFetchResult {
        EXPECT_EQ(fetch.fullTrackName, kTestTrackName);
        fetchPub = std::move(inFetchPub);
        eventBase_.add([this, fetchPub] {
          fetchPub->object(1, 0, 1, moxygen::test::makeBuf(100));
          // delay the bad API call one more loop, so the FETCH_HEADER comes
          // through - can remove with checkpoint() someday
          eventBase_.add([fetchPub] {
            // object 0 after object 1
            EXPECT_EQ(
                fetchPub->object(1, 0, 0, moxygen::test::makeBuf(100))
                    .error()
                    .code,
                MoQPublishError::API_ERROR);
            // group 0 after group 1
            EXPECT_EQ(
                fetchPub->object(0, 0, 2, moxygen::test::makeBuf(100))
                    .error()
                    .code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                fetchPub->objectNotExists(0, 0, 2).error().code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                fetchPub->groupNotExists(0, 0).error().code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                fetchPub->beginObject(0, 0, 0, 100, test::makeBuf(10))
                    .error()
                    .code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                fetchPub->endOfGroup(0, 0, 0).error().code,
                MoQPublishError::API_ERROR);
            EXPECT_EQ(
                fetchPub->endOfTrackAndGroup(0, 0, 0).error().code,
                MoQPublishError::API_ERROR);
            // writeHandle gone
            EXPECT_EQ(
                fetchPub->endOfFetch().error().code,
                MoQPublishError::CANCELLED);
          });
        });
        co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
      });

  EXPECT_CALL(
      *fetchCallback_, object(1, 0, 1, HasChainDataLengthOf(100), _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*fetchCallback_, reset(ResetStreamErrorCode::INTERNAL_ERROR));

  auto res = co_await clientSession_->fetch(
      getFetch(kLocationMin, kLocationMax), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

folly::coro::Task<void> MoQSessionTest::publishValidationTest(
    TestLogicFn testLogic) {
  co_await setupMoQSession();
  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  expectSubscribe(
      [this, testLogic, sg1](auto sub, auto pub) -> TaskSubscribeResult {
        auto sgp = pub->beginSubgroup(0, 0, 0).value();
        eventBase_.add([testLogic, sub, pub, sgp, sg1]() {
          testLogic(sub, pub, sgp, sg1);
        });
        co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
      });

  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, reset(ResetStreamErrorCode::INTERNAL_ERROR));
  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, DoubleBeginObject) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(testing::Return(
            folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)));
    EXPECT_EQ(
        sgp->beginObject(2, 100, test::makeBuf(10)).error().code,
        MoQPublishError::API_ERROR);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
  });
}

CO_TEST_F_X(MoQSessionTest, ObjectPayloadTooLong) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(testing::Return(
            folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)).hasValue());
    auto payloadFail =
        sgp->objectPayload(folly::IOBuf::copyBuffer(std::string(200, 'x')));
    EXPECT_EQ(payloadFail.error().code, MoQPublishError::API_ERROR);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
  });
}

CO_TEST_F_X(MoQSessionTest, ObjectPayloadEarlyFin) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(testing::Return(
            folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)).hasValue());

    // Attempt to send an object payload with length 20 and fin=true, which
    // should fail
    auto payloadFinFail = sgp->objectPayload(
        folly::IOBuf::copyBuffer(std::string(20, 'x')), true);
    EXPECT_EQ(payloadFinFail.error().code, MoQPublishError::API_ERROR);

    pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
  });
}

CO_TEST_F_X(MoQSessionTest, PublisherResetAfterBeginObject) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(testing::Return(
            folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)));

    // Call reset after beginObject
    sgp->reset(ResetStreamErrorCode::INTERNAL_ERROR);

    // Attempt to send an object payload after reset, which should fail
    auto payloadFail =
        sgp->objectPayload(folly::IOBuf::copyBuffer(std::string(20, 'x')));
    EXPECT_EQ(payloadFail.error().code, MoQPublishError::CANCELLED);

    pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
  });
}

// === TRACK STATUS tests ===

CO_TEST_F_X(MoQSessionTest, TrackStatus) {
  co_await setupMoQSession();
  EXPECT_CALL(*serverPublisher, trackStatus(_))
      .WillOnce(testing::Invoke(
          [](TrackStatusRequest request)
              -> folly::coro::Task<Publisher::TrackStatusResult> {
            co_return Publisher::TrackStatusResult{
                request.fullTrackName,
                TrackStatusCode::IN_PROGRESS,
                AbsoluteLocation{}};
          }));
  auto res = co_await clientSession_->trackStatus(getTrackStatusRequest());
  EXPECT_EQ(res.statusCode, TrackStatusCode::IN_PROGRESS);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// MAX SUBSCRIBE ID tests

CO_TEST_F_X(MoQSessionTest, MaxSubscribeID) {
  co_await setupMoQSession();
  {
    testing::InSequence enforceOrder;
    expectSubscribe(
        [](auto sub, auto) -> TaskSubscribeResult {
          co_return folly::makeUnexpected(SubscribeError{
              sub.subscribeID,
              SubscribeErrorCode::UNAUTHORIZED,
              "bad",
              folly::none});
        },
        SubscribeErrorCode::UNAUTHORIZED);
    expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
      eventBase_.add([pub, sub] {
        pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
      });
      co_return makeSubscribeOkResult(sub);
    });
    expectSubscribe([](auto sub, auto) -> TaskSubscribeResult {
      co_return makeSubscribeOkResult(sub);
    });
    expectSubscribe([](auto sub, auto) -> TaskSubscribeResult {
      co_return makeSubscribeOkResult(sub);
    });
  }
  auto trackPublisher1 =
      std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  auto trackPublisher2 =
      std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  auto trackPublisher3 =
      std::make_shared<testing::StrictMock<MockTrackConsumer>>();
  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeError(SubscribeErrorCode::UNAUTHORIZED));
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), trackPublisher1);
  co_await folly::coro::co_reschedule_on_current_executor;
  // This is true because initial is 2 in this test case and we grant credit
  // every 50%.
  auto expectedSubId = 3;
  EXPECT_EQ(serverSession_->maxSubscribeID(), expectedSubId);

  // subscribe again but this time we get a DONE
  EXPECT_CALL(*trackPublisher2, subscribeDone(_))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeSuccess());
  res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), trackPublisher2);
  co_await folly::coro::co_reschedule_on_current_executor;
  expectedSubId++;
  EXPECT_EQ(serverSession_->maxSubscribeID(), expectedSubId);

  // subscribe three more times, last one should fail, the first two will get
  // subscribeDone via the session closure
  EXPECT_CALL(*trackPublisher3, subscribeDone(_))
      .WillOnce(testing::Return(folly::unit))
      .WillOnce(testing::Return(folly::unit));

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeSuccess()).Times(2);
  auto sub = getSubscribe(kTestTrackName);
  res = co_await clientSession_->subscribe(sub, trackPublisher3);
  res = co_await clientSession_->subscribe(sub, trackPublisher3);
  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeError(SubscribeErrorCode::INTERNAL_ERROR));
  res = co_await clientSession_->subscribe(sub, trackPublisher3);
  EXPECT_TRUE(res.hasError());
}

// === SUBSCRIBE tests ===

CO_TEST_F_X(MoQSessionTest, Datagrams) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->datagram(
        ObjectHeader(sub.trackAlias, 0, 0, 1, 0, 11),
        folly::IOBuf::copyBuffer("hello world"));
    pub->datagram(
        ObjectHeader(
            sub.trackAlias, 0, 0, 2, 0, ObjectStatus::OBJECT_NOT_EXIST),
        nullptr);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  {
    testing::InSequence enforceOrder;
    EXPECT_CALL(*subscribeCallback_, datagram(_, _))
        .WillOnce(testing::Invoke([&](const auto& header, auto) {
          EXPECT_EQ(header.length, 11);
          return folly::unit;
        }));
    EXPECT_CALL(*subscribeCallback_, datagram(_, _))
        .WillOnce(testing::Invoke([&](const auto& header, auto) {
          EXPECT_EQ(header.status, ObjectStatus::OBJECT_NOT_EXIST);
          return folly::unit;
        }));
  }
  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, DatagramBeforeSessionSetup) {
  clientSession_->start();
  EXPECT_FALSE(clientWt_->isSessionClosed());
  clientSession_->onDatagram(folly::IOBuf::copyBuffer("hello world"));
  EXPECT_TRUE(clientWt_->isSessionClosed());
  co_return;
}

// SUBSCRIBE DONE tests

CO_TEST_F_X(MoQSessionTest, SubscribeDoneStreamCount) {
  co_await setupMoQSession();
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([pub, sub] {
      pub->objectStream(
          ObjectHeader(sub.trackAlias, 0, 0, 0, 0, 10),
          moxygen::test::makeBuf(10));
      auto sgp = pub->beginSubgroup(0, 1, 0).value();
      sgp->object(1, moxygen::test::makeBuf(10));
      sgp->object(2, moxygen::test::makeBuf(10), noExtensions(), true);
      pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
    });
    co_return makeSubscribeOkResult(sub);
  });
  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  auto sg2 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 1, 0))
      .WillOnce(testing::Return(sg2));
  EXPECT_CALL(*sg1, object(0, _, _, true))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg2, object(1, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg2, object(2, _, _, true))
      .WillOnce(testing::Return(folly::unit));
  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, SubscribeDoneFromSubscribe) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
    co_return makeSubscribeOkResult(sub);
  });
  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, SubscribeDoneAPIErrors) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
    // All these APIs fail after SUBSCRIBE_DONE
    EXPECT_EQ(
        pub->beginSubgroup(1, 1, 1).error().code, MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->awaitStreamCredit().error().code, MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->datagram(
               ObjectHeader(sub.trackAlias, 2, 2, 2, 2, 10),
               moxygen::test::makeBuf(10))
            .error()
            .code,
        MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID))
            .error()
            .code,
        MoQPublishError::API_ERROR);
    co_return makeSubscribeOkResult(sub);
  });

  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
CO_TEST_F_X(MoQSessionTest, SubscribeAndUnsubscribeAnnounces) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeAnnounces(_))
      .WillOnce(testing::Invoke(
          [](auto subAnn)
              -> folly::coro::Task<Publisher::SubscribeAnnouncesResult> {
            co_return makeSubscribeAnnouncesOkResult(subAnn);
          }));

  auto announceResult =
      co_await clientSession_->subscribeAnnounces(getSubscribeAnnounces());
  EXPECT_FALSE(announceResult.hasError());

  announceResult.value()->unsubscribeAnnounces();
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, TooFarBehindOneSubgroup) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add(
        [pub, sub, serverWt = serverWt_.get(), eventBase = &eventBase_] {
          auto sgp = pub->beginSubgroup(0, 0, 0).value();
          auto objectResult = sgp->object(0, moxygen::test::makeBuf(10));
          EXPECT_TRUE(objectResult.hasValue());

          // Run this stuff later on, otherwise the test will hang because of
          // the discrepancy in the stream count because the stream would have
          // been reset before the subgroup header got across.
          eventBase->add([pub, sub, serverWt, sgp] {
            // Start buffering data
            serverWt->writeHandles[2]->setImmediateDelivery(false);
            auto objectResult2 = sgp->object(1, moxygen::test::makeBuf(101));
            EXPECT_TRUE(objectResult2.hasError());
          });
        });
    co_return makeSubscribeOkResult(sub);
  });

  expectSubscribeDone();
  auto mockSubgroupConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(mockSubgroupConsumer));
  EXPECT_CALL(*mockSubgroupConsumer, object(0, _, _, _))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*mockSubgroupConsumer, reset(_));
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, FreeUpBufferSpaceOneSubgroup) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([pub, sub, serverWt = serverWt_.get()] {
      auto sgp = pub->beginSubgroup(0, 0, 0).value();
      auto objectResult = sgp->object(0, moxygen::test::makeBuf(10));
      EXPECT_TRUE(objectResult.hasValue());

      // Start buffering data
      serverWt->writeHandles[2]->setImmediateDelivery(false);
      for (uint32_t i = 0; i < 10; i++) {
        // Run this stuff later on, otherwise the test will hang because of
        // the discrepancy in the stream count because the stream would have
        // been reset before the subgroup header got across.
        objectResult = sgp->object(i + 1, moxygen::test::makeBuf(50));
        serverWt->writeHandles[2]->deliverInflightData();
        EXPECT_FALSE(objectResult.hasError());
      }
      pub->subscribeDone(getTrackEndedSubscribeDone(sub.subscribeID));
    });
    co_return makeSubscribeOkResult(sub);
  });

  expectSubscribeDone();
  auto mockSubgroupConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(mockSubgroupConsumer));
  EXPECT_CALL(*mockSubgroupConsumer, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  EXPECT_CALL(*mockSubgroupConsumer, reset(_));
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, TooFarBehindMultipleSubgroups) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add(
        [pub, sub, serverWt = serverWt_.get(), eventBase = &eventBase_] {
          std::vector<std::shared_ptr<SubgroupConsumer>> subgroupConsumers;

          for (uint32_t subgroupId = 0; subgroupId < 3; subgroupId++) {
            subgroupConsumers.push_back(
                pub->beginSubgroup(0, subgroupId, 0).value());
            auto objectResult = subgroupConsumers[subgroupId]->object(
                0, moxygen::test::makeBuf(10));
            EXPECT_TRUE(objectResult.hasValue());
          }

          // Run this stuff later on, otherwise the test will hang because of
          // the discrepancy in the stream count because the stream would have
          // been reset before the subgroup header got across.
          eventBase->add([pub, sub, serverWt, subgroupConsumers] {
            for (uint32_t subgroupId = 0; subgroupId < 2; subgroupId++) {
              serverWt->writeHandles[2 + subgroupId * 4]->setImmediateDelivery(
                  false);
              auto objectResult = subgroupConsumers[subgroupId]->object(
                  1, moxygen::test::makeBuf(30));
              EXPECT_TRUE(objectResult.hasValue());
            }

            serverWt->writeHandles[10]->setImmediateDelivery(false);
            auto objectResult =
                subgroupConsumers[2]->object(1, moxygen::test::makeBuf(40));
            EXPECT_TRUE(objectResult.hasError());
          });
        });
    co_return makeSubscribeOkResult(sub);
  });

  expectSubscribeDone();
  auto mockSubgroupConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(_, _, _))
      .WillRepeatedly(testing::Return(mockSubgroupConsumer));
  EXPECT_CALL(*mockSubgroupConsumer, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  EXPECT_CALL(*mockSubgroupConsumer, reset(_)).Times(3);
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_F_X(MoQSessionTest, PublisherAliveUntilAllBytesDelivered) {
  co_await setupMoQSession();
  folly::coro::Baton barricade;
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  expectSubscribe(
      [this, &subgroupConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        eventBase_.add([pub, sub, &subgroupConsumer] {
          auto sgp = pub->beginSubgroup(0, 0, 0).value();
          sgp->object(0, moxygen::test::makeBuf(10), Extensions(), false);
          subgroupConsumer = sgp;
        });
        co_return makeSubscribeOkResult(sub);
      });
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Invoke([&] {
        eventBase_.add([&] {
          serverWt_->writeHandles[2]->setImmediateDelivery(false);
          subgroupConsumer->object(
              1, moxygen::test::makeBuf(10), Extensions(), true);
          barricade.post();
        });
        return sg;
      }));
  EXPECT_CALL(*sg, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  EXPECT_CALL(*sg, reset(_));
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await barricade;
  serverWt_->writeHandles[2]->deliverInflightData();
  EXPECT_CALL(*subscribeCallback_, subscribeDone(_))
      .WillOnce(testing::Return(folly::unit));
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
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
// trackstatus
// announce/unannounce/announceCancel/announceError
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
