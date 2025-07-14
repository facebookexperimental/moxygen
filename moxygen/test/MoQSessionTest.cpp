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

const size_t kTestMaxRequestID = 2;
const FullTrackName kTestTrackName{TrackNamespace{{"foo"}}, "bar"};

const TrackAlias kUselessAlias(std::numeric_limits<uint32_t>::max());

MATCHER_P(HasChainDataLengthOf, n, "") {
  return arg->computeChainDataLength() == uint64_t(n);
}

std::shared_ptr<MockFetchHandle> makeFetchOkResult(
    const Fetch& fetch,
    const AbsoluteLocation& location) {
  return std::make_shared<MockFetchHandle>(FetchOk{
      fetch.requestID,
      GroupOrder::OldestFirst,
      /*endOfTrack=*/0,
      location,
      {}});
}

auto makeSubscribeOkResult(
    const SubscribeRequest& sub,
    const folly::Optional<AbsoluteLocation>& latest = folly::none) {
  return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
      sub.requestID,
      TrackAlias(sub.requestID.value),
      std::chrono::milliseconds(0),
      GroupOrder::OldestFirst,
      latest,
      {}});
}

Publisher::SubscribeAnnouncesResult makeSubscribeAnnouncesOkResult(
    const auto& subAnn) {
  return std::make_shared<MockSubscribeAnnouncesHandle>(
      SubscribeAnnouncesOk({RequestID(0), subAnn.trackNamespacePrefix}));
}

Subscriber::AnnounceResult makeAnnounceOkResult(const auto& ann) {
  return std::make_shared<MockAnnounceHandle>(
      AnnounceOk({ann.requestID, ann.trackNamespace}));
}

struct VersionParams {
  std::vector<uint64_t> clientVersions;
  uint64_t serverVersion;
};

class MoQSessionTest : public testing::TestWithParam<VersionParams>,
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

    EXPECT_EQ(setup.supportedVersions[0], getClientSupportedVersions()[0]);
    EXPECT_EQ(setup.params.at(0).key, folly::to_underlying(SetupKey::PATH));
    EXPECT_EQ(setup.params.at(0).asString, "/foo");
    EXPECT_EQ(
        setup.params.at(1).key, folly::to_underlying(SetupKey::MAX_REQUEST_ID));
    EXPECT_EQ(setup.params.at(1).asUint64, initialMaxRequestID_);
    if (setup.params.size() > 2) {
      EXPECT_EQ(
          setup.params.at(2).key,
          folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE));
    } else {
      EXPECT_LT(setup.supportedVersions[0], kVersionDraft11);
    }
    if (failServerSetup_) {
      return folly::makeTryWith(
          []() -> ServerSetup { throw std::runtime_error("failed"); });
    }
    return folly::Try<ServerSetup>(ServerSetup{
        .selectedVersion = getServerSelectedVersion(),
        .params = {
            SetupParameter{
                folly::to_underlying(SetupKey::MAX_REQUEST_ID),
                "",
                initialMaxRequestID_,
                {}},
            SetupParameter{
                folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE),
                "",
                16,
                {}}}});
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

  std::shared_ptr<moxygen::MockSubscriberStats> getSubscriberStatsCallback(
      MoQControlCodec::Direction direction) {
    switch (direction) {
      case MoQControlCodec::Direction::SERVER:
        return serverSubscriberStatsCallback_;
      case MoQControlCodec::Direction::CLIENT:
        return clientSubscriberStatsCallback_;
      default:
        return nullptr;
    }
  }

  std::shared_ptr<moxygen::MockPublisherStats> getPublisherStatsCallback(
      MoQControlCodec::Direction direction) {
    switch (direction) {
      case MoQControlCodec::Direction::SERVER:
        return serverPublisherStatsCallback_;
      case MoQControlCodec::Direction::CLIENT:
        return clientPublisherStatsCallback_;
      default:
        return nullptr;
    }
  }

  std::shared_ptr<moxygen::MockPublisher> getPublisher(
      MoQControlCodec::Direction direction) {
    switch (direction) {
      case MoQControlCodec::Direction::SERVER:
        return serverPublisher;
      case MoQControlCodec::Direction::CLIENT:
        return clientPublisher;
      default:
        return nullptr;
    }
  }

  std::shared_ptr<moxygen::MockSubscriber> getSubscriber(
      MoQControlCodec::Direction direction) {
    switch (direction) {
      case MoQControlCodec::Direction::SERVER:
        return serverSubscriber;
      case MoQControlCodec::Direction::CLIENT:
        return clientSubscriber;
      default:
        return nullptr;
    }
  }

  MoQControlCodec::Direction oppositeDirection(
      MoQControlCodec::Direction direction) {
    return (direction == MoQControlCodec::Direction::CLIENT)
        ? MoQControlCodec::Direction::SERVER
        : MoQControlCodec::Direction::CLIENT;
  }

  void expectSubscribe(
      std::function<TaskSubscribeResult(
          const SubscribeRequest&,
          std::shared_ptr<TrackConsumer>)> lambda,
      MoQControlCodec::Direction direction = MoQControlCodec::Direction::SERVER,
      const folly::Optional<SubscribeErrorCode>& error = folly::none) {
    EXPECT_CALL(*getPublisher(direction), subscribe(_, _))
        .WillOnce(testing::Invoke(
            [this, lambda = std::move(lambda), error, direction](
                auto sub, auto pub) -> TaskSubscribeResult {
              EXPECT_CALL(
                  *getSubscriberStatsCallback(oppositeDirection(direction)),
                  recordSubscribeLatency(_));
              if (error) {
                EXPECT_CALL(
                    *getPublisherStatsCallback(direction),
                    onSubscribeError(*error))
                    .RetiresOnSaturation();
              } else {
                EXPECT_CALL(
                    *getPublisherStatsCallback(direction), onSubscribeSuccess())
                    .RetiresOnSaturation();
                pub->setTrackAlias(TrackAlias(sub.requestID.value));
              }
              return lambda(sub, pub);
            }))
        .RetiresOnSaturation();
  }

  void expectSubscribeDone(
      MoQControlCodec::Direction recipient =
          MoQControlCodec::Direction::CLIENT) {
    EXPECT_CALL(
        *getPublisherStatsCallback(oppositeDirection(recipient)),
        onSubscribeDone(_));
    EXPECT_CALL(*getSubscriberStatsCallback(recipient), onSubscribeDone(_));
    EXPECT_CALL(*subscribeCallback_, subscribeDone(_))
        .WillOnce(testing::Invoke([&] {
          subscribeDone_.post();
          return folly::unit;
        }));
  }

  // GCC barfs when using struct brace initializers inside a coroutine?
  // Helper function to make ClientSetup with MAX_REQUEST_ID
  ClientSetup getClientSetup(uint64_t initialMaxRequestID = 2) {
    return ClientSetup{
        .supportedVersions = getClientSupportedVersions(),
        .params = {
            SetupParameter{folly::to_underlying(SetupKey::PATH), "/foo", 0, {}},
            SetupParameter{
                folly::to_underlying(SetupKey::MAX_REQUEST_ID),
                "",
                initialMaxRequestID,
                {}},
            SetupParameter{
                folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE),
                "",
                16,
                {}}}};
  }

  std::vector<uint64_t> getClientSupportedVersions() {
    return GetParam().clientVersions;
  }

  uint64_t getServerSelectedVersion() {
    return GetParam().serverVersion;
  }

  uint8_t getRequestIDMultiplier() const {
    return GetParam().serverVersion >= kVersionDraft11 ? 2 : 1;
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
  std::shared_ptr<MockSubscriber> clientSubscriber{
      std::make_shared<MockSubscriber>()};
  std::shared_ptr<MockSubscriber> serverSubscriber{
      std::make_shared<MockSubscriber>()};
  uint64_t initialMaxRequestID_{kTestMaxRequestID * getRequestIDMultiplier()};
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

INSTANTIATE_TEST_SUITE_P(
    MoQSessionTest,
    MoQSessionTest,
    testing::Values(
        VersionParams{{kVersionDraft08}, kVersionDraft08},
        VersionParams{{kVersionDraft09}, kVersionDraft09},
        VersionParams{{kVersionDraft10}, kVersionDraft10},
        VersionParams{{kVersionDraft11}, kVersionDraft11},
        VersionParams{{kVersionDraft12}, kVersionDraft12}));

// Helper function to make a Fetch request
Fetch getFetch(AbsoluteLocation start, AbsoluteLocation end) {
  return Fetch(
      RequestID(0), kTestTrackName, start, end, 0, GroupOrder::OldestFirst);
}

SubscribeRequest getSubscribe(const FullTrackName& ftn) {
  return SubscribeRequest{
      RequestID(0),
      TrackAlias(0),
      ftn,
      0,
      GroupOrder::OldestFirst,
      true,
      LocationType::LatestObject,
      folly::none,
      0,
      {}};
}

SubscribeDone getTrackEndedSubscribeDone(RequestID id) {
  return {id, SubscribeDoneStatusCode::TRACK_ENDED, 0, "end of track"};
}

TrackStatusRequest getTrackStatusRequest() {
  return TrackStatusRequest{RequestID(0), kTestTrackName};
}

moxygen::SubscribeAnnounces getSubscribeAnnounces() {
  return SubscribeAnnounces{RequestID(0), TrackNamespace{{"foo"}}, {}};
}

moxygen::Announce getAnnounce() {
  return Announce{RequestID(0), TrackNamespace{{"foo"}}, {}};
}

folly::coro::Task<void> MoQSessionTest::setupMoQSession() {
  clientSession_->setPublishHandler(clientPublisher);
  clientSession_->setSubscribeHandler(clientSubscriber);
  clientSession_->start();
  serverSession_->setPublishHandler(serverPublisher);
  serverSession_->setSubscribeHandler(serverSubscriber);
  serverSession_->start();
  auto serverSetup =
      co_await clientSession_->setup(getClientSetup(initialMaxRequestID_));

  EXPECT_EQ(serverSetup.selectedVersion, getServerSelectedVersion());
  EXPECT_EQ(
      serverSetup.params.at(0).key,
      folly::to_underlying(SetupKey::MAX_REQUEST_ID));
  EXPECT_EQ(serverSetup.params.at(0).asUint64, initialMaxRequestID_);
}
} // namespace

// === SETUP tests ===

using MoQVersionNegotiationTest = MoQSessionTest;

INSTANTIATE_TEST_SUITE_P(
    MoQVersionNegotiationTest,
    MoQVersionNegotiationTest,
    testing::Values(
        VersionParams{{kVersionDraft08}, kVersionDraft08},
        VersionParams{{kVersionDraft09}, kVersionDraft09},
        VersionParams{{kVersionDraft10}, kVersionDraft10},
        VersionParams{{kVersionDraft11}, kVersionDraft11},
        VersionParams{{kVersionDraft12}, kVersionDraft12},
        VersionParams{{kVersionDraft10, kVersionDraft11}, kVersionDraft10},
        VersionParams{{kVersionDraft10, kVersionDraft11}, kVersionDraft11}));

TEST_P(MoQVersionNegotiationTest, Setup) {
  folly::coro::blockingWait(setupMoQSession(), getExecutor());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

using CurrentVersionOnly = MoQSessionTest;

CO_TEST_P_X(CurrentVersionOnly, SetupTimeout) {
  ClientSetup setup;
  setup.supportedVersions.push_back(kVersionDraftCurrent);
  auto serverSetup = co_await co_awaitTry(clientSession_->setup(setup));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(CurrentVersionOnly, ServerSetupFail) {
  failServerSetup_ = true;
  clientSession_->start();
  auto serverSetup =
      co_await co_awaitTry(clientSession_->setup(getClientSetup()));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

INSTANTIATE_TEST_SUITE_P(
    CurrentVersionOnly,
    CurrentVersionOnly,
    testing::Values(
        VersionParams{{kVersionDraftCurrent}, kVersionDraftCurrent}));

CO_TEST_P_X(MoQSessionTest, InvalidVersion) {
  invalidVersion_ = true;
  clientSession_->start();
  co_await folly::coro::co_reschedule_on_current_executor;
  ClientSetup setup;
  setup.supportedVersions.push_back(0xfaceb001);
  auto serverSetup = co_await co_awaitTry(clientSession_->setup(setup));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}
class InvalidServerVersionTest : public MoQSessionTest {};

INSTANTIATE_TEST_SUITE_P(
    InvalidServerVersionTest,
    InvalidServerVersionTest,
    testing::Values(
        VersionParams{{kVersionDraftCurrent}, kVersionDraftCurrent - 1},
        VersionParams{{kVersionDraftCurrent}, 0xfaceb001}));

CO_TEST_P_X(InvalidServerVersionTest, InvalidServerVersion) {
  clientSession_->start();
  co_await folly::coro::co_reschedule_on_current_executor;
  auto serverSetup =
      co_await co_awaitTry(clientSession_->setup(getClientSetup()));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(InvalidServerVersionTest, ServerSetupUnsupportedVersion) {
  clientSession_->start();
  auto serverSetup =
      co_await co_awaitTry(clientSession_->setup(getClientSetup()));
  EXPECT_TRUE(serverSetup.hasException());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// === FETCH tests ===

CO_TEST_P_X(MoQSessionTest, Fetch) {
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
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordFetchLatency(_));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());
  co_await baton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, RelativeJoiningFetch) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->datagram(
        ObjectHeader(kUselessAlias, 0, 0, 1, 0, 11),
        folly::IOBuf::copyBuffer("hello world"));
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
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
      fetchCallback_,
      FetchType::RELATIVE_JOINING);
  EXPECT_FALSE(res.subscribeResult.hasError());
  EXPECT_FALSE(res.fetchResult.hasError());
  co_await subscribeDone_;
  co_await fetchBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, BadRelativeJoiningFetch) {
  co_await setupMoQSession();
  auto res = co_await clientSession_->fetch(
      Fetch(
          RequestID(0),
          RequestID(17),
          1,
          FetchType::RELATIVE_JOINING,
          128,
          GroupOrder::Default),
      fetchCallback_);
  EXPECT_TRUE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

using V11PlusTests = MoQSessionTest;

INSTANTIATE_TEST_SUITE_P(
    V11PlusTests,
    V11PlusTests,
    testing::Values(
        VersionParams{{kVersionDraft11, kVersionDraft12}, kVersionDraft12}));

CO_TEST_P_X(V11PlusTests, AbsoluteJoiningFetch) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    for (uint32_t group = 6; group < 10; group++) {
      pub->datagram(
          ObjectHeader(kUselessAlias, group, 0, 0, 0, 11),
          folly::IOBuf::copyBuffer("hello world"));
    }
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectFetch([](Fetch fetch, auto fetchPub) -> TaskFetchResult {
    auto joining = std::get_if<JoiningFetch>(&fetch.args);
    EXPECT_NE(joining, nullptr);
    EXPECT_EQ(fetch.fullTrackName, FullTrackName(kTestTrackName));
    for (uint32_t group = 2; group < 6; group++) {
      auto objectPubResult = fetchPub->object(
          /*groupID=*/group,
          /*subgroupID=*/0,
          /*objectID=*/0,
          moxygen::test::makeBuf(100),
          noExtensions(),
          /*finFetch=*/(group == 5));
      EXPECT_TRUE(objectPubResult.hasValue());
    }
    co_return makeFetchOkResult(fetch, AbsoluteLocation{100, 100});
  });
  EXPECT_CALL(*subscribeCallback_, datagram(_, _))
      .WillRepeatedly(testing::Invoke([&](const auto& header, auto) {
        EXPECT_EQ(header.length, 11);
        return folly::unit;
      }));
  expectSubscribeDone();
  folly::coro::Baton fetchBaton;
  for (uint32_t group = 2; group < 6; group++) {
    EXPECT_CALL(
        *fetchCallback_, object(group, 0, 0, HasChainDataLengthOf(100), _, _))
        .WillRepeatedly(testing::Invoke([&] {
          fetchBaton.post();
          return folly::unit;
        }));
  }
  auto res = co_await clientSession_->join(
      getSubscribe(kTestTrackName),
      subscribeCallback_,
      2 /* joiningStart */,
      129 /* fetchPri */,
      GroupOrder::Default,
      {},
      fetchCallback_,
      FetchType::ABSOLUTE_JOINING);
  EXPECT_FALSE(res.subscribeResult.hasError());
  EXPECT_FALSE(res.fetchResult.hasError());
  co_await subscribeDone_;
  co_await fetchBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Subscribe id passed into fetch() doesn't correspond to a subscription.
CO_TEST_P_X(V11PlusTests, BadAbsoluteJoiningFetch) {
  co_await setupMoQSession();
  auto res = co_await clientSession_->fetch(
      Fetch(
          RequestID(0),
          RequestID(17),
          1,
          FetchType::ABSOLUTE_JOINING,
          128,
          GroupOrder::Default),
      fetchCallback_);
  EXPECT_TRUE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, FetchCleanupFromStreamFin) {
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

CO_TEST_P_X(MoQSessionTest, FetchError) {
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

CO_TEST_P_X(MoQSessionTest, FetchPublisherError) {
  co_await setupMoQSession();
  expectFetch(
      [](Fetch fetch, auto) -> TaskFetchResult {
        co_return folly::makeUnexpected(FetchError{
            fetch.requestID, FetchErrorCode::TRACK_NOT_EXIST, "Bad trackname"});
      },
      FetchErrorCode::TRACK_NOT_EXIST);
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::TRACK_NOT_EXIST);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, FetchPublisherThrow) {
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

CO_TEST_P_X(MoQSessionTest, FetchCancel) {
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

CO_TEST_P_X(MoQSessionTest, FetchEarlyCancel) {
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

CO_TEST_P_X(MoQSessionTest, FetchBadLength) {
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

CO_TEST_P_X(MoQSessionTest, FetchOverLimit) {
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

CO_TEST_P_X(MoQSessionTest, FetchOutOfOrder) {
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
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
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
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, ServerInitiatedSubscribe) {
  co_await setupMoQSession();
  expectSubscribe(
      [this](auto sub, auto pub) -> TaskSubscribeResult {
        eventBase_.add([pub, sub] {
          auto sgp = pub->beginSubgroup(0, 0, 0).value();
          sgp->object(0, moxygen::test::makeBuf(10));
          sgp->object(1, moxygen::test::makeBuf(10), noExtensions(), true);
          pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
        });
        co_return makeSubscribeOkResult(sub);
      },
      MoQControlCodec::Direction::CLIENT);

  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, object(1, _, _, true))
      .WillOnce(testing::Return(folly::unit));
  expectSubscribeDone(MoQControlCodec::Direction::SERVER);
  auto res = co_await serverSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  serverSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, DoubleBeginObject) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(testing::Return(
            folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)));
    EXPECT_EQ(
        sgp->beginObject(2, 100, test::makeBuf(10)).error().code,
        MoQPublishError::API_ERROR);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
  });
}

CO_TEST_P_X(MoQSessionTest, ObjectPayloadTooLong) {
  co_await publishValidationTest([](auto sub, auto pub, auto sgp, auto sgc) {
    EXPECT_CALL(*sgc, beginObject(1, 100, _, _))
        .WillOnce(testing::Return(
            folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
    EXPECT_TRUE(sgp->beginObject(1, 100, test::makeBuf(10)).hasValue());
    auto payloadFail =
        sgp->objectPayload(folly::IOBuf::copyBuffer(std::string(200, 'x')));
    EXPECT_EQ(payloadFail.error().code, MoQPublishError::API_ERROR);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
  });
}

CO_TEST_P_X(MoQSessionTest, ObjectPayloadEarlyFin) {
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

    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
  });
}

CO_TEST_P_X(MoQSessionTest, PublisherResetAfterBeginObject) {
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

    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
  });
}

CO_TEST_P_X(MoQSessionTest, ObjectStatus) {
  co_await setupMoQSession();
  std::shared_ptr<TrackConsumer> trackConsumer;
  expectSubscribe(
      [this, &trackConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        eventBase_.add([pub, sub] {
          auto sgp1 = pub->beginSubgroup(0, 0, 0).value();
          sgp1->object(0, moxygen::test::makeBuf(10));
          sgp1->objectNotExists(1);
          sgp1->object(2, moxygen::test::makeBuf(11));
          sgp1->endOfGroup(3, noExtensions());
          pub->groupNotExists(1, 0, 0);
          auto sgp2 = pub->beginSubgroup(2, 0, 0).value();
          sgp2->object(0, moxygen::test::makeBuf(10));
          sgp2->endOfTrackAndGroup(2);
        });
        co_return makeSubscribeOkResult(sub);
      });
  auto sg1 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, objectNotExists(1, _, _))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, object(2, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*sg1, endOfGroup(3, _)).WillOnce(testing::Return(folly::unit));

  auto sg2 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(1, 0, 0))
      .WillOnce(testing::Return(sg2));
  EXPECT_CALL(*subscribeCallback_, groupNotExists(1, 0, 0, _))
      .WillOnce(testing::Return(folly::unit));

  auto sg3 = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(2, 0, 0))
      .WillOnce(testing::Return(sg3));
  EXPECT_CALL(*sg3, object(0, _, _, false))
      .WillOnce(testing::Return(folly::unit));
  folly::coro::Baton endOfTrackAndGroupBaton;
  EXPECT_CALL(*sg3, endOfTrackAndGroup(2, _)).WillOnce(testing::Invoke([&]() {
    endOfTrackAndGroupBaton.post();
    return folly::unit;
  }));
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  co_await endOfTrackAndGroupBaton;
  expectSubscribeDone();
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// === TRACK STATUS tests ===

CO_TEST_P_X(MoQSessionTest, TrackStatus) {
  co_await setupMoQSession();
  EXPECT_CALL(*serverPublisherStatsCallback_, onTrackStatus());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onTrackStatus());
  EXPECT_CALL(*serverPublisher, trackStatus(_))
      .WillOnce(testing::Invoke(
          [](TrackStatusRequest request)
              -> folly::coro::Task<Publisher::TrackStatusResult> {
            co_return Publisher::TrackStatusResult{
                request.requestID,
                request.fullTrackName,
                TrackStatusCode::IN_PROGRESS,
                AbsoluteLocation{}};
          }));
  auto res = co_await clientSession_->trackStatus(getTrackStatusRequest());
  EXPECT_EQ(res.statusCode, TrackStatusCode::IN_PROGRESS);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// MAX SUBSCRIBE ID tests

CO_TEST_P_X(MoQSessionTest, MaxRequestID) {
  co_await setupMoQSession();
  {
    testing::InSequence enforceOrder;
    expectSubscribe(
        [](auto sub, auto) -> TaskSubscribeResult {
          co_return folly::makeUnexpected(SubscribeError{
              sub.requestID,
              SubscribeErrorCode::UNAUTHORIZED,
              "bad",
              folly::none});
        },
        MoQControlCodec::Direction::SERVER,
        SubscribeErrorCode::UNAUTHORIZED);
    expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
      eventBase_.add([pub, sub] {
        pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
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
  // This is true because initial is 4 in this test case and we grant credit
  // every 50%.
  auto expectedSubId = 3 * getRequestIDMultiplier();
  EXPECT_EQ(serverSession_->maxRequestID(), expectedSubId);

  // subscribe again but this time we get a DONE
  EXPECT_CALL(*trackPublisher2, subscribeDone(_))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeSuccess());
  res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), trackPublisher2);
  co_await folly::coro::co_reschedule_on_current_executor;
  expectedSubId += getRequestIDMultiplier();
  EXPECT_EQ(serverSession_->maxRequestID(), expectedSubId);

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
  EXPECT_CALL(*clientSubscriberStatsCallback_, recordSubscribeLatency(_));
  res = co_await clientSession_->subscribe(sub, trackPublisher3);
  EXPECT_TRUE(res.hasError());
}

// === SUBSCRIBE tests ===

CO_TEST_P_X(MoQSessionTest, Datagrams) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->datagram(
        ObjectHeader(kUselessAlias, 0, 0, 1, 0, 11),
        folly::IOBuf::copyBuffer("hello world"));
    pub->datagram(
        ObjectHeader(kUselessAlias, 0, 0, 2, 0, ObjectStatus::OBJECT_NOT_EXIST),
        nullptr);
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
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

CO_TEST_P_X(MoQSessionTest, DatagramBeforeSessionSetup) {
  clientSession_->start();
  EXPECT_FALSE(clientWt_->isSessionClosed());
  clientSession_->onDatagram(folly::IOBuf::copyBuffer("hello world"));
  EXPECT_TRUE(clientWt_->isSessionClosed());
  co_return;
}

CO_TEST_P_X(MoQSessionTest, SubscribeUpdate) {
  co_await setupMoQSession();
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectSubscribeDone();
  expectSubscribe(
      [&mockSubscriptionHandle, &trackConsumer](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  SubscribeUpdate subscribeUpdate{
      subscribeRequest.requestID,
      AbsoluteLocation{0, 0},
      10,
      kDefaultPriority + 1,
      true,
      {}};
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeUpdate());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeUpdate());
  subscribeHandler->subscribeUpdate(subscribeUpdate);
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdate)
      .WillOnce(testing::Invoke([&](auto) { subscribeUpdateInvoked.post(); }));
  co_await subscribeUpdateInvoked;
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Checks to see that we return errors if we receive a subscribe request with
// forward == false and try to send data.
CO_TEST_P_X(V11PlusTests, SubscribeForwardingFalse) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    auto pubResult1 = pub->datagram(
        ObjectHeader(kUselessAlias, 0, 0, 1, 0, 11),
        folly::IOBuf::copyBuffer("hello world"));
    EXPECT_TRUE(pubResult1.hasError());
    auto pubResult2 =
        pub->objectStream(ObjectHeader(kUselessAlias, 0, 0, 1, 0, 11), nullptr);
    EXPECT_TRUE(pubResult2.hasError());
    auto pubResult3 = pub->beginSubgroup(0, 0, 0);
    EXPECT_TRUE(pubResult3.hasError());
    auto pubResult4 = pub->groupNotExists(0, 0, 0, noExtensions());
    EXPECT_TRUE(pubResult4.hasError());
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });
  expectSubscribeDone();
  auto subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.forward = false;
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// Checks to see that we return errors if we receive a subscribe update with
// forward == false and try to send data.
CO_TEST_P_X(V11PlusTests, SubscribeUpdateForwardingFalse) {
  co_await setupMoQSession();
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  expectSubscribeDone();
  expectSubscribe(
      [&subgroupConsumer, &trackConsumer, &mockSubscriptionHandle, this](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
        auto pubResult = pub->beginSubgroup(0, 0, 0);
        EXPECT_FALSE(pubResult.hasError());
        subgroupConsumer = pubResult.value();
        auto objectResult =
            subgroupConsumer->object(0, moxygen::test::makeBuf(10));
        EXPECT_FALSE(objectResult.hasError());
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });

  auto mockSubgroupConsumer =
      std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  folly::coro::Baton subgroupCreated;
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Invoke([&](auto, auto, auto) {
        subgroupCreated.post();
        return mockSubgroupConsumer;
      }));
  EXPECT_CALL(*mockSubgroupConsumer, object(0, _, _, _))
      .WillOnce(testing::Return(folly::unit));
  EXPECT_CALL(*mockSubgroupConsumer, reset(_));
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  auto subscribeHandler = res.value();

  co_await subgroupCreated;

  SubscribeUpdate subscribeUpdate{
      subscribeRequest.requestID,
      AbsoluteLocation{0, 0},
      10,
      kDefaultPriority,
      false,
      {}};
  subscribeHandler->subscribeUpdate(subscribeUpdate);
  folly::coro::Baton subscribeUpdateInvoked;
  EXPECT_CALL(*mockSubscriptionHandle, subscribeUpdate)
      .WillOnce(testing::Invoke(
          [&](auto /*blag*/) { subscribeUpdateInvoked.post(); }));
  co_await subscribeUpdateInvoked;
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  auto pubResult = subgroupConsumer->object(1, moxygen::test::makeBuf(10));
  EXPECT_TRUE(pubResult.hasError());
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

// SUBSCRIBE DONE tests

CO_TEST_P_X(MoQSessionTest, SubscribeDoneStreamCount) {
  co_await setupMoQSession();
  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([this, pub, sub] {
      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamOpened())
          .Times(2);
      EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
          .Times(2);
      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed())
          .Times(2);
      EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
          .Times(2);
      pub->objectStream(
          ObjectHeader(kUselessAlias, 0, 0, 0, 0, 10),
          moxygen::test::makeBuf(10));
      auto sgp = pub->beginSubgroup(0, 1, 0).value();
      sgp->object(1, moxygen::test::makeBuf(10));
      sgp->object(2, moxygen::test::makeBuf(10), noExtensions(), true);
      pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
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

CO_TEST_P_X(MoQSessionTest, SubscribeDoneFromSubscribe) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    co_return makeSubscribeOkResult(sub);
  });
  expectSubscribeDone();
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, SubscribeDoneAPIErrors) {
  co_await setupMoQSession();
  expectSubscribe([](auto sub, auto pub) -> TaskSubscribeResult {
    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    // All these APIs fail after SUBSCRIBE_DONE
    EXPECT_EQ(
        pub->beginSubgroup(1, 1, 1).error().code, MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->awaitStreamCredit().error().code, MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->datagram(
               ObjectHeader(kUselessAlias, 2, 2, 2, 2, 10),
               moxygen::test::makeBuf(10))
            .error()
            .code,
        MoQPublishError::API_ERROR);
    EXPECT_EQ(
        pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID))
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

CO_TEST_P_X(MoQSessionTest, Announce) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverSubscriber, announce(_, _))
      .WillOnce(testing::Invoke(
          [](auto ann, auto /* announceCallback */)
              -> folly::coro::Task<Subscriber::AnnounceResult> {
            co_return makeAnnounceOkResult(ann);
          }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onAnnounceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onAnnounceSuccess());
  EXPECT_CALL(*clientPublisherStatsCallback_, recordAnnounceLatency(_));
  auto announceResult = co_await clientSession_->announce(getAnnounce());
  EXPECT_FALSE(announceResult.hasError());
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, Unannounce) {
  co_await setupMoQSession();

  std::shared_ptr<MockAnnounceHandle> mockAnnounceHandle;
  EXPECT_CALL(*serverSubscriber, announce(_, _))
      .WillOnce(testing::Invoke(
          [&mockAnnounceHandle](auto ann, auto /* announceCallback */)
              -> folly::coro::Task<Subscriber::AnnounceResult> {
            mockAnnounceHandle = std::make_shared<MockAnnounceHandle>(
                AnnounceOk({ann.requestID, ann.trackNamespace}));
            Subscriber::AnnounceResult announceResult(mockAnnounceHandle);
            co_return announceResult;
          }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onAnnounceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onAnnounceSuccess());
  auto announceResult = co_await clientSession_->announce(getAnnounce());
  EXPECT_FALSE(announceResult.hasError());
  auto announceHandle = announceResult.value();
  EXPECT_CALL(*clientPublisherStatsCallback_, onUnannounce());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onUnannounce());
  EXPECT_CALL(*mockAnnounceHandle, unannounce());
  announceHandle->unannounce();
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, AnnounceCancel) {
  co_await setupMoQSession();

  std::shared_ptr<MockAnnounceHandle> mockAnnounceHandle;
  std::shared_ptr<moxygen::MoQSession::AnnounceCallback> announceCallback;
  EXPECT_CALL(*serverSubscriber, announce(_, _))
      .WillOnce(testing::Invoke(
          [&mockAnnounceHandle, &announceCallback](
              auto ann, auto announceCallbackIn)
              -> folly::coro::Task<Subscriber::AnnounceResult> {
            announceCallback = announceCallbackIn;
            mockAnnounceHandle = std::make_shared<MockAnnounceHandle>(
                AnnounceOk({ann.requestID, ann.trackNamespace}));
            Subscriber::AnnounceResult announceResult(mockAnnounceHandle);
            co_return announceResult;
          }));

  EXPECT_CALL(*clientPublisherStatsCallback_, onAnnounceSuccess());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onAnnounceSuccess());
  auto mockAnnounceCallback = std::make_shared<MockAnnounceCallback>();
  auto announceResult =
      co_await clientSession_->announce(getAnnounce(), mockAnnounceCallback);
  EXPECT_FALSE(announceResult.hasError());
  EXPECT_CALL(*clientPublisherStatsCallback_, onAnnounceCancel());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onAnnounceCancel());

  folly::coro::Baton barricade;
  EXPECT_CALL(*mockAnnounceCallback, announceCancel(_, _))
      .WillOnce(testing::Invoke(
          [&barricade](moxygen::AnnounceErrorCode, std::string) {
            barricade.post();
            return;
          }));
  announceCallback->announceCancel(
      AnnounceErrorCode::UNINTERESTED, "Not interested!");

  co_await barricade;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, SubscribeAndUnsubscribeAnnounces) {
  co_await setupMoQSession();

  std::shared_ptr<MockSubscribeAnnouncesHandle> mockSubscribeAnnouncesHandle;
  EXPECT_CALL(*serverPublisher, subscribeAnnounces(_))
      .WillOnce(testing::Invoke(
          [&mockSubscribeAnnouncesHandle](auto subAnn)
              -> folly::coro::Task<Publisher::SubscribeAnnouncesResult> {
            mockSubscribeAnnouncesHandle =
                std::make_shared<MockSubscribeAnnouncesHandle>(
                    SubscribeAnnouncesOk(
                        {RequestID(0), subAnn.trackNamespacePrefix}));
            co_return mockSubscribeAnnouncesHandle;
          }));

  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeAnnouncesSuccess());
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscribeAnnouncesSuccess());
  auto announceResult =
      co_await clientSession_->subscribeAnnounces(getSubscribeAnnounces());
  EXPECT_FALSE(announceResult.hasError());

  EXPECT_CALL(*clientSubscriberStatsCallback_, onUnsubscribeAnnounces());
  EXPECT_CALL(*serverPublisherStatsCallback_, onUnsubscribeAnnounces());
  EXPECT_CALL(*mockSubscribeAnnouncesHandle, unsubscribeAnnounces());
  announceResult.value()->unsubscribeAnnounces();
  co_await folly::coro::co_reschedule_on_current_executor;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, SubscribeAnnouncesError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverPublisher, subscribeAnnounces(_))
      .WillOnce(testing::Invoke(
          [](auto subAnn)
              -> folly::coro::Task<Publisher::SubscribeAnnouncesResult> {
            SubscribeAnnouncesError subAnnError{
                subAnn.requestID,
                subAnn.trackNamespacePrefix,
                SubscribeAnnouncesErrorCode::NOT_SUPPORTED,
                "not supported"};
            co_return folly::makeUnexpected(subAnnError);
          }));

  EXPECT_CALL(
      *clientSubscriberStatsCallback_,
      onSubscribeAnnouncesError(SubscribeAnnouncesErrorCode::NOT_SUPPORTED));
  EXPECT_CALL(
      *serverPublisherStatsCallback_,
      onSubscribeAnnouncesError(SubscribeAnnouncesErrorCode::NOT_SUPPORTED));
  auto subAnnResult =
      co_await clientSession_->subscribeAnnounces(getSubscribeAnnounces());
  EXPECT_TRUE(subAnnResult.hasError());

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, TooFarBehindOneSubgroup) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add(
        [this, pub, sub, serverWt = serverWt_.get(), eventBase = &eventBase_] {
          EXPECT_CALL(
              *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
          EXPECT_CALL(
              *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
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

  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
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

CO_TEST_P_X(MoQSessionTest, FreeUpBufferSpaceOneSubgroup) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([this, pub, sub, serverWt = serverWt_.get()] {
      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamOpened());
      EXPECT_CALL(
          *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
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
      pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    });
    co_return makeSubscribeOkResult(sub);
  });

  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
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

CO_TEST_P_X(MoQSessionTest, TooFarBehindMultipleSubgroups) {
  co_await setupMoQSession();

  MoQSettings moqSettings;
  moqSettings.bufferingThresholds.perSubscription = 100;
  serverSession_->setMoqSettings(moqSettings);

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    eventBase_.add([this,
                    pub,
                    sub,
                    serverWt = serverWt_.get(),
                    eventBase = &eventBase_] {
      std::vector<std::shared_ptr<SubgroupConsumer>> subgroupConsumers;

      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamOpened())
          .Times(3);
      EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamOpened())
          .Times(3);
      for (uint32_t subgroupId = 0; subgroupId < 3; subgroupId++) {
        subgroupConsumers.push_back(
            pub->beginSubgroup(0, subgroupId, 0).value());
        auto objectResult = subgroupConsumers[subgroupId]->object(
            0, moxygen::test::makeBuf(10));
        EXPECT_TRUE(objectResult.hasValue());
      }

      EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed())
          .Times(3);
      EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed())
          .Times(3);

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

CO_TEST_P_X(MoQSessionTest, PublisherAliveUntilAllBytesDelivered) {
  co_await setupMoQSession();
  folly::coro::Baton barricade;
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  expectSubscribe(
      [this, &subgroupConsumer](auto sub, auto pub) -> TaskSubscribeResult {
        eventBase_.add([this, pub, sub, &subgroupConsumer] {
          EXPECT_CALL(
              *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
          EXPECT_CALL(
              *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
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
          EXPECT_CALL(
              *serverPublisherStatsCallback_, onSubscriptionStreamClosed());
          EXPECT_CALL(
              *clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
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

CO_TEST_P_X(V11PlusTests, TrackStatusWithAuthorizationToken) {
  co_await setupMoQSession();
  EXPECT_CALL(*serverPublisherStatsCallback_, onTrackStatus());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onTrackStatus());
  EXPECT_CALL(*serverPublisher, trackStatus(_))
      .WillOnce(testing::Invoke(
          [this](TrackStatusRequest request)
              -> folly::coro::Task<Publisher::TrackStatusResult> {
            EXPECT_EQ(request.params.size(), 5);
            auto verifyParam = [this](
                                   const auto& param,
                                   const std::string& expectedTokenValue) {
              return param.key ==
                  getAuthorizationParamKey(getServerSelectedVersion()) &&
                  param.asAuthToken.tokenType == 0 &&
                  param.asAuthToken.tokenValue == expectedTokenValue;
            };

            EXPECT_TRUE(verifyParam(request.params.at(0), "abc"));
            EXPECT_TRUE(
                verifyParam(request.params.at(1), std::string(20, 'x')));
            EXPECT_TRUE(verifyParam(request.params.at(2), "abcd"));
            EXPECT_TRUE(verifyParam(request.params.at(3), "abcd"));
            EXPECT_TRUE(verifyParam(request.params.at(4), "xyzw"))
                << "'" << request.params.at(4).asAuthToken.tokenValue;
            co_return Publisher::TrackStatusResult{
                request.requestID,
                request.fullTrackName,
                TrackStatusCode::IN_PROGRESS,
                AbsoluteLocation{},
                {}};
          }));
  TrackStatusRequest request = getTrackStatusRequest();
  auto addAuthToken = [this](auto& params, const AuthToken& token) {
    params.push_back(
        {getAuthorizationParamKey(getServerSelectedVersion()), "", 0, token});
  };

  addAuthToken(request.params, {0, "abc", AuthToken::DontRegister});
  addAuthToken(request.params, {0, std::string(20, 'x'), AuthToken::Register});
  addAuthToken(request.params, {0, "abcd", AuthToken::Register});
  addAuthToken(request.params, {0, "abcd", AuthToken::Register});
  addAuthToken(request.params, {0, "xyzw", AuthToken::Register});
  auto res = co_await clientSession_->trackStatus(request);
  EXPECT_EQ(res.statusCode, TrackStatusCode::IN_PROGRESS);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, SubscribeWithParams) {
  co_await setupMoQSession();

  expectSubscribe([this](auto sub, auto pub) -> TaskSubscribeResult {
    EXPECT_EQ(sub.params.size(), 2);
    EXPECT_EQ(
        sub.params.at(0).key,
        getDeliveryTimeoutParamKey(getServerSelectedVersion()));
    EXPECT_EQ(sub.params.at(0).asUint64, 5000);
    EXPECT_EQ(
        sub.params.at(1).key,
        getAuthorizationParamKey(getServerSelectedVersion()));
    if (getServerSelectedVersion() < kVersionDraft11) {
      EXPECT_EQ(sub.params.at(1).asString, "auth_token_value");
    } else {
      EXPECT_EQ(sub.params.at(1).asAuthToken.tokenValue, "auth_token_value");
    }

    pub->subscribeDone(getTrackEndedSubscribeDone(sub.requestID));
    co_return makeSubscribeOkResult(sub);
  });

  expectSubscribeDone();

  SubscribeRequest subscribeRequest = getSubscribe(kTestTrackName);
  subscribeRequest.params.push_back(
      {getDeliveryTimeoutParamKey(getServerSelectedVersion()), "", 5000, {}});
  subscribeRequest.params.push_back(
      getAuthParam(getServerSelectedVersion(), "auth_token_value"));

  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_FALSE(res.hasError());
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, Unsubscribe) {
  co_await setupMoQSession();
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  expectSubscribe(
      [this, &subgroupConsumer, &trackConsumer, &mockSubscriptionHandle](
          auto sub, auto pub) -> TaskSubscribeResult {
        trackConsumer = pub;
        EXPECT_CALL(
            *serverPublisherStatsCallback_, onSubscriptionStreamOpened());
        EXPECT_CALL(
            *clientSubscriberStatsCallback_, onSubscriptionStreamOpened());
        auto pubResult = pub->beginSubgroup(0, 0, 0);
        EXPECT_FALSE(pubResult.hasError());
        subgroupConsumer = pubResult.value();
        subgroupConsumer->object(
            0, moxygen::test::makeBuf(10), Extensions(), false);
        mockSubscriptionHandle =
            makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
        co_return mockSubscriptionHandle;
      });
  auto subscribeRequest = getSubscribe(kTestTrackName);
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscribeSuccess());
  folly::coro::Baton subgroupCreated;
  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Invoke([&]() {
        subgroupCreated.post();
        return sg;
      }));
  EXPECT_CALL(*sg, object(_, _, _, _))
      .WillRepeatedly(testing::Return(folly::unit));
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  co_await subgroupCreated;
  auto subscribeHandler = res.value();
  folly::coro::Baton unsubscribeInvoked;
  EXPECT_CALL(*clientSubscriberStatsCallback_, onUnsubscribe());
  EXPECT_CALL(*serverPublisherStatsCallback_, onUnsubscribe());
  EXPECT_CALL(*mockSubscriptionHandle, unsubscribe)
      .WillOnce(testing::Invoke([&]() { unsubscribeInvoked.post(); }));
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  subscribeHandler->unsubscribe();
  co_await unsubscribeInvoked;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, SubscribeException) {
  co_await setupMoQSession();
  std::shared_ptr<SubgroupConsumer> subgroupConsumer = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  std::shared_ptr<MockSubscriptionHandle> mockSubscriptionHandle = nullptr;
  EXPECT_CALL(
      *getPublisher(MoQControlCodec::Direction::SERVER), subscribe(_, _))
      .WillOnce(testing::Invoke(
          [&](SubscribeRequest /* sub */,
              std::shared_ptr<TrackConsumer> /* pub */) -> TaskSubscribeResult {
            co_yield folly::coro::co_error(folly::exception_wrapper(
                std::runtime_error("Unsubscribe unsuccessful")));
          }));
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto res =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_TRUE(res.hasError());
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, NoPublishHandler) {
  co_await setupMoQSession();
  serverSession_->setPublishHandler(nullptr);
  auto subAnnResult =
      co_await clientSession_->subscribeAnnounces(getSubscribeAnnounces());
  EXPECT_TRUE(subAnnResult.hasError());
  auto res = co_await clientSession_->trackStatus(getTrackStatusRequest());
  EXPECT_EQ(res.statusCode, TrackStatusCode::UNKNOWN);
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

CO_TEST_P_X(MoQSessionTest, ClientReceivesBidiStream) {
  serverWt_->createBidiStream();
  // Check that the client called stopSending and resetStream on the newly
  // created stream.
  EXPECT_TRUE(clientWt_->readHandles.begin()
                  ->second->stopSendingErrorCode()
                  .hasValue());
  EXPECT_TRUE(
      clientWt_->writeHandles.begin()->second->getWriteErr().hasValue());
  co_return;
}

CO_TEST_P_X(MoQSessionTest, AnnounceError) {
  co_await setupMoQSession();

  EXPECT_CALL(*serverSubscriber, announce(_, _))
      .WillOnce(testing::Invoke(
          [](auto ann, auto /* announceCallback */)
              -> folly::coro::Task<Subscriber::AnnounceResult> {
            co_return folly::makeUnexpected(AnnounceError{
                ann.requestID,
                ann.trackNamespace,
                AnnounceErrorCode::UNAUTHORIZED,
                "Unauthorized"});
          }));

  EXPECT_CALL(
      *clientPublisherStatsCallback_,
      onAnnounceError(AnnounceErrorCode::UNAUTHORIZED));
  EXPECT_CALL(
      *serverSubscriberStatsCallback_,
      onAnnounceError(AnnounceErrorCode::UNAUTHORIZED));

  auto announceResult = co_await clientSession_->announce(getAnnounce());
  EXPECT_TRUE(announceResult.hasError());
  EXPECT_EQ(announceResult.error().errorCode, AnnounceErrorCode::UNAUTHORIZED);

  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
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

CO_TEST_P_X(MoQSessionTest, SubscribeDuringDrain) {
  co_await setupMoQSession();

  // Make a FETCH request so that we don't immediately close when drain()
  // is called.
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

  EXPECT_CALL(
      *fetchCallback_, object(0, 0, 0, HasChainDataLengthOf(100), _, true));
  auto res =
      co_await clientSession_->fetch(getFetch({0, 0}, {0, 1}), fetchCallback_);
  EXPECT_FALSE(res.hasError());

  clientSession_->drain();

  // Attempting to subscribe during the drain should return an error
  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto subscribeRes =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);
  EXPECT_TRUE(subscribeRes.hasError());
  EXPECT_EQ(subscribeRes.error().errorCode, SubscribeErrorCode::INTERNAL_ERROR);
}

CO_TEST_P_X(MoQSessionTest, TestOnObjectPayload) {
  co_await setupMoQSession();

  std::shared_ptr<SubgroupConsumer> subgroupPublisher = nullptr;
  std::shared_ptr<TrackConsumer> trackConsumer = nullptr;
  expectSubscribe([&](auto sub, auto pub) -> TaskSubscribeResult {
    auto sgp = pub->beginSubgroup(0, 0, 0).value();
    subgroupPublisher = sgp;
    trackConsumer = pub;
    sgp->beginObject(0, 100, test::makeBuf(10)).hasValue();
    co_return makeSubscribeOkResult(sub, AbsoluteLocation{0, 0});
  });

  auto sg = std::make_shared<testing::StrictMock<MockSubgroupConsumer>>();
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg));

  folly::coro::Baton receivedBeginObject;
  EXPECT_CALL(*sg, beginObject(0, _, _, _))
      .WillOnce(testing::Invoke([&receivedBeginObject]() {
        receivedBeginObject.post();
        return folly::unit;
      }));

  auto subscribeRequest = getSubscribe(kTestTrackName);
  auto subscribeRes =
      co_await clientSession_->subscribe(subscribeRequest, subscribeCallback_);

  co_await receivedBeginObject;

  auto payloadSendResult = subgroupPublisher->objectPayload(
      folly::IOBuf::copyBuffer(std::string(90, 'x')), true);
  EXPECT_TRUE(payloadSendResult.hasValue());
  folly::coro::Baton receivedObjectPayload;
  EXPECT_CALL(*sg, objectPayload(_, _))
      .WillOnce(testing::Invoke([&receivedObjectPayload]() {
        receivedObjectPayload.post();
        return ObjectPublishStatus::DONE;
      }));
  EXPECT_CALL(*sg, endOfSubgroup());
  co_await receivedObjectPayload;

  expectSubscribeDone();
  trackConsumer->subscribeDone(
      getTrackEndedSubscribeDone(subscribeRequest.requestID));
  co_await subscribeDone_;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

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
// uni stream or datagram before setup complete
