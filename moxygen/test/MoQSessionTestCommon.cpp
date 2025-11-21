/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQSessionTestCommon.h"

using namespace moxygen;
using testing::_;

namespace moxygen { namespace test {

// Constants
const size_t kTestMaxRequestID = 2;
const FullTrackName kTestTrackName{TrackNamespace{{"foo"}}, "bar"};
const TrackAlias kUselessAlias(std::numeric_limits<uint32_t>::max());

// Helper functions
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

std::shared_ptr<MockSubscriptionHandle> makeSubscribeOkResult(
    const SubscribeRequest& sub,
    const folly::Optional<AbsoluteLocation>& largest,
    const folly::Optional<uint8_t>& publisherPriority) {
  ParamBuilder paramBuilder;
  if (publisherPriority.has_value()) {
    paramBuilder.add(
        TrackRequestParamKey::PUBLISHER_PRIORITY, publisherPriority.value());
  }
  return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
      sub.requestID,
      TrackAlias(sub.requestID.value),
      std::chrono::milliseconds(0),
      GroupOrder::OldestFirst,
      largest,
      paramBuilder.build()});
}

TrackStatusOk makeTrackStatusOkResult(
    const TrackStatus& req,
    const folly::Optional<AbsoluteLocation>& largest) {
  return TrackStatusOk{
      req.requestID,
      TrackAlias(req.requestID.value),
      std::chrono::milliseconds(0),
      GroupOrder::OldestFirst,
      largest,
      {},
      {}, // fullTrackName
      {}  // statusCode
  };
}

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
      LocationType::LargestObject,
      folly::none,
      0,
      {}};
}

SubscribeDone getTrackEndedSubscribeDone(RequestID id) {
  return {id, SubscribeDoneStatusCode::TRACK_ENDED, 0, "end of track"};
}

TrackStatus getTrackStatus() {
  return TrackStatus{
      .requestID = RequestID(0),
      .fullTrackName = kTestTrackName,
      .groupOrder = GroupOrder::Default,
      .locType = LocationType::LargestObject,
      .endGroup = 0,
      .params = {}};
}

moxygen::SubscribeAnnounces getSubscribeAnnounces() {
  return SubscribeAnnounces{RequestID(0), TrackNamespace{{"foo"}}, {}};
}

moxygen::Announce getAnnounce() {
  return Announce{RequestID(0), TrackNamespace{{"foo"}}, {}};
}

std::shared_ptr<MockSubscriptionHandle> makePublishHandle() {
  return std::make_shared<MockSubscriptionHandle>(SubscribeOk{
      RequestID(0),
      TrackAlias(100),
      std::chrono::milliseconds(0), // expires
      GroupOrder::Default,          // groupOrder
      folly::none,                  // largest
      {}                            // params
  });
}

// ParamBuilder implementation
ParamBuilder& ParamBuilder::add(TrackRequestParamKey key, uint64_t value) {
  params_.insertParam(Parameter{folly::to_underlying(key), value});
  return *this;
}

ParamBuilder& ParamBuilder::add(
    TrackRequestParamKey key,
    const std::string& value) {
  if (key == TrackRequestParamKey::AUTHORIZATION_TOKEN) {
    params_.insertParam(
        Parameter{
            folly::to_underlying(key),
            AuthToken{0, value, AuthToken::DontRegister}});
  } else {
    params_.insertParam(Parameter{folly::to_underlying(key), value});
  }
  return *this;
}

TrackRequestParameters ParamBuilder::build() {
  return std::move(params_);
}

// VersionParams implementation
VersionParams::VersionParams(std::vector<uint64_t> cv, uint64_t sv)
    : clientVersions(std::move(cv)), serverVersion(sv) {}

std::vector<VersionParams> getSupportedVersionParams() {
  // In this, the client and the server will negotiate the exact same version
  // (the client vector will have just one element, and the server version will
  // be that element)
  std::vector<VersionParams> result;
  result.reserve(kSupportedVersions.size());
  for (auto supportedVersion : kSupportedVersions) {
    result.emplace_back(
        std::vector<uint64_t>{supportedVersion}, supportedVersion);
  }
  return result;
}

// TestTimeoutCallback implementation
void TestTimeoutCallback::timeoutExpired() noexcept {
  XLOG(FATAL) << "Test timeout expired after 10 seconds - test hung!";
}

// MoQSessionTest implementation
void MoQSessionTest::SetUp() {
  // Schedule timeout to crash test if it hangs
  eventBase_.timer().scheduleTimeout(&testTimeout_, std::chrono::seconds(10));
  MoQExecutor_ = std::make_shared<MoQFollyExecutorImpl>(&eventBase_);
  std::tie(clientWt_, serverWt_) =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  clientSession_ = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(clientWt_.get()),
      MoQExecutor_);
  serverWt_->setPeerHandler(clientSession_.get());

  serverSession_ = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(serverWt_.get()),
      *this,
      MoQExecutor_);
  clientWt_->setPeerHandler(serverSession_.get());

  fetchCallback_ = std::make_shared<testing::StrictMock<MockFetchConsumer>>();
  subscribeCallback_ =
      std::make_shared<testing::StrictMock<MockTrackConsumer>>();

  // Set default behavior for setTrackAlias to return success
  ON_CALL(*subscribeCallback_, setTrackAlias(_))
      .WillByDefault(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  EXPECT_CALL(*subscribeCallback_, setTrackAlias(_)).Times(testing::AtLeast(0));

  clientSubscriberStatsCallback_ = std::make_shared<MockSubscriberStats>();
  clientSession_->setSubscriberStatsCallback(clientSubscriberStatsCallback_);

  clientPublisherStatsCallback_ = std::make_shared<MockPublisherStats>();
  clientSession_->setPublisherStatsCallback(clientPublisherStatsCallback_);

  serverSubscriberStatsCallback_ = std::make_shared<MockSubscriberStats>();
  serverSession_->setSubscriberStatsCallback(serverSubscriberStatsCallback_);

  serverPublisherStatsCallback_ = std::make_shared<MockPublisherStats>();
  serverSession_->setPublisherStatsCallback(serverPublisherStatsCallback_);

  // For Draft15+, initialize version via ALPN since it's required
  if (getDraftMajorVersion(getServerSelectedVersion()) >= 15) {
    auto alpn = getAlpnFromVersion(getServerSelectedVersion());
    if (alpn.hasValue()) {
      clientSession_->validateAndSetVersionFromAlpn(alpn.value());
      serverSession_->validateAndSetVersionFromAlpn(alpn.value());
    }
  }
}

void MoQSessionTest::TearDown() {
  // Cancel the timeout to prevent false alarms after test completes
  testTimeout_.cancelTimeout();
}

folly::Expected<folly::Unit, SessionCloseErrorCode>
MoQSessionTest::validateAuthority(
    const ClientSetup& /* clientSetup */,
    uint64_t /* negotiatedVersion */,
    std::shared_ptr<MoQSession> /* session */) {
  // For test purposes, always return success
  return folly::unit;
}

folly::Try<ServerSetup> MoQSessionTest::onClientSetup(
    ClientSetup setup,
    const std::shared_ptr<MoQSession>&) {
  if (invalidVersion_) {
    return folly::Try<ServerSetup>(std::runtime_error("invalid version"));
  }

  // For Draft15+, supportedVersions is not included in CLIENT_SETUP
  if (getDraftMajorVersion(getServerSelectedVersion()) < 15) {
    EXPECT_EQ(setup.supportedVersions[0], getClientSupportedVersions()[0]);
  }
  EXPECT_EQ(setup.params.at(0).key, folly::to_underlying(SetupKey::PATH));
  EXPECT_EQ(setup.params.at(0).asString, "/foo");
  EXPECT_EQ(
      setup.params.at(1).key, folly::to_underlying(SetupKey::MAX_REQUEST_ID));
  EXPECT_EQ(setup.params.at(1).asUint64, initialMaxRequestID_);
  EXPECT_EQ(
      setup.params.at(2).key,
      folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE));
  if (failServerSetup_) {
    return folly::makeTryWith(
        []() -> ServerSetup { throw std::runtime_error("failed"); });
  }
  return folly::Try<ServerSetup>(ServerSetup{
      .selectedVersion = getServerSelectedVersion(),
      .params = {
          SetupParameter{
              folly::to_underlying(SetupKey::MAX_REQUEST_ID),
              initialMaxRequestID_},
          SetupParameter{
              folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE), 16}}});
}

folly::coro::Task<void> MoQSessionTest::setupMoQSession() {
  clientSession_->setPublishHandler(clientPublisher);
  clientSession_->setSubscribeHandler(clientSubscriber);
  clientSession_->start();
  serverSession_->setPublishHandler(serverPublisher);
  serverSession_->setSubscribeHandler(serverSubscriber);
  serverSession_->start();
  clientSession_->setServerMaxTokenCacheSizeGuess(1024);
  auto serverSetup =
      co_await clientSession_->setup(getClientSetup(initialMaxRequestID_));

  // For Draft15+, selectedVersion is negotiated via ALPN and not in
  // SERVER_SETUP
  if (getDraftMajorVersion(getServerSelectedVersion()) < 15) {
    EXPECT_EQ(serverSetup.selectedVersion, getServerSelectedVersion());
  }
  EXPECT_EQ(
      serverSetup.params.at(0).key,
      folly::to_underlying(SetupKey::MAX_REQUEST_ID));
  EXPECT_EQ(serverSetup.params.at(0).asUint64, initialMaxRequestID_);
}

folly::coro::Task<void> MoQSessionTest::setupMoQSessionForPublish(
    uint64_t maxRequestID) {
  // Set up expectations for stats callbacks to prevent GMOCK warnings
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublish())
      .WillRepeatedly(testing::Return());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishOk())
      .WillRepeatedly(testing::Return());
  EXPECT_CALL(*serverSubscriberStatsCallback_, onPublishError(testing::_))
      .WillRepeatedly(testing::Return());

  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishSuccess())
      .WillRepeatedly(testing::Return());
  EXPECT_CALL(*clientPublisherStatsCallback_, onPublishError(testing::_))
      .WillRepeatedly(testing::Return());
  EXPECT_CALL(*clientPublisherStatsCallback_, recordPublishLatency(testing::_))
      .WillRepeatedly(testing::Return());

  EXPECT_CALL(*serverPublisherStatsCallback_, onPublishSuccess())
      .WillRepeatedly(testing::Return());
  EXPECT_CALL(*serverPublisherStatsCallback_, onPublishError(testing::_))
      .WillRepeatedly(testing::Return());
  EXPECT_CALL(*serverPublisherStatsCallback_, recordPublishLatency(testing::_))
      .WillRepeatedly(testing::Return());

  clientSession_->setPublishHandler(clientPublisher);
  clientSession_->setSubscribeHandler(clientSubscriber);
  clientSession_->start();
  serverSession_->setPublishHandler(serverPublisher);
  serverSession_->setSubscribeHandler(serverSubscriber);
  serverSession_->start();
  auto serverSetup =
      co_await clientSession_->setup(getClientSetup(maxRequestID));

  // For Draft15+, selectedVersion is negotiated via ALPN and not in
  // SERVER_SETUP
  if (getDraftMajorVersion(getServerSelectedVersion()) < 15) {
    EXPECT_EQ(serverSetup.selectedVersion, getServerSelectedVersion());
  }
  EXPECT_EQ(
      serverSetup.params.at(0).key,
      folly::to_underlying(SetupKey::MAX_REQUEST_ID));
  EXPECT_EQ(serverSetup.params.at(0).asUint64, maxRequestID);
}

folly::DrivableExecutor* MoQSessionTest::getExecutor() {
  return &eventBase_;
}

void MoQSessionTest::expectFetch(
    const std::function<TaskFetchResult(Fetch, std::shared_ptr<FetchConsumer>)>&
        lambda,
    folly::Optional<FetchErrorCode> error) {
  if (error) {
    EXPECT_CALL(*serverPublisherStatsCallback_, onFetchError(*error))
        .RetiresOnSaturation();
  } else {
    EXPECT_CALL(*serverPublisherStatsCallback_, onFetchSuccess())
        .RetiresOnSaturation();
  }
  EXPECT_CALL(*serverPublisher, fetch(_, _))
      .WillOnce(lambda)
      .RetiresOnSaturation();
}

void MoQSessionTest::expectFetchSuccess() {
  EXPECT_CALL(*clientSubscriberStatsCallback_, onFetchSuccess());
}

std::shared_ptr<moxygen::MockSubscriberStats>
MoQSessionTest::getSubscriberStatsCallback(
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

std::shared_ptr<moxygen::MockPublisherStats>
MoQSessionTest::getPublisherStatsCallback(
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

std::shared_ptr<moxygen::MockPublisher> MoQSessionTest::getPublisher(
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

std::shared_ptr<moxygen::MockSubscriber> MoQSessionTest::getSubscriber(
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

MoQControlCodec::Direction MoQSessionTest::oppositeDirection(
    MoQControlCodec::Direction direction) {
  return (direction == MoQControlCodec::Direction::CLIENT)
      ? MoQControlCodec::Direction::SERVER
      : MoQControlCodec::Direction::CLIENT;
}

void MoQSessionTest::expectSubscribe(
    const std::function<TaskSubscribeResult(
        const SubscribeRequest&,
        std::shared_ptr<TrackConsumer>)>& lambda,
    MoQControlCodec::Direction direction,
    const folly::Optional<SubscribeErrorCode>& error) {
  EXPECT_CALL(*getPublisher(direction), subscribe(_, _))
      .WillOnce(
          [this, lambda, error, direction](
              auto sub, auto pub) -> TaskSubscribeResult {
            EXPECT_CALL(
                *getSubscriberStatsCallback(oppositeDirection(direction)),
                recordSubscribeLatency(_));
            pub->setTrackAlias(TrackAlias(sub.requestID.value));
            if (error) {
              EXPECT_CALL(
                  *getPublisherStatsCallback(direction),
                  onSubscribeError(*error))
                  .RetiresOnSaturation();
            } else {
              EXPECT_CALL(
                  *getPublisherStatsCallback(direction), onSubscribeSuccess())
                  .RetiresOnSaturation();
            }
            return lambda(sub, pub);
          })
      .RetiresOnSaturation();
}

void MoQSessionTest::expectSubscribeDone(MoQControlCodec::Direction recipient) {
  EXPECT_CALL(
      *getPublisherStatsCallback(oppositeDirection(recipient)),
      onSubscribeDone(_));
  EXPECT_CALL(*getSubscriberStatsCallback(recipient), onSubscribeDone(_));
  EXPECT_CALL(*subscribeCallback_, subscribeDone(_)).WillOnce([&] {
    subscribeDone_.post();
    return folly::unit;
  });
}

ClientSetup MoQSessionTest::getClientSetup(uint64_t initialMaxRequestID) {
  ClientSetup setup{
      .supportedVersions = getClientSupportedVersions(),
      .params = {
          SetupParameter{folly::to_underlying(SetupKey::PATH), "/foo"},
          SetupParameter{
              folly::to_underlying(SetupKey::MAX_REQUEST_ID),
              initialMaxRequestID},
          SetupParameter{
              folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE), 16}}};
  if (std::find(
          setup.supportedVersions.begin(),
          setup.supportedVersions.end(),
          kVersionDraft12) != setup.supportedVersions.end()) {
    setup.params.insertParam(getAuthParam(
        kVersionDraft12, "auth_token_value", 0, AuthToken::Register));
  }
  return setup;
}

std::vector<uint64_t> MoQSessionTest::getClientSupportedVersions() {
  return GetParam().clientVersions;
}

uint64_t MoQSessionTest::getServerSelectedVersion() {
  return GetParam().serverVersion;
}

uint8_t MoQSessionTest::getRequestIDMultiplier() const {
  return 2;
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

  folly::coro::Baton resetBaton;
  EXPECT_CALL(*subscribeCallback_, beginSubgroup(0, 0, 0))
      .WillOnce(testing::Return(sg1));
  EXPECT_CALL(*sg1, reset(ResetStreamErrorCode::INTERNAL_ERROR))
      .WillOnce([&](auto) { resetBaton.post(); });
  expectSubscribeDone();
  EXPECT_CALL(*serverPublisherStatsCallback_, onSubscriptionStreamClosed());
  EXPECT_CALL(*clientSubscriberStatsCallback_, onSubscriptionStreamClosed());
  auto res = co_await clientSession_->subscribe(
      getSubscribe(kTestTrackName), subscribeCallback_);
  co_await subscribeDone_;
  co_await resetBaton;
  clientSession_->close(SessionCloseErrorCode::NO_ERROR);
}

}} // namespace moxygen::test
