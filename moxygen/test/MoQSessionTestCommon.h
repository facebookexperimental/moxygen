/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Singleton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQClientBase.h>
#include <moxygen/relay/MoQRelayClient.h>
#include "moxygen/MoQSession.h"

#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/test/Mocks.h>
#include <moxygen/test/TestHelpers.h>
#include <moxygen/test/TestUtils.h>
#include "moxygen/MoQRelaySession.h"

namespace moxygen { namespace test {

// Constants
extern const size_t kTestMaxRequestID;
extern const FullTrackName kTestTrackName;
extern const TrackAlias kUselessAlias;

// Matcher for chain data length
MATCHER_P(HasChainDataLengthOf, n, "") {
  return arg->computeChainDataLength() == uint64_t(n);
}

// Helper functions
std::shared_ptr<MockFetchHandle> makeFetchOkResult(
    const Fetch& fetch,
    const AbsoluteLocation& location);

std::shared_ptr<MockSubscriptionHandle> makeSubscribeOkResult(
    const SubscribeRequest& sub,
    const folly::Optional<AbsoluteLocation>& largest = folly::none,
    const folly::Optional<uint8_t>& publisherPriority = folly::none);

TrackStatusOk makeTrackStatusOkResult(
    const TrackStatus& req,
    const folly::Optional<AbsoluteLocation>& largest = folly::none);

inline Publisher::SubscribeAnnouncesResult makeSubscribeAnnouncesOkResult(
    const auto& subAnn) {
  return std::make_shared<MockSubscribeAnnouncesHandle>(
      SubscribeAnnouncesOk({RequestID(0), subAnn.trackNamespacePrefix}));
}

inline Subscriber::AnnounceResult makeAnnounceOkResult(const auto& ann) {
  return std::make_shared<MockAnnounceHandle>(AnnounceOk({ann.requestID}));
}

inline Subscriber::PublishResult makePublishOkResult(
    const auto& pub,
    bool expectDone = true) {
  auto mockConsumer = std::make_shared<MockTrackConsumer>();
  EXPECT_CALL(*mockConsumer, setTrackAlias(testing::_))
      .WillRepeatedly(
          testing::Return(
              folly::Expected<folly::Unit, MoQPublishError>(folly::unit)));
  if (expectDone) {
    EXPECT_CALL(*mockConsumer, subscribeDone(testing::_))
        .WillOnce(testing::Return(folly::unit));
  }

  // Create PublishOk directly
  PublishOk publishOk{
      pub.requestID,
      true, // forward
      128,  // subscriber priority
      GroupOrder::Default,
      LocationType::LargestObject,
      folly::none,                       // start
      folly::make_optional(uint64_t(0)), // endGroup
  };

  // Create the reply task that returns the PublishOk
  auto replyTask =
      folly::coro::makeTask<folly::Expected<PublishOk, PublishError>>(
          std::move(publishOk));

  return Subscriber::PublishConsumerAndReplyTask{
      std::static_pointer_cast<TrackConsumer>(mockConsumer),
      std::move(replyTask)};
}

Fetch getFetch(AbsoluteLocation start, AbsoluteLocation end);

SubscribeRequest getSubscribe(const FullTrackName& ftn);

SubscribeDone getTrackEndedSubscribeDone(RequestID id);

TrackStatus getTrackStatus();

moxygen::SubscribeAnnounces getSubscribeAnnounces();

moxygen::Announce getAnnounce();

std::shared_ptr<MockSubscriptionHandle> makePublishHandle();

// Helper to set up mock expectations for subscribeUpdate
void expectSubscribeUpdate(
    std::shared_ptr<MockSubscriptionHandle> mockHandle,
    folly::coro::Baton& baton);

// Helper class to build a vector of Parameter for tests
class ParamBuilder {
 public:
  ParamBuilder() = default;

  // Add a uint64_t parameter
  ParamBuilder& add(TrackRequestParamKey key, uint64_t value);

  // Add a string parameter (for AUTHORIZATION_TOKEN)
  ParamBuilder& add(TrackRequestParamKey key, const std::string& value);

  std::vector<Parameter> build();

 private:
  std::vector<Parameter> params_;
};

struct VersionParams {
  std::vector<uint64_t> clientVersions;
  uint64_t serverVersion;

  VersionParams(std::vector<uint64_t> cv, uint64_t sv);
};

std::vector<VersionParams> getSupportedVersionParams();

// Timeout callback to prevent tests from hanging indefinitely
class TestTimeoutCallback : public folly::HHWheelTimer::Callback {
 public:
  void timeoutExpired() noexcept override;
};

// Main test fixture
class MoQSessionTest : public testing::TestWithParam<VersionParams>,
                       public MoQSession::ServerSetupCallback {
 public:
  void SetUp() override;
  void TearDown() override;

  folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
      const ClientSetup& clientSetup,
      uint64_t negotiatedVersion,
      std::shared_ptr<MoQSession> session) override;

  folly::Try<ServerSetup> onClientSetup(
      ClientSetup setup,
      const std::shared_ptr<MoQSession>&) override;

  virtual folly::coro::Task<void> setupMoQSession();
  virtual folly::coro::Task<void> setupMoQSessionForPublish(
      uint64_t maxRequestID = 10);

  folly::DrivableExecutor* getExecutor();

 protected:
  using TaskFetchResult = folly::coro::Task<Publisher::FetchResult>;
  void expectFetch(
      const std::function<
          TaskFetchResult(Fetch, std::shared_ptr<FetchConsumer>)>& lambda,
      folly::Optional<FetchErrorCode> error = folly::none);

  void expectFetchSuccess();

  using TaskSubscribeResult = folly::coro::Task<Publisher::SubscribeResult>;

  std::shared_ptr<moxygen::MockSubscriberStats> getSubscriberStatsCallback(
      MoQControlCodec::Direction direction);

  std::shared_ptr<moxygen::MockPublisherStats> getPublisherStatsCallback(
      MoQControlCodec::Direction direction);

  std::shared_ptr<moxygen::MockPublisher> getPublisher(
      MoQControlCodec::Direction direction);

  std::shared_ptr<moxygen::MockSubscriber> getSubscriber(
      MoQControlCodec::Direction direction);

  MoQControlCodec::Direction oppositeDirection(
      MoQControlCodec::Direction direction);

  void expectSubscribe(
      const std::function<TaskSubscribeResult(
          const SubscribeRequest&,
          std::shared_ptr<TrackConsumer>)>& lambda,
      MoQControlCodec::Direction direction = MoQControlCodec::Direction::SERVER,
      const folly::Optional<SubscribeErrorCode>& error = folly::none);

  void expectSubscribeDone(
      MoQControlCodec::Direction recipient =
          MoQControlCodec::Direction::CLIENT);

  // GCC barfs when using struct brace initializers inside a coroutine?
  // Helper function to make ClientSetup with MAX_REQUEST_ID
  ClientSetup getClientSetup(uint64_t initialMaxRequestID);

  std::vector<uint64_t> getClientSupportedVersions();

  uint64_t getServerSelectedVersion();

  uint8_t getRequestIDMultiplier() const;

  // Helper for object delivery validation tests (defined in
  // MoQSessionObjectDeliveryTests.cpp)
  using TestLogicFn = std::function<void(
      const SubscribeRequest& sub,
      std::shared_ptr<TrackConsumer> pub,
      std::shared_ptr<SubgroupConsumer> sgp,
      std::shared_ptr<MockSubgroupConsumer> sgc)>;
  folly::coro::Task<void> publishValidationTest(TestLogicFn testLogic);

  folly::EventBase eventBase_;
  std::shared_ptr<MoQFollyExecutorImpl> MoQExecutor_;
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
  TrackAlias nextAlias_{12345};
  std::shared_ptr<testing::StrictMock<MockFetchConsumer>> fetchCallback_;
  std::shared_ptr<testing::StrictMock<MockTrackConsumer>> subscribeCallback_;
  folly::coro::Baton subscribeDone_;
  std::shared_ptr<MockSubscriberStats> clientSubscriberStatsCallback_;
  std::shared_ptr<MockPublisherStats> clientPublisherStatsCallback_;
  std::shared_ptr<MockSubscriberStats> serverSubscriberStatsCallback_;
  std::shared_ptr<MockPublisherStats> serverPublisherStatsCallback_;
  TestTimeoutCallback testTimeout_;
};

}} // namespace moxygen::test
