/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/MoQLaneManager.h"
#include "moxygen/lane_manager/MoQLaneRouter.h"

#include "moxygen/test/MockMoQSession.h"
#include "moxygen/test/Mocks.h"

#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Timeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

#include <stdexcept>

namespace moxygen::lane_manager { namespace {

using namespace std::chrono_literals;
using testing::_;
using testing::Invoke;
using testing::NiceMock;
using testing::Return;
using Kind = SwitchOutcome::Kind;

const TrackNamespace kBroadcast{std::vector<std::string>{"live", "1"}};
constexpr std::chrono::milliseconds kMaxWait{100};

const std::string kCatalog = R"({"tracks":[
{"name":"video0","role":"video","altGroup":0,"width":320,"height":176,"initRef":"v0"},
{"name":"video1","role":"video","altGroup":0,"width":1920,"height":1080,"initRef":"v1"},
{"name":"audio0","role":"audio","initRef":"a0"}],
"initDataList":[{"id":"v0","data":"djA="},{"id":"v1","data":"djE="},{"id":"a0","data":"YTA="}]})";

class MockSwitchingPolicy : public SwitchingPolicy {
 public:
  MOCK_METHOD(std::string, initialLane, (const SwitchingSet&), (override));
  MOCK_METHOD(
      std::optional<SwitchPlan>,
      select,
      (const SwitchingSet&, std::string_view, uint64_t, Clock::time_point),
      (override));
  MOCK_METHOD(void, onSwitchOutcome, (const SwitchOutcome&), (override));
};

SwitchPlan planTo(std::string lane, std::chrono::milliseconds maxWait) {
  SwitchPlan plan;
  plan.targetLane = std::move(lane);
  plan.maxWait = maxWait;
  return plan;
}

// One object the client got: its group, payload and init.
struct Sent {
  uint64_t group;
  std::string payload;
  std::string init;
  bool operator==(const Sent&) const = default;
};

std::string initOf(const Extensions& extensions) {
  for (const auto& extension : extensions.getMutableExtensions()) {
    if (extension.type == kInitPropertyType) {
      return extension.arrayValue->to<std::string>();
    }
  }
  return "";
}

class LaneManagerTest : public testing::Test {
 protected:
  void SetUp() override {
    // The proxy answers every SUBSCRIBE except failLane_'s, with largest {5,3};
    // heldLane_'s answer waits for heldReply_.
    ON_CALL(*proxy_, subscribe(_, _))
        .WillByDefault([this](
                           const SubscribeRequest& request,
                           std::shared_ptr<TrackConsumer> consumer) {
          return answerSubscribe(request, std::move(consumer));
        });
    // The catalog FETCH returns kCatalog; lane FETCHes are kept.
    ON_CALL(*proxy_, fetch(_, _))
        .WillByDefault([this](Fetch fetch, std::shared_ptr<FetchConsumer> c) {
          upstreamFetches_.push_back(fetch.fullTrackName);
          if (fetch.fullTrackName.trackName == "catalog") {
            c->object(0, 0, 0, folly::IOBuf::copyBuffer(servedCatalog_));
            c->endOfFetch();
          } else {
            laneFetch_ = std::move(fetch);
            laneFetchConsumer_ = std::move(c);
            if (failLaneFetch_) {
              return folly::coro::makeTask<Publisher::FetchResult>(
                  folly::makeUnexpected(
                      FetchError{
                          RequestID(3), FetchErrorCode::INTERNAL_ERROR, ""}));
            }
          }
          return folly::coro::makeTask<Publisher::FetchResult>(
              std::make_shared<MockFetchHandle>(FetchOk{}));
        });
    ON_CALL(*client_, setTrackAlias(_)).WillByDefault([this](TrackAlias alias) {
      alias_ = alias;
      return folly::unit;
    });
    ON_CALL(*policy_, initialLane(_))
        .WillByDefault([](const SwitchingSet& set) {
          return set.lanes.front().trackName;
        });
    ON_CALL(*policy_, onSwitchOutcome(_))
        .WillByDefault([this](const SwitchOutcome& outcome) {
          outcomes_.push_back(outcome.kind);
        });
    // The client keeps every object it gets in sent_.
    ON_CALL(*client_, beginSubgroup(_, _, _, _))
        .WillByDefault(
            [this](uint64_t group, uint64_t, Priority, BeginSubgroupOptions)
                -> folly::Expected<
                    std::shared_ptr<SubgroupConsumer>,
                    MoQPublishError> {
              auto subgroup =
                  std::make_shared<NiceMock<MockSubgroupConsumer>>();
              ON_CALL(*subgroup, endOfSubgroup())
                  .WillByDefault(Return(folly::unit));
              ON_CALL(*subgroup, object(_, _, _, _))
                  .WillByDefault([this, group](
                                     uint64_t,
                                     Payload payload,
                                     const Extensions& extensions,
                                     bool) {
                    sent_.push_back(
                        {group,
                         payload->to<std::string>(),
                         initOf(extensions)});
                    return folly::unit;
                  });
              return subgroup;
            });
  }

  folly::coro::Task<Publisher::SubscribeResult> answerSubscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) {
    upstreamSubscriptions_.push_back(request.fullTrackName);
    const auto name = request.fullTrackName.trackName;
    const auto requestContext = MoQSession::getRequestContext();
    subscribeSessions_[name].push_back(requestContext.sessionId);
    EXPECT_EQ(requestContext.sessionId, session_->sessionId());
    ++subscribeAttempts_[name];
    if (name == heldLane_) {
      co_await heldReply_;
    }
    if (const auto remaining = subscribeFailures_.find(name);
        remaining != subscribeFailures_.end() && remaining->second > 0) {
      --remaining->second;
      SubscribeError error{
          request.requestID, RequestErrorCode::EXCESSIVE_LOAD, "retry"};
      error.retryInterval = 1ms;
      co_return folly::makeUnexpected(std::move(error));
    }
    if (name == failLane_) {
      co_return folly::makeUnexpected(
          SubscribeError{
              request.requestID, SubscribeErrorCode::NOT_SUPPORTED, ""});
    }
    lanes_[name] = std::move(consumer);
    aliasAtSubscribe_[name] = alias_;
    SubscribeOk ok;
    ok.requestID = request.requestID;
    ok.trackAlias = TrackAlias(99);
    ok.largest = AbsoluteLocation{5, 3};
    handles_[name] = std::make_shared<NiceMock<MockSubscriptionHandle>>(ok);
    if (name == endBeforeSubscribeOkLane_) {
      lanes_.at(name)->publishDone(
          PublishDone{
              request.requestID,
              PublishDoneStatusCode::TRACK_ENDED,
              0,
              "ended"});
    }
    co_return handles_[name];
  }

  // Runs `task` as the client session's request, then the lane SUBSCRIBEs it
  // started.
  template <typename T>
  T run(folly::coro::Task<T> task) {
    folly::RequestContextScopeGuard guard;
    folly::RequestContext::get()->setContextData(
        folly::RequestToken("moq_session"),
        std::make_unique<MoQSession::MoQSessionRequestData>(session_));
    auto result = folly::coro::blockingWait(std::move(task), &evb_);
    for (size_t i = 0; i < 4; ++i) {
      evb_.loopOnce(EVLOOP_NONBLOCK);
    }
    return result;
  }

  Publisher::SubscribeResult subscribe(Role role, uint64_t requestID = 1) {
    auto request = SubscribeRequest::make(FullTrackName{kBroadcast, "r--abr"});
    request.requestID = requestID;
    return run(manager_->subscribe(request, client_, role, policy_));
  }

  // `lane` sends one object of `group`, carrying the lane's name.
  void publish(const std::string& lane, uint64_t group) {
    auto subgroup =
        lanes_.at(lane)->beginSubgroup(group, 0, kDefaultPriority, {});
    ASSERT_TRUE(subgroup.hasValue());
    subgroup.value()->object(0, folly::IOBuf::copyBuffer(lane));
  }

  folly::EventBase evb_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&evb_)};
  std::shared_ptr<test::MockMoQSession> session_{
      std::make_shared<NiceMock<test::MockMoQSession>>(executor_)};
  std::shared_ptr<NiceMock<MockPublisher>> proxy_{
      std::make_shared<NiceMock<MockPublisher>>()};
  std::shared_ptr<NiceMock<MockTrackConsumer>> client_{
      std::make_shared<NiceMock<MockTrackConsumer>>()};
  std::shared_ptr<NiceMock<MockSwitchingPolicy>> policy_{
      std::make_shared<NiceMock<MockSwitchingPolicy>>()};
  std::shared_ptr<MoQLaneManager> manager_{
      std::make_shared<MoQLaneManager>(proxy_, kBroadcast, kMaxWait)};
  std::string failLane_;
  std::string servedCatalog_{kCatalog};
  std::string heldLane_;
  folly::coro::Baton heldReply_;
  std::string endBeforeSubscribeOkLane_;
  folly::F14FastMap<std::string, size_t> subscribeAttempts_;
  folly::F14FastMap<std::string, std::vector<SessionId>> subscribeSessions_;
  folly::F14FastMap<std::string, size_t> subscribeFailures_;
  bool failLaneFetch_{false};
  std::optional<TrackAlias> alias_;
  folly::F14FastMap<std::string, std::shared_ptr<TrackConsumer>> lanes_;
  folly::F14FastMap<std::string, std::optional<TrackAlias>> aliasAtSubscribe_;
  folly::F14FastMap<std::string, std::shared_ptr<MockSubscriptionHandle>>
      handles_;
  std::optional<Fetch> laneFetch_;
  std::shared_ptr<FetchConsumer> laneFetchConsumer_;
  std::vector<Sent> sent_;
  std::vector<Kind> outcomes_;
  std::vector<FullTrackName> upstreamSubscriptions_;
  std::vector<FullTrackName> upstreamFetches_;
};

const std::string kBitrateCatalog = R"({"tracks":[
{"name":"video0","role":"video","altGroup":0,"codec":"avc1","bitrate":300000,"initRef":"v0"},
{"name":"video1","role":"video","altGroup":0,"codec":"avc1","bitrate":600000,"initRef":"v1"},
{"name":"audio0","role":"audio","bitrate":160000,"initRef":"a0"}],
"initDataList":[{"id":"v0","type":"inline","data":"djA="},
{"id":"v1","type":"inline","data":"djE="},{"id":"a0","type":"inline","data":"YTA="}]})";

class LaneManagerCatalogFetchFallbackTest : public LaneManagerTest {
 protected:
  void SetUp() override {
    LaneManagerTest::SetUp();
    EXPECT_CALL(*proxy_, fetch(_, _))
        .WillOnce(Invoke(
            this, &LaneManagerCatalogFetchFallbackTest::rejectCatalogFetch))
        .WillOnce(Invoke(
            this, &LaneManagerCatalogFetchFallbackTest::acceptLaneFetch));
  }

  folly::coro::Task<Publisher::FetchResult> rejectCatalogFetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer>) {
    EXPECT_EQ(fetch.fullTrackName.trackName, "catalog");
    upstreamFetches_.push_back(fetch.fullTrackName);
    catalogFetchStarted_.setValue();
    co_await releaseFailure_.getFuture();
    catalogFetchRejected_.setValue();
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::NOT_SUPPORTED,
            "fetch unsupported"});
  }

  folly::coro::Task<Publisher::FetchResult> acceptLaneFetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) {
    EXPECT_EQ(fetch.fullTrackName.trackName, "video0");
    upstreamFetches_.push_back(fetch.fullTrackName);
    laneFetch_ = std::move(fetch);
    laneFetchConsumer_ = std::move(consumer);
    return folly::coro::makeTask<Publisher::FetchResult>(
        std::make_shared<MockFetchHandle>(FetchOk{}));
  }

  folly::coro::Task<Publisher::SubscribeResult> subscribeRole(
      Role role,
      RequestID requestID) {
    auto request = SubscribeRequest::make(FullTrackName{kBroadcast, "r--abr"});
    request.requestID = requestID;
    return manager_->subscribe(std::move(request), client_, role, policy_);
  }

  folly::coro::Task<Publisher::FetchResult> fetchVideo(RequestID requestID) {
    Fetch fetch(requestID, RequestID(1), 0, FetchType::RELATIVE_JOINING);
    fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};
    return manager_->fetch(
        std::move(fetch),
        std::make_shared<NiceMock<MockFetchConsumer>>(),
        Role::Video);
  }

  folly::coro::Task<Publisher::FetchResult> fetchBeforeLiveCatalog() {
    co_await catalogFetchStarted_.getFuture();
    EXPECT_CALL(*handles_.at("catalog"), unsubscribe()).Times(0);
    auto fetching =
        folly::coro::co_withExecutor(executor_.get(), fetchVideo(RequestID(3)))
            .start();
    co_await folly::coro::co_reschedule_on_current_executor;
    releaseFailure_.setValue();
    co_await catalogFetchRejected_.getFuture();
    co_await folly::coro::co_reschedule_on_current_executor;
    EXPECT_TRUE(
        lanes_.at("catalog")
            ->objectStream(
                ObjectHeader(1, 0, 0), folly::IOBuf::copyBuffer(kCatalog))
            .hasValue());
    co_return co_await std::move(fetching);
  }

  folly::coro::SharedPromise<void> catalogFetchStarted_;
  folly::coro::SharedPromise<void> releaseFailure_;
  folly::coro::SharedPromise<void> catalogFetchRejected_;
};

TEST_F(
    LaneManagerCatalogFetchFallbackTest,
    LiveCatalogCompletesConcurrentAndLaterRequests) {
  auto results =
      run(folly::coro::timeout(
          folly::coro::collectAll(
              subscribeRole(Role::Video, RequestID(1)),
              subscribeRole(Role::Audio, RequestID(2)),
              fetchBeforeLiveCatalog()),
          1s));

  auto& video = std::get<0>(results);
  ASSERT_TRUE(video.hasValue());
  EXPECT_EQ(video.value()->subscribeOk().requestID, RequestID(1));
  const auto& audio = std::get<1>(results);
  ASSERT_TRUE(audio.hasValue());
  EXPECT_EQ(audio.value()->subscribeOk().requestID, RequestID(2));
  EXPECT_TRUE(std::get<2>(results).hasValue());

  video.value()->unsubscribe();
  auto laterSubscription = run(subscribeRole(Role::Video, RequestID(4)));
  ASSERT_TRUE(laterSubscription.hasValue());
  EXPECT_EQ(laterSubscription.value()->subscribeOk().requestID, RequestID(4));
  EXPECT_EQ(subscribeAttempts_.at("catalog"), 1);
  EXPECT_EQ(
      upstreamFetches_,
      (std::vector<FullTrackName>{
          FullTrackName{kBroadcast, "catalog"},
          FullTrackName{kBroadcast, "video0"}}));
  testing::Mock::VerifyAndClearExpectations(handles_.at("catalog").get());
}
TEST_F(LaneManagerTest, VirtualVideoTrackUsesTheOriginNamespace) {
  const TrackNamespace origin(std::vector<std::string>{"file", "demo"});
  const FullTrackName video{kBroadcast, "video"};
  LaneManagerAbrOptions abr;
  abr.immutableCatalog = true;
  MoQLaneRouter::RoutingOptions routing;
  routing.virtualVideoTrack = video;
  routing.policyFactory = [this, video](const FullTrackName& track, Role role) {
    EXPECT_EQ(track, video);
    EXPECT_EQ(role, Role::Video);
    return policy_;
  };
  routing.originNamespace = [origin](const TrackNamespace& ns) {
    return ns == kBroadcast ? origin : ns;
  };
  MoQLaneRouter router(
      proxy_,
      [this](Role role) {
        EXPECT_EQ(role, Role::Video);
        return policy_;
      },
      kMaxWait,
      std::move(abr),
      std::move(routing));
  auto catalog = SubscribeRequest::make(FullTrackName{kBroadcast, "catalog"});
  catalog.requestID = 0;
  ASSERT_TRUE(run(router.subscribe(catalog, client_)).hasValue());
  auto request = SubscribeRequest::make(video);
  request.requestID = 7;
  auto handle = run(router.subscribe(request, client_));
  ASSERT_TRUE(handle.hasValue());
  ASSERT_EQ(upstreamSubscriptions_.size(), 3);
  for (const auto& track : upstreamSubscriptions_) {
    EXPECT_EQ(track.trackNamespace, origin);
    EXPECT_NE(track.trackName, "video");
  }
  ASSERT_EQ(upstreamFetches_.size(), 1);
  EXPECT_EQ(upstreamFetches_[0], (FullTrackName{origin, "catalog"}));
  Fetch fetch(RequestID(9), RequestID(7), 0, FetchType::RELATIVE_JOINING);
  fetch.fullTrackName = video;
  ASSERT_TRUE(run(router.fetch(fetch, std::make_shared<MockFetchConsumer>()))
                  .hasValue());
  ASSERT_TRUE(laneFetch_);
  EXPECT_EQ(laneFetch_->fullTrackName, (FullTrackName{origin, "video0"}));
}

TEST_F(LaneManagerTest, UnconfiguredVideoTrackGoesToTheProxy) {
  MoQLaneRouter router(
      proxy_,
      [](Role) -> std::shared_ptr<SwitchingPolicy> {
        ADD_FAILURE() << "unconfigured video must not select lanes";
        return nullptr;
      },
      kMaxWait);
  const FullTrackName video{kBroadcast, "video"};
  auto request = SubscribeRequest::make(video);
  request.requestID = 7;
  ASSERT_TRUE(run(router.subscribe(request, client_)).hasValue());
  EXPECT_EQ(upstreamSubscriptions_, (std::vector<FullTrackName>{video}));
  EXPECT_TRUE(upstreamFetches_.empty());
}

TEST_F(LaneManagerTest, HooksSeeTheStartAndEachOutcome) {
  servedCatalog_ = kBitrateCatalog;
  std::vector<Kind> hookOutcomes;
  size_t starts = 0;
  size_t closes = 0;
  LaneManagerAbrOptions abr;
  abr.onStarted = [&](const FullTrackName&, const SwitchingSet&) { ++starts; };
  abr.onClosed = [&](const FullTrackName&) { ++closes; };
  abr.onOutcome = [&](const FullTrackName&, const SwitchOutcome& outcome) {
    hookOutcomes.push_back(outcome.kind);
  };
  manager_ = std::make_shared<MoQLaneManager>(
      proxy_, kBroadcast, kMaxWait, std::move(abr));
  ON_CALL(*policy_, select(_, "video0", _, _))
      .WillByDefault(Return(planTo("video1", 50ms)));
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());
  EXPECT_TRUE(subscribe(Role::Video, 2).hasError());
  EXPECT_EQ(starts, 1);

  auto old = lanes_.at("video0")->beginSubgroup(0, 0, 0, {});
  ASSERT_TRUE(old.hasValue());
  ASSERT_TRUE(
      old.value()->object(0, folly::IOBuf::copyBuffer("old")).hasValue());
  publish("video1", 180000);
  // The GOP already sent from "video0" keeps going after the switch.
  ASSERT_TRUE(
      old.value()->object(1, folly::IOBuf::copyBuffer("tail")).hasValue());
  EXPECT_EQ(
      sent_,
      (std::vector<Sent>{
          {0, "old", "v0"}, {180000, "video1", "v1"}, {0, "tail", ""}}));
  EXPECT_EQ(hookOutcomes, (std::vector<Kind>{Kind::Prepared, Kind::Committed}));

  handle.value()->unsubscribe();
  EXPECT_EQ(closes, 1);
}

TEST_F(LaneManagerTest, CatalogInitModeSendsNoInitProperties) {
  servedCatalog_ = kBitrateCatalog;
  LaneManagerAbrOptions abr;
  abr.initializationFromCatalog = true;
  manager_ = std::make_shared<MoQLaneManager>(
      proxy_, kBroadcast, kMaxWait, std::move(abr));
  ON_CALL(*policy_, select(_, "video0", _, _))
      .WillByDefault(Return(planTo("video1", 50ms)));
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());

  publish("video0", 0);
  publish("video1", 180000);
  EXPECT_EQ(
      sent_, (std::vector<Sent>{{0, "video0", ""}, {180000, "video1", ""}}));
}

TEST_F(LaneManagerTest, PolicyChangeSendsTheHeldGroupAndAsksAgain) {
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  publish("video0", 6);
  publish("video0", 7);
  ASSERT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}}));

  EXPECT_CALL(*policy_, select(_, "video0", 7, _))
      .WillOnce(Return(std::nullopt));
  manager_->onPolicyChanged(Role::Video);
  publish("video1", 7);
  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}, {7, "video0", ""}}));
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Cancelled}));
}

TEST_F(LaneManagerTest, HeldBufferOverflowEndsOnlyTheAffectedSubscription) {
  LaneManagerAbrOptions abr;
  abr.holdLimits.bytes = 128;
  manager_ = std::make_shared<MoQLaneManager>(
      proxy_, kBroadcast, kMaxWait, std::move(abr));
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());
  publish("video0", 6);
  auto held = lanes_.at("video0")->beginSubgroup(7, 0, 0, {});
  ASSERT_TRUE(held.hasValue());
  EXPECT_CALL(*client_, publishDone(_)).WillOnce(Return(folly::unit));
  EXPECT_CALL(*handles_.at("video0"), unsubscribe());
  EXPECT_CALL(*handles_.at("video1"), unsubscribe());

  auto result =
      held.value()->object(0, folly::IOBuf::copyBuffer(std::string(256, 'x')));
  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::TOO_FAR_BEHIND);
  evb_.loopOnce(EVLOOP_NONBLOCK);

  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}}));
  EXPECT_TRUE(subscribe(Role::Video, 2).hasValue());
}

TEST_F(LaneManagerTest, HeldBufferLimitCoversAllHeldSubgroups) {
  LaneManagerAbrOptions abr;
  abr.holdLimits.bytes = 200;
  manager_ = std::make_shared<MoQLaneManager>(
      proxy_, kBroadcast, kMaxWait, std::move(abr));
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());
  publish("video0", 6);
  auto first = lanes_.at("video0")->beginSubgroup(7, 0, 0, {});
  auto second = lanes_.at("video0")->beginSubgroup(7, 1, 0, {});
  ASSERT_TRUE(first.hasValue());
  ASSERT_TRUE(second.hasValue());
  ASSERT_TRUE(first.value()
                  ->object(0, folly::IOBuf::copyBuffer(std::string(60, 'a')))
                  .hasValue());
  EXPECT_CALL(*client_, publishDone(_)).WillOnce(Return(folly::unit));

  EXPECT_TRUE(second.value()
                  ->object(0, folly::IOBuf::copyBuffer(std::string(60, 'b')))
                  .hasError());
  evb_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}}));
}

TEST_F(LaneManagerTest, TooManyHeldSubgroupsEndTheSubscription) {
  LaneManagerAbrOptions abr;
  abr.holdLimits.subgroups = 1;
  manager_ = std::make_shared<MoQLaneManager>(
      proxy_, kBroadcast, kMaxWait, std::move(abr));
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());
  publish("video0", 6);
  ASSERT_TRUE(lanes_.at("video0")->beginSubgroup(7, 0, 0, {}).hasValue());
  EXPECT_CALL(*client_, publishDone(_)).WillOnce(Return(folly::unit));

  EXPECT_TRUE(lanes_.at("video0")->beginSubgroup(7, 1, 0, {}).hasError());
  evb_.loopOnce(EVLOOP_NONBLOCK);
}

TEST_F(LaneManagerTest, HeldReplayFailureIsReportedToTheClient) {
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 1ms)));
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());
  publish("video0", 6);
  publish("video0", 7);
  auto rejected = std::make_shared<NiceMock<MockSubgroupConsumer>>();
  ON_CALL(*rejected, object(_, _, _, _))
      .WillByDefault(Return(
          folly::makeUnexpected(
              MoQPublishError{
                  MoQPublishError::CANCELLED, "downstream closed"})));
  EXPECT_CALL(*client_, beginSubgroup(7, _, _, _)).WillOnce(Return(rejected));
  EXPECT_CALL(*client_, publishDone(_)).WillOnce(Return(folly::unit));

  evb_.loop();

  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}}));
  EXPECT_TRUE(subscribe(Role::Video, 2).hasValue());
}

TEST_F(LaneManagerTest, LanesNeedSubgroupStreams) {
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());
  ObjectHeader header;
  EXPECT_TRUE(lanes_.at("video0")
                  ->objectStream(header, folly::IOBuf::copyBuffer("x"))
                  .hasError());
  EXPECT_TRUE(lanes_.at("video0")
                  ->datagram(header, folly::IOBuf::copyBuffer("x"))
                  .hasError());
  EXPECT_TRUE(sent_.empty());
}

TEST_F(LaneManagerTest, MissingPolicyReleasesRoleBeforeAnyUpstreamWork) {
  auto request = SubscribeRequest::make(FullTrackName{kBroadcast, "video"});
  auto rejected =
      run(manager_->subscribe(request, client_, Role::Video, nullptr));
  ASSERT_TRUE(rejected.hasError());
  EXPECT_TRUE(upstreamSubscriptions_.empty());
  EXPECT_TRUE(upstreamFetches_.empty());
  EXPECT_TRUE(subscribe(Role::Video, 2).hasValue());
}

TEST_F(LaneManagerTest, SubscribeOkComesFromTheLaneUnderTheClientAlias) {
  auto handle = subscribe(Role::Video);
  ASSERT_TRUE(handle.hasValue());

  const auto& ok = handle.value()->subscribeOk();
  EXPECT_EQ(ok.requestID, RequestID(1));
  EXPECT_EQ(ok.trackAlias, TrackAlias(1));
  EXPECT_EQ(ok.largest, (AbsoluteLocation{5, 3}));
  // Set first, so lane objects that beat the SUBSCRIBE_OK can be forwarded.
  EXPECT_EQ(aliasAtSubscribe_.at("video0"), TrackAlias(1));
}

TEST_F(LaneManagerTest, ForwardsTheLaneWithTheInitOnTheFirstObject) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  auto clientSubgroup = std::make_shared<NiceMock<MockSubgroupConsumer>>();
  EXPECT_CALL(
      *client_,
      beginSubgroup(
          6,
          0,
          _,
          testing::Field(&BeginSubgroupOptions::includeExtensions, true)))
      .WillOnce(Return(clientSubgroup));
  std::vector<std::string> inits;
  EXPECT_CALL(*clientSubgroup, object(_, _, _, _))
      .Times(2)
      .WillRepeatedly(
          [&](uint64_t, Payload, const Extensions& extensions, bool) {
            inits.push_back(initOf(extensions));
            return folly::unit;
          });

  BeginSubgroupOptions options;
  options.includeExtensions = false;
  auto subgroup =
      lanes_.at("video0")->beginSubgroup(6, 0, kDefaultPriority, options);
  subgroup.value()->object(0, folly::IOBuf::copyBuffer("a"));
  subgroup.value()->object(1, folly::IOBuf::copyBuffer("b"));

  EXPECT_EQ(inits, (std::vector<std::string>{"v0", ""}));
}

TEST_F(LaneManagerTest, JoiningFetchGoesToTheInitialLaneWithItsInit) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  Fetch fetch(RequestID(3), RequestID(1), 0, FetchType::RELATIVE_JOINING);
  fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};
  auto client = std::make_shared<NiceMock<MockFetchConsumer>>();
  ASSERT_TRUE(
      folly::coro::blockingWait(manager_->fetch(fetch, client, Role::Video))
          .hasValue());

  EXPECT_EQ(laneFetch_->fullTrackName.trackName, "video0");
  EXPECT_EQ(
      std::get<JoiningFetch>(laneFetch_->args).joiningRequestID, RequestID(1));
  std::vector<std::string> inits;
  EXPECT_CALL(*client, object(_, _, _, _, _, _, _))
      .Times(2)
      .WillRepeatedly([&](uint64_t,
                          uint64_t,
                          uint64_t,
                          Payload,
                          const Extensions& extensions,
                          bool,
                          bool) {
        inits.push_back(initOf(extensions));
        return folly::unit;
      });
  laneFetchConsumer_->object(5, 0, 0, folly::IOBuf::copyBuffer("a"));
  laneFetchConsumer_->object(5, 0, 1, folly::IOBuf::copyBuffer("b"));
  EXPECT_EQ(inits, (std::vector<std::string>{"v0", ""}));
  EXPECT_EQ(subscribeAttempts_.at("video0"), 1);
}

TEST_F(LaneManagerTest, JoiningFetchPreservesClientRequestParameters) {
  auto video = subscribe(Role::Video, 42);
  ASSERT_TRUE(video.hasValue());
  Fetch fetch(
      RequestID(3),
      std::nullopt,
      2,
      FetchType::RELATIVE_JOINING,
      7,
      GroupOrder::NewestFirst,
      {Parameter{
          static_cast<uint64_t>(TrackRequestParamKey::AUTHORIZATION_TOKEN),
          AuthToken{0, "fetch-token", true}}});
  fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};
  fetch.includeProperties = false;

  ASSERT_TRUE(run(manager_->fetch(
                      std::move(fetch),
                      std::make_shared<NiceMock<MockFetchConsumer>>(),
                      Role::Video))
                  .hasValue());

  ASSERT_TRUE(laneFetch_);
  EXPECT_EQ(laneFetch_->requestID, RequestID(3));
  EXPECT_EQ(laneFetch_->fullTrackName, (FullTrackName{kBroadcast, "video0"}));
  const auto& joining = std::get<JoiningFetch>(laneFetch_->args);
  EXPECT_EQ(joining.joiningRequestID, RequestID(42));
  EXPECT_EQ(joining.joiningStart, 2);
  EXPECT_EQ(joining.fetchType, FetchType::RELATIVE_JOINING);
  EXPECT_EQ(laneFetch_->priority, 7);
  EXPECT_EQ(laneFetch_->groupOrder, GroupOrder::NewestFirst);
  EXPECT_FALSE(laneFetch_->includeProperties);
  ASSERT_EQ(laneFetch_->params.size(), 1);
  EXPECT_EQ(laneFetch_->params.at(0).asAuthToken.tokenValue, "fetch-token");
}

class LaneManagerLaneExceptionTest : public LaneManagerTest {
 protected:
  void SetUp() override {
    LaneManagerTest::SetUp();
    ON_CALL(*proxy_, subscribe(_, _))
        .WillByDefault(
            testing::Invoke(
                this, &LaneManagerLaneExceptionTest::answerWithException));
  }

  folly::coro::Task<Publisher::SubscribeResult> answerWithException(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) {
    if (request.fullTrackName.trackName != "catalog") {
      co_await folly::coro::co_reschedule_on_current_executor;
      if (failAll_ || request.fullTrackName.trackName == "video0") {
        co_yield folly::coro::co_error(
            std::runtime_error("lane subscribe failed"));
      }
      co_await folly::coro::co_reschedule_on_current_executor;
    }
    co_return co_await answerSubscribe(std::move(request), std::move(consumer));
  }

  folly::coro::Task<folly::Try<Publisher::SubscribeResult>> startVideo() {
    auto request = SubscribeRequest::make(FullTrackName{kBroadcast, "r--abr"});
    request.requestID = 1;
    co_return co_await folly::coro::co_awaitTry(
        folly::coro::timeout(
            manager_->subscribe(
                std::move(request), client_, Role::Video, policy_),
            5s));
  }

  bool failAll_{false};
};

TEST_F(LaneManagerLaneExceptionTest, LaneExceptionKeepsWaitingForAHealthyLane) {
  auto result = run(startVideo());
  ASSERT_TRUE(result.hasValue());
  ASSERT_TRUE(result->hasValue());

  publish("video1", 6);

  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video1", "v1"}}));
}

TEST_F(LaneManagerLaneExceptionTest, AllLaneExceptionsReturnSubscribeError) {
  failAll_ = true;

  auto result = run(startVideo());

  ASSERT_TRUE(result.hasValue());
  ASSERT_TRUE(result->hasError());
  EXPECT_EQ(result->error().requestID, RequestID(1));
  EXPECT_EQ(result->error().errorCode, RequestErrorCode::INTERNAL_ERROR);
  EXPECT_THAT(
      result->error().reasonPhrase,
      testing::HasSubstr("lane subscribe failed"));
}

TEST_F(LaneManagerTest, TransientInitialFailureRetriesBeforeSubscribeOk) {
  subscribeFailures_["audio0"] = 1;

  EXPECT_TRUE(subscribe(Role::Audio).hasValue());

  EXPECT_EQ(subscribeAttempts_.at("audio0"), 2);
}

TEST_F(LaneManagerTest, InitialTransientFailureDoesNotBlockAHealthyLane) {
  subscribeFailures_["video0"] = 1;

  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  ASSERT_TRUE(lanes_.contains("video1"));
  publish("video1", 6);

  EXPECT_EQ(sent_.back().payload, "video1");
}

TEST_F(LaneManagerTest, AdoptsAHealthyLaneBeforeTheFirstLiveGroup) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  EXPECT_CALL(*client_, publishDone(_)).Times(0);

  lanes_.at("video0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, ""});
  publish("video1", 6);

  EXPECT_EQ(sent_.back().payload, "video1");
}

class LaneManagerEarlyDataTest : public LaneManagerTest {
 protected:
  void TearDown() override {
    acceptPreferred_.post();
    acceptEarly_.post();
    evb_.loop();
    LaneManagerTest::TearDown();
  }

  void SetUp() override {
    LaneManagerTest::SetUp();
    ON_CALL(*policy_, initialLane(_)).WillByDefault(Return("video0"));
    ON_CALL(*proxy_, subscribe(_, _))
        .WillByDefault([this](
                           SubscribeRequest request,
                           std::shared_ptr<TrackConsumer> consumer) {
          return answerWithEarlyData(std::move(request), std::move(consumer));
        });
  }

  folly::coro::Task<Publisher::SubscribeResult> answerWithEarlyData(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) {
    const auto name = request.fullTrackName.trackName;
    if (name == "video0") {
      co_await acceptPreferred_;
    }
    auto result =
        co_await answerSubscribe(std::move(request), std::move(consumer));
    if (name == "video1") {
      publish("video1", 6);
      earlyData_.post();
      co_await acceptEarly_;
    }
    co_return result;
  }

  folly::coro::Task<void> releaseReplies(bool acceptEarly) {
    co_await earlyData_;
    if (acceptEarly) {
      acceptEarly_.post();
    }
    acceptPreferred_.post();
  }

  Publisher::SubscribeResult subscribeAfterEarlyData(bool acceptEarly) {
    auto request = SubscribeRequest::make(FullTrackName{kBroadcast, "r--abr"});
    request.requestID = 1;
    auto results =
        run(folly::coro::timeout(
            folly::coro::collectAll(
                manager_->subscribe(request, client_, Role::Video, policy_),
                releaseReplies(acceptEarly)),
            5s));
    return std::move(std::get<0>(results));
  }

  folly::coro::Baton acceptPreferred_;
  folly::coro::Baton acceptEarly_;
  folly::coro::Baton earlyData_;
};

TEST_F(LaneManagerEarlyDataTest, ReadyEarlyLaneRemainsTheInitialLane) {
  auto video = subscribeAfterEarlyData(true);
  ASSERT_TRUE(video.hasValue());
  ASSERT_EQ(sent_, (std::vector<Sent>{{6, "video1", "v1"}}));

  Fetch fetch(RequestID(3), RequestID(1), 0, FetchType::RELATIVE_JOINING);
  fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};
  ASSERT_TRUE(run(manager_->fetch(
                      std::move(fetch),
                      std::make_shared<NiceMock<MockFetchConsumer>>(),
                      Role::Video))
                  .hasValue());
  ASSERT_TRUE(laneFetch_);
  EXPECT_EQ(laneFetch_->fullTrackName.trackName, "video1");
  publish("video0", 6);
  publish("video1", 7);

  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video1", "v1"}, {7, "video1", ""}}));
  EXPECT_TRUE(outcomes_.empty());
}

TEST_F(LaneManagerEarlyDataTest, PendingEarlyLaneSwitchesAtTheNextGroup) {
  auto video = subscribeAfterEarlyData(false);
  ASSERT_TRUE(video.hasValue());
  ASSERT_EQ(sent_, (std::vector<Sent>{{6, "video1", "v1"}}));
  ASSERT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared}));

  publish("video0", 6);
  publish("video0", 7);
  publish("video1", 7);

  EXPECT_EQ(
      sent_, (std::vector<Sent>{{6, "video1", "v1"}, {7, "video0", "v0"}}));
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Committed}));
  acceptEarly_.post();
  evb_.loop();
}

TEST_F(LaneManagerTest, TrackEndBeforeSubscribeOkUsesAHealthyLane) {
  endBeforeSubscribeOkLane_ = "video0";
  EXPECT_CALL(*client_, publishDone(_)).Times(0);
  EXPECT_CALL(*policy_, initialLane(_))
      .WillOnce(Return("video0"))
      .WillOnce([](const SwitchingSet& set) {
        EXPECT_EQ(set.find("video0"), nullptr);
        return std::string("video1");
      });

  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  publish("video1", 6);

  EXPECT_EQ(subscribeAttempts_.at("video0"), 1);
  EXPECT_EQ(sent_.back().payload, "video1");
}

TEST_F(
    LaneManagerTest,
    ActiveTransientFailureRetriesWithoutEndingVirtualTrack) {
  auto audio = subscribe(Role::Audio);
  ASSERT_TRUE(audio.hasValue());
  publish("audio0", 6);
  EXPECT_CALL(*client_, publishDone(_)).Times(0);
  EXPECT_CALL(*policy_, select(_, _, _, _)).Times(0);

  lanes_.at("audio0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, ""});
  EXPECT_TRUE(testing::Mock::VerifyAndClearExpectations(policy_.get()));
  EXPECT_TRUE(subscribe(Role::Audio, 2).hasError());
  evb_.loop();

  EXPECT_EQ(subscribeAttempts_.at("audio0"), 2);
  EXPECT_EQ(
      subscribeSessions_.at("audio0"),
      (std::vector<SessionId>{session_->sessionId(), session_->sessionId()}));
  publish("audio0", 7);
  EXPECT_EQ(sent_.back().payload, "audio0");
}

TEST_F(LaneManagerTest, BackgroundTransientFailureRetries) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  publish("video0", 6);
  EXPECT_CALL(*client_, publishDone(_)).Times(0);
  EXPECT_CALL(*policy_, select(_, _, _, _))
      .WillOnce([](const SwitchingSet& set, auto, auto, auto) {
        EXPECT_EQ(set.find("video1"), nullptr);
        return std::nullopt;
      });
  lanes_.at("video1")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, ""});
  EXPECT_TRUE(testing::Mock::VerifyAndClearExpectations(policy_.get()));
  publish("video0", 7);
  evb_.loop();

  EXPECT_EQ(subscribeAttempts_.at("video1"), 2);
}

TEST_F(LaneManagerTest, UnsubscribeCancelsLaneRetry) {
  auto audio = subscribe(Role::Audio);
  ASSERT_TRUE(audio.hasValue());
  lanes_.at("audio0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, ""});

  audio.value()->unsubscribe();
  evb_.loop();

  EXPECT_EQ(subscribeAttempts_.at("audio0"), 1);
}

TEST_F(LaneManagerTest, PermanentRecoveryFailureEndsTheActiveSubscription) {
  auto audio = subscribe(Role::Audio);
  ASSERT_TRUE(audio.hasValue());
  failLane_ = "audio0";
  EXPECT_CALL(*client_, publishDone(_)).WillOnce(Return(folly::unit));

  lanes_.at("audio0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, ""});
  evb_.loop();

  failLane_.clear();
  EXPECT_TRUE(subscribe(Role::Audio, 2).hasValue());
}

TEST_F(LaneManagerTest, JoiningFetchFailureDoesNotEndTheSubscription) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  failLaneFetch_ = true;
  Fetch fetch(RequestID(3), RequestID(1), 0, FetchType::RELATIVE_JOINING);
  fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};

  EXPECT_TRUE(run(manager_->fetch(
                      fetch,
                      std::make_shared<NiceMock<MockFetchConsumer>>(),
                      Role::Video))
                  .hasError());

  EXPECT_TRUE(subscribe(Role::Video, 2).hasError());
  publish("video0", 6);
  EXPECT_EQ(sent_.back().payload, "video0");
}

TEST_F(LaneManagerTest, RejectsAbsoluteJoiningFetch) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  Fetch fetch(RequestID(3), RequestID(1), 0, FetchType::ABSOLUTE_JOINING);
  fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};

  auto result = run(manager_->fetch(
      fetch, std::make_shared<NiceMock<MockFetchConsumer>>(), Role::Video));

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, FetchErrorCode::NOT_SUPPORTED);
  EXPECT_FALSE(laneFetch_.has_value());
}

TEST_F(LaneManagerTest, PicksAnotherLaneWhenTheLaneSubscribeFails) {
  failLane_ = "video0";
  EXPECT_CALL(*policy_, initialLane(_))
      .WillOnce(Return("video0"))
      .WillOnce([](const SwitchingSet& set) {
        EXPECT_EQ(set.find("video0"), nullptr);
        return std::string("video1");
      });

  EXPECT_TRUE(subscribe(Role::Video).hasValue());
  EXPECT_TRUE(lanes_.contains("video1"));
}

TEST_F(LaneManagerTest, ACancelledSubscribeFreesTheRole) {
  EXPECT_CALL(*proxy_, subscribe(_, _))
      .WillOnce(testing::DoDefault()) // the catalog
      .WillOnce([](const auto&, const auto&) {
        return folly::coro::makeErrorTask<Publisher::SubscribeResult>(
            folly::OperationCancelled());
      })
      .WillRepeatedly(testing::DoDefault());

  EXPECT_THROW(subscribe(Role::Video), folly::OperationCancelled);
  EXPECT_TRUE(subscribe(Role::Video, 2).hasValue());
}

TEST_F(
    LaneManagerTest,
    TerminalCatalogSubscribePreservesErrorForLaterRequests) {
  EXPECT_CALL(*proxy_, subscribe(_, _)).WillOnce([](const auto&, const auto&) {
    return folly::coro::makeTask<Publisher::SubscribeResult>(
        folly::makeUnexpected(
            SubscribeError{
                RequestID(1),
                RequestErrorCode::UNAUTHORIZED,
                "catalog access denied",
                0ms}));
  });
  EXPECT_CALL(*proxy_, fetch(_, _)).Times(0);

  auto result = subscribe(Role::Video);
  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().requestID, RequestID(1));
  EXPECT_EQ(result.error().errorCode, RequestErrorCode::UNAUTHORIZED);
  EXPECT_EQ(result.error().reasonPhrase, "catalog access denied");
  EXPECT_EQ(result.error().retryInterval, 0ms);

  auto laterSubscription = subscribe(Role::Audio, 2);
  ASSERT_TRUE(laterSubscription.hasError());
  EXPECT_EQ(laterSubscription.error().requestID, RequestID(2));
  EXPECT_EQ(
      laterSubscription.error().errorCode, RequestErrorCode::UNAUTHORIZED);
  EXPECT_EQ(laterSubscription.error().reasonPhrase, "catalog access denied");
  EXPECT_EQ(laterSubscription.error().retryInterval, 0ms);

  Fetch fetch(RequestID(3), RequestID(1), 0, FetchType::RELATIVE_JOINING);
  fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};
  auto laterFetch = run(manager_->fetch(
      std::move(fetch),
      std::make_shared<NiceMock<MockFetchConsumer>>(),
      Role::Video));
  ASSERT_TRUE(laterFetch.hasError());
  EXPECT_EQ(laterFetch.error().requestID, RequestID(3));
  EXPECT_EQ(laterFetch.error().errorCode, RequestErrorCode::UNAUTHORIZED);
  EXPECT_EQ(laterFetch.error().reasonPhrase, "catalog access denied");
  EXPECT_EQ(laterFetch.error().retryInterval, 0ms);
}

TEST_F(LaneManagerTest, RejectsASecondSubscribeForTheSameRole) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());

  auto second = subscribe(Role::Video, 2);
  ASSERT_TRUE(second.hasError());
  EXPECT_EQ(
      second.error().errorCode, SubscribeErrorCode::DUPLICATE_SUBSCRIPTION);
}

TEST_F(LaneManagerTest, UnsubscribeReleasesLanesAndTheLastReleasesTheCatalog) {
  EXPECT_CALL(*proxy_, subscribe(_, _)).Times(4);
  auto video = subscribe(Role::Video, 1).value();
  auto audio = subscribe(Role::Audio, 2).value();

  EXPECT_CALL(*handles_.at("video0"), unsubscribe());
  EXPECT_CALL(*handles_.at("video1"), unsubscribe());
  video->unsubscribe();
  EXPECT_CALL(*handles_.at("audio0"), unsubscribe());
  audio->unsubscribe();

  EXPECT_CALL(*handles_.at("catalog"), unsubscribe());
  manager_.reset();
}

TEST_F(LaneManagerTest, LanePublishDoneEndsTheVirtualTrack) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());

  EXPECT_CALL(*client_, publishDone(_)).WillOnce(Return(folly::unit));
  lanes_.at("video0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, ""});
  evb_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(subscribeAttempts_.at("video0"), 1);

  EXPECT_TRUE(subscribe(Role::Video, 2).hasValue());
}

TEST_F(LaneManagerTest, SwitchesAtTheNextGroupWithTheTargetsInit) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));

  publish("video0", 6);
  publish("video0", 7); // held for video1's group 7
  publish("video1", 7);
  publish("video0", 8);

  EXPECT_EQ(
      sent_, (std::vector<Sent>{{6, "video0", "v0"}, {7, "video1", "v1"}}));
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Committed}));
}

TEST_F(LaneManagerTest, SendsTheHeldGroupWhenTheTargetMissesTheWait) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 1ms)));

  publish("video0", 6);
  publish("video0", 7);
  evb_.loop(); // until the wait runs out
  publish("video1", 7);

  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}, {7, "video0", ""}}));
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Cancelled}));
}

TEST_F(LaneManagerTest, BackgroundLaneEndOnlyRemovesTheLane) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  EXPECT_CALL(*client_, publishDone(_)).Times(0);
  lanes_.at("video1")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, ""});
  evb_.loopOnce(EVLOOP_NONBLOCK);
  EXPECT_EQ(subscribeAttempts_.at("video1"), 1);

  auto update = lanes_.at("catalog")->beginSubgroup(7, 0, kDefaultPriority, {});
  ASSERT_TRUE(update.hasValue());
  update.value()->object(0, folly::IOBuf::copyBuffer(kCatalog));
  for (size_t i = 0; i < 4; ++i) {
    evb_.loopOnce(EVLOOP_NONBLOCK);
  }
  EXPECT_EQ(subscribeAttempts_.at("video1"), 1);

  EXPECT_CALL(*policy_, select(_, _, _, _))
      .WillOnce([](const SwitchingSet& set, auto, auto, auto) {
        EXPECT_EQ(set.find("video1"), nullptr);
        return std::nullopt;
      });
  publish("video0", 6);
  EXPECT_EQ(subscribeAttempts_.at("video1"), 1);
}

TEST_F(LaneManagerTest, TrackEndSendsTheHeldGroupFirst) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  publish("video0", 6);
  publish("video0", 7);

  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}}));
  EXPECT_CALL(*client_, publishDone(_)).WillOnce([this](const PublishDone&) {
    EXPECT_EQ(
        sent_, (std::vector<Sent>{{6, "video0", "v0"}, {7, "video0", ""}}));
    return folly::unit;
  });
  lanes_.at("video0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, ""});

  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}, {7, "video0", ""}}));
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Cancelled}));
}

TEST_F(LaneManagerTest, AnEndedTargetCancelsThePendingSwitch) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  publish("video0", 6);
  publish("video0", 7);

  lanes_.at("video1")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, ""});
  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}, {7, "video0", ""}}));
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Cancelled}));
}

TEST_F(LaneManagerTest, LastLaneEndCompletesThePendingSwitchFailure) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  publish("video0", 6);
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));

  size_t completions = 0;
  EXPECT_CALL(*client_, publishDone(_)).WillOnce([&](const PublishDone& done) {
    ++completions;
    EXPECT_EQ(done.requestID, RequestID(1));
    EXPECT_EQ(done.statusCode, PublishDoneStatusCode::TRACK_ENDED);
    return folly::unit;
  });
  lanes_.at("video0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::UNAUTHORIZED, 0, ""});
  EXPECT_EQ(completions, 0);
  ASSERT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared}));

  lanes_.at("video1")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, ""});
  EXPECT_EQ(completions, 1);
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Cancelled}));
  EXPECT_EQ(subscribeAttempts_.at("video0"), 1);
  EXPECT_EQ(subscribeAttempts_.at("video1"), 1);
  EXPECT_TRUE(subscribe(Role::Video, 2).hasValue());
}

TEST_F(LaneManagerTest, EndedSwitchTargetLeavesTheLiveLaneRecovering) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  publish("video0", 6);
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  EXPECT_CALL(*client_, publishDone(_)).Times(0);

  lanes_.at("video0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, ""});
  ASSERT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared}));
  lanes_.at("video1")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, ""});
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Cancelled}));

  evb_.loop();
  publish("video0", 7);
  EXPECT_EQ(subscribeAttempts_.at("video0"), 2);
  EXPECT_EQ(subscribeAttempts_.at("video1"), 1);
  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}, {7, "video0", ""}}));
}

TEST_F(LaneManagerTest, TerminalLiveLaneWaitsForTheRecoveringBackup) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  publish("video0", 6);
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  EXPECT_CALL(*client_, publishDone(_)).Times(0);

  lanes_.at("video1")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::GOING_AWAY, 0, ""});
  lanes_.at("video0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::UNAUTHORIZED, 0, ""});
  evb_.loop();

  publish("video1", 7);
  publish("video1", 8);
  EXPECT_EQ(subscribeAttempts_.at("video0"), 1);
  EXPECT_EQ(subscribeAttempts_.at("video1"), 2);
  EXPECT_EQ(
      sent_, (std::vector<Sent>{{6, "video0", "v0"}, {8, "video1", "v1"}}));
  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Committed}));
}

TEST_F(LaneManagerTest, SwitchCancellationCanUnsubscribeBeforeCompletion) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  publish("video0", 6);
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  ON_CALL(*policy_, onSwitchOutcome(_))
      .WillByDefault([&](const SwitchOutcome& outcome) {
        outcomes_.push_back(outcome.kind);
        if (outcome.kind == Kind::Cancelled) {
          video.value()->unsubscribe();
        }
      });
  EXPECT_CALL(*client_, publishDone(_)).Times(0);

  lanes_.at("video0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::UNAUTHORIZED, 0, ""});
  ASSERT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared}));
  lanes_.at("video1")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, ""});

  EXPECT_EQ(outcomes_, (std::vector<Kind>{Kind::Prepared, Kind::Cancelled}));
  EXPECT_TRUE(subscribe(Role::Video, 2).hasValue());
}

TEST_F(LaneManagerTest, JoiningFetchWaitsForTheInitialLane) {
  heldLane_ = "video0";
  failLane_ = "video1";
  auto request = SubscribeRequest::make(FullTrackName{kBroadcast, "r--abr"});
  request.requestID = 1;
  Fetch fetch(RequestID(3), RequestID(1), 0, FetchType::RELATIVE_JOINING);
  fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};
  auto release = [this]() -> folly::coro::Task<void> {
    EXPECT_FALSE(laneFetch_);
    heldReply_.post();
    co_return;
  };
  auto [subscribed, fetched, released] =
      run(folly::coro::collectAll(
          manager_->subscribe(request, client_, Role::Video, policy_),
          manager_->fetch(
              fetch,
              std::make_shared<NiceMock<MockFetchConsumer>>(),
              Role::Video),
          release()));
  ASSERT_TRUE(subscribed.hasValue());
  ASSERT_TRUE(fetched.hasValue());
  EXPECT_EQ(laneFetch_->fullTrackName.trackName, "video0");
}

TEST_F(LaneManagerTest, JoiningFetchFailsWhenItsSubscribeFails) {
  heldLane_ = "catalog";
  failLane_ = "catalog";
  auto request = SubscribeRequest::make(FullTrackName{kBroadcast, "r--abr"});
  request.requestID = 1;
  Fetch fetch(RequestID(3), RequestID(1), 0, FetchType::RELATIVE_JOINING);
  fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr"};
  auto release = [this]() -> folly::coro::Task<void> {
    heldReply_.post();
    co_return;
  };
  auto [subscribed, fetched, released] =
      run(folly::coro::collectAll(
          manager_->subscribe(request, client_, Role::Video, policy_),
          manager_->fetch(
              fetch,
              std::make_shared<NiceMock<MockFetchConsumer>>(),
              Role::Video),
          release()));
  ASSERT_TRUE(subscribed.hasError());
  EXPECT_EQ(subscribed.error().requestID, RequestID(1));
  EXPECT_EQ(subscribed.error().errorCode, SubscribeErrorCode::NOT_SUPPORTED);
  ASSERT_TRUE(fetched.hasError());
  EXPECT_EQ(fetched.error().requestID, RequestID(3));
  EXPECT_EQ(fetched.error().errorCode, FetchErrorCode::NOT_SUPPORTED);
  EXPECT_FALSE(laneFetch_);
}

TEST_F(LaneManagerTest, ALateOlderGroupIsNotPlannedFrom) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  EXPECT_CALL(*policy_, select(_, _, 8, _));
  EXPECT_CALL(*policy_, select(_, _, 7, _)).Times(0);

  publish("video0", 8);
  publish("video0", 7);
}

TEST_F(LaneManagerTest, DroppingTheHandleEndsTheVirtualTrack) {
  ASSERT_TRUE(subscribe(Role::Video).hasValue()); // dropped right away

  EXPECT_TRUE(subscribe(Role::Video, 2).hasValue());
}

TEST_F(LaneManagerTest, RouterPassesOtherTracksToTheProxy) {
  MoQLaneRouter router(proxy_, [this](Role) { return policy_; }, kMaxWait);
  auto request = SubscribeRequest::make(FullTrackName{kBroadcast, "video1"});

  EXPECT_TRUE(run(router.subscribe(request, client_)).hasValue());
  EXPECT_TRUE(lanes_.contains("video1"));
  EXPECT_FALSE(handles_.contains("catalog"));
}

TEST_F(LaneManagerTest, RouterKeepsOneManagerPerBroadcast) {
  std::vector<Role> roles;
  MoQLaneRouter router(
      proxy_,
      [&](Role role) {
        roles.push_back(role);
        return policy_;
      },
      kMaxWait);
  auto subscribe = [&](const std::string& name, uint64_t requestID) {
    auto request = SubscribeRequest::make(FullTrackName{kBroadcast, name});
    request.requestID = requestID;
    return run(router.subscribe(request, client_)).value();
  };

  // The catalog once, then every lane.
  EXPECT_CALL(*proxy_, subscribe(_, _)).Times(4);
  auto video = subscribe("r--abr-video", 1);
  auto audio = subscribe("r--abr-audio", 2);
  EXPECT_EQ(roles, (std::vector<Role>{Role::Video, Role::Audio}));

  // The last UNSUBSCRIBE frees the manager; the next one reopens the catalog.
  video->unsubscribe();
  audio->unsubscribe();
  EXPECT_CALL(*proxy_, subscribe(_, _)).Times(3);
  subscribe("r--abr-video", 3);
}

}} // namespace moxygen::lane_manager
