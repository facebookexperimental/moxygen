/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/MoQLaneManager.h"
#include "moxygen/lane_manager/MoQLaneRouter.h"

#include "moxygen/test/MockMoQSession.h"
#include "moxygen/test/Mocks.h"

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>

namespace moxygen::lane_manager { namespace {

using namespace std::chrono_literals;
using testing::_;
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
    // The proxy answers every SUBSCRIBE except failLane_'s, with largest {5,3}.
    ON_CALL(*proxy_, subscribe(_, _))
        .WillByDefault([this](
                           const SubscribeRequest& request,
                           std::shared_ptr<TrackConsumer> consumer) {
          const auto& name = request.fullTrackName.trackName;
          if (name == failLane_) {
            return folly::coro::makeTask<Publisher::SubscribeResult>(
                folly::makeUnexpected(
                    SubscribeError{
                        request.requestID,
                        SubscribeErrorCode::INTERNAL_ERROR,
                        ""}));
          }
          lanes_[name] = std::move(consumer);
          aliasAtSubscribe_[name] = alias_;
          SubscribeOk ok;
          ok.requestID = request.requestID;
          ok.trackAlias = TrackAlias(99);
          ok.largest = AbsoluteLocation{5, 3};
          handles_[name] =
              std::make_shared<NiceMock<MockSubscriptionHandle>>(ok);
          return folly::coro::makeTask<Publisher::SubscribeResult>(
              handles_[name]);
        });
    // The catalog FETCH returns kCatalog; lane FETCHes are kept.
    ON_CALL(*proxy_, fetch(_, _))
        .WillByDefault([this](Fetch fetch, std::shared_ptr<FetchConsumer> c) {
          if (fetch.fullTrackName.trackName == "catalog") {
            c->object(0, 0, 0, folly::IOBuf::copyBuffer(kCatalog));
            c->endOfFetch();
          } else {
            laneFetch_ = std::move(fetch);
            laneFetchConsumer_ = std::move(c);
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

  // Runs `task` as the client session's request, then the lane SUBSCRIBEs it
  // started.
  template <typename T>
  T run(folly::coro::Task<T> task) {
    folly::RequestContextScopeGuard guard;
    folly::RequestContext::get()->setContextData(
        folly::RequestToken("moq_session"),
        std::make_unique<MoQSession::MoQSessionRequestData>(session_));
    auto result = folly::coro::blockingWait(std::move(task), &evb_);
    evb_.loopOnce(EVLOOP_NONBLOCK);
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
  std::optional<TrackAlias> alias_;
  folly::F14FastMap<std::string, std::shared_ptr<TrackConsumer>> lanes_;
  folly::F14FastMap<std::string, std::optional<TrackAlias>> aliasAtSubscribe_;
  folly::F14FastMap<std::string, std::shared_ptr<MockSubscriptionHandle>>
      handles_;
  std::optional<Fetch> laneFetch_;
  std::shared_ptr<FetchConsumer> laneFetchConsumer_;
  std::vector<Sent> sent_;
  std::vector<Kind> outcomes_;
};

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

TEST_F(LaneManagerTest, ACatalogTimeoutFailsWithTimeoutAndRetry) {
  // Stands in for the first-catalog wait running out.
  EXPECT_CALL(*proxy_, subscribe(_, _)).WillOnce([](const auto&, const auto&) {
    return folly::coro::makeErrorTask<Publisher::SubscribeResult>(
        folly::FutureTimeout());
  });

  auto result = subscribe(Role::Video);
  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().errorCode, SubscribeErrorCode::TIMEOUT);
  EXPECT_TRUE(result.error().retryInterval.has_value());
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

  EXPECT_CALL(*policy_, select(_, _, _, _))
      .WillOnce([](const SwitchingSet& set, auto, auto, auto) {
        EXPECT_EQ(set.find("video1"), nullptr);
        return std::nullopt;
      });
  publish("video0", 6);
}

TEST_F(LaneManagerTest, TrackEndSendsTheHeldGroupFirst) {
  auto video = subscribe(Role::Video);
  ASSERT_TRUE(video.hasValue());
  ON_CALL(*policy_, select(_, "video0", 6, _))
      .WillByDefault(Return(planTo("video1", 10s)));
  publish("video0", 6);
  publish("video0", 7);

  EXPECT_CALL(*client_, publishDone(_)).WillOnce(Return(folly::unit));
  lanes_.at("video0")->publishDone(
      PublishDone{RequestID(1), PublishDoneStatusCode::TRACK_ENDED, 0, ""});

  EXPECT_EQ(sent_, (std::vector<Sent>{{6, "video0", "v0"}, {7, "video0", ""}}));
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
