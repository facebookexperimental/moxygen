/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

// A MoQProxy track and its upstreams over real MoQSession pairs on
// FakeSharedWebTransport: viewer <-> [edge proxy <->] proxy <-> test hubs,
// for every supported version.

#include <fmt/core.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/coro/Invoke.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "moxygen/MoQVersions.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQProxy.h"
#include "moxygen/test/MoQProxySwitchHarness.h"
#include "moxygen/test/MoQTestLinks.h"

namespace moxygen::test { namespace {

using namespace testing;

const FullTrackName kTrack{TrackNamespace{{"live"}}, "video"};

enum class Hops { One, Two };

struct SwitchCase {
  uint64_t version;
  // Two puts an edge proxy between the viewers and the proxy.
  Hops hops;
  std::string shapeName;
  SwitchTrackShape shape;
};

std::string caseName(const TestParamInfo<SwitchCase>& info) {
  return fmt::format(
      "Draft{}_{}_{}",
      getDraftMajorVersion(info.param.version),
      info.param.hops == Hops::One ? "OneHop" : "TwoHops",
      info.param.shapeName);
}

class MoQProxyUpstreamSwitchTest : public TestWithParam<SwitchCase> {
 protected:
  void TearDown() override {
    for (const auto& proxy : {edgeProxy_, proxy_}) {
      if (proxy) {
        proxy->close();
      }
    }
    links_.closeClients();
    // Detached coroutines may still hold keep-alives, so loop() would not
    // return.
    do {
      eventBase_.loopOnce(EVLOOP_NONBLOCK);
    } while (eventBase_.getNumLoopCallbacks() > 0 ||
             eventBase_.getNotificationQueueSize() > 0);
  }

  void run(folly::coro::Task<void> body) {
    folly::coro::blockingWait(std::move(body), &eventBase_);
  }

  // The proxy, in front of hubCount hubs in routing order, and for two hops
  // an edge proxy in front of it.
  folly::coro::Task<void> start(size_t hubCount) {
    for (size_t i = 0; i < hubCount; i++) {
      auto hub = std::make_shared<SwitchTestHub>(i, origin_);
      auto* link = co_await links_.connect(hub);
      hub->attach(link->server);
      hubProvider_->addUpstream(link->client);
      hubs_.push_back(std::move(hub));
    }
    proxy_ = MoQProxy::create({hubProvider_});
    if (GetParam().hops == Hops::Two) {
      auto* link = co_await links_.connect(proxy_);
      auto edgeProvider = std::make_shared<ScriptedUpstreamProvider>();
      edgeProvider->addUpstream(link->client);
      edgeProxy_ = MoQProxy::create({edgeProvider});
    }
  }

  folly::coro::Task<std::shared_ptr<CheckingViewer>> subscribeViewer() {
    auto* link = co_await links_.connect(edgeProxy_ ? edgeProxy_ : proxy_);
    auto viewer = std::make_shared<CheckingViewer>(origin_, kTrack);
    auto request = SubscribeRequest::make(kTrack);
    request.filter = LocationFilter::nextObject();
    const auto filter = request.filter;
    auto handle = co_await link->client->subscribe(std::move(request), viewer);
    EXPECT_TRUE(handle.hasValue());
    if (handle.hasValue()) {
      // The filter resolved against the Largest the viewer was told, so it
      // follows whichever upstream serves the track.
      viewer->expectedStart =
          filter
              .resolve(handle.value()->subscribeOk().largest, /*isFetch=*/false)
              .start;
      viewerHandles_.push_back(std::move(handle.value()));
    }
    co_return viewer;
  }

  folly::coro::Task<void> settle() {
    for (int i = 0; i < 200; i++) {
      co_await folly::coro::co_reschedule_on_current_executor;
    }
  }

  // Every proxy and hub lets go of the track once its viewers leave.
  folly::coro::Task<void> leaveAndExpectIdle() {
    for (auto& handle : std::exchange(viewerHandles_, {})) {
      handle->unsubscribe();
    }
    co_await settle();
    for (const auto& proxy : {edgeProxy_, proxy_}) {
      if (proxy) {
        EXPECT_EQ(proxy->findTrack(kTrack), nullptr);
      }
    }
    for (const auto& hub : hubs_) {
      EXPECT_EQ(hub->subscriptions(kTrack), 0);
    }
  }

  uint64_t objectsPerGroup() const {
    return GetParam().shape.objectsPerGroup;
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQExecutor> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_)};
  MoQTestLinks links_{GetParam().version, executor_};
  std::shared_ptr<SwitchOrigin> origin_{
      std::make_shared<SwitchOrigin>(GetParam().shape)};
  std::shared_ptr<ScriptedUpstreamProvider> hubProvider_{
      std::make_shared<ScriptedUpstreamProvider>()};
  std::vector<std::shared_ptr<SwitchTestHub>> hubs_;
  std::shared_ptr<MoQProxy> proxy_;
  std::shared_ptr<MoQProxy> edgeProxy_;
  std::vector<std::shared_ptr<Publisher::SubscriptionHandle>> viewerHandles_;
};

class MoQProxyCleanStreamTest : public MoQProxyUpstreamSwitchTest {};

// Every object reaches the viewer once, intact, in order, from the one hub,
// including a group that arrives in two parts.
TEST_P(MoQProxyCleanStreamTest, ViewerGetsTheHubsTrack) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/1);
    auto viewer = co_await subscribeViewer();

    for (uint64_t group = 0; group < 3; group++) {
      hubs_[0]->publishGroup(kTrack, group);
    }
    hubs_[0]->publish(kTrack, 3, 2);
    co_await settle();
    EXPECT_EQ(viewer->received().size(), 3 * objectsPerGroup() + 2);
    hubs_[0]->publishGroup(kTrack, 3);
    co_await settle();

    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    EXPECT_THAT(viewer->missing(), IsEmpty());
    EXPECT_EQ(viewer->received().size(), 4 * objectsPerGroup());
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 0}, {3, 0}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    EXPECT_THAT(
        hubs_[0]->subscribeFilters(kTrack),
        ElementsAre(LocationFilter::nextObject()));
    co_await leaveAndExpectIdle();
  }));
}

// A viewer that arrives mid-group gets the rest of that group, then whole
// groups.
TEST_P(MoQProxyCleanStreamTest, ViewerJoiningMidGroupStartsAtTheNextObject) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/1);
    hubs_[0]->publish(kTrack, 0, 2);
    auto viewer = co_await subscribeViewer();

    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[0]->publishGroup(kTrack, 1);
    co_await settle();

    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    EXPECT_THAT(viewer->missing(), IsEmpty());
    CO_ASSERT_FALSE(viewer->received().empty());
    EXPECT_EQ(viewer->received().front(), AbsoluteLocation(0, 2));
    EXPECT_EQ(viewer->received().size(), 2 * objectsPerGroup() - 2);
    co_await leaveAndExpectIdle();
  }));
}

std::vector<SwitchCase> cleanStreamCases() {
  const std::vector<std::pair<std::string, SwitchTrackShape>> shapes{
      {"OneSubgroup", {}},
      {"ThreeSubgroups", {.objectsPerGroup = 5, .subgroupsPerGroup = 3}},
      {"ObjectStreams",
       {.delivery = SwitchTrackShape::Delivery::ObjectStreams}},
      {"Datagrams", {.delivery = SwitchTrackShape::Delivery::Datagrams}}};
  std::vector<SwitchCase> cases;
  for (auto version : kSupportedVersions) {
    for (auto hops : {Hops::One, Hops::Two}) {
      for (const auto& [name, shape] : shapes) {
        cases.push_back(SwitchCase{version, hops, name, shape});
      }
    }
  }
  return cases;
}

INSTANTIATE_TEST_SUITE_P(
    AllVersions,
    MoQProxyCleanStreamTest,
    ValuesIn(cleanStreamCases()),
    caseName);

class MoQProxyUpstreamFaultTest : public MoQProxyUpstreamSwitchTest {};

// GOAWAYs from the hub reach the proxy, which only counts them for now: the
// hub keeps serving.
TEST_P(MoQProxyUpstreamFaultTest, GoawaysAreCountedWhileTheHubServes) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/1);
    auto viewer = co_await subscribeViewer();
    const bool requestStreams = getDraftMajorVersion(GetParam().version) >= 18;

    if (requestStreams) {
      hubs_[0]->sendRequestGoaway(kTrack);
    }
    hubs_[0]->sendGoaway(Goaway{});
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();

    EXPECT_EQ(proxy_->switchStats().upstreamGoaways, requestStreams ? 2 : 1);
    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    EXPECT_THAT(viewer->missing(), IsEmpty());
    EXPECT_EQ(viewer->received().size(), objectsPerGroup());
    co_await leaveAndExpectIdle();
  }));
}

// Today a hub that sends GOAWAY and then closes ends its viewers'
// subscriptions with GOING_AWAY, even though another hub could serve the
// track.
TEST_P(MoQProxyUpstreamFaultTest, GoawayThenCloseEndsViewers) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::GOING_AWAY;

    hubProvider_->setAvailable(0, false);
    hubs_[0]->sendGoaway(Goaway{});
    co_await settle();
    hubs_[0]->closeSessions();
    co_await settle();
    hubs_[1]->publishGroup(kTrack, 1);
    co_await settle();

    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::GOING_AWAY);
    const std::map<uint64_t, uint64_t> hubOfGroup{{0, 0}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    EXPECT_EQ(viewer->missing().size(), objectsPerGroup());
    EXPECT_EQ(proxy_->switchStats().upstreamGoaways, 1);
    EXPECT_EQ(proxy_->switchStats().upstreamsLost, 1);
    if (edgeProxy_) {
      EXPECT_EQ(edgeProxy_->switchStats().upstreamsLost, 1);
    }
    co_await leaveAndExpectIdle();
  }));
}

// A closing proxy ends its viewers with GOING_AWAY. With two hops the edge
// proxy gets it from the wire, counts a lost upstream, and passes it on.
TEST_P(MoQProxyUpstreamFaultTest, ProxyCloseEndsViewersWithGoingAway) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/1);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::GOING_AWAY;

    proxy_->close();
    co_await settle();

    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::GOING_AWAY);
    EXPECT_EQ(proxy_->switchStats().upstreamsLost, 0);
    if (edgeProxy_) {
      EXPECT_EQ(edgeProxy_->switchStats().upstreamsLost, 1);
    }
    co_await leaveAndExpectIdle();
  }));
}

std::vector<SwitchCase> faultCases() {
  std::vector<SwitchCase> cases;
  for (auto version : kSupportedVersions) {
    for (auto hops : {Hops::One, Hops::Two}) {
      cases.push_back(SwitchCase{version, hops, "OneSubgroup", {}});
    }
  }
  return cases;
}

INSTANTIATE_TEST_SUITE_P(
    AllVersions,
    MoQProxyUpstreamFaultTest,
    ValuesIn(faultCases()),
    caseName);

}} // namespace moxygen::test
