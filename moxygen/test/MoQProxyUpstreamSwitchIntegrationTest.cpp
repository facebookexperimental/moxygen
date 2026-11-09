/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

// A MoQProxy track and its upstreams over real MoQSession pairs on
// FakeSharedWebTransport: viewer <-> [edge proxy <->] proxy <-> test hubs,
// for every supported version.

#include <fmt/core.h>
#include <folly/Function.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Sleep.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
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

// Short enough for a test to wait out.
constexpr std::chrono::milliseconds kSwitchDeadline{300};

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

// How the proxy reaches the hubs: one provider choosing among them, as a
// pool does, or a provider per hub, tried in order.
enum class Providers { OnePool, OnePerHub };

class MoQProxyUpstreamSwitchTest : public TestWithParam<SwitchCase> {
 protected:
  void TearDown() override {
    for (const auto& proxy : proxies_) {
      proxy->close();
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

  virtual MoQProxyOptions proxyOptions() const {
    MoQProxyOptions options;
    options.upstreamSwitch = {
        .deadline = switchDeadline_,
        .retryBackoff = std::chrono::milliseconds(10),
        .maxRetryBackoff = std::chrono::milliseconds(50),
        .randomSeed = 1};
    return options;
  }

  std::shared_ptr<MoQProxy> makeProxy(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> providers) {
    auto proxy = MoQProxy::create(std::move(providers), proxyOptions());
    proxies_.push_back(proxy);
    return proxy;
  }

  std::shared_ptr<SwitchTestHub> addHub() {
    auto hub = std::make_shared<SwitchTestHub>(hubs_.size(), origin_);
    hubs_.push_back(hub);
    return hub;
  }

  // The proxy, in front of hubCount hubs in routing order, and for two hops
  // an edge proxy in front of it.
  folly::coro::Task<void> start(
      size_t hubCount,
      Providers providers = Providers::OnePool) {
    for (size_t i = 0; i < hubCount; i++) {
      auto hub = addHub();
      auto* link = co_await links_.connect(hub);
      hub->attach(link->server);
      hubTransports_.push_back(link->serverWt.get());
      hubSessionRefs_.push_back(link->client);
      auto session = link->client;
      if (trackOwnsHubSessions_) {
        link->client.reset();
      } else {
        hubSessions_.push_back(session);
      }
      if (providers == Providers::OnePerHub || hubProviders_.empty()) {
        hubProviders_.push_back(std::make_shared<ScriptedUpstreamProvider>());
        if (trackOwnsHubSessions_) {
          hubProviders_.back()->handOutOnce();
        }
      }
      hubProviders_.back()->addUpstream(std::move(session));
    }
    proxy_ = makeProxy({hubProviders_.begin(), hubProviders_.end()});
    if (GetParam().hops == Hops::Two) {
      auto* link = co_await links_.connect(proxy_);
      auto edgeProvider = std::make_shared<ScriptedUpstreamProvider>();
      edgeProvider->addUpstream(link->client);
      edgeProxy_ = makeProxy({edgeProvider});
    }
  }

  folly::coro::Task<std::shared_ptr<CheckingViewer>> subscribeViewer(
      FullTrackName track = kTrack) {
    auto* link = co_await links_.connect(edgeProxy_ ? edgeProxy_ : proxy_);
    viewerLinks_.push_back(link);
    viewerSessions_.push_back(link->client);
    auto viewer = std::make_shared<CheckingViewer>(origin_, track);
    auto request = SubscribeRequest::make(track);
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

  // Lets real time pass until done() holds, or for at most two seconds.
  folly::coro::Task<void> waitUntil(folly::Function<bool()> done) {
    const auto giveUp =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!done() && std::chrono::steady_clock::now() < giveUp) {
      co_await folly::coro::sleep(std::chrono::milliseconds(5));
    }
    co_await settle();
  }

  // Until the hub has received its count-th SUBSCRIBE for the track.
  folly::coro::Task<void> waitForSubscribe(
      const SwitchTestHub& hub,
      size_t count) {
    co_await waitUntil(
        [&] { return hub.subscribeFilters(kTrack).size() == count; });
  }

  // Every proxy and hub lets go of the tracks once their viewers leave.
  folly::coro::Task<void> leaveAndExpectIdle(
      std::vector<FullTrackName> tracks = {kTrack}) {
    for (auto& handle : std::exchange(viewerHandles_, {})) {
      handle->unsubscribe();
    }
    co_await settle();
    for (const auto& track : tracks) {
      for (const auto& proxy : proxies_) {
        EXPECT_EQ(proxy->findTrack(track), nullptr);
      }
      for (const auto& hub : hubs_) {
        EXPECT_EQ(hub->subscriptions(track), 0);
      }
    }
  }

  static void expectIntact(const CheckingViewer& viewer) {
    EXPECT_THAT(viewer.violations(), IsEmpty());
    EXPECT_THAT(viewer.unfinishedSubgroups(), IsEmpty());
    EXPECT_THAT(viewer.missing(), IsEmpty());
  }

  uint64_t objectsPerGroup() const {
    return GetParam().shape.objectsPerGroup;
  }

  // The streams a hub opens for one whole group.
  uint64_t streamsPerGroup() const {
    const auto& shape = GetParam().shape;
    return shape.delivery == SwitchTrackShape::Delivery::ObjectStreams
        ? shape.objectsPerGroup
        : shape.subgroupsPerGroup;
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQExecutor> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_)};
  MoQTestLinks links_{GetParam().version, executor_};
  std::shared_ptr<SwitchOrigin> origin_{
      std::make_shared<SwitchOrigin>(GetParam().shape)};
  std::vector<std::shared_ptr<ScriptedUpstreamProvider>> hubProviders_;
  // The proxy's end of each hub's session, in hub order.
  std::vector<std::shared_ptr<MoQSession>> hubSessions_;
  // Each hub's end of its transport, in hub order.
  std::vector<proxygen::test::FakeSharedWebTransport*> hubTransports_;
  // When set, start() leaves each hub session to its provider alone, which
  // hands it out once, so the proxy track that takes it is its only owner.
  // hubSessions_ then stays empty.
  bool trackOwnsHubSessions_{false};
  // The proxy's end of each hub's session, in hub order.
  std::vector<std::weak_ptr<MoQSession>> hubSessionRefs_;
  // For the proxies start() makes.
  std::chrono::milliseconds switchDeadline_{kSwitchDeadline};
  std::vector<std::shared_ptr<SwitchTestHub>> hubs_;
  // The proxy in front of the hubs, and the edge proxy in front of it.
  std::shared_ptr<MoQProxy> proxy_;
  std::shared_ptr<MoQProxy> edgeProxy_;
  // Every proxy, closed at teardown.
  std::vector<std::shared_ptr<MoQProxy>> proxies_;
  std::vector<std::shared_ptr<Publisher::SubscriptionHandle>> viewerHandles_;
  // Each viewer's link and session, in the order they subscribed.
  std::vector<MoQTestLink*> viewerLinks_;
  std::vector<std::shared_ptr<MoQSession>> viewerSessions_;
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

std::vector<SwitchCase> everyShapeCases() {
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

// Of two viewers, the first, whose SUBSCRIBE made the proxy subscribe to the
// hub, closes its session. The upstream subscription serves the other on, and
// doesn't keep the proxy's end of the first's link.
TEST_P(
    MoQProxyCleanStreamTest,
    TheUpstreamAViewerStartedDoesntKeepItsSessionOnceItCloses) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/1);
    co_await subscribeViewer();
    auto staying = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();

    viewerSessions_.front()->close(SessionCloseErrorCode::NO_ERROR);
    viewerHandles_.erase(viewerHandles_.begin());
    co_await settle();
    auto& link = *viewerLinks_.front();
    link.clientWt->setPeerHandler(nullptr);
    std::weak_ptr<MoQSession> proxySide = std::exchange(link.server, nullptr);
    hubs_[0]->publishGroup(kTrack, 1);
    co_await settle();
    EXPECT_TRUE(proxySide.expired());

    expectIntact(*staying);
    co_await leaveAndExpectIdle();
  }));
}

INSTANTIATE_TEST_SUITE_P(
    AllVersions,
    MoQProxyCleanStreamTest,
    ValuesIn(everyShapeCases()),
    caseName);

class MoQProxyLostUpstreamTest : public MoQProxyUpstreamSwitchTest {
 protected:
  // Hub 0 serves groups 0 and 1, which hub 1 also has, then is lost; hub 1
  // then publishes groups 2 and 3. The viewer gets every group whole, from
  // hub 1 from group 2.
  folly::coro::Task<void> expectHub1TakesOver(folly::Function<void()> lose) {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    for (uint64_t group = 0; group < 2; group++) {
      hubs_[0]->publishGroup(kTrack, group);
      hubs_[1]->publishGroup(kTrack, group);
    }
    co_await settle();

    lose();
    co_await waitForSubscribe(*hubs_[1], 1);
    hubs_[1]->publishGroup(kTrack, 2);
    hubs_[1]->publishGroup(kTrack, 3);
    co_await settle();
    // The switch is over, so nothing holds hub 0's session.
    if (trackOwnsHubSessions_) {
      EXPECT_TRUE(hubSessionRefs_[0].expired());
    }

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 1}, {3, 1}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    EXPECT_THAT(
        hubs_[1]->subscribeFilters(kTrack),
        ElementsAre(LocationFilter::nextObject()));
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.upstreamsLost, 1);
    EXPECT_EQ(stats.lostSwitches.started, 1);
    EXPECT_EQ(stats.lostSwitches.completed, 1);
    EXPECT_EQ(stats.lostGaps.gaps, 1);
    EXPECT_EQ(stats.lostGaps.groups, 1);
    EXPECT_EQ(stats.refusedStreams + stats.refusedDatagrams, 0);
    if (edgeProxy_) {
      EXPECT_EQ(edgeProxy_->switchStats().upstreamsLost, 0);
    }
    co_await leaveAndExpectIdle();
  }
};

// As a relay ends its subscriptions when its request-stream GOAWAY times out;
// health checks have marked the hub down.
TEST_P(MoQProxyLostUpstreamTest, AnUpstreamEndedWithGoingAwayIsReplaced) {
  run(expectHub1TakesOver([&] {
    hubProviders_[0]->setAvailable(0, false);
    hubs_[0]->endSubscriptions(kTrack, PublishDoneStatusCode::GOING_AWAY);
  }));
}

TEST_P(MoQProxyLostUpstreamTest, AClosedUpstreamIsReplaced) {
  run(expectHub1TakesOver([&] { hubs_[0]->closeSessions(); }));
}

// The track is each hub session's only owner, as with a provider that
// connects a new session for each attempt, so it destroys hub 0's session as
// it releases it from the session's own callbacks.
TEST_P(MoQProxyLostUpstreamTest, AClosedUpstreamTheTrackOwnsAloneIsReplaced) {
  trackOwnsHubSessions_ = true;
  run(expectHub1TakesOver([&] { hubs_[0]->closeSessions(); }));
}

TEST_P(
    MoQProxyLostUpstreamTest,
    AnUpstreamEndedWithGoingAwayTheTrackOwnsAloneIsReplaced) {
  trackOwnsHubSessions_ = true;
  run(expectHub1TakesOver([&] {
    hubProviders_[0]->setAvailable(0, false);
    hubs_[0]->endSubscriptions(kTrack, PublishDoneStatusCode::GOING_AWAY);
  }));
}

// Hub 0 is lost, and hub 1, which replaces it, is lost too before it
// forwards anything. The same switch moves on to hub 2.
TEST_P(MoQProxyLostUpstreamTest, AReplacementLostBeforeItForwardsIsReplaced) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/3);
    auto viewer = co_await subscribeViewer();
    for (uint64_t group = 0; group < 2; group++) {
      for (auto& hub : hubs_) {
        hub->publishGroup(kTrack, group);
      }
    }
    co_await settle();

    hubs_[0]->closeSessions();
    co_await waitForSubscribe(*hubs_[1], 1);
    co_await settle();
    hubs_[1]->closeSessions();
    co_await waitForSubscribe(*hubs_[2], 1);
    hubs_[2]->publishGroup(kTrack, 2);
    hubs_[2]->publishGroup(kTrack, 3);
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 2}, {3, 2}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.upstreamsLost, 2);
    EXPECT_EQ(stats.lostSwitches.started, 1);
    EXPECT_EQ(stats.lostSwitches.completed, 1);
    EXPECT_EQ(stats.lostSwitches.failed, 0);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 1 is two groups behind hub 0, so the groups it publishes first are
// refused, and none reaches the viewer twice.
TEST_P(MoQProxyLostUpstreamTest, TheReplacementNeverRedeliversTheLostGroups) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    for (uint64_t group = 0; group < 4; group++) {
      hubs_[0]->publishGroup(kTrack, group);
    }
    hubs_[1]->publishGroup(kTrack, 0);
    hubs_[1]->publishGroup(kTrack, 1);
    co_await settle();

    hubs_[0]->closeSessions();
    co_await waitForSubscribe(*hubs_[1], 1);
    for (uint64_t group = 2; group < 6; group++) {
      hubs_[1]->publishGroup(kTrack, group);
    }
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 1}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    if (GetParam().shape.delivery == SwitchTrackShape::Delivery::Datagrams) {
      EXPECT_EQ(stats.refusedDatagrams, 2 * objectsPerGroup());
    } else {
      EXPECT_EQ(stats.refusedStreams, 2 * streamsPerGroup());
    }
    EXPECT_EQ(stats.lostGaps.groups, 1);
    co_await leaveAndExpectIdle();
  }));
}

// The PUBLISH_DONE that ends hub 1's subscription counts the streams the
// proxy refused, which still reach the proxy's session: the viewer gets
// TRACK_ENDED without the session waiting out its stream-count timeout.
TEST_P(MoQProxyLostUpstreamTest, TheReplacementsTrackEndFollowsRefusedStreams) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    for (const auto& session : hubSessions_) {
      auto settings = session->getMoqSettings();
      settings.publishDoneStreamCountTimeout = std::chrono::minutes(1);
      session->setMoqSettings(settings);
    }
    auto viewer = co_await subscribeViewer();
    for (uint64_t group = 0; group < 4; group++) {
      hubs_[0]->publishGroup(kTrack, group);
    }
    hubs_[1]->publishGroup(kTrack, 0);
    hubs_[1]->publishGroup(kTrack, 1);
    co_await settle();
    hubs_[0]->closeSessions();
    co_await waitForSubscribe(*hubs_[1], 1);
    for (uint64_t group = 2; group < 5; group++) {
      hubs_[1]->publishGroup(kTrack, group);
    }
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::TRACK_ENDED;

    hubs_[1]->endSubscriptions(kTrack, PublishDoneStatusCode::TRACK_ENDED);
    co_await waitUntil([&] { return viewer->done().has_value(); });

    expectIntact(*viewer);
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::TRACK_ENDED);
    co_await leaveAndExpectIdle();
  }));
}

// On a quiet track hub 1 has published nothing newer than the group the viewer
// has: past the switch deadline the proxy carries on with it, and the viewer
// gets the next group it publishes.
TEST_P(MoQProxyLostUpstreamTest, AQuietTrackKeepsItsViewerPastTheDeadline) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[1]->publishGroup(kTrack, 0);
    co_await settle();

    hubs_[0]->closeSessions();
    co_await waitForSubscribe(*hubs_[1], 1);
    co_await folly::coro::sleep(2 * kSwitchDeadline);
    EXPECT_FALSE(viewer->done().has_value());
    hubs_[1]->publishGroup(kTrack, 1);
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{{0, 0}, {1, 1}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.lostSwitches.completed, 1);
    EXPECT_EQ(stats.lostSwitches.failed, 0);
    co_await leaveAndExpectIdle();
  }));
}

INSTANTIATE_TEST_SUITE_P(
    AllVersions,
    MoQProxyLostUpstreamTest,
    ValuesIn(everyShapeCases()),
    caseName);

class MoQProxyUpstreamFaultTest : public MoQProxyUpstreamSwitchTest {};

// GOAWAYs from the hub reach the proxy. With no other hub to switch to, the
// hub keeps serving.
TEST_P(MoQProxyUpstreamFaultTest, WithNoOtherHubAHubGoingAwayKeepsServing) {
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
    EXPECT_EQ(proxy_->switchStats().gracefulSwitches.started, 1);
    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    EXPECT_THAT(viewer->missing(), IsEmpty());
    EXPECT_EQ(viewer->received().size(), objectsPerGroup());
    co_await leaveAndExpectIdle();
  }));
}

// The hub closes right after its GOAWAY, before the switch's start delay
// ends, so the track resumes from the other hub only after losing it: a gap a
// replacement found in time would have avoided.
TEST_P(
    MoQProxyUpstreamFaultTest,
    AHubClosingRightAfterItsGoawayIsReplacedAsLost) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();

    hubs_[0]->sendGoaway(Goaway{});
    co_await settle();
    hubs_[0]->closeSessions();
    co_await waitForSubscribe(*hubs_[1], 1);
    hubs_[1]->publishGroup(kTrack, 1);
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{{0, 0}, {1, 1}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.upstreamGoaways, 1);
    EXPECT_EQ(stats.upstreamsLost, 1);
    EXPECT_EQ(stats.gracefulSwitches.started, 1);
    EXPECT_EQ(stats.lostSwitches.completed, 1);
    EXPECT_EQ(stats.lateReplacementGaps.gaps, 1);
    EXPECT_EQ(stats.lostGaps.gaps, 0);
    co_await leaveAndExpectIdle();
  }));
}

// The first provider's only hub closed, so the second provider's hub takes
// over.
TEST_P(MoQProxyUpstreamFaultTest, TheSwitchFallsBackToTheNextProvider) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2, Providers::OnePerHub);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();

    hubs_[0]->closeSessions();
    co_await waitForSubscribe(*hubs_[1], 1);
    hubs_[1]->publishGroup(kTrack, 1);
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{{0, 0}, {1, 1}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& first = hubProviders_[0]->attempts();
    CO_ASSERT_EQ(first.size(), 2);
    EXPECT_EQ(first[0].upstream, 0);
    EXPECT_FALSE(first[1].upstream.has_value());
    EXPECT_TRUE(first[1].hasFallbackProvider);
    const auto& second = hubProviders_[1]->attempts();
    CO_ASSERT_EQ(second.size(), 1);
    EXPECT_EQ(second[0].upstream, 0);
    EXPECT_FALSE(second[0].hasFallbackProvider);
    co_await leaveAndExpectIdle();
  }));
}

// Also the negative control for the switching rows: without a replacement
// the viewer's subscription ends. With two hops the edge proxy then loses
// its upstream too, and gives up at its own deadline.
TEST_P(
    MoQProxyUpstreamFaultTest,
    WithNoReplacementViewersGetGoingAwayAtTheDeadline) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/1);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::GOING_AWAY;

    const auto lostAt = std::chrono::steady_clock::now();
    hubs_[0]->closeSessions();
    co_await waitUntil([&] { return viewer->done().has_value(); });

    EXPECT_GE(std::chrono::steady_clock::now() - lostAt, kSwitchDeadline);
    expectIntact(*viewer);
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::GOING_AWAY);
    EXPECT_EQ(proxy_->switchStats().lostSwitches.failed, 1);
    if (edgeProxy_) {
      EXPECT_EQ(edgeProxy_->switchStats().upstreamsLost, 1);
      EXPECT_EQ(edgeProxy_->switchStats().lostSwitches.failed, 1);
    }
    const auto attempts = hubProviders_[0]->attempts().size();
    co_await folly::coro::sleep(std::chrono::milliseconds(100));
    EXPECT_EQ(hubProviders_[0]->attempts().size(), attempts);
    co_await leaveAndExpectIdle();
  }));
}

// A closing proxy ends its viewers with GOING_AWAY. With two hops the edge
// proxy gets it from the wire, counts a lost upstream, finds no replacement,
// and passes it on at its switch deadline.
TEST_P(MoQProxyUpstreamFaultTest, ProxyCloseEndsViewersWithGoingAway) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/1);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::GOING_AWAY;

    proxy_->close();
    co_await waitUntil([&] { return viewer->done().has_value(); });

    expectIntact(*viewer);
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::GOING_AWAY);
    EXPECT_EQ(proxy_->switchStats().upstreamsLost, 0);
    if (edgeProxy_) {
      EXPECT_EQ(edgeProxy_->switchStats().upstreamsLost, 1);
      EXPECT_EQ(edgeProxy_->switchStats().lostSwitches.failed, 1);
    }
    co_await leaveAndExpectIdle();
  }));
}

// The hub ends the track before group 2's stream has reached the proxy, so the
// proxy holds the TRACK_ENDED, waiting for that stream, when the hub's session
// closes.
TEST_P(
    MoQProxyUpstreamFaultTest,
    ATrackEndedBeforeItsSessionClosesReachesViewers) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[0]->publish(kTrack, 1, 2);
    co_await settle();
    hubTransports_[0]->setDeferNewStreamData(true);
    hubs_[0]->publish(kTrack, 2, 1);
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::TRACK_ENDED;
    viewer->expectedReset = ResetStreamErrorCode::CANCELLED;

    hubs_[0]->endSubscriptions(kTrack, PublishDoneStatusCode::TRACK_ENDED);
    co_await settle();
    EXPECT_FALSE(viewer->done().has_value());
    hubs_[0]->closeSessions();
    co_await settle();

    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::TRACK_ENDED);
    EXPECT_THAT(viewer->resets(), SizeIs(1));
    EXPECT_EQ(proxy_->switchStats().upstreamsLost, 0);
    EXPECT_EQ(proxy_->switchStats().lostSwitches.started, 0);
    EXPECT_THAT(hubs_[1]->subscribeFilters(kTrack), IsEmpty());
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

constexpr std::chrono::milliseconds kMaxStartDelay{50};
constexpr std::chrono::milliseconds kReorderGrace{50};
// Far longer than a switch takes here.
constexpr std::chrono::milliseconds kGoawayTimeout{2000};

// Hub 0 sends a GOAWAY, and the proxy moves the track to hub 1 while hub 0
// still serves.
class MoQProxyGracefulSwitchTest : public MoQProxyUpstreamSwitchTest {
 protected:
  MoQProxyOptions proxyOptions() const override {
    auto options = MoQProxyUpstreamSwitchTest::proxyOptions();
    options.upstreamSwitch.maxStartDelay = maxStartDelay_;
    options.upstreamSwitch.reorderGrace = kReorderGrace;
    return options;
  }

  std::chrono::milliseconds maxStartDelay_{kMaxStartDelay};

  // Hub 0 publishes groups [0, hub0Groups) and the other hubs
  // [0, otherGroups), then hub 0 goes away, and the proxy subscribes to hub 1
  // from its next group.
  folly::coro::Task<std::shared_ptr<CheckingViewer>> startWithHub0GoingAway(
      uint64_t hub0Groups,
      uint64_t otherGroups,
      size_t hubCount = 2) {
    co_await start(hubCount);
    auto viewer = co_await subscribeViewer();
    for (uint64_t group = 0; group < hub0Groups; group++) {
      hubs_[0]->publishGroup(kTrack, group);
    }
    for (size_t hub = 1; hub < hubCount; hub++) {
      for (uint64_t group = 0; group < otherGroups; group++) {
        hubs_[hub]->publishGroup(kTrack, group);
      }
    }
    co_await settle();
    goAway(Goaway{.timeout = uint64_t(kGoawayTimeout.count())});
    co_await waitForSubscribe(*hubs_[1], 1);
    co_return viewer;
  }

  // The hub sends a session GOAWAY. The pool stops choosing it, as health
  // checks mark a draining hub down.
  void goAway(Goaway goaway, size_t hub = 0) {
    hubProviders_[0]->setAvailable(hub, false);
    hubs_[hub]->sendGoaway(goaway);
  }

  // Until the proxy has unsubscribed from the hub.
  folly::coro::Task<void> waitForHandover(
      const FullTrackName& track = kTrack,
      size_t hub = 0) {
    co_await waitUntil([&] { return hubs_[hub]->subscriptions(track) == 0; });
  }

  // Each group reached the viewer whole, from one hub, and the switch let go
  // of hub 0 without losing it.
  void expectHandedOver(
      const CheckingViewer& viewer,
      const std::map<uint64_t, uint64_t>& hubOfGroup) {
    expectIntact(viewer);
    EXPECT_EQ(viewer.hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.gracefulSwitches.started, 1);
    EXPECT_EQ(stats.gracefulSwitches.completed, 1);
    EXPECT_EQ(stats.upstreamsLost, 0);
    EXPECT_EQ(stats.lostSwitches.started, 0);
    EXPECT_THAT(
        hubs_[1]->subscribeFilters(kTrack),
        ElementsAre(LocationFilter::nextObject()));
    if (edgeProxy_) {
      EXPECT_EQ(edgeProxy_->switchStats().upstreamGoaways, 0);
      EXPECT_EQ(edgeProxy_->switchStats().upstreamsLost, 0);
    }
  }

  uint64_t refused() const {
    const auto& stats = proxy_->switchStats();
    return stats.refusedStreams + stats.refusedDatagrams;
  }

  // What a hub sends for whole groups.
  uint64_t refusedPerGroup() const {
    return GetParam().shape.delivery == SwitchTrackShape::Delivery::Datagrams
        ? objectsPerGroup()
        : streamsPerGroup();
  }

  // Hub 1 starts at group 2, the next one hub 0 would send. Hub 0 is refused
  // group 2, and once it has stayed caught up the proxy lets it go.
  folly::coro::Task<void> expectHub1TakesOverAtTheNextGroup() {
    auto viewer = co_await startWithHub0GoingAway(2, 2);
    hubs_[1]->publishGroup(kTrack, 2);
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 2);
    co_await waitForHandover();
    hubs_[1]->publishGroup(kTrack, 3);
    co_await settle();
    // The switch is over, so nothing holds hub 0's session.
    if (trackOwnsHubSessions_) {
      EXPECT_TRUE(hubSessionRefs_[0].expired());
    }

    expectHandedOver(*viewer, {{0, 0}, {1, 0}, {2, 1}, {3, 1}});
    EXPECT_EQ(proxy_->switchStats().handoffs.equal, 1);
    EXPECT_EQ(refused(), 0);
    co_await leaveAndExpectIdle();
  }
};

// Hub 1 starts at group 2, the next one hub 0 would send. Hub 0 is refused
// group 2, and once it has stayed caught up the proxy lets it go, long before
// its GOAWAY times out.
TEST_P(MoQProxyGracefulSwitchTest, AReplacementAtTheNextGroupTakesOverThere) {
  run(expectHub1TakesOverAtTheNextGroup());
}

// As above, with the track each hub session's only owner, as with a provider
// that connects a new session for each attempt: it destroys hub 0's session as
// it lets go of it.
TEST_P(MoQProxyGracefulSwitchTest, AReplacementTheTrackOwnsAloneTakesOver) {
  trackOwnsHubSessions_ = true;
  run(expectHub1TakesOverAtTheNextGroup());
}

// Hub 1 is two groups behind: its groups 2 and 3, which hub 0 already sent,
// are refused, and it takes over at group 4.
TEST_P(MoQProxyGracefulSwitchTest, AReplacementBehindTakesOverAfterTheOldHub) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(4, 2);
    hubs_[1]->publishGroup(kTrack, 2);
    hubs_[1]->publishGroup(kTrack, 3);
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 4);
    hubs_[1]->publishGroup(kTrack, 4);
    co_await waitForHandover();
    hubs_[1]->publishGroup(kTrack, 5);
    co_await settle();

    expectHandedOver(*viewer, {{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 1}});
    EXPECT_EQ(proxy_->switchStats().handoffs.behind, 1);
    EXPECT_EQ(refused(), 2 * refusedPerGroup());
    co_await leaveAndExpectIdle();
  }));
}

// Hub 1 is two groups ahead, so it starts at group 4, which reaches the viewer
// while hub 0 still owes groups 2 and 3. The proxy keeps hub 0 until it has
// finished them and reached group 4.
TEST_P(
    MoQProxyGracefulSwitchTest,
    AReplacementAheadLetsTheOldHubFinishTheGroupsBeforeIt) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(2, 4);
    hubs_[1]->publishGroup(kTrack, 4);
    hubs_[0]->publishGroup(kTrack, 2);
    hubs_[0]->publish(kTrack, 3, 1);
    co_await folly::coro::sleep(2 * kReorderGrace);
    EXPECT_EQ(hubs_[0]->subscriptions(kTrack), 1);
    hubs_[0]->publishGroup(kTrack, 3);
    hubs_[0]->publishGroup(kTrack, 4);
    co_await waitForHandover();
    hubs_[1]->publishGroup(kTrack, 5);
    co_await settle();

    expectHandedOver(*viewer, {{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 1}});
    EXPECT_EQ(proxy_->switchStats().handoffs.ahead, 1);
    EXPECT_EQ(refused(), 0);
    co_await leaveAndExpectIdle();
  }));
}

// A quiet track: each hub published group 0 and nothing since, so neither has
// anything to send. Once that has lasted the reorder grace the proxy lets hub 0
// go, long before its GOAWAY times out, and hub 1 serves the next group.
TEST_P(MoQProxyGracefulSwitchTest, AQuietTrackHandsOverAfterTheReorderGrace) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(1, 1);
    const auto subscribed = std::chrono::steady_clock::now();
    co_await waitForHandover();
    EXPECT_LT(
        std::chrono::steady_clock::now() - subscribed, kGoawayTimeout / 2);
    hubs_[1]->publishGroup(kTrack, 1);
    co_await settle();

    expectHandedOver(*viewer, {{0, 0}, {1, 1}});
    EXPECT_EQ(refused(), 0);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 0's session closes while it is in group 2, once hub 1 took group 4: hub
// 1 takes over at once, the rest of group 2 and group 3 are a gap, and group
// 2's open streams reset.
TEST_P(
    MoQProxyGracefulSwitchTest,
    TheOldHubClosingBeforeItCatchesUpHandsOverAtOnce) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(2, 4);
    hubs_[1]->publishGroup(kTrack, 4);
    hubs_[0]->publish(kTrack, 2, 2);
    co_await settle();
    viewer->expectedReset = ResetStreamErrorCode::SESSION_CLOSED;

    hubs_[0]->closeSessions();
    co_await settle();
    hubs_[1]->publishGroup(kTrack, 5);
    co_await settle();

    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    std::vector<AbsoluteLocation> missing;
    for (uint64_t object = 2; object < objectsPerGroup(); object++) {
      missing.emplace_back(2, object);
    }
    for (uint64_t object = 0; object < objectsPerGroup(); object++) {
      missing.emplace_back(3, object);
    }
    EXPECT_EQ(viewer->missing(), missing);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 0}, {4, 1}, {5, 1}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.gracefulSwitches.completed, 1);
    EXPECT_EQ(stats.upstreamsLost, 1);
    EXPECT_EQ(stats.lostSwitches.started, 0);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 0 stalls in group 2 once hub 1 took group 4. The groups came faster than
// anything the proxy can measure, so after minTail the tail timer lets go of
// hub 0: group 2's open streams reset, the rest of it and group 3 are missing,
// and hub 1 serves.
TEST_P(MoQProxyGracefulSwitchTest, TheTailTimerLetsGoOfAHubThatStalls) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(2, 4);
    hubs_[1]->publishGroup(kTrack, 4);
    hubs_[0]->publish(kTrack, 2, 2);
    co_await settle();
    viewer->expectedReset = ResetStreamErrorCode::CANCELLED;

    co_await waitForHandover();
    hubs_[1]->publishGroup(kTrack, 5);
    co_await settle();

    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    std::vector<AbsoluteLocation> missing;
    for (uint64_t object = 2; object < objectsPerGroup(); object++) {
      missing.emplace_back(2, object);
    }
    for (uint64_t object = 0; object < objectsPerGroup(); object++) {
      missing.emplace_back(3, object);
    }
    EXPECT_EQ(viewer->missing(), missing);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 0}, {4, 1}, {5, 1}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.gracefulSwitches.completed, 1);
    EXPECT_EQ(stats.tailCutoffs.cutoffs, 1);
    // An object stream ends with its object.
    EXPECT_EQ(
        stats.tailCutoffs.unfinishedGroups,
        GetParam().shape.delivery == SwitchTrackShape::Delivery::ObjectStreams
            ? 0
            : 1);
    EXPECT_EQ(stats.upstreamsLost, 0);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 1 takes over from hub 0, then goes away too, and the proxy moves the
// track on to hub 2 the same way.
TEST_P(MoQProxyGracefulSwitchTest, AHubThatTookOverCanGoAwayInTurn) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(2, 2, /*hubCount=*/3);
    hubs_[1]->publishGroup(kTrack, 2);
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 2);
    co_await waitForHandover();
    for (uint64_t group = 2; group < 4; group++) {
      hubs_[1]->publishGroup(kTrack, group);
      hubs_[2]->publishGroup(kTrack, group);
    }
    co_await settle();

    goAway(Goaway{.timeout = uint64_t(kGoawayTimeout.count())}, /*hub=*/1);
    co_await waitForSubscribe(*hubs_[2], 1);
    hubs_[2]->publishGroup(kTrack, 4);
    co_await settle();
    hubs_[1]->publishGroup(kTrack, 4);
    co_await waitForHandover(kTrack, /*hub=*/1);
    hubs_[2]->publishGroup(kTrack, 5);
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 1}, {3, 1}, {4, 2}, {5, 2}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.gracefulSwitches.started, 2);
    EXPECT_EQ(stats.gracefulSwitches.completed, 2);
    EXPECT_EQ(stats.upstreamsLost, 0);
    EXPECT_EQ(stats.lostSwitches.started, 0);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 1 took group 4 while hub 0 still owes groups 2 and 3, then its session
// closes. Hub 0 finishes them and serves on after group 4, which hub 1 sent,
// until hub 2 takes over.
TEST_P(
    MoQProxyGracefulSwitchTest,
    AReplacementLostAfterTheSplitLeavesTheOldHubServingUntilTheNext) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(2, 4, /*hubCount=*/3);
    hubs_[1]->publishGroup(kTrack, 4);
    hubs_[0]->publishGroup(kTrack, 2);
    for (uint64_t group = 4; group < 6; group++) {
      hubs_[2]->publishGroup(kTrack, group);
    }
    co_await settle();

    hubs_[1]->closeSessions();
    co_await waitForSubscribe(*hubs_[2], 1);
    for (uint64_t group = 3; group < 6; group++) {
      hubs_[0]->publishGroup(kTrack, group);
    }
    co_await settle();
    hubs_[2]->publishGroup(kTrack, 6);
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 6);
    co_await waitForHandover();
    hubs_[2]->publishGroup(kTrack, 7);
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 0}, {6, 2}, {7, 2}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.gracefulSwitches.started, 1);
    EXPECT_EQ(stats.gracefulSwitches.completed, 1);
    EXPECT_EQ(stats.upstreamsLost, 1);
    EXPECT_EQ(stats.lostSwitches.started, 0);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 1 took group 4 while hub 0 still owes groups 2 and 3, then goes away
// too. The proxy keeps it, since hub 0's group 4 is already refused: hub 1
// takes over once hub 0 caught up, and the proxy then moves the track on to
// hub 2.
TEST_P(
    MoQProxyGracefulSwitchTest,
    AReplacementGoingAwayAfterTheSplitTakesOverThenHandsOn) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(2, 4, /*hubCount=*/3);
    hubs_[1]->publishGroup(kTrack, 4);
    hubs_[2]->publishGroup(kTrack, 4);
    co_await settle();

    goAway(Goaway{.timeout = uint64_t(kGoawayTimeout.count())}, /*hub=*/1);
    co_await settle();
    EXPECT_EQ(hubs_[1]->subscriptions(kTrack), 1);
    EXPECT_THAT(hubs_[2]->subscribeFilters(kTrack), IsEmpty());
    for (uint64_t group = 2; group < 5; group++) {
      hubs_[0]->publishGroup(kTrack, group);
    }
    co_await waitForHandover();
    co_await waitForSubscribe(*hubs_[2], 1);
    hubs_[2]->publishGroup(kTrack, 5);
    co_await settle();
    hubs_[1]->publishGroup(kTrack, 5);
    co_await waitForHandover(kTrack, /*hub=*/1);
    hubs_[2]->publishGroup(kTrack, 6);
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 2}, {6, 2}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.gracefulSwitches.started, 2);
    EXPECT_EQ(stats.gracefulSwitches.completed, 2);
    EXPECT_EQ(stats.upstreamsLost, 0);
    EXPECT_EQ(stats.lostSwitches.started, 0);
    co_await leaveAndExpectIdle();
  }));
}

// The viewer leaves once hub 1 delivered group 4, while hub 0 is partway
// through group 2 and then stalls. The track lasts while a stream of hub 0's is
// open; the tail timer ends it, and the proxy unsubscribes from both hubs.
TEST_P(
    MoQProxyGracefulSwitchTest,
    TheViewerLeavingDuringASwitchUnsubscribesFromBothHubs) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startWithHub0GoingAway(2, 4);
    hubs_[1]->publishGroup(kTrack, 4);
    hubs_[0]->publish(kTrack, 2, 2);
    co_await settle();
    EXPECT_EQ(hubs_[0]->subscriptions(kTrack), 1);
    EXPECT_EQ(hubs_[1]->subscriptions(kTrack), 1);

    for (auto& handle : std::exchange(viewerHandles_, {})) {
      handle->unsubscribe();
    }
    co_await waitUntil([&] {
      return hubs_[0]->subscriptions(kTrack) == 0 &&
          hubs_[1]->subscriptions(kTrack) == 0;
    });

    co_await leaveAndExpectIdle();
  }));
}

// A second viewer joins once hub 1 delivered group 4, while hub 0 still owes
// groups 2 and 3. It starts after group 4, gets none of hub 0's late groups,
// and the switch completes for both viewers.
TEST_P(
    MoQProxyGracefulSwitchTest,
    AViewerJoiningDuringASwitchStartsAfterTheNewestGroup) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(2, 4);
    hubs_[1]->publishGroup(kTrack, 4);
    hubs_[0]->publish(kTrack, 2, 2);
    co_await settle();

    auto joiner = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 2);
    hubs_[0]->publishGroup(kTrack, 3);
    hubs_[0]->publishGroup(kTrack, 4);
    co_await waitForHandover();
    hubs_[1]->publishGroup(kTrack, 5);
    co_await settle();

    expectHandedOver(*viewer, {{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {5, 1}});
    expectIntact(*joiner);
    const std::map<uint64_t, uint64_t> joinerHubOfGroup{{5, 1}};
    EXPECT_EQ(joiner->hubOfGroup(), joinerHubOfGroup);
    co_await leaveAndExpectIdle();
  }));
}

// Of two viewers, the first one's session closes once hub 1 delivered group 4,
// while hub 0 still owes groups 2 and 3. The other gets them from hub 0, then
// hub 1's.
TEST_P(
    MoQProxyGracefulSwitchTest,
    AViewerSessionClosingDuringASwitchLeavesTheOtherIntact) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startWithHub0GoingAway(2, 4);
    auto staying = co_await subscribeViewer();
    hubs_[1]->publishGroup(kTrack, 4);
    hubs_[0]->publish(kTrack, 2, 2);
    co_await settle();

    viewerSessions_.front()->close(SessionCloseErrorCode::NO_ERROR);
    viewerHandles_.erase(viewerHandles_.begin());
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 2);
    hubs_[0]->publishGroup(kTrack, 3);
    hubs_[0]->publishGroup(kTrack, 4);
    co_await waitForHandover();
    hubs_[1]->publishGroup(kTrack, 5);
    co_await settle();

    expectHandedOver(*staying, {{2, 0}, {3, 0}, {4, 1}, {5, 1}});
    co_await leaveAndExpectIdle();
  }));
}

// The proxy closes once hub 1 delivered group 3, while hub 0 still owes group
// 2: the viewer gets GOING_AWAY and the proxy unsubscribes from both hubs.
TEST_P(
    MoQProxyGracefulSwitchTest,
    ClosingTheProxyDuringASwitchUnsubscribesFromBothHubs) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto viewer = co_await startWithHub0GoingAway(2, 2);
    hubs_[1]->publishGroup(kTrack, 3);
    co_await settle();
    EXPECT_EQ(hubs_[0]->subscriptions(kTrack), 1);
    viewer->expectedEnd = PublishDoneStatusCode::GOING_AWAY;

    proxy_->close();
    co_await waitUntil([&] { return viewer->done().has_value(); });

    expectIntact(*viewer);
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::GOING_AWAY);
    EXPECT_EQ(hubs_[0]->subscriptions(kTrack), 0);
    EXPECT_EQ(hubs_[1]->subscriptions(kTrack), 0);
    EXPECT_EQ(proxy_->switchStats().gracefulSwitches.completed, 0);
    co_await leaveAndExpectIdle();
  }));
}

// The proxy closes while the switch waits out its start delay, before it
// subscribes to hub 1. The viewer gets GOING_AWAY, hub 1 gets no SUBSCRIBE,
// and nothing keeps the track waiting out the rest of the delay.
TEST_P(
    MoQProxyGracefulSwitchTest,
    ClosingTheProxyDuringTheStartDelayStopsTheSwitch) {
  maxStartDelay_ = std::chrono::minutes(10);
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[1]->publishGroup(kTrack, 0);
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::GOING_AWAY;
    std::weak_ptr<MoQProxyTrack> track = proxy_->findTrack(kTrack);

    goAway(
        Goaway{
            .timeout = uint64_t(
                std::chrono::milliseconds(std::chrono::hours(1)).count())});
    co_await settle();
    EXPECT_EQ(proxy_->switchStats().gracefulSwitches.started, 1);
    proxy_->close();
    co_await waitUntil([&] { return viewer->done().has_value(); });

    expectIntact(*viewer);
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::GOING_AWAY);
    EXPECT_EQ(hubs_[0]->subscriptions(kTrack), 0);
    EXPECT_THAT(hubs_[1]->subscribeFilters(kTrack), IsEmpty());
    EXPECT_TRUE(track.expired());
    co_await leaveAndExpectIdle();
  }));
}

// The proxy closes while the provider is still connecting the replacement.
// The viewer gets GOING_AWAY, the provider's attempt is cancelled, and nothing
// keeps the track waiting for it.
TEST_P(
    MoQProxyGracefulSwitchTest,
    ClosingTheProxyWhileTheReplacementConnectsStopsTheSwitch) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[1]->publishGroup(kTrack, 0);
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::GOING_AWAY;
    std::weak_ptr<MoQProxyTrack> track = proxy_->findTrack(kTrack);
    const auto attempts = hubProviders_[0]->attempts().size();
    hubProviders_[0]->holdNextAttempt();

    goAway(Goaway{.timeout = uint64_t(kGoawayTimeout.count())});
    co_await waitUntil(
        [&] { return hubProviders_[0]->attempts().size() > attempts; });
    proxy_->close();
    co_await waitUntil([&] { return viewer->done().has_value(); });

    expectIntact(*viewer);
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::GOING_AWAY);
    EXPECT_EQ(hubs_[0]->subscriptions(kTrack), 0);
    EXPECT_THAT(hubs_[1]->subscribeFilters(kTrack), IsEmpty());
    EXPECT_TRUE(track.expired());
    co_await leaveAndExpectIdle();
  }));
}

INSTANTIATE_TEST_SUITE_P(
    AllVersions,
    MoQProxyGracefulSwitchTest,
    ValuesIn(everyShapeCases()),
    caseName);

// The signals of a hub going away, on one track shape.
class MoQProxyGoawaySignalTest : public MoQProxyGracefulSwitchTest {
 protected:
  bool requestStreams() const {
    return getDraftMajorVersion(GetParam().version) >= 18;
  }
};

// Group IDs skip: hub 1 starts at group 9, the next one after hub 0's group 5,
// and no gap is reported for the IDs the origin never used.
TEST_P(MoQProxyGoawaySignalTest, GroupIdsThatSkipHandOverAtTheNextGroup) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    for (uint64_t group : {0, 5}) {
      hubs_[0]->publishGroup(kTrack, group);
      hubs_[1]->publishGroup(kTrack, group);
    }
    co_await settle();
    goAway(Goaway{.timeout = uint64_t(kGoawayTimeout.count())});
    co_await waitForSubscribe(*hubs_[1], 1);

    hubs_[1]->publishGroup(kTrack, 9);
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 9);
    co_await waitForHandover();
    hubs_[1]->publishGroup(kTrack, 14);
    co_await settle();

    expectHandedOver(*viewer, {{0, 0}, {5, 0}, {9, 1}, {14, 1}});
    EXPECT_EQ(proxy_->switchStats().handoffs.equal, 1);
    EXPECT_EQ(proxy_->switchStats().lateReplacementGaps.gaps, 0);
    co_await leaveAndExpectIdle();
  }));
}

// A request-stream GOAWAY moves only its own track: another track on the same
// session stays on hub 0.
TEST_P(MoQProxyGoawaySignalTest, ARequestStreamGoawayMovesOnlyItsTrack) {
  if (!requestStreams()) {
    GTEST_SKIP() << "request-stream GOAWAYs are draft 18+";
  }
  const FullTrackName audio{kTrack.trackNamespace, "audio"};
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto videoViewer = co_await subscribeViewer();
    auto audioViewer = co_await subscribeViewer(audio);
    for (const auto& track : {kTrack, audio}) {
      for (uint64_t group = 0; group < 2; group++) {
        hubs_[0]->publishGroup(track, group);
        hubs_[1]->publishGroup(track, group);
      }
    }
    co_await settle();

    hubProviders_[0]->setAvailable(0, false);
    hubs_[0]->sendRequestGoaway(kTrack, kGoawayTimeout);
    co_await waitForSubscribe(*hubs_[1], 1);
    hubs_[1]->publishGroup(kTrack, 2);
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 2);
    hubs_[0]->publishGroup(audio, 2);
    co_await waitForHandover();

    expectHandedOver(*videoViewer, {{0, 0}, {1, 0}, {2, 1}});
    expectIntact(*audioViewer);
    const std::map<uint64_t, uint64_t> audioHubOfGroup{{0, 0}, {1, 0}, {2, 0}};
    EXPECT_EQ(audioViewer->hubOfGroup(), audioHubOfGroup);
    EXPECT_EQ(hubs_[0]->subscriptions(audio), 1);
    EXPECT_THAT(hubs_[1]->subscribeFilters(audio), IsEmpty());
    EXPECT_EQ(proxy_->switchStats().upstreamGoaways, 1);
    co_await leaveAndExpectIdle({kTrack, audio});
  }));
}

// The session GOAWAY, whose new session URI the proxy ignores, the
// request-stream GOAWAY on draft 18+, and hub 0 then closing make one switch,
// to the hub the provider picks.
TEST_P(MoQProxyGoawaySignalTest, EverySignalOfAHubGoingAwayMakesOneSwitch) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    for (uint64_t group = 0; group < 2; group++) {
      hubs_[0]->publishGroup(kTrack, group);
      hubs_[1]->publishGroup(kTrack, group);
    }
    co_await settle();

    if (requestStreams()) {
      hubs_[0]->sendRequestGoaway(kTrack, kGoawayTimeout);
    }
    goAway(
        Goaway{
            .newSessionUri = "moqt://elsewhere.example/relay",
            .timeout = uint64_t(kGoawayTimeout.count())});
    co_await waitForSubscribe(*hubs_[1], 1);
    hubs_[1]->publishGroup(kTrack, 2);
    co_await settle();
    hubs_[0]->closeSessions();
    co_await settle();
    hubs_[1]->publishGroup(kTrack, 3);
    co_await settle();

    expectIntact(*viewer);
    const std::map<uint64_t, uint64_t> hubOfGroup{
        {0, 0}, {1, 0}, {2, 1}, {3, 1}};
    EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.upstreamGoaways, requestStreams() ? 2 : 1);
    EXPECT_EQ(stats.gracefulSwitches.started, 1);
    EXPECT_EQ(stats.gracefulSwitches.completed, 1);
    EXPECT_EQ(stats.lostSwitches.started, 0);
    EXPECT_THAT(hubs_[1]->subscribeFilters(kTrack), SizeIs(1));
    co_await leaveAndExpectIdle();
  }));
}

// With no other hub, the proxy stops looking at the GOAWAY's timeout, sooner
// than its switch deadline, so viewers get GOING_AWAY once the hub closes then
// rather than a switch deadline later. Through two hops the edge proxy then
// loses its upstream, and passes GOING_AWAY on at its own deadline.
TEST_P(
    MoQProxyGoawaySignalTest,
    AGoawayTimeoutShorterThanTheDeadlineBoundsTheSwitch) {
  if (!requestStreams()) {
    GTEST_SKIP() << "draft 16 GOAWAYs carry no timeout";
  }
  constexpr std::chrono::milliseconds kShortTimeout{100};
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/1);
    auto viewer = co_await subscribeViewer();
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();
    viewer->expectedEnd = PublishDoneStatusCode::GOING_AWAY;

    const auto goawayAt = std::chrono::steady_clock::now();
    hubs_[0]->sendGoaway(Goaway{.timeout = uint64_t(kShortTimeout.count())});
    co_await waitUntil([&] {
      return edgeProxy_ ? edgeProxy_->switchStats().upstreamsLost == 1
                        : viewer->done().has_value();
    });

    EXPECT_LT(std::chrono::steady_clock::now() - goawayAt, kSwitchDeadline);
    co_await waitUntil([&] { return viewer->done().has_value(); });
    expectIntact(*viewer);
    CO_ASSERT_TRUE(viewer->done().has_value());
    EXPECT_EQ(viewer->done()->statusCode, PublishDoneStatusCode::GOING_AWAY);
    const auto& stats = proxy_->switchStats();
    EXPECT_EQ(stats.gracefulSwitches.started, 1);
    EXPECT_EQ(stats.gracefulSwitches.failed + stats.lostSwitches.failed, 1);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 0 carries 100 tracks on its one session when it goes away, and every
// track moves to hub 1. How their SUBSCRIBEs spread is checked on manual time
// in MoQProxyTrackTest.
TEST_P(MoQProxyGoawaySignalTest, EveryTrackOfAHubGoingAwayMovesToAnotherHub) {
  constexpr size_t kTracks = 100;
  // Under ASan and parallel tests, the last of the 100 SUBSCRIBEs can take
  // longer than kSwitchDeadline, and the switches longer than kGoawayTimeout,
  // after which hub 0 closes its session.
  constexpr std::chrono::milliseconds kManyTracksGoawayTimeout{10000};
  switchDeadline_ = kManyTracksGoawayTimeout;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    std::vector<FullTrackName> tracks;
    std::vector<std::shared_ptr<CheckingViewer>> viewers;
    for (size_t i = 0; i < kTracks; i++) {
      tracks.push_back({kTrack.trackNamespace, fmt::format("video{}", i)});
      viewers.push_back(co_await subscribeViewer(tracks.back()));
    }
    for (const auto& track : tracks) {
      hubs_[0]->publishGroup(track, 0);
      hubs_[1]->publishGroup(track, 0);
    }
    co_await settle();

    goAway(Goaway{.timeout = uint64_t(kManyTracksGoawayTimeout.count())});
    co_await waitUntil([&] {
      return std::ranges::all_of(tracks, [&](const auto& track) {
        return hubs_[1]->subscriptions(track) == 1;
      });
    });
    for (const auto& track : tracks) {
      hubs_[1]->publishGroup(track, 1);
    }
    co_await settle();
    for (const auto& track : tracks) {
      hubs_[0]->publishGroup(track, 1);
    }
    co_await waitUntil([&] {
      return std::ranges::all_of(tracks, [&](const auto& track) {
        return hubs_[0]->subscriptions(track) == 0;
      });
    });

    for (const auto& viewer : viewers) {
      expectIntact(*viewer);
      const std::map<uint64_t, uint64_t> hubOfGroup{{0, 0}, {1, 1}};
      EXPECT_EQ(viewer->hubOfGroup(), hubOfGroup);
    }
    EXPECT_EQ(proxy_->switchStats().gracefulSwitches.completed, kTracks);
    EXPECT_EQ(proxy_->switchStats().upstreamsLost, 0);
    co_await leaveAndExpectIdle(tracks);
  }));
}

INSTANTIATE_TEST_SUITE_P(
    AllVersions,
    MoQProxyGoawaySignalTest,
    ValuesIn(faultCases()),
    caseName);

class MoQProxyChainSwitchTest : public MoQProxyUpstreamSwitchTest {};

// viewer <-> edge proxy <-> proxy 0 or proxy 1 <-> hub. Proxy 0 shuts down
// and sends GOING_AWAY; the edge proxy refuses proxy 0's session and resumes
// from proxy 1.
TEST_P(MoQProxyChainSwitchTest, AnUpstreamProxyShuttingDownIsReplaced) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto hub = addHub();
    std::vector<std::shared_ptr<MoQUpstreamProvider>> edgeProviders;
    std::vector<std::shared_ptr<MoQProxy>> inner;
    for (int i = 0; i < 2; i++) {
      auto* hubLink = co_await links_.connect(hub);
      hub->attach(hubLink->server);
      auto hubProvider = std::make_shared<ScriptedUpstreamProvider>();
      hubProvider->addUpstream(hubLink->client);
      inner.push_back(makeProxy({hubProvider}));
      auto* proxyLink = co_await links_.connect(inner.back());
      auto edgeProvider = std::make_shared<ScriptedUpstreamProvider>();
      edgeProvider->addUpstream(proxyLink->client);
      edgeProviders.push_back(edgeProvider);
    }
    edgeProxy_ = makeProxy(edgeProviders);
    auto viewer = co_await subscribeViewer();
    hub->publishGroup(kTrack, 0);
    co_await settle();

    inner[0]->close();
    co_await waitForSubscribe(*hub, 2);
    hub->publishGroup(kTrack, 1);
    co_await settle();

    expectIntact(*viewer);
    // The hub's first subscription served proxy 0, its second proxy 1.
    const std::map<uint64_t, CheckingViewer::Source> sourceOfGroup{
        {0, {0, 0}}, {1, {0, 1}}};
    EXPECT_EQ(viewer->sourceOfGroup(), sourceOfGroup);
    EXPECT_EQ(edgeProxy_->switchStats().upstreamsLost, 1);
    EXPECT_EQ(edgeProxy_->switchStats().lostSwitches.completed, 1);
    EXPECT_EQ(edgeProxy_->switchStats().lostGaps.gaps, 1);
    EXPECT_EQ(inner[0]->switchStats().upstreamsLost, 0);
    co_await leaveAndExpectIdle();
  }));
}

std::vector<SwitchCase> chainCases() {
  std::vector<SwitchCase> cases;
  for (auto version : kSupportedVersions) {
    cases.push_back(SwitchCase{version, Hops::Two, "OneSubgroup", {}});
  }
  return cases;
}

INSTANTIATE_TEST_SUITE_P(
    AllVersions,
    MoQProxyChainSwitchTest,
    ValuesIn(chainCases()),
    caseName);

class MoQProxyMidSubgroupTest : public MoQProxyUpstreamSwitchTest {};

// Hub 1's Largest is object 1 of group 2, so it serves group 2 from object 2:
// subgroup 2's stream starts with its first object and makes group 2 the
// replacement's, and the streams of subgroups 0 and 1 start mid-subgroup and
// are refused, so group 2 reaches the viewer without them.
TEST_P(MoQProxyMidSubgroupTest, AReplacementStreamStartingMidSubgroupIsAGap) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await start(/*hubCount=*/2);
    auto viewer = co_await subscribeViewer();
    for (uint64_t group = 0; group < 2; group++) {
      hubs_[0]->publishGroup(kTrack, group);
      hubs_[1]->publishGroup(kTrack, group);
    }
    hubs_[1]->publish(kTrack, 2, 2);
    co_await settle();
    viewer->expectedGapGroups = {2};

    hubs_[0]->closeSessions();
    co_await waitForSubscribe(*hubs_[1], 1);
    hubs_[1]->publishGroup(kTrack, 2);
    hubs_[1]->publishGroup(kTrack, 3);
    co_await settle();

    EXPECT_THAT(viewer->violations(), IsEmpty());
    EXPECT_THAT(viewer->unfinishedSubgroups(), IsEmpty());
    EXPECT_THAT(
        viewer->missing(),
        ElementsAre(
            AbsoluteLocation(2, 0),
            AbsoluteLocation(2, 1),
            AbsoluteLocation(2, 3),
            AbsoluteLocation(2, 4)));
    EXPECT_EQ(proxy_->switchStats().midSubgroupGaps, 2);
    co_await leaveAndExpectIdle();
  }));
}

// Only draft 18+ says when a stream starts mid-subgroup.
std::vector<SwitchCase> midSubgroupCases() {
  std::vector<SwitchCase> cases;
  for (auto version : kSupportedVersions) {
    if (getDraftMajorVersion(version) < 18) {
      continue;
    }
    for (auto hops : {Hops::One, Hops::Two}) {
      cases.push_back(
          SwitchCase{
              version,
              hops,
              "ThreeSubgroups",
              {.objectsPerGroup = 5, .subgroupsPerGroup = 3}});
    }
  }
  return cases;
}

INSTANTIATE_TEST_SUITE_P(
    Draft18AndLater,
    MoQProxyMidSubgroupTest,
    ValuesIn(midSubgroupCases()),
    caseName);

// Joining FETCH for history the proxy hasn't cached, during and after a switch.
// The hubs publish groups 0 and 1 before the first viewer subscribes, and
// group 2 after.
class MoQProxyJoinDuringSwitchTest : public MoQProxyGracefulSwitchTest {
 protected:
  // For a switch that must outlast a join and its wait, under load too.
  static constexpr std::chrono::milliseconds kLongDeadline{4000};
  static constexpr uint64_t kLongGoawayTimeout{5000};

  folly::coro::Task<std::shared_ptr<CheckingViewer>> startWithUncachedHistory(
      size_t hubCount = 2) {
    co_await start(hubCount);
    for (uint64_t group = 0; group < 2; group++) {
      for (auto& hub : hubs_) {
        hub->publishGroup(kTrack, group);
      }
    }
    co_await settle();
    auto viewer = co_await subscribeViewer();
    for (auto& hub : hubs_) {
      hub->publishGroup(kTrack, 2);
    }
    co_await settle();
    co_return viewer;
  }

  struct PendingJoin {
    std::shared_ptr<CheckingViewer> viewer;
    std::shared_ptr<CheckingFetch> history;
    folly::SemiFuture<Publisher::FetchResult> reply;
  };

  struct Join {
    std::shared_ptr<CheckingViewer> viewer;
    std::shared_ptr<CheckingFetch> history;
    Publisher::FetchResult result;
  };

  // A viewer subscribes, then sends a joining FETCH for everything from
  // fromGroup to its start.
  folly::coro::Task<PendingJoin> startJoin(uint64_t fromGroup = 0) {
    auto viewer = co_await subscribeViewer();
    auto history = std::make_shared<CheckingFetch>(origin_, kTrack);
    history->expectedStart = AbsoluteLocation{fromGroup, 0};
    history->expectedEnd = viewer->expectedStart;
    Fetch fetch(
        RequestID(0),
        viewerHandles_.back()->subscribeOk().requestID,
        /*joiningStart=*/fromGroup,
        FetchType::ABSOLUTE_JOINING,
        kDefaultPriority,
        GroupOrder::OldestFirst);
    fetch.fullTrackName = kTrack;
    auto reply = folly::coro::co_withExecutor(
                     co_await folly::coro::co_current_executor,
                     viewerSessions_.back()->fetch(fetch, history))
                     .start();
    co_await settle();
    co_return PendingJoin{
        std::move(viewer), std::move(history), std::move(reply)};
  }

  folly::coro::Task<Join> finishJoin(PendingJoin pending) {
    co_await waitUntil([&] { return pending.reply.isReady(); });
    if (!pending.reply.isReady()) {
      co_return Join{
          pending.viewer,
          pending.history,
          folly::makeUnexpected(
              FetchError{
                  RequestID(0),
                  FetchErrorCode::INTERNAL_ERROR,
                  "no FETCH reply in time"})};
    }
    co_return Join{
        pending.viewer, pending.history, std::move(pending.reply).get()};
  }

  folly::coro::Task<Join> joinViewer(uint64_t fromGroup = 0) {
    co_return co_await finishJoin(co_await startJoin(fromGroup));
  }

  // The joiner got exactly the objects the origin published from fromGroup to
  // its start, whole and in order, and the FETCH_OK ends at its start.
  static void expectWholeHistory(const Join& join, uint64_t fromGroup = 0) {
    ASSERT_TRUE(join.result.hasValue()) << join.result.error().reasonPhrase;
    EXPECT_EQ(
        join.result.value()->fetchOk().endLocation,
        *join.viewer->expectedStart);
    EXPECT_THAT(join.history->violations(), IsEmpty());
    EXPECT_TRUE(join.history->ended());
    ASSERT_FALSE(join.history->received().empty());
    EXPECT_EQ(join.history->received().front(), AbsoluteLocation(fromGroup, 0));
    EXPECT_THAT(
        join.history->missing({fromGroup, 0}, *join.viewer->expectedStart),
        IsEmpty());
  }

  // Each group of the history came from hub.
  static void expectHistoryFrom(const Join& join, uint64_t hub) {
    std::map<uint64_t, uint64_t> hubOfGroup;
    for (uint64_t group = 0; group <= join.viewer->expectedStart->group;
         group++) {
      hubOfGroup[group] = hub;
    }
    if (join.viewer->expectedStart->object == 0) {
      hubOfGroup.erase(join.viewer->expectedStart->group);
    }
    EXPECT_EQ(join.history->hubOfGroup(), hubOfGroup);
  }

  // Hub 0 serves groups 0 and 1 and goes away, and hub 1 takes over at group
  // 2 and serves group 3.
  folly::coro::Task<void> startWithHub0HandedOverAtGroup2() {
    co_await startWithHub0GoingAway(2, 2);
    hubs_[1]->publishGroup(kTrack, 2);
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 2);
    co_await waitForHandover();
    hubs_[1]->publishGroup(kTrack, 3);
    co_await settle();
  }

  bool oldHubTakesRequests() const {
    return getDraftMajorVersion(GetParam().version) < 18;
  }
};

// While hub 0 goes away, the history comes from it if it still takes requests
// (draft 16), and else from hub 1, the replacement. Through an edge proxy, the
// edge's FETCH reaches whichever hub the pool picks, hub 1.
TEST_P(
    MoQProxyJoinDuringSwitchTest,
    AJoinDuringASwitchGetsItsHistoryFromAHubThatTakesRequests) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startWithUncachedHistory();
    goAway(Goaway{.timeout = uint64_t(kGoawayTimeout.count())});
    co_await waitForSubscribe(*hubs_[1], 1);

    auto join = co_await joinViewer();

    expectWholeHistory(join);
    const uint64_t hub = oldHubTakesRequests() && !edgeProxy_ ? 0 : 1;
    expectHistoryFrom(join, hub);
    EXPECT_EQ(hubs_[hub]->fetches(kTrack), 1);
    EXPECT_EQ(hubs_[1 - hub]->fetches(kTrack), 0);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 0 refuses the FETCH before the proxy has a replacement: the FETCH waits
// until hub 1 is one.
TEST_P(MoQProxyJoinDuringSwitchTest, AJoinWaitsForTheReplacement) {
  if (oldHubTakesRequests() || GetParam().hops == Hops::Two) {
    GTEST_SKIP() << "the proxy's FETCH goes to hub 0 or through the pool";
  }
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    switchDeadline_ = kLongDeadline;
    co_await startWithUncachedHistory();
    hubProviders_[0]->failNextAttempts(1000);
    goAway(Goaway{.timeout = kLongGoawayTimeout});
    co_await settle();

    auto pending = co_await startJoin();
    EXPECT_FALSE(pending.reply.isReady());
    EXPECT_EQ(hubs_[1]->subscriptions(kTrack), 0);
    hubProviders_[0]->failNextAttempts(0);
    auto join = co_await finishJoin(std::move(pending));

    expectWholeHistory(join);
    expectHistoryFrom(join, 1);
    EXPECT_EQ(hubs_[0]->fetches(kTrack), 0);
    EXPECT_EQ(hubs_[1]->fetches(kTrack), 1);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 1 delivered group 4 while hub 0 still owes groups 2 and 3, so the proxy
// holds hub 1 until hub 0 catches up. Hub 0 refuses the FETCH, and hub 1
// serves it before it takes over.
TEST_P(
    MoQProxyJoinDuringSwitchTest,
    AJoinIsServedByTheReplacementBeforeItTakesOver) {
  if (oldHubTakesRequests() || GetParam().hops == Hops::Two) {
    GTEST_SKIP() << "the proxy's FETCH goes to hub 0 or through the pool";
  }
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startWithHub0GoingAway(2, 4);
    hubs_[1]->publishGroup(kTrack, 4);
    co_await settle();

    auto join = co_await joinViewer();

    expectWholeHistory(join);
    expectHistoryFrom(join, 1);
    EXPECT_EQ(hubs_[0]->subscriptions(kTrack), 1);
    EXPECT_EQ(hubs_[1]->fetches(kTrack), 1);
    hubs_[0]->publishGroup(kTrack, 2);
    hubs_[0]->publishGroup(kTrack, 3);
    co_await waitForHandover();
    co_await leaveAndExpectIdle();
  }));
}

// Hub 1, the replacement, goes away too once it froze the split, so the proxy
// keeps it until hub 0 catches up: it refuses the FETCH as well, which waits
// for hub 2, the replacement of the switch from hub 1.
TEST_P(
    MoQProxyJoinDuringSwitchTest,
    AJoinRefusedByAReplacementGoingAwayWaitsForTheNext) {
  if (oldHubTakesRequests() || GetParam().hops == Hops::Two) {
    GTEST_SKIP() << "the proxy's FETCH goes to hub 0 or through the pool";
  }
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    // The switch from hub 1 starts once it takes over, within its deadline.
    switchDeadline_ = kLongDeadline;
    co_await startWithHub0GoingAway(2, 4, /*hubCount=*/3);
    hubs_[1]->publishGroup(kTrack, 4);
    co_await settle();
    goAway(Goaway{.timeout = kLongGoawayTimeout}, /*hub=*/1);
    co_await settle();

    auto pending = co_await startJoin();
    EXPECT_FALSE(pending.reply.isReady());
    hubs_[2]->publishGroup(kTrack, 4);
    hubs_[0]->publishGroup(kTrack, 2);
    hubs_[0]->publishGroup(kTrack, 3);
    co_await waitForHandover();
    auto join = co_await finishJoin(std::move(pending));

    expectWholeHistory(join);
    expectHistoryFrom(join, 2);
    EXPECT_EQ(hubs_[0]->fetches(kTrack), 0);
    EXPECT_EQ(hubs_[1]->fetches(kTrack), 0);
    EXPECT_EQ(hubs_[2]->fetches(kTrack), 1);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 0 refuses the FETCH, and the switch fails at its deadline without a
// replacement: the FETCH fails with GOING_AWAY.
TEST_P(
    MoQProxyJoinDuringSwitchTest,
    AJoinFailsWhenTheSwitchFindsNoReplacement) {
  if (oldHubTakesRequests() || GetParam().hops == Hops::Two) {
    GTEST_SKIP() << "the proxy's FETCH goes to hub 0 or through the pool";
  }
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    switchDeadline_ = std::chrono::milliseconds(1000);
    co_await startWithUncachedHistory();
    hubProviders_[0]->failNextAttempts(1000);
    goAway(Goaway{.timeout = uint64_t(kGoawayTimeout.count())});
    co_await settle();

    auto pending = co_await startJoin();
    EXPECT_FALSE(pending.reply.isReady());
    auto join = co_await finishJoin(std::move(pending));

    CO_ASSERT_TRUE(join.result.hasError());
    EXPECT_EQ(join.result.error().errorCode, FetchErrorCode::GOING_AWAY);
    EXPECT_EQ(join.result.error().reasonPhrase, "Session received GOAWAY");
    EXPECT_EQ(proxy_->switchStats().gracefulSwitches.failed, 1);
    EXPECT_EQ(hubs_[0]->fetches(kTrack), 0);
    co_await leaveAndExpectIdle();
  }));
}

// After hub 1 took over at group 2, the proxy has groups 1 to 3 cached, the
// newest three. History from group 1 spans both hubs' groups and comes from
// the cache, so no hub gets a FETCH.
TEST_P(MoQProxyJoinDuringSwitchTest, AJoinAfterTheSwitchIsServedFromTheCache) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startWithHub0HandedOverAtGroup2();

    auto join = co_await joinViewer(/*fromGroup=*/1);

    expectWholeHistory(join, /*fromGroup=*/1);
    const std::map<uint64_t, uint64_t> hubOfGroup{{1, 0}, {2, 1}, {3, 1}};
    EXPECT_EQ(join.history->hubOfGroup(), hubOfGroup);
    EXPECT_EQ(hubs_[0]->fetches(kTrack), 0);
    EXPECT_EQ(hubs_[1]->fetches(kTrack), 0);
    co_await leaveAndExpectIdle();
  }));
}

// Group 0 has left the cache, so history from group 0 comes whole from hub 1,
// and none of it from the cache, which holds hub 0's group 1.
TEST_P(
    MoQProxyJoinDuringSwitchTest,
    HistoryPartlyEvictedAfterTheSwitchComesWholeFromTheReplacement) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startWithHub0HandedOverAtGroup2();

    auto join = co_await joinViewer();

    expectWholeHistory(join);
    expectHistoryFrom(join, 1);
    EXPECT_EQ(hubs_[0]->fetches(kTrack), 0);
    EXPECT_EQ(hubs_[1]->fetches(kTrack), 1);
    co_await leaveAndExpectIdle();
  }));
}

// Hub 0's session closes partway through group 3, and hub 1 takes over at
// group 4: the history spans the gap, so it comes whole from hub 1. A joiner
// that arrives while the proxy still looks for a replacement waits for it.
TEST_P(
    MoQProxyJoinDuringSwitchTest,
    AJoinSpanningALossGapGetsItFromTheReplacement) {
  if (GetParam().hops == Hops::Two) {
    GTEST_SKIP() << "the edge proxy's FETCH goes through the pool";
  }
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    switchDeadline_ = kLongDeadline;
    auto viewer = co_await startWithUncachedHistory();
    hubs_[1]->publishGroup(kTrack, 3);
    hubs_[0]->publish(kTrack, 3, 2);
    co_await settle();
    viewer->expectedReset = ResetStreamErrorCode::SESSION_CLOSED;
    hubProviders_[0]->setAvailable(0, false);
    hubProviders_[0]->failNextAttempts(1000);
    hubs_[0]->closeSessions();
    co_await settle();

    auto pending = co_await startJoin();
    EXPECT_FALSE(pending.reply.isReady());
    EXPECT_EQ(hubs_[1]->subscriptions(kTrack), 0);
    hubProviders_[0]->failNextAttempts(0);
    auto join = co_await finishJoin(std::move(pending));
    hubs_[1]->publishGroup(kTrack, 4);
    co_await settle();

    expectWholeHistory(join);
    expectHistoryFrom(join, 1);
    EXPECT_EQ(hubs_[1]->fetches(kTrack), 1);
    EXPECT_EQ(proxy_->switchStats().lostSwitches.completed, 1);
    co_await leaveAndExpectIdle();
  }));
}

// Joining FETCH exists on drafts 16 and 18; draft 22 replaces it with a fill.
std::vector<SwitchCase> joinCases() {
  std::vector<SwitchCase> cases;
  for (auto version : kSupportedVersions) {
    if (getDraftMajorVersion(version) >= 22) {
      continue;
    }
    for (auto hops : {Hops::One, Hops::Two}) {
      cases.push_back(SwitchCase{version, hops, "OneSubgroup", {}});
    }
  }
  return cases;
}

INSTANTIATE_TEST_SUITE_P(
    JoiningFetch,
    MoQProxyJoinDuringSwitchTest,
    ValuesIn(joinCases()),
    caseName);

}} // namespace moxygen::test
