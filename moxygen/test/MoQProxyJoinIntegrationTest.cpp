/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Joining FETCH through real MoQSession pairs on FakeSharedWebTransport:
// viewer <-> proxy <-> origin, for every supported version.

#include <folly/coro/BlockingWait.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/coro/Invoke.h>
#include <folly/io/async/EventBase.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>
#include <cinttypes>
#include <cstdio>
#include <map>
#include <random>
#include <set>

#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQVersions.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQProxy.h"
#include "moxygen/proxy/test/RecordingFetchConsumer.h"
#include "moxygen/relay/MoQRelay.h"
#include "moxygen/test/LiveViewerConsumer.h"
#include "moxygen/test/Mocks.h"

namespace moxygen::test {
namespace {

using namespace std::chrono_literals;
using proxygen::test::FakeSharedWebTransport;

const FullTrackName kTrack{TrackNamespace{{"live"}}, "video"};
constexpr uint64_t kMaxRequestID = 100;

Payload payloadOf(size_t length) {
  return folly::IOBuf::copyBuffer(std::string(length, 'x'));
}

TrackConsumer::BeginSubgroupOptions lastInGroup() {
  TrackConsumer::BeginSubgroupOptions options;
  options.containsLastInGroup = true;
  return options;
}

SubscribeRequest liveSubscribe() {
  auto request = SubscribeRequest::make(kTrack);
  request.locType = LocationType::LargestObject;
  return request;
}

// The origin: serves one live track. It answers FETCH only from history a
// test provides, as published location to subgroup.
class OriginPublisher : public Publisher {
 public:
  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest sub,
      std::shared_ptr<TrackConsumer> consumer) override {
    if (gate) {
      co_await *gate;
    }
    consumer->setTrackAlias(TrackAlias(sub.requestID.value));
    track = std::move(consumer);
    SubscribeOk ok{
        .requestID = sub.requestID,
        .trackAlias = TrackAlias(sub.requestID.value),
        .groupOrder = groupOrder,
        .largest = largest};
    // Draft 16+ carries the publisher's group order only as a track property.
    ok.extensions.insertMutableExtension(Extension(
        kPublisherGroupOrderExtensionType, folly::to_underlying(groupOrder)));
    co_return std::make_shared<testing::NiceMock<MockSubscriptionHandle>>(
        std::move(ok));
  }

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override {
    fetches.push_back(fetch);
    const auto* range = std::get_if<StandaloneFetch>(&fetch.args);
    if (!history || !range) {
      co_return folly::makeUnexpected(
          FetchError{
              fetch.requestID,
              FetchErrorCode::NOT_SUPPORTED,
              "no history at the origin"});
    }
    // An end object of 0 selects all of the end group.
    auto inRange = [&](const AbsoluteLocation& location) {
      return !(location < range->start) &&
          (range->end.object == 0 ? location.group <= range->end.group
                                  : location < range->end);
    };
    std::vector<std::pair<AbsoluteLocation, uint64_t>> served;
    for (const auto& [location, subgroup] : *history) {
      if (inRange(location)) {
        served.emplace_back(location, subgroup);
      }
    }
    for (size_t i = 0; i < served.size(); i++) {
      const auto& [location, subgroup] = served[i];
      consumer->object(
          location.group,
          subgroup,
          location.object,
          payloadOf(10),
          noExtensions(),
          /*finFetch=*/i + 1 == served.size());
    }
    if (served.empty()) {
      consumer->endOfFetch();
    }
    co_return std::make_shared<testing::NiceMock<MockFetchHandle>>(FetchOk{
        fetch.requestID,
        GroupOrder::OldestFirst,
        /*endOfTrack=*/0,
        range->end});
  }

  std::shared_ptr<TrackConsumer> track;
  GroupOrder groupOrder{GroupOrder::OldestFirst};
  std::optional<AbsoluteLocation> largest{AbsoluteLocation{40, 0}};
  std::vector<Fetch> fetches;
  folly::coro::Baton* gate{nullptr};
  const std::map<AbsoluteLocation, uint64_t>* history{nullptr};
};

class FixedUpstreamProvider : public MoQUpstreamProvider {
 public:
  explicit FixedUpstreamProvider(std::shared_ptr<MoQSession> session)
      : session_(std::move(session)) {}

  folly::coro::Task<MoQUpstreamSessionResult> getSession(
      const FullTrackName&,
      const TrackRequestParameters&,
      bool) override {
    co_return session_;
  }

 private:
  std::shared_ptr<MoQSession> session_;
};

class ProxyJoinIntegrationTest : public testing::TestWithParam<uint64_t>,
                                 public MoQSession::ServerSetupCallback {
 public:
  folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
      const moxygen::Setup&,
      uint64_t,
      std::shared_ptr<MoQSession>) override {
    return folly::unit;
  }

  folly::Try<moxygen::Setup> onClientSetup(
      moxygen::Setup,
      const std::shared_ptr<MoQSession>&) override {
    moxygen::Setup setup;
    setup.params.insertParam(
        SetupParameter{
            folly::to_underlying(SetupKey::MAX_REQUEST_ID), kMaxRequestID});
    if (!useBidiRequestStreams(GetParam())) {
      setup.params.insertParam(
          SetupParameter{
              folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE), 16});
    }
    return folly::Try<moxygen::Setup>(std::move(setup));
  }

 protected:
  struct Link {
    std::unique_ptr<FakeSharedWebTransport> clientWt;
    std::unique_ptr<FakeSharedWebTransport> serverWt;
    std::shared_ptr<MoQSession> client;
    std::shared_ptr<MoQSession> server;
  };

  void TearDown() override {
    if (proxy_) {
      proxy_->close();
    }
    for (auto& link : links_) {
      link->client->close(SessionCloseErrorCode::NO_ERROR);
    }
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

  folly::coro::Task<Link*> connect(
      std::shared_ptr<Publisher> serverPublisher,
      std::shared_ptr<Subscriber> serverSubscriber = nullptr) {
    auto link = std::make_unique<Link>();
    std::tie(link->clientWt, link->serverWt) =
        FakeSharedWebTransport::makeSharedWebTransport();
    link->client = std::make_shared<MoQRelaySession>(
        folly::MaybeManagedPtr<proxygen::WebTransport>(link->clientWt.get()),
        executor_);
    link->serverWt->setPeerHandler(link->client.get());
    link->server = std::make_shared<MoQRelaySession>(
        folly::MaybeManagedPtr<proxygen::WebTransport>(link->serverWt.get()),
        *this,
        executor_);
    link->clientWt->setPeerHandler(link->server.get());
    if (getDraftMajorVersion(GetParam()) >= 15) {
      auto alpn = getAlpnFromVersion(GetParam());
      link->client->validateAndSetVersionFromAlpn(*alpn);
      link->server->validateAndSetVersionFromAlpn(*alpn);
    }
    link->server->setPublishHandler(std::move(serverPublisher));
    if (serverSubscriber) {
      link->server->setSubscribeHandler(std::move(serverSubscriber));
    }
    link->client->start();
    link->server->start();
    if (useUniControlStreams(GetParam())) {
      moxygen::Setup serverSetup;
      serverSetup.params.insertParam(
          SetupParameter{
              folly::to_underlying(SetupKey::MAX_REQUEST_ID), kMaxRequestID});
      link->server->sendSetup(std::move(serverSetup));
    }
    moxygen::Setup clientSetup;
    clientSetup.params.insertParam(
        SetupParameter{folly::to_underlying(SetupKey::PATH), "/foo"});
    clientSetup.params.insertParam(
        SetupParameter{
            folly::to_underlying(SetupKey::MAX_REQUEST_ID), kMaxRequestID});
    clientSetup.params.insertParam(
        SetupParameter{
            folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE), 16});
    co_await link->client->setup(std::move(clientSetup));
    links_.push_back(std::move(link));
    co_return links_.back().get();
  }

  folly::coro::Task<void> startProxy(MoQProxyOptions options = {}) {
    upstreamLink_ = co_await connect(origin_);
    // Generous, so the completion below always lands inside the wait.
    options.cacheCompletionWait = 5s;
    proxy_ = MoQProxy::create(
        {std::make_shared<FixedUpstreamProvider>(upstreamLink_->client)},
        options);
  }

  // A viewer whose subscription keeps the proxy's upstream subscription warm.
  folly::coro::Task<std::shared_ptr<LiveViewerConsumer>> warmViewer() {
    auto* viewer = co_await connect(proxy_);
    auto live = std::make_shared<LiveViewerConsumer>();
    auto res = co_await viewer->client->subscribe(liveSubscribe(), live);
    EXPECT_TRUE(res.hasValue());
    if (res.hasValue()) {
      handles_.push_back(std::move(res.value()));
    }
    co_return live;
  }

  folly::SemiFuture<MoQSession::JoinResult> startJoin(
      Link& viewer,
      std::shared_ptr<TrackConsumer> live,
      std::shared_ptr<FetchConsumer> history,
      uint64_t joiningStart,
      GroupOrder groupOrder = GroupOrder::Default) {
    return folly::coro::co_withExecutor(
               &eventBase_,
               viewer.client->join(
                   liveSubscribe(),
                   std::move(live),
                   joiningStart,
                   129,
                   groupOrder,
                   {},
                   std::move(history),
                   FetchType::ABSOLUTE_JOINING))
        .start();
  }

  template <typename Predicate>
  folly::coro::Task<bool> waitUntil(Predicate predicate) {
    for (int i = 0; i < 2000 && !predicate(); i++) {
      co_await folly::coro::co_reschedule_on_current_executor;
    }
    co_return predicate();
  }

  folly::coro::Task<void> settle() {
    for (int i = 0; i < 100; i++) {
      co_await folly::coro::co_reschedule_on_current_executor;
    }
  }

  // Objects [0, count) of a group, ended with the subgroup's last-in-group
  // flag and a clean FIN; no END_OF_GROUP status is sent.
  void publishGroup(uint64_t group, uint64_t count, size_t objectBytes = 10) {
    auto subgroup = origin_->track->beginSubgroup(
        group, 0, kDefaultPriority, lastInGroup());
    ASSERT_TRUE(subgroup.hasValue());
    for (uint64_t objectID = 0; objectID < count; objectID++) {
      (*subgroup)->object(
          objectID,
          payloadOf(objectBytes),
          noExtensions(),
          objectID + 1 == count);
    }
  }

  // Unblocks every FETCH stream the proxy is holding for this viewer.
  static void makeWritable(Link& viewer) {
    for (auto& [_, handle] : viewer.serverWt->writeHandles) {
      if (handle->pendingWritableWaiters() > 0) {
        handle->setWritable(true);
      }
    }
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQExecutor> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_)};
  std::shared_ptr<OriginPublisher> origin_{std::make_shared<OriginPublisher>()};
  Link* upstreamLink_{nullptr};
  std::shared_ptr<MoQProxy> proxy_;
  // Outlives the sessions closed in TearDown, as a relay server's relay does.
  std::shared_ptr<MoQRelay> relay_;
  std::vector<std::unique_ptr<Link>> links_;
  std::vector<std::shared_ptr<Publisher::SubscriptionHandle>> handles_;
};

std::vector<AbsoluteLocation> historyLocations(
    const std::vector<std::string>& log) {
  std::vector<AbsoluteLocation> locations;
  for (const auto& entry : log) {
    uint64_t group = 0;
    uint64_t object = 0;
    if (std::sscanf(
            entry.c_str(), "object %" SCNu64 "/%" SCNu64, &group, &object) ==
        2) {
      locations.push_back({group, object});
    }
  }
  return locations;
}

// One subgroup of a scripted group: its objects in publishing order.
struct ScriptedSubgroup {
  uint64_t group;
  uint64_t subgroup;
  bool containsLastInGroup;
  std::vector<uint64_t> objects;
  size_t next{0};
  std::shared_ptr<SubgroupConsumer> consumer;
};

// Groups of one to six objects spread over one to three subgroups.
std::vector<std::vector<ScriptedSubgroup>>
scriptGroups(std::mt19937& rng, uint64_t firstGroup, uint64_t groupCount) {
  std::vector<std::vector<ScriptedSubgroup>> groups;
  for (uint64_t group = firstGroup; group < firstGroup + groupCount; group++) {
    const uint64_t objectCount = 1 + rng() % 6;
    const uint64_t subgroupCount =
        1 + rng() % std::min<uint64_t>(3, objectCount);
    std::vector<ScriptedSubgroup> subgroups;
    for (uint64_t subgroup = 0; subgroup < subgroupCount; subgroup++) {
      subgroups.push_back(
          ScriptedSubgroup{
              .group = group,
              .subgroup = subgroup,
              .containsLastInGroup =
                  (objectCount - 1) % subgroupCount == subgroup});
    }
    for (uint64_t objectID = 0; objectID < objectCount; objectID++) {
      subgroups[objectID % subgroupCount].objects.push_back(objectID);
    }
    groups.push_back(std::move(subgroups));
  }
  return groups;
}

std::vector<std::string> historyThrough432() {
  return {
      "object 41/0 10",
      "object 41/1 10",
      "object 42/0 10",
      "object 42/1 10",
      "object 43/0 10",
      "object 43/1 10",
      "object 43/2 20",
      "endOfFetch"};
}

} // namespace

// The principal scenario: groups 41 and 42 completed by the header flag and
// a clean FIN, group 43 mid-object. Joiners whose Joining Location is that
// object wait for it locally, receive complete history through it, and then
// only later locations live. The origin is never asked for history.
TEST_P(ProxyJoinIntegrationTest, MidObjectJoinsAreServedFromTheProxyCache) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    auto warmLive = co_await warmViewer();
    CO_ASSERT_TRUE(origin_->track != nullptr);
    publishGroup(41, 2);
    publishGroup(42, 2);
    auto active =
        origin_->track->beginSubgroup(43, 0, kDefaultPriority, {}).value();
    active->object(0, payloadOf(10), noExtensions(), false);
    active->object(1, payloadOf(10), noExtensions(), false);
    active->beginObject(2, 20, payloadOf(5), noExtensions());
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return warmLive->received.size() == 7; }));

    auto* first = co_await connect(proxy_);
    auto* second = co_await connect(proxy_);
    auto firstLive = std::make_shared<LiveViewerConsumer>();
    auto secondLive = std::make_shared<LiveViewerConsumer>();
    auto firstHistory = std::make_shared<RecordingFetchConsumer>();
    auto secondHistory = std::make_shared<RecordingFetchConsumer>();
    auto firstJoin = startJoin(*first, firstLive, firstHistory, 41);
    auto secondJoin = startJoin(*second, secondLive, secondHistory, 41);
    co_await settle();
    EXPECT_TRUE(origin_->fetches.empty());
    EXPECT_TRUE(firstHistory->log.empty());

    active->objectPayload(payloadOf(15), false);
    auto firstRes = co_await std::move(firstJoin);
    auto secondRes = co_await std::move(secondJoin);
    CO_ASSERT_TRUE(firstRes.subscribeResult.hasValue());
    CO_ASSERT_TRUE(firstRes.fetchResult.hasValue());
    CO_ASSERT_TRUE(secondRes.fetchResult.hasValue());
    EXPECT_EQ(
        firstRes.subscribeResult.value()->subscribeOk().largest,
        AbsoluteLocation(43, 2));
    CO_ASSERT_TRUE(co_await waitUntil([&] {
      return firstHistory->log.size() == 8 && secondHistory->log.size() == 8;
    }));
    EXPECT_EQ(firstHistory->log, historyThrough432());
    EXPECT_EQ(secondHistory->log, historyThrough432());

    active->object(3, payloadOf(10), noExtensions(), false);
    origin_->track->beginSubgroup(44, 0, kDefaultPriority, {})
        .value()
        ->object(0, payloadOf(10), noExtensions(), false);
    CO_ASSERT_TRUE(co_await waitUntil([&] {
      return firstLive->received.size() == 2 &&
          secondLive->received.size() == 2;
    }));
    const std::vector<AbsoluteLocation> live{{43, 3}, {44, 0}};
    EXPECT_EQ(firstLive->received, live);
    EXPECT_EQ(secondLive->received, live);
    EXPECT_TRUE(origin_->fetches.empty());
    auto cache = proxy_->findTrack(kTrack)->groupCache();
    EXPECT_EQ(cache->residentGroups(), 3);
    EXPECT_EQ(cache->retiredThrough(), 41);
  }));
}

// For a newest-first publisher. A join that omits its group order is
// ascending on the wire and replayed oldest first; an explicit newest-first
// join gets the newest group first.
TEST_P(ProxyJoinIntegrationTest, NewestFirstPublisherJoinsReplayInFetchOrder) {
  origin_->groupOrder = GroupOrder::NewestFirst;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    auto warmLive = co_await warmViewer();
    CO_ASSERT_TRUE(origin_->track != nullptr);
    publishGroup(41, 2);
    publishGroup(42, 2);
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return warmLive->received.size() == 4; }));

    auto* defaultViewer = co_await connect(proxy_);
    auto* newestViewer = co_await connect(proxy_);
    auto defaultHistory = std::make_shared<RecordingFetchConsumer>();
    auto newestHistory = std::make_shared<RecordingFetchConsumer>();
    auto defaultRes = co_await startJoin(
        *defaultViewer,
        std::make_shared<LiveViewerConsumer>(),
        defaultHistory,
        41);
    auto newestRes = co_await startJoin(
        *newestViewer,
        std::make_shared<LiveViewerConsumer>(),
        newestHistory,
        41,
        GroupOrder::NewestFirst);

    CO_ASSERT_TRUE(defaultRes.fetchResult.hasValue());
    CO_ASSERT_TRUE(newestRes.fetchResult.hasValue());
    CO_ASSERT_TRUE(co_await waitUntil([&] {
      return defaultHistory->log.size() == 5 && newestHistory->log.size() == 5;
    }));
    const std::vector<std::string> oldestFirst{
        "object 41/0 10",
        "object 41/1 10",
        "object 42/0 10",
        "object 42/1 10",
        "endOfFetch"};
    const std::vector<std::string> newestFirst{
        "object 42/0 10",
        "object 42/1 10",
        "object 41/0 10",
        "object 41/1 10",
        "endOfFetch"};
    EXPECT_EQ(defaultHistory->log, oldestFirst);
    EXPECT_EQ(newestHistory->log, newestFirst);
    EXPECT_FALSE(defaultHistory->resetCode.has_value());
    EXPECT_TRUE(origin_->fetches.empty());
  }));
}

// The real FETCH writer accepts writes even when the transport is not
// writable; the replay must still wait for writability between objects.
TEST_P(ProxyJoinIntegrationTest, LocalReplayWaitsForTransportWritability) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    co_await warmViewer();
    publishGroup(41, 3);
    co_await settle();
    auto* viewer = co_await connect(proxy_);
    viewer->serverWt->setNewStreamsWritable(false);
    auto history = std::make_shared<RecordingFetchConsumer>();

    auto join =
        startJoin(*viewer, std::make_shared<LiveViewerConsumer>(), history, 41);
    auto res = co_await std::move(join);
    CO_ASSERT_TRUE(res.fetchResult.hasValue());
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return history->log.size() == 1; }));
    co_await settle();
    EXPECT_EQ(history->log.size(), 1);
    for (auto& [_, handle] : viewer->serverWt->writeHandles) {
      if (handle->pendingWritableWaiters() > 0) {
        handle->setWritable(true);
      }
    }

    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return history->log.size() == 4; }));
    const std::vector<std::string> expected{
        "object 41/0 10", "object 41/1 10", "object 41/2 10", "endOfFetch"};
    EXPECT_EQ(history->log, expected);
    EXPECT_TRUE(origin_->fetches.empty());
  }));
}

// A relay's joining FETCH used to be forwarded upstream unresolved when it
// arrived before the relay finished subscribing. With the session holding it
// until the SUBSCRIBE is answered, the relay resolves it to a standalone
// range instead.
TEST_P(
    ProxyJoinIntegrationTest,
    RelayResolvesJoinAfterSubscriptionEstablished) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    relay_ = std::make_shared<MoQRelay>();
    folly::coro::Baton releaseSubscribe;
    origin_->gate = &releaseSubscribe;
    auto* originLink = co_await connect(relay_, relay_);
    originLink->client->setPublishHandler(origin_);
    auto announced = co_await originLink->client->publishNamespace(
        PublishNamespace{RequestID(0), kTrack.trackNamespace}, nullptr);
    CO_ASSERT_TRUE(announced.hasValue());
    auto* viewer = co_await connect(relay_);
    auto history = std::make_shared<RecordingFetchConsumer>();

    auto join =
        startJoin(*viewer, std::make_shared<LiveViewerConsumer>(), history, 0);
    co_await settle();
    releaseSubscribe.post();
    auto res = co_await std::move(join);

    EXPECT_TRUE(res.subscribeResult.hasValue());
    // The relay's cache can answer the joining FETCH before its own upstream
    // FETCH reaches the origin.
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return !origin_->fetches.empty(); }));
    EXPECT_EQ(origin_->fetches.size(), 1);
    EXPECT_NE(std::get_if<StandaloneFetch>(&origin_->fetches[0].args), nullptr);
  }));
}

// The publisher interleaves the subgroups of adjacent groups, so objects
// reach the proxy out of location order, and viewers join at random points.
// Whether its history comes from the cache or upstream, each viewer receives
// exactly the track: history is every object from its start group through
// its Joining Location, live is every later object, with nothing missing or
// duplicated.
TEST_P(ProxyJoinIntegrationTest, RandomizedPublisherJoinsSeeExactlyTheTrack) {
  // Both paths must have run: local replay, and upstream FETCH for ranges
  // the cache cannot prove.
  uint64_t localJoins = 0;
  uint64_t upstreamJoins = 0;
  for (uint32_t seed = 1; seed <= 8; seed++) {
    SCOPED_TRACE(fmt::format("seed={}", seed));
    run(folly::coro::co_invoke([&, seed]() -> folly::coro::Task<void> {
      std::mt19937 rng(seed);
      co_await startProxy(MoQProxyOptions{.maxGroupsPerTrack = 64});
      co_await warmViewer();
      CO_ASSERT_TRUE(origin_->track != nullptr);
      struct Viewer {
        uint64_t joiningStart;
        std::shared_ptr<LiveViewerConsumer> live;
        std::shared_ptr<RecordingFetchConsumer> history;
        folly::SemiFuture<MoQSession::JoinResult> join;
      };
      std::vector<Viewer> viewers;
      std::set<AbsoluteLocation> published;
      auto groups = scriptGroups(rng, 41, 8);
      // The origin has produced every object it has not sent yet, so its
      // FETCH answers from the whole script.
      std::map<AbsoluteLocation, uint64_t> script;
      for (const auto& group : groups) {
        for (const auto& subgroup : group) {
          for (auto objectID : subgroup.objects) {
            script[{subgroup.group, objectID}] = subgroup.subgroup;
          }
        }
      }
      origin_->history = &script;
      std::vector<ScriptedSubgroup*> active;
      size_t nextGroup = 0;
      uint64_t newestGroup = 41;
      while (nextGroup < groups.size() || !active.empty()) {
        if (nextGroup < groups.size() &&
            (active.empty() || (rng() % 3 == 0 && active.size() < 4))) {
          for (auto& subgroup : groups[nextGroup]) {
            active.push_back(&subgroup);
          }
          nextGroup++;
        }
        auto* subgroup = active[rng() % active.size()];
        if (!subgroup->consumer) {
          TrackConsumer::BeginSubgroupOptions options;
          options.containsLastInGroup = subgroup->containsLastInGroup;
          subgroup->consumer = origin_->track
                                   ->beginSubgroup(
                                       subgroup->group,
                                       subgroup->subgroup,
                                       kDefaultPriority,
                                       options)
                                   .value();
        }
        const auto objectID = subgroup->objects[subgroup->next++];
        const bool lastInSubgroup = subgroup->next == subgroup->objects.size();
        subgroup->consumer->object(
            objectID, payloadOf(10), noExtensions(), lastInSubgroup);
        published.insert({subgroup->group, objectID});
        newestGroup = std::max(newestGroup, subgroup->group);
        if (lastInSubgroup) {
          std::erase(active, subgroup);
        }
        co_await folly::coro::co_reschedule_on_current_executor;
        if (rng() % 6 == 0) {
          // Let the proxy see everything so far before the join is sent.
          co_await settle();
          auto* link = co_await connect(proxy_);
          const uint64_t joiningStart =
              newestGroup - std::min<uint64_t>(newestGroup - 41, rng() % 3);
          auto live = std::make_shared<LiveViewerConsumer>();
          auto history = std::make_shared<RecordingFetchConsumer>();
          auto join = startJoin(*link, live, history, joiningStart);
          viewers.push_back(
              Viewer{
                  joiningStart,
                  std::move(live),
                  std::move(history),
                  std::move(join)});
        }
      }
      co_await settle();

      for (auto& viewer : viewers) {
        auto res = co_await std::move(viewer.join);
        CO_ASSERT_TRUE(res.subscribeResult.hasValue());
        CO_ASSERT_TRUE(res.fetchResult.hasValue());
        const auto joiningLocation =
            res.subscribeResult.value()->subscribeOk().largest;
        CO_ASSERT_TRUE(joiningLocation.has_value());
        SCOPED_TRACE(
            fmt::format(
                "joiningStart={} joiningLocation={{{},{}}}",
                viewer.joiningStart,
                joiningLocation->group,
                joiningLocation->object));
        CO_ASSERT_TRUE(co_await waitUntil([&] {
          return !viewer.history->log.empty() &&
              viewer.history->log.back() == "endOfFetch";
        }));
        std::vector<AbsoluteLocation> expectedHistory;
        std::vector<AbsoluteLocation> expectedLive;
        for (const auto& location : published) {
          if (location > *joiningLocation) {
            expectedLive.push_back(location);
          } else if (location.group >= viewer.joiningStart) {
            expectedHistory.push_back(location);
          }
        }
        EXPECT_EQ(historyLocations(viewer.history->log), expectedHistory);
        auto live = viewer.live->received;
        std::sort(live.begin(), live.end());
        EXPECT_EQ(live, expectedLive);
      }
      const auto& stats = proxy_->joinStats();
      localJoins += stats.immediateHits + stats.hitsAfterWait;
      upstreamJoins += stats.fallbacksMiss + stats.fallbacksWaitExpired +
          stats.fallbacksPlanRejected;
      EXPECT_EQ(
          stats.immediateHits + stats.hitsAfterWait + stats.fallbacksMiss +
              stats.fallbacksWaitExpired + stats.fallbacksPlanRejected,
          viewers.size());
      origin_->history = nullptr;
    }));
    TearDown();
    proxy_.reset();
    links_.clear();
    handles_.clear();
    origin_ = std::make_shared<OriginPublisher>();
  }
  EXPECT_GT(localJoins, 0);
  EXPECT_GT(upstreamJoins, 0);
}

// Groups held by an accepted replay are evicted while the viewer is not
// reading. The replay still delivers the whole history, and its bytes return
// to the budget once it finishes.
TEST_P(ProxyJoinIntegrationTest, ReplayOutlivesEvictionUnderBudgetPressure) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy(
        MoQProxyOptions{.maxGroupsPerTrack = 3, .maxCacheBytes = 16 * 1024});
    auto warmLive = co_await warmViewer();
    for (uint64_t group = 41; group <= 43; group++) {
      publishGroup(group, 3, 1000);
    }
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return warmLive->received.size() == 9; }));
    auto* viewer = co_await connect(proxy_);
    viewer->serverWt->setNewStreamsWritable(false);
    auto history = std::make_shared<RecordingFetchConsumer>();

    auto res = co_await startJoin(
        *viewer, std::make_shared<LiveViewerConsumer>(), history, 41);
    CO_ASSERT_TRUE(res.fetchResult.hasValue());
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return history->log.size() == 1; }));
    for (uint64_t group = 44; group <= 46; group++) {
      publishGroup(group, 3, 1000);
    }
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return warmLive->received.size() == 18; }));
    auto cache = proxy_->findTrack(kTrack)->groupCache();
    EXPECT_GE(cache->retiredThrough().value_or(0), 43);
    const auto usedWhileReplaying = proxy_->cacheBudget()->used();
    makeWritable(*viewer);

    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return history->log.size() == 10; }));
    std::vector<AbsoluteLocation> expected;
    for (uint64_t group = 41; group <= 43; group++) {
      for (uint64_t objectID = 0; objectID < 3; objectID++) {
        expected.push_back({group, objectID});
      }
    }
    EXPECT_EQ(historyLocations(history->log), expected);
    EXPECT_EQ(history->log.back(), "endOfFetch");
    co_await settle();
    EXPECT_LT(proxy_->cacheBudget()->used(), usedWhileReplaying);
    EXPECT_TRUE(origin_->fetches.empty());
  }));
}

// A viewer that cancels its join while its replay is held stops receiving
// history; another viewer's replay of the same range completes, and both
// plans' bytes return to the budget.
TEST_P(ProxyJoinIntegrationTest, CancelledJoinStopsOnlyItsOwnReplay) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    co_await warmViewer();
    publishGroup(41, 3);
    publishGroup(42, 3);
    co_await settle();
    const auto usedBeforeJoins = proxy_->cacheBudget()->used();
    auto* cancelling = co_await connect(proxy_);
    auto* staying = co_await connect(proxy_);
    cancelling->serverWt->setNewStreamsWritable(false);
    staying->serverWt->setNewStreamsWritable(false);
    auto cancelledHistory = std::make_shared<RecordingFetchConsumer>();
    auto stayingHistory = std::make_shared<RecordingFetchConsumer>();

    auto cancelledRes = co_await startJoin(
        *cancelling,
        std::make_shared<LiveViewerConsumer>(),
        cancelledHistory,
        41);
    auto stayingRes = co_await startJoin(
        *staying, std::make_shared<LiveViewerConsumer>(), stayingHistory, 41);
    CO_ASSERT_TRUE(cancelledRes.fetchResult.hasValue());
    CO_ASSERT_TRUE(stayingRes.fetchResult.hasValue());
    CO_ASSERT_TRUE(co_await waitUntil([&] {
      return cancelledHistory->log.size() == 1 &&
          stayingHistory->log.size() == 1;
    }));
    cancelledRes.fetchResult.value()->fetchCancel();
    co_await settle();
    makeWritable(*cancelling);
    makeWritable(*staying);

    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return stayingHistory->log.size() == 7; }));
    EXPECT_EQ(stayingHistory->log.back(), "endOfFetch");
    co_await settle();
    EXPECT_LT(cancelledHistory->log.size(), 7);
    EXPECT_TRUE(
        cancelledHistory->log.empty() ||
        cancelledHistory->log.back() != "endOfFetch");
    EXPECT_EQ(proxy_->cacheBudget()->used(), usedBeforeJoins);
    EXPECT_TRUE(origin_->fetches.empty());
  }));
}

INSTANTIATE_TEST_SUITE_P(
    AllVersions,
    ProxyJoinIntegrationTest,
    testing::ValuesIn(
        std::vector<uint64_t>(
            kSupportedVersions.begin(),
            kSupportedVersions.end())));

} // namespace moxygen::test
