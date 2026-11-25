/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

// viewer <-> MoQLaneRouter(MoQProxy) <-> origin, over real sessions.

#include "moxygen/lane_manager/MoQLaneRouter.h"

#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQVersions.h"
#include "moxygen/ObjectReceiver.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/lane_manager/ManagedSubscription.h"
#include "moxygen/proxy/MoQProxy.h"
#include "moxygen/samples/lane_manager/ScriptedPolicy.h"
#include "moxygen/test/Mocks.h"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/GtestHelpers.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Timeout.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>
#include <proxygen/lib/http/webtransport/test/FakeSharedWebTransport.h>

namespace moxygen::lane_manager { namespace {

using proxygen::test::FakeSharedWebTransport;
using testing::NiceMock;

constexpr uint64_t kVersion = kVersionDraftCurrent;
constexpr uint64_t kMaxRequestID = 100;
constexpr std::chrono::milliseconds kMaxWait{10000};
const TrackNamespace kBroadcast{std::vector<std::string>{"live"}};

const std::string kCatalog = R"({"tracks":[
{"name":"video0","role":"video","altGroup":0,"width":320,"height":176,"initRef":"v0"},
{"name":"video1","role":"video","altGroup":0,"width":1920,"height":1080,"initRef":"v1"}],
"initDataList":[{"id":"v0","data":"djA="},{"id":"v1","data":"djE="}]})";

std::string initOf(const Extensions& extensions) {
  for (const auto& extension : extensions.getMutableExtensions()) {
    if (extension.type == kInitPropertyType) {
      return extension.arrayValue->to<std::string>();
    }
  }
  return "";
}

// Serves the catalog as object {0,0} and each lane live from group 41.
class Origin : public Publisher {
 public:
  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) override {
    const TrackAlias alias{request.requestID.value};
    consumer->setTrackAlias(alias);
    const auto& name = request.fullTrackName.trackName;
    tracks[name] = std::move(consumer);
    SubscribeOk ok;
    ok.requestID = request.requestID;
    ok.trackAlias = alias;
    ok.largest = AbsoluteLocation{name == "catalog" ? 0u : 40u, 0};
    co_return std::make_shared<NiceMock<MockSubscriptionHandle>>(std::move(ok));
  }

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override {
    if (fetch.fullTrackName.trackName != "catalog") {
      co_return folly::makeUnexpected(
          FetchError{fetch.requestID, FetchErrorCode::NOT_SUPPORTED, ""});
    }
    consumer->object(
        0, 0, 0, folly::IOBuf::copyBuffer(kCatalog), noExtensions(), true);
    FetchOk ok;
    ok.requestID = fetch.requestID;
    ok.endLocation = AbsoluteLocation{0, 1};
    co_return std::make_shared<NiceMock<MockFetchHandle>>(std::move(ok));
  }

  folly::F14FastMap<std::string, std::shared_ptr<TrackConsumer>> tracks;
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

class PausedRetryPublisher
    : public Publisher,
      public std::enable_shared_from_this<PausedRetryPublisher> {
 public:
  PausedRetryPublisher(std::shared_ptr<Publisher> proxy, MoQExecutor& executor)
      : proxy_(std::move(proxy)), executor_(executor) {}

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) override {
    if (request.fullTrackName.trackName == "video1" &&
        ++video1Subscriptions_ == 1) {
      initialVideo1Started.setValue();
      co_await releaseInitialVideo1.getFuture();
    }
    const bool retry = request.fullTrackName.trackName == "video0" &&
        ++video0Subscriptions_ == 2;
    if (retry) {
      retryConsumer = std::dynamic_pointer_cast<ManagedSubscription>(consumer);
      retryStarted.setValue();
      co_await releaseRetry.getFuture();
    }
    auto result =
        co_await proxy_->subscribe(std::move(request), std::move(consumer));
    if (retry && result.hasValue()) {
      retrySucceeded = true;
      executor_.add([self = shared_from_this()] {
        {
          auto consumer = self->retryConsumer.lock();
          if (consumer) {
            self->stateAtUnsubscribe = consumer->state();
          }
        }
        if (auto handle = self->virtualSubscription.lock()) {
          handle->unsubscribe();
          self->virtualUnsubscribed = true;
        }
        self->retryConsumer.reset();
      });
    }
    co_return result;
  }

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override {
    if (fetch.fullTrackName.trackName != "catalog") {
      ++laneFetches;
    }
    co_return co_await proxy_->fetch(std::move(fetch), std::move(consumer));
  }

  folly::coro::SharedPromise<void> initialVideo1Started;
  folly::coro::SharedPromise<void> releaseInitialVideo1;
  folly::coro::SharedPromise<void> retryStarted;
  folly::coro::SharedPromise<void> releaseRetry;
  std::weak_ptr<ManagedSubscription> retryConsumer;
  std::weak_ptr<SubscriptionHandle> virtualSubscription;
  std::optional<ManagedSubscription::State> stateAtUnsubscribe;
  bool retrySucceeded{false};
  bool virtualUnsubscribed{false};
  size_t laneFetches{0};

 private:
  std::shared_ptr<Publisher> proxy_;
  MoQExecutor& executor_;
  size_t video0Subscriptions_{0};
  size_t video1Subscriptions_{0};
};

class ObservedPublisher : public Publisher {
 public:
  explicit ObservedPublisher(std::shared_ptr<Publisher> publisher)
      : publisher_(std::move(publisher)) {}

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) override {
    auto result =
        co_await publisher_->subscribe(std::move(request), std::move(consumer));
    if (result.hasValue()) {
      subscription = result.value();
    }
    co_return result;
  }

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override {
    fetchArrived.setValue();
    co_return co_await publisher_->fetch(std::move(fetch), std::move(consumer));
  }

  std::weak_ptr<SubscriptionHandle> subscription;
  folly::coro::SharedPromise<void> fetchArrived;

 private:
  std::shared_ptr<Publisher> publisher_;
};

struct Received {
  uint64_t group;
  uint64_t object;
  std::string init;
  bool operator==(const Received&) const = default;
};

class Recorder : public ObjectReceiverCallback {
 public:
  FlowControlState onObject(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& header,
      Payload /*payload*/) override {
    received.push_back({header.group, header.id, initOf(header.extensions)});
    return FlowControlState::UNBLOCKED;
  }
  void onObjectStatus(
      std::optional<TrackAlias> /*trackAlias*/,
      const ObjectHeader& /*header*/) override {}
  void onEndOfStream() override {}
  void onError(ResetStreamErrorCode /*error*/) override {}
  void onPublishDone(PublishDone /*done*/) override {}

  std::vector<Received> received;
};

SubscribeRequest liveSubscribe(const std::string& name) {
  auto request = SubscribeRequest::make(FullTrackName{kBroadcast, name});
  request.filter = LocationFilter::nextObject();
  return request;
}

class LaneManagerIntegrationTest : public testing::Test,
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
    if (!useBidiRequestStreams(kVersion)) {
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
    do {
      eventBase_.loopOnce(EVLOOP_NONBLOCK);
    } while (eventBase_.getNumLoopCallbacks() > 0 ||
             eventBase_.getNotificationQueueSize() > 0);
  }

  void run(folly::coro::Task<void> body) {
    folly::coro::blockingWait(std::move(body), &eventBase_);
  }

  folly::coro::Task<Link*> connect(std::shared_ptr<Publisher> publisher) {
    auto link = std::make_unique<Link>();
    std::tie(link->clientWt, link->serverWt) =
        FakeSharedWebTransport::makeSharedWebTransport();
    link->client = std::make_shared<MoQRelaySession>(
        folly::MaybeManagedPtr<proxygen::WebTransport>(link->clientWt.get()),
        kVersion,
        executor_);
    link->serverWt->setPeerHandler(link->client.get());
    link->server = std::make_shared<MoQRelaySession>(
        folly::MaybeManagedPtr<proxygen::WebTransport>(link->serverWt.get()),
        kVersion,
        *this,
        executor_);
    link->clientWt->setPeerHandler(link->server.get());
    link->server->setPublishHandler(std::move(publisher));
    link->client->start();
    link->server->start();
    if (useUniControlStreams(kVersion)) {
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

  folly::coro::Task<void> startProxy() {
    auto* upstream = co_await connect(origin_);
    proxy_ = MoQProxy::create(
        {std::make_shared<FixedUpstreamProvider>(upstream->client)});
  }

  // A viewer session whose publish handler is a router over the proxy.
  folly::coro::Task<Link*> connectViewer(uint64_t groupsPerLane = 10) {
    co_return co_await connect(
        std::make_shared<MoQLaneRouter>(
            proxy_,
            [groupsPerLane](Role) {
              return std::make_shared<samples::ScriptedPolicy>(groupsPerLane);
            },
            kMaxWait));
  }

  template <typename Predicate>
  folly::coro::Task<bool> waitUntil(Predicate predicate) {
    for (int i = 0; i < 2000 && !predicate(); i++) {
      co_await folly::coro::co_reschedule_on_current_executor;
    }
    co_return predicate();
  }

  void publishGroup(const std::string& lane, uint64_t group, uint64_t count) {
    TrackConsumer::BeginSubgroupOptions options;
    options.containsLastInGroup = true;
    auto subgroup = origin_->tracks.at(lane)->beginSubgroup(
        group, 0, kDefaultPriority, options);
    ASSERT_TRUE(subgroup.hasValue());
    for (uint64_t object = 0; object < count; object++) {
      (*subgroup)->object(
          object,
          folly::IOBuf::copyBuffer("x"),
          noExtensions(),
          object + 1 == count);
    }
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQExecutor> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_)};
  std::shared_ptr<Origin> origin_{std::make_shared<Origin>()};
  std::shared_ptr<MoQProxy> proxy_;
  std::vector<std::unique_ptr<Link>> links_;
  std::vector<std::shared_ptr<SubscriptionHandle>> handles_;
};

TEST_F(LaneManagerIntegrationTest, VirtualSubscribeServesTheLaneWithItsInit) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    auto* viewer = co_await connectViewer();
    auto recorder = std::make_shared<Recorder>();
    auto result = co_await viewer->client->subscribe(
        liveSubscribe("r--abr-video"),
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, recorder));
    CO_ASSERT_TRUE(result.hasValue());
    handles_.push_back(std::move(result.value()));

    publishGroup("video0", 41, 2);
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return recorder->received.size() == 2; }));
    EXPECT_EQ(
        recorder->received,
        (std::vector<Received>{{41, 0, "v0"}, {41, 1, ""}}));
  }));
}

// Viewer A warms video0 at the proxy under its own alias; viewer B then joins
// the virtual track: history from the proxy's cache, then live.
TEST_F(LaneManagerIntegrationTest, JoinsTheVirtualTrackOnAWarmLane) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    auto* a = co_await connectViewer();
    auto warm = std::make_shared<Recorder>();
    auto warmResult = co_await a->client->subscribe(
        liveSubscribe("video0"),
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, warm));
    CO_ASSERT_TRUE(warmResult.hasValue());
    handles_.push_back(std::move(warmResult.value()));
    publishGroup("video0", 41, 2);
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return warm->received.size() == 2; }));

    auto* b = co_await connectViewer();
    auto live = std::make_shared<Recorder>();
    auto history = std::make_shared<Recorder>();
    auto join = co_await b->client->join(
        liveSubscribe("r--abr-video"),
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, live),
        /*joiningStart=*/0,
        kDefaultPriority,
        GroupOrder::OldestFirst,
        {},
        std::make_shared<ObjectReceiver>(ObjectReceiver::FETCH, history),
        FetchType::RELATIVE_JOINING);
    CO_ASSERT_TRUE(join.subscribeResult.hasValue());
    CO_ASSERT_TRUE(join.fetchResult.hasValue());
    handles_.push_back(std::move(join.subscribeResult.value()));

    publishGroup("video0", 42, 1);
    CO_ASSERT_TRUE(co_await waitUntil([&] {
      return history->received.size() == 2 && live->received.size() == 1;
    }));
    EXPECT_EQ(
        history->received, (std::vector<Received>{{41, 0, "v0"}, {41, 1, ""}}));
    EXPECT_EQ(live->received, (std::vector<Received>{{42, 0, "v0"}}));
  }));
}

// The policy moves the viewer to video1 after two groups, so video1's group 43
// replaces video0's.
TEST_F(LaneManagerIntegrationTest, SwitchesLanesAtAGroupBoundary) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    auto* viewer = co_await connectViewer(/*groupsPerLane=*/2);
    auto recorder = std::make_shared<Recorder>();
    auto result = co_await viewer->client->subscribe(
        liveSubscribe("r--abr-video"),
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, recorder));
    CO_ASSERT_TRUE(result.hasValue());
    handles_.push_back(std::move(result.value()));
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return origin_->tracks.contains("video1"); }));

    publishGroup("video0", 41, 1);
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return recorder->received.size() == 1; }));
    for (uint64_t group : {42, 43}) {
      publishGroup("video0", group, 1);
      publishGroup("video1", group, 1);
    }
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return recorder->received.size() == 3; }));
    EXPECT_EQ(
        recorder->received,
        (std::vector<Received>{{41, 0, "v0"}, {42, 0, ""}, {43, 0, "v1"}}));
  }));
}

TEST_F(
    LaneManagerIntegrationTest,
    PendingJoiningFetchSurvivesVirtualUnsubscribe) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    auto retry = std::make_shared<PausedRetryPublisher>(proxy_, *executor_);
    auto observed =
        std::make_shared<ObservedPublisher>(std::make_shared<MoQLaneRouter>(
            retry,
            [](Role) { return std::make_shared<samples::ScriptedPolicy>(10); },
            kMaxWait));
    auto* viewer = co_await connect(observed);
    auto live = std::make_shared<Recorder>();
    auto subscription = co_await viewer->client->subscribe(
        liveSubscribe("r--abr-video"),
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, live));
    CO_ASSERT_TRUE(subscription.hasValue());
    const auto virtualRequestID = subscription.value()->subscribeOk().requestID;
    handles_.push_back(std::move(subscription.value()));
    retry->virtualSubscription = observed->subscription;
    CO_ASSERT_FALSE(retry->virtualSubscription.expired());
    co_await folly::coro::timeout(
        retry->initialVideo1Started.getFuture(), kMaxWait);
    retry->releaseInitialVideo1.setValue();
    publishGroup("video0", 41, 1);
    CO_ASSERT_TRUE(
        co_await waitUntil([&] { return live->received.size() == 1; }));
    EXPECT_EQ(live->received, (std::vector<Received>{{41, 0, "v0"}}));

    CO_ASSERT_TRUE(origin_->tracks.at("video0")
                       ->publishDone(
                           PublishDone{
                               RequestID(0),
                               PublishDoneStatusCode::INTERNAL_ERROR,
                               0,
                               "retry"})
                       .hasValue());
    co_await folly::coro::timeout(retry->retryStarted.getFuture(), kMaxWait);

    // A ready proxy track makes alias registration and SUBSCRIBE_OK one turn.
    auto* warmer = co_await connect(proxy_);
    auto warm = std::make_shared<Recorder>();
    auto warmResult = co_await warmer->client->subscribe(
        liveSubscribe("video0"),
        std::make_shared<ObjectReceiver>(ObjectReceiver::SUBSCRIBE, warm));
    CO_ASSERT_TRUE(warmResult.hasValue());
    handles_.push_back(std::move(warmResult.value()));

    Fetch fetch(RequestID(0), virtualRequestID, 0, FetchType::RELATIVE_JOINING);
    fetch.fullTrackName = FullTrackName{kBroadcast, "r--abr-video"};
    auto history = std::make_shared<Recorder>();
    auto pendingFetch = folly::coro::co_withExecutor(
                            viewer->client->getExecutor(),
                            viewer->client->fetch(
                                std::move(fetch),
                                std::make_shared<ObjectReceiver>(
                                    ObjectReceiver::FETCH, history)))
                            .start();
    co_await folly::coro::timeout(observed->fetchArrived.getFuture(), kMaxWait);
    EXPECT_EQ(retry->laneFetches, 0);
    {
      auto consumer = retry->retryConsumer.lock();
      CO_ASSERT_TRUE(consumer);
      EXPECT_EQ(consumer->state(), ManagedSubscription::State::Pending);
    }
    retry->releaseRetry.setValue();

    auto result =
        co_await folly::coro::timeout(std::move(pendingFetch), kMaxWait);
    CO_ASSERT_TRUE(result.hasError());
    EXPECT_EQ(result.error().errorCode, FetchErrorCode::CANCELLED);
    EXPECT_TRUE(retry->retrySucceeded);
    EXPECT_TRUE(retry->virtualUnsubscribed);
    EXPECT_EQ(retry->stateAtUnsubscribe, ManagedSubscription::State::Active);
    EXPECT_EQ(retry->laneFetches, 0);
  }));
}

}} // namespace moxygen::lane_manager
