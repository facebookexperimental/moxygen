/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Sleep.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <memory>
#include <string>
#include <vector>

#include "moxygen/MoQVersions.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQProxy.h"
#include "moxygen/test/MoQSwitchChecker.h"
#include "moxygen/test/MoQTestLinks.h"

namespace moxygen::test { namespace {

using namespace testing;
using namespace std::chrono_literals;
using Clock = SwitchChecker::Clock;

const FullTrackName kTrack{TrackNamespace{{"live"}}, "video"};

folly::dynamic range(const char* from, const char* to) {
  return folly::dynamic::object("from", from)("to", to);
}

folly::dynamic hubRun(uint64_t first, uint64_t last, uint64_t hub) {
  return folly::dynamic::object("first", first)("last", last)("hub", hub);
}

std::vector<std::string> strings(const folly::dynamic& array) {
  std::vector<std::string> values;
  for (const auto& value : array) {
    values.push_back(value.asString());
  }
  return values;
}

// Answers SUBSCRIBE through a hub, and any FETCH, joining ones too, with
// FETCH_OK and the given objects of the hub's track. FETCH_OK ends where the
// last SUBSCRIBE started, unless okEnd says otherwise. The test ends the
// FETCH, or doesn't.
class HistoryPublisher : public Publisher {
 public:
  HistoryPublisher(
      std::shared_ptr<SwitchTestHub> hub,
      std::vector<AbsoluteLocation> objects)
      : hub_(std::move(hub)), objects_(std::move(objects)) {}

  // FETCH_OK waits for it.
  folly::coro::Baton replyGate;
  bool gated{false};
  std::optional<AbsoluteLocation> okEnd;

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) override {
    auto result =
        co_await hub_->subscribe(std::move(request), std::move(consumer));
    if (result.hasValue()) {
      if (const auto& largest = result.value()->subscribeOk().largest) {
        subscribedFrom_ = AbsoluteLocation{largest->group, largest->object + 1};
      }
    }
    co_return result;
  }

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override {
    for (auto location : objects_) {
      Extensions extensions;
      extensions.insertMutableExtension(
          Extension(kHubIdExtensionType, hub_->id()));
      consumer->object(
          location.group,
          /*subgroupID=*/0,
          location.object,
          folly::IOBuf::copyBuffer(
              SwitchOrigin::payload(fetch.fullTrackName, location)),
          std::move(extensions),
          /*finFetch=*/false,
          /*forwardingPreferenceIsDatagram=*/false);
    }
    consumer_ = std::move(consumer);
    if (gated) {
      co_await replyGate;
    }
    co_return std::make_shared<Handle>(FetchOk{
        fetch.requestID,
        GroupOrder::OldestFirst,
        /*endOfTrack=*/0,
        okEnd.value_or(subscribedFrom_)});
  }

  void end() {
    consumer_->endOfFetch();
  }
  void reset() {
    consumer_->reset(ResetStreamErrorCode::CANCELLED);
  }

 private:
  class Handle : public Publisher::FetchHandle {
   public:
    explicit Handle(FetchOk ok) : Publisher::FetchHandle(std::move(ok)) {}

    void fetchCancel() override {}

    folly::coro::Task<RequestUpdateResult> requestUpdate(
        RequestUpdate update) override {
      co_return folly::makeUnexpected(
          RequestError{
              update.requestID, RequestErrorCode::NOT_SUPPORTED, "no updates"});
    }
  };

  std::shared_ptr<SwitchTestHub> hub_;
  std::vector<AbsoluteLocation> objects_;
  std::shared_ptr<FetchConsumer> consumer_;
  AbsoluteLocation subscribedFrom_;
};

// Holds each SUBSCRIBE until the test opens the gate, then answers it through
// a hub.
class GatedPublisher : public Publisher {
 public:
  explicit GatedPublisher(std::shared_ptr<SwitchTestHub> hub)
      : hub_(std::move(hub)) {}

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) override {
    co_await gate;
    co_return co_await hub_->subscribe(std::move(request), std::move(consumer));
  }

  folly::coro::Baton gate;

 private:
  std::shared_ptr<SwitchTestHub> hub_;
};

// Hubs publishing four objects a group on draft 18, watched straight or
// through a proxy in front of them. The checker never sees what the hubs
// publish, as across processes, and tells time by now_.
class SwitchCheckerTest : public Test {
 protected:
  void TearDown() override {
    if (proxy_) {
      proxy_->close();
    }
    links_.closeClients();
    links22_.closeClients();
    run(checker().join());
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

  SwitchChecker& checker() {
    if (!checker_) {
      options_.clock = [this] { return now_; };
      checker_ = std::make_unique<SwitchChecker>(shape_, options_);
    }
    return *checker_;
  }

  folly::coro::Task<void> settle() {
    for (int i = 0; i < 200; i++) {
      co_await folly::coro::co_reschedule_on_current_executor;
    }
  }

  // Lets real time pass until done() holds, or for at most two seconds.
  folly::coro::Task<void> waitUntil(folly::Function<bool()> done) {
    const auto giveUp = Clock::now() + 2s;
    while (!done() && Clock::now() < giveUp) {
      co_await folly::coro::sleep(5ms);
    }
    co_await settle();
  }

  folly::coro::Task<std::shared_ptr<MoQSession>> connectTo(
      std::shared_ptr<Publisher> publisher,
      SwitchTestHub& hub) {
    auto* link = co_await links_.connect(std::move(publisher));
    hub.attach(link->server);
    co_return link->client;
  }

  folly::coro::Task<std::shared_ptr<MoQSession>> connectToHub(size_t hub) {
    co_return co_await connectTo(hubs_[hub], *hubs_[hub]);
  }

  // The proxy in front of every hub, one pool choosing among them in order.
  folly::coro::Task<void> startProxy() {
    for (const auto& hub : hubs_) {
      auto* link = co_await links_.connect(hub);
      hub->attach(link->server);
      provider_->addUpstream(link->client);
    }
    MoQProxyOptions options;
    options.upstreamSwitch = {
        .deadline = 300ms,
        .maxStartDelay = 50ms,
        .reorderGrace = 50ms,
        .retryBackoff = 10ms,
        .maxRetryBackoff = 50ms,
        .randomSeed = 1};
    proxy_ = MoQProxy::create({provider_}, options);
  }

  folly::coro::Task<std::shared_ptr<MoQSession>> connectToProxy() {
    auto* link = co_await links_.connect(proxy_);
    co_return link->client;
  }

  // A joiner straight to history, after hub 0 published groups 0-2: it
  // starts at 2/4 and asks for its history from 1/0.
  folly::coro::Task<void> joinThrough(
      std::shared_ptr<HistoryPublisher> history) {
    for (uint64_t group = 0; group < 3; group++) {
      hubs_[0]->publishGroup(kTrack, group);
    }
    co_await checker().addJoiners(co_await connectTo(history, *hubs_[0]));
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 3);
    co_await settle();
  }

  folly::dynamic report() {
    return checker().report(now_);
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_)};
  MoQTestLinks links_{kVersionDraft18, executor_};
  MoQTestLinks links22_{kVersionDraft22, executor_};
  std::shared_ptr<SwitchOrigin> hubOrigin_{std::make_shared<SwitchOrigin>()};
  std::vector<std::shared_ptr<SwitchTestHub>> hubs_{
      std::make_shared<SwitchTestHub>(0, hubOrigin_),
      std::make_shared<SwitchTestHub>(1, hubOrigin_)};
  std::shared_ptr<ScriptedUpstreamProvider> provider_{
      std::make_shared<ScriptedUpstreamProvider>()};
  std::shared_ptr<MoQProxy> proxy_;
  Clock::time_point now_{Clock::now()};
  SwitchTrackShape shape_;
  SwitchCheckerOptions options_{.tracks = {kTrack}};
  std::unique_ptr<SwitchChecker> checker_;
};

TEST_F(SwitchCheckerTest, AViewerGivenEveryObjectIsOk) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[0]->publishGroup(kTrack, 1);
    co_await settle();

    auto checked = report();
    EXPECT_TRUE(checked["ok"].asBool());
    EXPECT_EQ(checked["viewers"].size(), 1);
    const auto& viewer = checked["viewers"][0];
    EXPECT_EQ(viewer["track"], "live/video");
    EXPECT_FALSE(viewer["joiner"].asBool());
    EXPECT_EQ(viewer["start"], "0/0");
    EXPECT_EQ(viewer["received"], 8);
    EXPECT_EQ(viewer["first"], "0/0");
    EXPECT_EQ(viewer["last"], "1/3");
    EXPECT_EQ(viewer["missing"], folly::dynamic::array());
    EXPECT_EQ(viewer["behind"], 0);
    EXPECT_EQ(viewer["violations"], folly::dynamic::array());
    EXPECT_EQ(viewer["groups_by_hub"], folly::dynamic::array(hubRun(0, 1, 0)));
    EXPECT_EQ(viewer.count("publish_done"), 0);
  }));
}

TEST_F(SwitchCheckerTest, AGroupThatNeverArrivesIsMissingButNotAViolation) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[0]->publishGroup(kTrack, 2);
    co_await settle();

    auto checked = report();
    EXPECT_TRUE(checked["ok"].asBool());
    EXPECT_EQ(
        checked["viewers"][0]["missing"],
        folly::dynamic::array(range("1/0", "1/3")));
  }));
}

// What the origins published is what any viewer got: hub 1's viewer stopped
// a group short of hub 0's, so it is behind on group 2, not missing it.
TEST_F(SwitchCheckerTest, AViewerIsBehindOnWhatOthersGotPastItsLastObject) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    co_await checker().addViewers(co_await connectToHub(1));
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[0]->publishGroup(kTrack, 2);
    hubs_[1]->publishGroup(kTrack, 0);
    co_await settle();

    auto checked = report();
    EXPECT_EQ(
        checked["viewers"][0]["missing"],
        folly::dynamic::array(range("1/0", "1/3")));
    const auto& behind = checked["viewers"][1];
    EXPECT_EQ(behind["last"], "0/3");
    EXPECT_EQ(behind["missing"], folly::dynamic::array());
    EXPECT_EQ(behind["behind"], 8);
  }));
}

TEST_F(SwitchCheckerTest, AViewerThatReceivesNothingIsNotOk) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    co_await settle();

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_THAT(
        strings(checked["viewers"][0]["violations"]),
        ElementsAre("received nothing"));
  }));
}

TEST_F(SwitchCheckerTest, NoViewersIsNotOk) {
  EXPECT_FALSE(report()["ok"].asBool());
}

TEST_F(SwitchCheckerTest, AnUnexpectedPublishDoneIsAViolation) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[0]->endSubscriptions(kTrack, PublishDoneStatusCode::GOING_AWAY);
    co_await settle();

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    const auto& viewer = checked["viewers"][0];
    EXPECT_EQ(viewer["violations"].size(), 1);
    EXPECT_EQ(
        viewer["publish_done"]["status"],
        folly::to_underlying(PublishDoneStatusCode::GOING_AWAY));
  }));
}

// Once the track ended, the time since its last object is no stall.
TEST_F(SwitchCheckerTest, TheExpectedPublishDoneIsOk) {
  options_.expectedEnd = PublishDoneStatusCode::GOING_AWAY;
  options_.maxStall = 1s;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[0]->endSubscriptions(kTrack, PublishDoneStatusCode::GOING_AWAY);
    co_await settle();
    now_ += 5s;

    EXPECT_TRUE(report()["ok"].asBool());
  }));
}

// Mid-group, the track's open subgroup stream is reset before PUBLISH_DONE.
TEST_F(SwitchCheckerTest, AResetMidGroupIsAViolation) {
  options_.expectedEnd = PublishDoneStatusCode::GOING_AWAY;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publish(kTrack, 0, 2);
    co_await settle();
    hubs_[0]->endSubscriptions(kTrack, PublishDoneStatusCode::GOING_AWAY);
    co_await settle();

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_EQ(
        checked["viewers"][0]["resets"],
        folly::dynamic::array(
            folly::dynamic::object("group", 0)("subgroup", 0)(
                "error",
                folly::to_underlying(ResetStreamErrorCode::CANCELLED))));
  }));
}

TEST_F(SwitchCheckerTest, TheExpectedResetIsOk) {
  options_.expectedEnd = PublishDoneStatusCode::GOING_AWAY;
  options_.expectedReset = ResetStreamErrorCode::CANCELLED;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publish(kTrack, 0, 2);
    co_await settle();
    hubs_[0]->endSubscriptions(kTrack, PublishDoneStatusCode::GOING_AWAY);
    co_await settle();

    EXPECT_TRUE(report()["ok"].asBool());
  }));
}

TEST_F(SwitchCheckerTest, AViewerThatCannotSubscribeIsNotOk) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    // A proxy with no upstream refuses every SUBSCRIBE.
    proxy_ = MoQProxy::create({provider_}, MoQProxyOptions{});
    co_await checker().addViewers(co_await connectToProxy());

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_TRUE(checked["viewers"][0].count("error"));
  }));
}

TEST_F(SwitchCheckerTest, AConnectErrorIsNotOk) {
  checker().addConnectError("refused");

  auto checked = report();

  EXPECT_FALSE(checked["ok"].asBool());
  EXPECT_EQ(checked["connect_errors"], folly::dynamic::array("refused"));
}

// The viewer subscribed at 0, got group 0 at 100ms and group 1 at 400ms.
TEST_F(SwitchCheckerTest, TheLongestStallIncludesTheStretchSinceTheLastObject) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    now_ += 100ms;
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();
    now_ += 300ms;
    hubs_[0]->publishGroup(kTrack, 1);
    co_await settle();

    now_ += 200ms;
    EXPECT_EQ(report()["viewers"][0]["longest_stall_ms"], 300);
    now_ += 900ms;
    EXPECT_EQ(report()["viewers"][0]["longest_stall_ms"], 1100);
  }));
}

TEST_F(SwitchCheckerTest, AStallOverTheBoundIsNotOk) {
  options_.maxStall = 500ms;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();

    now_ += 500ms;
    EXPECT_TRUE(report()["ok"].asBool());
    now_ += 100ms;
    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_THAT(
        strings(checked["viewers"][0]["violations"]),
        ElementsAre("went 600ms without an object, over the 500ms allowed"));
  }));
}

TEST_F(SwitchCheckerTest, AnObjectOutsideTheShapeIsAViolation) {
  shape_.objectsPerGroup = 2;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_THAT(
        strings(checked["viewers"][0]["violations"]),
        Contains(
            "object 0/2 is outside the track's shape of 2 objects a group"));
  }));
}

TEST_F(SwitchCheckerTest, AJoinerChecksTheGroupsBeforeItsStart) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    co_await checker().addViewers(co_await connectToProxy());
    for (uint64_t group = 0; group < 3; group++) {
      hubs_[0]->publishGroup(kTrack, group);
    }
    co_await settle();

    co_await checker().addJoiners(co_await connectToProxy());
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 3);
    co_await settle();

    EXPECT_TRUE(checker().settled());
    auto checked = report();
    EXPECT_TRUE(checked["ok"].asBool());
    EXPECT_EQ(checked["viewers"].size(), 2);
    const auto& joiner = checked["viewers"][1];
    EXPECT_TRUE(joiner["joiner"].asBool());
    EXPECT_EQ(joiner["start"], "2/4");
    const auto& history = joiner["history"];
    EXPECT_EQ(history["start"], "1/0");
    EXPECT_EQ(history["end"], "2/4");
    EXPECT_EQ(history["received"], 8);
    EXPECT_TRUE(history["ended"].asBool());
    EXPECT_EQ(history["missing"], folly::dynamic::array());
    EXPECT_EQ(history["violations"], folly::dynamic::array());
    EXPECT_EQ(history.count("unverified"), 0);
  }));
}

// A hub answers standalone FETCH only, so a joiner straight to it is refused.
TEST_F(SwitchCheckerTest, AJoinerWhoseHistoryFailsIsNotOk) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    hubs_[0]->publishGroup(kTrack, 0);
    co_await checker().addJoiners(co_await connectToHub(0));
    co_await settle();

    EXPECT_TRUE(checker().settled());
    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_TRUE(checked["viewers"][0]["history"].count("error"));
  }));
}

// The history skips group 1, which no viewer received, so nothing shows it
// missing; the report says the checker couldn't verify it.
TEST_F(SwitchCheckerTest, HistoryNoViewerSawIsUnverified) {
  auto history = std::make_shared<HistoryPublisher>(
      hubs_[0], std::vector<AbsoluteLocation>{{2, 0}, {2, 1}, {2, 2}, {2, 3}});
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await joinThrough(history);
    history->end();
    co_await settle();

    auto checked = report();
    EXPECT_TRUE(checked["ok"].asBool());
    const auto& summary = checked["viewers"][0]["history"];
    EXPECT_EQ(summary["missing"], folly::dynamic::array());
    EXPECT_EQ(
        summary["unverified"],
        folly::dynamic(folly::dynamic::object("from", "1/0")("until", "2/0")));
  }));
}

// The history's FETCH_OK ends at 2/0, short of the joiner's start at 2/4.
TEST_F(SwitchCheckerTest, AFetchOkEndingShortOfTheStartIsNotOk) {
  auto history = std::make_shared<HistoryPublisher>(
      hubs_[0], std::vector<AbsoluteLocation>{{1, 0}, {1, 1}, {1, 2}, {1, 3}});
  history->okEnd = AbsoluteLocation{2, 0};
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await joinThrough(history);
    history->end();
    co_await settle();

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    const auto& summary = checked["viewers"][0]["history"];
    EXPECT_EQ(summary["fetch_ok_end"], "2/0");
    EXPECT_THAT(
        strings(summary["violations"]),
        ElementsAre("FETCH_OK ends at 2/0, not 2/4"));
  }));
}

TEST_F(SwitchCheckerTest, AHistoryResetAfterItsFetchOkIsNotOk) {
  auto history = std::make_shared<HistoryPublisher>(
      hubs_[0], std::vector<AbsoluteLocation>{{2, 0}});
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await joinThrough(history);
    history->reset();
    co_await settle();

    EXPECT_TRUE(checker().settled());
    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_THAT(
        strings(checked["viewers"][0]["history"]["violations"]),
        ElementsAre("reset with 1"));
  }));
}

TEST_F(SwitchCheckerTest, AnUnfinishedHistoryIsNotOk) {
  auto history = std::make_shared<HistoryPublisher>(
      hubs_[0], std::vector<AbsoluteLocation>{{2, 0}});
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await joinThrough(history);

    EXPECT_FALSE(checker().settled());
    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_THAT(
        strings(checked["viewers"][0]["history"]["violations"]),
        ElementsAre("unfinished"));
  }));
}

TEST_F(SwitchCheckerTest, ASubscribeWithNoReplyYetIsNotOk) {
  auto gated = std::make_shared<GatedPublisher>(hubs_[0]);
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto adding =
        folly::coro::co_withExecutor(
            co_await folly::coro::co_current_executor,
            checker().addViewers(co_await connectTo(gated, *hubs_[0])))
            .start();
    co_await settle();

    EXPECT_FALSE(checker().settled());
    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_EQ(checked["viewers"][0]["error"], "no SUBSCRIBE reply");
    gated->gate.post();
    co_await std::move(adding);
    EXPECT_TRUE(checker().settled());
  }));
}

// The SUBSCRIBE took 300ms, and the first object came with its reply.
TEST_F(SwitchCheckerTest, TheStallCountsFromSubscribeOk) {
  auto gated = std::make_shared<GatedPublisher>(hubs_[0]);
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    auto adding =
        folly::coro::co_withExecutor(
            co_await folly::coro::co_current_executor,
            checker().addViewers(co_await connectTo(gated, *hubs_[0])))
            .start();
    co_await settle();
    now_ += 300ms;
    gated->gate.post();
    co_await std::move(adding);
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();

    auto checked = report();
    EXPECT_TRUE(checked["ok"].asBool());
    EXPECT_EQ(checked["viewers"][0]["subscribe_ms"], 300);
    EXPECT_EQ(checked["viewers"][0]["longest_stall_ms"], 0);
  }));
}

// The whole history arrives before its FETCH_OK.
TEST_F(SwitchCheckerTest, AHistoryIsSettledOnceItsFetchOkArrives) {
  auto history = std::make_shared<HistoryPublisher>(
      hubs_[0],
      std::vector<AbsoluteLocation>{
          {1, 0}, {1, 1}, {1, 2}, {1, 3}, {2, 0}, {2, 1}, {2, 2}, {2, 3}});
  history->gated = true;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await joinThrough(history);
    history->end();
    co_await settle();

    EXPECT_FALSE(checker().settled());
    history->replyGate.post();
    co_await settle();
    EXPECT_TRUE(checker().settled());
    EXPECT_TRUE(report()["ok"].asBool());
  }));
}

// Group 0's stream stays open though group 1 arrives and the track goes on,
// until later publishing finishes it.
TEST_F(SwitchCheckerTest, ASubgroupLeftOpenBehindALaterGroupIsAViolation) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publish(kTrack, 0, 2);
    hubs_[0]->publishGroup(kTrack, 1);
    co_await settle();

    EXPECT_FALSE(checker().settled());
    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_THAT(
        strings(checked["viewers"][0]["violations"]),
        ElementsAre(
            "subgroup 0/0 never finished though a later group arrived"));
    hubs_[0]->publish(kTrack, 0, 4);
    co_await settle();
    EXPECT_TRUE(checker().settled());
    EXPECT_TRUE(report()["ok"].asBool());
  }));
}

// The track ends while group 0's stream is open, and later publishing
// finishes it.
TEST_F(SwitchCheckerTest, AnEndedTrackWaitsForItsOpenStreams) {
  options_.expectedEnd = PublishDoneStatusCode::TRACK_ENDED;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publish(kTrack, 0, 2);
    co_await settle();
    hubs_[0]->finishSubscriptions(kTrack, PublishDoneStatusCode::TRACK_ENDED);
    co_await settle();

    EXPECT_FALSE(checker().settled());
    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_EQ(
        checked["viewers"][0]["unfinished_subgroups"],
        folly::dynamic::array("0/0"));
    EXPECT_THAT(
        strings(checked["viewers"][0]["violations"]),
        ElementsAre("subgroup 0/0 never finished though the track ended"));
    hubs_[0]->publish(kTrack, 0, 4);
    co_await settle();
    EXPECT_TRUE(checker().settled());
    EXPECT_TRUE(report()["ok"].asBool());
  }));
}

TEST_F(SwitchCheckerTest, TheStallBeforeALatePublishDoneCounts) {
  options_.expectedEnd = PublishDoneStatusCode::GOING_AWAY;
  options_.maxStall = 1s;
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publishGroup(kTrack, 0);
    co_await settle();
    now_ += 5s;
    hubs_[0]->endSubscriptions(kTrack, PublishDoneStatusCode::GOING_AWAY);
    co_await settle();
    now_ += 5s;

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_EQ(checked["viewers"][0]["longest_stall_ms"], 5000);
  }));
}

TEST_F(SwitchCheckerTest, AHistoryObjectOutsideTheShapeIsAViolation) {
  auto history = std::make_shared<HistoryPublisher>(
      hubs_[0], std::vector<AbsoluteLocation>{{1, 4}});
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await joinThrough(history);
    history->end();
    co_await settle();

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_THAT(
        strings(checked["viewers"][0]["history"]["violations"]),
        Contains(
            "object 1/4 is outside the track's shape of 4 objects a group"));
  }));
}

TEST_F(SwitchCheckerTest, TheWatchStartIsReported) {
  auto& watching = checker();
  now_ += 250ms;
  watching.startWatch();

  EXPECT_EQ(report()["watch_started_at_ms"], 250);
}

TEST_F(SwitchCheckerTest, ObjectsSpanningImplausiblyManyGroupsAreAnError) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await checker().addViewers(co_await connectToHub(0));
    hubs_[0]->publishGroup(kTrack, 0);
    hubs_[0]->publishGroup(kTrack, 1'000'000'000);
    co_await settle();

    auto checked = report();
    EXPECT_FALSE(checked["ok"].asBool());
    EXPECT_THAT(
        strings(checked["errors"]),
        ElementsAre(
            "live/video: objects from 0/0 to 1000000000/3 span too many "
            "groups"));
  }));
}

TEST_F(SwitchCheckerTest, AJoinerOnADraftWithoutJoiningFetchOnlyWatches) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    hubs_[0]->publishGroup(kTrack, 0);
    auto* link = co_await links22_.connect(hubs_[0]);
    hubs_[0]->attach(link->server);
    co_await checker().addJoiners(link->client);
    hubs_[0]->publishGroup(kTrack, 1);
    co_await settle();

    EXPECT_TRUE(checker().settled());
    auto checked = report();
    EXPECT_TRUE(checked["ok"].asBool());
    EXPECT_EQ(
        checked["viewers"][0]["history"],
        folly::dynamic(
            folly::dynamic::object(
                "skipped", "draft 22 has no joining FETCH")));
  }));
}

// Hub 0 goes away after group 1, and the proxy hands the track to hub 1 at
// group 2.
TEST_F(SwitchCheckerTest, AnUpstreamSwitchShowsWhichHubServedEachGroup) {
  run(folly::coro::co_invoke([&]() -> folly::coro::Task<void> {
    co_await startProxy();
    co_await checker().addViewers(co_await connectToProxy());
    for (uint64_t group = 0; group < 2; group++) {
      hubs_[0]->publishGroup(kTrack, group);
      hubs_[1]->publishGroup(kTrack, group);
    }
    co_await settle();
    provider_->setAvailable(0, false);
    hubs_[0]->sendGoaway(Goaway{.timeout = 5000});
    co_await waitUntil([&] { return hubs_[1]->subscriptions(kTrack) == 1; });
    hubs_[1]->publishGroup(kTrack, 2);
    co_await settle();
    hubs_[0]->publishGroup(kTrack, 2);
    co_await waitUntil([&] { return hubs_[0]->subscriptions(kTrack) == 0; });
    hubs_[1]->publishGroup(kTrack, 3);
    co_await settle();

    auto checked = report();
    EXPECT_TRUE(checked["ok"].asBool());
    const auto& viewer = checked["viewers"][0];
    EXPECT_EQ(viewer["missing"], folly::dynamic::array());
    EXPECT_EQ(
        viewer["groups_by_hub"],
        folly::dynamic::array(hubRun(0, 1, 0), hubRun(2, 3, 1)));
  }));
}

}} // namespace moxygen::test
