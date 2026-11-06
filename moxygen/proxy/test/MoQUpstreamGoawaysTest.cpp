/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/proxy/MoQUpstreamGoaways.h"
#include "moxygen/test/MockMoQSession.h"

namespace moxygen { namespace {

// Delivers a GOAWAY as if the session had parsed it from its peer.
void receiveGoaway(MoQSession& session, const Goaway& goaway) {
  static_cast<MoQControlCodec::ControlCallback&>(session).onGoaway(goaway);
}

class MoQUpstreamGoawaysTest : public testing::Test {
 protected:
  std::shared_ptr<MoQSession> makeSession() {
    return std::make_shared<testing::NiceMock<test::MockMoQSession>>(executor_);
  }

  folly::EventBase eventBase_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_{
      std::make_shared<MoQFollyExecutorImpl>(&eventBase_)};
  std::shared_ptr<MoQUpstreamGoaways> goaways_{
      std::make_shared<MoQUpstreamGoaways>()};
};

TEST_F(MoQUpstreamGoawaysTest, SessionGoawayReachesEveryWatcherOfThatSession) {
  auto session = makeSession();
  auto otherSession = makeSession();
  std::vector<std::string> reached;
  auto first = goaways_->watch(*session, [&](const Goaway& goaway) {
    reached.push_back("first " + goaway.newSessionUri);
  });
  auto second = goaways_->watch(*session, [&](const Goaway& goaway) {
    reached.push_back("second " + goaway.newSessionUri);
  });
  auto other = goaways_->watch(
      *otherSession, [&](const Goaway&) { reached.emplace_back("other"); });

  receiveGoaway(*session, Goaway{.newSessionUri = "moqt://next"});

  std::sort(reached.begin(), reached.end());
  const std::vector<std::string> expected{
      "first moqt://next", "second moqt://next"};
  EXPECT_EQ(reached, expected);
}

TEST_F(MoQUpstreamGoawaysTest, ResetWatchStopsItsCallback) {
  auto session = makeSession();
  int calls = 0;
  auto watch = goaways_->watch(*session, [&](const Goaway&) { ++calls; });

  watch.reset();
  receiveGoaway(*session, Goaway{});

  EXPECT_EQ(calls, 0);
}

TEST_F(MoQUpstreamGoawaysTest, CallbackCanStopWatchesDuringTheGoaway) {
  auto session = makeSession();
  std::optional<MoQUpstreamGoaways::Watch> first;
  std::optional<MoQUpstreamGoaways::Watch> second;
  int calls = 0;
  auto stopBoth = [&](const Goaway&) {
    ++calls;
    first.reset();
    second.reset();
  };
  first = goaways_->watch(*session, stopBoth);
  second = goaways_->watch(*session, stopBoth);

  receiveGoaway(*session, Goaway{});

  EXPECT_EQ(calls, 1);
}

TEST_F(MoQUpstreamGoawaysTest, CallbackCanReleaseTheObserverDuringTheGoaway) {
  auto session = makeSession();
  int calls = 0;
  auto releaseObserver = [&](const Goaway&) {
    ++calls;
    session->setSubscribeHandler(nullptr);
  };
  auto first = goaways_->watch(*session, releaseObserver);
  auto second = goaways_->watch(*session, releaseObserver);

  receiveGoaway(*session, Goaway{});

  EXPECT_EQ(calls, 2);
}

TEST_F(MoQUpstreamGoawaysTest, ForgetsASessionOnceItDropsItsObserver) {
  auto session = makeSession();
  auto watch = goaways_->watch(*session, [](const Goaway&) {});
  watch.reset();
  EXPECT_EQ(goaways_->watchedSessions(), 1);

  session->setSubscribeHandler(nullptr);

  EXPECT_EQ(goaways_->watchedSessions(), 0);
}

TEST_F(
    MoQUpstreamGoawaysTest,
    WatchAfterTheSessionDroppedItsObserverStillWorks) {
  auto session = makeSession();
  int staleCalls = 0;
  int freshCalls = 0;
  auto stale = goaways_->watch(*session, [&](const Goaway&) { ++staleCalls; });
  // A closing session releases its handlers.
  session->setSubscribeHandler(nullptr);
  auto fresh = goaways_->watch(*session, [&](const Goaway&) { ++freshCalls; });

  receiveGoaway(*session, Goaway{});

  EXPECT_EQ(staleCalls, 0);
  EXPECT_EQ(freshCalls, 1);
}

}} // namespace moxygen
