/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/SwitchExecutor.h"

#include <folly/portability/GTest.h>

#include <stdexcept>

namespace moxygen::lane_manager { namespace {

using namespace std::chrono_literals;
using Action = SwitchExecutor::Action;
using Kind = SwitchOutcome::Kind;
using Outcomes = std::vector<std::pair<Kind, uint64_t>>;

const Clock::time_point kNow{};

LaneInfo lane(std::string name, std::string codec) {
  LaneInfo info;
  info.trackName = std::move(name);
  info.codec = std::move(codec);
  return info;
}

const SwitchingSet kSet{
    .id = 0,
    .lanes =
        {lane("low", "avc1.4d401f"),
         lane("high", "avc1.640028"),
         lane("hevc", "hvc1.1.6.L93.B0")},
    .audioAllowanceBps = std::nullopt};

SwitchPlan to(
    std::string lane,
    std::optional<std::chrono::milliseconds> maxWait = std::nullopt,
    TimeoutAction timeoutAction = TimeoutAction::KeepCurrent) {
  SwitchPlan plan;
  plan.targetLane = std::move(lane);
  plan.maxWait = maxWait;
  plan.timeoutAction = timeoutAction;
  return plan;
}

Outcomes outcomes(SwitchExecutor& executor) {
  Outcomes result;
  for (const auto& outcome : executor.takeOutcomes()) {
    result.emplace_back(outcome.kind, outcome.group);
  }
  return result;
}

TEST(SwitchExecutorTest, CommitsWhenTheTargetGroupComesFirst) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high"), 10));

  EXPECT_EQ(executor.onGroup("high", 10, kNow), Action::Drop);
  // Group IDs need not be consecutive.
  EXPECT_EQ(executor.onGroup("high", 13, kNow), Action::Forward);
  EXPECT_EQ(executor.onGroup("low", 13, kNow), Action::Drop);
  EXPECT_EQ(executor.liveLane(), "high");
  EXPECT_EQ(outcomes(executor), (Outcomes{{Kind::Committed, 13}}));
}

TEST(SwitchExecutorTest, HoldsTheLiveGroupUntilTheTargetArrives) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high"), 10));

  EXPECT_EQ(executor.onGroup("low", 11, kNow), Action::Hold);
  EXPECT_EQ(executor.deadline(), kNow + 100ms);
  EXPECT_EQ(executor.onGroup("high", 11, kNow + 30ms), Action::Forward);
  EXPECT_EQ(executor.deadline(), std::nullopt);
  EXPECT_EQ(outcomes(executor), (Outcomes{{Kind::Committed, 11}}));
}

TEST(SwitchExecutorTest, CancelsWhenTheWaitRunsOut) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high", 50ms), 10));
  EXPECT_EQ(executor.onGroup("low", 11, kNow), Action::Hold);

  executor.onDeadline(kNow + 50ms);
  EXPECT_EQ(outcomes(executor), (Outcomes{{Kind::Cancelled, 11}}));
  EXPECT_EQ(executor.onGroup("low", 11, kNow + 50ms), Action::Forward);
  EXPECT_EQ(executor.onGroup("high", 11, kNow + 60ms), Action::Drop);
  EXPECT_EQ(executor.liveLane(), "low");
}

TEST(SwitchExecutorTest, RetriesAtTheNextGroup) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(
      kSet, to("high", 50ms, TimeoutAction::RetryNextBoundary), 10));
  EXPECT_EQ(executor.onGroup("low", 11, kNow), Action::Hold);

  executor.onDeadline(kNow + 50ms);
  EXPECT_EQ(executor.onGroup("low", 11, kNow + 50ms), Action::Forward);
  EXPECT_EQ(executor.onGroup("high", 12, kNow + 60ms), Action::Forward);
  EXPECT_EQ(
      outcomes(executor),
      (Outcomes{{Kind::Retried, 11}, {Kind::Committed, 12}}));
}

TEST(SwitchExecutorTest, ZeroWaitNeverHolds) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high", 0ms), 10));

  EXPECT_EQ(executor.onGroup("low", 11, kNow), Action::Forward);
  EXPECT_EQ(outcomes(executor), (Outcomes{{Kind::Cancelled, 11}}));
}

TEST(SwitchExecutorTest, RejectsPlansItCannotApply) {
  SwitchExecutor executor("low", 100ms);

  EXPECT_FALSE(executor.plan(kSet, to("low"), 10));
  EXPECT_FALSE(executor.plan(kSet, to("hevc"), 10));
  EXPECT_FALSE(executor.plan(kSet, to("gone"), 10));
  EXPECT_TRUE(executor.plan(kSet, to("high"), 10));
}

TEST(SwitchExecutorTest, RejectsANegativeDefaultWait) {
  EXPECT_THROW(SwitchExecutor("low", -1ms), std::invalid_argument);
}

TEST(SwitchExecutorTest, ExpiresByTimeWhenTheTargetComesLate) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high", 50ms), 10));
  EXPECT_EQ(executor.onGroup("low", 11, kNow), Action::Hold);

  // The hold timer has not run yet.
  EXPECT_EQ(executor.onGroup("high", 11, kNow + 60ms), Action::Drop);
  EXPECT_EQ(executor.liveLane(), "low");
  EXPECT_EQ(outcomes(executor), (Outcomes{{Kind::Cancelled, 11}}));
}

TEST(SwitchExecutorTest, DropsAGroupOlderThanOneAlreadySent) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high", 50ms), 10));
  EXPECT_EQ(executor.onGroup("low", 11, kNow), Action::Hold);
  executor.onDeadline(kNow + 50ms);
  ASSERT_TRUE(executor.plan(kSet, to("high"), 11));
  EXPECT_EQ(executor.onGroup("high", 12, kNow + 60ms), Action::Forward);

  // Group 11 went out from "low"; a late "high" 11 must not reopen it.
  EXPECT_EQ(executor.onGroup("high", 11, kNow + 70ms), Action::Drop);
  EXPECT_EQ(executor.onGroup("high", 12, kNow + 70ms), Action::Forward);
}

TEST(SwitchExecutorTest, CancelSendsTheHeldGroup) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high"), 10));
  EXPECT_EQ(executor.onGroup("low", 11, kNow), Action::Hold);

  executor.cancel(kNow + 10ms);
  EXPECT_FALSE(executor.pending());
  EXPECT_EQ(executor.deadline(), std::nullopt);
  EXPECT_EQ(outcomes(executor), (Outcomes{{Kind::Cancelled, 11}}));
  EXPECT_EQ(executor.onGroup("low", 11, kNow + 10ms), Action::Forward);
  EXPECT_EQ(executor.onGroup("high", 11, kNow + 20ms), Action::Drop);
}

TEST(SwitchExecutorTest, CancelBeforeTheNextGroupReportsThePlannedGroup) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high"), 10));

  executor.cancel(kNow);
  EXPECT_EQ(outcomes(executor), (Outcomes{{Kind::Cancelled, 10}}));
  EXPECT_EQ(executor.onGroup("high", 11, kNow), Action::Drop);
}

TEST(SwitchExecutorTest, AnEndedTargetDropsThePlan) {
  SwitchExecutor executor("low", 100ms);
  ASSERT_TRUE(executor.plan(kSet, to("high"), 10));

  executor.onLaneEnded("hevc", kNow);
  EXPECT_TRUE(executor.pending());
  executor.onLaneEnded("high", kNow);
  EXPECT_FALSE(executor.pending());
  EXPECT_EQ(outcomes(executor), (Outcomes{{Kind::Cancelled, 10}}));
}

}} // namespace moxygen::lane_manager
