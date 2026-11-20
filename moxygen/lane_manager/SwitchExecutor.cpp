/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/SwitchExecutor.h"

#include <algorithm>
#include <stdexcept>

namespace moxygen::lane_manager {

namespace {

std::string_view family(std::string_view codec) {
  return codec.substr(0, 4);
}

} // namespace

SwitchExecutor::SwitchExecutor(
    std::string liveLane,
    std::chrono::milliseconds defaultMaxWait)
    : live_(std::move(liveLane)), defaultMaxWait_(defaultMaxWait) {
  if (defaultMaxWait_.count() < 0) {
    throw std::invalid_argument("negative default switch wait");
  }
}

std::optional<Clock::time_point> SwitchExecutor::deadline() const {
  if (pending_ && pending_->held) {
    return pending_->deadline;
  }
  return std::nullopt;
}

bool SwitchExecutor::plan(
    const SwitchingSet& set,
    const SwitchPlan& plan,
    uint64_t group) {
  const auto* live = set.find(live_);
  const auto* target = set.find(plan.targetLane);
  if (pending_ || !live || !target || target == live ||
      family(target->codec) != family(live->codec) ||
      (plan.maxWait && plan.maxWait->count() < 0)) {
    return false;
  }
  pending_ = Pending{plan, group, std::nullopt, Clock::time_point{}};
  return true;
}

SwitchExecutor::Action SwitchExecutor::onGroup(
    std::string_view lane,
    uint64_t group,
    Clock::time_point now) {
  onDeadline(now);
  if (lastSent_ && group < *lastSent_) {
    return Action::Drop;
  }
  if (!pending_ || group <= pending_->after) {
    return lane == live_ ? forward(group) : Action::Drop;
  }
  auto& pending = *pending_;
  if (lane == live_) {
    if (pending.held == group) {
      return Action::Hold;
    }
    if (pending.held) {
      expire(now);
      return onGroup(lane, group, now);
    }
    pending.held = group;
    const auto wait = pending.plan.maxWait.value_or(defaultMaxWait_);
    if (wait.count() == 0) {
      expire(now);
      return forward(group);
    }
    pending.deadline = now + wait;
    return Action::Hold;
  }
  if (lane != pending.plan.targetLane ||
      (pending.held && *pending.held > group)) {
    return Action::Drop;
  }
  if (pending.held && *pending.held < group) {
    expire(now);
    return onGroup(lane, group, now);
  }
  outcomes_.push_back(
      SwitchOutcome{
          SwitchOutcome::Kind::Committed,
          live_,
          std::string(lane),
          group,
          now});
  live_ = std::string(lane);
  pending_.reset();
  return forward(group);
}

SwitchExecutor::Action SwitchExecutor::forward(uint64_t group) {
  lastSent_ = std::max(group, lastSent_.value_or(group));
  return Action::Forward;
}

void SwitchExecutor::onDeadline(Clock::time_point now) {
  if (pending_ && pending_->held && now >= pending_->deadline) {
    expire(now);
  }
}

void SwitchExecutor::cancel(Clock::time_point now) {
  if (!pending_) {
    return;
  }
  const auto group = pending_->held.value_or(pending_->after);
  if (pending_->held) {
    forward(*pending_->held);
  }
  outcomes_.push_back(
      SwitchOutcome{
          SwitchOutcome::Kind::Cancelled,
          live_,
          pending_->plan.targetLane,
          group,
          now});
  pending_.reset();
}

void SwitchExecutor::onLaneEnded(std::string_view lane, Clock::time_point now) {
  if (pending_ && pending_->plan.targetLane == lane) {
    cancel(now);
  }
}

std::vector<SwitchOutcome> SwitchExecutor::takeOutcomes() {
  return std::exchange(outcomes_, {});
}

void SwitchExecutor::expire(Clock::time_point now) {
  auto& pending = *pending_;
  forward(*pending.held);
  const bool retry =
      pending.plan.timeoutAction == TimeoutAction::RetryNextBoundary;
  outcomes_.push_back(
      SwitchOutcome{
          retry ? SwitchOutcome::Kind::Retried : SwitchOutcome::Kind::Cancelled,
          live_,
          pending.plan.targetLane,
          *pending.held,
          now});
  if (retry) {
    pending.after = *pending.held;
    pending.held.reset();
  } else {
    pending_.reset();
  }
}

} // namespace moxygen::lane_manager
