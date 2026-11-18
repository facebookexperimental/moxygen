/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/lane_manager/SwitchingPolicy.h"

#include <chrono>
#include <optional>
#include <string>
#include <vector>

namespace moxygen::lane_manager {

// Applies one switch at a time to a virtual track. For each group a lane
// starts, it decides whether the group is forwarded, dropped, or held while a
// planned switch waits for the target's group. No I/O and no timers.
class SwitchExecutor {
 public:
  enum class Action { Forward, Drop, Hold };

  SwitchExecutor(
      std::string liveLane,
      std::chrono::milliseconds defaultMaxWait);

  const std::string& liveLane() const {
    return live_;
  }

  bool pending() const {
    return pending_.has_value();
  }

  // Set while a group is held.
  std::optional<Clock::time_point> deadline() const;

  // Arms `plan`, made at the live lane's `group`, for a later group. False if a
  // switch is pending, the target is the live lane, missing from `set` or of
  // another codec family, or the wait is negative.
  bool plan(const SwitchingSet& set, const SwitchPlan& plan, uint64_t group);

  Action onGroup(std::string_view lane, uint64_t group, Clock::time_point now);

  // The deadline passed; the held group is to be sent.
  void onDeadline(Clock::time_point now);

  std::vector<SwitchOutcome> takeOutcomes();

 private:
  struct Pending {
    SwitchPlan plan;
    uint64_t after{0};
    std::optional<uint64_t> held;
    Clock::time_point deadline;
  };

  void expire(Clock::time_point now);

  std::string live_;
  std::chrono::milliseconds defaultMaxWait_;
  std::optional<Pending> pending_;
  std::vector<SwitchOutcome> outcomes_;
};

} // namespace moxygen::lane_manager
