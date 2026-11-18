/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/lane_manager/SwitchingSet.h"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace moxygen::lane_manager {

using Clock = std::chrono::steady_clock;

// What happens when the wait runs out before the target lane's group starts.
enum class TimeoutAction {
  KeepCurrent,       // send the held group from the live lane and drop the plan
  RetryNextBoundary, // send the held group and try again at the next group
};

// Asks for a subscription set to move to another lane at the next group.
struct SwitchPlan {
  std::string targetLane;
  // How long the live lane's new group may be held while waiting for the
  // target's. Unset uses the switch executor's default; 0 never holds.
  std::optional<std::chrono::milliseconds> maxWait;
  TimeoutAction timeoutAction{TimeoutAction::KeepCurrent};

  bool operator==(const SwitchPlan&) const = default;
};

struct SwitchOutcome {
  enum class Kind {
    Prepared,  // the plan is armed for the next group
    Committed, // the target lane is live
    Cancelled, // the plan was dropped and the live lane stays
    Retried,   // the wait ran out and the plan moved to the next group
  };

  Kind kind{Kind::Prepared};
  std::string fromLane;
  std::string targetLane;
  // Where the switch takes effect; for Prepared, the group it was planned at;
  // for Retried, the group it missed.
  uint64_t group{0};
  Clock::time_point at{}; // on the same clock as select()'s `now`
};

// Chooses lanes for one subscription set (video or audio). Each set has its own
// instance, called on the set's EventBase. Inputs shared across a connection,
// such as a bandwidth estimate, are given to the policy when the policy factory
// creates it.
class SwitchingPolicy {
 public:
  virtual ~SwitchingPolicy() = default;

  // The lane to start on. `set` is never empty; return one of its lanes.
  virtual std::string initialLane(const SwitchingSet& set) = 0;

  // Called when `group` starts on the live lane, except while a switch is
  // pending; when a switch commits or is cancelled, it is called for the group
  // where that happened. A returned plan takes effect at the next group. `set`
  // holds the lanes that can be picked now, always including the live lane.
  virtual std::optional<SwitchPlan> select(
      const SwitchingSet& set,
      std::string_view liveLane,
      uint64_t group,
      Clock::time_point now) = 0;

  virtual void onSwitchOutcome(const SwitchOutcome& /*outcome*/) {}
};

} // namespace moxygen::lane_manager
