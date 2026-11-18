/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/samples/lane_manager/ScriptedPolicy.h"

#include <algorithm>

namespace moxygen::samples {

ScriptedPolicy::ScriptedPolicy(uint64_t groupsPerLane)
    : groupsPerLane_(std::max<uint64_t>(groupsPerLane, 1)) {}

std::string ScriptedPolicy::initialLane(const lane_manager::SwitchingSet& set) {
  if (set.lanes.empty()) {
    return {};
  }
  return set.lanes.front().trackName;
}

std::optional<lane_manager::SwitchPlan> ScriptedPolicy::select(
    const lane_manager::SwitchingSet& set,
    std::string_view liveLane,
    uint64_t /*group*/,
    lane_manager::Clock::time_point /*now*/) {
  if (++groups_ % groupsPerLane_ != 0 || set.lanes.empty()) {
    return std::nullopt;
  }
  const auto live =
      std::find_if(set.lanes.begin(), set.lanes.end(), [&](const auto& lane) {
        return lane.trackName == liveLane;
      });
  const size_t next = (live - set.lanes.begin() + 1) % set.lanes.size();
  const auto& target = set.lanes[next].trackName;
  if (target == liveLane) {
    return std::nullopt;
  }
  lane_manager::SwitchPlan plan;
  plan.targetLane = target;
  return plan;
}

} // namespace moxygen::samples
