/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/lane_manager/SwitchingPolicy.h"

#include <cstdint>
#include <optional>
#include <string>

namespace moxygen::samples {

// For tests: starts on the lowest lane and moves to the next one, the lowest
// after the highest, every `groupsPerLane` groups. Ignores the network.
class ScriptedPolicy : public lane_manager::SwitchingPolicy {
 public:
  explicit ScriptedPolicy(uint64_t groupsPerLane = 10);

  std::string initialLane(const lane_manager::SwitchingSet& set) override;

  std::optional<lane_manager::SwitchPlan> select(
      const lane_manager::SwitchingSet& set,
      std::string_view liveLane,
      uint64_t group,
      lane_manager::Clock::time_point now) override;

 private:
  uint64_t groupsPerLane_;
  uint64_t groups_{0};
};

} // namespace moxygen::samples
