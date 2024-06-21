/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include "moxygen/MoQFramer.h"

namespace moxygen {

struct AbsoluteLocation {
  uint64_t group;
  uint64_t object;

  std::strong_ordering operator<=>(const AbsoluteLocation& other) const {
    if (group < other.group) {
      return std::strong_ordering::less;
    } else if (group == other.group) {
      if (object < other.object) {
        return std::strong_ordering::less;
      } else if (object == other.object) {
        return std::strong_ordering::equivalent;
      } else {
        return std::strong_ordering::greater;
      }
    } else {
      return std::strong_ordering::greater;
    }
  }
};

constexpr AbsoluteLocation kLocationMax{
    std::numeric_limits<uint64_t>::max(),
    std::numeric_limits<uint64_t>::max()};

inline AbsoluteLocation toAbsolulte(
    LocationType locType,
    folly::Optional<GroupAndObject> groupAndObject,
    const uint64_t curGroup,
    const uint64_t curObj) {
  XLOG(DBG1) << "m=" << uint64_t(locType)
             << (groupAndObject ? folly::to<std::string>(
                                      "g=",
                                      groupAndObject->groupID,
                                      " o=",
                                      groupAndObject->objectID)
                                : std::string());
  AbsoluteLocation result;
  switch (locType) {
    case LocationType::LatestGroup:
      result.group = curGroup;
      result.object = 0;
      break;
    case LocationType::LatestObject:
      result.group = curGroup;
      result.object = curObj;
      break;
    case LocationType::AbsoluteStart:
    case LocationType::AbsoluteRange:
      result.group = groupAndObject->groupID;
      result.object = groupAndObject->objectID;
      break;
  }
  XLOG(DBG1) << "g=" << result.group << " o=" << result.object;
  return result;
}
} // namespace moxygen
