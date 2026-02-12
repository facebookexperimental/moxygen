/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include "moxygen/MoQFramer.h"

namespace moxygen {

struct SubscribeRange {
  AbsoluteLocation start;
  AbsoluteLocation end;
};

inline SubscribeRange toSubscribeRange(
    std::optional<AbsoluteLocation> start,
    std::optional<AbsoluteLocation> end,
    LocationType locType,
    std::optional<AbsoluteLocation> largest) {
  XLOG(DBG1) << "m=" << toString(locType)
             << (start ? folly::to<std::string>(
                             " g=", start->group, " o=", start->object)
                       : std::string());
  SubscribeRange result;
  result.end = kLocationMax;
  switch (locType) {
    case LocationType::NextGroupStart:
      result.start.group =
          (largest.has_value() ? largest->group + 1 : kLocationMin.group);
      result.start.object = 0;
      break;
    case LocationType::LargestObject:
      result.start.group = largest.value_or(kLocationMin).group;
      result.start.object = largest ? largest->object + 1 : kLocationMin.object;
      break;
    case LocationType::AbsoluteRange:
      XCHECK(end);
      // moxygen uses exclusive end objects, so no need to subtract 1 here
      result.end = *end;
      FMT_FALLTHROUGH;
    case LocationType::AbsoluteStart:
      XCHECK(start);
      result.start.group = start->group;
      result.start.object = start->object;
      if (largest && result.start < *largest) {
        XLOG(DBG2) << "Adjusting past start to largest + 1";
        result.start = AbsoluteLocation{largest->group, largest->object + 1};
      }
      break;
    case LocationType::LargestGroup:
      result.start.group = largest.value_or(kLocationMin).group;
      result.start.object = 0;
      break;
  }
  XLOG(DBG1) << "g=" << result.start.group << " o=" << result.start.object
             << " g=" << result.end.group << " o=" << result.end.object;
  return result;
}

inline SubscribeRange toSubscribeRange(
    const SubscribeRequest& sub,
    std::optional<AbsoluteLocation> largest) {
  std::optional<AbsoluteLocation> end;
  if (sub.locType == LocationType::AbsoluteRange) {
    end = AbsoluteLocation({sub.endGroup, 0});
  }
  return toSubscribeRange(sub.start, end, sub.locType, std::move(largest));
}

} // namespace moxygen
