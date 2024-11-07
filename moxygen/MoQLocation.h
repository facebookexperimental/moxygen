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
    folly::Optional<AbsoluteLocation> start,
    folly::Optional<AbsoluteLocation> end,
    LocationType locType,
    folly::Optional<AbsoluteLocation> latest) {
  XLOG(DBG1) << "m=" << uint64_t(locType)
             << (start ? folly::to<std::string>(
                             "g=", start->group, " o=", start->object)
                       : std::string());
  SubscribeRange result;
  result.end = kLocationMax;
  switch (locType) {
    case LocationType::LatestGroup:
      result.start.group = latest.value_or(kLocationMin).group;
      result.start.object = 0;
      break;
    case LocationType::LatestObject:
      result.start.group = latest.value_or(kLocationMin).group;
      result.start.object = latest.value_or(kLocationMin).object;
      break;
    case LocationType::AbsoluteRange:
      XCHECK(end);
      if (end->object == 0) {
        result.end.group = end->group + 1;
        result.end.object = 0;
      } else {
        // moxygen uses exclusive end objects, so no need to subtract 1 here
        result.end = *end;
      }
      FMT_FALLTHROUGH;
    case LocationType::AbsoluteStart:
      XCHECK(start);
      result.start.group = start->group;
      result.start.object = start->object;
      break;
  }
  XLOG(DBG1) << "g=" << result.start.group << " o=" << result.start.object
             << "g=" << result.end.group << " o=" << result.end.object;
  return result;
}

inline SubscribeRange toSubscribeRange(
    const SubscribeRequest& sub,
    folly::Optional<AbsoluteLocation> latest) {
  return toSubscribeRange(sub.start, sub.end, sub.locType, latest);
}

} // namespace moxygen
