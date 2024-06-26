/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include "moxygen/MoQFramer.h"

namespace moxygen {

inline AbsoluteLocation toAbsolute(
    LocationType locType,
    folly::Optional<AbsoluteLocation> groupAndObject,
    const uint64_t latestGroup,
    const uint64_t latestObj) {
  XLOG(DBG1)
      << "m=" << uint64_t(locType)
      << (groupAndObject
              ? folly::to<std::string>(
                    "g=", groupAndObject->group, " o=", groupAndObject->object)
              : std::string());
  AbsoluteLocation result;
  switch (locType) {
    case LocationType::LatestGroup:
      result.group = latestGroup;
      result.object = 0;
      break;
    case LocationType::LatestObject:
      result.group = latestGroup;
      result.object = latestObj;
      break;
    case LocationType::AbsoluteStart:
    case LocationType::AbsoluteRange:
      result.group = groupAndObject->group;
      result.object = groupAndObject->object;
      break;
  }
  XLOG(DBG1) << "g=" << result.group << " o=" << result.object;
  return result;
}
} // namespace moxygen
