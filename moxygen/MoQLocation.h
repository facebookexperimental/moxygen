/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include "moxygen/MoQFramer.h"

namespace moxygen {

inline SubscribeRange toSubscribeRange(
    const LocationFilter& filter,
    std::optional<AbsoluteLocation> largest) {
  return filter.resolve(largest, /*isFetch=*/false);
}

inline SubscribeRange toSubscribeRange(
    const SubscribeRequest& sub,
    std::optional<AbsoluteLocation> largest) {
  return sub.filter.resolve(largest, /*isFetch=*/false);
}

inline StandaloneFetch toFetchRange(
    const LocationFilter& filter,
    std::optional<AbsoluteLocation> largest) {
  auto [start, end] = filter.fetchRange(largest);
  return StandaloneFetch(start, end);
}

// A StandaloneFetch end is a wire End Location: object 0 means the whole end
// group, so only an earlier end group is empty.
inline bool isEmptyFetchRange(const StandaloneFetch& range) {
  if (range.end.group == range.start.group && range.end.object == 0) {
    return false;
  }
  return range.end <= range.start;
}

} // namespace moxygen
