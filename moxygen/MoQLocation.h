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

} // namespace moxygen
