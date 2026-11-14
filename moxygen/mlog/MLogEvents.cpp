/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/mlog/MLogEvents.h"

namespace moxygen {

MLogEventCreator::MLogEventCreator() {
  startTime_ = static_cast<uint64_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
}

MLogEvent MLogEventCreator::createEvent(
    VantagePoint vantagePoint,
    std::string_view name,
    MLogEventData data) {
  return MLogEvent(
      vantagePoint,
      static_cast<uint64_t>(
          std::chrono::system_clock::now().time_since_epoch().count()) -
          startTime_,
      name,
      std::move(data));
}

} // namespace moxygen
