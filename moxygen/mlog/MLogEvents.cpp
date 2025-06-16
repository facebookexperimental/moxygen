// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/mlog/MLogEvents.h"

namespace moxygen {

MLogEventCreator::MLogEventCreator() {
  startTime_ = static_cast<uint64_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
}

MLogEvent MLogEventCreator::createControlMessageCreatedEvent(
    VantagePoint vantagePoint,
    MOQTControlMessageCreated req) {
  auto log = MLogEvent(
      vantagePoint,
      static_cast<uint64_t>(
          std::chrono::system_clock::now().time_since_epoch().count()) -
          startTime_,
      kControlMessageCreatedName);

  log.data_ = std::move(req);
  return log;
};

} // namespace moxygen
