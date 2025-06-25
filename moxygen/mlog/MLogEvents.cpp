/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

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

MLogEvent MLogEventCreator::createControlMessageParsedEvent(
    VantagePoint vantagePoint,
    MOQTControlMessageParsed req) {
  auto log = MLogEvent(
      vantagePoint,
      static_cast<uint64_t>(
          std::chrono::system_clock::now().time_since_epoch().count()) -
          startTime_,
      kControlMessageParsedName);

  log.data_ = std::move(req);
  return log;
}

MLogEvent MLogEventCreator::createStreamTypeSetEvent(
    VantagePoint vantagePoint,
    MOQTStreamTypeSet req) {
  auto log = MLogEvent(
      vantagePoint,
      static_cast<uint64_t>(
          std::chrono::system_clock::now().time_since_epoch().count()) -
          startTime_,
      kStreamTypeSetName);

  log.data_ = std::move(req);
  return log;
}

MLogEvent MLogEventCreator::createObjectDatagramCreatedEvent(
    VantagePoint vantagePoint,
    MOQTObjectDatagramCreated req) {
  auto log = MLogEvent(
      vantagePoint,
      static_cast<uint64_t>(
          std::chrono::system_clock::now().time_since_epoch().count()) -
          startTime_,
      kObjectDatagramCreatedName);

  log.data_ = std::move(req);
  return log;
}

MLogEvent MLogEventCreator::createObjectDatagramParsedEvent(
    VantagePoint vantagePoint,
    MOQTObjectDatagramParsed req) {
  auto log = MLogEvent(
      vantagePoint,
      static_cast<uint64_t>(
          std::chrono::system_clock::now().time_since_epoch().count()) -
          startTime_,
      kObjectDatagramParsedName);

  log.data_ = std::move(req);
  return log;
}

} // namespace moxygen
