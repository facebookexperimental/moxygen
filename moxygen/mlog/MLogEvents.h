/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {
inline constexpr const char* kControlMessageCreatedName =
    "moqt:control_message_created";
inline constexpr const char* kControlMessageParsedName =
    "moqt:control_message_parsed";
inline constexpr const char* kStreamTypeSetName = "moqt:stream_type_set";
inline constexpr const char* kObjectDatagramCreatedName =
    "moqt:object_datagram_created";
inline constexpr const char* kObjectDatagramParsedName =
    "moqt:object_datagram_parsed";
inline constexpr const char* kObjectDatagramStatusCreatedName =
    "moqt:object_datagram_status_created";

// Basic Log Event Class
class MLogEvent {
 public:
  MLogEvent(VantagePoint vantagePoint, uint64_t time, std::string name)
      : vantagePoint_(vantagePoint), time_(time), name_(std::move(name)) {}

  VantagePoint vantagePoint_;
  uint64_t time_{0};
  std::string name_;
  std::variant<
      MOQTControlMessageCreated,
      MOQTControlMessageParsed,
      MOQTStreamTypeSet,
      MOQTObjectDatagramCreated,
      MOQTObjectDatagramParsed,
      MOQTObjectDatagramStatusCreated>
      data_; // Add more events to variant as implemented
};

// Class to Create Log Events - Going to be Used by MLogger to add logs to final
// JSON Output
class MLogEventCreator {
 public:
  MLogEventCreator();

  // Creates an MLogEvent in which MLogger will write
  // to a txt file as a JSON
  MLogEvent createControlMessageCreatedEvent(
      VantagePoint vantagePoint,
      MOQTControlMessageCreated req);

  MLogEvent createControlMessageParsedEvent(
      VantagePoint vantagePoint,
      MOQTControlMessageParsed req);

  MLogEvent createStreamTypeSetEvent(
      VantagePoint vantagePoint,
      MOQTStreamTypeSet req);

  MLogEvent createObjectDatagramCreatedEvent(
      VantagePoint vantagePoint,
      MOQTObjectDatagramCreated req);

  MLogEvent createObjectDatagramParsedEvent(
      VantagePoint vantagePoint,
      MOQTObjectDatagramParsed req);

  MLogEvent createObjectDatagramStatusCreatedEvent(
      VantagePoint vantagePoint,
      MOQTObjectDatagramStatusCreated req);

 private:
  uint64_t startTime_;
};

} // namespace moxygen
