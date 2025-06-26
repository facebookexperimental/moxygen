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

inline constexpr std::string_view kControlMessageCreatedName =
    "moqt:control_message_created";
inline constexpr std::string_view kControlMessageParsedName =
    "moqt:control_message_parsed";
inline constexpr std::string_view kStreamTypeSetName = "moqt:stream_type_set";
inline constexpr std::string_view kObjectDatagramCreatedName =
    "moqt:object_datagram_created";
inline constexpr std::string_view kObjectDatagramParsedName =
    "moqt:object_datagram_parsed";
inline constexpr std::string_view kObjectDatagramStatusCreatedName =
    "moqt:object_datagram_status_created";
inline constexpr std::string_view kObjectDatagramStatusParsedName =
    "moqt:object_datagram_status_parsed";
inline constexpr std::string_view kSubgroupHeaderCreatedName =
    "moqt:subgroup_header_created";
inline constexpr std::string_view kSubgroupHeaderParsedName =
    "moqt:subgroup_header_parsed";
inline constexpr std::string_view kSubgroupObjectCreatedName =
    "moqt:subgroup_object_created";
inline constexpr std::string_view kSubgroupObjectParsedName =
    "moqt:subgroup_object_parsed";

// Basic Log Event Class
class MLogEvent {
 public:
  MLogEvent(VantagePoint vantagePoint, uint64_t time, std::string_view name)
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
      MOQTObjectDatagramStatusCreated,
      MOQTObjectDatagramStatusParsed,
      MOQTSubgroupHeaderCreated,
      MOQTSubgroupHeaderParsed,
      MOQTSubgroupObjectCreated,
      MOQTSubgroupObjectParsed>
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

  MLogEvent createObjectDatagramStatusParsedEvent(
      VantagePoint vantagePoint,
      MOQTObjectDatagramStatusParsed req);

  MLogEvent createSubgroupHeaderCreatedEvent(
      VantagePoint vantagePoint,
      MOQTSubgroupHeaderCreated req);

  MLogEvent createSubgroupHeaderParsedEvent(
      VantagePoint vantagePoint,
      MOQTSubgroupHeaderParsed req);

  MLogEvent createSubgroupObjectCreatedEvent(
      VantagePoint vantagePoint,
      MOQTSubgroupObjectCreated req);

  MLogEvent createSubgroupObjectParsedEvent(
      VantagePoint vantagePoint,
      MOQTSubgroupObjectParsed req);

 private:
  uint64_t startTime_;
};

} // namespace moxygen
