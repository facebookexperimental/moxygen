// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {

const std::string kControlMessageCreatedName = "moqt:control_message_created";

// Basic Log Event Class
class MLogEvent {
 public:
  MLogEvent(VantagePoint vantagePoint, uint64_t time, std::string name)
      : vantagePoint_(vantagePoint), time_(time), name_(name){};

  VantagePoint vantagePoint_;
  uint64_t time_{0};
  std::string name_{""};
  std::variant<MOQTControlMessageCreated>
      data_; // Add more events to variant as implemented
};

// Class to Create Log Events - Going to be Used by MLogger to add logs to final
// JSON Output
class MLogEventCreator {
 public:
  MLogEventCreator();

  // Creates an MLogEvent for ControlMessageCreated in which MLogger will write
  // to a txt file as a JSON
  MLogEvent createControlMessageCreatedEvent(
      VantagePoint vantagePoint,
      MOQTControlMessageCreated req);

 private:
  uint64_t startTime_;
};

} // namespace moxygen
