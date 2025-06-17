// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "folly/json.h"
#include "folly/json/dynamic.h"
#include "moxygen/MoQFramer.h"
#include "moxygen/mlog/MLogEvents.h"
#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {

// Client-Initiated Bidirectional Stream ID
const uint64_t kFirstBidiStreamId = 0;
const std::string kDefaultLoggerFilePath = "./mlog.txt";

// Main Logger Class -> results in json output in mlog.txt file
class MLogger {
 public:
  explicit MLogger(VantagePoint vantagePoint) : vantagePoint_(vantagePoint) {}

  MOQTClientSetupMessage createClientSetupControlMessage(
      uint64_t numberOfSupportedVersions,
      std::vector<uint64_t> supportedVersions,
      uint64_t numberOfParameters,
      std::vector<MOQTParameter> params);

  void addControlMessageCreatedLog(MOQTControlMessageCreated req);

  void outputLogsToFile();

  void logClientSetup(const ClientSetup& setup);

  void setPath(const std::string& path);

 private:
  VantagePoint vantagePoint_;
  std::vector<MLogEvent> logs_;
  std::string path_ = kDefaultLoggerFilePath;
  MLogEventCreator eventCreator_ = MLogEventCreator();

  // Log Formatting
  folly::dynamic formatLog(const MLogEvent& log);
};

} // namespace moxygen
