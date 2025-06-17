// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/mlog/MLogger.h"
#include <fstream>
#include <utility>

namespace moxygen {

void MLogger::setPath(const std::string& path) {
  path_ = path;
}

void MLogger::addControlMessageCreatedLog(MOQTControlMessageCreated req) {
  auto log = eventCreator_.createControlMessageCreatedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

MOQTClientSetupMessage MLogger::createClientSetupControlMessage(
    uint64_t numberOfSupportedVersions,
    std::vector<uint64_t> supportedVersions,
    uint64_t numberOfParameters,
    std::vector<MOQTParameter> params) {
  MOQTClientSetupMessage client = MOQTClientSetupMessage();
  client.numberOfSupportedVersions = numberOfSupportedVersions;
  client.supportedVersions = std::move(supportedVersions);
  client.numberOfParameters = numberOfParameters;
  client.setupParameters = std::move(params);
  return client;
}

folly::dynamic MLogger::formatLog(const MLogEvent& log) {
  folly::dynamic logObject = folly::dynamic::object;
  logObject["vantagePoint"] =
      (log.vantagePoint_ == VantagePoint::CLIENT) ? "client" : "server";
  logObject["name"] = std::string(log.name_);
  logObject["time"] = std::to_string(log.time_);

  // Switch Based on Name to format data type correctly
  if (log.name_ == kControlMessageCreatedName) {
    const MOQTControlMessageCreated& msg =
        std::get<MOQTControlMessageCreated>(log.data_);
    logObject["data"] = msg.toDynamic();
  }

  return logObject;
}

void MLogger::logClientSetup(const ClientSetup& setup) {
  std::vector<uint64_t> versions = setup.supportedVersions;

  // Add Params to params vector
  std::vector<MOQTParameter> params;

  for (const auto& param : setup.params) {
    MOQTParameter p;
    switch (param.key) {
      case folly::to_underlying(SetupKey::PATH):
        p.name = "path";
        p.type = MOQTParameterType::STRING;
        p.stringValue = param.asString;
        break;
      case folly::to_underlying(SetupKey::MAX_REQUEST_ID):
        p.name = "max_request_id";
        p.type = MOQTParameterType::INT;
        p.intValue = param.asUint64;
        break;
      case folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE):
        p.name = "max_auth_token_cache_size";
        p.type = MOQTParameterType::INT;
        p.intValue = param.asUint64;
        break;
      default:
        p.name = "unknown";
        // Check if string value is initialized
        if (param.asString != "") {
          p.type = MOQTParameterType::STRING;
          p.stringValue = param.asString;
        } else {
          p.type = MOQTParameterType::INT;
          p.stringValue = param.asUint64;
        }
        break;
    }
    params.push_back(p);
  }

  // Log Setup Message
  MOQTControlMessageCreated req{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::make_unique<MOQTClientSetupMessage>(createClientSetupControlMessage(
          versions.size(), versions, params.size(), params)),
      nullptr};
  addControlMessageCreatedLog(std::move(req));
}

void MLogger::outputLogsToFile() {
  std::ofstream fileObj(path_);
  for (const auto& log : logs_) {
    auto obj = formatLog(log);
    std::string jsonLog = folly::toPrettyJson(obj);
    LOG(INFO) << jsonLog;
    fileObj << jsonLog << std::endl;
  }
  fileObj.close();
}

} // namespace moxygen
