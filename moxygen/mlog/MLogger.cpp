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

MOQTServerSetupMessage MLogger::createServerSetupControlMessage(
    uint64_t selectedVersion,
    uint64_t number_of_parameters,
    std::vector<MOQTParameter> params) {
  MOQTServerSetupMessage server = MOQTServerSetupMessage();
  server.selectedVersion = selectedVersion;
  server.numberOfParameters = number_of_parameters;
  server.setupParameters = std::move(params);
  return server;
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
  std::vector<MOQTParameter> params =
      convertSetupParamsToMoQTParams(setup.params);

  // Log Setup Message
  MOQTControlMessageCreated req{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::make_unique<MOQTClientSetupMessage>(createClientSetupControlMessage(
          versions.size(), versions, params.size(), params)),
      nullptr};
  addControlMessageCreatedLog(std::move(req));
}

void MLogger::logServerSetup(const ServerSetup& setup) {
  // Add Params to params vector
  std::vector<MOQTParameter> params =
      convertSetupParamsToMoQTParams(setup.params);

  // Log Setup Message
  MOQTControlMessageCreated req{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::make_unique<MOQTServerSetupMessage>(createServerSetupControlMessage(
          setup.selectedVersion, params.size(), params)),
      nullptr};
  addControlMessageCreatedLog(std::move(req));
}

void MLogger::logGoaway(const Goaway& goaway) {
  uint64_t length = goaway.newSessionUri.length();
  auto baseMsg = std::make_unique<MOQTGoaway>();
  baseMsg->length = length;
  baseMsg->newSessionUri = folly::IOBuf::copyBuffer(goaway.newSessionUri);
  MOQTControlMessageCreated req{
      kFirstBidiStreamId, folly::none, std::move(baseMsg), nullptr};
  addControlMessageCreatedLog(std::move(req));
}

void MLogger::logSubscribe(
    const SubscribeRequest& req,
    const MOQTByteStringType& type) {
  auto baseMsg = std::make_unique<MOQTSubscribe>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->trackAlias = req.trackAlias.value;
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.fullTrackName.trackNamespace.trackNamespace, type);
  baseMsg->trackName =
      convertTrackNameToByteStringFormat(req.fullTrackName.trackName, type);
  baseMsg->subscriberPriority = req.priority;
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);
  baseMsg->filterType = static_cast<uint8_t>(req.locType);
  if (req.start.hasValue()) {
    baseMsg->startGroup = req.start.value().group;
    baseMsg->startObject = req.start.value().object;
  }
  baseMsg->endGroup = req.endGroup;
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->subscribeParameters = convertTrackParamsToMoQTParams(req.params);

  // Add the message to the logs
  MOQTControlMessageCreated msgCreated{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msgCreated));
}

void MLogger::logSubscribeUpdate(const SubscribeUpdate& req) {
  auto baseMsg = std::make_unique<MOQTSubscribeUpdate>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->startGroup = req.start.group;
  baseMsg->startObject = req.start.object;
  baseMsg->endGroup = req.endGroup;
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->subscriberPriority = req.priority;
  baseMsg->subscribeParameters = convertTrackParamsToMoQTParams(req.params);

  // Add the message to the logs
  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logUnsubscribe(const Unsubscribe& req) {
  auto baseMsg = std::make_unique<MOQTUnsubscribe>();
  baseMsg->subscribeId = req.requestID.value;

  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logFetch(const Fetch& req, const MOQTByteStringType& type) {
  auto baseMsg = std::make_unique<MOQTFetch>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->subscriberPriority = req.priority;
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);

  auto [standalone, joining] = fetchType(req);
  if (joining) {
    baseMsg->fetchType = static_cast<uint64_t>(joining->fetchType);
    baseMsg->joiningSubscribeId =
        std::get<JoiningFetch>(req.args).joiningRequestID.value;
    baseMsg->precedingGroupOffset =
        std::get<JoiningFetch>(req.args).joiningStart;
  } else if (standalone) {
    baseMsg->fetchType = static_cast<uint64_t>(FetchType::STANDALONE);
    baseMsg->startGroup = std::get<StandaloneFetch>(req.args).start.group;
    baseMsg->startObject = std::get<StandaloneFetch>(req.args).start.object;
    baseMsg->endGroup = std::get<StandaloneFetch>(req.args).end.group;
    baseMsg->endObject = std::get<StandaloneFetch>(req.args).end.object;
  }

  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.fullTrackName.trackNamespace.trackNamespace, type);
  if (req.fullTrackName.trackName != "") {
    baseMsg->trackName =
        convertTrackNameToByteStringFormat(req.fullTrackName.trackName);
  }

  baseMsg->numberOfParameters = req.params.size();
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);

  // Add the message to the logs
  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logFetchCancel(const FetchCancel& req) {
  auto baseMsg = std::make_unique<MOQTFetchCancel>();
  baseMsg->subscribeId = req.requestID.value;

  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logAnnounceOk(
    const AnnounceOk& req,
    const MOQTByteStringType& type) {
  auto baseMsg = std::make_unique<MOQTAnnounceOk>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespace.trackNamespace, type);

  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logAnnounceError(
    const AnnounceError& req,
    const MOQTByteStringType& type) {
  auto baseMsg = std::make_unique<MOQTAnnounceError>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespace.trackNamespace, type);
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }

  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logAnnounceCancel(
    const AnnounceCancel& req,
    const MOQTByteStringType& type) {
  auto baseMsg = std::make_unique<MOQTAnnounceCancel>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespace.trackNamespace, type);
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }

  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logTrackStatusRequest(
    const TrackStatusRequest& req,
    const MOQTByteStringType& type) {
  auto baseMsg = std::make_unique<MOQTTrackStatusRequest>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.fullTrackName.trackNamespace.trackNamespace, type);
  baseMsg->trackName =
      convertTrackNameToByteStringFormat(req.fullTrackName.trackName, type);

  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logSubscribeAnnounces(
    const SubscribeAnnounces& req,
    const MOQTByteStringType& type) {
  auto baseMsg = std::make_unique<MOQTSubscribeAnnounces>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespacePrefix.trackNamespace, type);
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);

  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

void MLogger::logUnsubscribeAnnounces(
    const UnsubscribeAnnounces& req,
    const MOQTByteStringType& type) {
  auto baseMsg = std::make_unique<MOQTUnsubscribeAnnounces>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespacePrefix.trackNamespace, type);

  MOQTControlMessageCreated msg{
      kFirstBidiStreamId,
      folly::none /* length */,
      std::move(baseMsg),
      nullptr};
  addControlMessageCreatedLog(std::move(msg));
}

std::vector<MOQTParameter> MLogger::convertSetupParamsToMoQTParams(
    const std::vector<SetupParameter>& params) {
  // Add Params to params vector
  std::vector<MOQTParameter> moqParams;

  for (const auto& param : params) {
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
        if (param.key % 2) {
          p.type = MOQTParameterType::STRING;
          p.stringValue = param.asString;
        } else {
          p.type = MOQTParameterType::INT;
          p.stringValue = param.asUint64;
        }
        break;
    }
    moqParams.push_back(p);
  }
  return moqParams;
}

std::vector<MOQTParameter> MLogger::convertTrackParamsToMoQTParams(
    const std::vector<TrackRequestParameter>& params) {
  std::vector<MOQTParameter> moqParams;
  for (const auto& param : params) {
    MOQTParameter p;
    p.name = "unknown"; // No TrackParamKeys (like SetupKey) so default to
                        // unknown for now
    if (param.key % 2) {
      p.type = MOQTParameterType::STRING;
      p.stringValue = param.asString;
    } else {
      p.type = MOQTParameterType::INT;
      p.intValue = param.asUint64;
    }
  }
  return moqParams;
}

std::vector<MOQTByteString> MLogger::convertTrackNamespaceToByteStringFormat(
    const std::vector<std::string>& ns,
    const MOQTByteStringType& type) {
  std::vector<MOQTByteString> track;
  for (auto& t : ns) {
    MOQTByteString str;
    str.type = type;
    if (type == MOQTByteStringType::STRING_VALUE) {
      str.value = t;
      track.push_back(std::move(str));
    } else {
      str.valueBytes = folly::IOBuf::copyBuffer(t);
      track.push_back(std::move(str));
    }
  }
  return track;
}

MOQTByteString MLogger::convertTrackNameToByteStringFormat(
    const std::string& t,
    const MOQTByteStringType& type) {
  MOQTByteString str;
  str.type = type;
  if (type == MOQTByteStringType::STRING_VALUE) {
    str.value = t;
  } else {
    str.valueBytes = folly::IOBuf::copyBuffer(t);
  }
  return str;
}

bool MLogger::isHexstring(const std::string& s) {
  for (char c : s) {
    if (!std::isxdigit(static_cast<unsigned char>(c))) {
      return false;
    }
  }
  return true;
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
