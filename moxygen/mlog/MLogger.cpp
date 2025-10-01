/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

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

void MLogger::addControlMessageParsedLog(MOQTControlMessageParsed req) {
  auto log = eventCreator_.createControlMessageParsedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addStreamTypeSetLog(MOQTStreamTypeSet req) {
  auto log =
      eventCreator_.createStreamTypeSetEvent(vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addObjectDatagramCreatedLog(MOQTObjectDatagramCreated req) {
  auto log = eventCreator_.createObjectDatagramCreatedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addObjectDatagramParsedLog(MOQTObjectDatagramParsed req) {
  auto log = eventCreator_.createObjectDatagramParsedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addObjectDatagramStatusCreatedLog(
    MOQTObjectDatagramStatusCreated req) {
  auto log = eventCreator_.createObjectDatagramStatusCreatedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addObjectDatagramStatusParsedLog(
    MOQTObjectDatagramStatusParsed req) {
  auto log = eventCreator_.createObjectDatagramStatusParsedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addSubgroupHeaderCreatedLog(MOQTSubgroupHeaderCreated req) {
  auto log = eventCreator_.createSubgroupHeaderCreatedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addSubgroupHeaderParsedLog(MOQTSubgroupHeaderParsed req) {
  auto log = eventCreator_.createSubgroupHeaderParsedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addSubgroupObjectCreatedLog(MOQTSubgroupObjectCreated req) {
  auto log = eventCreator_.createSubgroupObjectCreatedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addSubgroupObjectParsedLog(MOQTSubgroupObjectParsed req) {
  auto log = eventCreator_.createSubgroupObjectParsedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addFetchHeaderCreatedLog(MOQTFetchHeaderCreated req) {
  auto log = eventCreator_.createFetchHeaderCreatedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addFetchHeaderParsedLog(MOQTFetchHeaderParsed req) {
  auto log =
      eventCreator_.createFetchHeaderParsedEvent(vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addFetchObjectCreatedLog(MOQTFetchObjectCreated req) {
  auto log = eventCreator_.createFetchObjectCreatedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addFetchObjectParsedLog(MOQTFetchObjectParsed req) {
  auto log =
      eventCreator_.createFetchObjectParsedEvent(vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

MOQTClientSetupMessage MLogger::createClientSetupControlMessage(
    uint64_t numberOfSupportedVersions,
    std::vector<uint64_t> supportedVersions,
    uint64_t numberOfParameters,
    std::vector<MOQTParameter> params) {
  MOQTClientSetupMessage client;
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
  MOQTServerSetupMessage server;
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
  } else if (log.name_ == kControlMessageParsedName) {
    const MOQTControlMessageParsed& msg =
        std::get<MOQTControlMessageParsed>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kStreamTypeSetName) {
    const MOQTStreamTypeSet& msg = std::get<MOQTStreamTypeSet>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kObjectDatagramCreatedName) {
    const MOQTObjectDatagramCreated& msg =
        std::get<MOQTObjectDatagramCreated>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kObjectDatagramParsedName) {
    const MOQTObjectDatagramParsed& msg =
        std::get<MOQTObjectDatagramParsed>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kObjectDatagramStatusCreatedName) {
    const MOQTObjectDatagramStatusCreated& msg =
        std::get<MOQTObjectDatagramStatusCreated>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kObjectDatagramStatusParsedName) {
    const MOQTObjectDatagramStatusParsed& msg =
        std::get<MOQTObjectDatagramStatusParsed>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kSubgroupHeaderCreatedName) {
    const MOQTSubgroupHeaderCreated& msg =
        std::get<MOQTSubgroupHeaderCreated>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kSubgroupHeaderParsedName) {
    const MOQTSubgroupHeaderParsed& msg =
        std::get<MOQTSubgroupHeaderParsed>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kSubgroupObjectCreatedName) {
    const MOQTSubgroupObjectCreated& msg =
        std::get<MOQTSubgroupObjectCreated>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kSubgroupObjectParsedName) {
    const MOQTSubgroupObjectParsed& msg =
        std::get<MOQTSubgroupObjectParsed>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kFetchHeaderCreatedName) {
    const MOQTFetchHeaderCreated& msg =
        std::get<MOQTFetchHeaderCreated>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kFetchHeaderParsedName) {
    const MOQTFetchHeaderParsed& msg =
        std::get<MOQTFetchHeaderParsed>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kFetchObjectCreatedName) {
    const MOQTFetchObjectCreated& msg =
        std::get<MOQTFetchObjectCreated>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kFetchObjectParsedName) {
    const MOQTFetchObjectParsed& msg =
        std::get<MOQTFetchObjectParsed>(log.data_);
    logObject["data"] = msg.toDynamic();
  }

  return logObject;
}

void MLogger::logClientSetup(
    const ClientSetup& setup,
    ControlMessageType controlType) {
  std::vector<uint64_t> versions = setup.supportedVersions;

  // Add Params to params vector
  std::vector<MOQTParameter> params =
      convertSetupParamsToMoQTParams(setup.params);

  std::unique_ptr<MOQTBaseControlMessage> msg =
      std::make_unique<MOQTClientSetupMessage>(createClientSetupControlMessage(
          versions.size(), versions, params.size(), params));
  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(msg));
}

void MLogger::logServerSetup(
    const ServerSetup& setup,
    ControlMessageType controlType) {
  // Add Params to params vector
  std::vector<MOQTParameter> params =
      convertSetupParamsToMoQTParams(setup.params);
  std::unique_ptr<MOQTBaseControlMessage> msg =
      std::make_unique<MOQTServerSetupMessage>(createServerSetupControlMessage(
          setup.selectedVersion, params.size(), params));
  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(msg));
}

void MLogger::logGoaway(const Goaway& goaway, ControlMessageType controlType) {
  uint64_t length = goaway.newSessionUri.length();
  auto baseMsg = std::make_unique<MOQTGoaway>();
  baseMsg->length = length;
  baseMsg->newSessionUri = folly::IOBuf::copyBuffer(goaway.newSessionUri);
  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribe(
    const SubscribeRequest& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribe>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->trackAlias =
      req.trackAlias ? req.trackAlias->value : req.requestID.value;
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

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribeUpdate(
    const SubscribeUpdate& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeUpdate>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->subscribeId = req.subscriptionRequestID.value;
  baseMsg->startGroup = req.start.group;
  baseMsg->startObject = req.start.object;
  baseMsg->endGroup = req.endGroup;
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->subscriberPriority = req.priority;
  baseMsg->subscribeParameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logUnsubscribe(
    const Unsubscribe& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTUnsubscribe>();
  baseMsg->subscribeId = req.requestID.value;

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logFetch(
    const Fetch& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
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

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logFetchCancel(
    const FetchCancel& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTFetchCancel>();
  baseMsg->subscribeId = req.requestID.value;

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logAnnounceOk(
    const AnnounceOk& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTAnnounceOk>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespace.trackNamespace, type);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logAnnounceError(
    const AnnounceError& req,
    const TrackNamespace& trackNamespace,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTAnnounceError>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      trackNamespace.trackNamespace, type);
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logAnnounceCancel(
    const AnnounceCancel& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTAnnounceCancel>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespace.trackNamespace, type);
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logTrackStatus(
    const TrackStatus& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTTrackStatus>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.fullTrackName.trackNamespace.trackNamespace, type);
  baseMsg->trackName =
      convertTrackNameToByteStringFormat(req.fullTrackName.trackName, type);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribeAnnounces(
    const SubscribeAnnounces& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeAnnounces>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespacePrefix.trackNamespace, type);
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logUnsubscribeAnnounces(
    const UnsubscribeAnnounces& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTUnsubscribeAnnounces>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespacePrefix.trackNamespace, type);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribeOk(
    const SubscribeOk& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeOk>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->expires = req.expires.count();
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);

  if (req.largest.has_value()) {
    baseMsg->contentExists = 1;
    baseMsg->largestGroupId = req.largest.value().group;
    baseMsg->largestObjectId = req.largest.value().object;
  } else {
    baseMsg->contentExists = 0;
  }

  baseMsg->numberOfParameters = req.params.size();
  baseMsg->subscribeParameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribeError(
    const SubscribeError& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeError>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }
  // retryAlias removed in unified RequestError - not available for logging

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logFetchOk(const FetchOk& req, ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTFetchOk>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);
  baseMsg->endOfTrack = req.endOfTrack;
  baseMsg->largestGroupId = req.endLocation.group;
  baseMsg->largestObjectId = req.endLocation.object;
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->subscribeParameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logFetchError(
    const FetchError& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTFetchError>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribeDone(
    const SubscribeDone& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeDone>();
  baseMsg->subscribeId = req.requestID.value;
  baseMsg->statusCode = static_cast<uint64_t>(req.statusCode);
  baseMsg->streamCount = req.streamCount;

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logMaxSubscribeId(
    const uint64_t maxRequestID,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTMaxSubscribeId>();
  baseMsg->subscribeId = maxRequestID;

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logUnannounce(
    const Unannounce& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTUnannounce>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespace.trackNamespace, type);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logTrackStatusOk(
    const TrackStatusOk& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTTrackStatusOk>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->expires = req.expires.count();
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);

  if (req.largest.has_value()) {
    baseMsg->contentExists = 1;
    baseMsg->largestGroupId = req.largest.value().group;
    baseMsg->largestObjectId = req.largest.value().object;
  } else {
    baseMsg->contentExists = 0;
  }

  baseMsg->numberOfParameters = req.params.size();
  baseMsg->subscribeParameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logTrackStatusError(
    const TrackStatusError& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTTrackStatusError>();

  baseMsg->requestId = req.requestID.value;
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }
  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribesBlocked(
    const uint64_t maxRequestID,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribesBlocked>();
  baseMsg->maximumSubscribeId = maxRequestID;

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logAnnounce(
    const Announce& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTAnnounce>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespace.trackNamespace, type);
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribeAnnouncesOk(
    const SubscribeAnnouncesOk& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeAnnouncesOk>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespacePrefix.trackNamespace, type);

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
}

void MLogger::logSubscribeAnnouncesError(
    const SubscribeAnnouncesError& req,
    const TrackNamespace& trackNamespace,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeAnnouncesError>();
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      trackNamespace.trackNamespace, type);
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, folly::none, std::move(baseMsg));
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

void MLogger::logStreamTypeSet(
    uint64_t streamId,
    MOQTStreamType type,
    folly::Optional<Owner> owner) {
  MOQTStreamTypeSet baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.streamType = type;
  baseMsg.owner = owner.has_value() ? owner.value() : Owner::LOCAL;
  addStreamTypeSetLog(baseMsg);
}

void MLogger::logObjectDatagramCreated(
    TrackAlias trackAlias,
    const ObjectHeader& header,
    const Payload& payload) {
  MOQTObjectDatagramCreated baseMsg;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = header.group;
  baseMsg.objectId = header.id;
  baseMsg.publisherPriority = header.priority;
  baseMsg.extensionHeadersLength = header.extensions.size();
  baseMsg.extensionHeaders =
      convertExtensionToMoQTExtensionHeaders(header.extensions);
  baseMsg.objectPayload = payload->clone();
  addObjectDatagramCreatedLog(std::move(baseMsg));
}

void MLogger::logObjectDatagramParsed(
    TrackAlias trackAlias,
    const ObjectHeader& header,
    const Payload& payload) {
  MOQTObjectDatagramParsed baseMsg;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = header.group;
  baseMsg.objectId = header.id;
  baseMsg.publisherPriority = header.priority;
  baseMsg.extensionHeadersLength = header.extensions.size();
  baseMsg.extensionHeaders =
      convertExtensionToMoQTExtensionHeaders(header.extensions);
  std::unique_ptr<folly::IOBuf> objPayload =
      folly::IOBuf::copyBuffer({payload->data(), payload->length()});
  baseMsg.objectPayload = std::move(objPayload);
  addObjectDatagramParsedLog(std::move(baseMsg));
}

void MLogger::logObjectDatagramStatusCreated(
    TrackAlias trackAlias,
    const ObjectHeader& header) {
  MOQTObjectDatagramStatusCreated baseMsg;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = header.group;
  baseMsg.objectId = header.id;
  baseMsg.publisherPriority = header.priority;
  baseMsg.extensionHeadersLength = header.extensions.size();
  baseMsg.extensionHeaders =
      convertExtensionToMoQTExtensionHeaders(header.extensions);
  baseMsg.objectStatus = static_cast<uint64_t>(header.status);
  addObjectDatagramStatusCreatedLog(std::move(baseMsg));
}

void MLogger::logObjectDatagramStatusParsed(
    TrackAlias trackAlias,
    const ObjectHeader& header) {
  MOQTObjectDatagramStatusParsed baseMsg;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = header.group;
  baseMsg.objectId = header.id;
  baseMsg.publisherPriority = header.priority;
  baseMsg.extensionHeadersLength = header.extensions.size();
  baseMsg.extensionHeaders =
      convertExtensionToMoQTExtensionHeaders(header.extensions);
  baseMsg.objectStatus = static_cast<uint64_t>(header.status);
  addObjectDatagramStatusParsedLog(std::move(baseMsg));
}

void MLogger::logSubgroupHeaderCreated(
    uint64_t streamId,
    TrackAlias trackAlias,
    uint64_t groupId,
    uint64_t sugroupId,
    uint8_t publisherPriority) {
  MOQTSubgroupHeaderCreated baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = groupId;
  baseMsg.subgroupId = sugroupId;
  baseMsg.publisherPriority = publisherPriority;
  addSubgroupHeaderCreatedLog(std::move(baseMsg));
}

void MLogger::logSubgroupHeaderParsed(
    uint64_t streamId,
    TrackAlias trackAlias,
    uint64_t groupId,
    uint64_t sugroupId,
    uint8_t publisherPriority) {
  MOQTSubgroupHeaderParsed baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = groupId;
  baseMsg.subgroupId = sugroupId;
  baseMsg.publisherPriority = publisherPriority;
  addSubgroupHeaderParsedLog(std::move(baseMsg));
}

void MLogger::logSubgroupObjectCreated(
    uint64_t streamId,
    TrackAlias trackAlias,
    const ObjectHeader& objHeader,
    Payload payload) {
  MOQTSubgroupObjectCreated baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.groupId = objHeader.group;
  baseMsg.subgroupId = objHeader.id;
  baseMsg.objectId = objHeader.id;
  baseMsg.extensionHeadersLength = objHeader.extensions.size();
  baseMsg.extensionHeaders =
      convertExtensionToMoQTExtensionHeaders(objHeader.extensions);
  baseMsg.objectPayloadLength = payload->length();
  baseMsg.objectStatus = static_cast<uint64_t>(objHeader.status);
  baseMsg.objectPayload = payload->clone();
  addSubgroupObjectCreatedLog(std::move(baseMsg));
}

void MLogger::logSubgroupObjectParsed(
    uint64_t streamId,
    TrackAlias trackAlias,
    const ObjectHeader& objHeader,
    Payload payload) {
  MOQTSubgroupObjectParsed baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.groupId = objHeader.group;
  baseMsg.subgroupId = objHeader.id;
  baseMsg.objectId = objHeader.id;
  baseMsg.extensionHeadersLength = objHeader.extensions.size();
  baseMsg.extensionHeaders =
      convertExtensionToMoQTExtensionHeaders(objHeader.extensions);
  baseMsg.objectPayloadLength = payload->length();
  baseMsg.objectStatus = static_cast<uint64_t>(objHeader.status);
  baseMsg.objectPayload = payload->clone();
  addSubgroupObjectParsedLog(std::move(baseMsg));
}

void MLogger::logFetchHeaderCreated(
    const uint64_t streamId,
    const uint64_t subscribeId) {
  MOQTFetchHeaderCreated baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.subscribeId = subscribeId;
  addFetchHeaderCreatedLog(std::move(baseMsg));
}

void MLogger::logFetchHeaderParsed(
    const uint64_t streamId,
    const uint64_t subscribeId) {
  MOQTFetchHeaderParsed baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.subscribeId = subscribeId;
  addFetchHeaderParsedLog(std::move(baseMsg));
}

void MLogger::logFetchObjectCreated(
    const uint64_t streamId,
    const ObjectHeader& objHeader,
    Payload payload) {
  MOQTFetchObjectCreated baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.groupId = objHeader.group;
  baseMsg.subgroupId = objHeader.id;
  baseMsg.objectId = objHeader.id;
  baseMsg.publisherPriority = objHeader.priority;
  baseMsg.extensionHeadersLength = objHeader.extensions.size();
  baseMsg.extensionHeaders =
      convertExtensionToMoQTExtensionHeaders(objHeader.extensions);
  baseMsg.objectPayloadLength = payload->length();
  baseMsg.objectStatus = static_cast<uint64_t>(objHeader.status);
  baseMsg.objectPayload = payload->clone();
  addFetchObjectCreatedLog(std::move(baseMsg));
}

void MLogger::logFetchObjectParsed(
    const uint64_t streamId,
    const ObjectHeader& objHeader,
    Payload payload) {
  MOQTFetchObjectParsed baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.groupId = objHeader.group;
  baseMsg.subgroupId = objHeader.id;
  baseMsg.objectId = objHeader.id;
  baseMsg.publisherPriority = objHeader.priority;
  baseMsg.extensionHeadersLength = objHeader.extensions.size();
  baseMsg.extensionHeaders =
      convertExtensionToMoQTExtensionHeaders(objHeader.extensions);
  baseMsg.objectPayloadLength = payload->length();
  baseMsg.objectStatus = static_cast<uint64_t>(objHeader.status);
  baseMsg.objectPayload = payload->clone();
  addFetchObjectParsedLog(std::move(baseMsg));
}

bool MLogger::isHexstring(const std::string& s) {
  for (char c : s) {
    if (!std::isxdigit(static_cast<unsigned char>(c))) {
      return false;
    }
  }
  return true;
}

std::vector<MOQTExtensionHeader>
MLogger::convertExtensionToMoQTExtensionHeaders(
    std::vector<Extension> extensions) {
  std::vector<MOQTExtensionHeader> moqExtensions;
  for (auto& ext : extensions) {
    MOQTExtensionHeader e;
    e.headerType = ext.type;
    if (ext.type % 2 == 1) {
      e.headerLength = ext.arrayValue->length();
      std::unique_ptr<folly::IOBuf> arrayPayload = folly::IOBuf::copyBuffer(
          {ext.arrayValue->data(), ext.arrayValue->length()});
      e.payload = std::move(arrayPayload);
    } else {
      e.headerValue = ext.intValue;
    }
    moqExtensions.push_back(std::move(e));
  }
  return moqExtensions;
}

void MLogger::logControlMessage(
    ControlMessageType controlType,
    uint64_t streamId,
    const folly::Optional<uint64_t>& length,
    std::unique_ptr<MOQTBaseControlMessage> message,
    std::unique_ptr<folly::IOBuf> raw) {
  switch (controlType) {
    case ControlMessageType::CREATED: {
      MOQTControlMessageCreated req{
          kFirstBidiStreamId,
          length,
          std::move(message),
          (raw) ? std::move(raw) : nullptr};
      addControlMessageCreatedLog(std::move(req));
      break;
    }
    case ControlMessageType::PARSED: {
      MOQTControlMessageParsed req{
          kFirstBidiStreamId,
          length,
          std::move(message),
          (raw) ? std::move(raw) : nullptr};
      addControlMessageParsedLog(std::move(req));
      break;
    }
    default: {
      break;
    }
  }
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
