/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/mlog/MLogger.h"
#include <array>
#include <iterator>
#include <limits>
#include <utility>
#include "moxygen/MoQFramer.h"
#include "moxygen/MoQVarint.h"

namespace moxygen {

namespace {
uint8_t toLogGroupOrder(std::optional<GroupOrder> groupOrder) {
  return groupOrder ? folly::to_underlying(*groupOrder) : 0;
}

folly::dynamic namedValue(std::string_view name, folly::dynamic value) {
  folly::dynamic fields = folly::dynamic::object;
  fields["name"] = std::string(name);
  fields["value"] = std::move(value);
  return fields;
}

folly::dynamic rawInfoForLength(uint64_t length) {
  return folly::dynamic::object("length", length);
}

folly::dynamic
unknownValue(uint64_t nameBytes, const std::string& bytes, uint64_t integer) {
  folly::dynamic fields = folly::dynamic::object;
  fields["name"] = "unknown";
  fields["name_bytes"] = nameBytes;
  if (nameBytes % 2 == 0) {
    fields["value"] = integer;
  } else {
    fields["length"] = bytes.size();
    fields["value_bytes"] = rawInfoForLength(bytes.size());
  }
  return fields;
}

folly::dynamic authorizationToken(const AuthToken& token) {
  folly::dynamic fields = folly::dynamic::object;
  fields["name"] = "authorization_token";
  if (token.alias.has_value()) {
    fields["alias_type"] = token.tokenValue.empty() ? "use_alias" : "register";
    fields["token_alias"] = token.alias.value();
  } else {
    fields["alias_type"] = "use_value";
  }
  if (!token.tokenValue.empty()) {
    fields["token_type"] = token.tokenType;
    fields["token_value"] = rawInfoForLength(token.tokenValue.size());
  }
  return fields;
}

folly::dynamic locationFilter(const LocationFilter& filter) {
  folly::dynamic value = folly::dynamic::object;
  const auto fields = filter.fields();
  const auto count = filter.fieldCount();
  if (count >= 1) {
    value["start_group"] = fields[0];
  }
  if (count >= 2) {
    value["start_object"] = fields[1];
  }
  if (count >= 3) {
    value["end_group_delta"] = fields[2];
  }
  if (count >= 4) {
    value["end_object"] = fields[3];
  }
  return value;
}

std::optional<folly::dynamic> rangeFilter(const Parameter& parameter);

folly::dynamic fillParametersValue(
    const FillParameters& fill,
    const std::vector<Parameter>& fillRangeFilters = {}) {
  std::vector<folly::dynamic> parameters;
  if (fill.fillTimeout.has_value()) {
    parameters.push_back(namedValue(
        "fill_timeout", static_cast<uint64_t>(fill.fillTimeout->count())));
  }
  if (fill.subscriberPriority.has_value()) {
    parameters.push_back(
        namedValue("subscriber_priority", fill.subscriberPriority.value()));
  }
  if (fill.locationFilter.has_value()) {
    parameters.push_back(namedValue(
        "location_filter", locationFilter(fill.locationFilter.value())));
  }
  if (fill.groupOrder.has_value()) {
    parameters.push_back(namedValue(
        "group_order", static_cast<uint64_t>(fill.groupOrder.value())));
  }
  for (const auto& filter : fillRangeFilters) {
    XCHECK_GE(filter.key, 0x25);
    XCHECK_LE(filter.key, 0x28);
    auto value = rangeFilter(filter);
    parameters.push_back(
        value.has_value()
            ? std::move(value.value())
            : unknownValue(filter.key, filter.asString, filter.asUint64));
  }
  return folly::dynamic::array(parameters);
}
std::optional<folly::dynamic> rangeFilter(const Parameter& parameter) {
  static constexpr std::array<const char*, 5> kNames = {
      "subgroup_filter",
      "objectid_filter",
      "priority_filter",
      "object_property_filter",
      "track_property_filter"};
  XCHECK_GE(parameter.key, 0x25);
  XCHECK_LE(parameter.key, 0x29);

  folly::dynamic filter = folly::dynamic::object;
  filter["name"] = kNames[parameter.key - 0x25];
  if (parameter.asString.empty()) {
    return filter;
  }

  auto data = folly::IOBuf::copyBuffer(parameter.asString);
  folly::io::Cursor cursor(data.get());
  uint64_t remaining = parameter.asString.size();
  filter["set_id"] = cursor.readBE<uint8_t>();
  --remaining;

  auto readVarint = [&]() -> std::optional<uint64_t> {
    auto decoded = decodeMoQVarint(cursor, remaining);
    if (!decoded.has_value()) {
      return std::nullopt;
    }
    remaining -= decoded->second;
    return decoded->first;
  };

  const bool propertyFilter = parameter.key >= 0x28;
  if (propertyFilter && remaining > 0) {
    auto propertyType = readVarint();
    if (!propertyType.has_value()) {
      return std::nullopt;
    }
    filter["property_type"] = propertyType.value();
  }

  std::vector<folly::dynamic> ranges;
  uint64_t previousEnd = 0;
  while (remaining > 0) {
    auto startDelta = readVarint();
    if (!startDelta.has_value() ||
        startDelta.value() >
            std::numeric_limits<uint64_t>::max() - previousEnd) {
      return std::nullopt;
    }
    const uint64_t start = previousEnd + startDelta.value();
    folly::dynamic range = folly::dynamic::object("start", start);
    if (remaining > 0) {
      auto endDelta = readVarint();
      if (!endDelta.has_value() ||
          endDelta.value() > std::numeric_limits<uint64_t>::max() - start) {
        return std::nullopt;
      }
      const uint64_t end = start + endDelta.value();
      range["end"] = end;
      previousEnd = end;
    }
    ranges.push_back(std::move(range));
  }
  if (!ranges.empty()) {
    filter["ranges"] = folly::dynamic::array(ranges);
  }
  return filter;
}

template <typename Event>
void populateFetchRangeEnd(
    Event& event,
    uint64_t streamId,
    uint64_t endGroup,
    uint64_t endObject,
    MOQTFetchRangeEndType rangeEndType) {
  event.streamId = streamId;
  // An End of Range carries an absolute endpoint. These are the MLog FETCH
  // object's only fields for the group and object values.
  event.groupIdDelta = endGroup;
  event.objectIdDelta = endObject;
  event.endOfNonexistentRange =
      rangeEndType == MOQTFetchRangeEndType::NONEXISTENT;
  event.endOfUnknownRange = rangeEndType == MOQTFetchRangeEndType::UNKNOWN;
  event.endOfTimedOutRange = rangeEndType == MOQTFetchRangeEndType::TIMED_OUT;
}
} // namespace

void MLogger::setDcid(const quic::ConnectionId& dcid) {
  dcid_ = dcid;
}

void MLogger::setSrcCid(const quic::ConnectionId& srcCid) {
  srcCid_ = srcCid;
}

void MLogger::setPeerAddress(const folly::SocketAddress& peerAddress) {
  peerAddress_ = peerAddress;
}

void MLogger::setLocalAddress(const folly::SocketAddress& localAddress) {
  localAddress_ = localAddress;
}

void MLogger::setNegotiatedMoQVersion(uint64_t version) {
  negotiatedMoQVersion_ = version;
}

void MLogger::setExperiments(const std::vector<std::string>& experiments) {
  experiments_ = experiments;
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

void MLogger::addPaddingDatagramCreatedLog(MOQTPaddingDatagramCreated req) {
  auto log = eventCreator_.createPaddingDatagramCreatedEvent(
      vantagePoint_, std::move(req));
  logs_.push_back(std::move(log));
}

void MLogger::addPaddingDatagramParsedLog(MOQTPaddingDatagramParsed req) {
  auto log = eventCreator_.createPaddingDatagramParsedEvent(
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

MOQTSetupMessage MLogger::createSetupControlMessage(
    const std::string& setupType,
    uint64_t /*numberOfParameters*/,
    std::vector<MOQTSetupParameter> params) {
  MOQTSetupMessage msg(setupType);
  msg.setupOptions = std::move(params);
  return msg;
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
  } else if (log.name_ == kPaddingDatagramCreatedName) {
    const MOQTPaddingDatagramCreated& msg =
        std::get<MOQTPaddingDatagramCreated>(log.data_);
    logObject["data"] = msg.toDynamic();
  } else if (log.name_ == kPaddingDatagramParsedName) {
    const MOQTPaddingDatagramParsed& msg =
        std::get<MOQTPaddingDatagramParsed>(log.data_);
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
    const Setup& setup,
    uint64_t version,
    ControlMessageType controlType) {
  negotiatedMoQVersion_ = version;
  // Add Params to params vector
  std::vector<MOQTSetupParameter> params =
      convertSetupParamsToMoQTSetupParams(setup.params);

  auto msg = std::make_unique<MOQTSetupMessage>("client_setup");
  msg->setupOptions = std::move(params);

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(msg));
}

void MLogger::logServerSetup(
    const Setup& setup,
    uint64_t version,
    ControlMessageType controlType) {
  negotiatedMoQVersion_ = version;
  // Add Params to params vector
  std::vector<MOQTSetupParameter> params =
      convertSetupParamsToMoQTSetupParams(setup.params);

  auto msg = std::make_unique<MOQTSetupMessage>("server_setup");
  msg->setupOptions = std::move(params);

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(msg));
}

void MLogger::logGoaway(const Goaway& goaway, ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTGoaway>();
  baseMsg->newSessionUri.length = goaway.newSessionUri.length();
  baseMsg->timeout = goaway.timeout;
  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logSubscribe(
    const SubscribeRequest& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  logSubscribe(req, std::vector<Parameter>{}, type, controlType);
}

void MLogger::logSubscribe(
    const SubscribeRequest& req,
    const std::vector<Parameter>& fillRangeFilters,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribe>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.fullTrackName.trackNamespace.trackNamespace, type);
  baseMsg->trackName =
      convertTrackNameToByteStringFormat(req.fullTrackName.trackName, type);
  baseMsg->subscriberPriority = req.priority;
  baseMsg->groupOrder = toLogGroupOrder(req.groupOrder);
  auto legacyFilter = toSubscriptionFilter(req.filter);
  baseMsg->filterType = static_cast<uint8_t>(legacyFilter.filterType);
  if (legacyFilter.location.has_value()) {
    MOQTLocation loc;
    loc.group = legacyFilter.location->group;
    loc.object = legacyFilter.location->object;
    baseMsg->startLocation = loc;
  }
  baseMsg->endGroup = legacyFilter.endGroup.value_or(0);
  baseMsg->subscribeParameters = convertTrackParamsToMoQTParams(req.params);
  baseMsg->subscribeParameters.emplace_back(
      namedValue("location_filter", locationFilter(req.filter)));
  if (req.fill.has_value() || !fillRangeFilters.empty()) {
    baseMsg->subscribeParameters.emplace_back(namedValue(
        "fill_parameters",
        fillParametersValue(
            req.fill.value_or(FillParameters{}), fillRangeFilters)));
  }
  if (req.priority != kDefaultPriority) {
    baseMsg->subscribeParameters.emplace_back(
        namedValue("subscriber_priority", req.priority));
  }
  if (req.groupOrder.has_value()) {
    baseMsg->subscribeParameters.emplace_back(
        namedValue("group_order", folly::to_underlying(*req.groupOrder)));
  }
  if (!req.forward) {
    baseMsg->subscribeParameters.emplace_back(namedValue("forward", 0));
  }
  if (!req.includeProperties) {
    baseMsg->subscribeParameters.emplace_back(
        namedValue("include_properties", 0));
  }
  baseMsg->numberOfParameters = baseMsg->subscribeParameters.size();

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logSubscribeUpdate(
    const SubscribeUpdate& req,
    ControlMessageType controlType) {
  logSubscribeUpdate(req, std::vector<Parameter>{}, controlType);
}

void MLogger::logSubscribeUpdate(
    const SubscribeUpdate& req,
    const std::vector<Parameter>& fillRangeFilters,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeUpdate>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->subscriptionRequestId = req.existingRequestID.value;
  if (req.filter.has_value()) {
    if (auto start = req.filter->absoluteStartLocation()) {
      baseMsg->startLocation.group = start->group;
      baseMsg->startLocation.object = start->object;
    }
    if (auto endGroup = req.filter->legacyEndGroup()) {
      baseMsg->endGroup = *endGroup;
    }
  }
  if (req.priority.has_value()) {
    baseMsg->subscriberPriority = req.priority.value();
  }
  baseMsg->forward = req.forward.value_or(false) ? 1 : 0;
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);
  if (req.filter.has_value()) {
    baseMsg->parameters.emplace_back(
        namedValue("location_filter", locationFilter(req.filter.value())));
  }
  if (req.fill.has_value() || !fillRangeFilters.empty()) {
    baseMsg->parameters.emplace_back(namedValue(
        "fill_parameters",
        fillParametersValue(
            req.fill.value_or(FillParameters{}), fillRangeFilters)));
  }
  if (req.priority.has_value()) {
    baseMsg->parameters.emplace_back(
        namedValue("subscriber_priority", req.priority.value()));
  }
  if (req.forward.has_value()) {
    baseMsg->parameters.emplace_back(
        namedValue("forward", req.forward.value() ? 1 : 0));
  }
  baseMsg->numberOfParameters = baseMsg->parameters.size();

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logRequestOk(
    const RequestOk& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTRequestOk>();
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);
  for (const auto& parameter : req.requestSpecificParams) {
    TrackRequestParameters extra(req.params.getFrameType());
    if (negotiatedMoQVersion_.has_value()) {
      extra.setMajorVersion(
          getDraftMajorVersion(negotiatedMoQVersion_.value()));
    }
    if (extra.insertParam(parameter).hasValue()) {
      auto converted = convertTrackParamsToMoQTParams(extra);
      baseMsg->parameters.insert(
          baseMsg->parameters.end(),
          std::make_move_iterator(converted.begin()),
          std::make_move_iterator(converted.end()));
    } else {
      baseMsg->parameters.emplace_back(
          unknownValue(parameter.key, parameter.asString, parameter.asUint64));
    }
  }
  baseMsg->numberOfParameters = baseMsg->parameters.size();
  baseMsg->trackProperties = convertExtensionsToProperties(req.trackProperties);
  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logRequestError(
    const RequestError& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTRequestError>();
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);
  baseMsg->retryInterval = req.retryInterval.has_value()
      ? static_cast<uint64_t>(req.retryInterval->count())
      : 0;
  if (!req.reasonPhrase.empty()) {
    baseMsg->reasonPhrase = req.reasonPhrase;
  }
  if (req.redirect.has_value()) {
    MOQTRedirect redirect;
    redirect.connectUri.length = req.redirect->connectUri.length();
    redirect.trackNamespace = convertTrackNamespaceToByteStringFormat(
        req.redirect->fullTrackName.trackNamespace.trackNamespace);
    redirect.trackName = convertTrackNameToByteStringFormat(
        req.redirect->fullTrackName.trackName);
    baseMsg->redirect = std::move(redirect);
  }
  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logPublishStateNotify(
    const TrackRequestParameters& params,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTPublishStateNotify>();
  baseMsg->parameters = convertTrackParamsToMoQTParams(params);
  baseMsg->numberOfParameters = baseMsg->parameters.size();
  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logUnsubscribe(
    const Unsubscribe& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTUnsubscribe>();
  baseMsg->subscribeId = req.requestID.value;

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logFetch(
    const Fetch& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTFetch>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.fullTrackName.trackNamespace.trackNamespace, type);
  baseMsg->trackName =
      convertTrackNameToByteStringFormat(req.fullTrackName.trackName, type);
  baseMsg->subscriberPriority = req.priority;
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);

  auto [standalone, joining] = fetchType(req);
  if (joining) {
    if (joining->fetchType == FetchType::RELATIVE_JOINING) {
      baseMsg->fetchType = "relative_joining";
    } else {
      baseMsg->fetchType = "absolute_joining";
    }
    MOQTJoiningFetch joiningFetchMsg;
    const auto& joiningReqID =
        std::get<JoiningFetch>(req.args).joiningRequestID;
    joiningFetchMsg.joiningRequestId = joiningReqID ? joiningReqID->value : 0;
    joiningFetchMsg.joiningStart =
        std::get<JoiningFetch>(req.args).joiningStart;
    baseMsg->joiningFetch = std::move(joiningFetchMsg);
  } else if (standalone) {
    baseMsg->fetchType = "standalone";
    MOQTStandaloneFetch standaloneFetchMsg;
    standaloneFetchMsg.trackNamespace = convertTrackNamespaceToByteStringFormat(
        req.fullTrackName.trackNamespace.trackNamespace, type);
    standaloneFetchMsg.trackName =
        convertTrackNameToByteStringFormat(req.fullTrackName.trackName);
    standaloneFetchMsg.startLocation.group =
        std::get<StandaloneFetch>(req.args).start.group;
    standaloneFetchMsg.startLocation.object =
        std::get<StandaloneFetch>(req.args).start.object;
    standaloneFetchMsg.endLocation.group =
        std::get<StandaloneFetch>(req.args).end.group;
    standaloneFetchMsg.endLocation.object =
        std::get<StandaloneFetch>(req.args).end.object;
    baseMsg->standaloneFetch = std::move(standaloneFetchMsg);
  }

  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);
  if (standalone) {
    baseMsg->parameters.emplace_back(namedValue(
        "location_filter",
        locationFilter(
            req.filter == LocationFilter::none() ? toLocationFilter(*standalone)
                                                 : req.filter)));
  }
  if (req.priority != kDefaultPriority) {
    baseMsg->parameters.emplace_back(
        namedValue("subscriber_priority", req.priority));
  }
  if (req.groupOrder != GroupOrder::OldestFirst) {
    baseMsg->parameters.emplace_back(
        namedValue("group_order", folly::to_underlying(req.groupOrder)));
  }
  if (!req.includeProperties) {
    baseMsg->parameters.emplace_back(namedValue("include_properties", 0));
  }
  baseMsg->numberOfParameters = baseMsg->parameters.size();

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logFetchCancel(
    const FetchCancel& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTFetchCancel>();
  baseMsg->requestId = req.requestID.value;

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logPublishNamespaceOk(
    const PublishNamespaceOk& req,
    ControlMessageType controlType) {
  logRequestOk(req, controlType);
}

void MLogger::logPublishNamespaceError(
    const PublishNamespaceError& req,
    ControlMessageType controlType) {
  logRequestError(req, controlType);
}

void MLogger::logPublishNamespaceCancel(
    const PublishNamespaceCancel& req,
    const MOQTByteStringType& /*type*/,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTPublishNamespaceCancel>();
  if (req.requestID.has_value()) {
    baseMsg->requestId = req.requestID->value;
  }
  baseMsg->errorCode = static_cast<uint64_t>(req.errorCode);

  if (isHexstring(req.reasonPhrase)) {
    baseMsg->reasonBytes = req.reasonPhrase;
  } else {
    baseMsg->reason = req.reasonPhrase;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logTrackStatus(
    const TrackStatus& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTTrackStatus>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.fullTrackName.trackNamespace.trackNamespace, type);
  baseMsg->trackName =
      convertTrackNameToByteStringFormat(req.fullTrackName.trackName, type);
  baseMsg->subscriberPriority = req.priority;
  baseMsg->groupOrder = toLogGroupOrder(req.groupOrder);
  baseMsg->forward = req.forward;
  baseMsg->filterType = 0;
  if (auto start = req.filter.absoluteStartLocation()) {
    MOQTLocation loc;
    loc.group = start->group;
    loc.object = start->object;
    baseMsg->startLocation = loc;
  }
  if (auto endGroup = req.filter.legacyEndGroup()) {
    baseMsg->endGroup = *endGroup;
  }
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logSubscribeNamespace(
    const SubscribeNamespace& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeNamespace>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->trackNamespacePrefix = convertTrackNamespaceToByteStringFormat(
      req.trackNamespacePrefix.trackNamespace, type);
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logSubscribeTracks(
    const SubscribeTracks& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeTracks>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->trackNamespacePrefix = convertTrackNamespaceToByteStringFormat(
      req.trackNamespacePrefix.trackNamespace, type);
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);
  if (!req.includeProperties) {
    baseMsg->parameters.emplace_back(namedValue("include_properties", 0));
  }
  baseMsg->numberOfParameters = baseMsg->parameters.size();
  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logNamespace(
    const Namespace& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTNamespace>();
  baseMsg->trackNamespaceSuffix = convertTrackNamespaceToByteStringFormat(
      req.trackNamespaceSuffix.trackNamespace, type);
  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logNamespaceDone(
    const NamespaceDone& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTNamespaceDone>();
  baseMsg->trackNamespaceSuffix = convertTrackNamespaceToByteStringFormat(
      req.trackNamespaceSuffix.trackNamespace, type);
  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logPublishSkipped(
    const PublishBlocked& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTPublishSkipped>();
  baseMsg->trackNamespaceSuffix = convertTrackNamespaceToByteStringFormat(
      req.trackNamespaceSuffix.trackNamespace, type);
  baseMsg->trackName = convertTrackNameToByteStringFormat(req.trackName, type);
  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logUnsubscribeNamespace(
    const UnsubscribeNamespace& req,
    const MOQTByteStringType& /*type*/,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTUnsubscribeNamespace>();
  if (req.requestID.has_value()) {
    baseMsg->requestId = req.requestID->value;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logSubscribeOk(
    const SubscribeOk& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTSubscribeOk>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->trackAlias = req.trackAlias.value;
  baseMsg->expires = req.expires.count();
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);

  if (req.largest.has_value()) {
    baseMsg->contentExists = 1;
    MOQTLocation loc;
    loc.group = req.largest.value().group;
    loc.object = req.largest.value().object;
    baseMsg->largestLocation = loc;
  } else {
    baseMsg->contentExists = 0;
  }

  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);
  if (req.expires.count() != 0) {
    baseMsg->parameters.emplace_back(
        namedValue("expires", static_cast<uint64_t>(req.expires.count())));
  }
  if (req.largest.has_value()) {
    folly::dynamic largest = folly::dynamic::object;
    largest["group"] = req.largest->group;
    largest["object"] = req.largest->object;
    baseMsg->parameters.emplace_back(
        namedValue("largest_object", std::move(largest)));
  }
  baseMsg->numberOfParameters = baseMsg->parameters.size();
  baseMsg->trackProperties = convertExtensionsToProperties(req.extensions);

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logSubscribeError(
    const SubscribeError& req,
    ControlMessageType controlType) {
  logRequestError(req, controlType);
}

void MLogger::logFetchOk(const FetchOk& req, ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTFetchOk>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);
  baseMsg->endOfTrack = req.endOfTrack;
  baseMsg->endLocation.group = req.endLocation.group;
  baseMsg->endLocation.object = req.endLocation.object;
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);
  baseMsg->numberOfParameters = baseMsg->parameters.size();
  baseMsg->trackProperties = convertExtensionsToProperties(req.extensions);

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logFetchError(
    const FetchError& req,
    ControlMessageType controlType) {
  logRequestError(req, controlType);
}

void MLogger::logPublishDone(
    const PublishDone& req,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTPublishDone>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->statusCode = static_cast<uint64_t>(req.statusCode);
  baseMsg->streamCount = req.streamCount;
  if (!req.reasonPhrase.empty()) {
    baseMsg->reason = req.reasonPhrase;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logPublishNamespaceDone(
    const PublishNamespaceDone& req,
    const MOQTByteStringType& /*type*/,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTPublishNamespaceDone>();
  if (req.requestID.has_value()) {
    baseMsg->requestId = req.requestID->value;
  }

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logTrackStatusOk(
    const TrackStatusOk& req,
    const MOQTByteStringType& /*type*/,
    ControlMessageType controlType) {
  logRequestOk(RequestOk::fromTrackStatusOk(req), controlType);
}

void MLogger::logTrackStatusError(
    const TrackStatusError& req,
    const MOQTByteStringType& /*type*/,
    ControlMessageType controlType) {
  logRequestError(req, controlType);
}

void MLogger::logMaxRequestId(
    const uint64_t requestId,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTMaxRequestId>();
  baseMsg->requestId = requestId;

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logRequestsBlocked(
    const uint64_t maximumRequestId,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTRequestsBlocked>();
  baseMsg->maximumRequestId = maximumRequestId;

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logPublishNamespace(
    const PublishNamespace& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTPublishNamespace>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.trackNamespace.trackNamespace, type);
  baseMsg->numberOfParameters = req.params.size();
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logSubscribeNamespaceOk(
    const SubscribeNamespaceOk& req,
    ControlMessageType controlType) {
  logRequestOk(req, controlType);
}

void MLogger::logSubscribeNamespaceError(
    const SubscribeNamespaceError& req,
    ControlMessageType controlType) {
  logRequestError(req, controlType);
}

void MLogger::logPublish(
    const PublishRequest& req,
    const MOQTByteStringType& type,
    ControlMessageType controlType) {
  auto baseMsg = std::make_unique<MOQTPublish>();
  baseMsg->requestId = req.requestID.value;
  baseMsg->trackNamespace = convertTrackNamespaceToByteStringFormat(
      req.fullTrackName.trackNamespace.trackNamespace, type);
  baseMsg->trackName =
      convertTrackNameToByteStringFormat(req.fullTrackName.trackName, type);
  baseMsg->trackAlias = req.trackAlias.value;
  baseMsg->groupOrder = static_cast<uint8_t>(req.groupOrder);
  baseMsg->contentExists = req.largest.has_value() ? 1 : 0;
  if (req.largest.has_value()) {
    MOQTLocation loc;
    loc.group = req.largest.value().group;
    loc.object = req.largest.value().object;
    baseMsg->largest = loc;
  }
  baseMsg->forward = req.forward ? 1 : 0;
  baseMsg->parameters = convertTrackParamsToMoQTParams(req.params);
  if (!req.forward) {
    baseMsg->parameters.emplace_back(namedValue("forward", 0));
  }
  if (req.largest.has_value()) {
    folly::dynamic largest = folly::dynamic::object;
    largest["group"] = req.largest->group;
    largest["object"] = req.largest->object;
    baseMsg->parameters.emplace_back(
        namedValue("largest_object", std::move(largest)));
  }
  baseMsg->numberOfParameters = baseMsg->parameters.size();
  baseMsg->trackProperties = convertExtensionsToProperties(req.extensions);

  logControlMessage(
      controlType, kFirstBidiStreamId, std::nullopt, std::move(baseMsg));
}

void MLogger::logPublishOk(
    const PublishOk& req,
    ControlMessageType controlType) {
  const auto version = negotiatedMoQVersion_.value_or(kVersionDraftCurrent);
  logRequestOk(
      RequestOk::fromPublishOk(req, getDraftMajorVersion(version)),
      controlType);
}

void MLogger::logPublishError(
    const PublishError& req,
    ControlMessageType controlType) {
  logRequestError(req, controlType);
}

std::vector<MOQTSetupParameter> MLogger::convertSetupParamsToMoQTSetupParams(
    const SetupParameters& params) {
  std::vector<MOQTSetupParameter> setupOptions;

  for (const auto& param : params) {
    switch (param.key) {
      case folly::to_underlying(SetupKey::PATH): {
        setupOptions.emplace_back(namedValue("path", param.asString));
        break;
      }
      case 0x06: {
        setupOptions.emplace_back(
            namedValue("max_filter_ranges", param.asUint64));
        break;
      }
      case 0x08: {
        setupOptions.emplace_back(
            namedValue("max_request_updates", param.asUint64));
        break;
      }
      case folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE): {
        setupOptions.emplace_back(
            namedValue("max_auth_token_cache_size", param.asUint64));
        break;
      }
      case folly::to_underlying(SetupKey::AUTHORITY): {
        setupOptions.emplace_back(namedValue("authority", param.asString));
        break;
      }
      case folly::to_underlying(SetupKey::MOQT_IMPLEMENTATION): {
        setupOptions.emplace_back(namedValue("implementation", param.asString));
        break;
      }
      case folly::to_underlying(SetupKey::AUTHORIZATION_TOKEN): {
        folly::dynamic auth = folly::dynamic::object;
        auth["name"] = "authorization_token";
        auth["alias_type"] = "use_value";
        if (!param.asString.empty()) {
          auth["token_value"] = rawInfoForLength(param.asString.size());
        }
        setupOptions.emplace_back(std::move(auth));
        break;
      }
      default: {
        setupOptions.emplace_back(
            unknownValue(param.key, param.asString, param.asUint64));
        break;
      }
    }
  }
  return setupOptions;
}

std::vector<MOQTParameter> MLogger::convertTrackParamsToMoQTParams(
    const TrackRequestParameters& params) {
  std::vector<MOQTParameter> moqParams;

  for (const auto& param : params) {
    switch (param.key) {
      case folly::to_underlying(TrackRequestParamKey::AUTHORIZATION_TOKEN): {
        moqParams.emplace_back(authorizationToken(param.asAuthToken));
        break;
      }
      case folly::to_underlying(
          TrackRequestParamKey::OBJECT_DELIVERY_TIMEOUT): {
        moqParams.emplace_back(
            namedValue("object_delivery_timeout", param.asUint64));
        break;
      }
      case folly::to_underlying(
          TrackRequestParamKey::SUBGROUP_DELIVERY_TIMEOUT): {
        moqParams.emplace_back(
            namedValue("subgroup_delivery_timeout", param.asUint64));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::RENDEZVOUS_TIMEOUT): {
        moqParams.emplace_back(
            namedValue("rendezvous_timeout", param.asUint64));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::SUBSCRIBER_PRIORITY): {
        moqParams.emplace_back(
            namedValue("subscriber_priority", param.asUint64));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::GROUP_ORDER): {
        moqParams.emplace_back(namedValue("group_order", param.asUint64));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::LOCATION_FILTER): {
        moqParams.emplace_back(namedValue(
            "location_filter", locationFilter(param.asLocationFilter)));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::FILL_PARAMETERS): {
        moqParams.emplace_back(namedValue(
            "fill_parameters", fillParametersValue(param.asFillParameters)));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::EXPIRES): {
        moqParams.emplace_back(namedValue("expires", param.asUint64));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::LARGEST_OBJECT): {
        if (param.largestObject.has_value()) {
          folly::dynamic largest = folly::dynamic::object;
          largest["group"] = param.largestObject->group;
          largest["object"] = param.largestObject->object;
          moqParams.emplace_back(
              namedValue("largest_object", std::move(largest)));
        }
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::FILL_TIMEOUT): {
        moqParams.emplace_back(namedValue("fill_timeout", param.asUint64));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::FORWARD): {
        moqParams.emplace_back(namedValue("forward", param.asUint64));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::NEW_GROUP_REQUEST): {
        moqParams.emplace_back(namedValue("new_group_request", param.asUint64));
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::TRACK_NAMESPACE_PREFIX): {
        const auto version =
            negotiatedMoQVersion_.value_or(kVersionDraftCurrent);
        auto parsed = MoQFrameParser::parseTrackNamespacePrefixParam(
            param.asString, version);
        if (parsed.hasValue()) {
          std::vector<folly::dynamic> elements;
          elements.reserve(parsed->trackNamespace.size());
          for (const auto& element : parsed->trackNamespace) {
            elements.push_back(folly::dynamic::object("value", element));
          }
          moqParams.emplace_back(namedValue(
              "track_namespace_prefix", folly::dynamic::array(elements)));
        } else {
          moqParams.emplace_back(
              unknownValue(param.key, param.asString, param.asUint64));
        }
        break;
      }
      case folly::to_underlying(TrackRequestParamKey::INCLUDE_PROPERTIES): {
        moqParams.emplace_back(
            namedValue("include_properties", param.asUint64));
        break;
      }
      case 0x25:
      case 0x26:
      case 0x27:
      case 0x28:
      case 0x29: {
        auto filter = rangeFilter(param);
        moqParams.emplace_back(
            filter.has_value()
                ? std::move(filter.value())
                : unknownValue(param.key, param.asString, param.asUint64));
        break;
      }
      default: {
        moqParams.emplace_back(
            unknownValue(param.key, param.asString, param.asUint64));
        break;
      }
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
    std::optional<Owner> owner) {
  MOQTStreamTypeSet baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.streamType = type;
  baseMsg.owner = owner;
  addStreamTypeSetLog(std::move(baseMsg));
}

void MLogger::logObjectDatagramCreated(
    TrackAlias trackAlias,
    const ObjectHeader& header,
    const Payload& payload,
    bool endOfGroup) {
  MOQTObjectDatagramCreated baseMsg;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = header.group;
  baseMsg.objectId = header.id;
  baseMsg.publisherPriority = header.priority;
  baseMsg.properties = convertExtensionsToProperties(header.extensions);
  if (header.status != ObjectStatus::NORMAL) {
    baseMsg.objectStatus = static_cast<uint64_t>(header.status);
  }
  if (payload) {
    baseMsg.objectPayload = MOQTRawInfo{payload->computeChainDataLength()};
  }
  baseMsg.endOfGroup = endOfGroup;
  addObjectDatagramCreatedLog(std::move(baseMsg));
}

void MLogger::logObjectDatagramParsed(
    TrackAlias trackAlias,
    const ObjectHeader& header,
    const Payload& payload,
    bool endOfGroup) {
  MOQTObjectDatagramParsed baseMsg;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = header.group;
  baseMsg.objectId = header.id;
  baseMsg.publisherPriority = header.priority;
  baseMsg.properties = convertExtensionsToProperties(header.extensions);
  if (header.status != ObjectStatus::NORMAL) {
    baseMsg.objectStatus = static_cast<uint64_t>(header.status);
  }
  if (payload) {
    baseMsg.objectPayload = MOQTRawInfo{payload->computeChainDataLength()};
  }
  baseMsg.endOfGroup = endOfGroup;
  addObjectDatagramParsedLog(std::move(baseMsg));
}

void MLogger::logPaddingDatagramCreated(uint64_t paddingLength) {
  addPaddingDatagramCreatedLog(MOQTPaddingDatagramCreated{paddingLength});
}

void MLogger::logPaddingDatagramParsed(uint64_t paddingLength) {
  addPaddingDatagramParsedLog(MOQTPaddingDatagramParsed{paddingLength});
}

void MLogger::logSubgroupHeaderCreated(
    uint64_t streamId,
    TrackAlias trackAlias,
    uint64_t groupId,
    uint64_t sugroupId,
    uint8_t publisherPriority,
    SubgroupIDFormat format,
    bool includeExtensions,
    bool endOfGroup,
    bool firstObject,
    bool publisherPriorityPresent) {
  MOQTSubgroupHeaderCreated baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = groupId;
  baseMsg.subgroupIdMode = static_cast<uint8_t>(format);
  if (format == SubgroupIDFormat::Present) {
    baseMsg.subgroupId = sugroupId;
  }
  if (publisherPriorityPresent) {
    baseMsg.publisherPriority = publisherPriority;
  }
  baseMsg.containsEndOfGroup = endOfGroup;
  baseMsg.propertiesPresent = includeExtensions;
  baseMsg.firstObject = firstObject;
  createdObjectStreamState_[streamId] = ObjectStreamState{
      groupId, sugroupId, std::nullopt, baseMsg.publisherPriority};
  addSubgroupHeaderCreatedLog(std::move(baseMsg));
}

void MLogger::logSubgroupHeaderParsed(
    uint64_t streamId,
    TrackAlias trackAlias,
    uint64_t groupId,
    uint64_t sugroupId,
    uint8_t publisherPriority,
    const SubgroupOptions& options) {
  MOQTSubgroupHeaderParsed baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.trackAlias = trackAlias.value;
  baseMsg.groupId = groupId;
  baseMsg.subgroupIdMode = static_cast<uint8_t>(options.subgroupIDFormat);
  if (options.subgroupIDFormat == SubgroupIDFormat::Present) {
    baseMsg.subgroupId = sugroupId;
  }
  if (options.priorityPresent) {
    baseMsg.publisherPriority = publisherPriority;
  }
  baseMsg.containsEndOfGroup = options.hasEndOfGroup;
  baseMsg.propertiesPresent = options.hasExtensions;
  baseMsg.firstObject = options.beginsWithFirstObject;
  parsedObjectStreamState_[streamId] = ObjectStreamState{
      groupId, sugroupId, std::nullopt, baseMsg.publisherPriority};
  addSubgroupHeaderParsedLog(std::move(baseMsg));
}

void MLogger::logSubgroupObjectCreated(
    uint64_t streamId,
    TrackAlias /*trackAlias*/,
    const ObjectHeader& objHeader,
    Payload payload) {
  MOQTSubgroupObjectCreated baseMsg;
  baseMsg.streamId = streamId;
  auto& state = createdObjectStreamState_[streamId];
  baseMsg.objectIdDelta =
      state.object.has_value() && objHeader.id > state.object.value()
      ? objHeader.id - state.object.value() - 1
      : objHeader.id;
  state.object = objHeader.id;
  baseMsg.properties = convertExtensionsToProperties(objHeader.extensions);
  baseMsg.objectPayloadLength = objHeader.length.value_or(
      payload ? payload->computeChainDataLength() : 0);
  if (objHeader.status != ObjectStatus::NORMAL) {
    baseMsg.objectStatus = static_cast<uint64_t>(objHeader.status);
  }
  addSubgroupObjectCreatedLog(std::move(baseMsg));
}

void MLogger::logSubgroupObjectParsed(
    uint64_t streamId,
    TrackAlias /*trackAlias*/,
    const ObjectHeader& objHeader,
    Payload payload) {
  MOQTSubgroupObjectParsed baseMsg;
  baseMsg.streamId = streamId;
  auto& state = parsedObjectStreamState_[streamId];
  baseMsg.objectIdDelta =
      state.object.has_value() && objHeader.id > state.object.value()
      ? objHeader.id - state.object.value() - 1
      : objHeader.id;
  state.object = objHeader.id;
  baseMsg.properties = convertExtensionsToProperties(objHeader.extensions);
  baseMsg.objectPayloadLength = objHeader.length.value_or(
      payload ? payload->computeChainDataLength() : 0);
  if (objHeader.status != ObjectStatus::NORMAL) {
    baseMsg.objectStatus = static_cast<uint64_t>(objHeader.status);
  }
  addSubgroupObjectParsedLog(std::move(baseMsg));
}

void MLogger::logFetchHeaderCreated(
    const uint64_t streamId,
    const uint64_t requestId,
    bool /*isFill*/) {
  MOQTFetchHeaderCreated baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.requestId = requestId;
  createdObjectStreamState_[streamId] = ObjectStreamState{};
  addFetchHeaderCreatedLog(std::move(baseMsg));
}

void MLogger::logFetchHeaderParsed(
    const uint64_t streamId,
    const uint64_t requestId,
    bool /*isFill*/) {
  MOQTFetchHeaderParsed baseMsg;
  baseMsg.streamId = streamId;
  baseMsg.requestId = requestId;
  parsedObjectStreamState_[streamId] = ObjectStreamState{};
  addFetchHeaderParsedLog(std::move(baseMsg));
}

void MLogger::logFetchObjectCreated(
    const uint64_t streamId,
    const ObjectHeader& objHeader,
    Payload payload) {
  MOQTFetchObjectCreated baseMsg;
  baseMsg.streamId = streamId;
  auto& state = createdObjectStreamState_[streamId];
  baseMsg.datagram = objHeader.forwardingPreferenceIsDatagram;
  if (!baseMsg.datagram) {
    if (objHeader.subgroup == 0) {
      baseMsg.subgroupIdBits = 0;
    } else if (state.subgroup == objHeader.subgroup) {
      baseMsg.subgroupIdBits = 1;
    } else if (
        state.subgroup.has_value() &&
        objHeader.subgroup == state.subgroup.value() + 1) {
      baseMsg.subgroupIdBits = 2;
    } else {
      baseMsg.subgroupIdBits = 3;
      baseMsg.subgroupId = objHeader.subgroup;
    }
  }
  if (!state.group.has_value()) {
    baseMsg.groupIdDelta = objHeader.group;
  } else if (objHeader.group != state.group.value()) {
    if (objHeader.group < state.group.value()) {
      baseMsg.groupIdDelta = state.group.value() - objHeader.group - 1;
    } else {
      baseMsg.groupIdDelta = objHeader.group - state.group.value() - 1;
    }
  }
  const bool nextObject =
      state.object.has_value() && objHeader.id == state.object.value() + 1;
  if (!state.object.has_value()) {
    baseMsg.objectIdDelta = objHeader.id;
  } else if (baseMsg.groupIdDelta.has_value()) {
    if (!nextObject) {
      baseMsg.objectIdDelta = objHeader.id;
    }
  } else if (!nextObject) {
    baseMsg.objectIdDelta = objHeader.id >= state.object.value()
        ? objHeader.id - state.object.value()
        : objHeader.id;
  }
  if (!state.priority.has_value() || objHeader.priority != state.priority) {
    baseMsg.publisherPriority = objHeader.priority;
  }
  state.group = objHeader.group;
  state.subgroup = objHeader.subgroup;
  state.object = objHeader.id;
  state.priority = objHeader.priority;
  baseMsg.properties = convertExtensionsToProperties(objHeader.extensions);
  baseMsg.objectPayloadLength = objHeader.length.value_or(
      payload ? payload->computeChainDataLength() : 0);
  addFetchObjectCreatedLog(std::move(baseMsg));
}

void MLogger::logFetchObjectCreated(
    uint64_t streamId,
    uint64_t endGroup,
    uint64_t endObject,
    MOQTFetchRangeEndType rangeEndType) {
  MOQTFetchObjectCreated baseMsg;
  populateFetchRangeEnd(baseMsg, streamId, endGroup, endObject, rangeEndType);
  auto& state = createdObjectStreamState_[streamId];
  state.group = endGroup;
  state.object = endObject;
  addFetchObjectCreatedLog(std::move(baseMsg));
}

void MLogger::logFetchObjectParsed(
    const uint64_t streamId,
    const ObjectHeader& objHeader,
    Payload payload) {
  MOQTFetchObjectParsed baseMsg;
  baseMsg.streamId = streamId;
  auto& state = parsedObjectStreamState_[streamId];
  baseMsg.datagram = objHeader.forwardingPreferenceIsDatagram;
  if (!baseMsg.datagram) {
    if (objHeader.subgroup == 0) {
      baseMsg.subgroupIdBits = 0;
    } else if (state.subgroup == objHeader.subgroup) {
      baseMsg.subgroupIdBits = 1;
    } else if (
        state.subgroup.has_value() &&
        objHeader.subgroup == state.subgroup.value() + 1) {
      baseMsg.subgroupIdBits = 2;
    } else {
      baseMsg.subgroupIdBits = 3;
      baseMsg.subgroupId = objHeader.subgroup;
    }
  }
  if (!state.group.has_value()) {
    baseMsg.groupIdDelta = objHeader.group;
  } else if (objHeader.group != state.group.value()) {
    if (objHeader.group < state.group.value()) {
      baseMsg.groupIdDelta = state.group.value() - objHeader.group - 1;
    } else {
      baseMsg.groupIdDelta = objHeader.group - state.group.value() - 1;
    }
  }
  const bool nextObject =
      state.object.has_value() && objHeader.id == state.object.value() + 1;
  if (!state.object.has_value()) {
    baseMsg.objectIdDelta = objHeader.id;
  } else if (baseMsg.groupIdDelta.has_value()) {
    if (!nextObject) {
      baseMsg.objectIdDelta = objHeader.id;
    }
  } else if (!nextObject) {
    baseMsg.objectIdDelta = objHeader.id >= state.object.value()
        ? objHeader.id - state.object.value()
        : objHeader.id;
  }
  if (!state.priority.has_value() || objHeader.priority != state.priority) {
    baseMsg.publisherPriority = objHeader.priority;
  }
  state.group = objHeader.group;
  state.subgroup = objHeader.subgroup;
  state.object = objHeader.id;
  state.priority = objHeader.priority;
  baseMsg.properties = convertExtensionsToProperties(objHeader.extensions);
  baseMsg.objectPayloadLength = objHeader.length.value_or(
      payload ? payload->computeChainDataLength() : 0);
  addFetchObjectParsedLog(std::move(baseMsg));
}

void MLogger::logFetchObjectParsed(
    uint64_t streamId,
    uint64_t endGroup,
    uint64_t endObject,
    MOQTFetchRangeEndType rangeEndType) {
  MOQTFetchObjectParsed baseMsg;
  populateFetchRangeEnd(baseMsg, streamId, endGroup, endObject, rangeEndType);
  auto& state = parsedObjectStreamState_[streamId];
  state.group = endGroup;
  state.object = endObject;
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

std::vector<MOQTProperty> MLogger::convertExtensionsToProperties(
    const Extensions& extensions) {
  std::vector<MOQTProperty> properties;
  auto append = [&properties](const std::vector<Extension>& source) {
    properties.reserve(properties.size() + source.size());
    for (const auto& ext : source) {
      MOQTProperty property;
      property.type = ext.type;
      if (ext.type % 2 == 1) {
        if (ext.arrayValue) {
          const auto length = ext.arrayValue->computeChainDataLength();
          property.length = length;
          property.valueBytes = MOQTRawInfo{length};
        }
      } else {
        property.value = ext.intValue;
      }
      properties.push_back(std::move(property));
    }
  };
  append(extensions.getMutableExtensions());
  append(extensions.getImmutableExtensions());
  return properties;
}

void MLogger::logControlMessage(
    ControlMessageType controlType,
    uint64_t streamId,
    const std::optional<uint64_t>& length,
    std::unique_ptr<MOQTBaseControlMessage> message) {
  switch (controlType) {
    case ControlMessageType::CREATED: {
      MOQTControlMessageCreated req{streamId, length, std::move(message)};
      addControlMessageCreatedLog(std::move(req));
      break;
    }
    case ControlMessageType::PARSED: {
      MOQTControlMessageParsed req{streamId, length, std::move(message)};
      addControlMessageParsedLog(std::move(req));
      break;
    }
    default: {
      break;
    }
  }
}

} // namespace moxygen
