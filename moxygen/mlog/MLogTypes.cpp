/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {

folly::dynamic MOQTControlMessageCreated::toDynamic() const {
  folly::dynamic jsonObject = folly::dynamic::object;
  jsonObject["stream_id"] = streamId;

  if (length.has_value()) {
    jsonObject["length"] = length.value();
  }
  jsonObject["message"] = message->toDynamic();

  return jsonObject;
}

folly::dynamic MOQTControlMessageParsed::toDynamic() const {
  folly::dynamic jsonObject = folly::dynamic::object;
  jsonObject["stream_id"] = streamId;

  if (length.has_value()) {
    jsonObject["length"] = length.value();
  }
  jsonObject["message"] = message->toDynamic();

  return jsonObject;
}

folly::dynamic MOQTSetupMessage::toDynamic() const {
  folly::dynamic setupObj = folly::dynamic::object;
  setupObj["type"] = type;

  if (!setupOptions.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(setupOptions.size());
    for (const auto& param : setupOptions) {
      paramObjects.push_back(setupParameterToDynamic(param));
    }
    setupObj["setup_options"] = folly::dynamic::array(paramObjects);
  }
  return setupObj;
}

folly::dynamic parameterToDynamic(const MOQTParameter& param) {
  return param.fields;
}

folly::dynamic setupParameterToDynamic(const MOQTSetupParameter& param) {
  return param.fields;
}

folly::dynamic MOQTLocation::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["group"] = group;
  obj["object"] = object;
  return obj;
}

folly::dynamic MOQTSubscribe::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["track_namespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["track_name"] = parseTrackName(trackName);
  obj["subscriber_priority"] = subscriberPriority;
  obj["group_order"] = groupOrder;
  obj["forward"] = forward;
  obj["filter_type"] = filterType;

  if (startLocation.has_value()) {
    obj["start_location"] = startLocation->toDynamic();
  }
  if (endGroup.has_value()) {
    obj["end_group"] = endGroup.value();
  }
  obj["number_of_parameters"] = numberOfParameters;

  if (!subscribeParameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(subscribeParameters.size());
    for (const auto& param : subscribeParameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTSubscribeUpdate::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["subscription_request_id"] = subscriptionRequestId;
  obj["start_location"] = startLocation.toDynamic();
  obj["end_group"] = endGroup;
  obj["subscriber_priority"] = subscriberPriority;
  obj["forward"] = forward;
  obj["number_of_parameters"] = numberOfParameters;

  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTUnsubscribe::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  return obj;
}

folly::dynamic MOQTStandaloneFetch::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  std::vector<std::string> trackNamespaceStr;
  for (const auto& ns : trackNamespace) {
    if (ns.type == MOQTByteStringType::STRING_VALUE) {
      trackNamespaceStr.push_back(ns.value);
    }
  }
  obj["track_namespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  if (trackName.type == MOQTByteStringType::STRING_VALUE) {
    obj["track_name"] = trackName.value;
  }
  obj["start_location"] = startLocation.toDynamic();
  obj["end_location"] = endLocation.toDynamic();
  return obj;
}

folly::dynamic MOQTJoiningFetch::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["joining_request_id"] = joiningRequestId;
  obj["joining_start"] = joiningStart;
  return obj;
}

folly::dynamic MOQTFetch::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["subscriber_priority"] = subscriberPriority;
  obj["group_order"] = groupOrder;
  obj["fetch_type"] = fetchType;

  if (standaloneFetch.has_value()) {
    obj["standalone_fetch"] = standaloneFetch->toDynamic();
  }

  if (joiningFetch.has_value()) {
    obj["joining_fetch"] = joiningFetch->toDynamic();
  }

  obj["number_of_parameters"] = numberOfParameters;
  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTFetchCancel::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  return obj;
}

folly::dynamic MOQTPublishNamespaceOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  return obj;
}

folly::dynamic MOQTPublishNamespaceError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.has_value()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.has_value()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTPublishNamespaceCancel::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  if (requestId.has_value()) {
    obj["request_id"] = requestId.value();
  }
  obj["error_code"] = errorCode;
  if (reason.has_value()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.has_value()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTTrackStatus::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["track_namespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["track_name"] = parseTrackName(trackName);
  obj["subscriber_priority"] = subscriberPriority;
  obj["group_order"] = groupOrder;
  obj["forward"] = forward;
  obj["filter_type"] = filterType;
  if (startLocation.has_value()) {
    obj["start_location"] = startLocation->toDynamic();
  }
  if (endGroup.has_value()) {
    obj["end_group"] = endGroup.value();
  }
  obj["number_of_parameters"] = numberOfParameters;
  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTSubscribeNamespace::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  auto trackNamespacePrefixStr = parseTrackNamespace(trackNamespacePrefix);
  obj["track_namespace_prefix"] = folly::dynamic::array(
      trackNamespacePrefixStr.begin(), trackNamespacePrefixStr.end());
  obj["number_of_parameters"] = numberOfParameters;
  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTUnsubscribeNamespace::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;

  if (requestId.has_value()) {
    obj["request_id"] = requestId.value();
  }

  return obj;
}

folly::dynamic MOQTSubscribeOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["track_alias"] = trackAlias;
  obj["expires"] = expires;
  obj["group_order"] = groupOrder;
  obj["content_exists"] = contentExists;
  if (largestLocation.has_value()) {
    obj["largest_location"] = largestLocation->toDynamic();
  }
  obj["number_of_parameters"] = numberOfParameters;
  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTSubscribeError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.has_value()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.has_value()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTFetchOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["group_order"] = groupOrder;
  obj["end_of_track"] = endOfTrack;
  obj["end_location"] = endLocation.toDynamic();
  obj["number_of_parameters"] = numberOfParameters;

  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTFetchError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.has_value()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.has_value()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTPublishDone::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["status_code"] = statusCode;
  obj["stream_count"] = streamCount;
  if (reason.has_value()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.has_value()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTMaxRequestId::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  return obj;
}

folly::dynamic MOQTRequestsBlocked::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["maximum_request_id"] = maximumRequestId;
  return obj;
}

folly::dynamic MOQTPublishNamespace::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["track_namespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["number_of_parameters"] = numberOfParameters;
  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTPublishNamespaceDone::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  if (requestId.has_value()) {
    obj["request_id"] = requestId.value();
  }
  return obj;
}

folly::dynamic MOQTTrackStatusOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["track_alias"] = trackAlias;
  obj["expires"] = expires;
  obj["group_order"] = groupOrder;
  obj["content_exists"] = contentExists;
  if (largestLocation.has_value()) {
    obj["largest_location"] = largestLocation->toDynamic();
  }
  obj["number_of_parameters"] = numberOfParameters;
  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTTrackStatusError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.has_value()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.has_value()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTSubscribeNamespaceOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  return obj;
}

folly::dynamic MOQTSubscribeNamespaceError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.has_value()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.has_value()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

std::vector<std::string> MOQTBaseControlMessage::parseTrackNamespace(
    const std::vector<MOQTByteString>& trackNamespace) const {
  std::vector<std::string> track;
  if (trackNamespace.empty()) {
    return track;
  }
  // Check If TrackNamespace is string or value Bytes

  switch (trackNamespace[0].type) {
    case MOQTByteStringType::VALUE_BYTES: {
      for (auto& t : trackNamespace) {
        track.emplace_back(t.valueBytes->to<std::string>());
      }
      break;
    }
    case MOQTByteStringType::STRING_VALUE: {
      for (auto& t : trackNamespace) {
        track.emplace_back(t.value);
      }
      break;
    }
    default: {
    }
  }

  return track;
}

std::string MOQTBaseControlMessage::parseTrackName(
    const MOQTByteString& trackName) const {
  std::string name;
  switch (trackName.type) {
    case MOQTByteStringType::VALUE_BYTES: {
      name = trackName.valueBytes->to<std::string>();
      break;
    }
    case MOQTByteStringType::STRING_VALUE: {
      name = trackName.value;
      break;
    }
    default: {
    }
  }
  return name;
}

folly::dynamic MOQTGoaway::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  if ((length).has_value()) {
    obj["length"] = std::to_string(length.value());
  }

  if (newSessionUri && !newSessionUri->empty()) {
    obj["new_session_uri"] = newSessionUri->to<std::string>();
  } else {
    obj["new_session_uri"] = "";
  }
  obj["timeout"] = timeout;
  if (requestId.has_value()) {
    obj["request_id"] = *requestId;
  }

  return obj;
}

folly::dynamic MOQTStreamTypeSet::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  if (owner.has_value()) {
    obj["owner"] = owner.value() == Owner::LOCAL ? "local" : "remote";
  }
  obj["stream_id"] = streamId;
  switch (streamType) {
    case MOQTStreamType::CONTROL:
      obj["stream_type"] = "control";
      break;
    case MOQTStreamType::SUBGROUP_HEADER:
      obj["stream_type"] = "subgroup_header";
      break;
    case MOQTStreamType::FETCH_HEADER:
      obj["stream_type"] = "fetch_header";
      break;
    case MOQTStreamType::PADDING:
      obj["stream_type"] = "padding";
      break;
  }
  return obj;
}

folly::dynamic MOQTRawInfo::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  if (length.has_value()) {
    obj["length"] = length.value();
  }
  if (payloadLength.has_value()) {
    obj["payload_length"] = payloadLength.value();
  }
  if (data.has_value()) {
    obj["data"] = data.value();
  }
  return obj;
}

folly::dynamic MOQTProperty::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  if (value.has_value()) {
    obj["value"] = value.value();
  }
  if (length.has_value()) {
    obj["length"] = length.value();
  }
  if (valueBytes.has_value()) {
    obj["value_bytes"] = valueBytes->toDynamic();
  }
  return obj;
}

namespace {

folly::dynamic propertiesToDynamic(
    const std::vector<MOQTProperty>& properties) {
  std::vector<folly::dynamic> values;
  values.reserve(properties.size());
  for (const auto& property : properties) {
    values.push_back(property.toDynamic());
  }
  return folly::dynamic::array(values);
}

template <typename Event>
folly::dynamic objectDatagramToDynamic(const Event& event) {
  folly::dynamic obj = folly::dynamic::object;
  obj["track_alias"] = event.trackAlias;
  obj["group_id"] = event.groupId;
  if (event.objectId.has_value()) {
    obj["object_id"] = event.objectId.value();
  }
  if (event.publisherPriority.has_value()) {
    obj["publisher_priority"] = event.publisherPriority.value();
  }
  if (event.propertiesLength.has_value()) {
    obj["properties_length"] = event.propertiesLength.value();
  }
  if (!event.properties.empty()) {
    obj["properties"] = propertiesToDynamic(event.properties);
  }
  if (event.objectStatus.has_value()) {
    obj["object_status"] = event.objectStatus.value();
  }
  if (event.objectPayload.has_value()) {
    obj["object_payload"] = event.objectPayload->toDynamic();
  }
  obj["end_of_group"] = event.endOfGroup;
  return obj;
}

template <typename Event>
folly::dynamic subgroupHeaderToDynamic(const Event& event) {
  folly::dynamic obj = folly::dynamic::object;
  obj["stream_id"] = event.streamId;
  obj["track_alias"] = event.trackAlias;
  obj["group_id"] = event.groupId;
  obj["subgroup_id_mode"] = event.subgroupIdMode;
  if (event.subgroupId.has_value()) {
    obj["subgroup_id"] = event.subgroupId.value();
  }
  if (event.publisherPriority.has_value()) {
    obj["publisher_priority"] = event.publisherPriority.value();
  }
  obj["contains_end_of_group"] = event.containsEndOfGroup;
  obj["properties_present"] = event.propertiesPresent;
  obj["first_object"] = event.firstObject;
  return obj;
}

template <typename Event>
folly::dynamic subgroupObjectToDynamic(const Event& event) {
  folly::dynamic obj = folly::dynamic::object;
  obj["stream_id"] = event.streamId;
  obj["object_id_delta"] = event.objectIdDelta;
  if (!event.properties.empty()) {
    obj["properties"] = propertiesToDynamic(event.properties);
  }
  obj["object_payload_length"] = event.objectPayloadLength;
  if (event.objectStatus.has_value()) {
    obj["object_status"] = event.objectStatus.value();
  }
  return obj;
}

template <typename Event>
folly::dynamic fetchObjectToDynamic(const Event& event) {
  folly::dynamic obj = folly::dynamic::object;
  obj["stream_id"] = event.streamId;
  obj["datagram"] = event.datagram;
  obj["end_of_nonexistent_range"] = event.endOfNonexistentRange;
  obj["end_of_unknown_range"] = event.endOfUnknownRange;
  obj["end_of_timed_out_range"] = event.endOfTimedOutRange;
  if (event.subgroupIdBits.has_value()) {
    obj["subgroup_id_bits"] = event.subgroupIdBits.value();
  }
  if (event.groupIdDelta.has_value()) {
    obj["group_id_delta"] = event.groupIdDelta.value();
  }
  if (event.subgroupId.has_value()) {
    obj["subgroup_id"] = event.subgroupId.value();
  }
  if (event.objectIdDelta.has_value()) {
    obj["object_id_delta"] = event.objectIdDelta.value();
  }
  if (event.publisherPriority.has_value()) {
    obj["publisher_priority"] = event.publisherPriority.value();
  }
  if (event.propertiesLength.has_value()) {
    obj["properties_length"] = event.propertiesLength.value();
  }
  if (!event.properties.empty()) {
    obj["properties"] = propertiesToDynamic(event.properties);
  }
  obj["object_payload_length"] = event.objectPayloadLength;
  return obj;
}

} // namespace

folly::dynamic MOQTObjectDatagramCreated::toDynamic() const {
  return objectDatagramToDynamic(*this);
}

folly::dynamic MOQTObjectDatagramParsed::toDynamic() const {
  return objectDatagramToDynamic(*this);
}

folly::dynamic MOQTSubgroupHeaderCreated::toDynamic() const {
  return subgroupHeaderToDynamic(*this);
}

folly::dynamic MOQTSubgroupHeaderParsed::toDynamic() const {
  return subgroupHeaderToDynamic(*this);
}

folly::dynamic MOQTSubgroupObjectCreated::toDynamic() const {
  return subgroupObjectToDynamic(*this);
}

folly::dynamic MOQTSubgroupObjectParsed::toDynamic() const {
  return subgroupObjectToDynamic(*this);
}

folly::dynamic MOQTFetchHeaderCreated::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["stream_id"] = streamId;
  obj["request_id"] = requestId;
  return obj;
}

folly::dynamic MOQTFetchHeaderParsed::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["stream_id"] = streamId;
  obj["request_id"] = requestId;
  return obj;
}

folly::dynamic MOQTFetchObjectCreated::toDynamic() const {
  return fetchObjectToDynamic(*this);
}

folly::dynamic MOQTFetchObjectParsed::toDynamic() const {
  return fetchObjectToDynamic(*this);
}

folly::dynamic MOQTPaddingDatagramCreated::toDynamic() const {
  return folly::dynamic::object("padding_length", paddingLength);
}

folly::dynamic MOQTPaddingDatagramParsed::toDynamic() const {
  return folly::dynamic::object("padding_length", paddingLength);
}

folly::dynamic MOQTPublish::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["track_namespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["track_name"] = parseTrackName(trackName);
  obj["track_alias"] = trackAlias;
  obj["group_order"] = groupOrder;
  obj["content_exists"] = contentExists;
  if (largest.has_value()) {
    obj["largest"] = largest->toDynamic();
  }
  obj["forward"] = forward;
  obj["number_of_parameters"] = numberOfParameters;
  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTPublishOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["forward"] = forward;
  obj["subscriber_priority"] = subscriberPriority;
  obj["group_order"] = groupOrder;
  obj["filter_type"] = filterType;
  if (start.has_value()) {
    obj["start"] = start->toDynamic();
  }
  if (endGroup.has_value()) {
    obj["end_group"] = endGroup.value();
  }
  obj["number_of_parameters"] = numberOfParameters;
  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (const auto& param : parameters) {
      paramObjects.push_back(parameterToDynamic(param));
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTPublishError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.has_value()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.has_value()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

} // namespace moxygen
