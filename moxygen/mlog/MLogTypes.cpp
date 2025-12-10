/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {

folly::dynamic MOQTControlMessageCreated::toDynamic() const {
  folly::dynamic jsonObject = folly::dynamic::object;
  jsonObject["streamId"] = std::to_string(streamId);

  if (length.has_value()) {
    jsonObject["length"] = std::to_string(length.value());
  }
  jsonObject["message"] = message->toDynamic();

  return jsonObject;
}

folly::dynamic MOQTControlMessageParsed::toDynamic() const {
  folly::dynamic jsonObject = folly::dynamic::object;
  jsonObject["streamId"] = std::to_string(streamId);

  if (length.has_value()) {
    jsonObject["length"] = std::to_string(length.value());
  }
  jsonObject["message"] = message->toDynamic();

  return jsonObject;
}

folly::dynamic MOQTClientSetupMessage::toDynamic() const {
  folly::dynamic clientSetupObj = folly::dynamic::object;
  clientSetupObj["type"] = type;
  clientSetupObj["numberOfSupportedVersions"] = numberOfSupportedVersions;
  clientSetupObj["supportedVersions"] =
      folly::dynamic::array(supportedVersions.begin(), supportedVersions.end());
  clientSetupObj["numberOfParameters"] = numberOfParameters;

  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(setupParameters.size());
  for (auto& param : setupParameters) {
    paramObjects.push_back(param.toDynamic());
  }
  clientSetupObj["setupParameters"] = folly::dynamic::array(paramObjects);
  return clientSetupObj;
}

folly::dynamic MOQTServerSetupMessage::toDynamic() const {
  folly::dynamic serverSetupObj = folly::dynamic::object;
  serverSetupObj["type"] = type;
  serverSetupObj["selectedVersion"] = selectedVersion;
  serverSetupObj["numberOfParameters"] = numberOfParameters;

  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(setupParameters.size());
  for (auto& param : setupParameters) {
    paramObjects.push_back(param.toDynamic());
  }
  serverSetupObj["setupParameters"] = folly::dynamic::array(paramObjects);
  return serverSetupObj;
}

folly::dynamic MOQTParameter::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = name;

  if (type == MOQTParameterType::INT) {
    obj["value"] = std::to_string(intValue);
  } else if (type == MOQTParameterType::STRING) {
    obj["value"] = stringValue;
  } else {
    obj["value"] = "Invalid Data Type For MOQTParameter";
  }

  return obj;
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

  if (startLocation.hasValue()) {
    obj["start_location"] = startLocation->toDynamic();
  }
  if (endGroup.hasValue()) {
    obj["end_group"] = endGroup.value();
  }
  obj["number_of_parameters"] = numberOfParameters;

  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(subscribeParameters.size());
  for (auto& param : subscribeParameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["parameters"] = folly::dynamic::array(paramObjects);
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

  if (numberOfParameters > 0) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (auto& param : parameters) {
      paramObjects.push_back(param.toDynamic());
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

  if (standaloneFetch.hasValue()) {
    obj["standalone_fetch"] = standaloneFetch->toDynamic();
  }

  if (joiningFetch.hasValue()) {
    obj["joining_fetch"] = joiningFetch->toDynamic();
  }

  if (!parameters.empty()) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (auto& param : parameters) {
      paramObjects.push_back(param.toDynamic());
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

folly::dynamic MOQTAnnounceOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  return obj;
}

folly::dynamic MOQTAnnounceError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTAnnounceCancel::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["track_namespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["error_code"] = errorCode;
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
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
  if (startLocation.hasValue()) {
    obj["start_location"] = startLocation->toDynamic();
  }
  if (endGroup.hasValue()) {
    obj["end_group"] = endGroup.value();
  }
  obj["number_of_parameters"] = numberOfParameters;
  if (numberOfParameters > 0) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (auto& param : parameters) {
      paramObjects.push_back(param.toDynamic());
    }
    obj["parameters"] = folly::dynamic::array(paramObjects);
  }
  return obj;
}

folly::dynamic MOQTSubscribeAnnounces::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  auto trackNamespacePrefixStr = parseTrackNamespace(trackNamespacePrefix);
  obj["track_namespace_prefix"] = folly::dynamic::array(
      trackNamespacePrefixStr.begin(), trackNamespacePrefixStr.end());
  obj["number_of_parameters"] = numberOfParameters;
  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(parameters.size());
  for (auto& param : parameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["parameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTUnsubscribeAnnounces::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
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
  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(parameters.size());
  for (auto& param : parameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["parameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTSubscribeError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
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

  if (numberOfParameters > 0) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (auto& param : parameters) {
      paramObjects.push_back(param.toDynamic());
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
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTSubscribeDone::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  obj["statusCode"] = std::to_string(statusCode);
  obj["streamCount"] = std::to_string(streamCount);
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reasonBytes"] = reasonBytes.value();
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

folly::dynamic MOQTAnnounce::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["track_namespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["number_of_parameters"] = numberOfParameters;
  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(parameters.size());
  for (auto& param : parameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["parameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTUnannounce::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
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
  if (numberOfParameters > 0) {
    std::vector<folly::dynamic> paramObjects;
    paramObjects.reserve(parameters.size());
    for (auto& param : parameters) {
      paramObjects.push_back(param.toDynamic());
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
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTSubscribeAnnouncesOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  return obj;
}

folly::dynamic MOQTSubscribeAnnouncesError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

std::vector<std::string> MOQTBaseControlMessage::parseTrackNamespace(
    const std::vector<MOQTByteString>& trackNamespace) const {
  std::vector<std::string> track;
  // Check If TrackNamespace is string or value Bytes

  switch (trackNamespace[0].type) {
    case MOQTByteStringType::VALUE_BYTES: {
      for (auto& t : trackNamespace) {
        track.emplace_back(
            reinterpret_cast<const char*>(t.valueBytes->data()),
            t.valueBytes->length());
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
      name = std::string(
          reinterpret_cast<const char*>(trackName.valueBytes->data()),
          trackName.valueBytes->length());
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
  if ((length).hasValue()) {
    obj["length"] = std::to_string(length.value());
  }

  if (newSessionUri && !newSessionUri->empty()) {
    std::string uri(
        reinterpret_cast<const char*>(newSessionUri->data()),
        newSessionUri->length());
    obj["new_session_uri"] = uri;
  } else {
    obj["new_session_uri"] = "";
  }

  return obj;
}

folly::dynamic MOQTStreamTypeSet::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  if (owner.hasValue()) {
    obj["owner"] = std::to_string(owner.value());
  }
  obj["streamId"] = std::to_string(streamId);
  obj["streamType"] = std::to_string(streamType);
  return obj;
}

folly::dynamic MOQTExtensionHeader::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["headerType"] = headerType;
  if (headerValue.hasValue()) {
    obj["headerValue"] = std::to_string(headerValue.value());
  }
  if (headerLength.hasValue()) {
    obj["headerLength"] = std::to_string(headerLength.value());
  }
  if (payload) {
    obj["payload"] = std::string(
        reinterpret_cast<const char*>(payload->data()), payload->length());
  }
  return obj;
}

folly::dynamic MOQTObjectDatagramCreated::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["track_alias"] = trackAlias;
  obj["group_id"] = groupId;

  if (objectId.hasValue()) {
    obj["object_id"] = objectId.value();
  }

  obj["publisher_priority"] = publisherPriority;

  if (extensionHeadersLength.hasValue()) {
    obj["extension_headers_length"] = extensionHeadersLength.value();
  }

  if (!extensionHeaders.empty()) {
    std::vector<folly::dynamic> headerObjects;
    headerObjects.reserve(extensionHeaders.size());
    for (auto& header : extensionHeaders) {
      headerObjects.push_back(header.toDynamic());
    }
    obj["extension_headers"] = folly::dynamic::array(headerObjects);
  }

  if (objectStatus.hasValue()) {
    obj["object_status"] = objectStatus.value();
  }

  if (objectPayload) {
    obj["object_payload"] = std::string(
        reinterpret_cast<const char*>(objectPayload->data()),
        objectPayload->length());
  }

  obj["end_of_group"] = endOfGroup;

  return obj;
}

folly::dynamic MOQTObjectDatagramParsed::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["track_alias"] = trackAlias;
  obj["group_id"] = groupId;

  if (objectId.hasValue()) {
    obj["object_id"] = objectId.value();
  }

  obj["publisher_priority"] = publisherPriority;

  if (extensionHeadersLength.hasValue()) {
    obj["extension_headers_length"] = extensionHeadersLength.value();
  }

  if (!extensionHeaders.empty()) {
    std::vector<folly::dynamic> headerObjects;
    headerObjects.reserve(extensionHeaders.size());
    for (auto& header : extensionHeaders) {
      headerObjects.push_back(header.toDynamic());
    }
    obj["extension_headers"] = folly::dynamic::array(headerObjects);
  }

  if (objectStatus.hasValue()) {
    obj["object_status"] = objectStatus.value();
  }

  if (objectPayload) {
    obj["object_payload"] = std::string(
        reinterpret_cast<const char*>(objectPayload->data()),
        objectPayload->length());
  }

  obj["end_of_group"] = endOfGroup;

  return obj;
}

folly::dynamic MOQTSubgroupHeaderCreated::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["stream_id"] = streamId;
  obj["track_alias"] = trackAlias;
  obj["group_id"] = groupId;

  if (subgroupId.hasValue()) {
    obj["subgroup_id"] = subgroupId.value();
  }

  obj["publisher_priority"] = publisherPriority;
  obj["contains_end_of_group"] = containsEndOfGroup;
  obj["extensions_present"] = extensionsPresent;

  return obj;
}

folly::dynamic MOQTSubgroupHeaderParsed::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["stream_id"] = streamId;
  obj["track_alias"] = trackAlias;
  obj["group_id"] = groupId;

  if (subgroupId.hasValue()) {
    obj["subgroup_id"] = subgroupId.value();
  }

  obj["publisher_priority"] = publisherPriority;
  obj["contains_end_of_group"] = containsEndOfGroup;
  obj["extensions_present"] = extensionsPresent;

  return obj;
}

folly::dynamic MOQTSubgroupObjectCreated::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["streamId"] = std::to_string(streamId);
  if (groupId.hasValue()) {
    obj["groupId"] = std::to_string(groupId.value());
  }
  if (subgroupId.hasValue()) {
    obj["subgroupId"] = std::to_string(subgroupId.value());
  }
  obj["objectId"] = std::to_string(objectId);
  obj["extensionHeadersLength"] = std::to_string(extensionHeadersLength);
  if (!extensionHeaders.empty()) {
    std::vector<folly::dynamic> headerObjects;
    headerObjects.reserve(extensionHeaders.size());
    for (auto& header : extensionHeaders) {
      headerObjects.push_back(header.toDynamic());
    }
    obj["extensionHeaders"] = folly::dynamic::array(headerObjects);
  }
  obj["objectPayloadLength"] = std::to_string(objectPayloadLength);
  if (objectStatus.value()) {
    obj["objectStatus"] = std::to_string(objectStatus.value());
  }
  if (objectPayload) {
    obj["objectPayload"] = std::string(
        reinterpret_cast<const char*>(objectPayload->data()),
        objectPayload->length());
  }
  return obj;
}

folly::dynamic MOQTSubgroupObjectParsed::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["streamId"] = std::to_string(streamId);
  if (groupId.hasValue()) {
    obj["groupId"] = std::to_string(groupId.value());
  }
  if (subgroupId.hasValue()) {
    obj["subgroupId"] = std::to_string(subgroupId.value());
  }
  obj["objectId"] = std::to_string(objectId);
  obj["extensionHeadersLength"] = std::to_string(extensionHeadersLength);
  if (!extensionHeaders.empty()) {
    std::vector<folly::dynamic> headerObjects;
    headerObjects.reserve(extensionHeaders.size());
    for (auto& header : extensionHeaders) {
      headerObjects.push_back(header.toDynamic());
    }
    obj["extensionHeaders"] = folly::dynamic::array(headerObjects);
  }
  obj["objectPayloadLength"] = std::to_string(objectPayloadLength);
  if (objectStatus.hasValue()) {
    obj["objectStatus"] = std::to_string(objectStatus.value());
  }
  if (objectPayload) {
    obj["objectPayload"] = std::string(
        reinterpret_cast<const char*>(objectPayload->data()),
        objectPayload->length());
  }
  return obj;
}

folly::dynamic MOQTFetchHeaderCreated::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["streamId"] = std::to_string(streamId);
  obj["subscribeId"] = std::to_string(subscribeId);
  return obj;
}

folly::dynamic MOQTFetchHeaderParsed::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["streamId"] = std::to_string(streamId);
  obj["subscribeId"] = std::to_string(subscribeId);
  return obj;
}

folly::dynamic MOQTFetchObjectCreated::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["streamId"] = std::to_string(streamId);
  obj["groupId"] = std::to_string(groupId);
  obj["subgroupId"] = std::to_string(subgroupId);
  obj["objectId"] = std::to_string(objectId);
  obj["publisherPriority"] = std::to_string(publisherPriority);
  obj["extensionHeadersLength"] = std::to_string(extensionHeadersLength);
  if (!extensionHeaders.empty()) {
    std::vector<folly::dynamic> headerObjects;
    headerObjects.reserve(extensionHeaders.size());
    for (auto& header : extensionHeaders) {
      headerObjects.push_back(header.toDynamic());
    }
    obj["extensionHeaders"] = folly::dynamic::array(headerObjects);
  }
  obj["objectPayloadLength"] = std::to_string(objectPayloadLength);
  if (objectStatus.hasValue()) {
    obj["objectStatus"] = std::to_string(objectStatus.value());
  }
  if (objectPayload) {
    obj["objectPayload"] = std::string(
        reinterpret_cast<const char*>(objectPayload->data()),
        objectPayload->length());
  }
  return obj;
}

folly::dynamic MOQTFetchObjectParsed::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["streamId"] = std::to_string(streamId);
  obj["groupId"] = std::to_string(groupId);
  obj["subgroupId"] = std::to_string(subgroupId);
  obj["objectId"] = std::to_string(objectId);
  obj["publisherPriority"] = std::to_string(publisherPriority);
  obj["extensionHeadersLength"] = std::to_string(extensionHeadersLength);
  if (!extensionHeaders.empty()) {
    std::vector<folly::dynamic> headerObjects;
    headerObjects.reserve(extensionHeaders.size());
    for (auto& header : extensionHeaders) {
      headerObjects.push_back(header.toDynamic());
    }
    obj["extensionHeaders"] = folly::dynamic::array(headerObjects);
  }
  obj["objectPayloadLength"] = std::to_string(objectPayloadLength);
  if (objectStatus.hasValue()) {
    obj["objectStatus"] = std::to_string(objectStatus.value());
  }
  if (objectPayload) {
    obj["objectPayload"] = std::string(
        reinterpret_cast<const char*>(objectPayload->data()),
        objectPayload->length());
  }
  return obj;
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
  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(parameters.size());
  for (auto& param : parameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["parameters"] = folly::dynamic::array(paramObjects);
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
  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(parameters.size());
  for (auto& param : parameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["parameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTPublishError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["request_id"] = requestId;
  obj["error_code"] = errorCode;
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reason_bytes"] = reasonBytes.value();
  }
  return obj;
}

} // namespace moxygen
