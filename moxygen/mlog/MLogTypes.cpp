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

folly::dynamic MOQTSubscribe::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  obj["trackAlias"] = trackAlias;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["trackName"] = parseTrackName(trackName);
  obj["subscriberPriority"] = std::to_string(subscriberPriority);
  obj["groupOrder"] = std::to_string(groupOrder);
  obj["filterType"] = std::to_string(filterType);

  if (startGroup.hasValue()) {
    obj["startGroup"] = std::to_string(startGroup.value());
  }
  if (startObject.hasValue()) {
    obj["startObject"] = std::to_string(startObject.value());
  }
  if (endGroup.hasValue()) {
    obj["endGroup"] = std::to_string(endGroup.value());
  }
  obj["numberOfParameters"] = numberOfParameters;

  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(subscribeParameters.size());
  for (auto& param : subscribeParameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["subscribeParameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTSubscribeUpdate::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  obj["startGroup"] = std::to_string(startGroup);
  obj["startObject"] = std::to_string(startObject);
  obj["endGroup"] = std::to_string(endGroup);
  obj["subscriberPriority"] = std::to_string(subscriberPriority);
  obj["numberOfParameters"] = std::to_string(numberOfParameters);

  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(subscribeParameters.size());
  for (auto& param : subscribeParameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["subscribeParameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTUnsubscribe::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  return obj;
}

folly::dynamic MOQTFetch::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  obj["subscriberPriority"] = std::to_string(subscriberPriority);
  obj["groupOrder"] = std::to_string(groupOrder);
  obj["fetchType"] = std::to_string(fetchType);
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  if (trackName.hasValue()) {
    obj["trackName"] = parseTrackName(trackName.value());
  }
  if (startGroup.hasValue()) {
    obj["startGroup"] = std::to_string(startGroup.value());
  }
  if (startObject.hasValue()) {
    obj["startObject"] = std::to_string(startObject.value());
  }
  if (endGroup.hasValue()) {
    obj["endGroup"] = std::to_string(endGroup.value());
  }
  if (endObject.hasValue()) {
    obj["endObject"] = std::to_string(endObject.value());
  }
  if (joiningSubscribeId.hasValue()) {
    obj["joiningSubscribeId"] = std::to_string(joiningSubscribeId.value());
  }
  if (precedingGroupOffset.hasValue()) {
    obj["precedingGroupOffset"] = std::to_string(precedingGroupOffset.value());
  }
  obj["numberOfParameters"] = std::to_string(numberOfParameters);
  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(parameters.size());
  for (auto& param : parameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["parameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTFetchCancel::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  return obj;
}

folly::dynamic MOQTAnnounceOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  return obj;
}

folly::dynamic MOQTAnnounceError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["errorCode"] = std::to_string(errorCode);
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reasonBytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTAnnounceCancel::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["errorCode"] = std::to_string(errorCode);
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reasonBytes"] = reasonBytes.value();
  }
  return obj;
}

folly::dynamic MOQTTrackStatusRequest::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["trackName"] = parseTrackName(trackName);
  return obj;
}

folly::dynamic MOQTSubscribeAnnounces::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["numberOfParameters"] = std::to_string(numberOfParameters);
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
  obj["subscribeId"] = std::to_string(subscribeId);
  obj["expires"] = std::to_string(expires);
  obj["groupOrder"] = std::to_string(groupOrder);
  obj["contentExists"] = std::to_string(contentExists);
  if (largestGroupId.has_value()) {
    obj["largestGroupId"] = std::to_string(largestGroupId.value());
  }
  if (largestObjectId.has_value()) {
    obj["largestObjectId"] = std::to_string(largestObjectId.value());
  }
  obj["numberOfParameters"] = std::to_string(numberOfParameters);
  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(subscribeParameters.size());
  for (auto& param : subscribeParameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["subscribeParameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTSubscribeError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  obj["errorCode"] = std::to_string(errorCode);
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reasonBytes"] = reasonBytes.value();
  }
  if (trackAlias.hasValue()) {
    obj["trackAlias"] = std::to_string(trackAlias.value());
  }
  return obj;
}

folly::dynamic MOQTFetchOk::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  obj["groupOrder"] = std::to_string(groupOrder);
  obj["endOfTrack"] = std::to_string(endOfTrack);
  obj["largestGroupId"] = std::to_string(largestGroupId);
  obj["largestObjectId"] = std::to_string(largestObjectId);
  obj["numberOfParameters"] = std::to_string(numberOfParameters);
  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(subscribeParameters.size());
  for (auto& param : subscribeParameters) {
    paramObjects.push_back(param.toDynamic());
  }
  obj["subscribeParameters"] = folly::dynamic::array(paramObjects);
  return obj;
}

folly::dynamic MOQTFetchError::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["subscribeId"] = std::to_string(subscribeId);
  obj["errorCode"] = std::to_string(errorCode);
  if (reason.hasValue()) {
    obj["reason"] = reason.value();
  }
  if (reasonBytes.hasValue()) {
    obj["reasonBytes"] = reasonBytes.value();
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

folly::dynamic MOQTMaxSubscribeId::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["maxSubscribeId"] = std::to_string(subscribeId);
  return obj;
}

folly::dynamic MOQTSubscribesBlocked::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  obj["maximumSubscribeId"] = std::to_string(maximumSubscribeId);
  return obj;
}

folly::dynamic MOQTAnnounce::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["type"] = type;
  auto trackNamespaceStr = parseTrackNamespace(trackNamespace);
  obj["trackNamespace"] =
      folly::dynamic::array(trackNamespaceStr.begin(), trackNamespaceStr.end());
  obj["numberOfParameters"] = std::to_string(numberOfParameters);
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

} // namespace moxygen
