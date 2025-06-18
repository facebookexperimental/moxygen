// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

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
