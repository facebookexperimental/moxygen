// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/mlog/MLogTypes.h"

namespace moxygen {

folly::dynamic MOQTControlMessageCreated::toDynamic() const {
  folly::dynamic jsonObject = folly::dynamic::object;
  jsonObject["streamId"] = std::to_string(this->streamId);

  if (this->length.has_value()) {
    jsonObject["length"] = std::to_string(this->length.value());
  }
  folly::dynamic controlObject = folly::dynamic::object;
  controlObject["clientSetup"] = this->message->toDynamic();
  jsonObject["message"] = controlObject;

  return jsonObject;
}

folly::dynamic MOQTClientSetupMessage::toDynamic() const {
  folly::dynamic clientSetupObj = folly::dynamic::object;
  clientSetupObj["type"] = this->type;
  clientSetupObj["numberOfSupportedVersions"] = this->numberOfSupportedVersions;
  clientSetupObj["supportedVersions"] = folly::dynamic::array(
      this->supportedVersions.begin(), this->supportedVersions.end());
  clientSetupObj["numberOfParameters"] = this->numberOfParameters;

  std::vector<folly::dynamic> paramObjects;
  paramObjects.reserve(this->setupParameters.size());
  for (auto& param : this->setupParameters) {
    paramObjects.push_back(param.toDynamic());
  }
  clientSetupObj["setupParameters"] = folly::dynamic::array(paramObjects);
  return clientSetupObj;
}

folly::dynamic MOQTParameter::toDynamic() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = this->name;

  if (this->type == MOQTParameterType::INT) {
    obj["value"] = std::to_string(this->intValue);
  } else if (this->type == MOQTParameterType::STRING) {
    obj["value"] = this->stringValue;
  } else {
    obj["value"] = "Invalid Data Type For MOQTParameter";
  }

  return obj;
}

} // namespace moxygen
