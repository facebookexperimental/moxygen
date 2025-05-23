// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/moqtest/Utils.h"

namespace moxygen {

const int kNumParams = 16;
const std::string kField0 = "moq-test-00";

folly::Expected<folly::Unit, std::runtime_error> validateMoQTestParameters(
    MoQTestParameters* track) {
  // Check if Forwarding Preference is valid (0-3) (Tuple Field 1)
  int forwardingPreferenceNumber =
      static_cast<int>(track->forwardingPreference);
  if (!(0 <= forwardingPreferenceNumber && forwardingPreferenceNumber <= 3)) {
    return folly::makeUnexpected(
        std::runtime_error("Invalid Forwarding Preference Value"));
  }

  // Check if Start Group and Start Object are less than last group and last
  // object (Tuple Fields 2 & 3)
  if (track->startGroup > track->lastGroupInTrack) {
    return folly::makeUnexpected(
        std::runtime_error("Start Group Exceeds Last Group in Track"));
  }

  if (track->startObject > track->lastObjectInTrack) {
    return folly::makeUnexpected(
        std::runtime_error("Start Object Exceeds Last Object in Track"));
  }

  // Check if Last Group in track field is within valid range (less than max)
  // (Tuple Field 4)
  if (track->lastGroupInTrack > static_cast<uint64_t>(pow(2, 62)) - 1) {
    return folly::makeUnexpected(std::runtime_error(
        "Last Group In Track field exceeds maximum allowed groups"));
  }

  // Check if Last Object is Less than Maximum allowed value (Tuple Field 5)
  uint64_t maximumObjects =
      track->objectsPerGroup + static_cast<int>(track->sendEndOfGroupMarkers);

  if (track->lastObjectInTrack > maximumObjects) {
    return folly::makeUnexpected(std::runtime_error(
        "Last Object In Track field exceeds maximum allowed objects"));
  }

  // Checks for Tuple Field 10
  if (track->groupIncrement == 0) {
    return folly::makeUnexpected(
        std::runtime_error("Group Increment Cannot Be Zero"));
  }

  // Checks for Tuple Field 11
  if (track->objectIncrement == 0) {
    return folly::makeUnexpected(
        std::runtime_error("Object Increment Cannot Be Zero"));
  }

  return folly::Unit();
}

folly::Expected<moxygen::TrackNamespace, std::runtime_error>
convertMoqTestParamToTrackNamespace(MoQTestParameters* params) {
  if (!validateMoQTestParameters(params)) {
    return folly::makeUnexpected(
        std::runtime_error("MoQTestParameters are invalid"));
  }

  TrackNamespace trackNamespace({
      kField0,
      std::to_string(static_cast<int>(params->forwardingPreference)),
      std::to_string(params->startGroup),
      std::to_string(params->startObject),
      std::to_string(params->lastGroupInTrack),
      std::to_string(params->lastObjectInTrack),
      std::to_string(params->objectsPerGroup),
      std::to_string(params->sizeOfObjectZero),
      std::to_string(params->sizeOfObjectGreaterThanZero),
      std::to_string(params->objectFrequency),
      std::to_string(params->groupIncrement),
      std::to_string(params->objectIncrement),
      std::to_string(static_cast<int>(params->sendEndOfGroupMarkers)),
      std::to_string(static_cast<int>(params->testIntegerExtension)),
      std::to_string(static_cast<int>(params->testVariableExtension)),
      std::to_string(params->publisherDeliveryTimeout),
  });
  return trackNamespace;
}

folly::Expected<moxygen::MoQTestParameters, std::runtime_error>
convertTrackNamespaceToMoqTestParam(TrackNamespace* track) {
  // Check if TrackNamespace is of length 16
  if ((track->trackNamespace).size() != kNumParams) {
    return folly::makeUnexpected(
        std::runtime_error("TrackNamespace is not of length 16"));
  }
  // Check if TrackNamespace is correct protocol (Tuple Field 0)
  if ((track->trackNamespace)[0] != kField0) {
    return folly::makeUnexpected(
        std::runtime_error("Tuple element 0 is not moq-test-00"));
  }

  // Create Empty MoQTestParameters
  MoQTestParameters params = MoQTestParameters();

  // Assign values to appropriate positions in params
  try {
    params.forwardingPreference =
        ForwardingPreference(std::stoi((track->trackNamespace)[1]));
    params.startGroup = std::stoull((track->trackNamespace)[2]);
    params.startObject = std::stoull((track->trackNamespace)[3]);
    params.lastGroupInTrack = std::stoull((track->trackNamespace)[4]);
    params.lastObjectInTrack = std::stoull((track->trackNamespace)[5]);
    params.objectsPerGroup = std::stoull((track->trackNamespace)[6]);
    params.sizeOfObjectZero = std::stoull((track->trackNamespace)[7]);
    params.sizeOfObjectGreaterThanZero =
        std::stoull((track->trackNamespace)[8]);
    params.objectFrequency = std::stoull((track->trackNamespace)[9]);
    params.groupIncrement = std::stoull((track->trackNamespace)[10]);
    params.objectIncrement = std::stoull((track->trackNamespace)[11]);
    params.sendEndOfGroupMarkers =
        static_cast<bool>(std::stoi((track->trackNamespace)[12]));
    params.testIntegerExtension =
        static_cast<bool>(std::stoi((track->trackNamespace)[13]));
    params.testVariableExtension =
        static_cast<bool>(std::stoi((track->trackNamespace)[14]));
    params.publisherDeliveryTimeout = std::stoull((track->trackNamespace)[15]);
  } catch (const std::exception& e) {
    return folly::makeUnexpected(std::runtime_error(
        "Error Converting TrackNamespace String value to Digit: " +
        std::string(e.what())));
  }

  // Check if the new params is Valid
  auto res = validateMoQTestParameters(&params);
  if (res.hasError()) {
    return folly::makeUnexpected(
        std::runtime_error("MoQTestParameters was created, but is invalid."));
  }

  return params;
}

} // namespace moxygen
