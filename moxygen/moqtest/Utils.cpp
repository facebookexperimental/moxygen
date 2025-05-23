// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/moqtest/Utils.h"

namespace moxygen {

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

} // namespace moxygen
