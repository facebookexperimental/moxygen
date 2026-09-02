/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moqtest/Utils.h"

#include <folly/Conv.h>
#include <folly/String.h>
#include <algorithm>
#include <chrono>
#include <optional>

namespace moxygen {

const int kNumParams = 16;
const std::string kField0 = "moq-test-00";
const int kTestVariableExtensionMax = 20;

folly::Expected<folly::Unit, std::runtime_error> validateMoQTestParameters(
    const MoQTestParameters& track) {
  // Check if Forwarding Preference is valid (0-3) (Tuple Field 1)
  int forwardingPreferenceNumber = static_cast<int>(track.forwardingPreference);
  if (!(0 <= forwardingPreferenceNumber && forwardingPreferenceNumber <= 3)) {
    return folly::makeUnexpected(
        std::runtime_error("Invalid Forwarding Preference Value"));
  }

  // Check if Start Group and Start Object are less than last group and last
  // object (Tuple Fields 2 & 3)
  if (track.startGroup > track.lastGroupInTrack) {
    return folly::makeUnexpected(
        std::runtime_error("Start Group Exceeds Last Group in Track"));
  }

  if (track.startObject > track.lastObjectInTrack) {
    return folly::makeUnexpected(
        std::runtime_error("Start Object Exceeds Last Object in Track"));
  }

  // Check if Last Group in track field is within valid range (less than max)
  // (Tuple Field 4)
  if (track.lastGroupInTrack > static_cast<uint64_t>(pow(2, 62)) - 1) {
    return folly::makeUnexpected(
        std::runtime_error(
            "Last Group In Track field exceeds maximum allowed groups"));
  }

  // Check if Last Object is Less than Maximum allowed value (Tuple Field 5)
  uint64_t highestExpectedObjectId =
      (track.objectsPerGroup + static_cast<int>(track.sendEndOfGroupMarkers)) *
      track.objectIncrement;

  if (track.lastObjectInTrack > highestExpectedObjectId) {
    return folly::makeUnexpected(
        std::runtime_error(
            "Last Object In Track field exceeds maximum allowed objects"));
  }

  // Checks for Tuple Field 10
  if (track.groupIncrement == 0) {
    return folly::makeUnexpected(
        std::runtime_error("Group Increment Cannot Be Zero"));
  }

  // Checks for Tuple Field 11
  if (track.objectIncrement == 0) {
    return folly::makeUnexpected(
        std::runtime_error("Object Increment Cannot Be Zero"));
  }

  // Tuple Field 9. Bounds how long a conformance run can sit waiting on the
  // next object, on the publisher as well as the subscriber.
  if (track.objectFrequency > kMaxObjectFrequencyMs) {
    return folly::makeUnexpected(
        std::runtime_error("Object Frequency Exceeds One Minute"));
  }

  return folly::Unit();
}

folly::Expected<moxygen::TrackNamespace, std::runtime_error>
convertMoqTestParamToTrackNamespace(const MoQTestParameters& params) {
  auto validateResult = validateMoQTestParameters(params);
  if (!validateResult) {
    return folly::makeUnexpected(validateResult.error());
  }

  TrackNamespace trackNamespace({
      kField0,
      std::to_string(static_cast<int>(params.forwardingPreference)),
      std::to_string(params.startGroup),
      std::to_string(params.startObject),
      std::to_string(params.lastGroupInTrack),
      std::to_string(params.lastObjectInTrack),
      std::to_string(params.objectsPerGroup),
      std::to_string(params.sizeOfObjectZero),
      std::to_string(params.sizeOfObjectGreaterThanZero),
      std::to_string(params.objectFrequency),
      std::to_string(params.groupIncrement),
      std::to_string(params.objectIncrement),
      std::to_string(static_cast<int>(params.sendEndOfGroupMarkers)),
      std::to_string((params.testIntegerExtension)),
      std::to_string((params.testVariableExtension)),
      std::to_string(params.publisherDeliveryTimeout),
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
    params.testIntegerExtension = (std::stoi((track->trackNamespace)[13]));
    params.testVariableExtension = (std::stoi((track->trackNamespace)[14]));
    params.publisherDeliveryTimeout = std::stoull((track->trackNamespace)[15]);
  } catch (const std::exception& e) {
    return folly::makeUnexpected(
        std::runtime_error(
            "Error Converting TrackNamespace String value to Digit: " +
            std::string(e.what())));
  }

  // Check if the new params is Valid
  auto res = validateMoQTestParameters(params);
  if (res.hasError()) {
    return folly::makeUnexpected(
        std::runtime_error("MoQTestParameters was created, but is invalid."));
  }

  return params;
}

std::vector<Extension> getExtensions(
    int integerExtensionId,
    int variableExtensionId,
    bool includeTimestamp) {
  std::vector<Extension> extensions;
  if (integerExtensionId >= 0) {
    uint64_t randomNumber = std::rand();
    Extension ext{static_cast<uint64_t>(2 * integerExtensionId), randomNumber};
    extensions.push_back(ext);
  }
  if (variableExtensionId >= 0) {
    uint64_t randomNumber = std::rand() % kTestVariableExtensionMax + 1;
    auto buf = folly::IOBuf::create(randomNumber);
    buf->append(randomNumber);
    Extension ext{
        static_cast<uint64_t>(2 * variableExtensionId + 1), {std::move(buf)}};
    extensions.push_back(ext);
  }
  if (includeTimestamp) {
    uint64_t timestampMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();
    extensions.emplace_back(kTimestampExtensionType, timestampMs);
  }
  return extensions;
}

uint8_t publisherPriorityForGroup(uint64_t groupNumber) {
  return kMoQTestPublisherPriority + (groupNumber % 2);
}

uint64_t priorGroupGap(const MoQTestParameters& params, uint64_t groupID) {
  return groupID == params.startGroup ? params.startGroup
                                      : params.groupIncrement - 1;
}

uint64_t priorObjectGap(const MoQTestParameters& params, uint64_t objectID) {
  return objectID == params.startObject ? params.startObject
                                        : params.objectIncrement - 1;
}

std::vector<Extension> getGapExtensions(
    const MoQTestParameters& params,
    uint64_t groupID,
    uint64_t objectID) {
  std::vector<Extension> extensions;
  // Only tracks that already declare a test extension.  The subgroup header's
  // extension bit is a property of the whole track, so a gap on a track
  // without one would cost the suite its no-extensions header coverage.
  if (params.testIntegerExtension < 0 && params.testVariableExtension < 0) {
    return extensions;
  }
  // Only the group's first object carries the group gap; a receiver rejects a
  // group whose objects disagree about it.
  if (objectID == params.startObject) {
    auto groupGap = priorGroupGap(params, groupID);
    if (groupGap > 0) {
      extensions.emplace_back(kPriorGroupIdGapExtensionType, groupGap);
    }
  }
  auto objectGap = priorObjectGap(params, objectID);
  if (objectGap > 0) {
    extensions.emplace_back(kPriorObjectIdGapExtensionType, objectGap);
  }
  return extensions;
}

bool validateGapExtensions(
    const Extensions& extensions,
    const MoQTestParameters& params,
    uint64_t groupID,
    uint64_t objectID) {
  return extensions.getImmutableExtensions() ==
      getGapExtensions(params, groupID, objectID);
}

int getObjectSize(uint64_t objectId, MoQTestParameters* params) {
  if (objectId == params->startObject) {
    return params->sizeOfObjectZero;
  } else {
    return params->sizeOfObjectGreaterThanZero;
  }
}

namespace {

bool trackHasExtensions(
    const MoQTestParameters& params,
    bool includeTimestampExtension) {
  return params.testIntegerExtension >= 0 ||
      params.testVariableExtension >= 0 || includeTimestampExtension;
}

uint64_t firstObjectInSubgroup(
    const MoQTestParameters& params,
    uint64_t subgroupID) {
  switch (params.forwardingPreference) {
    case ForwardingPreference::ONE_SUBGROUP_PER_OBJECT:
      return subgroupID;
    case ForwardingPreference::TWO_SUBGROUPS_PER_GROUP:
      // Subgroup 0 carries the even object IDs and subgroup 1 the odd ones, so
      // the subgroup that doesn't match startObject's parity starts one
      // increment later.
      return (subgroupID % 2 == params.startObject % 2)
          ? params.startObject
          : params.startObject + params.objectIncrement;
    case ForwardingPreference::ONE_SUBGROUP_PER_GROUP:
    case ForwardingPreference::DATAGRAM:
      break;
  }
  return params.startObject;
}

} // namespace

bool subgroupCarriesLastObject(
    const MoQTestParameters& params,
    uint64_t subgroupID) {
  switch (params.forwardingPreference) {
    case ForwardingPreference::ONE_SUBGROUP_PER_OBJECT:
      return subgroupID == lastObjectInGroup(params);
    case ForwardingPreference::TWO_SUBGROUPS_PER_GROUP:
      return subgroupID == lastObjectInGroup(params) % 2;
    case ForwardingPreference::ONE_SUBGROUP_PER_GROUP:
    case ForwardingPreference::DATAGRAM:
      break;
  }
  // The group's only subgroup necessarily carries its last object
  return true;
}

uint64_t lastObjectInGroup(const MoQTestParameters& params) {
  if (params.lastObjectInTrack <= params.startObject) {
    return params.startObject;
  }
  auto steps =
      (params.lastObjectInTrack - params.startObject) / params.objectIncrement;
  return params.startObject + steps * params.objectIncrement;
}

BeginSubgroupOptions subgroupOptionsFor(
    const MoQTestParameters& params,
    uint64_t subgroupID,
    bool includeTimestampExtension) {
  const auto firstObject = firstObjectInSubgroup(params, subgroupID);
  BeginSubgroupOptions options;
  options.subgroupIDFormat = subgroupID == 0 ? SubgroupIDFormat::Zero
      : subgroupID == firstObject            ? SubgroupIDFormat::FirstObject
                                             : SubgroupIDFormat::Present;
  options.includeExtensions =
      trackHasExtensions(params, includeTimestampExtension);
  options.containsLastInGroup = subgroupCarriesLastObject(params, subgroupID);
  // The test server always opens a fresh subgroup and writes it from its first
  // object; it never resumes one published elsewhere.
  options.beginsWithFirstObject = true;
  return options;
}

namespace {

// Smallest value on the grid `base + k * increment` that is >= `value`.
uint64_t snapUp(uint64_t value, uint64_t base, uint64_t increment) {
  if (value <= base) {
    return base;
  }
  return base + ((value - base + increment - 1) / increment) * increment;
}

// Largest value on the grid `base + k * increment` that is <= `value`.
// `value` must be >= `base`.
uint64_t snapDown(uint64_t value, uint64_t base, uint64_t increment) {
  return base + ((value - base) / increment) * increment;
}

// The (group, object) lattice a set of track parameters generates.
struct TrackGrid {
  uint64_t firstGroup;
  uint64_t lastGroup;
  uint64_t groupIncrement;
  uint64_t firstObject;
  uint64_t lastObject;
  uint64_t objectIncrement;
};

TrackGrid trackGrid(const MoQTestParameters& params) {
  return TrackGrid{
      params.startGroup,
      snapDown(
          params.lastGroupInTrack, params.startGroup, params.groupIncrement),
      params.groupIncrement,
      params.startObject,
      lastObjectInGroup(params),
      params.objectIncrement};
}

// First location the grid generates at or after `loc`, or nullopt if `loc` is
// past the end of the track.
std::optional<AbsoluteLocation> snapLocationUp(
    const TrackGrid& grid,
    AbsoluteLocation loc) {
  auto group = snapUp(loc.group, grid.firstGroup, grid.groupIncrement);
  uint64_t object = grid.firstObject;
  if (group == loc.group) {
    object = snapUp(loc.object, grid.firstObject, grid.objectIncrement);
    if (object > grid.lastObject) {
      group += grid.groupIncrement;
      object = grid.firstObject;
    }
  }
  if (group > grid.lastGroup) {
    return std::nullopt;
  }
  return AbsoluteLocation{group, object};
}

// On the wire an end object of 0 asks for all of the end group; every other
// value is already one past the last object.
AbsoluteLocation toExclusiveEnd(AbsoluteLocation wireEnd) {
  if (wireEnd.object != 0) {
    return wireEnd;
  }
  return wireEnd.nextGroup().value_or(kLocationMax);
}

// Last location the grid generates at or before `loc`, or nullopt if `loc` is
// before the start of the track.
std::optional<AbsoluteLocation> snapLocationDown(
    const TrackGrid& grid,
    AbsoluteLocation loc) {
  if (loc.group < grid.firstGroup) {
    return std::nullopt;
  }
  if (loc.group > grid.lastGroup) {
    return AbsoluteLocation{grid.lastGroup, grid.lastObject};
  }
  auto group = snapDown(loc.group, grid.firstGroup, grid.groupIncrement);
  if (group == loc.group) {
    if (loc.object >= grid.firstObject) {
      return AbsoluteLocation{
          group,
          std::min(
              snapDown(loc.object, grid.firstObject, grid.objectIncrement),
              grid.lastObject)};
    }
    if (group == grid.firstGroup) {
      return std::nullopt;
    }
    group -= grid.groupIncrement;
  }
  return AbsoluteLocation{group, grid.lastObject};
}

} // namespace

ForwardingPreference fetchForwardingPreference(
    ForwardingPreference preference) {
  return preference == ForwardingPreference::DATAGRAM
      ? ForwardingPreference::ONE_SUBGROUP_PER_GROUP
      : preference;
}

uint64_t fetchSubgroupID(const MoQTestParameters& params, uint64_t objectID) {
  switch (fetchForwardingPreference(params.forwardingPreference)) {
    case ForwardingPreference::ONE_SUBGROUP_PER_OBJECT:
      return objectID;
    case ForwardingPreference::TWO_SUBGROUPS_PER_GROUP:
      return objectID % 2;
    case ForwardingPreference::ONE_SUBGROUP_PER_GROUP:
    case ForwardingPreference::DATAGRAM:
      break;
  }
  return 0;
}

StandaloneFetch wholeTrackFetch(const MoQTestParameters& params) {
  return StandaloneFetch(
      AbsoluteLocation{params.startGroup, params.startObject},
      AbsoluteLocation{params.lastGroupInTrack, 0});
}

AbsoluteLocation fetchEndLocation(
    const MoQTestParameters& params,
    const StandaloneFetch& fetch) {
  const auto grid = trackGrid(params);
  const AbsoluteLocation trackStart{grid.firstGroup, grid.firstObject};
  const AbsoluteLocation trackEnd{grid.lastGroup, grid.lastObject + 1};
  const auto end = std::min(toExclusiveEnd(fetch.end), trackEnd);
  // A range that ends before the track begins delivers nothing, the mirror of
  // one that begins after the track ends.
  if (end <= trackStart) {
    return fetch.start;
  }
  return std::max(end, fetch.start);
}

MoQTestFetchWindow resolveFetchWindow(
    const MoQTestParameters& params,
    const StandaloneFetch& fetch) {
  const auto grid = trackGrid(params);
  MoQTestFetchWindow window;
  window.firstObjectPerGroup = grid.firstObject;
  window.lastObjectPerGroup = grid.lastObject;

  // toExclusiveEnd folds in "an end object of 0 selects the whole end group";
  // the location before that end is the last one requested.
  const auto requestedLast = toExclusiveEnd(fetch.end).prev();

  const auto first = snapLocationUp(grid, fetch.start);
  const auto last =
      requestedLast ? snapLocationDown(grid, *requestedLast) : std::nullopt;
  if (!first || !last || *last < *first) {
    return window;
  }

  window.first = *first;
  window.last = *last;
  window.endOfTrack =
      *last == AbsoluteLocation{grid.lastGroup, grid.lastObject};
  return window;
}

MoQTestFetchWindow resolveFetchWindow(const MoQTestParameters& params) {
  return resolveFetchWindow(params, wholeTrackFetch(params));
}

std::set<std::pair<uint64_t, uint64_t>> expectedObjectsIn(
    const MoQTestParameters& params,
    const MoQTestFetchWindow& window) {
  std::set<std::pair<uint64_t, uint64_t>> objects;
  for (uint64_t group = window.first.group; group <= window.last.group;
       group += params.groupIncrement) {
    const uint64_t lastObject = window.lastObjectIn(group);
    for (uint64_t object = window.firstObjectIn(group); object <= lastObject;
         object += params.objectIncrement) {
      objects.insert({group, object});
    }
  }
  return objects;
}

folly::Expected<AbsoluteLocation, std::runtime_error> parseLocation(
    const std::string& value) {
  std::vector<folly::StringPiece> parts;
  folly::split(',', value, parts);
  if (parts.size() == 2) {
    auto group = folly::tryTo<uint64_t>(parts[0]);
    auto object = folly::tryTo<uint64_t>(parts[1]);
    if (group.hasValue() && object.hasValue()) {
      return AbsoluteLocation{group.value(), object.value()};
    }
  }
  return folly::makeUnexpected(
      std::runtime_error(
          folly::to<std::string>(
              "expected \"group,object\", got \"", value, "\"")));
}

// Extension Validation Helper Functions
bool validateExtensionSize(
    std::vector<Extension> extensions,
    MoQTestParameters* params) {
  return extensions.size() ==
      static_cast<size_t>(params->testIntegerExtension >= 0) +
      static_cast<size_t>(params->testVariableExtension >= 0);
}

bool validateIntExtensions(Extension intExt, MoQTestParameters* params) {
  if (params->testIntegerExtension < 0) {
    return false;
  }
  return intExt.type == static_cast<uint64_t>(2 * params->testIntegerExtension);
}

bool validateVarExtensions(Extension varExt, MoQTestParameters* params) {
  if (params->testVariableExtension < 0) {
    return false;
  }
  return (
      varExt.type ==
      static_cast<uint64_t>(2 * params->testVariableExtension + 1));
}

// Payload Validation Helper Function
bool validatePayload(int objectSize, std::string payload) {
  int payloadLength = (payload).length();
  if (payloadLength != objectSize) {
    return false;
  }

  if (payload != std::string(payloadLength, 't')) {
    return false;
  }

  return true;
}
} // namespace moxygen
