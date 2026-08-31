/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <set>
#include <string>
#include <utility>
#include "moxygen/MoQConsumers.h"
#include "moxygen/MoQFramer.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

// Fixed extension type for the send timestamp (milliseconds since epoch).
// Even type => integer extension. Value is large enough to avoid collision with
// test integer extensions (which use 2 * testIntegerExtension).
constexpr uint64_t kTimestampExtensionType = 0xC000;

// The priority the server advertises as the track's publisher priority.  It is
// deliberately not kDefaultPriority, so a subscriber that mishandles the
// draft-15+ priority elision surfaces the protocol default instead of this
// value.
constexpr uint8_t kMoQTestPublisherPriority = 200;

folly::Expected<folly::Unit, std::runtime_error> validateMoQTestParameters(
    const MoQTestParameters& track);

folly::Expected<moxygen::TrackNamespace, std::runtime_error>
convertMoqTestParamToTrackNamespace(const MoQTestParameters& params);

folly::Expected<moxygen::MoQTestParameters, std::runtime_error>
convertTrackNamespaceToMoqTestParam(TrackNamespace* track);

std::vector<Extension> getExtensions(
    int integerExtensionId,
    int variableExtensionId,
    bool includeTimestamp = false);

int getObjectSize(uint64_t objectId, MoQTestParameters* params);

// The priority every subgroup and datagram of `groupNumber` is published at.
// Alternating on the group makes half the track match the advertised publisher
// priority, so it is elided from the wire, and half differ, so it is written
// explicitly.  Group parity is independent of the subgroup ID, the extension
// bit and the end-of-group bit, so both encodings appear for every other
// header shape a track produces.
uint8_t publisherPriorityForGroup(uint64_t groupNumber);

// Highest object ID a group actually carries.  This is only lastObjectInTrack
// when objectIncrement divides the range evenly.
uint64_t lastObjectInGroup(const MoQTestParameters& params);

// True when `subgroupID` is the subgroup that carries a group's last object,
// and so must signal end-of-group.
bool subgroupCarriesLastObject(
    const MoQTestParameters& params,
    uint64_t subgroupID);

// The most compact subgroup header encoding a publisher can use for
// `subgroupID` given the track parameters.
BeginSubgroupOptions subgroupOptionsFor(
    const MoQTestParameters& params,
    uint64_t subgroupID,
    bool includeTimestampExtension = false);

// An inclusive slice of a generated track, snapped onto the group and object
// grid the track parameters describe.  Only the boundary groups are partially
// covered.  A default-constructed window is empty.
struct MoQTestFetchWindow {
  AbsoluteLocation first{kLocationMax};
  AbsoluteLocation last{kLocationMin};
  uint64_t firstObjectPerGroup{0};
  uint64_t lastObjectPerGroup{0};
  bool endOfTrack{false};

  bool empty() const {
    return last < first;
  }

  uint64_t firstObjectIn(uint64_t group) const {
    return group == first.group ? first.object : firstObjectPerGroup;
  }

  uint64_t lastObjectIn(uint64_t group) const {
    return group == last.group ? last.object : lastObjectPerGroup;
  }
};

// How the objects of a track with `preference` are grouped into subgroups when
// they are delivered over a FETCH.  A datagram object carries no subgroup: from
// draft 16 the FETCH object sets the datagram flag and omits the subgroup
// field, and the receiver reports 0, so the track arrives shaped like one
// subgroup per group.  Publisher and subscriber must agree on this or the
// subscriber will reject a conformant response.
ForwardingPreference fetchForwardingPreference(ForwardingPreference preference);

// The subgroup a fetched object is delivered on, which follows the track's
// fetch forwarding preference.
uint64_t fetchSubgroupID(const MoQTestParameters& params, uint64_t objectID);

// A FETCH for everything the track will ever produce.
StandaloneFetch wholeTrackFetch(const MoQTestParameters& params);

// The End Location a FETCH_OK for `fetch` must carry: the request's own end,
// clamped to where the track stops.  Never below the request's start, because
// a receiver must close the session over an End Location it sits behind.  The
// draft defines it from the request, so it is deliberately not snapped onto the
// grid the generator walks and may sit above the last object delivered.
AbsoluteLocation fetchEndLocation(
    const MoQTestParameters& params,
    const StandaloneFetch& fetch);

// The part of the track `fetch` asks for.  FETCH end objects are exclusive,
// except that an end object of 0 selects the whole end group.  The result is
// empty when the requested range and the track don't overlap.
MoQTestFetchWindow resolveFetchWindow(
    const MoQTestParameters& params,
    const StandaloneFetch& fetch);

// The window covering the whole track, which is what SUBSCRIBE delivers.
MoQTestFetchWindow resolveFetchWindow(const MoQTestParameters& params);

// Every (group, object) pair `window` covers.
std::set<std::pair<uint64_t, uint64_t>> expectedObjectsIn(
    const MoQTestParameters& params,
    const MoQTestFetchWindow& window);

// Parses a `"group,object"` FETCH location.
folly::Expected<AbsoluteLocation, std::runtime_error> parseLocation(
    const std::string& value);

bool validatePayload(int objectSize, std::string payload);

bool validateExtensionSize(
    std::vector<Extension> extensions,
    MoQTestParameters* params);
bool validateIntExtensions(Extension intExt, MoQTestParameters* params);
bool validateVarExtensions(Extension varExt, MoQTestParameters* params);

} // namespace moxygen
