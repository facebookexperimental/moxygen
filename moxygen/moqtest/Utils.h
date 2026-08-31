/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
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

// How the objects of a track with `preference` are grouped into subgroups when
// they are delivered over a FETCH.  A datagram object carries no subgroup: from
// draft 16 the FETCH object sets the datagram flag and omits the subgroup
// field, and the receiver reports 0, so the track arrives shaped like one
// subgroup per group.  Publisher and subscriber must agree on this or the
// subscriber will reject a conformant response.
ForwardingPreference fetchForwardingPreference(ForwardingPreference preference);

bool validatePayload(int objectSize, std::string payload);

bool validateExtensionSize(
    std::vector<Extension> extensions,
    MoQTestParameters* params);
bool validateIntExtensions(Extension intExt, MoQTestParameters* params);
bool validateVarExtensions(Extension varExt, MoQTestParameters* params);

} // namespace moxygen
