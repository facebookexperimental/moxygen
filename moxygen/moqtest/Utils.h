/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Expected.h>
#include "moxygen/MoQFramer.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

// Fixed extension type for the send timestamp (milliseconds since epoch).
// Even type => integer extension. Value is large enough to avoid collision with
// test integer extensions (which use 2 * testIntegerExtension).
constexpr uint64_t kTimestampExtensionType = 0xC000;

folly::Expected<folly::Unit, std::runtime_error> validateMoQTestParameters(
    const MoQTestParameters& track);

folly::Expected<moxygen::TrackNamespace, std::runtime_error>
convertMoqTestParamToTrackNamespace(const MoQTestParameters& params);

folly::Expected<moxygen::MoQTestParameters, std::runtime_error>
convertTrackNamespaceToMoqTestParam(TrackNamespace* track);

std::vector<Extension> getExtensions(
    int integerExtensionId,
    int variableExtensionId);

int getObjectSize(uint64_t objectId, MoQTestParameters* params);

bool validatePayload(int objectSize, std::string payload);

bool validateExtensionSize(
    std::vector<Extension> extensions,
    MoQTestParameters* params);
bool validateIntExtensions(Extension intExt, MoQTestParameters* params);
bool validateVarExtensions(Extension varExt, MoQTestParameters* params);

} // namespace moxygen
