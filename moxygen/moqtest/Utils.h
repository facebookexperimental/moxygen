/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Expected.h>
#include "moxygen/MoQFramer.h"
#include "moxygen/moqtest/Types.h"

namespace moxygen {

folly::Expected<folly::Unit, std::runtime_error> validateMoQTestParameters(
    MoQTestParameters* track);

folly::Expected<moxygen::TrackNamespace, std::runtime_error>
convertMoqTestParamToTrackNamespace(MoQTestParameters* params);

folly::Expected<moxygen::MoQTestParameters, std::runtime_error>
convertTrackNamespaceToMoqTestParam(TrackNamespace* track);

std::vector<Extension> getExtensions(
    int integerExtensionId,
    int variableExtensionId);

int getObjectSize(int objectId, MoQTestParameters* params);

bool validatePayload(int objectSize, std::string payload);

bool validateExtensionSize(
    std::vector<Extension> extensions,
    MoQTestParameters* params);
bool validateIntExtensions(Extension intExt, MoQTestParameters* params);
bool validateVarExtensions(Extension varExt, MoQTestParameters* params);

} // namespace moxygen
