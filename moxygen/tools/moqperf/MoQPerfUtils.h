/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once
#include "moxygen/MoQFramer.h"
#include "moxygen/tools/moqperf/MoQPerfParams.h"

namespace moxygen {

bool validateMoQPerfParams(MoQPerfParams& params);

TrackNamespace convertMoQPerfParamsToTrackNamespace(MoQPerfParams& params);
MoQPerfParams convertTrackNamespaceToMoQPerfParams(TrackNamespace& ns);

} // namespace moxygen
