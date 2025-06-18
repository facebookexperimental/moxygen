// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once
#include "moxygen/MoQFramer.h"
#include "moxygen/tools/moqperf/MoQPerfParams.h"

namespace moxygen {

bool validateMoQPerfParams(MoQPerfParams& params);

TrackNamespace convertMoQPerfParamsToTrackNamespace(MoQPerfParams& params);
MoQPerfParams convertTrackNamespaceToMoQPerfParams(TrackNamespace& ns);

} // namespace moxygen
