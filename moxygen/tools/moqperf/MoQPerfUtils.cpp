/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <moxygen/tools/moqperf/MoQPerfUtils.h>

namespace moxygen {

// Update Based on Validation Needs (TBD)
bool validateMoQPerfParams(MoQPerfParams& params) {
  return params.numObjectsPerSubgroup && params.numSubgroupsPerGroup &&
      params.objectSize && params.request <= RequestType::FETCH &&
      params.request >= 0;
}

TrackNamespace convertMoQPerfParamsToTrackNamespace(MoQPerfParams& params) {
  TrackNamespace trackNamespace;

  std::vector<std::string> track(5);
  track[0] = std::to_string(params.numObjectsPerSubgroup);
  track[1] = std::to_string(params.numSubgroupsPerGroup);
  track[2] = std::to_string(params.objectSize);
  track[3] = std::to_string((int)params.sendEndOfGroupMarkers);
  track[4] = std::to_string((int)params.request);
  trackNamespace.trackNamespace = track;
  return trackNamespace;
}

MoQPerfParams convertTrackNamespaceToMoQPerfParams(TrackNamespace& ns) {
  MoQPerfParams params;

  params.numObjectsPerSubgroup = std::stoull(ns.trackNamespace[0]);
  params.numSubgroupsPerGroup = std::stoull(ns.trackNamespace[1]);
  params.objectSize = std::stoull(ns.trackNamespace[2]);
  params.sendEndOfGroupMarkers = (bool)std::stoi(ns.trackNamespace[3]);
  params.request = RequestType(std::stoi(ns.trackNamespace[4]));
  return params;
}

} // namespace moxygen
