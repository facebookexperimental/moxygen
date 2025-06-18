// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <moxygen/tools/moqperf/MoQPerfUtils.h>

namespace moxygen {

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
