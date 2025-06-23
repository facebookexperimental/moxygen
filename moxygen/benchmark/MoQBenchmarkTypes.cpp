// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <moxygen/benchmark/MoQBenchmarkTypes.h>

namespace moxygen {

const std::string kTrackName = "benchmark";

TrackNamespace convertToTrackNamespace(const MoQBenchmarkParams& params) {
  TrackNamespace track;
  track.trackNamespace.push_back(folly::to<std::string>(params.objectSize));
  track.trackNamespace.push_back(
      folly::to<std::string>(params.numObjectsPerGroup));
  track.trackNamespace.push_back(folly::to<std::string>(params.numGroups));
  return track;
}

folly::Expected<MoQBenchmarkParams, std::runtime_error>
convertToMoqBenchmarkParams(const TrackNamespace& track) {
  if (track.trackNamespace.size() != 3) {
    return folly::makeUnexpected(std::runtime_error(
        "TrackNamespace must have 3 elements for benchmark"));
  }

  auto params = MoQBenchmarkParams();
  params.objectSize = folly::to<size_t>(track.trackNamespace[0]);
  params.numObjectsPerGroup = folly::to<size_t>(track.trackNamespace[1]);
  params.numGroups = folly::to<size_t>(track.trackNamespace[2]);
  return params;
}

} // namespace moxygen
