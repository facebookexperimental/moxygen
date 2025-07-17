/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQFramer.h>

namespace moxygen {

struct MoQBenchmarkParams {
  size_t objectSize;
  size_t numObjectsPerGroup;
  size_t numGroups;
};

/*
 * We're assuming that the track namespace is in the following format:
 * <objectSize>,<numObjectsPerGroup>,<numGroups>
 * We're assuming, additionally, that the track name is "benchmark"
 */
TrackNamespace convertToTrackNamespace(const MoQBenchmarkParams& params);

folly::Expected<MoQBenchmarkParams, std::runtime_error>
convertToMoqBenchmarkParams(const TrackNamespace& track);

} // namespace moxygen
