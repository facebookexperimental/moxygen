/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once
#include <cstdint>

namespace moxygen {

enum RequestType : int { SUBSCRIBE = 0, FETCH = 1 };

struct MoQPerfParams {
  uint64_t numObjectsPerSubgroup{1};
  uint64_t numSubgroupsPerGroup{1};
  uint64_t objectSize{1};
  bool sendEndOfGroupMarkers{false};
  RequestType request{SUBSCRIBE};
};

} // namespace moxygen
