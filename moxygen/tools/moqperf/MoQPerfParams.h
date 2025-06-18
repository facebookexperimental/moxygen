// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

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
