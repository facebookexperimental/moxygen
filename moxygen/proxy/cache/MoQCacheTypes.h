/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "moxygen/MoQTypes.h"
#include "moxygen/proxy/cache/MoQCacheBudget.h"

namespace moxygen {

enum class MoQCacheCoverage {
  // The whole range can be captured as a replay plan.
  Ready,
  // Every gap can still be filled by ingress that is open now.
  AwaitIngress,
  // Some gap has no local way to be filled.
  Miss,
};

enum class MoQCacheResult {
  Recorded,
  // Not admitted, evicted, or bypassed; never an error for live delivery.
  NotCached,
  // Contradicts what the track already proved, e.g. conflicting objects.
  Malformed,
};

// An immutable complete object or status marker. The cache and accepted
// replays share it; its charge is released by whichever drops it last.
struct MoQCachedObject {
  uint64_t subgroup{0};
  ObjectStatus status{ObjectStatus::NORMAL};
  Extensions extensions;
  Payload payload;
  uint64_t payloadLength{0};
  bool datagram{false};
  MoQCacheCharge charge;
};

// An ordered snapshot of a cache-covered FETCH range. Shared entries keep
// cached objects alive if their groups are evicted before replay completes.
struct MoQReplayPlan {
  struct Entry {
    AbsoluteLocation location;
    std::shared_ptr<const MoQCachedObject> object;
  };
  // Groups in the requested order; objects ascending within a group.
  std::vector<Entry> entries;
  // Exclusive end of the requested range.
  AbsoluteLocation endLocation;
  bool endOfTrack{false};
  // The plan's own index storage; payloads share the objects' charges.
  MoQCacheCharge charge;
};

inline uint64_t payloadBytes(const Payload& payload) {
  return payload ? payload->computeChainDataLength() : 0;
}

inline bool isEndStatus(ObjectStatus status) {
  return status == ObjectStatus::END_OF_GROUP ||
      status == ObjectStatus::END_OF_TRACK;
}

// Charged in addition to payload bytes, so zero-length objects and empty
// groups cannot evade the budget.
size_t groupMetadataChargeBytes();
size_t objectChargeBytes(uint64_t payloadLength, const Extensions& extensions);
size_t planChargeBytes(size_t entryCount);

} // namespace moxygen
