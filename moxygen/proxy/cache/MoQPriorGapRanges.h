/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <optional>

#include "moxygen/MoQTypes.h"
#include "moxygen/proxy/cache/MoQIdIntervals.h"

namespace moxygen {

// What an object's Prior Object ID Gap and Prior Group ID Gap extensions
// prove: the IDs just before the object, or before its group, do not exist.
struct MoQPriorGapRanges {
  // Object IDs in the object's group.
  std::optional<MoQIdRange> objects;
  // Group IDs.
  std::optional<MoQIdRange> groups;
  // False when a gap reaches below ID 0.
  bool valid{true};
};

MoQPriorGapRanges decodePriorGapRanges(
    uint64_t groupID,
    uint64_t objectID,
    const Extensions& extensions);

} // namespace moxygen
