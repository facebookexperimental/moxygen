/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/cache/MoQPriorGapRanges.h"

namespace moxygen {

MoQPriorGapRanges decodePriorGapRanges(
    uint64_t groupID,
    uint64_t objectID,
    const Extensions& extensions) {
  MoQPriorGapRanges ranges;
  if (auto gap = extensions.getIntExtension(kPriorObjectIdGapExtensionType)) {
    if (*gap > objectID) {
      ranges.valid = false;
    } else if (*gap > 0) {
      ranges.objects = MoQIdRange{.start = objectID - *gap, .end = objectID};
    }
  }
  if (auto gap = extensions.getIntExtension(kPriorGroupIdGapExtensionType)) {
    if (*gap > groupID) {
      ranges.valid = false;
    } else if (*gap > 0) {
      ranges.groups = MoQIdRange{.start = groupID - *gap, .end = groupID};
    }
  }
  return ranges;
}

} // namespace moxygen
