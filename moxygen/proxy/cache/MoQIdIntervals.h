/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <quic/common/IntervalSet.h>
#include <cstdint>
#include <optional>

namespace moxygen {

// [start, end) of object or group IDs.
struct MoQIdRange {
  uint64_t start{0};
  uint64_t end{0};

  bool operator==(const MoQIdRange&) const = default;
};

// Disjoint ranges of object or group IDs. Inserting a range merges it with
// the ranges it overlaps or touches.
class MoQIdIntervals {
 public:
  void insert(MoQIdRange range);
  // The end of the range containing id.
  std::optional<uint64_t> endContaining(uint64_t id) const;
  // The start of the first range that begins after id.
  std::optional<uint64_t> nextStartAfter(uint64_t id) const;
  std::optional<uint64_t> lastEnd() const;
  // Drops the ranges that end at or before end.
  void eraseEndingBy(uint64_t end);

  bool empty() const {
    return ranges_.empty();
  }
  void clear() {
    ranges_.clear();
  }

 private:
  // Closed intervals: [start, end) is stored as [start, end - 1].
  quic::IntervalSet<uint64_t> ranges_;
};

} // namespace moxygen
