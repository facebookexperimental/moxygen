/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace moxygen {

// A most-recently-used window of group IDs, so that counting distinct groups
// tolerates interleaved or reordered streams. A group counts again only after
// kSize other groups pass it.
class RecentGroupWindow {
 public:
  static constexpr size_t kSize = 8;

  // True the first time a group is seen, or the first time since it aged out.
  [[nodiscard]] bool admit(uint64_t groupID) {
    for (size_t i = 0; i < count_; ++i) {
      if (groups_[i] == groupID) {
        std::rotate(groups_, groups_ + i, groups_ + i + 1);
        return false;
      }
    }
    count_ = std::min(count_ + 1, kSize);
    std::rotate(groups_, groups_ + kSize - 1, groups_ + kSize);
    groups_[0] = groupID;
    return true;
  }

 private:
  uint64_t groups_[kSize]{};
  size_t count_{0};
};

} // namespace moxygen
