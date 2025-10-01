/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <folly/container/F14Map.h>
#include <moxygen/MoQFramer.h>
#include <cstdint>
#include <list>
#include <optional>

namespace moxygen {

template <typename T2>
class FetchIntervalSet {
 public:
  struct Interval {
    AbsoluteLocation start; // can be updated
    AbsoluteLocation end;
    T2 value;

    bool operator==(const Interval& other) const {
      return start == other.start && end == other.end && value == other.value;
    }

    bool operator<(const Interval& other) const {
      return start < other.start;
    }
  };

  using IntervalList = std::list<Interval>;
  using IntervalMap = folly::F14FastMap<uint64_t, IntervalList>;

  // Insert a new interval at the end of the list
  typename IntervalList::iterator
  insert(AbsoluteLocation start, AbsoluteLocation end, T2 value) {
    Interval interval{start, end, value};
    auto [it, inserted] = intervals_.emplace(start.group, IntervalList{});
    auto& list = it->second;
    list.push_back(interval);
    return std::prev(list.end());
  }

  // Erase an interval from the set given the group id and the iterator
  void erase(uint64_t key, typename IntervalList::iterator it) {
    auto setIt = intervals_.find(key);
    if (setIt != intervals_.end()) {
      setIt->second.erase(it);
      if (setIt->second.empty()) {
        intervals_.erase(setIt);
      }
    }
  }

  // Get an optional iterator to the interval associated with the given key and
  // index
  std::optional<typename IntervalList::iterator> getValue(
      uint64_t key,
      AbsoluteLocation index) {
    auto values = intervals_.find(key);
    if (values == intervals_.end()) {
      return std::nullopt;
    }

    // Check in intervals to see if any of the intervals overlap with the given
    for (auto it = values->second.begin(); it != values->second.end(); ++it) {
      if (index >= it->start && index < it->end) {
        return it;
      }
    }
    return std::nullopt;
  }

 private:
  IntervalMap intervals_;
};

} // namespace moxygen
