/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

namespace moxygen {

#include <map>

template <typename T1, typename T2>
class FetchIntervalSet {
 public:
  struct Interval {
    T1 start; // can be updated
    T1 end;
    T2 value;
  };

  using IntervalMap = std::map<T1, Interval>;
  // Insert a new interval into the set
  typename IntervalMap::iterator insert(T1 start, T1 end, T2 value) {
    XLOG(DBG1) << "insert " << start << " " << end;
    return intervals_.emplace(start, Interval{start, end, value}).first;
  }

  void erase(typename IntervalMap::iterator it) {
    XLOG(DBG1) << "erase " << it->first;
    intervals_.erase(it);
  }

  // Get the data associated with the given index
  T2* getValue(T1 index) {
    for (auto& entry : intervals_) {
      XLOG(DBG1) << "Checking entry " << index << " " << entry.first << " "
                 << entry.second.end;
      if (index >= entry.second.start && index < entry.second.end) {
        return &entry.second.value;
      }
    }
    return nullptr;
  }

 private:
  IntervalMap intervals_;
};

} // namespace moxygen
