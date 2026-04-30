/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <moxygen/util/LocationIntervalSet.h>

namespace moxygen {

void LocationIntervalSet::insert(AbsoluteLocation start, AbsoluteLocation end) {
  if (end < start) {
    return;
  }

  auto mergeStart = start;
  auto mergeEnd = end;

  auto prevStart = start.prev();
  auto nextEnd = end.next();

  auto it = intervals_.lower_bound(start);

  // Check the preceding interval — its end may overlap or be adjacent.
  if (it != intervals_.begin()) {
    auto prev = std::prev(it);
    if (prevStart ? prev->second >= *prevStart : prev->second >= start) {
      it = prev;
    }
  }

  while (it != intervals_.end()) {
    if (nextEnd ? it->first > *nextEnd : it->first > end) {
      break;
    }
    if (it->first < mergeStart) {
      mergeStart = it->first;
    }
    if (it->second > mergeEnd) {
      mergeEnd = it->second;
    }
    it = intervals_.erase(it);
  }

  intervals_[mergeStart] = mergeEnd;
}

LocationIntervalSet::MapType::const_iterator
LocationIntervalSet::findContaining(AbsoluteLocation loc) const {
  if (intervals_.empty()) {
    return intervals_.end();
  }
  auto it = intervals_.upper_bound(loc);
  if (it == intervals_.begin()) {
    return intervals_.end();
  }
  --it;
  if (loc <= it->second) {
    return it;
  }
  return intervals_.end();
}

bool LocationIntervalSet::contains(AbsoluteLocation loc) const {
  return findContaining(loc) != intervals_.end();
}

std::optional<AbsoluteLocation> LocationIntervalSet::findIntervalEnd(
    AbsoluteLocation loc) const {
  auto it = findContaining(loc);
  if (it != intervals_.end()) {
    return it->second;
  }
  return std::nullopt;
}

std::optional<std::pair<AbsoluteLocation, AbsoluteLocation>>
LocationIntervalSet::findInterval(AbsoluteLocation loc) const {
  auto it = findContaining(loc);
  if (it != intervals_.end()) {
    return std::make_pair(it->first, it->second);
  }
  return std::nullopt;
}

} // namespace moxygen
