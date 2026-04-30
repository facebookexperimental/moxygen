/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <folly/logging/xlog.h>
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

void LocationIntervalSet::remove(AbsoluteLocation start, AbsoluteLocation end) {
  if (end < start || intervals_.empty()) {
    return;
  }

  auto it = intervals_.lower_bound(start);

  // Check if the preceding interval overlaps with [start, end].
  if (it != intervals_.begin()) {
    auto prev = std::prev(it);
    if (prev->second >= start) {
      it = prev;
    }
  }

  while (it != intervals_.end() && it->first <= end) {
    auto intervalStart = it->first;
    auto intervalEnd = it->second;
    it = intervals_.erase(it);

    // Keep the portion before [start, end]. intervalStart < start implies
    // start > {0,0}, so start.prev() is always valid.
    if (intervalStart < start) {
      auto trimEnd = start.prev();
      XCHECK(trimEnd.has_value());
      intervals_[intervalStart] = *trimEnd;
    }

    // Keep the portion after [start, end]. intervalEnd > end implies
    // end < kLocationMax, so end.next() is always valid.
    if (intervalEnd > end) {
      auto trimStart = end.next();
      XCHECK(trimStart.has_value());
      intervals_[*trimStart] = intervalEnd;
    }
  }
}

std::optional<std::pair<AbsoluteLocation, AbsoluteLocation>>
LocationIntervalSet::findNextInterval(AbsoluteLocation loc) const {
  // If loc is inside an interval, skip past it.
  auto containing = findContaining(loc);
  if (containing != intervals_.end()) {
    auto nextLoc = containing->second.next();
    if (!nextLoc) {
      return std::nullopt;
    }
    loc = *nextLoc;
  }

  auto it = intervals_.lower_bound(loc);
  if (it != intervals_.end()) {
    return std::make_pair(it->first, it->second);
  }
  return std::nullopt;
}

std::optional<std::pair<AbsoluteLocation, AbsoluteLocation>>
LocationIntervalSet::findPrevInterval(AbsoluteLocation loc) const {
  // If loc is inside an interval, start from that interval; otherwise start
  // from the first interval whose start is > loc. In both cases, step back
  // one element to get the previous interval.
  auto containing = findContaining(loc);
  auto it = (containing != intervals_.end()) ? containing
                                             : intervals_.upper_bound(loc);
  if (it == intervals_.begin()) {
    return std::nullopt;
  }
  --it;
  return std::make_pair(it->first, it->second);
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
