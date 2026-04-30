/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <moxygen/MoQFramer.h>
#include <limits>
#include <map>
#include <optional>

namespace moxygen {

/**
 * LocationIntervalSet stores disjoint intervals of AbsoluteLocation.
 *
 * This is a portable alternative to quic::IntervalSet that works with
 * AbsoluteLocation (two uint64_t values) without requiring uint128_t.
 *
 * Key operations:
 * - insert(start, end): Add an interval, merging with overlapping/adjacent
 * - contains(loc): Check if a location is within any interval
 * - findIntervalEnd(loc): Find the end of the interval containing loc
 *
 * Query operations are O(log n) where n is the number of intervals.
 * Insert is O(log n + k) where k is the number of merged intervals.
 */
class LocationIntervalSet {
 public:
  LocationIntervalSet() = default;

  // Non-copyable / non-movable: lastModified_ is an iterator into intervals_
  // and would dangle across copy or move.
  LocationIntervalSet(const LocationIntervalSet&) = delete;
  LocationIntervalSet& operator=(const LocationIntervalSet&) = delete;
  LocationIntervalSet(LocationIntervalSet&&) = delete;
  LocationIntervalSet& operator=(LocationIntervalSet&&) = delete;

  /**
   * Insert an interval [start, end] (inclusive on both ends).
   * Automatically merges with overlapping or adjacent intervals.
   */
  void insert(AbsoluteLocation start, AbsoluteLocation end);

  /**
   * Insert a single position. Equivalent to insert(loc, loc).
   */
  void insert(AbsoluteLocation loc) {
    insert(loc, loc);
  }

  /**
   * Check if a location is contained in any interval.
   */
  bool contains(AbsoluteLocation loc) const;

  /**
   * If loc is in an interval, return the end of that interval.
   * Otherwise return nullopt.
   */
  std::optional<AbsoluteLocation> findIntervalEnd(AbsoluteLocation loc) const;

  /**
   * If loc is in an interval, return both start and end of that interval.
   * Otherwise return nullopt.
   */
  std::optional<std::pair<AbsoluteLocation, AbsoluteLocation>> findInterval(
      AbsoluteLocation loc) const;

  /**
   * Remove the interval [start, end] (inclusive on both ends).
   * Handles partial overlaps by trimming or splitting existing intervals.
   * Runs in O(log n + k), where k is the number of intervals overlapping
   * [start, end].
   */
  void remove(AbsoluteLocation start, AbsoluteLocation end);

  /**
   * Find the next interval after loc: if loc lies inside an interval, returns
   * the interval immediately after it; otherwise returns the first interval
   * whose start is >= loc. Returns nullopt if no such interval exists.
   * Runs in O(log n).
   */
  std::optional<std::pair<AbsoluteLocation, AbsoluteLocation>> findNextInterval(
      AbsoluteLocation loc) const;

  /**
   * Find the previous interval before loc: if loc lies inside an interval,
   * returns the interval immediately before it; otherwise returns the last
   * interval whose start is <= loc. Returns nullopt if no such interval
   * exists. Runs in O(log n).
   */
  std::optional<std::pair<AbsoluteLocation, AbsoluteLocation>> findPrevInterval(
      AbsoluteLocation loc) const;

  /**
   * Check if the set is empty.
   */
  bool empty() const {
    return intervals_.empty();
  }

  /**
   * Clear all intervals.
   */
  void clear() {
    intervals_.clear();
    invalidateLastModified();
  }

  /**
   * Get the number of intervals (not positions).
   */
  size_t size() const {
    return intervals_.size();
  }

 private:
  using MapType = std::map<AbsoluteLocation, AbsoluteLocation>;
  MapType::const_iterator findContaining(AbsoluteLocation loc) const;
  void invalidateLastModified() {
    lastModified_ = intervals_.end();
  }

  // Map from interval start to interval end (inclusive).
  // Invariant: intervals are disjoint and non-adjacent.
  MapType intervals_;
  // Cached iterator for fast-path consecutive inserts
  MapType::iterator lastModified_{intervals_.end()};
};

} // namespace moxygen
