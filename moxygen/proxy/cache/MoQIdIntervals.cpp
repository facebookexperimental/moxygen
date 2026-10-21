/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/cache/MoQIdIntervals.h"

#include <algorithm>

namespace moxygen {
namespace {

auto firstStartingAfter(
    const quic::IntervalSet<uint64_t>& ranges,
    uint64_t id) {
  return std::upper_bound(
      ranges.begin(), ranges.end(), id, [](uint64_t value, const auto& range) {
        return value < range.start;
      });
}

} // namespace

void MoQIdIntervals::insert(MoQIdRange range) {
  if (range.start < range.end) {
    ranges_.insert(range.start, range.end - 1);
  }
}

std::optional<uint64_t> MoQIdIntervals::endContaining(uint64_t id) const {
  auto it = firstStartingAfter(ranges_, id);
  if (it == ranges_.begin()) {
    return std::nullopt;
  }
  --it;
  if (id <= it->end) {
    return it->end + 1;
  }
  return std::nullopt;
}

std::optional<uint64_t> MoQIdIntervals::nextStartAfter(uint64_t id) const {
  auto it = firstStartingAfter(ranges_, id);
  if (it == ranges_.end()) {
    return std::nullopt;
  }
  return it->start;
}

std::optional<uint64_t> MoQIdIntervals::lastEnd() const {
  if (ranges_.empty()) {
    return std::nullopt;
  }
  return ranges_.back().end + 1;
}

void MoQIdIntervals::eraseEndingBy(uint64_t end) {
  while (!ranges_.empty() && ranges_.front().end < end) {
    const auto first = ranges_.front();
    ranges_.withdraw(first);
  }
}

} // namespace moxygen
