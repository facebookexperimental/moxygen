/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/cache/MoQCachedGroup.h"

#include <folly/CppAttributes.h>
#include <algorithm>
#include <limits>

namespace moxygen {

void MoQCachedGroup::disableCaching() {
  objects_.clear();
  gaps_.clear();
  subgroups_.clear();
  endExclusive_.reset();
  cachingDisabled_ = true;
}

bool MoQCachedGroup::started() const {
  return !objects_.empty() || !subgroups_.empty() || !gaps_.empty() ||
      endExclusive_;
}

void MoQCachedGroup::openSubgroup(uint64_t subgroup, bool containsLastInGroup) {
  auto& state = subgroups_[subgroup];
  state.open = true;
  state.partialObject.reset();
  state.containsLastInGroup = containsLastInGroup;
}

bool MoQCachedGroup::isSubgroupOpen(uint64_t subgroup) const {
  return openSubgroupState(subgroup) != nullptr;
}

bool MoQCachedGroup::hasPartialObject(uint64_t subgroup) const {
  const auto* state = openSubgroupState(subgroup);
  return state && state->partialObject;
}

void MoQCachedGroup::noteDelivered(uint64_t subgroup, uint64_t objectID) {
  if (auto* state = openSubgroupState(subgroup)) {
    state->lastObjectID = objectID;
  }
}

std::optional<MoQCachedGroup::CompletedObject>
MoQCachedGroup::beginPartialObject(
    uint64_t subgroup,
    uint64_t objectID,
    uint64_t payloadLength,
    const Payload& initialPayload,
    const Extensions& extensions,
    MoQCacheCharge charge) {
  auto* state = openSubgroupState(subgroup);
  if (!state) {
    return std::nullopt;
  }
  // Not emplace(): clang 19 rejects default-constructing a nested class with
  // member initializers through std::optional.
  state->partialObject = PartialObject{};
  auto& partial = *state->partialObject;
  partial.objectID = objectID;
  partial.payloadLength = payloadLength;
  partial.received = payloadBytes(initialPayload);
  partial.extensions = extensions;
  partial.charge = std::move(charge);
  if (initialPayload) {
    partial.payload.append(initialPayload->clone());
  }
  return tryTakeCompletedObject(*state);
}

folly::Expected<std::optional<MoQCachedGroup::CompletedObject>, MoQCacheResult>
MoQCachedGroup::appendToPartialObject(
    uint64_t subgroup,
    const Payload& payload) {
  auto* state = openSubgroupState(subgroup);
  if (!state || !state->partialObject) {
    return folly::makeUnexpected(MoQCacheResult::NotCached);
  }
  auto& partial = *state->partialObject;
  const auto length = payloadBytes(payload);
  if (length > partial.payloadLength - partial.received) {
    return folly::makeUnexpected(MoQCacheResult::Malformed);
  }
  if (payload) {
    partial.payload.append(payload->clone());
  }
  partial.received += length;
  return tryTakeCompletedObject(*state);
}

MoQCacheResult MoQCachedGroup::endSubgroup(uint64_t subgroup, bool cleanFin) {
  auto* state = openSubgroupState(subgroup);
  if (!state) {
    return MoQCacheResult::NotCached;
  }
  if (cleanFin && state->partialObject) {
    return MoQCacheResult::Malformed;
  }
  state->open = false;
  state->partialObject.reset();
  if (cleanFin && state->containsLastInGroup && state->lastObjectID) {
    return setEnd(*state->lastObjectID + 1);
  }
  return MoQCacheResult::Recorded;
}

MoQCacheResult MoQCachedGroup::validateObject(
    uint64_t objectID,
    ObjectStatus status,
    uint64_t payloadLength) const {
  if (auto it = objects_.find(objectID); it != objects_.end()) {
    const auto& existing = *it->second;
    return existing.status == status && existing.payloadLength == payloadLength
        ? MoQCacheResult::NotCached
        : MoQCacheResult::Malformed;
  }
  if (gaps_.endContaining(objectID)) {
    return MoQCacheResult::Malformed;
  }
  if (endExclusive_ && objectID >= *endExclusive_) {
    return MoQCacheResult::Malformed;
  }
  if (isEndStatus(status) && !objects_.empty() &&
      objects_.rbegin()->first > objectID) {
    return MoQCacheResult::Malformed;
  }
  return MoQCacheResult::Recorded;
}

MoQCacheResult MoQCachedGroup::addObjectGap(MoQIdRange range) {
  auto it = objects_.lower_bound(range.start);
  if ((it != objects_.end() && it->first < range.end) ||
      hasPartialObjectIn(range)) {
    return MoQCacheResult::Malformed;
  }
  gaps_.insert(range);
  return MoQCacheResult::Recorded;
}

MoQCacheResult MoQCachedGroup::setEnd(uint64_t endExclusive) {
  if (endExclusive_) {
    return *endExclusive_ == endExclusive ? MoQCacheResult::Recorded
                                          : MoQCacheResult::Malformed;
  }
  if ((!objects_.empty() && objects_.rbegin()->first >= endExclusive) ||
      hasPartialObjectIn(
          {.start = endExclusive,
           .end = std::numeric_limits<uint64_t>::max()})) {
    return MoQCacheResult::Malformed;
  }
  endExclusive_ = endExclusive;
  return MoQCacheResult::Recorded;
}

void MoQCachedGroup::insertObject(
    uint64_t objectID,
    std::shared_ptr<const MoQCachedObject> object) {
  const bool datagram = object->datagram;
  const auto subgroup = object->subgroup;
  objects_.emplace(objectID, std::move(object));
  if (auto it = subgroups_.find(subgroup);
      !datagram && it != subgroups_.end()) {
    it->second.lastObjectID = objectID;
  }
}

MoQCacheCoverage MoQCachedGroup::coverage(
    uint64_t start,
    std::optional<uint64_t> end) const {
  if (cachingDisabled_) {
    return MoQCacheCoverage::Miss;
  }
  if (end) {
    return rangeCoverage(start, *end);
  }
  if (endExclusive_) {
    return rangeCoverage(start, *endExclusive_);
  }
  // The whole group was requested but its tail is unproven. Only a subgroup
  // that is still open can still prove it.
  uint64_t known = start;
  if (!objects_.empty()) {
    known = std::max(known, objects_.rbegin()->first + 1);
  }
  if (auto gapsEnd = gaps_.lastEnd()) {
    known = std::max(known, *gapsEnd);
  }
  if (rangeCoverage(start, known) == MoQCacheCoverage::Miss) {
    return MoQCacheCoverage::Miss;
  }
  const bool anyOpen =
      std::any_of(subgroups_.begin(), subgroups_.end(), [](const auto& entry) {
        return entry.second.open;
      });
  return anyOpen ? MoQCacheCoverage::AwaitIngress : MoQCacheCoverage::Miss;
}

void MoQCachedGroup::collect(
    uint64_t groupID,
    uint64_t first,
    AbsoluteLocation end,
    std::vector<MoQReplayPlan::Entry>& entries) const {
  for (auto it = objects_.lower_bound(first);
       it != objects_.end() && AbsoluteLocation{groupID, it->first} < end;
       ++it) {
    entries.push_back({{groupID, it->first}, it->second});
  }
}

MoQCachedGroup::SubgroupReceiveState* FOLLY_NULLABLE
MoQCachedGroup::openSubgroupState(uint64_t subgroup) {
  auto it = subgroups_.find(subgroup);
  if (it == subgroups_.end() || !it->second.open) {
    return nullptr;
  }
  return &it->second;
}

const MoQCachedGroup::SubgroupReceiveState* FOLLY_NULLABLE
MoQCachedGroup::openSubgroupState(uint64_t subgroup) const {
  auto it = subgroups_.find(subgroup);
  if (it == subgroups_.end() || !it->second.open) {
    return nullptr;
  }
  return &it->second;
}

std::optional<MoQCachedGroup::CompletedObject>
MoQCachedGroup::tryTakeCompletedObject(SubgroupReceiveState& state) {
  auto& partial = *state.partialObject;
  if (partial.received != partial.payloadLength) {
    return std::nullopt;
  }
  CompletedObject completed{
      .objectID = partial.objectID,
      .payload = partial.payload.move(),
      .payloadLength = partial.payloadLength,
      .extensions = std::move(partial.extensions),
      .charge = std::move(partial.charge)};
  state.partialObject.reset();
  return completed;
}

MoQCacheCoverage MoQCachedGroup::rangeCoverage(uint64_t start, uint64_t end)
    const {
  auto coverage = MoQCacheCoverage::Ready;
  uint64_t position = start;
  auto objIt = objects_.lower_bound(position);
  while (position < end) {
    if (objIt != objects_.end() && objIt->first == position) {
      ++position;
      ++objIt;
      continue;
    }
    if (auto gapEnd = gaps_.endContaining(position)) {
      position = *gapEnd;
      objIt = objects_.lower_bound(position);
      continue;
    }
    uint64_t missingEnd = end;
    if (objIt != objects_.end()) {
      missingEnd = std::min(missingEnd, objIt->first);
    }
    if (auto nextGap = gaps_.nextStartAfter(position)) {
      missingEnd = std::min(missingEnd, *nextGap);
    }
    if (!mayStillArrive(position)) {
      return MoQCacheCoverage::Miss;
    }
    coverage = MoQCacheCoverage::AwaitIngress;
    position = missingEnd;
  }
  return coverage;
}

bool MoQCachedGroup::mayStillArrive(uint64_t objectID) const {
  // Object IDs only increase within a subgroup, so an open subgroup can still
  // deliver objectID only if it has not passed it.
  for (const auto& [_, state] : subgroups_) {
    if (!state.open) {
      continue;
    }
    uint64_t nextPossible = 0;
    if (state.partialObject) {
      nextPossible = state.partialObject->objectID;
    } else if (state.lastObjectID) {
      nextPossible = *state.lastObjectID + 1;
    }
    if (nextPossible <= objectID) {
      return true;
    }
  }
  return false;
}

bool MoQCachedGroup::hasPartialObjectIn(MoQIdRange range) const {
  return std::any_of(
      subgroups_.begin(), subgroups_.end(), [&](const auto& entry) {
        const auto& partial = entry.second.partialObject;
        return partial && partial->objectID >= range.start &&
            partial->objectID < range.end;
      });
}

} // namespace moxygen
