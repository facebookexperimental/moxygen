/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQGroupCache.h"

#include <folly/CppAttributes.h>
#include <folly/logging/xlog.h>
#include <algorithm>

#include "moxygen/proxy/cache/MoQPriorGapRanges.h"

namespace moxygen {
namespace {

Payload clonePayload(const Payload& payload) {
  return payload ? payload->clone() : nullptr;
}

} // namespace

MoQGroupCache::MoQGroupCache(
    size_t maxGroups,
    std::shared_ptr<MoQCacheBudget> budget)
    : maxGroups_(maxGroups), budget_(std::move(budget)) {}

MoQGroupCache::~MoQGroupCache() {
  notifyWaiters();
}

bool MoQGroupCache::openSubgroup(
    uint64_t group,
    uint64_t subgroup,
    bool containsLastInGroup) {
  if (!admitGroup(group)) {
    return false;
  }
  groups_.at(group).openSubgroup(subgroup, containsLastInGroup);
  return true;
}

MoQGroupCache::Result MoQGroupCache::subgroupObject(
    uint64_t group,
    uint64_t subgroup,
    uint64_t objectID,
    ObjectStatus status,
    const Payload& payload,
    const Extensions& extensions) {
  auto* cached = groupWithOpenSubgroup(group, subgroup);
  if (!cached) {
    return Result::NotCached;
  }
  if (cached->hasPartialObject(subgroup)) {
    return failMalformed(group);
  }
  const auto length = payloadBytes(payload);
  auto validation = cached->validateObject(objectID, status, length);
  if (validation == Result::Malformed) {
    return failMalformed(group);
  }
  if (validation == Result::NotCached) {
    cached->noteDelivered(subgroup, objectID);
    return Result::Recorded;
  }
  auto charge = reserveForGroup(group, objectChargeBytes(length, extensions));
  if (!charge) {
    return Result::NotCached;
  }
  return store(
      ObjectToStore{
          .group = group,
          .subgroup = subgroup,
          .objectID = objectID,
          .status = status,
          .payload = clonePayload(payload),
          .payloadLength = length,
          .extensions = extensions,
          .datagram = false},
      std::move(*charge));
}

MoQGroupCache::Result MoQGroupCache::beginObject(
    uint64_t group,
    uint64_t subgroup,
    uint64_t objectID,
    uint64_t length,
    const Payload& initialPayload,
    const Extensions& extensions) {
  auto* cached = groupWithOpenSubgroup(group, subgroup);
  if (!cached) {
    return Result::NotCached;
  }
  if (cached->hasPartialObject(subgroup) ||
      payloadBytes(initialPayload) > length) {
    return failMalformed(group);
  }
  auto validation =
      cached->validateObject(objectID, ObjectStatus::NORMAL, length);
  if (validation == Result::Malformed) {
    return failMalformed(group);
  }
  if (validation == Result::NotCached) {
    cached->noteDelivered(subgroup, objectID);
    return Result::Recorded;
  }
  // Reserve the declared length up front, before any payload is cloned.
  auto charge = reserveForGroup(group, objectChargeBytes(length, extensions));
  if (!charge) {
    return Result::NotCached;
  }
  cached = groupWithOpenSubgroup(group, subgroup);
  if (!cached) {
    return Result::NotCached;
  }
  auto completed = cached->beginPartialObject(
      subgroup,
      objectID,
      length,
      initialPayload,
      extensions,
      std::move(*charge));
  if (completed) {
    return storeCompleted(group, subgroup, std::move(*completed));
  }
  return Result::Recorded;
}

MoQGroupCache::Result MoQGroupCache::objectPayload(
    uint64_t group,
    uint64_t subgroup,
    const Payload& payload) {
  auto* cached = cachingGroup(group);
  if (!cached) {
    return Result::NotCached;
  }
  auto completed = cached->appendToPartialObject(subgroup, payload);
  if (completed.hasError()) {
    return completed.error() == Result::Malformed ? failMalformed(group)
                                                  : completed.error();
  }
  if (*completed) {
    return storeCompleted(group, subgroup, std::move(**completed));
  }
  return Result::Recorded;
}

MoQGroupCache::Result
MoQGroupCache::endSubgroup(uint64_t group, uint64_t subgroup, bool cleanFin) {
  auto* cached = cachingGroup(group);
  if (!cached) {
    return Result::NotCached;
  }
  auto result = cached->endSubgroup(subgroup, cleanFin);
  if (result == Result::Malformed) {
    return failMalformed(group);
  }
  if (result == Result::Recorded) {
    notifyWaiters();
  }
  return result;
}

MoQGroupCache::Result MoQGroupCache::standaloneObject(
    const ObjectHeader& header,
    const Payload& payload,
    bool datagram) {
  if (!admitGroup(header.group)) {
    return Result::NotCached;
  }
  const auto length = payloadBytes(payload);
  auto validation =
      groups_.at(header.group).validateObject(header.id, header.status, length);
  if (validation == Result::Malformed) {
    return failMalformed(header.group);
  }
  if (validation == Result::NotCached) {
    return Result::Recorded;
  }
  auto charge = reserveForGroup(
      header.group, objectChargeBytes(length, header.extensions));
  if (!charge) {
    return Result::NotCached;
  }
  return store(
      ObjectToStore{
          .group = header.group,
          .subgroup = header.subgroup,
          .objectID = header.id,
          .status = header.status,
          .payload = clonePayload(payload),
          .payloadLength = length,
          .extensions = header.extensions,
          .datagram = datagram},
      std::move(*charge));
}

bool MoQGroupCache::isCaching(uint64_t group) const {
  auto it = groups_.find(group);
  return it != groups_.end() && !it->second.cachingDisabled();
}

void MoQGroupCache::clear() {
  groups_.clear();
  nonexistentGroups_.clear();
  retiredThrough_.reset();
  endOfTrack_.reset();
  notifyWaiters();
}

void MoQGroupCache::shutdown() {
  shutdown_ = true;
  clear();
}

MoQGroupCache::Coverage MoQGroupCache::inspect(
    AbsoluteLocation start,
    AbsoluteLocation end) const {
  if (shutdown_ || maxGroups_ == 0) {
    return Coverage::Miss;
  }
  if (!(start < end)) {
    return Coverage::Ready;
  }
  // An exclusive end of (g, 0) requests nothing from group g.
  const uint64_t lastGroup = end.object == 0 ? end.group - 1 : end.group;
  auto coverage = Coverage::Ready;
  uint64_t groupID = start.group;
  while (groupID <= lastGroup) {
    if (retiredThrough_ && groupID <= *retiredThrough_) {
      return Coverage::Miss;
    }
    if (auto nonexistentEnd = nonexistentGroups_.endContaining(groupID)) {
      groupID = *nonexistentEnd;
      continue;
    }
    auto it = groups_.find(groupID);
    if (it == groups_.end()) {
      return Coverage::Miss;
    }
    const uint64_t first = groupID == start.group ? start.object : 0;
    const std::optional<uint64_t> limit = groupID == end.group
        ? std::optional<uint64_t>(end.object)
        : std::nullopt;
    auto groupResult = it->second.coverage(first, limit);
    if (groupResult == Coverage::Miss) {
      return Coverage::Miss;
    }
    if (groupResult == Coverage::AwaitIngress) {
      coverage = Coverage::AwaitIngress;
    }
    if (groupID == lastGroup) {
      break;
    }
    groupID++;
  }
  return coverage;
}

std::optional<MoQReplayPlan> MoQGroupCache::snapshot(
    AbsoluteLocation start,
    AbsoluteLocation end,
    GroupOrder order) const {
  if (inspect(start, end) != Coverage::Ready) {
    return std::nullopt;
  }
  std::vector<std::vector<MoQReplayPlan::Entry>> perGroup;
  size_t entryCount = 0;
  for (auto it = groups_.lower_bound(start.group);
       it != groups_.end() && AbsoluteLocation{it->first, 0} < end;
       ++it) {
    const uint64_t first = it->first == start.group ? start.object : 0;
    std::vector<MoQReplayPlan::Entry> entries;
    it->second.collect(it->first, first, end, entries);
    entryCount += entries.size();
    perGroup.push_back(std::move(entries));
  }
  auto charge = MoQCacheCharge::reserve(budget_, planChargeBytes(entryCount));
  if (!charge) {
    return std::nullopt;
  }
  if (order == GroupOrder::NewestFirst) {
    std::reverse(perGroup.begin(), perGroup.end());
  }
  MoQReplayPlan plan;
  plan.entries.reserve(entryCount);
  for (auto& entries : perGroup) {
    std::move(entries.begin(), entries.end(), std::back_inserter(plan.entries));
  }
  plan.endLocation = end;
  plan.endOfTrack = endOfTrack_.has_value() && *endOfTrack_ < end;
  plan.charge = std::move(*charge);
  return plan;
}

uint64_t MoQGroupCache::addWaiter(folly::Function<void()> wake) {
  if (shutdown_) {
    wake();
    return 0;
  }
  const auto waiter = nextWaiter_++;
  waiters_.emplace_back(waiter, std::move(wake));
  return waiter;
}

void MoQGroupCache::removeWaiter(uint64_t waiter) {
  std::erase_if(
      waiters_, [waiter](const auto& entry) { return entry.first == waiter; });
}

bool MoQGroupCache::admitGroup(uint64_t group) {
  if (shutdown_ || maxGroups_ == 0) {
    return false;
  }
  if (retiredThrough_ && group <= *retiredThrough_) {
    return false;
  }
  if (nonexistentGroups_.endContaining(group)) {
    return false;
  }
  if (auto it = groups_.find(group); it != groups_.end()) {
    return !it->second.cachingDisabled();
  }
  if (groups_.size() >= maxGroups_) {
    const auto smallest = groups_.begin()->first;
    if (group < smallest) {
      return false;
    }
    retireGroup(smallest);
  }
  auto charge = reserveForGroup(group, groupMetadataChargeBytes());
  if (!charge) {
    return false;
  }
  groups_.try_emplace(group, std::move(*charge));
  return true;
}

void MoQGroupCache::retireGroup(uint64_t group) {
  groups_.erase(group);
  if (!retiredThrough_ || *retiredThrough_ < group) {
    retiredThrough_ = group;
  }
  // Evidence about groups at or below the watermark can no longer be used.
  nonexistentGroups_.eraseEndingBy(*retiredThrough_ + 1);
  notifyWaiters();
}

std::optional<MoQCacheCharge> MoQGroupCache::reserveForGroup(
    uint64_t group,
    size_t bytes) {
  // Under budget pressure, give up this track's own oldest groups first;
  // other tracks are never evicted.
  while (true) {
    if (auto charge = MoQCacheCharge::reserve(budget_, bytes)) {
      return charge;
    }
    if (groups_.empty() || groups_.begin()->first > group) {
      break;
    }
    const auto oldest = groups_.begin()->first;
    retireGroup(oldest);
    if (oldest == group) {
      return std::nullopt;
    }
  }
  // Nothing older is left to give up; skip this group for good.
  if (!retiredThrough_ || *retiredThrough_ < group) {
    retiredThrough_ = group;
  }
  return std::nullopt;
}

MoQCachedGroup* FOLLY_NULLABLE MoQGroupCache::cachingGroup(uint64_t group) {
  auto it = groups_.find(group);
  if (it == groups_.end() || it->second.cachingDisabled()) {
    return nullptr;
  }
  return &it->second;
}

MoQCachedGroup* FOLLY_NULLABLE
MoQGroupCache::groupWithOpenSubgroup(uint64_t group, uint64_t subgroup) {
  auto* cached = cachingGroup(group);
  if (!cached || !cached->isSubgroupOpen(subgroup)) {
    return nullptr;
  }
  return cached;
}

MoQGroupCache::Result MoQGroupCache::store(
    ObjectToStore object,
    MoQCacheCharge charge) {
  auto* group = cachingGroup(object.group);
  if (!group) {
    // Budget pressure retired this group while reserving.
    return Result::NotCached;
  }
  auto validation = group->validateObject(
      object.objectID, object.status, object.payloadLength);
  if (validation != Result::Recorded) {
    return validation == Result::Malformed ? failMalformed(object.group)
                                           : Result::Recorded;
  }
  if (applyPriorGapRanges(
          *group, object.group, object.objectID, object.extensions) ==
      Result::Malformed) {
    return failMalformed(object.group);
  }
  if (isEndStatus(object.status)) {
    if (group->setEnd(object.objectID + 1) == Result::Malformed) {
      return failMalformed(object.group);
    }
    if (object.status == ObjectStatus::END_OF_TRACK) {
      endOfTrack_ = AbsoluteLocation{object.group, object.objectID};
    }
  }
  auto cached = std::make_shared<MoQCachedObject>();
  cached->subgroup = object.subgroup;
  cached->status = object.status;
  cached->extensions = std::move(object.extensions);
  cached->payload = std::move(object.payload);
  cached->payloadLength = object.payloadLength;
  cached->datagram = object.datagram;
  cached->charge = std::move(charge);
  group->insertObject(object.objectID, std::move(cached));
  notifyWaiters();
  return Result::Recorded;
}

MoQGroupCache::Result MoQGroupCache::storeCompleted(
    uint64_t group,
    uint64_t subgroup,
    MoQCachedGroup::CompletedObject object) {
  return store(
      ObjectToStore{
          .group = group,
          .subgroup = subgroup,
          .objectID = object.objectID,
          .status = ObjectStatus::NORMAL,
          .payload = std::move(object.payload),
          .payloadLength = object.payloadLength,
          .extensions = std::move(object.extensions),
          .datagram = false},
      std::move(object.charge));
}

MoQGroupCache::Result MoQGroupCache::applyPriorGapRanges(
    MoQCachedGroup& group,
    uint64_t groupID,
    uint64_t objectID,
    const Extensions& extensions) {
  auto ranges = decodePriorGapRanges(groupID, objectID, extensions);
  if (!ranges.valid) {
    return Result::Malformed;
  }
  if (ranges.objects &&
      group.addObjectGap(*ranges.objects) == Result::Malformed) {
    return Result::Malformed;
  }
  if (ranges.groups) {
    return markGroupsNonexistent(*ranges.groups);
  }
  return Result::Recorded;
}

MoQGroupCache::Result MoQGroupCache::markGroupsNonexistent(MoQIdRange groups) {
  const auto first = groups_.lower_bound(groups.start);
  const auto last = groups_.lower_bound(groups.end);
  // A group the publisher has started cannot also be absent.
  if (std::any_of(first, last, [](const auto& entry) {
        return entry.second.started();
      })) {
    return Result::Malformed;
  }
  // Nothing may be recorded later for a group proven not to exist.
  std::for_each(
      first, last, [](auto& entry) { entry.second.disableCaching(); });
  nonexistentGroups_.insert(groups);
  return Result::Recorded;
}

MoQGroupCache::Result MoQGroupCache::failMalformed(uint64_t group) {
  XLOG(ERR) << "Malformed data for cached group=" << group;
  if (auto it = groups_.find(group); it != groups_.end()) {
    it->second.disableCaching();
  }
  notifyWaiters();
  return Result::Malformed;
}

void MoQGroupCache::notifyWaiters() {
  auto waiters = std::move(waiters_);
  waiters_.clear();
  for (auto& [_, wake] : waiters) {
    wake();
  }
}

} // namespace moxygen
