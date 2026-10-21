/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Function.h>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "moxygen/MoQTypes.h"
#include "moxygen/proxy/cache/MoQCacheBudget.h"
#include "moxygen/proxy/cache/MoQCacheTypes.h"
#include "moxygen/proxy/cache/MoQCachedGroup.h"
#include "moxygen/proxy/cache/MoQIdIntervals.h"

namespace moxygen {

// The newest groups of one live track, fed only by its upstream subscription.
// A joining FETCH is served from it only when every requested location is
// proven: a complete object, or authoritative evidence that it does not
// exist.
class MoQGroupCache {
 public:
  using Coverage = MoQCacheCoverage;
  using Result = MoQCacheResult;

  // maxGroups == 0 disables caching.
  MoQGroupCache(size_t maxGroups, std::shared_ptr<MoQCacheBudget> budget);
  ~MoQGroupCache();

  MoQGroupCache(const MoQGroupCache&) = delete;
  MoQGroupCache& operator=(const MoQGroupCache&) = delete;
  MoQGroupCache(MoQGroupCache&&) = delete;
  MoQGroupCache& operator=(MoQGroupCache&&) = delete;

  // --- Ingress from the upstream subscription ---

  // Returns whether the subgroup's group is cached; false means the caller
  // should not record anything for this subgroup.
  bool
  openSubgroup(uint64_t group, uint64_t subgroup, bool containsLastInGroup);
  // A complete object or status on an open subgroup.
  Result subgroupObject(
      uint64_t group,
      uint64_t subgroup,
      uint64_t objectID,
      ObjectStatus status,
      const Payload& payload,
      const Extensions& extensions);
  Result beginObject(
      uint64_t group,
      uint64_t subgroup,
      uint64_t objectID,
      uint64_t length,
      const Payload& initialPayload,
      const Extensions& extensions);
  Result
  objectPayload(uint64_t group, uint64_t subgroup, const Payload& payload);
  // cleanFin is false for a reset stream.
  Result endSubgroup(uint64_t group, uint64_t subgroup, bool cleanFin);
  // A datagram or single-object stream; admits its group itself.
  Result standaloneObject(
      const ObjectHeader& header,
      const Payload& payload,
      bool datagram);

  bool isCaching(uint64_t group) const;

  // Drops everything; later groups can be cached again.
  void clear();
  // Drops everything and refuses further ingress.
  void shutdown();

  // --- Joining FETCH ---

  Coverage inspect(AbsoluteLocation start, AbsoluteLocation end) const;
  // Only for a Ready range. Nullopt if the plan cannot be charged.
  std::optional<MoQReplayPlan> snapshot(
      AbsoluteLocation start,
      AbsoluteLocation end,
      GroupOrder order) const;
  // Called once, at the next change that can affect coverage. Returns an ID
  // for removeWaiter.
  uint64_t addWaiter(folly::Function<void()> wake);
  void removeWaiter(uint64_t waiter);
  size_t waiterCount() const {
    return waiters_.size();
  }

  size_t residentGroups() const {
    return groups_.size();
  }
  std::optional<uint64_t> retiredThrough() const {
    return retiredThrough_;
  }
  bool isShutdown() const {
    return shutdown_;
  }

 private:
  struct ObjectToStore {
    uint64_t group;
    uint64_t subgroup;
    uint64_t objectID;
    ObjectStatus status;
    Payload payload;
    uint64_t payloadLength;
    Extensions extensions;
    bool datagram;
  };

  bool admitGroup(uint64_t group);
  void retireGroup(uint64_t group);
  std::optional<MoQCacheCharge> reserveForGroup(uint64_t group, size_t bytes);
  MoQCachedGroup* cachingGroup(uint64_t group);
  MoQCachedGroup* groupWithOpenSubgroup(uint64_t group, uint64_t subgroup);

  Result store(ObjectToStore object, MoQCacheCharge charge);
  Result storeCompleted(
      uint64_t group,
      uint64_t subgroup,
      MoQCachedGroup::CompletedObject object);
  Result applyPriorGapRanges(
      MoQCachedGroup& group,
      uint64_t groupID,
      uint64_t objectID,
      const Extensions& extensions);
  Result markGroupsNonexistent(MoQIdRange groups);
  Result failMalformed(uint64_t group);

  void notifyWaiters();

  size_t maxGroups_;
  std::shared_ptr<MoQCacheBudget> budget_;
  std::map<uint64_t, MoQCachedGroup> groups_;
  MoQIdIntervals nonexistentGroups_;
  std::optional<uint64_t> retiredThrough_;
  std::optional<AbsoluteLocation> endOfTrack_;
  std::vector<std::pair<uint64_t, folly::Function<void()>>> waiters_;
  uint64_t nextWaiter_{1};
  bool shutdown_{false};
};

} // namespace moxygen
