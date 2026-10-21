/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <folly/container/F14Map.h>
#include <folly/io/IOBufQueue.h>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "moxygen/proxy/cache/MoQCacheTypes.h"
#include "moxygen/proxy/cache/MoQIdIntervals.h"

namespace moxygen {

// One cached group: its complete objects, the evidence for objects that do
// not exist, and the subgroups still delivering it.
class MoQCachedGroup {
 public:
  // An object whose last payload byte arrived, for the caller to store.
  struct CompletedObject {
    uint64_t objectID{0};
    Payload payload;
    uint64_t payloadLength{0};
    Extensions extensions;
    MoQCacheCharge charge;
  };

  explicit MoQCachedGroup(MoQCacheCharge charge) : charge_(std::move(charge)) {}

  // Once caching is disabled, the group holds nothing and records nothing
  // more.
  bool cachingDisabled() const {
    return cachingDisabled_;
  }
  void disableCaching();
  // Whether any object, subgroup, or evidence was recorded.
  bool started() const;

  // --- Subgroups ---

  // A publisher can reopen a subgroup; progress continues from the objects
  // already proven.
  void openSubgroup(uint64_t subgroup, bool containsLastInGroup);
  bool isSubgroupOpen(uint64_t subgroup) const;
  bool hasPartialObject(uint64_t subgroup) const;
  // The open subgroup delivered objectID without it being stored, e.g. a copy
  // of an object already held.
  void noteDelivered(uint64_t subgroup, uint64_t objectID);
  // Starts a partial object on an open subgroup; returns it completed at once
  // if initialPayload holds its whole payload.
  std::optional<CompletedObject> beginPartialObject(
      uint64_t subgroup,
      uint64_t objectID,
      uint64_t payloadLength,
      const Payload& initialPayload,
      const Extensions& extensions,
      MoQCacheCharge charge);
  // Returns the object once its payload is complete. NotCached if the
  // subgroup has no partial object, Malformed if the payload exceeds the
  // declared length.
  folly::Expected<std::optional<CompletedObject>, MoQCacheResult>
  appendToPartialObject(uint64_t subgroup, const Payload& payload);
  // NotCached if the subgroup is not open. A subgroup that contains the
  // group's last object, ended by a clean FIN, proves no larger object ID
  // exists; a reset proves nothing.
  MoQCacheResult endSubgroup(uint64_t subgroup, bool cleanFin);

  // --- Objects ---

  // Recorded for a new object, NotCached for an identical copy of one held,
  // Malformed for a contradiction.
  MoQCacheResult validateObject(
      uint64_t objectID,
      ObjectStatus status,
      uint64_t payloadLength) const;
  // The objects in range do not exist.
  MoQCacheResult addObjectGap(MoQIdRange range);
  // No object at or after endExclusive exists.
  MoQCacheResult setEnd(uint64_t endExclusive);
  void insertObject(
      uint64_t objectID,
      std::shared_ptr<const MoQCachedObject> object);

  // --- Joining FETCH ---

  // Whether objects [start, end) are proven; without an end, through the
  // group's last object.
  MoQCacheCoverage coverage(uint64_t start, std::optional<uint64_t> end) const;
  // Appends the objects from `first` up to the exclusive `end`.
  void collect(
      uint64_t groupID,
      uint64_t first,
      AbsoluteLocation end,
      std::vector<MoQReplayPlan::Entry>& entries) const;

 private:
  // Temporary state for an object whose payload is arriving in multiple
  // chunks.
  struct PartialObject {
    uint64_t objectID{0};
    uint64_t payloadLength{0};
    uint64_t received{0};
    Extensions extensions;
    folly::IOBufQueue payload{folly::IOBufQueue::cacheChainLength()};
    MoQCacheCharge charge;
  };

  struct SubgroupReceiveState {
    bool open{true};
    bool containsLastInGroup{false};
    std::optional<uint64_t> lastObjectID;
    std::optional<PartialObject> partialObject;
  };

  SubgroupReceiveState* openSubgroupState(uint64_t subgroup);
  const SubgroupReceiveState* openSubgroupState(uint64_t subgroup) const;
  static std::optional<CompletedObject> tryTakeCompletedObject(
      SubgroupReceiveState& state);
  // Returns whether [start, end) is ready, may still be completed by ingress,
  // or cannot be served from this cache.
  MoQCacheCoverage rangeCoverage(uint64_t start, uint64_t end) const;
  bool mayStillArrive(uint64_t objectID) const;
  bool hasPartialObjectIn(MoQIdRange range) const;

  std::map<uint64_t, std::shared_ptr<const MoQCachedObject>> objects_;
  MoQIdIntervals gaps_;
  // Records the known end of the group: no object with that ID or a larger ID
  // can exist.
  std::optional<uint64_t> endExclusive_;
  folly::F14FastMap<uint64_t, SubgroupReceiveState> subgroups_;
  // True when the group is proven not to exist or the cache detected malformed
  // data for it.
  bool cachingDisabled_{false};
  MoQCacheCharge charge_;
};

} // namespace moxygen
