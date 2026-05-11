/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/logging/xlog.h>
#include <moxygen/relay/MoQCache.h>

// Maxmimum cache size / per track? Number of groups
// Fancy: handle streaming incomplete objects (forwarder?)

namespace {
using namespace moxygen;

// Cap on prior-gap validation scans to avoid O(n) work for huge gaps.
constexpr uint64_t kMaxGapValidation = 100;

// Compute the next iterator position after a gap when iterating descending
// (NewestFirst). Groups iterate high-to-low, but objects within a group go
// low-to-high. Returns std::nullopt if the gap extends past the iteration
// boundary.
std::optional<AbsoluteLocation> computeDescendingAfterGap(
    const AbsoluteLocation& current,
    const AbsoluteLocation& gapStart,
    const AbsoluteLocation& minLocation) {
  // Gap starts mid-group in a DIFFERENT group: there may be valid objects
  // before the gap in that group, so jump to the start of it.
  if (gapStart.object > 0 && gapStart.group != current.group) {
    AbsoluteLocation afterGap{gapStart.group, 0};
    if (afterGap.group == minLocation.group) {
      afterGap.object = minLocation.object;
    }
    return afterGap;
  }
  // Either gap starts at object 0, OR gap starts in the current group. If gap
  // starts in the current group, we've already served objects before the gap
  // (since objects iterate low-to-high). Skip to the previous group.
  auto afterGap = gapStart.prevGroup();
  if (afterGap && afterGap->group == minLocation.group) {
    afterGap->object = minLocation.object;
  }
  return afterGap;
}

// Splits one contiguous cache miss region into multiple intervals
// Depending on whether we are fetching asc or desc
// For asc, no split. Desc cases may or may not need splitting
std::vector<std::pair<AbsoluteLocation, AbsoluteLocation>> getFetchIntervals(
    AbsoluteLocation start,
    AbsoluteLocation end,
    AbsoluteLocation missLocation,
    AbsoluteLocation hitLocation,
    GroupOrder order,
    bool isTail) {
  std::vector<std::pair<AbsoluteLocation, AbsoluteLocation>> intervals;

  // Return one interval for asc case
  if (order != GroupOrder::NewestFirst) {
    if (!isTail) {
      intervals.emplace_back(missLocation, hitLocation);
    } else {
      intervals.emplace_back(missLocation, end);
    }
    return intervals;
  }

  // Desc cases
  // If we are within the same group, we only need to see if tail or not
  // The interval is straightforward
  if (missLocation.group == hitLocation.group) {
    // same group, just return the one interval
    if (!isTail) {
      intervals.emplace_back(missLocation, hitLocation);
    } else {
      intervals.emplace_back(
          missLocation, missLocation.nextGroup().value_or(end));
    }
    return intervals;
  }

  // Since this is the desc case
  // We will start finding the last interval first
  // and then evaulate intermediate if any
  // and then find the start interval
  AbsoluteLocation intervalEnd = missLocation;
  // For missLocation, end is either full group or where fetch ends
  intervalEnd = missLocation.nextGroup().value_or(end);
  if (missLocation.group == end.group) {
    // If we are in the end group, fetch till end and not whole group
    intervalEnd = end;
  }
  intervals.emplace_back(missLocation, intervalEnd);

  uint64_t numIntermediateGroups = missLocation.group - hitLocation.group - 1;
  if (numIntermediateGroups > 0) {
    if (missLocation.object == 0) {
      // Coalesce with existing intervals, clamping to `start` so the
      // sentinel hitLocation={0,0} (used by skipUncached when no prior
      // cached content exists) doesn't extend the interval below
      // minLocation.
      intervals.back().first = std::max(
          start,
          AbsoluteLocation{missLocation.group - numIntermediateGroups, 0});
    } else {
      // create a new interval with intermediate groups
      AbsoluteLocation intEnd = {missLocation.group, 0};
      AbsoluteLocation intStart = {hitLocation.group + 1, 0};
      if (start.group == intStart.group) {
        intStart.object = start.object;
      }
      intervals.emplace_back(intStart, intEnd);
    }
  }

  if (hitLocation.object != 0) {
    // Fetch the beginning of the hit group, up to the first cached object.
    // Clamp to the fetch start if the hit is in the first group.
    AbsoluteLocation lastStart = {hitLocation.group, 0};
    if (start.group == hitLocation.group) {
      lastStart = start;
    }
    // Skip if start is already at the cache hit (no gap to fill).
    if (lastStart < hitLocation) {
      intervals.emplace_back(lastStart, hitLocation);
    }
  }

  return intervals;
}

bool isEndOfTrack(ObjectStatus status) {
  return status == ObjectStatus::END_OF_TRACK;
}

// Helper to compute gap ranges for markNonExistentTo.
// Returns vector of (start, end) intervals to mark as gaps.
// Handles both ascending and descending iteration orders.
//
// fetchStart/fetchEnd are the user's original fetch range (inclusive start,
// exclusive end), used to clamp DESC top-group and bottom-group ranges so we
// never mark positions outside the requested range as non-existent.
std::vector<std::pair<AbsoluteLocation, AbsoluteLocation>> getGapRanges(
    AbsoluteLocation start,
    AbsoluteLocation end,
    GroupOrder order,
    AbsoluteLocation fetchStart,
    AbsoluteLocation fetchEnd) {
  std::vector<std::pair<AbsoluteLocation, AbsoluteLocation>> ranges;

  // Check if range is empty based on iteration order
  // Ascending: start should be < end
  // Descending: groups go high-to-low, but objects within groups go low-to-high
  //   so start.group > end.group OR (same group with start.object < end.object)
  bool isEmpty = (order != GroupOrder::NewestFirst)
      ? (start >= end)
      : (start.group < end.group ||
         (start.group == end.group && start.object >= end.object));
  if (isEmpty) {
    return ranges;
  }

  if (start.group == end.group) {
    // Same group - simple range from start to end-1
    if (auto prev = end.prevInGroup()) {
      ranges.emplace_back(start, *prev);
    }
    return ranges;
  }

  // For ascending: straightforward contiguous range
  if (order != GroupOrder::NewestFirst) {
    auto gapEnd = end.prev();
    XCHECK(gapEnd)
        << "ascending different-group with end={0,0} should be impossible";
    ranges.emplace_back(start, *gapEnd);
    return ranges;
  }

  // Descending: groups are iterated in reverse, but objects within groups
  // are still ascending. This may require up to 3 ranges.

  // 1. Rest of start.group, clamped to fetchEnd when start.group is the
  // fetch's top group. start <= startGroupEnd is guaranteed by the
  // same-group early return above and the invariant start <= fetchEnd-1.
  AbsoluteLocation startGroupEnd = (start.group == fetchEnd.group)
      ? *fetchEnd.prevInGroup()
      : AbsoluteLocation{start.group, kLocationMax.object};
  ranges.emplace_back(start, startGroupEnd);

  // 2. Intermediate groups (between start.group-1 and end.group+1)
  // start.group > end.group guaranteed (descending, different groups),
  // so end.group < MAX and start.group > 0.
  auto endNextGroup = end.nextGroup();
  auto startPrevGroupEnd = start.prevGroupEnd();
  XCHECK(endNextGroup && startPrevGroupEnd);
  if (startPrevGroupEnd->group >= endNextGroup->group) {
    ranges.emplace_back(*endNextGroup, *startPrevGroupEnd);
  } // else the groups were consecutive

  // 3. Partial end.group, clamped to fetchStart when end.group is the fetch's
  // bottom group. endGroupStart < end skips both end.object == 0 (no partial
  // group) and end == fetchStart (iterator at its terminal position), and
  // proves end.prevInGroup() is non-empty.
  AbsoluteLocation endGroupStart = (end.group == fetchStart.group)
      ? fetchStart
      : AbsoluteLocation{end.group, 0};
  if (endGroupStart < end) {
    ranges.emplace_back(endGroupStart, *end.prevInGroup());
  }

  return ranges;
}

// Check if a group is known to be entirely nonexistent (all locations in
// [{groupID, 0}, {groupID, MAX}] are in gaps).
bool isGroupNonExistent(const LocationIntervalSet& gaps, uint64_t groupID) {
  auto end = gaps.findIntervalEnd({groupID, 0});
  return end &&
      (end->group > groupID ||
       (end->group == groupID && end->object == kLocationMax.object));
}

folly::Expected<folly::Unit, MoQPublishError> publishObject(
    ObjectStatus status,
    std::shared_ptr<FetchConsumer> consumer,
    const AbsoluteLocation& current,
    const MoQCache::CacheEntry& object,
    bool lastObject) {
  switch (status) {
    case ObjectStatus::NORMAL:
      return consumer->object(
          current.group,
          object.subgroup,
          current.object,
          object.payload->clone(),
          object.extensions,
          lastObject,
          object.forwardingPreferenceIsDatagram);
    case ObjectStatus::END_OF_GROUP:
      return consumer->endOfGroup(
          current.group, object.subgroup, current.object, lastObject);
    case ObjectStatus::END_OF_TRACK:
      return consumer->endOfTrackAndGroup(
          current.group, object.subgroup, current.object);
  }
  return folly::makeUnexpected(
      MoQPublishError{MoQPublishError::API_ERROR, "Unknown status"});
}

} // namespace

namespace moxygen {

folly::Expected<folly::Unit, MoQPublishError> MoQCache::CacheGroup::cacheObject(
    CacheTrack& track,
    uint64_t groupID,
    uint64_t subgroup,
    uint64_t objectID,
    ObjectStatus status,
    const Extensions& extensions,
    Payload payload,
    bool complete,
    bool forwardingPreferenceIsDatagram,
    TimePoint now) {
  XLOG(DBG1) << "caching group=" << groupID << " objID=" << objectID
             << " status=" << (uint32_t)status
             << " complete=" << uint32_t(complete);

  // Reject caching into a known gap
  if (track.gaps.contains({groupID, objectID})) {
    XLOG(ERR) << "Attempting to cache object in known gap; group=" << groupID
              << " objID=" << objectID;
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::MALFORMED_TRACK, "Object in known gap"));
  }

  // Process gap extensions first
  auto gapRes = track.processGapExtensions(groupID, objectID, extensions);
  if (gapRes.hasError()) {
    return gapRes;
  }

  // Handle END_OF_GROUP and END_OF_TRACK - mark remaining objects/groups as
  // gaps. TODO: we could skip caching EOG/EOT entirely by including objectID
  // in the gap range (start at objectID instead of objectID+1), then infer
  // end-of-group/track from gap data when serving.
  if (status == ObjectStatus::END_OF_GROUP ||
      status == ObjectStatus::END_OF_TRACK) {
    if (auto next = AbsoluteLocation{groupID, objectID}.nextInGroup()) {
      track.insertGap(*next, {groupID, kLocationMax.object});
    }
    if (status == ObjectStatus::END_OF_TRACK) {
      if (auto nextGrp = AbsoluteLocation{groupID, 0}.nextGroup()) {
        track.insertGap(*nextGrp, kLocationMax);
      }
    }
    endOfGroup = true;
  }

  // Cache the object (including END_OF_GROUP/END_OF_TRACK status)
  // Compute new payload size once before the move
  size_t newPayloadSize = payload ? payload->computeChainDataLength() : 0;
  auto it = objects.find(objectID);
  if (it != objects.end()) {
    auto& cachedObject = it->second;
    if (status != cachedObject->status) {
      XLOG(ERR) << "Invalid cache status change; objID=" << objectID
                << " status=" << (uint32_t)status
                << " already exists with different status";
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::MALFORMED_TRACK, "Invalid status change"));
    }
    if (status == ObjectStatus::NORMAL && cachedObject->complete &&
        ((!payload && cachedObject->payload) ||
         (payload && !cachedObject->payload) ||
         (payload && cachedObject->payload &&
          newPayloadSize != cachedObject->payloadSize))) {
      XLOG(ERR) << "Payload mismatch; objID=" << objectID;
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::MALFORMED_TRACK, "payload mismatch"));
    }
    if (cachedObject->forwardingPreferenceIsDatagram !=
        forwardingPreferenceIsDatagram) {
      XLOG(ERR) << "forwardingPreferenceIsDatagram mismatch; objID="
                << objectID;
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::MALFORMED_TRACK, "forwardingPreference mismatch"));
    }

    // TODO: Consider removing status from CacheEntry. For fetch streams, we
    // could only publish NORMAL objects and let END_OF_GROUP/END_OF_TRACK be
    // implicit from the gap information.
    cachedObject->status = status;
    cachedObject->extensions = extensions;
    totalBytes -= cachedObject->payloadSize;
    cachedObject->payload = std::move(payload);
    cachedObject->payloadSize = newPayloadSize;
    totalBytes += newPayloadSize;
    cachedObject->complete = complete;
    cachedObject->cachedAt = now;
  } else {
    auto entry = std::make_unique<CacheEntry>(
        subgroup,
        status,
        extensions,
        std::move(payload),
        newPayloadSize,
        complete,
        forwardingPreferenceIsDatagram,
        now);
    totalBytes += newPayloadSize;
    objects[objectID] = std::move(entry);
  }
  if (complete) {
    track.cachedContent.insert({groupID, objectID});
  }
  if (objectID >= maxCachedObject) {
    maxCachedObject = objectID;
  }
  return folly::unit;
}

class MoQCache::FetchHandle : public Publisher::FetchHandle {
 public:
  explicit FetchHandle(FetchOk ok) : Publisher::FetchHandle(std::move(ok)) {}
  FetchHandle() = delete;
  FetchHandle(const FetchHandle&) = delete;
  FetchHandle& operator=(const FetchHandle&) = delete;
  FetchHandle(FetchHandle&&) = delete;
  FetchHandle& operator=(FetchHandle&&) = delete;

  void fetchCancel() override {
    XLOG(DBG1) << __func__;
    source_.requestCancellation();
    if (upstreamFetchHandle_) {
      XLOG(DBG1) << __func__ << " cancel upstream";
      upstreamFetchHandle_->fetchCancel();
    }
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "REQUEST_UPDATE not supported for cached FETCH"});
  }

  folly::CancellationToken getToken() {
    return source_.getToken();
  }

  void setUpstreamFetchHandle(std::shared_ptr<Publisher::FetchHandle> handle) {
    upstreamFetchHandle_ = handle;
  }

 private:
  folly::CancellationSource source_;
  std::shared_ptr<Publisher::FetchHandle> upstreamFetchHandle_;
};

folly::Expected<folly::Unit, MoQPublishError>
MoQCache::CacheTrack::updateLargest(AbsoluteLocation current, bool eot) {
  // Check for a new largest object past the old endOfTrack
  if (!largestGroupAndObject || current > *largestGroupAndObject) {
    if (endOfTrack) {
      XLOG(ERR) << "Malformed track, end of track set, but new largest object";
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::MALFORMED_TRACK, "Malformed track"));
    } else {
      endOfTrack = eot;
    }
    largestGroupAndObject = current;
  } else if (eot && current != *largestGroupAndObject) {
    // End of track is not the largest
    XLOG(ERR) << "Malformed track, eot is not the largest object";
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::MALFORMED_TRACK, "Malformed track"));
  }
  return folly::unit;
}

MoQCache::CacheGroup& MoQCache::CacheTrack::getOrCreateGroup(uint64_t groupID) {
  auto it = groups.find(groupID);
  if (it == groups.end()) {
    it = groups.emplace(groupID, std::make_shared<CacheGroup>()).first;
  }
  return *it->second;
}

MoQCache::CacheGroup& MoQCache::CacheTrack::getOrCreateGroupWithEviction(
    uint64_t groupID,
    MoQCache& cache) {
  auto it = groups.find(groupID);
  if (it == groups.end()) {
    cache.evictOldestGroupsIfNeeded(*this);
    it = groups.emplace(groupID, std::make_shared<CacheGroup>()).first;
    // New group starts in LRU (evictable)
    cache.addGroupToLRU(groupID, *it->second, *this);
  }
  return *it->second;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQCache::CacheTrack::processGapExtensions(
    uint64_t groupID,
    uint64_t objectID,
    const Extensions& objectExtensions) {
  // Process Prior Group ID Gap extension (0x3C)
  auto priorGroupGap =
      objectExtensions.getIntExtension(kPriorGroupIdGapExtensionType);
  if (priorGroupGap) {
    uint64_t gap = *priorGroupGap;

    // Validate: gap must not be larger than groupID
    if (gap > groupID) {
      XLOG(ERR) << "Prior Group ID Gap " << gap << " is larger than Group ID "
                << groupID;
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::MALFORMED_TRACK,
          "Prior Group ID Gap larger than Group ID"));
    }

    // Check if we've already seen a Prior Group ID Gap for this group
    auto& currentGroup = getOrCreateGroup(groupID);
    if (currentGroup.seenPriorGroupIdGap) {
      if (*currentGroup.seenPriorGroupIdGap != gap) {
        XLOG(ERR) << "Prior Group ID Gap mismatch in group " << groupID
                  << ": previously saw " << *currentGroup.seenPriorGroupIdGap
                  << ", now got " << gap;
        return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::MALFORMED_TRACK,
            "Inconsistent Prior Group ID Gap values in group"));
      }
      // Same value, already processed - skip
    } else {
      // First time seeing Prior Group ID Gap for this group
      currentGroup.seenPriorGroupIdGap = gap;

      if (gap > 0) {
        // Check for conflicts with existing real objects in the gap
        // Only check up to kMaxGapValidation items to avoid O(n) for huge gaps
        uint64_t checkCount = std::min(gap, kMaxGapValidation);
        for (uint64_t g = groupID - checkCount; g < groupID; ++g) {
          auto groupIt = groups.find(g);
          if (groupIt != groups.end() && !groupIt->second->objects.empty()) {
            XLOG(ERR) << "Prior Group ID Gap covers existing object in group "
                      << g;
            return folly::makeUnexpected(MoQPublishError(
                MoQPublishError::MALFORMED_TRACK,
                "Prior Group ID Gap covers existing object"));
          }
        }

        // Mark groups in the gap as known (non-existent) via
        // LocationIntervalSet
        insertGap({groupID - gap, 0}, {groupID - 1, kLocationMax.object});
      }
    }
  }

  // Process Prior Object ID Gap extension (0x3E)
  // Unlike Prior Group ID Gap, different values across objects in a group are
  // valid (each object can independently indicate which prior objects don't
  // exist). We allow redundant marking of already-not-existing objects.
  auto priorObjectGap =
      objectExtensions.getIntExtension(kPriorObjectIdGapExtensionType);
  if (priorObjectGap) {
    uint64_t gap = *priorObjectGap;

    // Validate: gap must not be larger than objectID
    if (gap > objectID) {
      XLOG(ERR) << "Prior Object ID Gap " << gap << " is larger than Object ID "
                << objectID;
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::MALFORMED_TRACK,
          "Prior Object ID Gap larger than Object ID"));
    }

    if (gap > 0) {
      // Check for conflicts with existing real objects in the gap
      // Only check up to kMaxGapValidation items to avoid O(n) for huge gaps
      uint64_t checkCount = std::min(gap, kMaxGapValidation);
      auto& group = getOrCreateGroup(groupID);
      for (uint64_t o = objectID - checkCount; o < objectID; ++o) {
        auto it = group.objects.find(o);
        if (it != group.objects.end()) {
          XLOG(ERR) << "Prior Object ID Gap covers existing object " << o
                    << " in group " << groupID;
          return folly::makeUnexpected(MoQPublishError(
              MoQPublishError::MALFORMED_TRACK,
              "Prior Object ID Gap covers existing object"));
        }
      }

      // Mark objects in the gap as known (non-existent) via LocationIntervalSet
      insertGap({groupID, objectID - gap}, {groupID, objectID - 1});
    }
  }

  return folly::unit;
}

class MoQCache::SubgroupWriteback : public SubgroupConsumer {
 public:
  SubgroupWriteback(
      uint64_t group,
      uint64_t subgroup,
      std::shared_ptr<SubgroupConsumer> consumer,
      std::shared_ptr<CacheTrack> cacheTrackPtr,
      std::shared_ptr<CacheGroup> cacheGroupPtr,
      MoQCache& cache)
      : group_(group),
        subgroup_(subgroup),
        consumer_(std::move(consumer)),
        cacheTrackPtr_(std::move(cacheTrackPtr)),
        cacheTrack_(*cacheTrackPtr_),
        cacheGroupPtr_(std::move(cacheGroupPtr)),
        cacheGroup_(*cacheGroupPtr_),
        cache_(cache) {
    cache_.removeGroupFromLRU(cacheGroup_, cacheTrack_);
  }
  SubgroupWriteback() = delete;
  SubgroupWriteback(const SubgroupWriteback&) = delete;
  SubgroupWriteback& operator=(const SubgroupWriteback&) = delete;
  SubgroupWriteback(SubgroupWriteback&&) = delete;
  SubgroupWriteback& operator=(SubgroupWriteback&&) = delete;

  ~SubgroupWriteback() override {
    // TODO: If the publisher writes many groups concurrently, all are pinned
    // and none can be evicted, potentially using a lot of memory.
    cache_.addGroupToLRU(group_, cacheGroup_, cacheTrack_);
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objID,
      Payload payload,
      Extensions ext,
      bool finSub) override {
    if (cacheTrack_.evicted) {
      return consumer_->object(
          objID, std::move(payload), std::move(ext), finSub);
    }
    auto res = cacheTrack_.updateLargest({group_, objID});
    if (!res) {
      consumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      return res;
    }
    auto cPayload = payload ? payload->clone() : nullptr;
    auto cacheRes = cache_.cacheObjectAndUpdateBytes(
        cacheGroup_,
        cacheTrack_,
        group_,
        subgroup_,
        objID,
        ObjectStatus::NORMAL,
        ext,
        std::move(cPayload),
        true /* complete */,
        false /* forwardingPreferenceIsDatagram */,
        cache_.now());
    if (cacheRes.hasError()) {
      consumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      return cacheRes;
    }
    return consumer_->object(objID, std::move(payload), std::move(ext), finSub);
  }

  void checkpoint() override {
    return consumer_->checkpoint();
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    if (cacheTrack_.evicted) {
      return consumer_->beginObject(
          objectID, length, std::move(initialPayload), std::move(extensions));
    }
    auto res = cacheTrack_.updateLargest({group_, objectID});
    if (!res) {
      consumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      return res;
    }
    auto cacheRes = cache_.cacheObjectAndUpdateBytes(
        cacheGroup_,
        cacheTrack_,
        group_,
        subgroup_,
        objectID,
        ObjectStatus::NORMAL,
        extensions,
        initialPayload ? initialPayload->clone() : nullptr,
        false,
        false,
        cache_.now());
    if (cacheRes.hasError()) {
      consumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      return cacheRes;
    }
    currentObject_ = objectID;
    currentLength_ = length;
    if (initialPayload) {
      size_t initBytes = initialPayload->computeChainDataLength();
      XCHECK_GE(currentLength_, initBytes);
      currentLength_ -= initBytes;
    }
    return consumer_->beginObject(
        objectID, length, std::move(initialPayload), std::move(extensions));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    if (cacheTrack_.evicted) {
      return consumer_->objectPayload(std::move(payload), finSubgroup);
    }
    auto& object = cacheGroup_.objects[currentObject_];
    size_t addedBytes = payload->computeChainDataLength();
    if (object->payload) {
      object->payload->appendChain(payload->clone());
    } else {
      object->payload = payload->clone();
    }
    XCHECK_GE(currentLength_, addedBytes);
    currentLength_ -= addedBytes;
    if (currentLength_ == 0) {
      object->complete = true;
      cacheTrack_.cachedContent.insert({group_, currentObject_});
    }
    object->payloadSize += addedBytes;
    cacheGroup_.totalBytes += addedBytes;
    cache_.totalCachedBytes_ += addedBytes;
    cache_.evictForByteLimitIfNeeded();
    return consumer_->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) override {
    if (cacheTrack_.evicted) {
      return consumer_->endOfGroup(endOfGroupObjectID);
    }
    auto res = cacheTrack_.updateLargest({group_, endOfGroupObjectID});
    if (!res) {
      consumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      return res;
    }
    // TODO: END_OF_GROUP doesn't need to be cached as an object — just insert
    // the gap. Requires removing status from CacheEntry and inferring
    // end-of-group from gap data in publishObject.
    auto cacheRes = cache_.cacheObjectAndUpdateBytes(
        cacheGroup_,
        cacheTrack_,
        group_,
        subgroup_,
        endOfGroupObjectID,
        ObjectStatus::END_OF_GROUP,
        noExtensions(),
        nullptr,
        true,
        false,
        cache_.now());
    if (cacheRes.hasError()) {
      consumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      return cacheRes;
    }
    return consumer_->endOfGroup(endOfGroupObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    if (cacheTrack_.evicted) {
      return consumer_->endOfTrackAndGroup(endOfTrackObjectID);
    }
    auto res = cacheTrack_.updateLargest({group_, endOfTrackObjectID}, true);
    if (!res) {
      consumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      return res;
    }
    auto cacheRes = cache_.cacheObjectAndUpdateBytes(
        cacheGroup_,
        cacheTrack_,
        group_,
        subgroup_,
        endOfTrackObjectID,
        ObjectStatus::END_OF_TRACK,
        noExtensions(),
        nullptr,
        true,
        false,
        cache_.now());
    if (cacheRes.hasError()) {
      consumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
      return cacheRes;
    }
    return consumer_->endOfTrackAndGroup(endOfTrackObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    return consumer_->endOfSubgroup();
  }

  void reset(ResetStreamErrorCode error) override {
    return consumer_->reset(error);
  }

 private:
  uint64_t group_;
  uint64_t subgroup_;
  std::shared_ptr<SubgroupConsumer> consumer_;
  std::shared_ptr<CacheTrack> cacheTrackPtr_; // Prevent UAF if cache cleared
  CacheTrack& cacheTrack_;
  std::shared_ptr<CacheGroup> cacheGroupPtr_; // Prevent UAF if cache cleared
  CacheGroup& cacheGroup_;
  MoQCache& cache_;
  uint64_t currentObject_{0};
  uint64_t currentLength_{0};
};

// Caches incoming objects from a subscription and forwards to the consumer.
// Also maintains the "live" bit for tracks in the cache.
class MoQCache::SubscribeWriteback : public TrackConsumer {
 public:
  SubscribeWriteback(
      std::shared_ptr<TrackConsumer> consumer,
      std::shared_ptr<CacheTrack> trackPtr,
      MoQCache& cache,
      const FullTrackName& ftn)
      : consumer_(std::move(consumer)),
        trackPtr_(std::move(trackPtr)),
        track_(*trackPtr_),
        cache_(cache),
        ftn_(ftn) {
    // Track becomes non-evictable (remove from LRU)
    cache_.removeTrackFromLRU(track_);
    track_.liveWritebackCount++;
  }
  SubscribeWriteback() = delete;
  SubscribeWriteback(const SubscribeWriteback&) = delete;
  SubscribeWriteback& operator=(const SubscribeWriteback&) = delete;
  SubscribeWriteback(SubscribeWriteback&&) = delete;
  SubscribeWriteback& operator=(SubscribeWriteback&&) = delete;

  ~SubscribeWriteback() override {
    XCHECK_GT(track_.liveWritebackCount, 0u);
    track_.liveWritebackCount--;
    // Track may become evictable (add back to LRU if still in cache)
    cache_.onTrackBecameEvictable(ftn_);
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override {
    return consumer_->setTrackAlias(std::move(alias));
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      bool /*containsLastInGroup*/ = false) override {
    // TODO: Handle containsLastInGroup parameter when caching
    // Check if the group is known to not exist
    if (isGroupNonExistent(track_.gaps, groupID)) {
      XLOG(ERR) << "Attempting to begin subgroup in group already marked as "
                   "not existing; group="
                << groupID;
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::MALFORMED_TRACK, "Invalid status change"));
    }
    auto res = consumer_->beginSubgroup(groupID, subgroupID, priority);
    if (res.hasValue() && !track_.evicted) {
      track_.getOrCreateGroupWithEviction(groupID, cache_);
      return std::make_shared<SubgroupWriteback>(
          groupID,
          subgroupID,
          std::move(res.value()),
          trackPtr_,
          trackPtr_->groups[groupID],
          cache_);
    } else {
      return res;
    }
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return consumer_->awaitStreamCredit();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool /*lastInGroup*/ = false) override {
    if (track_.evicted) {
      return consumer_->objectStream(header, std::move(payload));
    }
    // TODO: Handle lastInGroup parameter when caching
    auto res = track_.updateLargest(
        {header.group, header.id}, isEndOfTrack(header.status));
    if (!res) {
      return res;
    }
    auto cacheRes = cache_.cacheObjectAndUpdateBytes(
        track_.getOrCreateGroupWithEviction(header.group, cache_),
        track_,
        header.group,
        header.subgroup,
        header.id,
        header.status,
        header.extensions,
        payload ? payload->clone() : nullptr,
        true,
        false,
        cache_.now());
    if (cacheRes.hasError()) {
      return cacheRes;
    }
    return consumer_->objectStream(header, std::move(payload));
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool /*lastInGroup*/ = false) override {
    if (track_.evicted) {
      return consumer_->datagram(header, std::move(payload));
    }
    // TODO: Handle lastInGroup parameter when caching
    auto res = track_.updateLargest(
        {header.group, header.id}, isEndOfTrack(header.status));
    if (!res) {
      return res;
    }
    auto cacheRes = cache_.cacheObjectAndUpdateBytes(
        track_.getOrCreateGroupWithEviction(header.group, cache_),
        track_,
        header.group,
        header.subgroup,
        header.id,
        header.status,
        header.extensions,
        payload ? payload->clone() : nullptr,
        true,
        true /* forwardingPreferenceIsDatagram */,
        cache_.now());
    if (cacheRes.hasError()) {
      return cacheRes;
    }
    return consumer_->datagram(header, std::move(payload));
  }

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override {
    return consumer_->publishDone(std::move(pubDone));
  }

 private:
  std::shared_ptr<TrackConsumer> consumer_;
  std::shared_ptr<CacheTrack> trackPtr_; // Prevent UAF if cache cleared
  CacheTrack& track_;
  MoQCache& cache_;
  FullTrackName ftn_;
};

MoQCache::FetchWriteback* MoQCache::CacheTrack::findFetchInProgress(
    AbsoluteLocation loc) {
  if (fetchesInProgress.empty()) {
    return nullptr;
  }
  auto it = fetchesInProgress.upper_bound(loc);
  if (it == fetchesInProgress.begin()) {
    return nullptr;
  }
  --it;
  if (loc >= it->first && loc < it->second.end && loc >= it->second.progress) {
    return it->second.writeback;
  }
  return nullptr;
}

// Caches incoming objects and forwards them to the consumer. Handles gaps in
// the range by marking NonExistent objects.
class MoQCache::FetchWriteback : public FetchConsumer {
 public:
  FetchWriteback(
      AbsoluteLocation start,
      AbsoluteLocation end,
      bool proxyFin,
      std::shared_ptr<FetchConsumer> consumer,
      FetchRangeIterator fetchRangeIt,
      MoQCache& cache,
      const FullTrackName& ftn)
      : start_(start),
        end_(end),
        proxyFin_(proxyFin),
        consumer_(std::move(consumer)),
        fetchRangeIt_(std::move(fetchRangeIt)),
        cache_(cache),
        ftn_(ftn) {
    // Track becomes non-evictable (remove from LRU)
    cache_.removeTrackFromLRU(*fetchRangeIt_.track);

    emplaceAndPinGroup(start, FetchInProgressEntry{end, start, this});
  }

  ~FetchWriteback() override {
    XLOG(DBG1) << "FetchWriteback destructing";
    inProgress_.post();
    if (fetchInProgressIt_ != fetchRangeIt_.track->fetchesInProgress.end()) {
      eraseAndMakeGroupEvictable();
    }
    cancelSource_.requestCancellation();

    // Track may become evictable (add back to LRU if still in cache)
    cache_.onTrackBecameEvictable(ftn_);
  }

  void updateInProgress() {
    inProgress_.post();
    if (fetchRangeIt_.isValid()) {
      auto current = *fetchRangeIt_;
      fetchInProgressIt_->second.progress = current;
      // Re-key the map entry when crossing a group boundary so the
      // old key slot is available for new fetches of evicted groups.
      if (current.group > fetchInProgressIt_->first.group) {
        auto entry = fetchInProgressIt_->second;
        eraseAndMakeGroupEvictable();
        emplaceAndPinGroup(current, entry);
      }
      inProgress_.reset();
    } else {
      eraseAndMakeGroupEvictable();
    }
  }

  folly::coro::Task<void> waitFor(AbsoluteLocation loc) {
    if (loc >= fetchRangeIt_.maxLocation) {
      co_return;
    }
    auto token = cancelSource_.getToken();
    while (!token.isCancellationRequested() && fetchRangeIt_.isValid() &&
           loc >= *fetchRangeIt_) {
      co_await inProgress_;
    }
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t gID,
      uint64_t sgID,
      uint64_t objID,
      Payload payload,
      Extensions ext,
      bool fin,
      bool forwardingPreferenceIsDatagram = false) override {
    if (fetchRangeIt_.track->evicted) {
      return consumer_->object(
          gID,
          sgID,
          objID,
          std::move(payload),
          std::move(ext),
          fin && proxyFin_,
          forwardingPreferenceIsDatagram);
    }
    constexpr auto kNormal = ObjectStatus::NORMAL;
    auto res = cacheImpl(
        gID,
        sgID,
        objID,
        kNormal,
        ext,
        payload->clone(),
        true,
        fin,
        forwardingPreferenceIsDatagram);
    if (!res) {
      return res;
    }
    XLOG(DBG1) << "forward object " << AbsoluteLocation(gID, objID);
    return consumer_->object(
        gID,
        sgID,
        objID,
        std::move(payload),
        std::move(ext),
        fin && proxyFin_,
        forwardingPreferenceIsDatagram);
  }

  void checkpoint() override {
    consumer_->checkpoint();
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t gID,
      uint64_t sgID,
      uint64_t objID,
      uint64_t len,
      Payload initPayload,
      Extensions ext) override {
    if (fetchRangeIt_.track->evicted) {
      return consumer_->beginObject(
          gID, sgID, objID, len, std::move(initPayload), std::move(ext));
    }
    constexpr auto kNormal = ObjectStatus::NORMAL;
    auto payload = initPayload ? initPayload->clone() : nullptr;
    auto res = cacheImpl(
        gID, sgID, objID, kNormal, ext, std::move(payload), false, false);
    if (!res) {
      return res;
    }
    currentLength_ = len;
    if (initPayload) {
      size_t initBytes = initPayload->computeChainDataLength();
      XCHECK_GE(currentLength_, initBytes);
      currentLength_ -= initBytes;
    }
    return consumer_->beginObject(
        gID, sgID, objID, len, std::move(initPayload), std::move(ext));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finFetch) override {
    if (fetchRangeIt_.track->evicted) {
      return consumer_->objectPayload(
          std::move(payload), finFetch && proxyFin_);
    }
    auto& group = fetchRangeIt_.track->getOrCreateGroupWithEviction(
        fetchRangeIt_->group, cache_);
    auto& object = group.objects[fetchRangeIt_->object];
    size_t addedBytes = payload->computeChainDataLength();
    if (object->payload) {
      object->payload->appendChain(payload->clone());
    } else {
      object->payload = payload->clone();
    }
    XCHECK_GE(currentLength_, addedBytes);
    currentLength_ -= addedBytes;
    if (currentLength_ == 0) {
      object->complete = true;
      fetchRangeIt_.track->cachedContent.insert(*fetchRangeIt_);
    }
    object->payloadSize += addedBytes;
    group.totalBytes += addedBytes;
    cache_.totalCachedBytes_ += addedBytes;
    cache_.evictForByteLimitIfNeeded();
    if (finFetch) {
      // Iterator still ON the just-completed object; step past it before
      // tail-marking so the gap range doesn't overlap it. Use the iterator's
      // order-aware end (DESC: lowest position; ASC: end_).
      fetchRangeIt_.next();
      markNonExistentTo(fetchRangeIt_.end());
      updateInProgress();
    }
    return consumer_->objectPayload(std::move(payload), finFetch && proxyFin_);
  }

  folly::Expected<folly::Unit, MoQPublishError>
  endOfGroup(uint64_t gID, uint64_t sgID, uint64_t objID, bool fin) override {
    if (fetchRangeIt_.track->evicted) {
      return consumer_->endOfGroup(gID, sgID, objID, fin && proxyFin_);
    }
    // cacheImpl -> cacheObject inserts gap for remaining objects in this group
    constexpr auto kEndOfGroup = ObjectStatus::END_OF_GROUP;
    auto res = cacheImpl(
        gID, sgID, objID, kEndOfGroup, noExtensions(), nullptr, true, fin);
    if (!res) {
      return res;
    }
    return consumer_->endOfGroup(gID, sgID, objID, fin && proxyFin_);
  }

  folly::Expected<folly::Unit, MoQPublishError>
  endOfTrackAndGroup(uint64_t gID, uint64_t sgID, uint64_t objID) override {
    if (fetchRangeIt_.track->evicted) {
      return consumer_->endOfTrackAndGroup(gID, sgID, objID);
    }
    // cacheImpl -> cacheObject inserts gaps for remaining objects and groups
    constexpr auto kEndOfTrack = ObjectStatus::END_OF_TRACK;
    auto res = cacheImpl(
        gID, sgID, objID, kEndOfTrack, noExtensions(), nullptr, true, true);
    if (!res) {
      return res;
    }
    return consumer_->endOfTrackAndGroup(gID, sgID, objID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    // Mark all remaining positions as known (upstream has completed)
    markNonExistentTo(fetchRangeIt_.end());
    updateInProgress();
    complete_.post();
    if (proxyFin_) {
      XLOG(DBG1) << "Forward End of Fetch";
      return consumer_->endOfFetch();
    }
    return folly::unit;
  }

  void reset(ResetStreamErrorCode error) override {
    XLOG(DBG1) << "FetchWriteback reset=" << uint32_t(error);
    consumer_->reset(error);
    wasReset_ = true;
    complete_.post();
    start_ = end_; // nothing else is coming
    fetchRangeIt_.invalidate();
    updateInProgress();
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    // Currently we fill at the consumer's rate.  We could also allow some
    // buffering here and fill at a higher rate.
    return consumer_->awaitReadyToConsume();
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfUnknownRange(
      uint64_t groupId,
      uint64_t objectId,
      bool finFetch) override {
    // Skip iterator forward WITHOUT marking objects as missing.
    // This allows the cache to potentially serve these objects later
    // if they become available from another source.
    AbsoluteLocation target{groupId, objectId};
    fetchRangeIt_.advanceTo(target);
    // Advance past target
    fetchRangeIt_.next();

    // Record that this upstream fetch has made progress (or finished) so any
    // other concurrent fetches waiting on the same range can continue.
    updateInProgress();

    if (finFetch) {
      complete_.post();
    }

    // Forward to downstream consumer
    return consumer_->endOfUnknownRange(
        groupId, objectId, finFetch && proxyFin_);
  }

  folly::coro::Task<void> complete() {
    co_await complete_;
  }

  bool wasReset() const {
    return wasReset_;
  }

 private:
  void emplaceAndPinGroup(AbsoluteLocation loc, FetchInProgressEntry entry) {
    auto [it, inserted] =
        fetchRangeIt_.track->fetchesInProgress.emplace(loc, entry);
    XCHECK(inserted);
    fetchInProgressIt_ = it;
    // Eagerly create the group if it doesn't exist so we can pin it before
    // any object lands. Otherwise getOrCreateGroupWithEviction would put
    // it in the LRU and a concurrent eviction could free the group while
    // this fetch is actively writing into it.
    auto& groupMap = fetchRangeIt_.track->groups;
    auto groupIt = groupMap.find(loc.group);
    if (groupIt == groupMap.end()) {
      cache_.evictOldestGroupsIfNeeded(*fetchRangeIt_.track);
      groupIt =
          groupMap.emplace(loc.group, std::make_shared<CacheGroup>()).first;
    }
    pinnedGroup_ = groupIt->second;
    cache_.removeGroupFromLRU(*pinnedGroup_, *fetchRangeIt_.track);
  }

  void eraseAndMakeGroupEvictable() {
    auto groupID = fetchInProgressIt_->first.group;
    fetchRangeIt_.track->fetchesInProgress.erase(fetchInProgressIt_);
    fetchInProgressIt_ = fetchRangeIt_.track->fetchesInProgress.end();
    if (pinnedGroup_) {
      cache_.addGroupToLRU(groupID, *pinnedGroup_, *fetchRangeIt_.track);
      pinnedGroup_.reset();
    }
  }

  AbsoluteLocation start_;
  AbsoluteLocation end_;
  FetchesInProgressMap::iterator fetchInProgressIt_;
  std::shared_ptr<CacheGroup> pinnedGroup_;
  bool proxyFin_{false};
  std::shared_ptr<FetchConsumer> consumer_;
  folly::coro::Baton inProgress_;
  folly::coro::Baton complete_;
  uint64_t currentLength_{0};
  bool wasReset_{false};
  folly::CancellationSource cancelSource_;
  FetchRangeIterator fetchRangeIt_;
  MoQCache& cache_;
  FullTrackName ftn_;

  void markNonExistentTo(AbsoluteLocation target) {
    // Mark all positions from current iterator position up to (but not
    // including) target as nonexistent, using range inserts for efficiency.
    if (*fetchRangeIt_ == target) {
      return;
    }

    auto ranges = getGapRanges(
        *fetchRangeIt_,
        target,
        fetchRangeIt_.order,
        fetchRangeIt_.minLocation,
        fetchRangeIt_.maxLocation);
    for (const auto& [start, end] : ranges) {
      fetchRangeIt_.track->insertGap(start, end);
    }

    // Advance iterator directly to target
    fetchRangeIt_.advanceTo(target);
  }

  folly::Expected<folly::Unit, MoQPublishError> cacheImpl(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      ObjectStatus status,
      const Extensions& extensions,
      Payload payload,
      bool complete,
      bool finFetch,
      bool forwardingPreferenceIsDatagram = false) {
    auto& group =
        fetchRangeIt_.track->getOrCreateGroupWithEviction(groupID, cache_);
    auto cacheRes = cache_.cacheObjectAndUpdateBytes(
        group,
        *fetchRangeIt_.track,
        groupID,
        subgroupID,
        objectID,
        status,
        extensions,
        std::move(payload),
        complete,
        forwardingPreferenceIsDatagram,
        cache_.now());
    // In a fetch, positions between received objects are known to not exist
    // (fetch is ordered, gaps indicate non-existence)
    markNonExistentTo({groupID, objectID});
    if (cacheRes.hasError()) {
      updateInProgress();
      return cacheRes;
    }
    auto res = fetchRangeIt_.track->updateLargest(
        {groupID, objectID}, isEndOfTrack(status));
    if (!res) {
      updateInProgress();
      return res;
    }
    if (complete) {
      fetchRangeIt_.next();
      updateInProgress();
      if (finFetch) {
        // Use the iterator's order-aware end. In DESC, end_ is the user's
        // highest endpoint (the wrong direction); fetchRangeIt_.end()
        // returns the iteration end (the lowest position).
        markNonExistentTo(fetchRangeIt_.end());
        complete_.post();
      }
    }
    return folly::unit;
  }
};

std::shared_ptr<TrackConsumer> MoQCache::getSubscribeWriteback(
    const FullTrackName& ftn,
    std::shared_ptr<TrackConsumer> consumer) {
  auto trackIt = cache_.find(ftn);
  if (trackIt == cache_.end()) {
    // New track - try to evict if over limit
    if (cache_.size() >= maxCachedTracks_) {
      evictOldestTrackIfNeeded();
      // Note: eviction may fail if all tracks have active operations,
      // in which case we temporarily exceed maxCachedTracks_
    }
    trackIt = cache_.emplace(ftn, std::make_shared<CacheTrack>()).first;
  }
  return std::make_shared<SubscribeWriteback>(
      std::move(consumer), trackIt->second, *this, ftn);
}

folly::coro::Task<Publisher::FetchResult> MoQCache::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer,
    std::shared_ptr<Publisher> upstream) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  CHECK(standalone);
  auto emplaceResult =
      cache_.emplace(fetch.fullTrackName, std::make_shared<CacheTrack>());
  auto trackIt = emplaceResult.first;
  auto track = trackIt->second;
  if (emplaceResult.second) {
    // New track - try to evict if over limit
    if (cache_.size() >= maxCachedTracks_) {
      evictOldestTrackIfNeeded();
      // Note: eviction may fail if all tracks have active operations,
      // in which case we temporarily exceed maxCachedTracks_
    }
    // New track starts in LRU (evictable)
    addTrackToLRU(fetch.fullTrackName, *track);

    // track is new (not cached), forward upstream, with writeback
    XLOG(DBG1) << "Cache miss, upstream fetch";
    FetchRangeIterator fetchRangeIt(
        standalone->start, standalone->end, fetch.groupOrder, track);
    co_return co_await upstream->fetch(
        fetch,
        std::make_shared<FetchWriteback>(
            standalone->start,
            standalone->end,
            true,
            std::move(consumer),
            fetchRangeIt,
            *this,
            fetch.fullTrackName));
  }
  AbsoluteLocation last = standalone->end;
  if (last.object > 0) {
    last.object--;
  } else {
    // {N, 0} means "all of group N" — inclusive last is {N, MAX}
    last.object = kLocationMax.object;
    auto nextGrp = standalone->end.nextGroup();
    if (!nextGrp) {
      XLOG(ERR) << "Fetch end group=" << standalone->end.group
                << " at max, using kLocationMax as exclusive end";
    }
    standalone->end = nextGrp.value_or(kLocationMax);
    // TODO: handle case where track.largestGroupAndObject is an END_OF_GROUP
    // or END_OF_TRACK
  }
  if (track->largestGroupAndObject &&
      (track->liveWritebackCount > 0 ||
       last <= *track->largestGroupAndObject)) {
    // we can immediately return fetch OK
    XLOG(DBG1) << "Live track or known past data, return FetchOK";
    AbsoluteLocation largestInFetch = standalone->end;
    bool isEndOfTrack = false;
    if (standalone->end > *track->largestGroupAndObject) {
      standalone->end = *track->largestGroupAndObject;
      auto next = standalone->end.next();
      XCHECK(next) << "largestGroupAndObject.next() must be valid";
      standalone->end = *next;
      largestInFetch = standalone->end;
      isEndOfTrack = track->endOfTrack;
      // fetchImpl range exclusive of end
    } else if (largestInFetch.object == 0) {
      auto pg = largestInFetch.prevGroup();
      XCHECK(pg) << "largestInFetch.group must be > 0 when object == 0";
      largestInFetch = *pg;
    }
    auto fetchHandle = std::make_shared<FetchHandle>(FetchOk{
        fetch.requestID,
        fetch.groupOrder,
        isEndOfTrack,
        largestInFetch,
        track->extensions});
    co_withExecutor(
        co_await folly::coro::co_current_executor,
        folly::coro::co_withCancellation(
            fetchHandle->getToken(),
            fetchImpl(
                fetchHandle,
                std::move(fetch),
                track,
                std::move(consumer),
                std::move(upstream))))
        .start();
    co_return fetchHandle;
  } else {
    XLOG(DBG1) << "No objects, or end > lastest and not live, fetchImpl";
    co_return co_await fetchImpl(
        nullptr,
        std::move(fetch),
        track,
        std::move(consumer),
        std::move(upstream));
  }
}

folly::coro::Task<Publisher::FetchResult> MoQCache::fetchImpl(
    std::shared_ptr<FetchHandle> fetchHandle,
    Fetch fetch,
    std::shared_ptr<CacheTrack> track,
    std::shared_ptr<FetchConsumer> consumer,
    std::shared_ptr<Publisher> upstream) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  XLOG(DBG1) << "fetchImpl for {" << standalone->start.group << ","
             << standalone->start.object << "}, {" << standalone->end.group
             << "," << standalone->end.object << "}";
  CHECK(standalone);
  auto token = co_await folly::coro::co_current_cancellation_token;
  std::optional<AbsoluteLocation> fetchStart;
  bool servedOneObject = false;
  folly::CancellationCallback cancelCallback(token, [consumer] {
    XLOG(DBG1) << "Fetch cancelled";
    consumer->reset(ResetStreamErrorCode::CANCELLED);
  });
  auto lastObject = false;
  auto cachedNow = now();
  FetchRangeIterator fetchRangeIt(
      standalone->start, standalone->end, fetch.groupOrder, track);
  while (!token.isCancellationRequested() &&
         (*fetchRangeIt) != fetchRangeIt.end()) {
    auto current = *fetchRangeIt;
    auto* blockingWriteback = track->findFetchInProgress(current);
    if (blockingWriteback) {
      XLOG(DBG1) << "fetchInProgress for {" << current.group << ","
                 << current.object << "}";
      co_await blockingWriteback->waitFor(current);
      cachedNow = now();
      // The in-progress fetch may have inserted new gap info
      fetchRangeIt.skipGaps();
      if (*fetchRangeIt != current) {
        // Gap was inserted at this position; restart loop
        continue;
      }
    }

    // Call getCachedObjectMaybe first: it realizes TTL by clearing
    // expired entries from cachedContent, avoiding a phantom flush.
    auto* cachedObject = getCachedObjectMaybe(*track, current, cachedNow);
    if (!cachedObject && !track->gaps.contains(current)) {
      if (!fetchStart) {
        fetchStart = current;
      }
      // Jump to the next known boundary (cached object or gap) so the
      // upstream fetch range stays bounded without O(n) iteration.
      fetchRangeIt.skipUncached();
      continue;
    }

    // For hits, pin the group so evictOldestGroupsIfNeeded (triggered
    // by FetchWriteback's cacheImpl during the await below) can't free
    // the CacheEntry* we're about to publish. Skip for gap boundaries:
    // no object to keep alive, and the gap's group may not exist, so
    // groups[current.group] would auto-insert a null entry.
    auto pin = cachedObject ? track->groups[current.group] : nullptr;
    // Boundary (hit or gap): flush any accumulated upstream miss range
    // so the upstream fetch doesn't span the boundary. A well-formed
    // upstream cannot transition this position from exists -> not-exists
    // during the await (writeback rejects out-of-range data and
    // gap/EOG over a served object), so the pre-await hit/gap decision
    // stays authoritative.
    if (fetchStart) {
      XLOG(DBG1) << "flushing accumulated upstream miss at boundary {"
                 << current.group << "," << current.object << "} ("
                 << (cachedObject ? "hit" : "gap") << ")";
      auto intervals = getFetchIntervals(
          fetchRangeIt.minLocation,
          fetchRangeIt.maxLocation,
          fetchStart.value(),
          current,
          fetchRangeIt.order,
          false);
      for (auto& interval : intervals) {
        auto res = co_await fetchUpstream(
            fetchHandle,
            interval.first,
            interval.second,
            /*lastObject=*/false,
            fetch,
            track,
            consumer,
            upstream);
        cachedNow = now();
        if (res.hasError()) {
          co_return folly::makeUnexpected(res.error());
        } // else success but only returns FetchOk on lastObject
      }
      fetchStart.reset();
    }

    if (!cachedObject) {
      // Gap boundary: skip past it and keep going.
      fetchRangeIt.skipGaps();
      continue;
    }

    XLOG(DBG1) << "publish HIT for {" << current.group << "," << current.object
               << "}";
    auto* object = cachedObject;
    fetchRangeIt.next();
    lastObject = (*fetchRangeIt) == fetchRangeIt.end();
    auto res =
        publishObject(object->status, consumer, current, *object, lastObject);
    if (res.hasError()) {
      if (res.error().code == MoQPublishError::BLOCKED) {
        XLOG(DBG1) << "Fetch blocked, waiting";
        auto blockedRes = co_await handleBlocked(consumer, fetch);
        cachedNow = now();
        if (blockedRes.hasError()) {
          co_return folly::makeUnexpected(blockedRes.error());
        }
      } else {
        XLOG(ERR) << "Consumer error=" << res.error().msg;
        auto resetCode = res.error().code == MoQPublishError::MALFORMED_TRACK
            ? ResetStreamErrorCode::MALFORMED_TRACK
            : ResetStreamErrorCode::INTERNAL_ERROR;
        consumer->reset(resetCode);
        co_return folly::makeUnexpected(
            FetchError{
                fetch.requestID,
                FetchErrorCode::INTERNAL_ERROR,
                folly::to<std::string>(
                    "Consumer error on object=", res.error().msg)});
      }
    } // else publish success
    servedOneObject = true;
  }
  if (fetchStart) {
    auto intervals = getFetchIntervals(
        fetchRangeIt.minLocation,
        fetchRangeIt.maxLocation,
        fetchStart.value(),
        *fetchRangeIt,
        fetchRangeIt.order,
        true);
    XLOG(DBG1) << "Fetching missing tail";
    Publisher::FetchResult res;
    for (auto& interval : intervals) {
      res = co_await fetchUpstream(
          fetchHandle,
          interval.first,
          interval.second,
          /*lastObject=*/(&interval == &intervals.back()),
          fetch,
          track,
          consumer,
          upstream);
      if (res.hasError()) {
        co_return folly::makeUnexpected(res.error());
      }
    }
    if (!fetchHandle) {
      XCHECK(res.value());
      if (fetch.groupOrder != GroupOrder::NewestFirst) {
        co_return std::move(res.value());
      } else {
        bool isEndOfTrack = track->endOfTrack &&
            standalone->end >= *track->largestGroupAndObject;
        fetchHandle = std::make_shared<FetchHandle>(FetchOk{
            fetch.requestID,
            fetch.groupOrder,
            isEndOfTrack,
            standalone->end,
            {}});
        fetchHandle->setUpstreamFetchHandle(res.value());
        co_return fetchHandle;
      }
    } else {
      co_return nullptr;
    }
  }
  // Call endOfFetch if we didn't serve an object with fin=true
  if (!(lastObject && servedOneObject)) {
    consumer->endOfFetch();
  }
  if (!fetchHandle) {
    XLOG(DBG1) << "Fetch completed entirely from cache";
    if (servedOneObject) {
      if (standalone->end.object == 0) {
        auto pg = standalone->end.prevGroup();
        XCHECK(pg) << "standalone->end.group must be > 0 when object == 0";
        standalone->end = *pg;
      }
      bool endOfTrack = false;
      if (track->endOfTrack &&
          standalone->end >= *track->largestGroupAndObject) {
        endOfTrack = true;
        standalone->end = *track->largestGroupAndObject;
      }
      co_return std::make_shared<FetchHandle>(FetchOk{
          fetch.requestID, fetch.groupOrder, endOfTrack, standalone->end, {}});
    } else {
      co_return std::make_shared<FetchHandle>(FetchOk{
          fetch.requestID, fetch.groupOrder, false, standalone->end, {}});
    }
  }
  co_return nullptr;
}

// Returns valid CacheEntry* on cache hit, nullptr on miss.
// Gap-skipping is handled by FetchRangeIterator before this is called.
MoQCache::CacheEntry* MoQCache::getCachedObjectMaybe(
    CacheTrack& track,
    AbsoluteLocation current,
    TimePoint now) {
  auto groupIt = track.groups.find(current.group);
  if (groupIt == track.groups.end()) {
    XLOG(DBG1) << "group cache miss for {" << current.group << "}";
    return nullptr;
  }

  auto& group = groupIt->second;
  auto objIt = group->objects.find(current.object);
  if (objIt == group->objects.end()) {
    XLOG(DBG1) << "object cache miss for {" << current.group << ","
               << current.object << "}";
    return nullptr;
  }

  auto effectiveDuration = track.maxCacheDuration ? track.maxCacheDuration
                                                  : defaultMaxCacheDuration_;
  if (effectiveDuration) {
    auto age = now - objIt->second->cachedAt;
    if (age > *effectiveDuration) {
      XLOG(DBG1) << "object expired for {" << current.group << ","
                 << current.object << "}";
      auto expiredBytes = objIt->second->payloadSize;
      group->totalBytes -= expiredBytes;
      totalCachedBytes_ -= expiredBytes;
      track.cachedContent.remove(current, current);
      group->objects.erase(objIt);
      return nullptr;
    }
  }

  if (!objIt->second->complete) {
    XLOG(DBG1) << "object incomplete for {" << current.group << ","
               << current.object << "}";
    return nullptr;
  }

  return objIt->second.get();
}

folly::coro::Task<Publisher::FetchResult> MoQCache::fetchUpstream(
    std::shared_ptr<MoQCache::FetchHandle> fetchHandle,
    const AbsoluteLocation& fetchStart,
    const AbsoluteLocation& fetchEnd,
    bool lastObject,
    Fetch fetch,
    std::shared_ptr<CacheTrack> track,
    std::shared_ptr<FetchConsumer> consumer,
    std::shared_ptr<Publisher> upstream) {
  XLOG(DBG1) << "Fetching upstream for {" << fetchStart.group << ","
             << fetchStart.object << "}, {" << fetchEnd.group << ","
             << fetchEnd.object << "}";
  auto adjFetchEnd = fetchEnd;
  if (adjFetchEnd.object == 0) {
    auto pg = adjFetchEnd.prevGroup();
    XCHECK(pg) << "fetchEnd={0,0} should not reach fetchUpstream";
    adjFetchEnd = *pg;
  }
  FetchRangeIterator fetchRangeIt(
      fetchStart, fetchEnd, fetch.groupOrder, track);
  // TODO: reconcile writeback end with upstream FetchOk.endLocation;
  // a smaller upstream end leaves stale fetchInProgress range that
  // makes concurrent lookups wait until endOfFetch.
  auto writeback = std::make_shared<FetchWriteback>(
      fetchStart,
      adjFetchEnd,
      lastObject,
      consumer,
      fetchRangeIt,
      *this,
      fetch.fullTrackName);
  auto res = co_await upstream->fetch(
      Fetch(
          0,
          fetch.fullTrackName,
          fetchStart,
          adjFetchEnd,
          fetch.priority,
          fetch.groupOrder),
      writeback);
  if (res.hasError()) {
    XLOG(ERR) << "upstream fetch failed err=" << res.error().reasonPhrase;
    consumer->reset(ResetStreamErrorCode::CANCELLED);
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID, res.error().errorCode, res.error().reasonPhrase});
  }

  XLOG(DBG1) << "upstream success";
  track->extensions = res.value()->fetchOk().extensions;
  if (lastObject) {
    if (!fetchHandle) {
      XLOG(DBG1) << "no fetchHandle and last object";
      fetchHandle = std::make_shared<FetchHandle>(FetchOk(
          fetch.requestID,
          fetch.groupOrder,
          res.value()->fetchOk().endOfTrack,
          res.value()->fetchOk().endLocation,
          res.value()->fetchOk().extensions,
          res.value()->fetchOk().params));
    }
    fetchHandle->setUpstreamFetchHandle(res.value());
    co_return fetchHandle;
  } else if (fetchHandle) {
    XLOG(DBG1) << "fetchHandle and not last object";
    fetchHandle->setUpstreamFetchHandle(res.value());
  }
  // not the last object, wait for writeback
  co_await writeback->complete();
  if (writeback->wasReset()) {
    // FetchOk but fetch stream was reset, can't continue
    XLOG(ERR) << "Fetch was reset, returning error";
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::INTERNAL_ERROR,
            folly::to<std::string>("Upstream fetch reset")});
  }
  // completed successfully, ready for next object
  co_return nullptr;
}

folly::coro::Task<folly::Expected<folly::Unit, FetchError>>
MoQCache::handleBlocked(
    std::shared_ptr<FetchConsumer> consumer,
    const Fetch& fetch) {
  auto awaitRes = consumer->awaitReadyToConsume();
  if (!awaitRes) {
    XLOG(ERR) << "awaitReadyToConsume error: " << awaitRes.error().what();
    consumer->reset(ResetStreamErrorCode::INTERNAL_ERROR);
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::INTERNAL_ERROR,
            folly::to<std::string>(
                "Consumer error awaiting ready=", awaitRes.error().msg)});
  }
  co_await std::move(awaitRes.value());
  co_return folly::unit;
}

void MoQCache::setMaxCacheDuration(
    const FullTrackName& ftn,
    std::chrono::milliseconds duration) {
  auto it = cache_.find(ftn);
  if (it == cache_.end()) {
    it = cache_.emplace(ftn, std::make_shared<CacheTrack>()).first;
    addTrackToLRU(ftn, *it->second);
  }
  it->second->maxCacheDuration = duration;
}

void MoQCache::clearMaxCacheDuration(const FullTrackName& ftn) {
  auto it = cache_.find(ftn);
  if (it != cache_.end()) {
    it->second->maxCacheDuration.reset();
  }
}

void MoQCache::setTrackExtensions(
    const FullTrackName& ftn,
    Extensions extensions) {
  auto it = cache_.find(ftn);
  if (it == cache_.end()) {
    it = cache_.emplace(ftn, std::make_shared<CacheTrack>()).first;
    addTrackToLRU(ftn, *it->second);
  }
  it->second->extensions = std::move(extensions);
}

MoQCache::FetchRangeIterator::FetchRangeIterator(
    AbsoluteLocation start,
    AbsoluteLocation end,
    GroupOrder order,
    std::shared_ptr<CacheTrack> track)
    : minLocation(start),
      maxLocation(end),
      order(order),
      track(track),
      current_(start),
      end_(end) {
  if (order == GroupOrder::NewestFirst) {
    // Newest first order iterates over groups in reverse order
    // But objects are iterarted in ascending order

    // Same group, objs ascending.
    if (start.group != end.group) {
      // --- set iterator start ---
      // If the last group object == 0, current needs to be
      // one group down, and the object starts at:
      //  0 if its not the start group
      //  start.object if it is
      if (end_.object == 0) {
        current_.group = end_.group - 1;
        if (current_.group == start.group) {
          current_.object = start.object;
        } else {
          current_.object = 0;
        }
      } else {
        // Set current to first obj of last group
        current_ = end_;
        current_.object = 0;
      }

      // --- set iterator end ---
      // Set end to the last object of the first group + 1
      // This could be endOfGroup if known, or max_int
      auto groupIt = track->groups.find(start.group);
      end_ = start;
      if (groupIt != track->groups.end()) {
        auto& group = groupIt->second;
        end_.object = kLocationMax.object;
        if (group->endOfGroup && group->maxCachedObject < kLocationMax.object) {
          end_.object = group->maxCachedObject + 1;
        }
      } else {
        // No group found, so we set it to max for now
        // Track update at callback will signal endOfGroup
        end_.object = kLocationMax.object;
      }
    }
  }
  // Skip any gaps at initial position
  skipGaps();
}

bool MoQCache::FetchRangeIterator::isValid() const {
  return isValid_;
}

void MoQCache::FetchRangeIterator::invalidate() {
  current_ = maxLocation;
  isValid_ = false;
}

void MoQCache::FetchRangeIterator::advanceTo(const AbsoluteLocation& loc) {
  current_ = loc;
  // Check if we've passed the valid range based on iteration direction
  // Note: use strict inequality - the loop condition handles reaching exactly
  // the end
  if (order != GroupOrder::NewestFirst) {
    // Ascending: invalid if current > maxLocation (passed the end)
    if (current_ > maxLocation) {
      isValid_ = false;
      return;
    }
  } else {
    // Descending: invalid if current < minLocation (passed the minimum)
    if (current_ < minLocation) {
      isValid_ = false;
      return;
    }
  }
  // Skip any gaps at new position
  skipGaps();
}

const AbsoluteLocation& MoQCache::FetchRangeIterator::operator*() const {
  return current_;
}

const AbsoluteLocation* MoQCache::FetchRangeIterator::operator->() const {
  return &current_;
}

void MoQCache::FetchRangeIterator::next() {
  if (!isValid_) {
    return;
  }
  advanceOne();
  skipGaps();
}

void MoQCache::FetchRangeIterator::advanceOne() {
  if (!isValid_) {
    return;
  }
  if (current_ == end_) {
    XLOG(DBG2) << "advanceOne: current_ == end_, staying at end";
    return;
  }

  auto groupEndMaybe =
      findGroupEndMaybe(current_.group, cachedGroupId_, cachedGroupPtr_);
  XLOG(DBG2) << "advanceOne: current={" << current_.group << ","
             << current_.object << "} groupEndMaybe="
             << (groupEndMaybe ? std::to_string(*groupEndMaybe) : "nullopt");
  if (groupEndMaybe && current_.object < groupEndMaybe.value()) {
    // Found group end, we can increment object till that
    current_.object++;
  } else {
    if (order == GroupOrder::NewestFirst) {
      // desc
      if (current_.group == 0) {
        XLOG(DBG2) << "advanceOne: at group 0, invalidating iterator";
        isValid_ = false;
        return;
      }
      current_.group--;
      current_.object = 0;
      if (current_.group == minLocation.group) {
        current_.object = minLocation.object;
      }
    } else {
      // asc
      if (current_.group == maxLocation.group) {
        XLOG(DBG2) << "advanceOne: at maxLocation.group, clamping to end";
        current_ = end_;
        return;
      }
      current_.group++;
      current_.object = 0;
    }
  }
  XLOG(DBG2) << "advanceOne: new current={" << current_.group << ","
             << current_.object << "}";
}

void MoQCache::FetchRangeIterator::skipGaps() {
  while (isValid_ && track->gaps.contains(current_)) {
    XLOG(DBG2) << "skipGaps: current={" << current_.group << ","
               << current_.object << "} is in gap";
    auto gapInterval = track->gaps.findInterval(current_);
    if (!gapInterval) {
      // Shouldn't happen, but fallback to single step
      advanceOne();
      continue;
    }

    auto [gapStart, gapEnd] = *gapInterval;
    XLOG(DBG2) << "skipGaps: gap=[{" << gapStart.group << "," << gapStart.object
               << "}, {" << gapEnd.group << "," << gapEnd.object << "}]";
    std::optional<AbsoluteLocation> afterGap;

    if (order != GroupOrder::NewestFirst) {
      // Ascending: skip to position after gap end
      afterGap = gapEnd.next();
    } else {
      afterGap = computeDescendingAfterGap(current_, gapStart, minLocation);
    }

    if (afterGap) {
      XLOG(DBG2) << "skipGaps: afterGap={" << afterGap->group << ","
                 << afterGap->object << "}";
      // Check bounds and clamp to end if needed
      // For ascending: afterGap must be < end_ (before exclusive end)
      // For descending: afterGap must be >= minLocation (within fetch range)
      bool inRange = (order != GroupOrder::NewestFirst)
          ? (*afterGap < end_)
          : (*afterGap >= minLocation);
      if (inRange) {
        current_ = *afterGap;
        XLOG(DBG2) << "skipGaps: moved to {" << current_.group << ","
                   << current_.object << "}";
      } else {
        // Gap extends to or past end - clamp to end
        XLOG(DBG2) << "skipGaps: clamping to end {" << end_.group << ","
                   << end_.object << "}";
        current_ = end_;
        return; // No point continuing the loop
      }
    } else {
      // Gap extends to boundary - clamp to end
      XLOG(DBG2) << "skipGaps: no afterGap, clamping to end";
      current_ = end_;
      return;
    }
  }
}

void MoQCache::FetchRangeIterator::skipUncached() {
  if (!isValid_ || current_ == end_) {
    return;
  }

  if (track->cachedContent.contains(current_)) {
    return;
  }

  if (order != GroupOrder::NewestFirst) {
    // Ascending: find next cached interval after current position
    auto nextInt = track->cachedContent.findNextInterval(current_);
    if (!nextInt || nextInt->first >= end_) {
      current_ = end_;
      return;
    }
    current_ = nextInt->first;
  } else {
    // Descending: groups iterate high-to-low, objects within groups
    // low-to-high. First check if there's more cached content later in
    // the current group.
    auto nextInGroup = track->cachedContent.findNextInterval(current_);
    if (nextInGroup && nextInGroup->first.group == current_.group) {
      current_ = nextInGroup->first;
      return;
    }
    // Otherwise jump to a lower group with cached content.
    auto target = findPrevGroupCachedPosition();
    if (!target) {
      current_ = {0, 0};
      isValid_ = false;
      return;
    }
    current_ = *target;
  }
}

std::optional<AbsoluteLocation>
MoQCache::FetchRangeIterator::findPrevGroupCachedPosition() const {
  // Query from the end of the previous group. This avoids finding
  // intervals in the current group (already consumed) or cross-group
  // intervals whose end is in the current group.
  auto prevGE = current_.prevGroupEnd();
  if (!prevGE) {
    return std::nullopt;
  }
  auto prev = track->cachedContent.findInterval(*prevGE);
  if (!prev) {
    prev = track->cachedContent.findPrevInterval(*prevGE);
  }
  // prev->second < minLocation rejects the case where the only cached
  // content in minLocation.group sits below minLocation — without this,
  // target gets clamped to minLocation, contains() returns false, and
  // findNextInterval(target) returns nullopt, tripping the XCHECK below.
  if (!prev || prev->second < minLocation) {
    return std::nullopt;
  }
  // prev->second.group can exceed prevGE->group when findInterval returned
  // a cross-group interval (e.g. {2,MAX}-{3,0}). Clamp to the previous
  // group so we don't revisit the current group.
  auto targetGroup = std::min(prev->second.group, prevGE->group);
  AbsoluteLocation target = {targetGroup, 0};
  if (target < minLocation) {
    target = minLocation;
  }
  // Find the first cached object in this group.
  if (!track->cachedContent.contains(target)) {
    auto nextInt = track->cachedContent.findNextInterval(target);
    XCHECK(nextInt && nextInt->first.group == targetGroup);
    target = nextInt->first;
  }
  return target;
}

/*
Semantics of finding group end are tricky becasue they depend of whether we are
asc or desc and also on, if this is some intermediate group, fetchEnd group (for
asc), fetchStart group (for desc), or if EOG is known or not.
For each group being traversed, FetchRangeIterator will iterate to the
value returned by this functon before moving to the next group
*/
std::optional<uint64_t> MoQCache::FetchRangeIterator::findGroupEndMaybe(
    uint64_t currGroup,
    uint64_t& cachedGroupId,
    std::shared_ptr<CacheGroup>& cachedGroupPtr) const {
  const bool isFirstGroup = currGroup == minLocation.group;
  const bool isLastGroup = currGroup == maxLocation.group;
  const bool isDescending = order == GroupOrder::NewestFirst;

  // Special case: last group in iteration range
  if (isLastGroup) {
    if (isDescending && !isFirstGroup) {
      return std::make_optional(maxLocation.object - 1);
    }
    return std::make_optional(maxLocation.object);
  }

  // Update cached group pointer if needed
  if (cachedGroupId != currGroup) {
    auto groupIt = track->groups.find(currGroup);
    cachedGroupPtr =
        (groupIt != track->groups.end()) ? groupIt->second : nullptr;
    cachedGroupId = currGroup;
  }

  if (cachedGroupPtr == nullptr) {
    return std::nullopt;
  }

  // Extract group information
  const auto& group = cachedGroupPtr;
  const bool isEndKnown = group->endOfGroup;
  const uint64_t maxObjectInGroup = group->maxCachedObject;

  // Calculate object boundaries (with overflow protection)
  const uint64_t endIncludingMaxCached =
      (maxObjectInGroup < kLocationMax.object) ? maxObjectInGroup + 1
                                               : maxObjectInGroup;
  const uint64_t firstUncachedObject =
      (endIncludingMaxCached < kLocationMax.object) ? endIncludingMaxCached + 1
                                                    : endIncludingMaxCached;

  // Handle first group
  if (isFirstGroup) {
    if (isDescending) {
      // Descending: this is where iteration ends
      return isEndKnown ? std::make_optional(endIncludingMaxCached)
                        : std::make_optional(firstUncachedObject);
    } else {
      // Ascending: this is the first group of iteration
      return isEndKnown ? std::make_optional(maxObjectInGroup)
                        : std::make_optional(endIncludingMaxCached);
    }
  }

  // Handle intermediate groups
  return isEndKnown ? std::make_optional(maxObjectInGroup)
                    : std::make_optional(endIncludingMaxCached);
}

AbsoluteLocation MoQCache::FetchRangeIterator::end() {
  if ((track->endOfTrack && current_ > track->largestGroupAndObject.value()) ||
      !isValid_) {
    end_ = current_;
    return end_;
  }

  // asc
  if (order != GroupOrder::NewestFirst) {
    return end_;
  }

  // desc
  auto endGroupLastObjMaybe =
      findGroupEndMaybe(end_.group, cachedEndGroupId_, cachedEndGroupPtr_);
  if (endGroupLastObjMaybe) {
    end_.object = endGroupLastObjMaybe.value();
  }
  return end_;
}

// ============================================================================
// Track LRU Management Helpers
// ============================================================================

void MoQCache::addTrackToLRU(const FullTrackName& ftn, CacheTrack& track) {
  if (track.lruIter_.hasValue()) {
    // Already in LRU
    return;
  }
  trackLRU_.push_front(ftn);
  track.lruIter_ = trackLRU_.begin();
  XLOG(DBG2) << "Added track to LRU: " << ftn;
}

void MoQCache::removeTrackFromLRU(CacheTrack& track) {
  if (!track.lruIter_.hasValue()) {
    // Not in LRU
    return;
  }
  trackLRU_.erase(*track.lruIter_);
  track.lruIter_.reset();
  XLOG(DBG2) << "Removed track from LRU";
}

void MoQCache::onTrackBecameEvictable(const FullTrackName& ftn) {
  auto it = cache_.find(ftn);
  if (it == cache_.end()) {
    // Track was already evicted from cache
    return;
  }
  auto& track = *it->second;
  if (track.canEvict()) {
    addTrackToLRU(ftn, track);
  }
}

// ============================================================================
// Group LRU Management Helpers
// ============================================================================

void MoQCache::addGroupToLRU(
    uint64_t groupID,
    CacheGroup& group,
    CacheTrack& track) {
  if (group.lruIter_.hasValue()) {
    // Already in LRU
    return;
  }
  track.groupLRU.push_front(groupID);
  group.lruIter_ = track.groupLRU.begin();
  XLOG(DBG2) << "Added group " << groupID << " to LRU";
}

void MoQCache::removeGroupFromLRU(CacheGroup& group, CacheTrack& track) {
  if (!group.lruIter_.hasValue()) {
    // Not in LRU
    return;
  }
  track.groupLRU.erase(*group.lruIter_);
  group.lruIter_.reset();
  XLOG(DBG2) << "Removed group from LRU";
}

// ============================================================================
// Eviction Methods
// ============================================================================

bool MoQCache::evictOldestTrackIfNeeded() {
  if (maxCachedTracks_ == 0 || cache_.size() < maxCachedTracks_) {
    return true;
  }

  if (trackLRU_.empty()) {
    // All tracks are non-evictable (have active operations)
    XLOG(DBG1) << "Cannot evict any track, all have active operations. "
               << "Cache size: " << cache_.size()
               << ", limit: " << maxCachedTracks_;
    return false;
  }

  // Take oldest evictable track from back of LRU
  const FullTrackName& oldestTrack = trackLRU_.back();
  XLOG(DBG1) << "Evicting oldest track: " << oldestTrack
             << " (cache size: " << cache_.size() << ")";
  evictTrack(oldestTrack);
  return true;
}

void MoQCache::evictTrack(const FullTrackName& ftn) {
  auto it = cache_.find(ftn);
  if (it == cache_.end()) {
    return;
  }

  auto& track = *it->second;
  // Remove from LRU if present
  if (track.lruIter_.hasValue()) {
    trackLRU_.erase(*track.lruIter_);
  }

  // Subtract bytes for all groups in the track
  for (auto& [gid, group] : track.groups) {
    XCHECK_GE(totalCachedBytes_, group->totalBytes);
    totalCachedBytes_ -= group->totalBytes;
  }

  // Remove from cache
  cache_.erase(it);
  XLOG(DBG1) << "Evicted track: " << ftn;
}

void MoQCache::evictOldestGroupsIfNeeded(CacheTrack& track) {
  if (maxCachedGroupsPerTrack_ == 0) {
    return; // Unlimited groups
  }

  while (track.groups.size() > maxCachedGroupsPerTrack_ &&
         !track.groupLRU.empty()) {
    uint64_t oldestGroupID = track.groupLRU.back();
    XLOG(DBG1) << "Evicting oldest group: " << oldestGroupID << " (track has "
               << track.groups.size() << " groups)";
    evictGroup(track, oldestGroupID);
  }

  if (track.groups.size() > maxCachedGroupsPerTrack_ &&
      track.groupLRU.empty()) {
    XLOG(DBG1) << "Cannot evict groups, all have active fetches. "
               << "Track has " << track.groups.size()
               << " groups, limit: " << maxCachedGroupsPerTrack_;
  }
}

void MoQCache::evictGroup(CacheTrack& track, uint64_t groupID) {
  auto it = track.groups.find(groupID);
  if (it == track.groups.end()) {
    return;
  }

  auto& group = *it->second;
  // Remove from LRU if present
  if (group.lruIter_.hasValue()) {
    track.groupLRU.erase(*group.lruIter_);
  }

  // Remove from cachedContent
  track.cachedContent.remove({groupID, 0}, {groupID, kLocationMax.object});

  // Subtract group's bytes from the cache total
  XCHECK_GE(totalCachedBytes_, group.totalBytes);
  totalCachedBytes_ -= group.totalBytes;

  // Remove from groups map
  track.groups.erase(it);
  XLOG(DBG1) << "Evicted group: " << groupID;
}

bool MoQCache::evictForByteLimitIfNeeded() {
  if (maxCachedBytes_ == 0 || totalCachedBytes_ <= maxCachedBytes_) {
    return true;
  }
  // Evict down to the low watermark to avoid thrashing
  size_t targetBytes = minEvictionBytes_ < maxCachedBytes_
      ? maxCachedBytes_ - minEvictionBytes_
      : 0;
  while (totalCachedBytes_ > targetBytes && !trackLRU_.empty()) {
    const FullTrackName& oldestTrackName = trackLRU_.back();
    auto it = cache_.find(oldestTrackName);
    if (it == cache_.end()) {
      trackLRU_.pop_back();
      continue;
    }
    auto& track = *it->second;
    if (!track.groupLRU.empty()) {
      // Evict oldest evictable group from the LRU track
      uint64_t oldestGroupID = track.groupLRU.back();
      XLOG(DBG1) << "Evicting group " << oldestGroupID << " from track "
                 << oldestTrackName
                 << " for byte limit (bytes: " << totalCachedBytes_
                 << " > limit: " << maxCachedBytes_ << ")";
      evictGroup(track, oldestGroupID);
      // If track now has no groups, evict the empty shell
      if (track.groups.empty()) {
        evictTrack(oldestTrackName);
      }
    } else if (track.groups.empty()) {
      // Empty shell track — safe to evict
      XLOG(DBG1) << "Evicting empty track for byte limit: " << oldestTrackName;
      evictTrack(oldestTrackName);
    } else {
      // Should be unreachable: if a track is in trackLRU_ it is evictable
      // (canEvict() == true), which means no active fetches or subscribes,
      // so all its groups should be in groupLRU.
      XLOG(DFATAL) << "LRU track " << oldestTrackName
                   << " has groups but empty groupLRU";
      break;
    }
  }
  if (totalCachedBytes_ > maxCachedBytes_) {
    XLOG(DBG1) << "Cannot reduce cache below byte limit, all tracks have "
                  "active operations. Bytes: "
               << totalCachedBytes_ << ", limit: " << maxCachedBytes_;
    return false;
  }
  return true;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQCache::cacheObjectAndUpdateBytes(
    CacheGroup& group,
    CacheTrack& track,
    uint64_t groupID,
    uint64_t subgroup,
    uint64_t objectID,
    ObjectStatus status,
    const Extensions& extensions,
    Payload payload,
    bool complete,
    bool forwardingPreferenceIsDatagram,
    TimePoint now) {
  size_t oldGroupBytes = group.totalBytes;
  auto res = group.cacheObject(
      track,
      groupID,
      subgroup,
      objectID,
      status,
      extensions,
      std::move(payload),
      complete,
      forwardingPreferenceIsDatagram,
      now);
  if (res.hasValue()) {
    if (group.totalBytes >= oldGroupBytes) {
      totalCachedBytes_ += group.totalBytes - oldGroupBytes;
    } else {
      XCHECK_GE(totalCachedBytes_, oldGroupBytes - group.totalBytes);
      totalCachedBytes_ -= oldGroupBytes - group.totalBytes;
    }
    evictForByteLimitIfNeeded();
  }
  return res;
}

} // namespace moxygen
