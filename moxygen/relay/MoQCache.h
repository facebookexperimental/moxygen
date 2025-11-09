/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <folly/container/F14Set.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Task.h>
#include <moxygen/MoQConsumers.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/Publisher.h>
#include <moxygen/util/BidiIterator.h>
#include <moxygen/util/FetchIntervalSet.h>
#include <limits>

namespace moxygen {

// Default cache size limits
constexpr size_t kDefaultMaxCachedTracks = 100;
constexpr size_t kDefaultMaxCachedGroupsPerTrack = 3;

class MoQCache {
 public:
  explicit MoQCache(
      size_t maxCachedTracks = kDefaultMaxCachedTracks,
      size_t maxCachedGroupsPerTrack = kDefaultMaxCachedGroupsPerTrack)
      : maxCachedTracks_(maxCachedTracks),
        maxCachedGroupsPerTrack_(maxCachedGroupsPerTrack) {}

  // Returns a filter for a subscribe that writes objects to the cache and
  // passes to the next consumer
  std::shared_ptr<TrackConsumer> getSubscribeWriteback(
      const FullTrackName& ftn,
      std::shared_ptr<TrackConsumer> consumer);

  // Serves objects from the cache to the consumer.  If objects in the range are
  // not in cache, issue one-or-more FETCH'es upstream.  Objects fetched from
  // upstream are written back to the cache and passed to the consumer.
  //
  // MoQCache internally coalesces multiple concurrent upstream requests for the
  // same object.
  folly::coro::Task<Publisher::FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer,
      std::shared_ptr<Publisher> upstream);

  void clear() {
    cache_.clear();
    trackLRU_.clear();
  }

  bool hasTrack(const FullTrackName& ftn) const {
    return cache_.find(ftn) != cache_.end();
  }

  size_t size() const {
    return cache_.size();
  }

  // Helper for testing - checks if object is cached
  bool hasCachedObject(const FullTrackName& ftn, AbsoluteLocation obj) {
    auto it = cache_.find(ftn);
    if (it == cache_.end()) {
      return false;
    }
    return getCachedObjectMaybe(*it->second, obj).hasValue();
  }

  // Setters for testing - update cache limits and evict if necessary
  void setMaxCachedTracks(size_t maxTracks) {
    maxCachedTracks_ = maxTracks;
    // Evict tracks if now over limit
    while (maxCachedTracks_ > 0 && cache_.size() > maxCachedTracks_ &&
           !trackLRU_.empty()) {
      evictOldestTrackIfNeeded();
    }
  }

  void setMaxCachedGroupsPerTrack(size_t maxGroups) {
    maxCachedGroupsPerTrack_ = maxGroups;
    // Evict groups from all tracks if now over limit
    for (auto& [ftn, track] : cache_) {
      evictOldestGroupsIfNeeded(*track);
    }
  }

  // Entry for single cached object
  struct CacheEntry {
    CacheEntry(
        uint64_t inSubgroup,
        ObjectStatus inStatus,
        Extensions inExtensions,
        Payload inPayload,
        bool inComplete)
        : subgroup(inSubgroup),
          status(inStatus),
          extensions(std::move(inExtensions)),
          payload(std::move(inPayload)),
          complete(inComplete) {}

    uint64_t subgroup{0};
    ObjectStatus status;
    Extensions extensions;
    Payload payload;
    bool complete{false};
  };

 private:
  class SubscribeWriteback;
  class SubgroupWriteback;
  class FetchWriteback;
  class FetchHandle;

  // Entry for a group
  struct CacheGroup {
    folly::F14FastMap<uint64_t, std::unique_ptr<CacheEntry>> objects;
    uint64_t maxCachedObject{0};
    bool endOfGroup{false};
    // LRU iterator - present if group is evictable (not in active fetch)
    folly::Optional<std::list<uint64_t>::iterator> lruIter_;

    folly::Expected<folly::Unit, MoQPublishError> cacheObject(
        uint64_t subgroup,
        uint64_t objectID,
        ObjectStatus status,
        const Extensions& extensions,
        Payload payload,
        bool complete);
    void cacheMissingStatus(uint64_t objectID, ObjectStatus status);
  };

  // Entry for a track
  using FetchInProgressSet = FetchIntervalSet<FetchWriteback*>;
  // Type alias for the complex BidiIterator type used in FetchWriteback
  using InProgressFetchesIter =
      BidiIterator<std::vector<FetchInProgressSet::IntervalList::iterator>>;

  struct CacheTrack {
    folly::F14FastMap<uint64_t, std::shared_ptr<CacheGroup>> groups;
    bool isLive{false};
    bool endOfTrack{false};
    folly::Optional<AbsoluteLocation> largestGroupAndObject;
    FetchInProgressSet fetchInProgress;
    // LRU iterator - present if track is evictable (not live, no active
    // fetches)
    folly::Optional<std::list<FullTrackName>::iterator> lruIter_;
    // Group LRU list for this track
    std::list<uint64_t> groupLRU;
    // Count of active fetch intervals (for O(1) canEvict check)
    size_t activeFetchCount{0};

    folly::Expected<folly::Unit, MoQPublishError> updateLargest(
        AbsoluteLocation current,
        bool endOfTrack = false);
    CacheGroup& getOrCreateGroup(uint64_t groupID, MoQCache* cache = nullptr);

    // Returns true if track can be evicted (not live, no active fetches)
    bool canEvict() const {
      return !isLive && activeFetchCount == 0;
    }
  };

  // Group-order-aware iterator for traversing groups (objects within groups
  // always ascending)
  class FetchRangeIterator {
   public:
    FetchRangeIterator(
        AbsoluteLocation start,
        AbsoluteLocation end,
        GroupOrder order,
        std::shared_ptr<CacheTrack> track);

    AbsoluteLocation end();
    void next();
    const AbsoluteLocation& operator*() const;
    const AbsoluteLocation* operator->() const;
    void invalidate();
    bool isValid() const;

    const AbsoluteLocation minLocation;
    const AbsoluteLocation maxLocation;
    const GroupOrder order;
    std::shared_ptr<CacheTrack> track;

   private:
    AbsoluteLocation current_;
    AbsoluteLocation end_;
    bool isValid_ = true;
    mutable uint64_t cachedGroupId_{std::numeric_limits<uint64_t>::max()};
    mutable std::shared_ptr<CacheGroup> cachedGroupPtr_{nullptr};
    mutable uint64_t cachedEndGroupId_{std::numeric_limits<uint64_t>::max()};
    mutable std::shared_ptr<CacheGroup> cachedEndGroupPtr_{nullptr};
    folly::Optional<uint64_t> findGroupEndMaybe(
        uint64_t groupId,
        uint64_t& cachedGroupId_,
        std::shared_ptr<CacheGroup>& cachedGroupPtr_) const;
  };

  folly::F14FastMap<
      FullTrackName,
      std::shared_ptr<CacheTrack>,
      FullTrackName::hash>
      cache_;

  // LRU list of evictable tracks (oldest at back)
  std::list<FullTrackName> trackLRU_;

  // Cache size limits
  size_t maxCachedTracks_;
  size_t maxCachedGroupsPerTrack_;

  folly::Optional<MoQCache::CacheEntry*> getCachedObjectMaybe(
      CacheTrack& track,
      AbsoluteLocation obj);

  folly::coro::Task<Publisher::FetchResult> fetchImpl(
      std::shared_ptr<FetchHandle> fetchHandle,
      Fetch fetch,
      std::shared_ptr<CacheTrack> track,
      std::shared_ptr<FetchConsumer> consumer,
      std::shared_ptr<Publisher> upstream);

  folly::coro::Task<Publisher::FetchResult> fetchUpstream(
      std::shared_ptr<MoQCache::FetchHandle> fetchHandle,
      const AbsoluteLocation& fetchStart,
      const AbsoluteLocation& fetchEnd,
      bool lastObject,
      Fetch fetch,
      std::shared_ptr<CacheTrack> track,
      std::shared_ptr<FetchConsumer> consumer,
      std::shared_ptr<Publisher> upstream);

  folly::coro::Task<folly::Expected<folly::Unit, FetchError>> handleBlocked(
      std::shared_ptr<FetchConsumer> consumer,
      const Fetch& fetch);

  // Track LRU management helpers
  void addTrackToLRU(const FullTrackName& ftn, CacheTrack& track);
  void removeTrackFromLRU(CacheTrack& track);
  void onTrackBecameEvictable(const FullTrackName& ftn);

  // Group LRU management helpers
  void addGroupToLRU(uint64_t groupID, CacheGroup& group, CacheTrack& track);
  void removeGroupFromLRU(CacheGroup& group, CacheTrack& track);
  bool canEvictGroup(uint64_t groupID, CacheTrack& track);

  // Eviction methods
  bool evictOldestTrackIfNeeded();
  void evictTrack(const FullTrackName& ftn);
  void evictOldestGroupsIfNeeded(CacheTrack& track);
  void evictGroup(CacheTrack& track, uint64_t groupID);
};

} // namespace moxygen
