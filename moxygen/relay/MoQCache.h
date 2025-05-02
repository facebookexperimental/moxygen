#pragma once

#include <folly/container/F14Set.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Task.h>
#include <moxygen/MoQConsumers.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/Publisher.h>
#include <moxygen/util/FetchIntervalSet.h>

namespace moxygen {

class MoQCache {
 public:
  std::shared_ptr<TrackConsumer> getSubscribeWriteback(
      const FullTrackName& ftn,
      std::shared_ptr<TrackConsumer> consumer);

  folly::coro::Task<Publisher::FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer,
      std::shared_ptr<Publisher> upstream);

  void clear() {
    cache_.clear();
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
  using FetchInProgresSet = FetchIntervalSet<AbsoluteLocation, FetchWriteback*>;
  struct CacheTrack {
    folly::F14FastMap<uint64_t, std::unique_ptr<CacheGroup>> groups;
    bool isLive{false};
    bool endOfTrack{false};
    folly::Optional<AbsoluteLocation> latestGroupAndObject;
    FetchInProgresSet fetchInProgress;

    folly::Expected<folly::Unit, MoQPublishError> updateLatest(
        AbsoluteLocation current,
        bool endOfTrack = false);
    CacheGroup& getGroup(uint64_t groupID);
  };

  folly::F14FastMap<FullTrackName, CacheTrack, FullTrackName::hash> cache_;

  folly::coro::Task<Publisher::FetchResult> fetchImpl(
      std::shared_ptr<FetchHandle> fetchHandle,
      Fetch fetch,
      CacheTrack& track,
      std::shared_ptr<FetchConsumer> consumer,
      std::shared_ptr<Publisher> upstream);

  folly::coro::Task<Publisher::FetchResult> fetchUpstream(
      std::shared_ptr<MoQCache::FetchHandle> fetchHandle,
      const AbsoluteLocation& fetchStart,
      const AbsoluteLocation& fetchEnd,
      bool lastObject,
      Fetch fetch,
      CacheTrack& track,
      std::shared_ptr<FetchConsumer> consumer,
      std::shared_ptr<Publisher> upstream);

  folly::coro::Task<folly::Expected<folly::Unit, FetchError>> handleBlocked(
      std::shared_ptr<FetchConsumer> consumer,
      const Fetch& fetch);
};

} // namespace moxygen
