/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <moxygen/relay/MoQCache.h>
#include <moxygen/util/BidiIterator.h>

#include <folly/logging/xlog.h>

// TTL / MAX_CACHE_DURATION
// Maxmimum cache size / per track? Number of groups
// Fancy: handle streaming incomplete objects (forwarder?)

namespace {
using namespace moxygen;

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
          missLocation, AbsoluteLocation{missLocation.group + 1, 0});
    }
    return intervals;
  }

  // Since this is the desc case
  // We will start finding the last interval first
  // and then evaulate intermediate if any
  // and then find the start interval
  AbsoluteLocation intervalEnd = missLocation;
  // For missLocation, end is either full group or where fetch ends
  intervalEnd = {missLocation.group + 1, 0};
  if (missLocation.group == end.group) {
    // If we are in the end group, fetch till end and not whole group
    intervalEnd = end;
  }
  intervals.emplace_back(missLocation, intervalEnd);

  uint64_t numIntermediateGroups = missLocation.group - hitLocation.group - 1;
  if (numIntermediateGroups > 0) {
    if (missLocation.object == 0) {
      // Coalesce with existing intervals
      intervals.back().first.group = missLocation.group - numIntermediateGroups;
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
    intervals.emplace_back(start, hitLocation);
  }

  return intervals;
}

bool isEndOfTrack(ObjectStatus status) {
  return status == ObjectStatus::END_OF_TRACK;
}

bool exists(ObjectStatus status) {
  return status != ObjectStatus::OBJECT_NOT_EXIST &&
      status != ObjectStatus::GROUP_NOT_EXIST;
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
          lastObject);
    // These are implicit
    case ObjectStatus::OBJECT_NOT_EXIST:
    case ObjectStatus::GROUP_NOT_EXIST:
      if (lastObject) {
        consumer->endOfFetch();
      }
      return folly::unit;
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
    uint64_t subgroup,
    uint64_t objectID,
    ObjectStatus status,
    const Extensions& extensions,
    Payload payload,
    bool complete) {
  XLOG(DBG1) << "caching objID=" << objectID << " status=" << (uint32_t)status
             << " complete=" << uint32_t(complete);
  auto it = objects.find(objectID);
  if (it != objects.end()) {
    auto& cachedObject = it->second;
    if (status != cachedObject->status &&
        status != ObjectStatus::OBJECT_NOT_EXIST) {
      XLOG(ERR) << "Invalid cache status change; objID=" << objectID
                << " status=" << (uint32_t)status
                << " already exists with different status";
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Invalid status change"));
    }
    if (status == ObjectStatus::NORMAL && cachedObject->complete &&
        ((!payload && cachedObject->payload) ||
         (payload && !cachedObject->payload) ||
         (payload && cachedObject->payload &&
          payload->computeChainDataLength() !=
              cachedObject->payload->computeChainDataLength()))) {
      XLOG(ERR) << "Payload mismatch; objID=" << objectID;
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "payload mismatch"));
    }

    cachedObject->status = status;
    cachedObject->extensions = extensions;
    cachedObject->payload = std::move(payload);
    cachedObject->complete = complete;
  } else {
    objects[objectID] = std::make_unique<CacheEntry>(
        subgroup, status, extensions, std::move(payload), complete);
  }
  if (objectID >= maxCachedObject) {
    maxCachedObject = objectID;
    endOfGroup =
        (status == ObjectStatus::END_OF_GROUP ||
         status == ObjectStatus::GROUP_NOT_EXIST);
  }
  return folly::unit;
}

void MoQCache::CacheGroup::cacheMissingStatus(
    uint64_t objectID,
    ObjectStatus status) {
  XLOG(DBG1) << "caching missing objID=" << objectID;
  static constexpr auto kInvalidSubgroup = std::numeric_limits<uint64_t>::max();
  // can't have status or payload mismatch
  cacheObject(
      kInvalidSubgroup, objectID, status, noExtensions(), nullptr, true);
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

folly::Expected<folly::Unit, MoQPublishError>
MoQCache::CacheTrack::processGapExtensions(
    uint64_t groupID,
    uint64_t objectID,
    const Extensions& extensions) {
  // Process Prior Group ID Gap extension (0x3C)
  auto priorGroupGap =
      extensions.getIntExtension(kPriorGroupIdGapExtensionType);
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

      // Cache groups in the gap as GROUP_NOT_EXIST
      // If groupID is G and gap is N, groups G-N to G-1 don't exist
      for (uint64_t g = groupID - gap; g < groupID; ++g) {
        auto& group = getOrCreateGroup(g);
        // Allow if already marked as GROUP_NOT_EXIST (single object 0)
        if (!group.objects.empty() &&
            (group.objects.size() != 1 || group.objects.begin()->first != 0 ||
             group.objects.begin()->second->status !=
                 ObjectStatus::GROUP_NOT_EXIST)) {
          XLOG(ERR) << "Prior Group ID Gap covers existing object in group "
                    << g;
          return folly::makeUnexpected(MoQPublishError(
              MoQPublishError::MALFORMED_TRACK,
              "Prior Group ID Gap covers existing object"));
        }
        group.cacheMissingStatus(0, ObjectStatus::GROUP_NOT_EXIST);
      }
    }
  }

  // Process Prior Object ID Gap extension (0x3E)
  // Unlike Prior Group ID Gap, different values across objects in a group are
  // valid (each object can independently indicate which prior objects don't
  // exist). We allow redundant marking of already-not-existing objects.
  auto priorObjectGap =
      extensions.getIntExtension(kPriorObjectIdGapExtensionType);
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

    // Cache objects in the gap as OBJECT_NOT_EXIST
    // If objectID is O and gap is N, objects O-N to O-1 don't exist
    auto& group = getOrCreateGroup(groupID);
    for (uint64_t o = objectID - gap; o < objectID; ++o) {
      auto it = group.objects.find(o);
      if (it != group.objects.end()) {
        // Object already exists - only ok if it's already OBJECT_NOT_EXIST
        if (it->second->status != ObjectStatus::OBJECT_NOT_EXIST) {
          XLOG(ERR) << "Prior Object ID Gap covers existing object " << o
                    << " in group " << groupID;
          return folly::makeUnexpected(MoQPublishError(
              MoQPublishError::MALFORMED_TRACK,
              "Prior Object ID Gap covers existing object"));
        }
        // Already marked as not existing, skip
        continue;
      }
      group.cacheMissingStatus(o, ObjectStatus::OBJECT_NOT_EXIST);
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
      CacheTrack& cacheTrack,
      CacheGroup& cacheGroup)
      : group_(group),
        subgroup_(subgroup),
        consumer_(std::move(consumer)),
        cacheTrack_(cacheTrack),
        cacheGroup_(cacheGroup) {}
  SubgroupWriteback() = delete;
  SubgroupWriteback(const SubgroupWriteback&) = delete;
  SubgroupWriteback& operator=(const SubgroupWriteback&) = delete;
  SubgroupWriteback(SubgroupWriteback&&) = delete;
  SubgroupWriteback& operator=(SubgroupWriteback&&) = delete;

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objID,
      Payload payload,
      Extensions ext,
      bool finSub) override {
    auto res = cacheTrack_.updateLargest({group_, objID});
    if (!res) {
      return res;
    }
    auto cPayload = payload ? payload->clone() : nullptr;
    auto cacheRes = cacheGroup_.cacheObject(
        subgroup_, objID, ObjectStatus::NORMAL, ext, std::move(cPayload), true);
    if (cacheRes.hasError()) {
      return cacheRes;
    }
    auto gapRes = cacheTrack_.processGapExtensions(group_, objID, ext);
    if (gapRes.hasError()) {
      return gapRes;
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
    auto res = cacheTrack_.updateLargest({group_, objectID});
    if (!res) {
      return res;
    }
    auto cacheRes = cacheGroup_.cacheObject(
        subgroup_,
        objectID,
        ObjectStatus::NORMAL,
        extensions,
        initialPayload ? initialPayload->clone() : nullptr,
        false);
    if (cacheRes.hasError()) {
      return cacheRes;
    }
    auto gapRes =
        cacheTrack_.processGapExtensions(group_, objectID, extensions);
    if (gapRes.hasError()) {
      return gapRes;
    }
    currentObject_ = objectID;
    currentLength_ = length;
    if (initialPayload) {
      currentLength_ -= initialPayload->computeChainDataLength();
    }
    return consumer_->beginObject(
        objectID, length, std::move(initialPayload), std::move(extensions));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    auto& object = cacheGroup_.objects[currentObject_];
    object->payload->appendChain(payload->clone());
    currentLength_ -= payload->computeChainDataLength();
    if (currentLength_ == 0) {
      object->complete = true;
    }
    return consumer_->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) override {
    auto res = cacheTrack_.updateLargest({group_, endOfGroupObjectID});
    if (!res) {
      return res;
    }
    auto cacheRes = cacheGroup_.cacheObject(
        subgroup_,
        endOfGroupObjectID,
        ObjectStatus::END_OF_GROUP,
        noExtensions(),
        nullptr,
        true);
    if (cacheRes.hasError()) {
      return cacheRes;
    }
    return consumer_->endOfGroup(endOfGroupObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    auto res = cacheTrack_.updateLargest({group_, endOfTrackObjectID}, true);
    if (!res) {
      return res;
    }
    auto cacheRes = cacheGroup_.cacheObject(
        subgroup_,
        endOfTrackObjectID,
        ObjectStatus::END_OF_TRACK,
        noExtensions(),
        nullptr,
        true);
    if (cacheRes.hasError()) {
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
  CacheTrack& cacheTrack_;
  CacheGroup& cacheGroup_;
  uint64_t currentObject_{0};
  uint64_t currentLength_{0};
};

// Caches incoming objects from a subscription and forwards to the consumer.
// Also maintains the "live" bit for tracks in the cache.
class MoQCache::SubscribeWriteback : public TrackConsumer {
 public:
  SubscribeWriteback(std::shared_ptr<TrackConsumer> consumer, CacheTrack& track)
      : consumer_(std::move(consumer)), track_(track) {
    track_.isLive = true;
  }
  SubscribeWriteback() = delete;
  SubscribeWriteback(const SubscribeWriteback&) = delete;
  SubscribeWriteback& operator=(const SubscribeWriteback&) = delete;
  SubscribeWriteback(SubscribeWriteback&&) = delete;
  SubscribeWriteback& operator=(SubscribeWriteback&&) = delete;

  ~SubscribeWriteback() override {
    track_.isLive = false;
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override {
    return consumer_->setTrackAlias(std::move(alias));
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override {
    auto res = consumer_->beginSubgroup(groupID, subgroupID, priority);
    if (res.hasValue()) {
      return std::make_shared<SubgroupWriteback>(
          groupID,
          subgroupID,
          std::move(res.value()),
          track_,
          track_.getOrCreateGroup(groupID));
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
      Payload payload) override {
    auto res = track_.updateLargest(
        {header.group, header.id}, isEndOfTrack(header.status));
    if (!res) {
      return res;
    }
    auto cacheRes = track_.getOrCreateGroup(header.group)
                        .cacheObject(
                            header.subgroup,
                            header.id,
                            header.status,
                            header.extensions,
                            payload ? payload->clone() : nullptr,
                            true);
    if (cacheRes.hasError()) {
      return cacheRes;
    }
    auto gapRes =
        track_.processGapExtensions(header.group, header.id, header.extensions);
    if (gapRes.hasError()) {
      return gapRes;
    }
    return consumer_->objectStream(header, std::move(payload));
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override {
    auto res = track_.updateLargest(
        {header.group, header.id}, isEndOfTrack(header.status));
    if (!res) {
      return res;
    }
    auto cacheRes = track_.getOrCreateGroup(header.group)
                        .cacheObject(
                            header.subgroup,
                            header.id,
                            header.status,
                            header.extensions,
                            payload ? payload->clone() : nullptr,
                            true);
    if (cacheRes.hasError()) {
      return cacheRes;
    }
    auto gapRes =
        track_.processGapExtensions(header.group, header.id, header.extensions);
    if (gapRes.hasError()) {
      return gapRes;
    }
    return consumer_->datagram(header, std::move(payload));
  }

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override {
    return consumer_->publishDone(std::move(pubDone));
  }

 private:
  std::shared_ptr<TrackConsumer> consumer_;
  CacheTrack& track_;
};

// Caches incoming objects and forwards them to the consumer. Handles gaps in
// the range by caching missing object status.
class MoQCache::FetchWriteback : public FetchConsumer {
 public:
  FetchWriteback(
      AbsoluteLocation start,
      AbsoluteLocation end,
      bool proxyFin,
      std::shared_ptr<FetchConsumer> consumer,
      FetchRangeIterator fetchRangeIt)
      : start_(start),
        end_(end),
        proxyFin_(proxyFin),
        consumer_(std::move(consumer)),
        fetchRangeIt_(std::move(fetchRangeIt)) {
    // Handle start group
    auto setIt = fetchRangeIt_.track->fetchInProgress.insert(
        start,
        (start.group == end.group)
            ? end
            : AbsoluteLocation{start.group, std::numeric_limits<uint64_t>::max()},
        this);
    inProgressItersList_.push_back(setIt);
    // Handle middle groups (if any)
    for (auto currGroup = start.group + 1; currGroup < end.group; ++currGroup) {
      setIt = fetchRangeIt_.track->fetchInProgress.insert(
          AbsoluteLocation{currGroup, 0},
          AbsoluteLocation{currGroup, std::numeric_limits<uint64_t>::max()},
          this);
      inProgressItersList_.push_back(setIt);
    }
    // Handle end group (if different from start)
    if (end.group != start.group) {
      setIt = fetchRangeIt_.track->fetchInProgress.insert(
          AbsoluteLocation{end.group, 0}, end, this);
      inProgressItersList_.push_back(setIt);
    }

    // Set the iterator to first or last element depending on order
    bool forward = (fetchRangeIt_.order != GroupOrder::NewestFirst);
    dualIter_ = MoQCache::InProgressFetchesIter(inProgressItersList_, forward);
  }

  ~FetchWriteback() override {
    XLOG(DBG1) << "FetchWriteback destructing";
    inProgress_.post();
    if (!inProgressItersList_.empty()) {
      while (dualIter_ != dualIter_.end()) {
        auto it = *dualIter_;
        fetchRangeIt_.track->fetchInProgress.erase(it->start.group, it);
        ++dualIter_;
      }
      inProgressItersList_.clear();
    }
    cancelSource_.requestCancellation();
  }

  void updateInProgress() {
    inProgress_.post();
    // Update in progress within the group
    if (dualIter_ == dualIter_.end()) {
      return;
    }
    // iterators in dualIter_ are group scoped
    if (start_.group == (*dualIter_)->start.group &&
        start_ < (*dualIter_)->end) {
      // Update the start_ value of this interval
      (*dualIter_)->start = start_;
      inProgress_.reset();
    } else {
      // Remove the iterator from track level tracking
      fetchRangeIt_.track->fetchInProgress.erase(
          (*dualIter_)->start.group, (*dualIter_));
      ++dualIter_;

      // Iterator has processed the last element
      if (dualIter_ != dualIter_.end()) {
        // Update the start_ value of this interval
        (*dualIter_)->start = *fetchRangeIt_;
        inProgress_.reset();
      }
    }
  }

  folly::coro::Task<void> waitFor(AbsoluteLocation loc) {
    if (loc >= fetchRangeIt_.maxLocation) {
      co_return;
    }
    auto token = cancelSource_.getToken();
    while (!token.isCancellationRequested() && loc >= *fetchRangeIt_) {
      co_await inProgress_;
    }
  }

  void noObjects() {
    cacheMissing(end_);
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t gID,
      uint64_t sgID,
      uint64_t objID,
      Payload payload,
      Extensions ext,
      bool fin) override {
    constexpr auto kNormal = ObjectStatus::NORMAL;
    auto res =
        cacheImpl(gID, sgID, objID, kNormal, ext, payload->clone(), true, fin);
    if (!res) {
      return res;
    }
    auto gapRes = fetchRangeIt_.track->processGapExtensions(gID, objID, ext);
    if (gapRes.hasError()) {
      return gapRes;
    }
    XLOG(DBG1) << "forward object " << AbsoluteLocation(gID, objID);
    return consumer_->object(
        gID, sgID, objID, std::move(payload), std::move(ext), fin && proxyFin_);
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
    constexpr auto kNormal = ObjectStatus::NORMAL;
    auto payload = initPayload ? initPayload->clone() : nullptr;
    auto res = cacheImpl(
        gID, sgID, objID, kNormal, ext, std::move(payload), false, false);
    if (!res) {
      return res;
    }
    auto gapRes = fetchRangeIt_.track->processGapExtensions(gID, objID, ext);
    if (gapRes.hasError()) {
      return gapRes;
    }
    currentLength_ = len;
    if (initPayload) {
      currentLength_ -= initPayload->computeChainDataLength();
    }
    return consumer_->beginObject(
        gID, sgID, objID, len, std::move(initPayload), std::move(ext));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finFetch) override {
    auto& group = fetchRangeIt_.track->getOrCreateGroup(fetchRangeIt_->group);
    auto& object = group.objects[fetchRangeIt_->object];
    if (object->payload) {
      object->payload->appendChain(payload->clone());
    } else {
      object->payload = payload->clone();
    }
    currentLength_ -= payload->computeChainDataLength();
    if (currentLength_ == 0) {
      object->complete = true;
    }
    if (finFetch) {
      cacheMissing(end_);
      updateInProgress();
    }
    return consumer_->objectPayload(std::move(payload), finFetch && proxyFin_);
  }

  folly::Expected<folly::Unit, MoQPublishError>
  endOfGroup(uint64_t gID, uint64_t sgID, uint64_t objID, bool fin) override {
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
    constexpr auto kEndOfTrack = ObjectStatus::END_OF_TRACK;
    auto res = cacheImpl(
        gID, sgID, objID, kEndOfTrack, noExtensions(), nullptr, true, true);
    if (!res) {
      return res;
    }
    return consumer_->endOfTrackAndGroup(gID, sgID, objID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    cacheMissing(fetchRangeIt_.end());
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

  folly::coro::Task<void> complete() {
    co_await complete_;
  }

  bool wasReset() const {
    return wasReset_;
  }

 private:
  AbsoluteLocation start_;
  AbsoluteLocation end_;
  bool proxyFin_{false};
  std::shared_ptr<FetchConsumer> consumer_;
  std::vector<FetchInProgressSet::IntervalList::iterator> inProgressItersList_;
  MoQCache::InProgressFetchesIter dualIter_;
  folly::coro::Baton inProgress_;
  folly::coro::Baton complete_;
  uint64_t currentLength_{0};
  bool wasReset_{false};
  folly::CancellationSource cancelSource_;
  FetchRangeIterator fetchRangeIt_;

  void cacheMissing(AbsoluteLocation current) {
    while (*fetchRangeIt_ != current) {
      auto& group = fetchRangeIt_.track->getOrCreateGroup(fetchRangeIt_->group);
      if (fetchRangeIt_->group != current.group) {
        if (fetchRangeIt_->object == 0) {
          fetchRangeIt_.track->updateLargest({fetchRangeIt_->group, 0});
          group.cacheMissingStatus(0, ObjectStatus::GROUP_NOT_EXIST);
        } else {
          group.endOfGroup = true;
        }
      } else {
        fetchRangeIt_.track->updateLargest(
            {fetchRangeIt_->group, fetchRangeIt_->object});
        group.cacheMissingStatus(
            fetchRangeIt_->object, ObjectStatus::OBJECT_NOT_EXIST);
      }
      fetchRangeIt_.next();
    }
  }

  folly::Expected<folly::Unit, MoQPublishError> cacheImpl(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      ObjectStatus status,
      const Extensions& extensions,
      Payload payload,
      bool complete,
      bool finFetch) {
    auto& group = fetchRangeIt_.track->getOrCreateGroup(groupID);
    auto cacheRes = group.cacheObject(
        subgroupID, objectID, status, extensions, std::move(payload), complete);
    cacheMissing({groupID, objectID});
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
        cacheMissing(end_);
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
    trackIt = cache_.emplace(ftn, std::make_shared<CacheTrack>()).first;
  }
  return std::make_shared<SubscribeWriteback>(
      std::move(consumer), *trackIt->second);
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
            fetchRangeIt));
  }
  AbsoluteLocation last = standalone->end;
  if (last.object > 0) {
    last.object--;
  } else {
    // if end is 1,0, that means all of group 1
    last.object = std::numeric_limits<uint64_t>::max();
    standalone->end.group++;
    // TODO: handle case where track.largestGroupAndObject is an END_OF_GROUP
    // or END_OF_TRACK
  }
  if (track->largestGroupAndObject &&
      (track->isLive || last <= *track->largestGroupAndObject)) {
    // we can immediately return fetch OK
    XLOG(DBG1) << "Live track or known past data, return FetchOK";
    AbsoluteLocation largestInFetch = standalone->end;
    bool isEndOfTrack = false;
    if (standalone->end > *track->largestGroupAndObject) {
      standalone->end = *track->largestGroupAndObject;
      standalone->end.object++;
      largestInFetch = standalone->end;
      isEndOfTrack = track->endOfTrack;
      // fetchImpl range exclusive of end
    } else if (largestInFetch.object == 0) {
      largestInFetch.group--;
    }
    auto fetchHandle = std::make_shared<FetchHandle>(FetchOk{
        fetch.requestID, fetch.groupOrder, isEndOfTrack, largestInFetch});
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
  FetchRangeIterator fetchRangeIt(
      standalone->start, standalone->end, fetch.groupOrder, track);
  while (!token.isCancellationRequested() &&
         (*fetchRangeIt) != fetchRangeIt.end()) {
    auto current = *fetchRangeIt;
    auto maybeBlockingInterval =
        track->fetchInProgress.getValue(current.group, current);
    if (maybeBlockingInterval) {
      // Extract the value field from the blocking interval
      auto& writeback = maybeBlockingInterval.value()->value;
      XLOG(DBG1) << "fetchInProgress for {" << current.group << ","
                 << current.object << "}";
      co_await writeback->waitFor(current);
    }

    // Gets cached object if cache hit, else none.
    auto cachedObjectMaybe = getCachedObjectMaybe(*track, current);
    if (!cachedObjectMaybe) {
      if (!fetchStart) {
        fetchStart = current;
      }
      fetchRangeIt.next();
      continue;
    }

    // found the object, first fetch missing range, if any
    XLOG(DBG1) << "object cache HIT for {" << current.group << ","
               << current.object << "}";
    // TODO: once we support eviction, this object may need to be
    // shared_ptr
    if (fetchStart) {
      auto intervals = getFetchIntervals(
          fetchRangeIt.minLocation,
          fetchRangeIt.maxLocation,
          fetchStart.value(),
          current,
          fetchRangeIt.order,
          false);
      for (auto& interval : intervals) {
        // Call the helper function
        auto res = co_await fetchUpstream(
            fetchHandle,
            interval.first,
            interval.second,
            /*lastObject=*/false,
            fetch,
            track,
            consumer,
            upstream);
        if (res.hasError()) {
          co_return folly::makeUnexpected(res.error());
        } // else success but only returns FetchOk on lastObject
      }
      fetchStart.reset();
    }
    XLOG(DBG1) << "Publish object from cache";
    auto object = cachedObjectMaybe.value();
    fetchRangeIt.next();
    lastObject = (*fetchRangeIt) == fetchRangeIt.end();
    auto res =
        publishObject(object->status, consumer, current, *object, lastObject);
    if (res.hasError()) {
      if (res.error().code == MoQPublishError::BLOCKED) {
        XLOG(DBG1) << "Fetch blocked, waiting";
        auto blockedRes = co_await handleBlocked(consumer, fetch);
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
    servedOneObject |= exists(object->status);
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
            fetch.requestID, fetch.groupOrder, isEndOfTrack, standalone->end});
        fetchHandle->setUpstreamFetchHandle(res.value());
        co_return fetchHandle;
      }
    } else {
      co_return nullptr;
    }
  }
  if (!fetchHandle) {
    XLOG(DBG1) << "Fetch completed entirely from cache";
    // test for empty range with no largest group and object?
    if (servedOneObject) {
      if (standalone->end.object == 0) {
        standalone->end.group--;
      }
      bool endOfTrack = false;
      if (track->endOfTrack &&
          standalone->end >= *track->largestGroupAndObject) {
        endOfTrack = true;
        standalone->end = *track->largestGroupAndObject;
      }
      co_return std::make_shared<FetchHandle>(FetchOk{
          fetch.requestID, fetch.groupOrder, endOfTrack, standalone->end});
    } else {
      consumer->endOfFetch();
      co_return std::make_shared<FetchHandle>(
          FetchOk{fetch.requestID, fetch.groupOrder, false, standalone->end});
    }
  }
  co_return nullptr;
}

std::optional<MoQCache::CacheEntry*> MoQCache::getCachedObjectMaybe(
    CacheTrack& track,
    AbsoluteLocation current) {
  auto groupIt = track.groups.find(current.group);
  // Group missing from cache, advance.
  if (groupIt == track.groups.end()) {
    // object not cached or incomplete, count as miss.
    XLOG(DBG1) << "group cache miss for {" << current.group << "}";
    return std::nullopt;
  }

  auto& group = groupIt->second;
  auto objIt = group->objects.find(current.object);
  if (objIt == group->objects.end() || !objIt->second->complete) {
    // object not cached or incomplete, count as miss.
    XLOG(DBG1) << "object cache miss for {" << current.group << ","
               << current.object << "}";
    return std::nullopt;
  }
  return std::make_optional(objIt->second.get());
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
    adjFetchEnd.group--;
  }
  FetchRangeIterator fetchRangeIt(
      fetchStart, fetchEnd, fetch.groupOrder, track);
  auto writeback = std::make_shared<FetchWriteback>(
      fetchStart, adjFetchEnd, lastObject, consumer, fetchRangeIt);
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
    if (start.group == end.group) {
      return;
    }

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
    if (groupIt == track->groups.end()) {
      // No group found, so we set it to max for now
      // Track update at callback will signal endOfGroup
      end_.object = std::numeric_limits<uint64_t>::max();
      return;
    }

    auto& group = groupIt->second;
    end_.object = std::numeric_limits<uint64_t>::max();
    if (group->endOfGroup &&
        group->maxCachedObject < std::numeric_limits<uint64_t>::max()) {
      end_.object = group->maxCachedObject + 1;
    }
  }
}

void MoQCache::FetchRangeIterator::invalidate() {
  current_ = maxLocation;
  isValid_ = false;
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
  if (current_ == end_) {
    isValid_ = false;
    return;
  }

  auto groupEndMaybe =
      findGroupEndMaybe(current_.group, cachedGroupId_, cachedGroupPtr_);
  if (groupEndMaybe && current_.object < groupEndMaybe.value()) {
    // Found group end, we can increment object till that
    current_.object++;
  } else {
    if (order == GroupOrder::NewestFirst) {
      // desc
      if (current_.group == 0) {
        XLOG(ERR) << "GroupID underflow in FetchRangeIter: Current groupID "
                  << current_.group << " CurrentObjID=" << current_.object;
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
        XLOG(ERR)
            << "GroupID beyond maxLocation in FetchRangeIter: Current groupID "
            << current_.group << " CurrentObjID=" << current_.object;
        isValid_ = false;
        return;
      }
      current_.group++;
      current_.object = 0;
    }
  }
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
      (maxObjectInGroup < std::numeric_limits<uint64_t>::max())
      ? maxObjectInGroup + 1
      : maxObjectInGroup;
  const uint64_t firstUncachedObject =
      (endIncludingMaxCached < std::numeric_limits<uint64_t>::max())
      ? endIncludingMaxCached + 1
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
} // namespace moxygen
