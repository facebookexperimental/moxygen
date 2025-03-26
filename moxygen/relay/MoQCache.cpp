#include <moxygen/relay/MoQCache.h>

// TTL / MAX_CACHE_DURATION
// Maxmimum cache size / per track? Number of groups
// Fancy: handle streaming incomplete objects (forwarder?)

namespace {
using namespace moxygen;

bool isEndOfTrack(ObjectStatus status) {
  return status == ObjectStatus::END_OF_TRACK ||
      status == ObjectStatus::END_OF_TRACK_AND_GROUP;
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
      return folly::unit;
    case ObjectStatus::GROUP_NOT_EXIST:
      return folly::unit;
    case ObjectStatus::END_OF_GROUP:
      return consumer->endOfGroup(
          current.group,
          object.subgroup,
          current.object,
          object.extensions,
          lastObject);
    case ObjectStatus::END_OF_TRACK_AND_GROUP:
    case ObjectStatus::END_OF_TRACK:
      return consumer->endOfTrackAndGroup(
          current.group, object.subgroup, current.object, object.extensions);
  }
  return folly::makeUnexpected(
      MoQPublishError{MoQPublishError::API_ERROR, "Unknown status"});
}

} // namespace

namespace moxygen {

void MoQCache::CacheGroup::cacheObject(
    uint64_t subgroup,
    uint64_t objectID,
    ObjectStatus status,
    const Extensions& extensions,
    Payload payload,
    bool complete) {
  XLOG(DBG1) << "caching objID=" << objectID << " status=" << (uint32_t)status;
  objects[objectID] = std::make_unique<CacheEntry>(
      subgroup, status, extensions, std::move(payload), complete);
  if (objectID > maxCachedObject) {
    maxCachedObject = objectID;
    endOfGroup =
        (status == ObjectStatus::END_OF_GROUP ||
         status == ObjectStatus::GROUP_NOT_EXIST);
  }
}

void MoQCache::CacheGroup::cacheMissingStatus(
    uint64_t objectID,
    ObjectStatus status) {
  XLOG(DBG1) << "caching missing objID=" << objectID;
  static constexpr auto kInvalidSubgroup = std::numeric_limits<uint64_t>::max();
  cacheObject(
      kInvalidSubgroup, objectID, status, noExtensions(), nullptr, true);
}

class MoQCache::FetchHandle : public Publisher::FetchHandle {
 public:
  explicit FetchHandle(FetchOk ok) : Publisher::FetchHandle(std::move(ok)) {}

  void fetchCancel() override {
    // TODO
  }

  folly::CancellationToken getToken() {
    return source_.getToken();
  }

  folly::CancellationSource source_;
};

folly::Expected<folly::Unit, MoQPublishError>
MoQCache::CacheTrack::updateLatest(AbsoluteLocation current, bool eot) {
  // TODO: check for duplicate end of track?  Maybe it can happen with same id
  if (!latestGroupAndObject || current > *latestGroupAndObject) {
    if (endOfTrack) {
      XLOG(ERR) << "Malformed track, end of track set, but new largest object";
      return folly::makeUnexpected(
          MoQPublishError(MoQPublishError::API_ERROR, "Malformed track"));
    }
    latestGroupAndObject = current;
  }
  endOfTrack = eot;
  return folly::unit;
}

MoQCache::CacheGroup& MoQCache::CacheTrack::getGroup(uint64_t groupID) {
  auto it = groups.find(groupID);
  if (it == groups.end()) {
    it = groups.emplace(groupID, std::make_unique<CacheGroup>()).first;
  }
  return *it->second;
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

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objID,
      Payload payload,
      Extensions ext,
      bool finSub = false) override {
    auto res = cacheTrack_.updateLatest({group_, objID});
    if (!res) {
      return res;
    }
    auto cPayload = payload ? payload->clone() : nullptr;
    cacheGroup_.cacheObject(
        subgroup_, objID, ObjectStatus::NORMAL, ext, std::move(cPayload), true);
    return consumer_->object(objID, std::move(payload), std::move(ext), finSub);
  }

  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t objID,
      Extensions ext,
      bool finSub = false) override {
    auto res = cacheTrack_.updateLatest({group_, objID});
    if (!res) {
      return res;
    }
    cacheGroup_.cacheObject(
        subgroup_, objID, ObjectStatus::OBJECT_NOT_EXIST, ext, nullptr, true);
    return consumer_->objectNotExists(objID, std::move(ext), finSub);
  }

  void checkpoint() override {
    return consumer_->checkpoint();
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    auto res = cacheTrack_.updateLatest({group_, objectID});
    if (!res) {
      return res;
    }
    cacheGroup_.cacheObject(
        subgroup_,
        objectID,
        ObjectStatus::NORMAL,
        extensions,
        initialPayload ? initialPayload->clone() : nullptr,
        false);
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
      bool finSubgroup = false) override {
    auto& object = cacheGroup_.objects[currentObject_];
    object->payload->appendChain(payload->clone());
    currentLength_ -= payload->computeChainDataLength();
    if (currentLength_ == 0) {
      object->complete = true;
    }
    return consumer_->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID,
      Extensions extensions) override {
    auto res = cacheTrack_.updateLatest({group_, endOfGroupObjectID});
    if (!res) {
      return res;
    }
    cacheGroup_.cacheObject(
        subgroup_,
        endOfGroupObjectID,
        ObjectStatus::END_OF_GROUP,
        extensions,
        nullptr,
        true);
    return consumer_->endOfGroup(endOfGroupObjectID, std::move(extensions));
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID,
      Extensions extensions) override {
    auto res = cacheTrack_.updateLatest({group_, endOfTrackObjectID}, true);
    if (!res) {
      return res;
    }
    cacheGroup_.cacheObject(
        subgroup_,
        endOfTrackObjectID,
        ObjectStatus::END_OF_TRACK_AND_GROUP,
        extensions,
        nullptr,
        true);
    return consumer_->endOfTrackAndGroup(
        endOfTrackObjectID, std::move(extensions));
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

class MoQCache::SubscribeWriteback : public TrackConsumer {
 public:
  SubscribeWriteback(std::shared_ptr<TrackConsumer> consumer, CacheTrack& track)
      : consumer_(std::move(consumer)), track_(track) {
    track_.isLive = true;
  }

  ~SubscribeWriteback() {
    track_.isLive = false;
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
          track_.getGroup(groupID));
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
    auto res = track_.updateLatest(
        {header.group, header.id}, isEndOfTrack(header.status));
    if (!res) {
      return res;
    }
    track_.getGroup(header.group)
        .cacheObject(
            header.subgroup,
            header.id,
            header.status,
            header.extensions,
            payload ? payload->clone() : nullptr,
            true);
    return consumer_->objectStream(header, std::move(payload));
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override {
    auto res = track_.updateLatest(
        {header.group, header.id}, isEndOfTrack(header.status));
    if (!res) {
      return res;
    }
    track_.getGroup(header.group)
        .cacheObject(
            header.subgroup,
            header.id,
            header.status,
            header.extensions,
            payload ? payload->clone() : nullptr,
            true);
    return consumer_->datagram(header, std::move(payload));
  }

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroup,
      Priority pri,
      Extensions extensions) override {
    auto res = track_.updateLatest({groupID, 0});
    if (!res) {
      return res;
    }
    track_.getGroup(groupID).cacheMissingStatus(
        0, ObjectStatus::GROUP_NOT_EXIST);
    return consumer_->groupNotExists(
        groupID, subgroup, pri, std::move(extensions));
  }

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override {
    return consumer_->subscribeDone(std::move(subDone));
  }

 private:
  std::shared_ptr<TrackConsumer> consumer_;
  CacheTrack& track_;
};

class MoQCache::FetchWriteback : public FetchConsumer {
 public:
  FetchWriteback(
      AbsoluteLocation start,
      AbsoluteLocation end,
      bool proxyFin,
      std::shared_ptr<FetchConsumer> consumer,
      CacheTrack& track)
      : start_(start),
        end_(end),
        proxyFin_(proxyFin),
        consumer_(std::move(consumer)),
        track_(track) {
    inProgressIntervalIt_ = track_.fetchInProgress.insert(start_, end_, this);
  }

  ~FetchWriteback() {
    inProgress_.post();
    if (inProgressIntervalIt_) {
      track_.fetchInProgress.erase(*inProgressIntervalIt_);
    }
    cancelSource_.requestCancellation();
  }

  void updateInProgress() {
    inProgress_.post();
    if (start_ < end_) {
      inProgressIntervalIt_.value()->second.start = start_;
      inProgress_.reset();
    } else if (inProgressIntervalIt_) {
      track_.fetchInProgress.erase(*inProgressIntervalIt_);
      inProgressIntervalIt_.reset();
    }
  }

  folly::coro::Task<void> waitFor(AbsoluteLocation loc) {
    if (loc >= end_) {
      co_return;
    }
    auto token = cancelSource_.getToken();
    while (!token.isCancellationRequested() && loc >= start_) {
      co_await inProgress_;
    }
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
    return consumer_->object(
        gID, sgID, objID, std::move(payload), std::move(ext), fin && proxyFin_);
  }

  // Deliver Object Status=ObjectNotExists for the given object.
  folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
      uint64_t gID,
      uint64_t sgID,
      uint64_t objID,
      Extensions ext,
      bool fin) override {
    constexpr auto kNotExist = ObjectStatus::OBJECT_NOT_EXIST;
    auto res = cacheImpl(gID, sgID, objID, kNotExist, ext, nullptr, true, fin);
    if (!res) {
      return res;
    }
    return consumer_->objectNotExists(
        gID, sgID, objID, std::move(ext), fin && proxyFin_);
  }

  // Deliver Object Status=ObjectNotExists for the givenobject.
  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t gID,
      uint64_t sgID,
      Extensions ext,
      bool fin) override {
    constexpr auto kNotExist = ObjectStatus::GROUP_NOT_EXIST;
    auto res = cacheImpl(gID, sgID, 0, kNotExist, ext, nullptr, true, fin);
    if (!res) {
      return res;
    }
    return consumer_->groupNotExists(
        gID, sgID, std::move(ext), fin && proxyFin_);
  }

  // Advance the reliable offset of the fetch stream to the current offset.
  void checkpoint() override {
    consumer_->checkpoint();
  }

  // Begin delivering the next object in this subgroup.
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
    auto& group = track_.getGroup(start_.group);
    auto& object = group.objects[start_.object];
    object->payload->appendChain(payload->clone());
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

  // Deliver Object Status=EndOfGroup for the given object ID.
  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t gID,
      uint64_t sgID,
      uint64_t objID,
      Extensions ext,
      bool fin) override {
    constexpr auto kEndOfGroup = ObjectStatus::END_OF_GROUP;
    auto res =
        cacheImpl(gID, sgID, objID, kEndOfGroup, ext, nullptr, true, fin);
    if (!res) {
      return res;
    }
    return consumer_->endOfGroup(
        gID, sgID, objID, std::move(ext), fin && proxyFin_);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t gID,
      uint64_t sgID,
      uint64_t objID,
      Extensions ext) override {
    constexpr auto kEndOfTrack = ObjectStatus::END_OF_TRACK_AND_GROUP;
    auto res =
        cacheImpl(gID, sgID, objID, kEndOfTrack, ext, nullptr, true, true);
    if (!res) {
      return res;
    }
    return consumer_->groupNotExists(gID, sgID, std::move(ext), proxyFin_);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    cacheMissing(end_);
    updateInProgress();
    complete_.post();
    if (proxyFin_) {
      return consumer_->endOfFetch();
    }
    return folly::unit;
  }

  void reset(ResetStreamErrorCode error) override {
    consumer_->reset(error);
    wasReset_ = true;
    complete_.post();
    start_ = end_; // nothing else is coming
    updateInProgress();
  }

  // Wait for the fetch to become writable
  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitReadyToConsume() override {
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
  CacheTrack& track_;
  folly::Optional<FetchInProgresSet::IntervalMap::iterator>
      inProgressIntervalIt_;
  folly::coro::Baton inProgress_;
  folly::coro::Baton complete_;
  uint64_t currentLength_{0};
  bool wasReset_{false};
  folly::CancellationSource cancelSource_;

  void cacheMissing(AbsoluteLocation current) {
    while (start_ < current) {
      auto& group = track_.getGroup(start_.group);
      if (start_.group < current.group) {
        if (start_.object == 0) {
          track_.updateLatest({start_.group, 0});
          group.cacheMissingStatus(0, ObjectStatus::GROUP_NOT_EXIST);
        } else {
          group.endOfGroup = true;
        }
        start_.group++;
        start_.object = 0;
      } else {
        track_.updateLatest({start_.group, start_.object});
        group.cacheMissingStatus(start_.object, ObjectStatus::OBJECT_NOT_EXIST);
        start_.object++;
      }
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
    cacheMissing({groupID, objectID});
    auto& group = track_.getGroup(groupID);
    auto res = track_.updateLatest({groupID, objectID}, isEndOfTrack(status));
    if (!res) {
      updateInProgress();
      return res;
    }
    group.cacheObject(
        subgroupID, objectID, status, extensions, std::move(payload), complete);
    start_ = AbsoluteLocation{groupID, objectID + 1};
    updateInProgress();
    if (finFetch) {
      cacheMissing(end_);
      complete_.post();
    }
    return folly::unit;
  }
};

std::shared_ptr<TrackConsumer> MoQCache::getSubscribeWriteback(
    const FullTrackName& ftn,
    std::shared_ptr<TrackConsumer> consumer) {
  auto trackIt = cache_.find(ftn);
  if (trackIt == cache_.end()) {
    trackIt = cache_.emplace(ftn, CacheTrack()).first;
  }
  return std::shared_ptr<TrackConsumer>(
      new SubscribeWriteback(std::move(consumer), trackIt->second));
}

folly::coro::Task<Publisher::FetchResult> MoQCache::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer,
    std::shared_ptr<Publisher> upstream) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  CHECK(standalone);
  auto emplaceResult = cache_.emplace(fetch.fullTrackName, CacheTrack());
  auto trackIt = emplaceResult.first;
  auto& track = trackIt->second;
  if (emplaceResult.second) {
    // track is new (not cached), forward upstream, with writeback
    XLOG(DBG1) << "Cache miss, upstream fetch";
    co_return co_await upstream->fetch(
        fetch,
        std::make_shared<FetchWriteback>(
            standalone->start,
            standalone->end,
            true,
            std::move(consumer),
            track));
  }
  AbsoluteLocation last = standalone->end;
  if (last.object > 0) {
    last.object--;
  }
  if (track.latestGroupAndObject &&
      (track.isLive || last <= *track.latestGroupAndObject)) {
    // we can immediately return fetch OK
    XLOG(DBG1) << "Live track or known past data, return FetchOK";
    AbsoluteLocation largestInFetch = *track.latestGroupAndObject;
    if (standalone->end >= *track.latestGroupAndObject) {
      largestInFetch = *track.latestGroupAndObject;
      // fetchImpl range inclusive of end
      standalone->end = {largestInFetch.group, largestInFetch.object + 1};
    }
    auto fetchHandle = std::make_shared<FetchHandle>(FetchOk(
        {fetch.subscribeID,
         GroupOrder::OldestFirst,
         track.endOfTrack,
         largestInFetch,
         {}}));
    folly::coro::co_withCancellation(
        fetchHandle->getToken(),
        fetchImpl(
            std::move(fetch),
            false,
            track,
            std::move(consumer),
            std::move(upstream)))
        .scheduleOn(co_await folly::coro::co_current_executor)
        .start();
    co_return fetchHandle;
  } else {
    XLOG(DBG1) << "No objects, or end > lastest and not live, fetchImpl";
    co_return co_await fetchImpl(
        std::move(fetch),
        true,
        track,
        std::move(consumer),
        std::move(upstream));
  }
}

folly::coro::Task<Publisher::FetchResult> MoQCache::fetchImpl(
    Fetch fetch,
    bool needsFetchOk,
    CacheTrack& track,
    std::shared_ptr<FetchConsumer> consumer,
    std::shared_ptr<Publisher> upstream) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  XLOG(DBG1) << "fetchImpl for {" << standalone->start.group << ","
             << standalone->start.object << "}, {" << standalone->end.group
             << "," << standalone->end.object << "}";
  CHECK(standalone);
  folly::Optional<AbsoluteLocation> fetchStart;
  auto current = standalone->start;
  while (current < standalone->end &&
         (!track.endOfTrack || current <= *track.latestGroupAndObject)) {
    auto writeback = track.fetchInProgress.getValue(current);
    if (writeback) {
      XLOG(DBG1) << "fetchInProgress for {" << current.group << ","
                 << current.object << "}";
      co_await (*writeback)->waitFor(current);
    }
    auto groupIt = track.groups.find(current.group);
    if (groupIt == track.groups.end()) {
      // group not cached, include in range
      XLOG(DBG1) << "group cache miss for g=" << current.group;
      if (!fetchStart) {
        fetchStart = current;
      }
      current.group++;
      current.object = 0;
      continue;
    }
    auto& group = groupIt->second;
    auto objIt = group->objects.find(current.object);
    if (objIt == group->objects.end() || !objIt->second->complete) {
      // object not cached or complete, include in range
      XLOG(DBG1) << "object cache miss for {" << current.group << ","
                 << current.object << "}";
      if (!fetchStart) {
        fetchStart = current;
      }
      current.object++;
      if (current.object > group->maxCachedObject) {
        current.group++;
        current.object = 0;
      }
      continue;
    }
    // found the object, first fetch missing range, if any
    XLOG(DBG1) << "object cache HIT for {" << current.group << ","
               << current.object << "}";
    auto object = objIt->second.get();
    // TODO: once we support eviction, this object may need to be
    // shared_ptr
    if (fetchStart) {
      // Call the helper function
      auto res = co_await fetchUpstream(
          *fetchStart,
          current,
          /*lastObject=*/false,
          fetch,
          needsFetchOk,
          track,
          consumer,
          upstream);
      if (res.hasError()) {
        co_return folly::makeUnexpected(res.error());
      } else if (needsFetchOk) {
        // fetchUpstream starts another fetchImpl to complete the fetch if
        // needed
        XCHECK(res.value());
        co_return std::move(res.value());
      } // else continue
      fetchStart.reset();
    }
    XLOG(DBG1) << "Publish object from cache";

    auto next = current;
    next.object++;
    if (next.object > group->maxCachedObject && group->endOfGroup) {
      next.group++;
      next.object = 0;
    } // unless known end of group, continue current and trigger upstream
      // fetch
    auto lastObject = next >= standalone->end || isEndOfTrack(object->status);
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
        consumer->reset(ResetStreamErrorCode::INTERNAL_ERROR);
        co_return folly::makeUnexpected(FetchError{
            fetch.subscribeID,
            FetchErrorCode::INTERNAL_ERROR,
            folly::to<std::string>(
                "Consumer error on object=", res.error().msg)});
      }
    } // else publish success
    current = next;
  }
  if (fetchStart) {
    XLOG(DBG1) << "Fetching missing tail";
    auto res = co_await fetchUpstream(
        *fetchStart,
        standalone->end,
        /*lastObject=*/true,
        fetch,
        needsFetchOk,
        track,
        std::move(consumer),
        std::move(upstream));
    if (res.hasError()) {
      co_return folly::makeUnexpected(res.error());
    } else if (needsFetchOk) {
      XCHECK(res.value());
      co_return std::move(res.value());
    } else {
      co_return nullptr;
    }
  }
  if (needsFetchOk) {
    XLOG(DBG1) << "Fetch completed entirely from cache";
    // test for empty range with no latest group and object?
    co_return std::make_shared<FetchHandle>(FetchOk(
        fetch.subscribeID,
        GroupOrder::OldestFirst,
        track.endOfTrack,
        *track.latestGroupAndObject,
        {}));
  }
  co_return nullptr;
}

folly::coro::Task<Publisher::FetchResult> MoQCache::fetchUpstream(
    const AbsoluteLocation& fetchStart,
    const AbsoluteLocation& fetchEnd,
    bool lastObject,
    Fetch fetch,
    bool needsFetchOk,
    CacheTrack& track,
    std::shared_ptr<FetchConsumer> consumer,
    std::shared_ptr<Publisher> upstream) {
  XLOG(DBG1) << "Fetching upstream for {" << fetchStart.group << ","
             << fetchStart.object << "}, {" << fetchEnd.group << ","
             << fetchEnd.object << "}";
  auto writeback = std::make_shared<FetchWriteback>(
      fetchStart, fetchEnd, lastObject, consumer, track);
  auto res = co_await upstream->fetch(
      Fetch(
          0,
          fetch.fullTrackName,
          fetchStart,
          fetchEnd,
          fetch.priority,
          fetch.groupOrder),
      writeback);
  if (res.hasError()) {
    XLOG(ERR) << "upstream fetch failed err=" << res.error().reasonPhrase;
    consumer->reset(ResetStreamErrorCode::CANCELLED);
    co_return folly::makeUnexpected(FetchError{
        fetch.subscribeID, res.error().errorCode, res.error().reasonPhrase});
  } else if (needsFetchOk) {
    XLOG(DBG1) << "upstream success and needsFetchOk";
    auto fetchHandle = std::make_shared<FetchHandle>(FetchOk(
        {fetch.subscribeID,
         GroupOrder::OldestFirst,
         res.value()->fetchOk().endOfTrack,
         res.value()->fetchOk().latestGroupAndObject,
         res.value()->fetchOk().params}));
    if (!lastObject) {
      folly::coro::co_withCancellation(
          fetchHandle->getToken(),
          folly::coro::co_invoke(
              [this,
               writeback,
               current = fetchEnd,
               fetch,
               &track,
               consumer,
               upstream]() mutable -> folly::coro::Task<void> {
                XLOG(DBG1) << "Waiting for writeback complete";
                co_await writeback->complete();
                if (writeback->wasReset()) {
                  // writeback was reset, can't continue
                  XLOG(ERR) << "Fetch was reset, returning";
                  co_return;
                }
                writeback.reset();
                auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
                standalone->start = current;
                XLOG(DBG1) << "Invoking fetchImpl";
                co_await fetchImpl(
                    fetch,
                    false,
                    track,
                    std::move(consumer),
                    std::move(upstream));
              }))
          .scheduleOn(co_await folly::coro::co_current_executor)
          .start();
    }
    co_return fetchHandle;
  }
  // upstream FetchOK but not needed
  if (lastObject) {
    // don't need to wait
    co_return nullptr;
  }
  co_await writeback->complete();
  if (writeback->wasReset()) {
    // FetchOk but fetch stream was reset, can't continue
    XLOG(ERR) << "Fetch was reset, returning error";
    co_return folly::makeUnexpected(FetchError{
        fetch.subscribeID,
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
    co_return folly::makeUnexpected(FetchError{
        fetch.subscribeID,
        FetchErrorCode::INTERNAL_ERROR,
        folly::to<std::string>(
            "Consumer error awaiting ready=", awaitRes.error().msg)});
  }
  co_await std::move(awaitRes.value());
  co_return folly::unit;
}

} // namespace moxygen
