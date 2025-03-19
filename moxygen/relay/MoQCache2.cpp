bool isLastObject(StandaloneFetch* standalone, AbsoluteLocation current) {
  return (standalone->end <=>
          AbsoluteLocation{current.group, current.object}) ==
      std::strong_ordering::equivalent;
}

struct CacheLookupResult {
  CacheGroup* group;
  CacheObject* object;
  AbsoluteLocation location;
};

CacheLookupResult getCachedObject(CacheTrack& track, AbsoluteLocation current) {
  auto groupIt = track.groups.find(current.group);
  if (groupIt == track.groups.end()) {
    XLOG(DBG1) << "group cache miss for g=" << current.group;
    return {nullptr, nullptr, kLocationMax};
  }
  auto& group = groupIt->second;
  auto objIt = group->objects.find(current.object);
  if (objIt == group->objects.end() || !objIt->second->complete) {
    XLOG(DBG1) << "object cache miss for {" << current.group << ","
               << current.object << "}";
    return {&group, nullptr, kLocationMax};
  }
  return {&group, objIt->second.get(), current};
}

CacheLookupResult MoQCache::findNextCachedObject(
    CacheTrack& track,
    AbsoluteLocation current,
    AbsoluteLocation end) {
  // Start searching from the next object
  AbsoluteLocation next = current;
  next.object++;

  CacheGroup* currentGroup = nullptr;
  while (current < end &&
         (!track.endOfTrack || current < track.latestGroupAndObject)) {
    if (!currentGroup) {
      auto groupIt = track.groups.find(current.group);
      if (groupIt == track.groups.end()) {
        XLOG(DBG1) << "group cache miss for g=" << current.group;
        current.group++;
        current.object = 0;
        currentGroup = nullptr;
        continue;
      }
      currentGroup = &groupIt->second;
    }
    auto objIt = group->objects.find(current.object);
    if (objIt == group->objects.end() || !objIt->second->complete) {
      XLOG(DBG1) << "object cache miss for {" << current.group << ","
                 << current.object << "}";
      current.object++;
      if (current.object > group->maxCachedObject) {
        current.group++;
        current.object = 0;
        currentGroup = nullptr;
      }
      continue;
    }
    return {currentGroup, objIt->second.get(), current};
  }
  return {nullptr, nulptr, kLocationMax};
}

folly::Expected<folly::Unit, MoQPublishError> publishObjectFromCache(
    const std::shared_ptr<FetchConsumer>& consumer,
    AbsoluteLocation current,
    CacheEntry* object,
    bool lastObject) {
  folly::Expected<folly::Unit, MoQPublishError> res{folly::unit};
  switch (object->status) {
    case ObjectStatus::NORMAL:
      res = consumer->object(
          current.group,
          object->subgroup,
          current.object,
          object->payload->clone(),
          lastObject);
      break;
    case ObjectStatus::OBJECT_NOT_EXIST:
      res = consumer->objectNotExists(
          current.group, object->subgroup, current.object, lastObject);
      break;
    case ObjectStatus::GROUP_NOT_EXIST:
      res =
          consumer->groupNotExists(current.group, object->subgroup, lastObject);
      break;
    case ObjectStatus::END_OF_GROUP:
      res = consumer->endOfGroup(
          current.group, object->subgroup, current.object, lastObject);
      break;
    case ObjectStatus::END_OF_TRACK_AND_GROUP:
    case ObjectStatus::END_OF_TRACK:
      res = consumer->endOfTrackAndGroup(
          current.group, object->subgroup, current.object);
      break;
  }
  return res;
}

Publisher::FetchResult internalError(
    const std::shared_ptr<FetchConsumer>& consumer,
    const Fetch& fetch,
    const std::string& what,
    const std::string& detail) {
  XLOG(ERR) << what << err.msg;
  if (consumer) {
    consumer->reset(ResetStreamErrorCode::INTERNAL_ERROR);
  }
  return folly::makeUnexpected(FetchError{
      fetch.subscribeID,
      FetchErrorCode::INTERNAL_ERROR,
      folly::to<std::string>(what, detail)});
}

folly::coro::Task<Publisher::FetchResult> MoQCache::fetchImpl(
    Fetch fetch,
    bool needsFetchOk,
    CacheTrack& track,
    std::shared_ptr<FetchConsumer> consumer,
    std::shared_ptr<MoQSession> upstream,
    std::shared_ptr<FetchWriteback> writeback = nullptr) {
  auto standalone = std::get_if<StandaloneFetch>(&fetch.args);
  CHECK(standalone);
  folly::Optional<AbsoluteLocation> fetchStart;
  auto& current = standalone->start;
  CacheLookupResult lookupRes{nullptr, nullptr};
  while (current < standalone->end &&
         (!track.endOfTrack || current < track.latestGroupAndObject)) {
    if (writeback) {
      XLOG(DBG1) << "Waiting for writeback complete";
      co_await writeback->complete();
      if (writeback->wasReset()) {
        // FetchOk but fetch stream was reset, can't continue
        co_return internalError(nullptr, fetch, "Upstream fetch reset", "");
      }
      writeback.reset();
    }
    CHECK(!writeback);
    auto fetchInProgress = track.fetchInProgress.getValue(current);
    if (fetchInProgress) {
      XLOG(DBG1) << "fetchInProgress for {" << current.group << ","
                 << current.object << "}";
      co_await **fetchInProgress;
    }
    if (!lookupRes.object) {
      lookupRes = getCachedObject(track, current);
    } else {
      CHECK_EQ(lookupRes.location, current);
    }
    if (lookupRes.object) {
      auto publishRes = publishObjectFromCache(
          consumer,
          current,
          lookupRes.object,
          isLastObject(standalone, current));
      if (res.hasError()) {
        if (res.error().code == MoQPublishError::BLOCKED) {
          XLOG(DBG1) << "Fetch blocked, waiting";
          auto awaitRes = consumer->awaitReadyToConsume();
          if (!awaitRes) {
            co_return internalError(
                fetch, "Consumer error awaiting ready=", res.error().msg);
          }
          co_await std::move(awaitRes.value());
        } else {
          co_return internalError(
              consumer, fetch, "Consumer error on object=", res.error().msg);
        }
      } // else publish success
      current.object++;
      if (current.object > lookupRes.group->maxCachedObject) {
        current.group++;
        current.object = 0;
      }
      lookupRes = {nullptr, nullptr};
    } else {
      lookupRes = findNextCachedObject(track, current, standalone->end);
      AbsoluteLocation fetchEnd = standalone->end;
      if (lookupRes.object) {
        fetchEnd = lookupRes.location;
      }
      XLOG(DBG1) << "Fetching upstream for {" << fetchStart->group << ","
                 << fetchStart->object << "}, {" << fetchEnd.group << ","
                 << fetchEnd.object << "}";
      writeback = std::make_shared<FetchWriteback>(
          current, fetchEnd, lastObject, consumer, track);
      folly::coro::Baton baton;
      track.fetchInProgress.insert(current, fetchEnd, &baton);
      auto g = folly::makeGuard([&baton, current] {
        baton.post();
        track.fetchInProgress.erase(current);
      });
      auto res = co_await upstream->fetch(
          Fetch(
              0,
              fetch.fullTrackName,
              current,
              fetchEnd,
              fetch.priority,
              fetch.groupOrder),
          writeback);
      current = fetchEnd;
      if (res.hasError()) {
        // discarded res.error().errorCode
        co_return internalError(
            consumer, fetch, "upstream fetch err=", res.error().reasonPhrase);
      } else if (needsFetchOk) {
        XLOG(DBG1) << "upstream success and needsFetchOk";
        auto fetchHandle = std::make_shared<FetchHandle>(FetchOk(
            {fetch.subscribeID,
             GroupOrder::OldestFirst,
             res.value()->fetchOk().endOfTrack,
             res.value()->fetchOk().latestGroupAndObject,
             res.value()->fetchOk().params}));
        CHECK_EQ(standalone->start, current);
        folly::coro::co_withCancellation(
            fetchHandle->getToken(),
            fetchImpl(
                fetch,
                false,
                track,
                std::move(consumer),
                std::move(upstream),
                std::move(writeback)))
            .scheduleOn(co_await folly::coro::co_current_executor)
            .start();

        co_return fetchHandle;
      }
    }
  }

  // TODO: shouldn't need endOfFetch is lastObject is correct
  consumer->endOfFetch();
  XLOG(DBG1) << "Fetch completed entirely from cache";
  co_return std::make_shared<FetchHandle>(FetchOk(
      fetch.subscribeID,
      GroupOrder::OldestFirst,
      track.endOfTrack,
      track.latestGroupAndObject,
      {}));
}
