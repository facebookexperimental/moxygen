/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moqtest/MoQTestPublisher.h"
#include <folly/ScopeGuard.h>
#include <folly/coro/Sleep.h>
#include <folly/logging/xlog.h>
#include <vector>
#include "moxygen/moqtest/Utils.h"

namespace moxygen {

const std::string kDefaultPublishDoneReason = "Testing";

folly::coro::Task<MoQTestFetchHandle::RequestUpdateResult>
MoQTestFetchHandle::requestUpdate(RequestUpdate update) {
  XLOG(INFO) << "Received Request Update for Fetch";
  co_return folly::makeUnexpected(
      RequestError{
          update.requestID,
          RequestErrorCode::NOT_SUPPORTED,
          "Request update not implemented"});
}

void MoQTestFetchHandle::fetchCancel() {
  cancelSource_->requestCancellation();
}

folly::coro::Task<void> MoQTestPublisher::delay(uint64_t ms) {
  co_await folly::coro::sleep(std::chrono::milliseconds(ms), &timekeeper_);
}

void MoQTestPublisher::cancelAll() {
  // Move before cancelling: a generator that unwinds inline retires its own
  // entry.
  auto tracks = std::move(tracks_);
  tracks_.clear();
  for (auto& [ftn, state] : tracks) {
    state.cancelSource.requestCancellation();
  }
  // Move before cancelling: cancel() can resume a waiting publish inline, and
  // that coroutine erases its own entry on the way out.
  auto pending = std::move(pendingUnpauses_);
  pendingUnpauses_.clear();
  for (auto& p : pending) {
    p->cancel();
  }
  auto fetches = std::move(activeFetches_);
  activeFetches_.clear();
  for (auto& f : fetches) {
    f->requestCancellation();
  }
}

std::shared_ptr<MoQForwarder> MoQTestPublisher::makeForwarder(
    const FullTrackName& ftn,
    MoQSession& session) {
  auto forwarder = std::make_shared<MoQForwarder>(ftn);

  // Advertise the priority even-numbered groups are published at, so the
  // draft-15+ subgroup and datagram encodings elide it for those and write it
  // explicitly for the odd ones.  The framer downgrades the extension to a
  // PUBLISHER_PRIORITY param for draft 15; earlier drafts have no way to carry
  // it, so the priority always stays on the wire instead.
  auto version = session.getNegotiatedVersion();
  if (version && getDraftMajorVersion(*version) >= 15) {
    Extensions trackProperties;
    trackProperties.insertMutableExtension(
        Extension{kPublisherPriorityExtensionType, kMoQTestPublisherPriority});
    forwarder->setExtensions(std::move(trackProperties));
  }

  auto cb = std::make_shared<TrackCallback>();
  cb->publisher = shared_from_this();
  cb->ftn = ftn;
  forwarder->setCallback(std::move(cb));
  return forwarder;
}

void MoQTestPublisher::retireTrack(
    const FullTrackName& ftn,
    const MoQForwarder* forwarder) {
  auto it = tracks_.find(ftn);
  // A track that ended and restarted has a different forwarder under the same
  // name.
  if (it == tracks_.end() || it->second.forwarder.get() != forwarder) {
    return;
  }
  it->second.cancelSource.requestCancellation();
  tracks_.erase(it);
}

folly::coro::Task<MoQSession::SubscribeResult> MoQTestPublisher::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  XLOG(INFO) << "Recieved Subscription";

  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &sub.fullTrackName.trackNamespace);
  if (res.hasError()) {
    SubscribeError error;
    error.requestID = sub.requestID;
    error.errorCode = SubscribeErrorCode::NOT_SUPPORTED;
    error.reasonPhrase = "Invalid Parameters";
    co_return folly::makeUnexpected(error);
  }

  auto session = MoQSession::getRequestSession();
  auto trackIt = tracks_.find(sub.fullTrackName);
  const bool isNewTrack = (trackIt == tracks_.end());
  auto forwarder = isNewTrack ? makeForwarder(sub.fullTrackName, *session)
                              : trackIt->second.forwarder;

  auto subscriber = forwarder->addSubscriber(session, sub, std::move(callback));
  if (!subscriber) {
    co_return folly::makeUnexpected(
        SubscribeError{
            sub.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "failed to add subscriber"});
  }
  if (!isNewTrack) {
    co_return subscriber;
  }

  // Register only once the first subscriber is attached, so a failed subscribe
  // can't leave an entry with no generator behind it.
  auto* executor = co_await folly::coro::co_current_executor;
  auto& state = tracks_[sub.fullTrackName];
  state.forwarder = forwarder;
  co_withCancellation(
      state.cancelSource.getToken(),
      co_withExecutor(
          executor,
          runTrack(
              sub.fullTrackName,
              std::move(forwarder),
              res.value(),
              sub.requestID)))
      .start();

  co_return subscriber;
}

folly::coro::Task<void> MoQTestPublisher::runTrack(
    FullTrackName ftn,
    std::shared_ptr<MoQForwarder> forwarder,
    MoQTestParameters params,
    RequestID requestID) {
  auto unregister =
      folly::makeGuard([self = shared_from_this(), ftn, fwd = forwarder.get()] {
        self->retireTrack(ftn, fwd);
      });
  co_await sendTrackData(params, requestID, forwarder);
}

folly::coro::Task<void> MoQTestPublisher::sendTrackData(
    MoQTestParameters params,
    RequestID requestID,
    std::shared_ptr<TrackConsumer> callback) {
  // Publish Objects in Accordance to params

  // Publisher Delivery Timeout (To be implemented later)

  // Switch based on forwarding preference
  switch (params.forwardingPreference) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      co_await sendOneSubgroupPerGroup(params, callback);
      break;
    }

    case (ForwardingPreference::ONE_SUBGROUP_PER_OBJECT): {
      co_await sendOneSubgroupPerObject(params, callback);

      break;
    }

    case (ForwardingPreference::TWO_SUBGROUPS_PER_GROUP): {
      co_await sendTwoSubgroupsPerGroup(params, callback);
      break;
    }

    case (ForwardingPreference::DATAGRAM): {
      co_await MoQTestPublisher::sendDatagram(requestID, params, callback);
      break;
    }

    default: {
      break;
    }
  }

  // Inform Consumer that publisher is finished opening subgroups/datagrams
  // Default PublishDone For Now

  PublishDone done;
  done.requestID = requestID;
  done.statusCode = PublishDoneStatusCode::TRACK_ENDED;
  done.reasonPhrase = kDefaultPublishDoneReason;
  callback->publishDone(std::move(done));
}

folly::coro::Task<folly::coro::Task<void>> MoQTestPublisher::startPublishTrack(
    const std::shared_ptr<MoQSession>& session,
    FullTrackName ftn,
    MoQTestParameters params,
    RequestID requestID) {
  auto forwarder = std::make_shared<MoQForwarder>(std::move(ftn));
  forwarder->setTrackAlias(TrackAlias(requestID.value));

  // The track starts paused and is unpaused once the peer asks for data. When
  // the subscriber registered before we published, PUBLISH_OK already carries
  // forward=1 and this fires during onPublishOk; when we published first it
  // fires on the peer's REQUEST_UPDATE. Generating objects before then would
  // just discard them.
  //
  // Only the first unpause is honored. If the peer pauses again mid-track we
  // keep generating and the forwarder drops the objects, which for a test
  // publisher is simpler than parking and re-arming.
  auto unpauseCb = std::make_shared<PendingUnpause>();
  forwarder->setCallback(unpauseCb);
  pendingUnpauses_.push_back(unpauseCb);

  auto subscriber = forwarder->addSubscriber(session, /*forward=*/false);
  if (!subscriber) {
    co_yield folly::coro::co_error(
        std::runtime_error("PUBLISH failed: addSubscriber returned null"));
  }

  auto publishResponse =
      session->publish(subscriber->getPublishRequest(), subscriber);
  if (publishResponse.hasError()) {
    co_yield folly::coro::co_error(
        std::runtime_error(
            folly::to<std::string>(
                "PUBLISH failed: ", publishResponse.error().reasonPhrase)));
  }
  subscriber->trackConsumer = std::move(publishResponse.value().consumer);

  auto pubResult = co_await folly::coro::co_awaitTry(
      std::move(publishResponse.value().reply));
  if (pubResult.hasException()) {
    co_yield folly::coro::co_error(
        std::runtime_error(
            folly::to<std::string>(
                "PUBLISH failed: ", pubResult.exception().what())));
  }
  if (pubResult.value().hasError()) {
    co_yield folly::coro::co_error(
        std::runtime_error(
            folly::to<std::string>(
                "PUBLISH rejected: ", pubResult.value().error().reasonPhrase)));
  }
  subscriber->onPublishOk(pubResult.value().value());

  co_return streamPublishedTrack(
      std::move(unpauseCb), std::move(forwarder), params, requestID);
}

folly::coro::Task<void> MoQTestPublisher::streamPublishedTrack(
    std::shared_ptr<PendingUnpause> unpauseCb,
    std::shared_ptr<MoQForwarder> forwarder,
    MoQTestParameters params,
    RequestID requestID) {
  // Drop the registration however this track ends, so a long-lived publisher
  // doesn't accumulate one fulfilled entry per publish.
  auto unregister = folly::makeGuard(
      [this, unpauseCb] { std::erase(pendingUnpauses_, unpauseCb); });
  co_await unpauseCb->unpaused.getFuture();
  co_await sendTrackData(params, requestID, std::move(forwarder));
}

folly::coro::Task<void> MoQTestPublisher::publishTrack(
    const std::shared_ptr<MoQSession>& session,
    FullTrackName ftn,
    MoQTestParameters params,
    RequestID requestID) {
  auto streamTask =
      co_await startPublishTrack(session, std::move(ftn), params, requestID);
  co_await std::move(streamTask);
}

folly::coro::Task<void> MoQTestPublisher::sendOneSubgroupPerGroup(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Groups
  auto token = co_await folly::coro::co_current_cancellation_token;
  const auto subgroupOptions =
      subgroupOptionsFor(params, 0, includeTimestampExtension_);
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    auto maybeSubConsumer = callback->beginSubgroup(
        groupNum, 0, publisherPriorityForGroup(groupNum), subgroupOptions);
    auto subConsumer = maybeSubConsumer->get();

    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (token.isCancellationRequested()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < lastObjectInGroup(params) ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        subConsumer->object(
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);
      } else {
        subConsumer->endOfGroup(objectId);
      }

      // Set Delay Based on Object Frequency
      co_await delay(params.objectFrequency);
    }

    // If SubGroup Hasn't Been Ended Already
    if (!token.isCancellationRequested() && !params.sendEndOfGroupMarkers) {
      subConsumer->endOfSubgroup();
    }
  }
}

folly::coro::Task<void> MoQTestPublisher::sendOneSubgroupPerObject(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Objects
  auto token = co_await folly::coro::co_current_cancellation_token;
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (token.isCancellationRequested()) {
        co_return;
      }
      auto maybeSubConsumer = callback->beginSubgroup(
          groupNum,
          objectId,
          publisherPriorityForGroup(groupNum),
          subgroupOptionsFor(params, objectId, includeTimestampExtension_));
      auto subConsumer = maybeSubConsumer->get();
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < lastObjectInGroup(params) ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        subConsumer->object(
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            true);
      } else {
        subConsumer->endOfGroup(objectId);
      }

      // Set Delay Based on Object Frequency
      co_await delay(params.objectFrequency);
    }
  }
  co_return;
}

folly::coro::Task<void> MoQTestPublisher::sendTwoSubgroupsPerGroup(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Objects
  auto token = co_await folly::coro::co_current_cancellation_token;
  // Odd number of objects in track means end on subgroupZero
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    std::vector<std::shared_ptr<SubgroupConsumer>> subConsumers;
    if (params.startObject % 2 == 0 ||
        (params.objectsPerGroup > 1 && params.objectIncrement % 2 == 1)) {
      // we have at least one even object
      subConsumers.push_back(
          callback
              ->beginSubgroup(
                  groupNum,
                  0,
                  publisherPriorityForGroup(groupNum),
                  subgroupOptionsFor(params, 0, includeTimestampExtension_))
              .value());
    } else {
      subConsumers.push_back(nullptr);
    }

    if (params.startObject % 2 == 1 ||
        (params.objectsPerGroup > 1 && params.objectIncrement % 2 == 1)) {
      // we have at least one odd object
      subConsumers.push_back(
          callback
              ->beginSubgroup(
                  groupNum,
                  1,
                  publisherPriorityForGroup(groupNum),
                  subgroupOptionsFor(params, 1, includeTimestampExtension_))
              .value());
    } else {
      subConsumers.push_back(nullptr);
    }

    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (token.isCancellationRequested()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);
      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < lastObjectInGroup(params) ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        int index = objectId % 2;
        XLOG(DBG1) << "Sending Object " << objectId << " to Subgroup " << index;
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        subConsumers[index]->object(
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);

      } else {
        auto lastSubgroup = objectId % 2;
        XLOG(DBG1) << "Sending End of Group Marker to Subgroup "
                   << lastSubgroup;
        subConsumers[lastSubgroup]->endOfGroup(objectId);

        // For case of only 1 object being sent
        if (subConsumers[1 - lastSubgroup]) {
          subConsumers[1 - lastSubgroup]->endOfSubgroup();
        }
      }

      // Set Delay Based on Object Frequency
      co_await delay(params.objectFrequency);
    }

    // If SubGroup Hasn't Been Ended Already
    if (!token.isCancellationRequested() && !params.sendEndOfGroupMarkers) {
      for (auto& subConsumer : subConsumers) {
        if (subConsumer) {
          subConsumer->endOfSubgroup();
        }
      }
    }
  }

  co_return;
}

folly::coro::Task<void> MoQTestPublisher::sendDatagram(
    RequestID requestID,
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  auto token = co_await folly::coro::co_current_cancellation_token;
  const auto lastObject = lastObjectInGroup(params);
  // Iterate through Objects
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (token.isCancellationRequested()) {
        // Instead of returning an error, callback->publishDone with error
        PublishDone done;
        done.requestID = requestID;
        done.reasonPhrase = "Datagram Subscription Cancelled";
        done.statusCode = PublishDoneStatusCode::INTERNAL_ERROR;
        callback->publishDone(std::move(done));
        co_return;
      }
      // Build object header
      ObjectHeader header;
      header.group = groupNum;
      header.id = objectId;
      header.priority = publisherPriorityForGroup(groupNum);

      // The datagram type byte carries either the end-of-group bit or an object
      // status, never both.  When the track asks for markers the group's last
      // object is sent as an END_OF_GROUP status datagram instead of a payload
      // object, which is what the subgroup path emits.
      const bool endOfGroupMarker =
          params.sendEndOfGroupMarkers && objectId == lastObject;
      Payload objectPayload;
      if (endOfGroupMarker) {
        // Draft 15+ rejects extensions on a non-NORMAL status object.
        header.status = ObjectStatus::END_OF_GROUP;
        header.length = 0;
      } else {
        int objectSize = getObjectSize(objectId, &params);
        objectPayload = folly::IOBuf::copyBuffer(std::string(objectSize, 't'));
        // Add Integer/Variable Extensions if needed
        header.extensions = Extensions(
            getExtensions(
                params.testIntegerExtension,
                params.testVariableExtension,
                includeTimestampExtension_),
            {});
      }

      auto res = callback->datagram(
          header,
          std::move(objectPayload),
          !endOfGroupMarker && objectId == lastObject);
      if (res.hasError()) {
        // If sending datagram fails, callback->publishDone with error
        PublishDone done;
        done.requestID = requestID;
        done.reasonPhrase = "Error Sending Datagram Objects";
        done.statusCode = PublishDoneStatusCode::INTERNAL_ERROR;
        callback->publishDone(std::move(done));
        co_return;
      }

      // Set Delay Based on Object Frequency
      co_await delay(params.objectFrequency);
    }
  }

  co_return;
}

folly::Expected<StandaloneFetch, FetchError>
MoQTestPublisher::resolveJoiningFetch(
    const Fetch& fetch,
    const JoiningFetch& joining) {
  auto trackIt = tracks_.find(fetch.fullTrackName);
  if (trackIt == tracks_.end()) {
    return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::DOES_NOT_EXIST,
            "No subscription for joining FETCH"});
  }
  auto& forwarder = *trackIt->second.forwarder;
  // A joining FETCH is anchored on the subscription's Largest, so a track that
  // has published nothing has nothing to anchor to.
  if (!forwarder.largest()) {
    return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::INVALID_RANGE,
            "No objects published for track"});
  }
  auto range =
      forwarder.resolveJoiningFetch(MoQSession::getRequestSession(), joining);
  if (range.hasError()) {
    auto error = range.error();
    error.requestID = fetch.requestID;
    return folly::makeUnexpected(error);
  }
  return StandaloneFetch(range->start, range->end);
}

// Fetch Methods
folly::coro::Task<MoQSession::FetchResult> MoQTestPublisher::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  // Ensure Params are valid according to spec, if not return FetchError
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &fetch.fullTrackName.trackNamespace);
  if (res.hasError()) {
    FetchError error;
    error.requestID = fetch.requestID;
    error.errorCode = FetchErrorCode::NOT_SUPPORTED;
    error.reasonPhrase = "Invalid Parameters";
    co_return folly::makeUnexpected(error);
  }

  // The generators only walk the track forwards.
  if (fetch.groupOrder == GroupOrder::NewestFirst) {
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::NOT_SUPPORTED,
            "Descending group order not supported"});
  }

  auto [standalone, joining] = fetchType(fetch);
  const bool isJoining = joining != nullptr;
  if (isJoining) {
    auto range = resolveJoiningFetch(fetch, *joining);
    if (range.hasError()) {
      co_return folly::makeUnexpected(range.error());
    }
    fetch.args = range.value();
    standalone = fetchType(fetch).first;
  }

  auto params = res.value();
  const auto window = resolveFetchWindow(params, *standalone);
  XLOG(INFO) << (isJoining ? "Joining FETCH " : "FETCH ") << standalone->start
             << ".." << standalone->end;
  if (window.empty()) {
    XLOG(INFO) << "Requested range misses the track, sending no objects";
  } else {
    XLOG(INFO) << "Serving " << window.first << ".." << window.last;
  }

  // A backfill paced at the live rate would never catch the subscription up.
  if (isJoining) {
    params.objectFrequency = 0;
  }

  auto cancelSource = std::make_shared<folly::CancellationSource>();
  activeFetches_.push_back(cancelSource);

  // Start a Co-routine with cancellation support
  co_withCancellation(
      cancelSource->getToken(),
      co_withExecutor(
          co_await folly::coro::co_current_executor,
          runFetch(cancelSource, params, window, std::move(fetchCallback))))
      .start();

  FetchOk ok;
  ok.requestID = fetch.requestID;
  ok.groupOrder = GroupOrder::OldestFirst;
  ok.endOfTrack = window.endOfTrack ? 1 : 0;
  ok.endLocation = fetchEndLocation(params, *standalone);

  co_return std::make_shared<MoQTestFetchHandle>(ok, std::move(cancelSource));
}

folly::coro::Task<void> MoQTestPublisher::runFetch(
    std::shared_ptr<folly::CancellationSource> cancelSource,
    MoQTestParameters params,
    MoQTestFetchWindow window,
    std::shared_ptr<FetchConsumer> callback) {
  // Drop the registration however the fetch ends, so a long-lived publisher
  // doesn't accumulate one entry per completed fetch.  The self-reference keeps
  // the publisher alive for the detached coroutine.  This erase and cancelAll()
  // both run on the publisher's EventBase, so they never interleave.
  auto unregister = folly::makeGuard([self = shared_from_this(), cancelSource] {
    std::erase(self->activeFetches_, cancelSource);
  });
  co_await onFetch(params, window, std::move(callback));
}

folly::coro::Task<void> MoQTestPublisher::onFetch(
    MoQTestParameters params,
    MoQTestFetchWindow window,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  if (window.empty()) {
    // FETCH_OK has already gone out, so close the fetch without any objects.
    fetchCallback->endOfFetch();
    co_return;
  }

  // Publish Objects in Accordance to params

  // Publisher Delivery Timeout (To be implemented later)

  // Switch based on forwarding preference
  switch (fetchForwardingPreference(params.forwardingPreference)) {
    // fetchForwardingPreference() remaps DATAGRAM to one subgroup per group.
    case (ForwardingPreference::DATAGRAM):
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      co_await fetchOneSubgroupPerGroup(params, fetchCallback, window);
      break;
    }

    case (ForwardingPreference::ONE_SUBGROUP_PER_OBJECT): {
      co_await fetchOneSubgroupPerObject(params, fetchCallback, window);
      break;
    }

    case (ForwardingPreference::TWO_SUBGROUPS_PER_GROUP): {
      co_await fetchTwoSubgroupsPerGroup(params, fetchCallback, window);
      break;
    }

    default: {
      break;
    }
  }

  co_return;
}

folly::coro::Task<void> MoQTestPublisher::fetchOneSubgroupPerGroup(
    MoQTestParameters params,
    std::shared_ptr<FetchConsumer> callback,
    MoQTestFetchWindow window) {
  // Iterate through Groups
  auto token = co_await folly::coro::co_current_cancellation_token;
  // A datagram track fetches back through here because its objects have no
  // subgroup.  From draft 16 the flag tells the framer to omit the subgroup
  // field entirely; on draft 15 there is no flag and the 0 below is written.
  const bool isDatagram =
      params.forwardingPreference == ForwardingPreference::DATAGRAM;
  // FetchConsumer::endOfGroup() has no datagram flag to set, so a marker here
  // would advertise a second forwarding preference inside the group.  The
  // subscribe path marks the group on a status datagram instead.
  const bool sendEndOfGroupMarkers =
      params.sendEndOfGroupMarkers && !isDatagram;
  for (uint64_t groupNum = window.first.group; groupNum <= window.last.group;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    const uint64_t lastObject = window.lastObjectIn(groupNum);
    for (uint64_t objectId = window.firstObjectIn(groupNum);
         objectId <= lastObject;
         objectId += params.objectIncrement) {
      if (token.isCancellationRequested()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < lastObjectInGroup(params) || !sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        callback->object(
            groupNum,
            0 /* subgroupId */,
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false,
            isDatagram);
      } else {
        callback->endOfGroup(groupNum, 0 /* subgroupId */, objectId, false);
      }

      // Set Delay Based on Object Frequency
      co_await delay(params.objectFrequency);
    }
  }

  // Inform Consumer that fetch is completed
  callback->endOfFetch();
}

folly::coro::Task<void> MoQTestPublisher::fetchOneSubgroupPerObject(
    MoQTestParameters params,
    std::shared_ptr<FetchConsumer> callback,
    MoQTestFetchWindow window) {
  // Iterate through Groups
  auto token = co_await folly::coro::co_current_cancellation_token;
  for (uint64_t groupNum = window.first.group; groupNum <= window.last.group;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects
    const uint64_t lastObject = window.lastObjectIn(groupNum);
    for (uint64_t objectId = window.firstObjectIn(groupNum);
         objectId <= lastObject;
         objectId += params.objectIncrement) {
      if (token.isCancellationRequested()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < lastObjectInGroup(params) ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        callback->object(
            groupNum,
            objectId,
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);
      } else {
        callback->endOfGroup(groupNum, objectId, objectId, false);
      }

      // Set Delay Based on Object Frequency
      co_await delay(params.objectFrequency);
    }
  }

  // Inform Consumer that fetch is completed
  callback->endOfFetch();
}

folly::coro::Task<void> MoQTestPublisher::fetchTwoSubgroupsPerGroup(
    MoQTestParameters params,
    std::shared_ptr<FetchConsumer> callback,
    MoQTestFetchWindow window) {
  // Iterate through Groups
  auto token = co_await folly::coro::co_current_cancellation_token;
  for (uint64_t groupNum = window.first.group; groupNum <= window.last.group;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    const uint64_t lastObject = window.lastObjectIn(groupNum);
    for (uint64_t objectId = window.firstObjectIn(groupNum);
         objectId <= lastObject;
         objectId += params.objectIncrement) {
      if (token.isCancellationRequested()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      // The same split the subscribe path uses.
      const uint64_t subgroupId = objectId % 2;
      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < lastObjectInGroup(params) ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        callback->object(
            groupNum,
            subgroupId,
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);
      } else {
        callback->endOfGroup(groupNum, subgroupId, objectId, false);
      }

      // Set Delay Based on Object Frequency
      co_await delay(params.objectFrequency);
    }
  }

  // Inform Consumer that fetch is completed
  callback->endOfFetch();
}

} // namespace moxygen
