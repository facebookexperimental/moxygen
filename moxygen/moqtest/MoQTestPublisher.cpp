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
  cancelSource_.requestCancellation();
}

folly::coro::Task<void> MoQTestPublisher::delay(uint64_t ms) {
  co_await folly::coro::sleep(std::chrono::milliseconds(ms), &timekeeper_);
}

void MoQTestPublisher::cancelAll() {
  for (auto& [key, state] : activeSubscriptions_) {
    state.cancelSource.requestCancellation();
  }
  // Move before cancelling: cancel() can resume a waiting publish inline, and
  // that coroutine erases its own entry on the way out.
  auto pending = std::move(pendingUnpauses_);
  pendingUnpauses_.clear();
  for (auto& p : pending) {
    p->cancel();
  }
}

void MoQTestPublisher::removeSubscription(SubKey key) {
  auto it = activeSubscriptions_.find(key);
  if (it != activeSubscriptions_.end()) {
    it->second.cancelSource.requestCancellation();
    activeSubscriptions_.erase(it);
  }
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
  auto forwarder = std::make_shared<MoQForwarder>(sub.fullTrackName);
  forwarder->setTrackAlias(TrackAlias(sub.requestID.value));

  // Advertise the priority even-numbered groups are published at, so the
  // draft-15+ subgroup and datagram encodings elide it for those and write it
  // explicitly for the odd ones.  The framer downgrades the extension to a
  // PUBLISHER_PRIORITY param for draft 15; earlier drafts have no way to carry
  // it, so the priority always stays on the wire instead.
  auto version = session->getNegotiatedVersion();
  if (version && getDraftMajorVersion(*version) >= 15) {
    Extensions trackProperties;
    trackProperties.insertMutableExtension(
        Extension{kPublisherPriorityExtensionType, kMoQTestPublisherPriority});
    forwarder->setExtensions(std::move(trackProperties));
  }

  SubKey subKey{session.get(), sub.requestID.value};
  auto& state = activeSubscriptions_[subKey];
  state.forwarder = forwarder;
  auto token = state.cancelSource.getToken();

  struct EmptyCb : public MoQForwarder::Callback {
    std::weak_ptr<MoQTestPublisher> publisher;
    SubKey key;
    void onEmpty(MoQForwarder*) override {
      if (auto p = publisher.lock()) {
        p->removeSubscription(key);
      }
    }
  };
  auto cb = std::make_shared<EmptyCb>();
  cb->publisher = shared_from_this();
  cb->key = subKey;
  forwarder->setCallback(std::move(cb));

  auto subscriber = forwarder->addSubscriber(session, sub, std::move(callback));

  co_withCancellation(
      token,
      co_withExecutor(
          co_await folly::coro::co_current_executor,
          onSubscribe(sub, forwarder)))
      .start();

  co_return subscriber;
}

// Perform Co-routine
folly::coro::Task<void> MoQTestPublisher::onSubscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  // Make a MoQTestParams (Only valid params are passed through from subscribe
  // function)
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &sub.fullTrackName.trackNamespace);
  XCHECK(res.hasValue())
      << "Only valid params must be passed into this function";
  co_await sendTrackData(res.value(), sub.requestID, std::move(callback));
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
      if (objectId < params.lastObjectInTrack ||
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
      if (objectId < params.lastObjectInTrack ||
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
      if (objectId < params.lastObjectInTrack ||
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
  auto alias = TrackAlias(requestID.value);
  callback->setTrackAlias(alias);
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
      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      std::string p = std::string(objectSize, 't');
      auto objectPayload = folly::IOBuf::copyBuffer(p);

      // Build object header
      ObjectHeader header;
      header.group = groupNum;
      header.id = objectId;
      header.priority = publisherPriorityForGroup(groupNum);
      header.extensions = Extensions(extensions, {});

      auto res = callback->datagram(
          header, std::move(objectPayload), objectId == lastObject);
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

// Fetch Methods
folly::coro::Task<MoQSession::FetchResult> MoQTestPublisher::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  XLOG(INFO) << "Recieved Fetch Request";

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

  // Declare cancellation source
  folly::CancellationSource cancelSource;

  // Start a Co-routine with cancellation support
  co_withCancellation(
      cancelSource.getToken(),
      co_withExecutor(
          co_await folly::coro::co_current_executor,
          onFetch(fetch, fetchCallback)))
      .start();

  FetchOk ok;
  ok.requestID = fetch.requestID;
  ok.groupOrder = fetch.groupOrder;

  co_return std::make_shared<MoQTestFetchHandle>(ok, std::move(cancelSource));
}

folly::coro::Task<void> MoQTestPublisher::onFetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  // Make a MoQTestParams (Only valid params are passed through from fetch
  // function)
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &fetch.fullTrackName.trackNamespace);
  XCHECK(res.hasValue())
      << "Only valid params must be passed into this function";
  MoQTestParameters params = res.value();

  // Publish Objects in Accordance to params

  // Publisher Delivery Timeout (To be implemented later)

  // Switch based on forwarding preference
  switch (params.forwardingPreference) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      co_await fetchOneSubgroupPerGroup(params, fetchCallback);
      break;
    }

    case (ForwardingPreference::ONE_SUBGROUP_PER_OBJECT):
    case (ForwardingPreference::DATAGRAM): {
      co_await fetchOneSubgroupPerObject(params, fetchCallback);
      break;
    }

    case (ForwardingPreference::TWO_SUBGROUPS_PER_GROUP): {
      co_await fetchTwoSubgroupsPerGroup(params, fetchCallback);
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
    std::shared_ptr<FetchConsumer> callback) {
  // Iterate through Groups
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
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < params.lastObjectInTrack ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        callback->object(
            groupNum,
            0 /* subgroupId */,
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);
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
    std::shared_ptr<FetchConsumer> callback) {
  // Iterate through Groups
  auto token = co_await folly::coro::co_current_cancellation_token;
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects
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
      if (objectId < params.lastObjectInTrack ||
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
    std::shared_ptr<FetchConsumer> callback) {
  // Iterate through Groups
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
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension,
          params.testVariableExtension,
          includeTimestampExtension_);

      int subgroupId;
      if (params.objectsPerGroup > 1) {
        subgroupId = (objectId - params.startObject) % 2;
      } else {
        subgroupId = 0;
      }
      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < params.lastObjectInTrack ||
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
