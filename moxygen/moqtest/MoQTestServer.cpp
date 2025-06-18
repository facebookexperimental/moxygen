// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/moqtest/MoQTestServer.h"
#include <folly/coro/Sleep.h>
#include "moxygen/moqtest/Utils.h"

std::string kCert = "fake_cert";
std::string kKey = "fake_key";
std::string kEndpointName = "fake_endpoint";

namespace moxygen {

const int kDefaultExpires = 0;
const std::string kDefaultSubscribeDoneReason = "Testing";

void MoQTestSubscriptionHandle::unsubscribe() {
  cancelSource_->requestCancellation();
}

void MoQTestSubscriptionHandle::subscribeUpdate(SubscribeUpdate subUpdate) {
  LOG(INFO) << "Received Subscribe Update";
}

void MoQTestFetchHandle::fetchCancel() {
  cancelSource_->requestCancellation();
}

void MoQTestServer::goaway(Goaway goaway) {
  LOG(INFO) << "Server goaway uri=" << goaway.newSessionUri;

  // Call Go Away on sessions
  if (subSession_) {
    subSession_->goaway(goaway);
  }

  if (fetchSession_) {
    fetchSession_->goaway(goaway);
  }
}

MoQTestServer::MoQTestServer(uint16_t port)
    : MoQServer(port, kCert, kKey, kEndpointName) {}

folly::coro::Task<MoQSession::SubscribeResult> MoQTestServer::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  LOG(INFO) << "Recieved Subscription";
  if (subCancelSource_) {
    SubscribeError error;
    error.requestID = sub.requestID;
    error.errorCode = SubscribeErrorCode::INTERNAL_ERROR;
    error.reasonPhrase = "Cannot have concurrent subscriptions";
    return folly::coro::makeTask<SubscribeResult>(folly::makeUnexpected(error));
  }

  subCancelSource_ = std::make_shared<folly::CancellationSource>();

  // Ensure Params are valid according to spec, if not return SubscribeError
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &sub.fullTrackName.trackNamespace);
  if (res.hasError()) {
    SubscribeError error;
    error.requestID = sub.requestID;
    error.errorCode = SubscribeErrorCode::NOT_SUPPORTED;
    error.reasonPhrase = "Invalid Parameters";
    return folly::coro::makeTask<SubscribeResult>(folly::makeUnexpected(error));
  }

  // Request Session
  subSession_ = MoQSession::getRequestSession();

  if (logger_) {
    subSession_->setLogger(logger_);
  }

  // Start a Co-routine to send objects back according to spec
  onSubscribe(sub, callback).scheduleOn(subSession_->getEventBase()).start();

  // Return a SubscribeOk
  SubscribeOk subRes{
      sub.requestID,
      std::chrono::milliseconds(kDefaultExpires),
      sub.groupOrder,
      folly::none};
  return folly::coro::makeTask<SubscribeResult>(
      std::make_shared<MoQTestSubscriptionHandle>(
          subRes, &(*subCancelSource_)));
}

// Perform Co-routine
folly::coro::Task<void> MoQTestServer::onSubscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  // Make a MoQTestParams (Only valid params are passed through from subscribe
  // function)
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &sub.fullTrackName.trackNamespace);
  CHECK(res.hasValue())
      << "Only valid params must be passed into this function";
  MoQTestParameters params = res.value();

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
      auto res = co_await MoQTestServer::sendDatagram(sub, params, callback);
      if (res.hasError()) {
        // Return a SubscribeDone With an Error to indicate Datagram process
        // failed
        SubscribeDone done;
        done.requestID = sub.requestID;
        done.reasonPhrase = "Error Sending Datagram Objects";
        done.statusCode = SubscribeDoneStatusCode::INTERNAL_ERROR;
        callback->subscribeDone(done);
        co_return;
      }
      break;
    }

    default: {
      break;
    }
  }

  // Inform Consumer that publisher is finished opening subgroups/datagrams
  // Default SubscribeDone For Now
  SubscribeDone done;
  done.requestID = sub.requestID;
  done.reasonPhrase = kDefaultSubscribeDoneReason;
  callback->subscribeDone(done);

  // Reset Session
  subCancelSource_ = nullptr;

  co_return;
}

folly::coro::Task<void> MoQTestServer::sendOneSubgroupPerGroup(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Groups
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Begin a New Subgroup (Default Priority)
    auto maybeSubConsumer =
        callback->beginSubgroup(groupNum, 0, kDefaultPriority);
    auto subConsumer = maybeSubConsumer->get();

    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (isSubCancelled()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension, params.testVariableExtension);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < params.lastObjectInTrack ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        subConsumer->object(
            objectId, std::move(objectPayload), extensions, false);
      } else {
        subConsumer->endOfGroup(objectId);
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }

    // If SubGroup Hasn't Been Ended Already
    if (!isSubCancelled() && !params.sendEndOfGroupMarkers) {
      subConsumer->endOfSubgroup();
    }
  }
}

folly::coro::Task<void> MoQTestServer::sendOneSubgroupPerObject(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Objects
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (isSubCancelled()) {
        co_return;
      }
      // Begin a New Subgroup per object (Default Priority)
      auto maybeSubConsumer =
          callback->beginSubgroup(groupNum, objectId, kDefaultPriority);
      auto subConsumer = maybeSubConsumer->get();
      // Find Object Size
      int objectSize = moxygen::getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension, params.testVariableExtension);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < params.lastObjectInTrack ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        subConsumer->object(
            objectId, std::move(objectPayload), extensions, false);
      } else {
        subConsumer->endOfGroup(objectId);
      }

      // If SubGroup Hasn't Been Ended Already
      if (!params.sendEndOfGroupMarkers) {
        subConsumer->endOfSubgroup();
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }
  }
  co_return;
}

folly::coro::Task<void> MoQTestServer::sendTwoSubgroupsPerGroup(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Objects
  LOG(INFO) << "Starting Two Subgroups Per Group" << std::endl;

  // Odd number of objects in track means end on subgroupZero
  bool endZero = (params.lastObjectInTrack - params.startObject) % 2 == 1;
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    std::vector<std::shared_ptr<SubgroupConsumer>> subConsumers;
    subConsumers.push_back(
        callback->beginSubgroup(groupNum, 0, kDefaultPriority).value());

    if (params.objectsPerGroup > 1) {
      subConsumers.push_back(
          callback->beginSubgroup(groupNum, 1, kDefaultPriority).value());
    }

    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (isSubCancelled()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);
      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension, params.testVariableExtension);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < params.lastObjectInTrack ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        int index;
        if (params.objectsPerGroup > 1) {
          index = (objectId - params.startObject) % 2;
        } else {
          index = 0;
        }
        LOG(INFO) << "Sending Object " << objectId << " to Subgroup " << index;
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        subConsumers[index]->object(
            objectId, std::move(objectPayload), extensions, false);

      } else {
        LOG(INFO) << "Sending End of Group Marker to Subgroup " << !endZero;
        subConsumers[(int)!endZero]->endOfGroup(objectId);

        // For case of only 1 object being sent
        if (params.objectsPerGroup > 1) {
          subConsumers[(int)endZero]->endOfSubgroup();
        }
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }

    // If SubGroup Hasn't Been Ended Already
    if (!isSubCancelled() && !params.sendEndOfGroupMarkers) {
      subConsumers[0]->endOfSubgroup();
      if (params.objectsPerGroup > 1) {
        subConsumers[1]->endOfSubgroup();
      }
    }
  }

  co_return;
}

folly::coro::Task<MoQSession::SubscribeResult> MoQTestServer::sendDatagram(
    SubscribeRequest sub,
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Objects
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (isSubCancelled()) {
        co_return folly::makeUnexpected(SubscribeError{
            sub.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "Datagram Subscription Cancelled"});
      }
      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension, params.testVariableExtension);

      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      std::string p = std::string(objectSize, 't');
      auto objectPayload = folly::IOBuf::copyBuffer(p);

      // Build object header
      ObjectHeader header;
      header.trackIdentifier = TrackIdentifier(sub.trackAlias);
      header.group = groupNum;
      header.id = objectId;
      header.extensions = extensions;

      auto res = callback->datagram(header, std::move(objectPayload));
      if (res.hasError()) {
        co_return folly::makeUnexpected(SubscribeError{
            sub.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "Error Sending Datagram Objects"});
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency))
          .scheduleOn(folly::getGlobalCPUExecutor());
    }
  }

  // Return SubscribeOK
  SubscribeOk subRes{
      sub.requestID,
      std::chrono::milliseconds(kDefaultExpires),
      sub.groupOrder,
      folly::none};
  co_return std::make_shared<MoQTestSubscriptionHandle>(
      subRes, &(*subCancelSource_));
}

// Fetch Methods
folly::coro::Task<MoQSession::FetchResult> MoQTestServer::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  LOG(INFO) << "Recieved Fetch Request";
  if (fetchCancelSource_) {
    FetchError error;
    error.requestID = fetch.requestID;
    error.errorCode = FetchErrorCode::INTERNAL_ERROR;
    error.reasonPhrase = "Cannot have concurrent fetches";
    return folly::coro::makeTask<FetchResult>(folly::makeUnexpected(error));
  }
  fetchCancelSource_ = std::make_shared<folly::CancellationSource>();

  // Ensure Params are valid according to spec, if not return FetchError
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &fetch.fullTrackName.trackNamespace);
  if (res.hasError()) {
    FetchError error;
    error.requestID = fetch.requestID;
    error.errorCode = FetchErrorCode::NOT_SUPPORTED;
    error.reasonPhrase = "Invalid Parameters";
    return folly::coro::makeTask<FetchResult>(folly::makeUnexpected(error));
  }
  if (res.value().forwardingPreference == ForwardingPreference::DATAGRAM) {
    FetchError error;
    error.requestID = fetch.requestID;
    error.errorCode = FetchErrorCode::NOT_SUPPORTED;
    error.reasonPhrase =
        "Datagram Forwarding Preference is not supported for fetch";
    return folly::coro::makeTask<FetchResult>(folly::makeUnexpected(error));
  }

  // Request Session
  fetchSession_ = MoQSession::getRequestSession();
  if (logger_) {
    fetchSession_->setLogger(logger_);
  }

  // Start a Co-routine
  onFetch(fetch, fetchCallback)
      .scheduleOn(fetchSession_->getEventBase())
      .start();

  FetchOk ok;
  ok.requestID = fetch.requestID;
  ok.groupOrder = fetch.groupOrder;

  return folly::coro::makeTask<FetchResult>(
      std::make_shared<MoQTestFetchHandle>(ok, &(*fetchCancelSource_)));
}

folly::coro::Task<void> MoQTestServer::onFetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  // Make a MoQTestParams (Only valid params are passed through from fetch
  // function)
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &fetch.fullTrackName.trackNamespace);
  CHECK(res.hasValue())
      << "Only valid params must be passed into this function";
  CHECK_NE(
      static_cast<int>(res.value().forwardingPreference),
      static_cast<int>(ForwardingPreference::DATAGRAM))
      << "Datagram Forwarding Preference is not supported for fetch";
  MoQTestParameters params = res.value();

  // Publish Objects in Accordance to params

  // Publisher Delivery Timeout (To be implemented later)

  // Switch based on forwarding preference
  switch (params.forwardingPreference) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      co_await fetchOneSubgroupPerGroup(params, fetchCallback);
      break;
    }

    case (ForwardingPreference::ONE_SUBGROUP_PER_OBJECT): {
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

  // Reset Session
  fetchCancelSource_ = nullptr;

  co_return;
}

folly::coro::Task<void> MoQTestServer::fetchOneSubgroupPerGroup(
    MoQTestParameters params,
    std::shared_ptr<FetchConsumer> callback) {
  // Iterate through Groups
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (isFetchCancelled()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension, params.testVariableExtension);

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
            extensions,
            false);
      } else {
        callback->endOfGroup(
            groupNum, 0 /* subgroupId */, objectId, extensions, false);
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }
  }

  // Inform Consumer that fetch is completed
  callback->endOfFetch();
}

folly::coro::Task<void> MoQTestServer::fetchOneSubgroupPerObject(
    MoQTestParameters params,
    std::shared_ptr<FetchConsumer> callback) {
  // Iterate through Groups
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (isFetchCancelled()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension, params.testVariableExtension);

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
            extensions,
            false);
      } else {
        callback->endOfGroup(groupNum, objectId, objectId, extensions, false);
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }
  }

  // Inform Consumer that fetch is completed
  callback->endOfFetch();
}

folly::coro::Task<void> MoQTestServer::fetchTwoSubgroupsPerGroup(
    MoQTestParameters params,
    std::shared_ptr<FetchConsumer> callback) {
  // Iterate through Groups
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (isFetchCancelled()) {
        co_return;
      }
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension, params.testVariableExtension);

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
            extensions,
            false);
      } else {
        callback->endOfGroup(groupNum, subgroupId, objectId, extensions, false);
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }
  }

  // Inform Consumer that fetch is completed
  callback->endOfFetch();
}

bool MoQTestServer::isSubCancelled() {
  return subCancelSource_->isCancellationRequested();
}

bool MoQTestServer::isFetchCancelled() {
  return fetchCancelSource_->isCancellationRequested();
}

} // namespace moxygen
