// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "moxygen/moqtest/MoQTestServer.h"
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Sleep.h>
#include "moxygen/moqtest/Utils.h"

std::string kCert = "fake_cert";
std::string kKey = "fake_key";
std::string kEndpointName = "fake_endpoint";

namespace moxygen {

const int kDefaultExpires = 0;
const std::string kDefaultSubscribeDoneReason = "Testing";

void MoQTestSubscriptionHandle::unsubscribe() {
  // Empty Method Body For Now
}

void MoQTestSubscriptionHandle::subscribeUpdate(SubscribeUpdate subUpdate) {
  // Empty Method Body For Now
}

void MoQTestFetchHandle::fetchCancel() {
  // Empty Method Body For Now
}

MoQTestServer::MoQTestServer(uint16_t port)
    : MoQServer(port, kCert, kKey, kEndpointName) {}

folly::coro::Task<MoQSession::SubscribeResult> MoQTestServer::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  LOG(INFO) << "Recieved Subscription";

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
  auto session = MoQSession::getRequestSession();

  // Start a Co-routine to send objects back according to spec
  onSubscribe(sub, callback).scheduleOn(session->getEventBase()).start();

  // Return a SubscribeOk
  SubscribeOk subRes{
      sub.requestID,
      std::chrono::milliseconds(kDefaultExpires),
      sub.groupOrder,
      folly::none};
  return folly::coro::makeTask<SubscribeResult>(
      std::make_shared<MoQTestSubscriptionHandle>(subRes));
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

  co_return;
}

folly::coro::Task<void> MoQTestServer::sendOneSubgroupPerGroup(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Groups
  for (int groupNum = params.startGroup; groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Begin a New Subgroup (Default Priority)
    auto maybeSubConsumer =
        callback->beginSubgroup(groupNum, 0, kDefaultPriority);
    auto subConsumer = maybeSubConsumer->get();

    // Iterate Through Objects in SubGroup
    for (int objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
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
    if (!params.sendEndOfGroupMarkers) {
      subConsumer->endOfSubgroup();
    }
  }
}

folly::coro::Task<void> MoQTestServer::sendOneSubgroupPerObject(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Objects
  for (int groupNum = params.startGroup; groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (int objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
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

  // Odd number of objects in track means end on subgroupZero
  bool endZero = params.lastObjectInTrack % 2 == 1;
  for (int groupNum = params.startGroup; groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    std::vector<std::shared_ptr<SubgroupConsumer>> subConsumers;
    subConsumers.push_back(
        callback->beginSubgroup(groupNum, 0, kDefaultPriority).value());
    subConsumers.push_back(
        callback->beginSubgroup(groupNum, 1, kDefaultPriority).value());

    // Iterate Through Objects in SubGroup
    for (int objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
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
        subConsumers[(objectId % 2)]->object(
            objectId, std::move(objectPayload), extensions, false);

      } else {
        subConsumers[(int)!endZero]->endOfGroup(objectId);
        subConsumers[(int)endZero]->endOfSubgroup();
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }

    // If SubGroup Hasn't Been Ended Already
    if (!params.sendEndOfGroupMarkers) {
      subConsumers[0]->endOfSubgroup();
      subConsumers[1]->endOfSubgroup();
    }
  }

  co_return;
}

folly::coro::Task<MoQSession::SubscribeResult> MoQTestServer::sendDatagram(
    SubscribeRequest sub,
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Objects
  for (int groupNum = params.startGroup; groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (int objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
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

      // Try/Catch Datagram

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
  co_return std::make_shared<MoQTestSubscriptionHandle>(subRes);
}

// Fetch Methods
folly::coro::Task<MoQSession::FetchResult> MoQTestServer::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  LOG(INFO) << "Recieved Fetch Request";

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

  // Request Session
  auto session = MoQSession::getRequestSession();

  // Start a Co-routine
  onFetch(fetch, fetchCallback).scheduleOn(session->getEventBase()).start();

  FetchOk ok;
  ok.requestID = fetch.requestID;
  ok.groupOrder = fetch.groupOrder;
  return folly::coro::makeTask<FetchResult>(
      std::make_shared<MoQTestFetchHandle>(ok));
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

  co_return;
}

folly::coro::Task<void> MoQTestServer::fetchOneSubgroupPerGroup(
    MoQTestParameters params,
    std::shared_ptr<FetchConsumer> callback) {
  // Iterate through Groups
  for (int groupNum = params.startGroup; groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (int objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
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
  for (int groupNum = params.startGroup; groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects
    for (int objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
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
  for (int groupNum = params.startGroup; groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (int objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      // Find Object Size
      int objectSize = getObjectSize(objectId, &params);

      // Add Integer/Variable Extensions if needed
      std::vector<Extension> extensions = getExtensions(
          params.testIntegerExtension, params.testVariableExtension);

      int subGroupId = objectId % 2;
      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < params.lastObjectInTrack ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        callback->object(
            groupNum,
            subGroupId,
            objectId,
            std::move(objectPayload),
            extensions,
            false);
      } else {
        callback->endOfGroup(groupNum, subGroupId, objectId, extensions, false);
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }
  }

  // Inform Consumer that fetch is completed
  callback->endOfFetch();
}

} // namespace moxygen
