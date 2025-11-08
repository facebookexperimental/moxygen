/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include "moxygen/moqtest/MoQTestServer.h"
#include <folly/coro/Sleep.h>
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include "moxygen/moqtest/Utils.h"

std::string kCert = "fake_cert";
std::string kKey = "fake_key";
std::string kEndpointName = "fake_endpoint";

namespace moxygen {

const int kDefaultExpires = 0;
const std::string kDefaultSubscribeDoneReason = "Testing";

void MoQTestSubscriptionHandle::unsubscribe() {
  cancelSource_.requestCancellation();
}

void MoQTestSubscriptionHandle::subscribeUpdate(SubscribeUpdate subUpdate) {
  LOG(INFO) << "Received Subscribe Update";
}

void MoQTestFetchHandle::fetchCancel() {
  cancelSource_.requestCancellation();
}

MoQTestServer::MoQTestServer()
    : MoQServer(
          quic::samples::createFizzServerContextWithInsecureDefault(
              {"h3", "moq-00"},
              fizz::server::ClientAuthMode::None,
              kCert,
              kKey),
          kEndpointName) {}

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
    co_return folly::makeUnexpected(error);
  }

  // Start a Co-routine to send objects back according to spec
  auto alias = sub.trackAlias.value_or(TrackAlias(sub.requestID.value));
  callback->setTrackAlias(alias);
  // Declare cancellation source
  folly::CancellationSource cancelSource;

  co_withCancellation(
      cancelSource.getToken(),
      co_withExecutor(
          co_await folly::coro::co_current_executor,
          onSubscribe(sub, callback)))
      .start();

  // Return a SubscribeOk
  SubscribeOk subRes{
      sub.requestID,
      alias,
      std::chrono::milliseconds(kDefaultExpires),
      MoQSession::resolveGroupOrder(GroupOrder::OldestFirst, sub.groupOrder),
      folly::none};
  co_return std::make_shared<MoQTestSubscriptionHandle>(
      subRes, std::move(cancelSource));
}

// Perform Co-routine
folly::coro::Task<void> MoQTestServer::onSubscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  // Make a MoQTestParams (Only valid params are passed through from subscribe
  // function)
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &sub.fullTrackName.trackNamespace);
  XCHECK(res.hasValue())
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
      co_await MoQTestServer::sendDatagram(sub, params, callback);
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
  callback->subscribeDone(std::move(done));
}

folly::coro::Task<void> MoQTestServer::sendOneSubgroupPerGroup(
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  // Iterate through Groups
  auto token = co_await folly::coro::co_current_cancellation_token;
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
      if (token.isCancellationRequested()) {
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
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);
      } else {
        subConsumer->endOfGroup(objectId);
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }

    // If SubGroup Hasn't Been Ended Already
    if (!token.isCancellationRequested() && !params.sendEndOfGroupMarkers) {
      subConsumer->endOfSubgroup();
    }
  }
}

folly::coro::Task<void> MoQTestServer::sendOneSubgroupPerObject(
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
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);
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
  LOG(INFO) << "Starting Two Subgroups Per Group";
  auto token = co_await folly::coro::co_current_cancellation_token;
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
      if (token.isCancellationRequested()) {
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
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);

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
    if (!token.isCancellationRequested() && !params.sendEndOfGroupMarkers) {
      subConsumers[0]->endOfSubgroup();
      if (params.objectsPerGroup > 1) {
        subConsumers[1]->endOfSubgroup();
      }
    }
  }

  co_return;
}

folly::coro::Task<void> MoQTestServer::sendDatagram(
    SubscribeRequest sub,
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  auto alias = sub.trackAlias.value_or(TrackAlias(sub.requestID.value));
  callback->setTrackAlias(alias);
  auto token = co_await folly::coro::co_current_cancellation_token;
  // Iterate through Objects
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    // Iterate Through Objects in SubGroup
    for (uint64_t objectId = params.startObject;
         objectId <= params.lastObjectInTrack;
         objectId += params.objectIncrement) {
      if (token.isCancellationRequested()) {
        // Instead of returning an error, callback->subscribeDone with error
        SubscribeDone done;
        done.requestID = sub.requestID;
        done.reasonPhrase = "Datagram Subscription Cancelled";
        done.statusCode = SubscribeDoneStatusCode::INTERNAL_ERROR;
        callback->subscribeDone(std::move(done));
        co_return;
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
      header.group = groupNum;
      header.id = objectId;
      header.extensions = Extensions(extensions, {});

      auto res = callback->datagram(header, std::move(objectPayload));
      if (res.hasError()) {
        // If sending datagram fails, callback->subscribeDone with error
        SubscribeDone done;
        done.requestID = sub.requestID;
        done.reasonPhrase = "Error Sending Datagram Objects";
        done.statusCode = SubscribeDoneStatusCode::INTERNAL_ERROR;
        callback->subscribeDone(std::move(done));
        co_return;
      }

      // Set Delay Based on Object Frequency
      co_await co_withExecutor(
          folly::getGlobalCPUExecutor(),
          folly::coro::sleep(
              std::chrono::milliseconds(params.objectFrequency)));
    }
  }

  co_return;
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
    co_return folly::makeUnexpected(error);
  }
  if (res.value().forwardingPreference == ForwardingPreference::DATAGRAM) {
    FetchError error;
    error.requestID = fetch.requestID;
    error.errorCode = FetchErrorCode::NOT_SUPPORTED;
    error.reasonPhrase =
        "Datagram Forwarding Preference is not supported for fetch";
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

folly::coro::Task<void> MoQTestServer::onFetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  // Make a MoQTestParams (Only valid params are passed through from fetch
  // function)
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &fetch.fullTrackName.trackNamespace);
  XCHECK(res.hasValue())
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

  co_return;
}

folly::coro::Task<void> MoQTestServer::fetchOneSubgroupPerGroup(
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
            Extensions(extensions, {}),
            false);
      } else {
        callback->endOfGroup(
            groupNum,
            0 /* subgroupId */,
            objectId,
            Extensions(extensions, {}),
            false);
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
            Extensions(extensions, {}),
            false);
      } else {
        callback->endOfGroup(
            groupNum, objectId, objectId, Extensions(extensions, {}), false);
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
            Extensions(extensions, {}),
            false);
      } else {
        callback->endOfGroup(
            groupNum, subgroupId, objectId, Extensions(extensions, {}), false);
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
