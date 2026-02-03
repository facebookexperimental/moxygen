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
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"

std::string kEndpointName = "/test";

namespace moxygen {

const int kDefaultExpires = 0;
const std::string kDefaultPublishDoneReason = "Testing";

void MoQTestSubscriptionHandle::unsubscribe() {
  cancelSource_.requestCancellation();
}

folly::coro::Task<folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>>
MoQTestSubscriptionHandle::subscribeUpdate(SubscribeUpdate update) {
  LOG(INFO) << "Received Subscribe Update";
  co_return folly::makeUnexpected(
      SubscribeUpdateError{
          update.requestID,
          SubscribeUpdateErrorCode::NOT_SUPPORTED,
          "Subscribe update not implemented"});
}

void MoQTestFetchHandle::fetchCancel() {
  cancelSource_.requestCancellation();
}

MoQTestServer::MoQTestServer(const std::string& cert, const std::string& key)
    : MoQServer(
          quic::samples::createFizzServerContextWithInsecureDefault(
              []() {
                std::vector<std::string> alpns = {"h3"};
                auto moqt = getDefaultMoqtProtocols(
                    true); // Always experimental for tests
                alpns.insert(alpns.end(), moqt.begin(), moqt.end());
                return alpns;
              }(),
              fizz::server::ClientAuthMode::None,
              cert,
              key),
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
  auto alias = TrackAlias(sub.requestID.value);
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
      std::nullopt};
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
  // Default PublishDone For Now

  PublishDone done;
  done.requestID = sub.requestID;
  done.statusCode = PublishDoneStatusCode::TRACK_ENDED;
  done.reasonPhrase = kDefaultPublishDoneReason;
  callback->publishDone(std::move(done));
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
            true);
      } else {
        subConsumer->endOfGroup(objectId);
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
  for (uint64_t groupNum = params.startGroup;
       groupNum <= params.lastGroupInTrack;
       groupNum += params.groupIncrement) {
    std::vector<std::shared_ptr<SubgroupConsumer>> subConsumers;
    if (params.startObject % 2 == 0 ||
        (params.objectsPerGroup > 1 && params.objectIncrement % 2 == 1)) {
      // we have at least one even object
      subConsumers.push_back(
          callback->beginSubgroup(groupNum, 0, kDefaultPriority).value());
    } else {
      subConsumers.push_back(nullptr);
    }

    if (params.startObject % 2 == 1 ||
        (params.objectsPerGroup > 1 && params.objectIncrement % 2 == 1)) {
      // we have at least one odd object
      subConsumers.push_back(
          callback->beginSubgroup(groupNum, 1, kDefaultPriority).value());
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
          params.testIntegerExtension, params.testVariableExtension);

      // If there are send end of group markers and j == lastObjectID, send
      // the end of group
      if (objectId < params.lastObjectInTrack ||
          !params.sendEndOfGroupMarkers) {
        // Begin Delivering Object With Payload
        int index = objectId % 2;
        LOG(INFO) << "Sending Object " << objectId << " to Subgroup " << index;
        std::string p = std::string(objectSize, 't');
        auto objectPayload = folly::IOBuf::copyBuffer(p);
        subConsumers[index]->object(
            objectId,
            std::move(objectPayload),
            Extensions(extensions, {}),
            false);

      } else {
        auto lastSubgroup = objectId % 2;
        LOG(INFO) << "Sending End of Group Marker to Subgroup " << lastSubgroup;
        subConsumers[lastSubgroup]->endOfGroup(objectId);

        // For case of only 1 object being sent
        if (subConsumers[1 - lastSubgroup]) {
          subConsumers[1 - lastSubgroup]->endOfSubgroup();
        }
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
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

folly::coro::Task<void> MoQTestServer::sendDatagram(
    SubscribeRequest sub,
    MoQTestParameters params,
    std::shared_ptr<TrackConsumer> callback) {
  auto alias = TrackAlias(sub.requestID.value);
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
        // Instead of returning an error, callback->publishDone with error
        PublishDone done;
        done.requestID = sub.requestID;
        done.reasonPhrase = "Datagram Subscription Cancelled";
        done.statusCode = PublishDoneStatusCode::INTERNAL_ERROR;
        callback->publishDone(std::move(done));
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
        // If sending datagram fails, callback->publishDone with error
        PublishDone done;
        done.requestID = sub.requestID;
        done.reasonPhrase = "Error Sending Datagram Objects";
        done.statusCode = PublishDoneStatusCode::INTERNAL_ERROR;
        callback->publishDone(std::move(done));
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
        callback->endOfGroup(groupNum, 0 /* subgroupId */, objectId, false);
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
        callback->endOfGroup(groupNum, objectId, objectId, false);
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
        callback->endOfGroup(groupNum, subgroupId, objectId, false);
      }

      // Set Delay Based on Object Frequency
      co_await folly::coro::sleep(
          std::chrono::milliseconds(params.objectFrequency));
    }
  }

  // Inform Consumer that fetch is completed
  callback->endOfFetch();
}

folly::coro::Task<void> MoQTestServer::doRelaySetup(
    const std::string& relayUrl,
    int32_t connectTimeout,
    int32_t transactionTimeout) {
  // Setup MoQ session on the client
  co_await relayClient_->setupMoQSession(
      std::chrono::milliseconds(connectTimeout),
      std::chrono::milliseconds(transactionTimeout),
      /*publishHandler=*/shared_from_this(),
      /*subscribeHandler=*/nullptr,
      quic::TransportSettings(),
      {});

  // Get the session
  relaySession_ =
      std::dynamic_pointer_cast<MoQRelaySession>(relayClient_->moqSession_);
  if (!relaySession_) {
    XLOG(ERR) << "Failed to get MoQRelaySession";
    co_return;
  }

  // Send PUBLISH_NAMESPACE for the base namespace "moq-test-00"
  PublishNamespace publishNamespace;
  publishNamespace.trackNamespace = TrackNamespace("moq-test-00", "/");

  auto publishNamespaceResult =
      co_await relaySession_->publishNamespace(publishNamespace);
  if (publishNamespaceResult.hasError()) {
    XLOG(ERR) << "Failed to publishNamespace namespace: "
              << publishNamespaceResult.error().reasonPhrase;
    co_return;
  }

  // Store publishNamespace handle to keep it alive
  publishNamespaceHandle_ = publishNamespaceResult.value();

  XLOG(INFO) << "Successfully published namespace 'moq-test-00' to relay at "
             << relayUrl;

  // Pass session to onNewSession to treat it like any other client
  onNewSession(relaySession_);

  co_return;
}

bool MoQTestServer::startRelayClient(
    const std::string& relayUrl,
    int32_t connectTimeout,
    int32_t transactionTimeout,
    bool useQuicTransport) {
  proxygen::URL url(relayUrl);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid relay url: " << relayUrl;
    return false;
  }

  // Get event base and create executor
  auto evb = getWorkerEvbs()[0];
  if (!moqEvb_) {
    moqEvb_ = std::make_shared<MoQFollyExecutorImpl>(evb);
  }

  // Create client connection with MoQRelaySession factory
  if (useQuicTransport) {
    relayClient_ = std::make_unique<MoQClient>(
        moqEvb_,
        url,
        MoQRelaySession::createRelaySessionFactory(),
        std::make_shared<
            test::InsecureVerifierDangerousDoNotUseInProduction>());
  } else {
    relayClient_ = std::make_unique<MoQWebTransportClient>(
        moqEvb_,
        url,
        MoQRelaySession::createRelaySessionFactory(),
        std::make_shared<
            test::InsecureVerifierDangerousDoNotUseInProduction>());
  }

  // Start async relay setup (schedule on evb, don't block)
  co_withExecutor(
      evb, doRelaySetup(relayUrl, connectTimeout, transactionTimeout))
      .start();

  return true;
}

} // namespace moxygen
