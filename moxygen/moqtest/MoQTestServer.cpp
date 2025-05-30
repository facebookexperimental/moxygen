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

MoQTestServer::MoQTestServer(uint16_t port)
    : MoQServer(port, kCert, kKey, kEndpointName) {}

folly::coro::Task<MoQSession::SubscribeResult> MoQTestServer::subscribe(
    SubscribeRequest sub,
    std::shared_ptr<TrackConsumer> callback) {
  LOG(INFO) << "Recieved Subscription";

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
  // Make a MoQTestParams
  auto res = moxygen::convertTrackNamespaceToMoqTestParam(
      &sub.fullTrackName.trackNamespace);
  if (res.hasError()) {
    SubscribeDone done;
    done.requestID = sub.requestID;
    done.reasonPhrase = "Invalid Parameters";
    done.statusCode = SubscribeDoneStatusCode::INTERNAL_ERROR;
    callback->subscribeDone(done);
    co_return;
  }
  MoQTestParameters params = res.value();

  // Publish Objects in Accordance to params

  // Publisher Delivery Timeout (To be implemented later)

  // Switch based on forwarding preference
  switch (params.forwardingPreference) {
    case (ForwardingPreference::ONE_SUBGROUP_PER_GROUP): {
      co_await sendObjectsForForwardPreferenceZero(params, callback);
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

folly::coro::Task<void> MoQTestServer::sendObjectsForForwardPreferenceZero(
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

} // namespace moxygen
