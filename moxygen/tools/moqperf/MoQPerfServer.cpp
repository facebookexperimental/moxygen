// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <moxygen/tools/moqperf/MoQPerfServer.h>
#include <moxygen/tools/moqperf/MoQPerfUtils.h>

#include <utility>

namespace moxygen {

constexpr std::string_view kEndpointName = "blah";

MoQPerfServer::MoQPerfServer(
    uint16_t sourcePort,
    std::string cert,
    std::string key)
    : MoQServer(
          sourcePort,
          std::move(cert),
          std::move(key),
          std::string(kEndpointName)) {}

folly::coro::Task<Publisher::SubscribeResult> MoQPerfServer::subscribe(
    SubscribeRequest subscribeRequest,
    std::shared_ptr<TrackConsumer> callback) {
  auto session = MoQSession::getRequestSession();

  writeLoop(callback, subscribeRequest)
      .scheduleOn(session->getEventBase())
      .start();
  SubscribeOk ok{
      subscribeRequest.requestID,
      std::chrono::milliseconds(0) /* never expires */,
      subscribeRequest.groupOrder,
      folly::none,
      {}};
  return folly::coro::makeTask<SubscribeResult>(
      std::make_shared<PerfSubscriptionHandle>(ok, &cancellationSource_));
}

void MoQPerfServer::onNewSession(std::shared_ptr<MoQSession> clientSession) {
  clientSession->setPublishHandler(shared_from_this());
}

folly::coro::Task<void> MoQPerfServer::writeLoop(
    std::shared_ptr<TrackConsumer> trackConsumer,
    SubscribeRequest req) {
  moxygen::MoQPerfParams params = moxygen::convertTrackNamespaceToMoQPerfParams(
      req.fullTrackName.trackNamespace);

  // Group number
  uint64_t group = 0;

  while (!cancellationSource_.isCancellationRequested()) {
    for (uint64_t subgroup = 0; subgroup < params.numSubgroupsPerGroup;
         subgroup++) {
      auto beginSubgroupResult =
          trackConsumer->beginSubgroup(group, subgroup, kDefaultPriority);
      CHECK(beginSubgroupResult.hasValue())
          << "Unable to create subgroup with num - " << subgroup;
      auto subgroupConsumer = beginSubgroupResult.value();
      for (uint64_t objectId = 0; objectId < params.numObjectsPerSubgroup;
           objectId++) {
        auto awaitResult = subgroupConsumer->awaitReadyToConsume();
        if (awaitResult.hasValue()) {
          co_await std::move(awaitResult.value());
        }
        auto data = folly::IOBuf::create(params.objectSize);
        data->append(params.objectSize);
        subgroupConsumer->object(objectId++, std::move(data));
      }
    }
    if (params.sendEndOfGroupMarkers) {
      // For End of group marker, create a final subgroup in group and call
      // endOfGroup
      auto endMarkerSubgroupConsumer =
          (trackConsumer->beginSubgroup(
               group, params.numSubgroupsPerGroup, kDefaultPriority))
              .value();
      endMarkerSubgroupConsumer->endOfGroup(0);
    }
    group++;
  }
  // We get out of the while loop once we receive an UNSUBSCRIBE from
  // the peer.
}

} // namespace moxygen
