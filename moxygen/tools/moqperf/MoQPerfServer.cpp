/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <moxygen/tools/moqperf/MoQPerfServer.h>
#include <moxygen/tools/moqperf/MoQPerfUtils.h>

#include <utility>

namespace moxygen {

constexpr std::string_view kEndpointName = "blah";

MoQPerfServer::MoQPerfServer(std::string cert, std::string key)
    : MoQServer(std::move(cert), std::move(key), std::string(kEndpointName)) {}

folly::coro::Task<Publisher::SubscribeResult> MoQPerfServer::subscribe(
    SubscribeRequest subscribeRequest,
    std::shared_ptr<TrackConsumer> callback) {
  auto session = MoQSession::getRequestSession();
  auto alias = TrackAlias(subscribeRequest.requestID.value);
  callback->setTrackAlias(alias);
  co_withExecutor(session->getExecutor(), writeLoop(callback, subscribeRequest))
      .start();
  SubscribeOk ok{
      subscribeRequest.requestID,
      alias,
      std::chrono::milliseconds(0) /* never expires */,
      subscribeRequest.groupOrder,
      std::nullopt};
  return folly::coro::makeTask<SubscribeResult>(
      std::make_shared<PerfSubscriptionHandle>(ok, &cancellationSource_));
}

folly::coro::Task<Publisher::FetchResult> MoQPerfServer::fetch(
    Fetch fetchRequest,
    std::shared_ptr<FetchConsumer> callback) {
  CHECK(!requestId_.has_value()) << "Cannot get more than one fetch, as of now";
  auto session = MoQSession::getRequestSession();
  co_withExecutor(
      session->getExecutor(),

      writeLoopFetch(callback, fetchRequest))
      .start();
  FetchOk ok{
      fetchRequest.requestID,
      GroupOrder(fetchRequest.groupOrder),
      0,
      AbsoluteLocation{0, 0}};
  requestId_ = fetchRequest.requestID;
  return folly::coro::makeTask<FetchResult>(
      std::make_shared<PerfFetchHandle>(ok, &cancellationSource_));
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

folly::coro::Task<void> MoQPerfServer::writeLoopFetch(
    std::shared_ptr<FetchConsumer> fetchConsumer,
    Fetch req) {
  moxygen::MoQPerfParams params = moxygen::convertTrackNamespaceToMoQPerfParams(
      req.fullTrackName.trackNamespace);

  // Group number
  uint64_t group = 0;

  while (!cancellationSource_.isCancellationRequested()) {
    int objectIdStart = -1;
    for (uint64_t subgroup = 0; subgroup < params.numSubgroupsPerGroup;
         subgroup++) {
      int start = objectIdStart + 1;
      for (uint64_t objectId = start;
           objectId < params.numObjectsPerSubgroup + start;
           objectId++) {
        auto data = folly::IOBuf::create(params.objectSize);
        data->append(params.objectSize);
        auto awaitResult = fetchConsumer->awaitReadyToConsume();
        if (awaitResult.hasValue()) {
          co_await std::move(awaitResult.value());
        }
        fetchConsumer->object(group, subgroup, objectId, std::move(data));

        objectIdStart++;
      }
    }
    if (params.sendEndOfGroupMarkers) {
      // For End of group marker, create a final subgroup in group and call
      // endOfGroup
      fetchConsumer->endOfGroup(group, params.numSubgroupsPerGroup, 0);
    }
    group++;
  }

  // Get out of the while loop once we receive a FetchCancel
}

} // namespace moxygen
