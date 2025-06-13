// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <moxygen/tools/moqperf/MoQPerfServer.h>

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
  writeLoop(callback).scheduleOn(session->getEventBase()).start();
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
    std::shared_ptr<TrackConsumer> trackConsumer) {
  auto beginSubgroupResult =
      trackConsumer->beginSubgroup(0, 0, kDefaultPriority);
  CHECK(beginSubgroupResult.hasValue()) << "Unable to create subgroup";
  auto subgroupConsumer = beginSubgroupResult.value();
  uint64_t objectId = 0;
  while (!cancellationSource_.isCancellationRequested()) {
    auto awaitResult = subgroupConsumer->awaitReadyToConsume();
    if (awaitResult.hasValue()) {
      co_await std::move(awaitResult.value());
    }
    auto data = folly::IOBuf::create(500);
    data->append(500);
    subgroupConsumer->object(objectId++, std::move(data));
  }
  // We get out of the while loop once we receive an UNSUBSCRIBE from
  // the peer.
}

} // namespace moxygen
