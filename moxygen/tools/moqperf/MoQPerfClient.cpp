// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <moxygen/tools/moqperf/MoQPerfClient.h>

namespace moxygen {

MoQPerfClient::MoQPerfClient(
    const folly::SocketAddress& peerAddr,
    folly::EventBase* evb,
    std::chrono::milliseconds connectTimeout,
    std::chrono::milliseconds transactionTimeout)
    : moqClient_(
          evb,
          proxygen::URL("", peerAddr.getIPAddress().str(), peerAddr.getPort())),
      connectTimeout_(connectTimeout),
      transactionTimeout_(transactionTimeout) {}

folly::coro::Task<void> MoQPerfClient::connect() {
  return moqClient_.setupMoQSession(
      connectTimeout_, transactionTimeout_, nullptr, shared_from_this());
}

folly::coro::Task<MoQSession::SubscribeResult> MoQPerfClient::subscribe(
    std::shared_ptr<MoQPerfClientTrackConsumer> trackConsumer) {
  // TODO: For now, we're just providing an arbitrary track namespace, but later
  // on, we'll want to serialize parameters (e.g. object size, objects per
  // subgroup, etc.)
  SubscribeRequest subscribeRequest{
      .requestID = 0,
      .trackAlias = 1,
      .fullTrackName =
          moxygen::FullTrackName({{TrackNamespace("blah", "/")}, "blah"}),
      .priority = 0,
      .groupOrder = GroupOrder::OldestFirst,
      .forward = true,
      .locType = LocationType::LatestObject,
      .start = folly::none,
      .endGroup = 0,
      .params = {}};
  auto subscribeResult = co_await moqClient_.moqSession_->subscribe(
      subscribeRequest, trackConsumer);
  CHECK(subscribeResult.hasValue()) << "Issue with subscribing to peer";
  co_return subscribeResult;
}

void MoQPerfClient::drain() {
  moqClient_.moqSession_->drain();
}

} // namespace moxygen
