// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <moxygen/tools/moqperf/MoQPerfClient.h>
#include <moxygen/tools/moqperf/MoQPerfUtils.h>
#include <functional>

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
    std::shared_ptr<MoQPerfClientTrackConsumer> trackConsumer,
    MoQPerfParams params) {
  // Validate the params
  if (!moxygen::validateMoQPerfParams(params)) {
    XLOG(ERR) << "Invalid MoQPerfParams";
    co_return folly::makeUnexpected(moxygen::SubscribeError{
        0,
        SubscribeErrorCode::INTERNAL_ERROR,
        "Client Parameters are Invalid"});
  }
  // Create a SubRequest with the created TrackNamespace in its fullTrackName
  TrackNamespace track = convertMoQPerfParamsToTrackNamespace(params);

  SubscribeRequest subscribeRequest{
      .requestID = 0,
      .trackAlias = 1,
      .fullTrackName = moxygen::FullTrackName({track, "blah"}),
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
