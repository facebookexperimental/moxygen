/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <moxygen/tools/moqperf/MoQPerfClient.h>
#include <moxygen/tools/moqperf/MoQPerfUtils.h>

namespace moxygen {

folly::Expected<folly::Unit, MoQPublishError>
MoQPerfClientFetchConsumer::object(
    uint64_t groupID,
    uint64_t subgroupID,
    uint64_t objectID,
    Payload payload,
    Extensions extensions,
    bool finFetch) {
  incrementFetchDataSent(payload->computeChainDataLength());
  return folly::Unit();
}

void MoQPerfClientFetchConsumer::incrementFetchDataSent(uint64_t amount) {
  fetchDataSent_ += amount;
}

uint64_t MoQPerfClientFetchConsumer::getFetchDataSent() {
  return fetchDataSent_;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQPerfClientFetchConsumer::objectNotExists(
    uint64_t groupID,
    uint64_t subgroupID,
    uint64_t objectID,
    Extensions extensions,
    bool finFetch) {
  return folly::Unit();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQPerfClientFetchConsumer::groupNotExists(
    uint64_t groupID,
    uint64_t subgroupID,
    Extensions extensions,
    bool finFetch) {
  return folly::Unit();
}

void MoQPerfClientFetchConsumer::checkpoint() {}

folly::Expected<folly::Unit, MoQPublishError>
MoQPerfClientFetchConsumer::beginObject(
    uint64_t groupID,
    uint64_t subgroupID,
    uint64_t objectID,
    uint64_t length,
    Payload initialPayload,
    Extensions extensions) {
  incrementFetchDataSent(initialPayload->computeChainDataLength());
  return folly::Unit();
}

folly::Expected<ObjectPublishStatus, MoQPublishError>
MoQPerfClientFetchConsumer::objectPayload(Payload payload, bool finSubgroup) {
  incrementFetchDataSent(payload->computeChainDataLength());
  return ObjectPublishStatus::DONE;
}
folly::Expected<folly::Unit, MoQPublishError>
MoQPerfClientFetchConsumer::endOfGroup(
    uint64_t groupID,
    uint64_t subgroupID,
    uint64_t objectID,
    Extensions extensions,
    bool finFetch) {
  return folly::Unit();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQPerfClientFetchConsumer::endOfTrackAndGroup(
    uint64_t groupID,
    uint64_t subgroupID,
    uint64_t objectID,
    Extensions extensions) {
  return folly::Unit();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQPerfClientFetchConsumer::endOfFetch() {
  return folly::Unit();
}

void MoQPerfClientFetchConsumer::reset(ResetStreamErrorCode error) {}

folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
MoQPerfClientFetchConsumer::awaitReadyToConsume() {
  return folly::makeSemiFuture<uint64_t>(0);
}

MoQPerfClient::MoQPerfClient(
    const folly::SocketAddress& peerAddr,
    folly::EventBase* evb,
    std::chrono::milliseconds connectTimeout,
    std::chrono::milliseconds transactionTimeout)
    : moqExecutor_(std::make_shared<MoQFollyExecutorImpl>(evb)),
      moqClient_(
          moqExecutor_,
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

  auto subscribeRequest = SubscribeRequest::make(
      /*fullTrackName=*/moxygen::FullTrackName({track, "blah"}),
      /*priority=*/0,
      /*groupOrder=*/GroupOrder::OldestFirst,
      /*forward=*/true,
      /*locType=*/LocationType::LargestObject,
      /*start=*/folly::none,
      /*endGroup=*/0,
      /*params=*/{});
  auto subscribeResult = co_await moqClient_.moqSession_->subscribe(
      subscribeRequest, trackConsumer);
  CHECK(subscribeResult.hasValue()) << "Issue with subscribing to peer";
  co_return subscribeResult;
}

folly::coro::Task<MoQSession::FetchResult> MoQPerfClient::fetch(
    std::shared_ptr<MoQPerfClientFetchConsumer> fetchConsumer,
    MoQPerfParams params) {
  // Validate the params
  if (!moxygen::validateMoQPerfParams(params)) {
    XLOG(ERR) << "Invalid MoQPerfParams";
    co_return folly::makeUnexpected(moxygen::FetchError{
        0, FetchErrorCode::INTERNAL_ERROR, "Client Parameters are Invalid"});
  }
  // Create a SubRequest with the created TrackNamespace in its fullTrackName
  TrackNamespace track = convertMoQPerfParamsToTrackNamespace(params);

  Fetch fetchRequest{
      0,
      moxygen::FullTrackName({track, "blah"}),
      moxygen::AbsoluteLocation{0, 0},
      moxygen::AbsoluteLocation{0, 0}};
  auto fetchResult =
      co_await moqClient_.moqSession_->fetch(fetchRequest, fetchConsumer);
  CHECK(fetchResult.hasValue()) << "Issue with fetching to peer";
  co_return fetchResult;
}

void MoQPerfClient::drain() {
  moqClient_.moqSession_->drain();
}

} // namespace moxygen
