/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQProxyTrack.h"

#include <utility>

#include "moxygen/MoQSession.h"
#include "moxygen/relay/MoQForwarder.h"

namespace moxygen {

class MoQProxyTrack::DownstreamSubscriptionHandle final
    : public SubscriptionHandle {
 public:
  explicit DownstreamSubscriptionHandle(
      std::shared_ptr<MoQForwarder::Subscriber> subscriber)
      : SubscriptionHandle(subscriber->subscribeOk()),
        subscriber_(std::move(subscriber)) {}

  ~DownstreamSubscriptionHandle() override {
    unsubscribe();
  }

  void unsubscribe() override {
    if (auto subscriber = std::exchange(subscriber_, nullptr)) {
      subscriber->unsubscribe();
    }
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate requestUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            requestUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "request updates are not supported"});
  }

 private:
  std::shared_ptr<MoQForwarder::Subscriber> subscriber_;
};

std::shared_ptr<MoQProxyTrack> MoQProxyTrack::create(
    FullTrackName fullTrackName) {
  return std::shared_ptr<MoQProxyTrack>(
      new MoQProxyTrack(std::move(fullTrackName)));
}

MoQProxyTrack::MoQProxyTrack(FullTrackName fullTrackName)
    : fullTrackName_(std::move(fullTrackName)),
      forwarder_(std::make_shared<MoQForwarder>(fullTrackName_)) {}

MoQProxyTrack::~MoQProxyTrack() {
  if (!forwarder_->empty()) {
    forwarder_->publishDone(
        PublishDone{
            RequestID(0),
            PublishDoneStatusCode::SESSION_CLOSED,
            0,
            "proxy track destroyed"});
  }
}

folly::coro::Task<Publisher::SubscribeResult> MoQProxyTrack::subscribe(
    SubscribeRequest subscribeRequest,
    std::shared_ptr<TrackConsumer> consumer,
    std::shared_ptr<MoQSession> downstreamSession) {
  auto self = shared_from_this();
  if (subscribeRequest.fullTrackName != fullTrackName_) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::DOES_NOT_EXIST,
            "track name does not match proxy track"});
  }
  if (!consumer || !downstreamSession) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "subscriber session and consumer are required"});
  }
  if (subscriptionStarted_) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::DUPLICATE_SUBSCRIPTION,
            "proxy track already has a subscription"});
  }

  subscriptionStarted_ = true;
  auto subscriberResult =
      addSubscriber(subscribeRequest, std::move(consumer), downstreamSession);
  if (subscriberResult.hasError()) {
    subscriptionStarted_ = false;
  }
  co_return subscriberResult;
}

Publisher::SubscribeResult MoQProxyTrack::addSubscriber(
    const SubscribeRequest& subscribeRequest,
    std::shared_ptr<TrackConsumer> consumer,
    std::shared_ptr<MoQSession> downstreamSession) {
  auto subscriber = forwarder_->addSubscriber(
      std::move(downstreamSession), subscribeRequest, std::move(consumer));
  if (!subscriber) {
    return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "failed to add subscriber"});
  }
  return std::make_shared<DownstreamSubscriptionHandle>(std::move(subscriber));
}

} // namespace moxygen
