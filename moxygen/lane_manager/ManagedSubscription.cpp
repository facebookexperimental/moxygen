/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/ManagedSubscription.h"

#include <utility>

namespace moxygen::lane_manager {

ManagedSubscription::ManagedSubscription(
    std::shared_ptr<Publisher> upstream,
    SubscribeRequest request,
    std::shared_ptr<TrackConsumer> downstreamLaneConsumer)
    : TrackConsumerFilter(std::move(downstreamLaneConsumer)),
      upstream_(std::move(upstream)),
      requestTemplate_(std::move(request)) {}

ManagedSubscription::~ManagedSubscription() {
  unsubscribe();
}

folly::coro::Task<ManagedSubscription::SubscribeResult>
ManagedSubscription::subscribe() {
  if (stopped_) {
    co_return folly::makeUnexpected(
        SubscribeError{
            requestTemplate_.requestID,
            RequestErrorCode::CANCELLED,
            "stopped"});
  }
  if (subscription_) {
    co_return subscription_->subscribeOk();
  }
  auto subscribed =
      co_await upstream_->subscribe(requestTemplate_, shared_from_this());
  if (subscribed.hasError()) {
    co_return folly::makeUnexpected(std::move(subscribed.error()));
  }
  if (stopped_) {
    subscribed.value()->unsubscribe();
    co_return folly::makeUnexpected(
        SubscribeError{
            requestTemplate_.requestID,
            RequestErrorCode::CANCELLED,
            "stopped"});
  }
  subscription_ = std::move(subscribed.value());
  co_return subscription_->subscribeOk();
}

void ManagedSubscription::unsubscribe() {
  stopped_ = true;
  if (auto subscription = std::exchange(subscription_, nullptr)) {
    subscription->unsubscribe();
  }
}

folly::Expected<folly::Unit, MoQPublishError> ManagedSubscription::publishDone(
    PublishDone pubDone) {
  auto self = shared_from_this();
  auto result = downstream_->publishDone(std::move(pubDone));
  subscription_.reset();
  return result;
}

} // namespace moxygen::lane_manager
