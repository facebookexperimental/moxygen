/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQProxyTrack.h"

#include <fmt/core.h>
#include <folly/coro/Result.h>
#include <utility>

#include "moxygen/MoQSession.h"
#include "moxygen/relay/MoQForwarder.h"

namespace moxygen {
namespace {

SubscribeError makeUpstreamFailure(
    const SubscribeRequest& request,
    SubscribeErrorCode errorCode,
    std::string reasonPhrase) {
  return SubscribeError{request.requestID, errorCode, std::move(reasonPhrase)};
}

SubscribeError makeUpstreamFailure(
    const SubscribeRequest& request,
    std::string reasonPhrase) {
  return makeUpstreamFailure(
      request, SubscribeErrorCode::INTERNAL_ERROR, std::move(reasonPhrase));
}

} // namespace

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

  // Called after upstream SUBSCRIBE_OK to propagate its largest object
  // location.
  void updateLargest(AbsoluteLocation largest) {
    if (subscriber_) {
      subscriber_->updateLargest(largest);
    }
  }

  // Called before returning downstream to copy the updated SUBSCRIBE_OK.
  void refreshSubscribeOk() {
    if (subscriber_) {
      setSubscribeOk(subscriber_->subscribeOk());
    }
  }

 private:
  std::shared_ptr<MoQForwarder::Subscriber> subscriber_;
};

std::shared_ptr<MoQProxyTrack> MoQProxyTrack::create(
    FullTrackName fullTrackName,
    std::shared_ptr<MoQUpstreamProvider> upstreamProvider) {
  return std::shared_ptr<MoQProxyTrack>(
      new MoQProxyTrack(std::move(fullTrackName), std::move(upstreamProvider)));
}

MoQProxyTrack::MoQProxyTrack(
    FullTrackName fullTrackName,
    std::shared_ptr<MoQUpstreamProvider> upstreamProvider)
    : fullTrackName_(std::move(fullTrackName)),
      upstreamProvider_(std::move(upstreamProvider)),
      forwarder_(std::make_shared<MoQForwarder>(fullTrackName_)) {
  XCHECK(upstreamProvider_) << "MoQProxyTrack requires an upstream provider";
}

MoQProxyTrack::~MoQProxyTrack() {
  if (auto handle = std::move(upstreamHandle_)) {
    handle->unsubscribe();
  }
  upstreamSession_.reset();
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
    co_return subscriberResult;
  }

  auto subscriber = subscriberResult.value();
  auto failure =
      co_await establishUpstream(subscribeRequest, downstreamSession);
  if (failure) {
    subscriptionStarted_ = false;
    // Roll back the subscriber registered before upstream establishment.
    subscriber->unsubscribe();
    co_return folly::makeUnexpected(std::move(*failure));
  }

  auto downstreamHandle =
      std::dynamic_pointer_cast<DownstreamSubscriptionHandle>(subscriber);
  XCHECK(downstreamHandle);
  if (auto largest = forwarder_->largest()) {
    downstreamHandle->updateLargest(*largest);
  }
  downstreamHandle->refreshSubscribeOk();
  co_return subscriber;
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

folly::coro::Task<std::optional<SubscribeError>>
MoQProxyTrack::establishUpstream(
    const SubscribeRequest& subscribeRequest,
    const std::shared_ptr<MoQSession>& downstreamSession) {
  auto sessionResult =
      co_await folly::coro::co_awaitTry(upstreamProvider_->getSession(
          fullTrackName_, subscribeRequest.params, false));
  if (sessionResult.hasException()) {
    co_return makeUpstreamFailure(
        subscribeRequest,
        fmt::format(
            "upstream session failed: {}",
            sessionResult.exception().what().toStdString()));
  }
  if (sessionResult->hasError()) {
    co_return makeUpstreamFailure(
        subscribeRequest,
        fmt::format(
            "upstream session failed: {}", sessionResult->error().message));
  }

  auto upstreamSession = std::move(sessionResult->value());
  if (!upstreamSession) {
    co_return makeUpstreamFailure(
        subscribeRequest, "upstream provider returned a null session");
  }
  if (upstreamSession == downstreamSession) {
    co_return makeUpstreamFailure(
        subscribeRequest, "upstream and downstream sessions are the same");
  }

  auto upstreamRequest = subscribeRequest;
  upstreamRequest.priority = kDefaultPriority;
  upstreamRequest.groupOrder = GroupOrder::Default;
  upstreamRequest.locType = LocationType::LargestObject;

  auto downstreamVersion = downstreamSession->getNegotiatedVersion();
  auto upstreamVersion = upstreamSession->getNegotiatedVersion();
  if ((downstreamVersion && getDraftMajorVersion(*downstreamVersion) >= 18) ||
      (upstreamVersion && getDraftMajorVersion(*upstreamVersion) >= 18)) {
    // TrackRequestParamKey::RENDEZVOUS_TIMEOUT changes meaning across
    // versions. The rendezvous timeout was introduced in draft 18, and isn't
    // propagated end-to-end (it's one hop only). In versions below 18,
    // TrackRequestParamKey::RENDEZVOUS_TIMEOUT means MAX_CACHE_DURATION.
    upstreamRequest.params.eraseAllParamsOfType(
        TrackRequestParamKey::RENDEZVOUS_TIMEOUT);
  }

  auto subscribeResult = co_await folly::coro::co_awaitTry(
      upstreamSession->subscribe(std::move(upstreamRequest), forwarder_));
  if (subscribeResult.hasException()) {
    co_return makeUpstreamFailure(
        subscribeRequest,
        fmt::format(
            "upstream subscribe failed: {}",
            subscribeResult.exception().what().toStdString()));
  }
  if (subscribeResult->hasError()) {
    co_return makeUpstreamFailure(
        subscribeRequest,
        subscribeResult->error().errorCode,
        fmt::format(
            "upstream subscribe failed: {}",
            subscribeResult->error().reasonPhrase));
  }

  auto upstreamHandle = std::move(subscribeResult->value());
  if (!upstreamHandle || !upstreamHandle->hasSubscribeOk()) {
    co_return makeUpstreamFailure(
        subscribeRequest, "upstream subscribe returned an invalid handle");
  }

  const auto& subscribeOk = upstreamHandle->subscribeOk();
  if (subscribeOk.largest) {
    forwarder_->updateLargest(
        subscribeOk.largest->group, subscribeOk.largest->object);
  }
  forwarder_->setExtensions(subscribeOk.extensions);
  upstreamSession_ = std::move(upstreamSession);
  upstreamHandle_ = std::move(upstreamHandle);
  co_return std::nullopt;
}

} // namespace moxygen
