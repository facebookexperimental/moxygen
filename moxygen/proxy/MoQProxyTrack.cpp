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

folly::Unexpected<SubscribeError> makeUpstreamFailure(
    const SubscribeRequest& request,
    SubscribeErrorCode errorCode,
    std::string reasonPhrase) {
  return folly::makeUnexpected(
      SubscribeError{request.requestID, errorCode, std::move(reasonPhrase)});
}

folly::Unexpected<SubscribeError> makeUpstreamFailure(
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

class MoQProxyTrack::ForwarderCallback final : public MoQForwarder::Callback {
 public:
  explicit ForwarderCallback(std::weak_ptr<MoQProxyTrack> track)
      : track_(std::move(track)) {}

  void onEmpty(MoQForwarder*) override {
    if (auto track = track_.lock()) {
      track->onForwarderEmpty();
    }
  }

  void onPublishDone(MoQForwarder*) override {
    if (auto track = track_.lock()) {
      track->onUpstreamPublishDone();
    }
  }

 private:
  std::weak_ptr<MoQProxyTrack> track_;
};

std::shared_ptr<MoQProxyTrack> MoQProxyTrack::create(
    FullTrackName fullTrackName,
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders) {
  auto track = std::shared_ptr<MoQProxyTrack>(new MoQProxyTrack(
      std::move(fullTrackName), std::move(upstreamProviders)));
  track->forwarder_->setCallback(std::make_shared<ForwarderCallback>(track));
  return track;
}

MoQProxyTrack::MoQProxyTrack(
    FullTrackName fullTrackName,
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders)
    : fullTrackName_(std::move(fullTrackName)),
      upstreamProviders_(std::move(upstreamProviders)),
      forwarder_(std::make_shared<MoQForwarder>(fullTrackName_)) {
  XCHECK(!upstreamProviders_.empty())
      << "MoQProxyTrack requires upstream providers";
  for (const auto& provider : upstreamProviders_) {
    XCHECK(provider) << "MoQProxyTrack requires non-null upstream providers";
  }
}

MoQProxyTrack::~MoQProxyTrack() {
  forwarder_->setCallback(nullptr);
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
  if (state_ == State::IDLE) {
    co_return co_await handleFirstSubscription(
        std::move(subscribeRequest),
        std::move(consumer),
        std::move(downstreamSession));
  }

  if (state_ == State::CONNECTING) {
    co_await upstreamSubscriptionReadyPromise_.getFuture();
  }

  if (upstreamSubscriptionFailure_) {
    co_return folly::makeUnexpected(makeSubscribeError(
        subscribeRequest.requestID, *upstreamSubscriptionFailure_));
  }
  if (state_ != State::READY) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::DOES_NOT_EXIST,
            "upstream subscription is no longer available"});
  }

  co_return addSubscriber(
      subscribeRequest, std::move(consumer), std::move(downstreamSession));
}

folly::coro::Task<Publisher::SubscribeResult>
MoQProxyTrack::handleFirstSubscription(
    SubscribeRequest subscribeRequest,
    std::shared_ptr<TrackConsumer> consumer,
    std::shared_ptr<MoQSession> downstreamSession) {
  state_ = State::CONNECTING;
  auto subscriberResult =
      addSubscriber(subscribeRequest, std::move(consumer), downstreamSession);
  if (subscriberResult.hasError()) {
    // If the first subscriber failed, set the state to CLOSED and notify
    // the parent so that it can clean up this MoQProxyTrack (and potentially
    // create a new one if and when another subscription to this track is
    // made)
    state_ = State::CLOSED;
    completeUpstreamEstablishment(
        UpstreamEstablishmentFailure{
            subscriberResult.error().errorCode,
            subscriberResult.error().reasonPhrase});
    notifyNoSubscribers();
    co_return subscriberResult;
  }

  auto subscriber = subscriberResult.value();
  auto failure =
      co_await establishUpstream(subscribeRequest, downstreamSession);
  if (failure) {
    if (!upstreamSubscriptionFailure_) {
      completeUpstreamEstablishment(
          UpstreamEstablishmentFailure{
              failure->errorCode,
              failure->reasonPhrase,
          });
    }
    auto returnedFailure = upstreamSubscriptionFailure_
        ? makeSubscribeError(
              subscribeRequest.requestID, *upstreamSubscriptionFailure_)
        : std::move(*failure);
    // Roll back the subscriber registered before upstream establishment.
    subscriber->unsubscribe();
    co_return folly::makeUnexpected(std::move(returnedFailure));
  }
  auto downstreamHandle =
      std::dynamic_pointer_cast<DownstreamSubscriptionHandle>(subscriber);
  XCHECK(downstreamHandle);
  if (auto largest = forwarder_->largest()) {
    downstreamHandle->updateLargest(*largest);
  }
  downstreamHandle->refreshSubscribeOk();
  state_ = State::READY;
  completeUpstreamEstablishment(std::nullopt);
  co_return subscriber;
}

Publisher::SubscribeResult MoQProxyTrack::addSubscriber(
    const SubscribeRequest& subscribeRequest,
    std::shared_ptr<TrackConsumer> consumer,
    std::shared_ptr<MoQSession> downstreamSession) {
  if (forwarder_->largest() &&
      subscribeRequest.locType == LocationType::AbsoluteRange &&
      subscribeRequest.endGroup < forwarder_->largest()->group) {
    return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::INVALID_RANGE,
            "range is no longer available"});
  }

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
  std::optional<SubscribeError> lastFailure;
  for (size_t providerIndex = 0; providerIndex < upstreamProviders_.size();
       ++providerIndex) {
    const auto& upstreamProvider = upstreamProviders_[providerIndex];
    const bool hasFallbackProvider =
        providerIndex + 1 < upstreamProviders_.size();
    auto result = co_await establishWithProvider(
        upstreamProvider,
        subscribeRequest,
        downstreamSession,
        hasFallbackProvider);
    if (result.hasError()) {
      if (state_ != State::CONNECTING) {
        co_return std::move(result.error());
      }
      lastFailure = std::move(result.error());
      continue;
    }

    auto establishedUpstream = std::move(result.value());
    const auto& subscribeOk = establishedUpstream.handle->subscribeOk();
    if (subscribeOk.largest) {
      forwarder_->updateLargest(
          subscribeOk.largest->group, subscribeOk.largest->object);
    }
    forwarder_->setExtensions(subscribeOk.extensions);
    upstreamSession_ = std::move(establishedUpstream.session);
    upstreamHandle_ = std::move(establishedUpstream.handle);
    co_return std::nullopt;
  }
  co_return lastFailure;
}

folly::coro::Task<MoQProxyTrack::UpstreamEstablishmentResult>
MoQProxyTrack::establishWithProvider(
    const std::shared_ptr<MoQUpstreamProvider>& upstreamProvider,
    const SubscribeRequest& subscribeRequest,
    const std::shared_ptr<MoQSession>& downstreamSession,
    bool hasFallbackProvider) {
  auto sessionResult =
      co_await folly::coro::co_awaitTry(upstreamProvider->getSession(
          fullTrackName_, subscribeRequest.params, hasFallbackProvider));
  if (state_ != State::CONNECTING) {
    co_return makeUpstreamFailure(
        subscribeRequest,
        SubscribeErrorCode::CANCELLED,
        "proxy track closed while establishing the upstream");
  }
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
  upstreamRequest.forward = forwarder_->numForwardingSubscribers() > 0;

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
  if (state_ != State::CONNECTING) {
    if (subscribeResult.hasValue() && subscribeResult->hasValue()) {
      if (auto handle = std::move(subscribeResult->value())) {
        handle->unsubscribe();
      }
    }
    co_return makeUpstreamFailure(
        subscribeRequest,
        SubscribeErrorCode::CANCELLED,
        "proxy track closed while establishing the subscription");
  }
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

  co_return EstablishedUpstream{
      .session = std::move(upstreamSession),
      .handle = std::move(upstreamHandle),
  };
}

SubscribeError MoQProxyTrack::makeSubscribeError(
    RequestID requestID,
    const UpstreamEstablishmentFailure& failure) const {
  return SubscribeError{requestID, failure.errorCode, failure.reasonPhrase};
}

void MoQProxyTrack::completeUpstreamEstablishment(
    std::optional<UpstreamEstablishmentFailure> failure) {
  if (upstreamSubscriptionPromiseResolved_) {
    return;
  }
  upstreamSubscriptionPromiseResolved_ = true;
  upstreamSubscriptionFailure_ = std::move(failure);
  upstreamSubscriptionReadyPromise_.setValue(folly::unit);
}

void MoQProxyTrack::onForwarderEmpty() {
  if (state_ == State::CONNECTING) {
    completeUpstreamEstablishment(
        UpstreamEstablishmentFailure{
            SubscribeErrorCode::CANCELLED,
            "proxy track became empty while establishing the upstream"});
  }
  state_ = State::CLOSED;
  if (auto handle = std::move(upstreamHandle_)) {
    handle->unsubscribe();
  }
  upstreamSession_.reset();
  notifyNoSubscribers();
}

void MoQProxyTrack::onUpstreamPublishDone() {
  upstreamHandle_.reset();
  upstreamSession_.reset();
  if (state_ == State::CONNECTING) {
    completeUpstreamEstablishment(
        UpstreamEstablishmentFailure{
            SubscribeErrorCode::DOES_NOT_EXIST,
            "upstream ended while establishing the subscription"});
  }
  if (state_ != State::CLOSED) {
    state_ = State::DRAINING;
  }
}

void MoQProxyTrack::notifyNoSubscribers() {
  if (noSubscribersNotificationSent_) {
    return;
  }
  noSubscribersNotificationSent_ = true;
  if (auto callback = callback_.lock()) {
    callback->onNoSubscribers(this);
  }
}

void MoQProxyTrack::close() {
  if (state_ == State::CLOSED) {
    return;
  }
  auto self = shared_from_this();
  if (state_ == State::CONNECTING) {
    completeUpstreamEstablishment(
        UpstreamEstablishmentFailure{
            SubscribeErrorCode::GOING_AWAY, "proxy track closed"});
  }
  state_ = State::DRAINING;
  if (auto handle = std::move(upstreamHandle_)) {
    handle->unsubscribe();
  }
  upstreamSession_.reset();

  if (forwarder_->empty()) {
    state_ = State::CLOSED;
    notifyNoSubscribers();
    return;
  }
  forwarder_->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::SESSION_CLOSED,
          0,
          "proxy track closed"});
}

} // namespace moxygen
