/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQProxy.h"

#include <folly/coro/Result.h>
#include <stdexcept>
#include <utility>
#include <variant>

#include "moxygen/MoQFilters.h"
#include "moxygen/MoQSession.h"
#include "moxygen/proxy/MoQProxyTrack.h"
#include "moxygen/relay/MoQCache.h"

namespace moxygen {

template <typename Result, typename Operation>
folly::coro::Task<folly::Expected<Result, RequestError>> MoQProxy::tryUpstreams(
    RequestID requestID,
    const FullTrackName& fullTrackName,
    const TrackRequestParameters& params,
    Operation operation) {
  RequestError failure{
      requestID, RequestErrorCode::INTERNAL_ERROR, "no upstream available"};
  auto setInternalFailure = [&](std::string reason) {
    failure = RequestError{
        requestID, RequestErrorCode::INTERNAL_ERROR, std::move(reason)};
  };

  for (size_t i = 0; i < upstreamProviders_.size(); ++i) {
    auto sessionResult = co_await folly::coro::co_awaitTry(
        upstreamProviders_[i]->getSession(
            fullTrackName, params, i + 1 < upstreamProviders_.size()));
    if (closed_) {
      co_return folly::makeUnexpected(
          RequestError{
              requestID, RequestErrorCode::GOING_AWAY, "proxy is closed"});
    }
    if (sessionResult.hasException() || sessionResult->hasError()) {
      setInternalFailure(
          sessionResult.hasException()
              ? sessionResult.exception().what().toStdString()
              : sessionResult->error().message);
      continue;
    }

    auto upstreamSession = std::move(sessionResult->value());
    if (!upstreamSession) {
      setInternalFailure("upstream provider returned a null session");
      continue;
    }
    auto result = co_await folly::coro::co_awaitTry(
        operation(std::move(upstreamSession)));
    if (result.hasException()) {
      setInternalFailure(result.exception().what().toStdString());
      continue;
    }
    if (result->hasValue()) {
      co_return std::move(result->value());
    }
    failure = std::move(result->error());
    failure.requestID = requestID;
  }
  co_return folly::makeUnexpected(std::move(failure));
}

// Defers forwarding until upstream selection and keeps aliases hop-local.
class MoQProxy::LateBoundPublishConsumer final : public TrackConsumerFilter {
 public:
  LateBoundPublishConsumer() : TrackConsumerFilter(nullptr) {}

  void bind(
      std::shared_ptr<TrackConsumer> consumer,
      std::shared_ptr<MoQSession> upstreamSession) {
    setDownstream(std::move(consumer));
    upstreamSession_ = std::move(upstreamSession);
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias) override {
    // We don't want to set the track alias on the upstream publish consumer
    // when the MoQ library sets the alias on the LateBoundPublishConsumer. The
    // track alias on the upstream publish consumer is set independently.
    return folly::unit;
  }

 private:
  std::shared_ptr<MoQSession> upstreamSession_;
};

std::shared_ptr<MoQProxy> MoQProxy::create(
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders) {
  return std::shared_ptr<MoQProxy>(new MoQProxy(std::move(upstreamProviders)));
}

MoQProxy::MoQProxy(
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders)
    : upstreamProviders_(std::move(upstreamProviders)),
      cache_(std::make_shared<MoQCache>()) {
  if (upstreamProviders_.empty()) {
    throw std::invalid_argument("MoQProxy requires upstream providers");
  }
  for (const auto& provider : upstreamProviders_) {
    if (!provider) {
      throw std::invalid_argument(
          "MoQProxy requires non-null upstream providers");
    }
  }
}

MoQProxy::~MoQProxy() {
  close();
}

folly::coro::Task<Publisher::SubscribeResult> MoQProxy::subscribe(
    SubscribeRequest subscribeRequest,
    std::shared_ptr<TrackConsumer> consumer) {
  auto self = shared_from_this();
  if (closed_) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::GOING_AWAY,
            "proxy is closed"});
  }

  auto downstreamSession = MoQSession::getRequestSession();
  auto track = getOrCreateTrack(subscribeRequest.fullTrackName);
  co_return co_await track->subscribe(
      std::move(subscribeRequest),
      std::move(consumer),
      std::move(downstreamSession));
}

folly::coro::Task<Publisher::FetchResult> MoQProxy::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  auto self = shared_from_this();
  if (closed_) {
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID, FetchErrorCode::GOING_AWAY, "proxy is closed"});
  }
  if (!std::holds_alternative<StandaloneFetch>(fetch.args)) {
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::NOT_SUPPORTED,
            "joining fetch is not supported"});
  }

  co_return co_await tryUpstreams<std::shared_ptr<Publisher::FetchHandle>>(
      fetch.requestID,
      fetch.fullTrackName,
      fetch.params,
      [this, fetch, consumer](std::shared_ptr<MoQSession> upstreamSession) {
        return cache_->fetch(fetch, consumer, std::move(upstreamSession));
      });
}

Subscriber::PublishResult MoQProxy::publish(
    PublishRequest publishRequest,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  if (closed_) {
    return folly::makeUnexpected(
        PublishError{
            publishRequest.requestID,
            PublishErrorCode::GOING_AWAY,
            "proxy is closed"});
  }

  auto consumer = std::make_shared<LateBoundPublishConsumer>();
  auto reply = forwardPublish(
      shared_from_this(),
      std::move(publishRequest),
      std::move(handle),
      consumer);

  return Subscriber::PublishConsumerAndReplyTask{
      std::move(consumer), std::move(reply), /*consumerReady=*/false};
}

MoQProxy::PublishReplyTask MoQProxy::forwardPublish(
    std::shared_ptr<MoQProxy> self,
    PublishRequest publishRequest,
    std::shared_ptr<Publisher::SubscriptionHandle> handle,
    std::shared_ptr<LateBoundPublishConsumer> consumer) {
  auto requestID = publishRequest.requestID;
  auto sessionResult = co_await folly::coro::co_awaitTry(
      self->upstreamProviders_.front()->getSession(
          publishRequest.fullTrackName,
          publishRequest.params,
          /*hasFallbackProvider=*/false));
  if (self->closed_) {
    co_return folly::makeUnexpected(
        PublishError{
            requestID, PublishErrorCode::GOING_AWAY, "proxy is closed"});
  }
  if (sessionResult.hasException() || sessionResult->hasError()) {
    auto reason = sessionResult.hasException()
        ? sessionResult.exception().what().toStdString()
        : std::move(sessionResult->error().message);
    co_return folly::makeUnexpected(
        PublishError{
            requestID, PublishErrorCode::INTERNAL_ERROR, std::move(reason)});
  }
  auto upstreamSession = std::move(sessionResult->value());
  if (!upstreamSession) {
    co_return folly::makeUnexpected(
        PublishError{
            requestID,
            PublishErrorCode::INTERNAL_ERROR,
            "upstream provider returned a null session"});
  }

  auto publishResult = upstreamSession->publish(publishRequest, handle);
  if (publishResult.hasError()) {
    auto error = std::move(publishResult.error());
    error.requestID = requestID;
    co_return folly::makeUnexpected(std::move(error));
  }
  if (!publishResult->consumer) {
    co_return folly::makeUnexpected(
        PublishError{
            requestID,
            PublishErrorCode::INTERNAL_ERROR,
            "upstream returned a null publish consumer"});
  }
  auto response = std::move(publishResult.value());
  consumer->bind(std::move(response.consumer), std::move(upstreamSession));

  auto replyResult =
      co_await folly::coro::co_awaitTry(std::move(response.reply));
  if (self->closed_) {
    co_return folly::makeUnexpected(
        PublishError{
            requestID, PublishErrorCode::GOING_AWAY, "proxy is closed"});
  }
  if (replyResult.hasException() || replyResult->hasError()) {
    auto error = replyResult.hasException()
        ? PublishError{
              requestID,
              PublishErrorCode::INTERNAL_ERROR,
              replyResult.exception().what().toStdString()}
        : std::move(replyResult->error());
    error.requestID = requestID;
    co_return folly::makeUnexpected(std::move(error));
  }
  auto ok = std::move(replyResult->value());
  ok.requestID = requestID;
  co_return ok;
}

std::shared_ptr<MoQProxyTrack> MoQProxy::getOrCreateTrack(
    const FullTrackName& fullTrackName) {
  auto it = tracks_.find(fullTrackName);
  if (it != tracks_.end()) {
    return it->second;
  }

  auto track = MoQProxyTrack::create(fullTrackName, upstreamProviders_);
  track->setCallback(shared_from_this());
  tracks_.emplace(fullTrackName, track);
  return track;
}

void MoQProxy::onNoSubscribers(MoQProxyTrack* track) {
  auto it = tracks_.find(track->fullTrackName());
  if (it != tracks_.end() && it->second.get() == track) {
    tracks_.erase(it);
  }
}

void MoQProxy::close() {
  if (closed_) {
    return;
  }
  closed_ = true;

  auto tracks = std::move(tracks_);
  for (auto& [_, track] : tracks) {
    track->setCallback({});
    track->close();
  }
  cache_->clear();
}

} // namespace moxygen
