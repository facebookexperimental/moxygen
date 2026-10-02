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

template <
    typename Result,
    typename Operation,
    typename Cleanup,
    typename... Args>
folly::coro::Task<folly::Expected<Result, RequestError>> MoQProxy::tryUpstreams(
    RequestID requestID,
    const FullTrackName& fullTrackName,
    const TrackRequestParameters& params,
    Operation operation,
    Cleanup cleanup,
    Args... args) {
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
        operation(std::move(upstreamSession), args...));
    if (closed_) {
      if (result.hasValue() && result->hasValue()) {
        cleanup(result->value());
      }
      co_return folly::makeUnexpected(
          RequestError{
              requestID, RequestErrorCode::GOING_AWAY, "proxy is closed"});
    }
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

  const auto reqCtx = MoQSession::getRequestContext();
  auto track = getOrCreateTrack(subscribeRequest.fullTrackName);
  co_return co_await track->subscribe(
      std::move(subscribeRequest),
      std::move(consumer),
      MoQProxyTrack::DownstreamPeer{reqCtx.sessionId, reqCtx.version});
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
      },
      [](std::shared_ptr<Publisher::FetchHandle>& handle) {
        if (handle) {
          handle->fetchCancel();
        }
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
  if (!handle) {
    return folly::makeUnexpected(
        PublishError{
            publishRequest.requestID,
            PublishErrorCode::INTERNAL_ERROR,
            "publish handle is required"});
  }

  auto downstreamSessionId = MoQSession::getRequestContext().sessionId;
  auto consumer = std::make_shared<LateBoundPublishConsumer>();
  auto reply = forwardPublish(
      shared_from_this(),
      std::move(publishRequest),
      std::move(handle),
      consumer,
      downstreamSessionId);

  return Subscriber::PublishConsumerAndReplyTask{
      std::move(consumer), std::move(reply), /*consumerReady=*/false};
}

MoQProxy::PublishReplyTask MoQProxy::forwardPublish(
    std::shared_ptr<MoQProxy> self,
    PublishRequest publishRequest,
    std::shared_ptr<Publisher::SubscriptionHandle> handle,
    std::shared_ptr<LateBoundPublishConsumer> consumer,
    SessionId downstreamSessionId) {
  auto requestID = publishRequest.requestID;
  auto attemptResult = co_await self->tryUpstreams<PublishAttempt>(
      requestID,
      publishRequest.fullTrackName,
      publishRequest.params,
      &MoQProxy::publishToUpstream,
      &MoQProxy::cancelPublishAttempt,
      downstreamSessionId,
      publishRequest,
      handle);
  if (attemptResult.hasError()) {
    co_return folly::makeUnexpected(std::move(attemptResult.error()));
  }

  auto attempt = std::move(attemptResult.value());
  if (self->closed_) {
    cancelPublishAttempt(attempt);
    co_return folly::makeUnexpected(
        PublishError{
            requestID, PublishErrorCode::GOING_AWAY, "proxy is closed"});
  }
  consumer->bind(std::move(attempt.consumer), std::move(attempt.session));
  auto ok = std::move(attempt.ok);
  ok.requestID = requestID;
  co_return ok;
}

folly::coro::Task<MoQProxy::PublishAttemptResult> MoQProxy::publishToUpstream(
    std::shared_ptr<MoQSession> upstreamSession,
    SessionId downstreamSessionId,
    PublishRequest publishRequest,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  if (upstreamSession->sessionId() == downstreamSessionId) {
    co_return folly::makeUnexpected(
        RequestError{
            publishRequest.requestID,
            RequestErrorCode::INTERNAL_ERROR,
            "upstream and downstream sessions are the same"});
  }

  auto result = upstreamSession->publish(publishRequest, handle);
  if (result.hasError()) {
    co_return folly::makeUnexpected(std::move(result.error()));
  }
  if (!result->consumer) {
    co_return folly::makeUnexpected(
        RequestError{
            publishRequest.requestID,
            RequestErrorCode::INTERNAL_ERROR,
            "upstream returned a null publish consumer"});
  }
  auto response = std::move(result.value());
  auto replyResult =
      co_await folly::coro::co_awaitTry(std::move(response.reply));
  if (replyResult.hasException() || replyResult->hasError()) {
    auto error = replyResult.hasException()
        ? PublishError{
              publishRequest.requestID,
              PublishErrorCode::INTERNAL_ERROR,
              replyResult.exception().what().toStdString()}
        : std::move(replyResult->error());
    error.requestID = publishRequest.requestID;
    co_return folly::makeUnexpected(std::move(error));
  }
  co_return PublishAttempt{
      std::move(replyResult->value()),
      std::move(response.consumer),
      std::move(upstreamSession)};
}

void MoQProxy::cancelPublishAttempt(PublishAttempt& attempt) {
  if (attempt.consumer) {
    (void)attempt.consumer->publishDone(
        PublishDone{
            attempt.ok.requestID,
            PublishDoneStatusCode::GOING_AWAY,
            0,
            "proxy is closed"});
  }
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
