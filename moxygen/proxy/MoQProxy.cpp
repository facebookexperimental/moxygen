/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQProxy.h"

#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Invoke.h>
#include <folly/coro/Result.h>
#include <folly/coro/WithCancellation.h>
#include <stdexcept>
#include <utility>
#include <variant>

#include "moxygen/MoQFilters.h"
#include "moxygen/MoQSession.h"
#include "moxygen/proxy/MoQJoinReplay.h"
#include "moxygen/proxy/MoQProxyTrack.h"

namespace moxygen {
namespace {

FetchError
fetchFailure(RequestID requestID, FetchErrorCode code, std::string reason) {
  return FetchError{requestID, code, std::move(reason)};
}

// The standalone wire form of an exclusive end: an end object of zero asks
// for all of the end group.
AbsoluteLocation toWireFetchEnd(AbsoluteLocation exclusiveEnd) {
  if (exclusiveEnd.object != 0) {
    return exclusiveEnd;
  }
  return AbsoluteLocation{exclusiveEnd.group - 1, 0};
}

// Forwards every callback and records whether any response data, status or
// framing reached the downstream consumer.
class ExposureTrackingFetchConsumer final : public FetchConsumer {
 public:
  explicit ExposureTrackingFetchConsumer(
      std::shared_ptr<FetchConsumer> consumer)
      : consumer_(std::move(consumer)) {}

  bool exposed() const {
    return exposed_;
  }

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finFetch,
      bool forwardingPreferenceIsDatagram) override {
    exposed_ = true;
    return consumer_->object(
        groupID,
        subgroupID,
        objectID,
        std::move(payload),
        std::move(extensions),
        finFetch,
        forwardingPreferenceIsDatagram);
  }

  void checkpoint() override {
    consumer_->checkpoint();
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    exposed_ = true;
    return consumer_->beginObject(
        groupID,
        subgroupID,
        objectID,
        length,
        std::move(initialPayload),
        std::move(extensions));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    exposed_ = true;
    return consumer_->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID,
      bool finFetch) override {
    exposed_ = true;
    return consumer_->endOfGroup(groupID, subgroupID, objectID, finFetch);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t groupID,
      uint64_t subgroupID,
      uint64_t objectID) override {
    exposed_ = true;
    return consumer_->endOfTrackAndGroup(groupID, subgroupID, objectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfFetch() override {
    exposed_ = true;
    return consumer_->endOfFetch();
  }

  void reset(ResetStreamErrorCode error) override {
    exposed_ = true;
    consumer_->reset(error);
  }

  void goaway(Goaway goaway) override {
    consumer_->goaway(std::move(goaway));
  }

  folly::Expected<folly::SemiFuture<uint64_t>, MoQPublishError>
  awaitReadyToConsume() override {
    return consumer_->awaitReadyToConsume();
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfUnknownRange(
      uint64_t groupID,
      uint64_t objectID,
      bool finFetch) override {
    exposed_ = true;
    return consumer_->endOfUnknownRange(groupID, objectID, finFetch);
  }

 private:
  std::shared_ptr<FetchConsumer> consumer_;
  bool exposed_{false};
};

// The handle for a joining FETCH served by one upstream FETCH.
class UpstreamJoiningFetchHandle final : public Publisher::FetchHandle {
 public:
  UpstreamJoiningFetchHandle(
      std::shared_ptr<Publisher::FetchHandle> upstream,
      std::shared_ptr<MoQSession> session)
      : Publisher::FetchHandle(upstream->fetchOk()),
        upstream_(std::move(upstream)),
        session_(std::move(session)) {}

  void fetchCancel() override {
    if (auto upstream = std::exchange(upstream_, nullptr)) {
      upstream->fetchCancel();
    }
    session_.reset();
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate requestUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            requestUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "REQUEST_UPDATE not supported for a joining FETCH"});
  }

 private:
  std::shared_ptr<Publisher::FetchHandle> upstream_;
  std::shared_ptr<MoQSession> session_;
};

} // namespace

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
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
    MoQProxyOptions options) {
  return std::shared_ptr<MoQProxy>(
      new MoQProxy(std::move(upstreamProviders), std::move(options)));
}

MoQProxy::MoQProxy(
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
    MoQProxyOptions options)
    : upstreamProviders_(std::move(upstreamProviders)),
      options_(std::move(options)),
      budget_(std::make_shared<MoQCacheBudget>(options_.maxCacheBytes)) {
  if (options_.replayStallTimeout.count() <= 0) {
    throw std::invalid_argument("MoQProxy requires a positive stall timeout");
  }
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
  const auto requestID = subscribeRequest.requestID;
  const bool fillRequested = subscribeRequest.fill.has_value();
  auto track = getOrCreateTrack(subscribeRequest.fullTrackName);
  auto result = co_await track->subscribe(
      std::move(subscribeRequest),
      consumer,
      MoQProxyTrack::DownstreamPeer{reqCtx.sessionId, reqCtx.version});
  // The proxy does not serve fills yet, so a requested fill opens and resets.
  if (result.hasValue() && fillRequested) {
    if (auto fill = consumer->beginFill(requestID); fill.hasValue()) {
      fill.value()->reset(ResetStreamErrorCode::INTERNAL_ERROR);
    }
  }
  co_return result;
}

folly::coro::Task<Publisher::FetchResult> MoQProxy::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  auto self = shared_from_this();
  if (closed_) {
    co_return folly::makeUnexpected(fetchFailure(
        fetch.requestID, FetchErrorCode::GOING_AWAY, "proxy is closed"));
  }
  if (std::holds_alternative<StandaloneFetch>(fetch.args)) {
    co_return co_await forwardStandaloneFetch(
        std::move(fetch), std::move(consumer));
  }
  co_return co_await joiningFetch(std::move(fetch), std::move(consumer));
}

folly::coro::Task<Publisher::FetchResult> MoQProxy::forwardStandaloneFetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  const auto requestID = fetch.requestID;
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto failure = fetchFailure(
      requestID, FetchErrorCode::INTERNAL_ERROR, "no upstream available");
  auto cancelled = [&] {
    return folly::makeUnexpected(
        fetchFailure(requestID, FetchErrorCode::CANCELLED, "fetch cancelled"));
  };
  for (size_t i = 0; i < upstreamProviders_.size(); ++i) {
    if (token.isCancellationRequested()) {
      co_return cancelled();
    }
    auto sessionResult = co_await folly::coro::co_awaitTry(
        upstreamProviders_[i]->getSession(
            fetch.fullTrackName,
            fetch.params,
            i + 1 < upstreamProviders_.size()));
    if (closed_) {
      co_return folly::makeUnexpected(fetchFailure(
          requestID, FetchErrorCode::GOING_AWAY, "proxy is closed"));
    }
    if (token.isCancellationRequested()) {
      co_return cancelled();
    }
    if (sessionResult.hasException() || sessionResult->hasError()) {
      failure.reasonPhrase = sessionResult.hasException()
          ? sessionResult.exception().what().toStdString()
          : sessionResult->error().message;
      continue;
    }
    auto upstreamSession = std::move(sessionResult->value());
    if (!upstreamSession) {
      failure.reasonPhrase = "upstream provider returned a null session";
      continue;
    }
    auto exposure = std::make_shared<ExposureTrackingFetchConsumer>(consumer);
    auto result = co_await folly::coro::co_awaitTry(
        upstreamSession->fetch(fetch, exposure));
    if (closed_ || token.isCancellationRequested()) {
      if (result.hasValue() && result->hasValue()) {
        result->value()->fetchCancel();
      }
      if (closed_) {
        co_return folly::makeUnexpected(fetchFailure(
            requestID, FetchErrorCode::GOING_AWAY, "proxy is closed"));
      }
      co_return cancelled();
    }
    if (result.hasValue() && result->hasValue()) {
      co_return std::move(result->value());
    }
    if (result.hasException()) {
      failure = fetchFailure(
          requestID,
          FetchErrorCode::INTERNAL_ERROR,
          result.exception().what().toStdString());
    } else {
      failure = std::move(result->error());
      failure.requestID = requestID;
    }
    // Another provider may only be tried before this one exposed anything.
    if (token.isCancellationRequested() || exposure->exposed() ||
        failure.errorCode == FetchErrorCode::CANCELLED) {
      co_return folly::makeUnexpected(std::move(failure));
    }
  }
  co_return folly::makeUnexpected(std::move(failure));
}

folly::coro::Task<Publisher::FetchResult> MoQProxy::joiningFetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  const auto requestID = fetch.requestID;
  const auto downstream = MoQSession::getRequestContext();
  const auto& joining = std::get<JoiningFetch>(fetch.args);
  auto track = findTrack(fetch.fullTrackName);
  if (!track) {
    co_return folly::makeUnexpected(fetchFailure(
        requestID,
        FetchErrorCode::INVALID_JOINING_REQUEST_ID,
        "no such subscription"));
  }
  auto range =
      track->resolveJoiningFetch(downstream.sessionId, requestID, joining);
  if (range.hasError()) {
    co_return folly::makeUnexpected(std::move(range.error()));
  }
  // A FETCH that omits its group order is ascending from draft 15, and
  // MoQSession writes and reads a Default-order FETCH stream oldest first.
  const auto groupOrder = fetch.groupOrder == GroupOrder::Default
      ? GroupOrder::OldestFirst
      : fetch.groupOrder;
  fetch.groupOrder = groupOrder;
  auto cache = track->groupCache();
  auto coverage = cache ? cache->inspect(range->start, range->end)
                        : MoQGroupCache::Coverage::Miss;
  const bool waited = coverage == MoQGroupCache::Coverage::AwaitIngress;
  if (waited) {
    auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token,
        shutdown_.getToken());
    const auto waitStart = std::chrono::steady_clock::now();
    coverage = co_await folly::coro::co_withCancellation(
        token,
        awaitCacheCoverage(
            cache,
            range->start,
            range->end,
            options_.cacheCompletionWait,
            options_.timekeeper.get()));
    joinStats_->completionWaitTime +=
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - waitStart);
    if (closed_) {
      co_return folly::makeUnexpected(fetchFailure(
          requestID, FetchErrorCode::GOING_AWAY, "proxy is closed"));
    }
    if (token.isCancellationRequested()) {
      co_return folly::makeUnexpected(fetchFailure(
          requestID, FetchErrorCode::CANCELLED, "fetch cancelled"));
    }
  }
  switch (coverage) {
    case MoQGroupCache::Coverage::Ready:
      // Nothing has been written yet, so a plan that cannot be charged
      // still falls back upstream.
      if (auto plan = cache->snapshot(range->start, range->end, groupOrder)) {
        (waited ? joinStats_->hitsAfterWait : joinStats_->immediateHits)++;
        co_return co_await startLocalReplay(
            track,
            requestID,
            groupOrder,
            std::move(*plan),
            std::move(consumer));
      }
      joinStats_->fallbacksPlanRejected++;
      break;
    case MoQGroupCache::Coverage::AwaitIngress:
      joinStats_->fallbacksWaitExpired++;
      break;
    case MoQGroupCache::Coverage::Miss:
      joinStats_->fallbacksMiss++;
      break;
  }
  co_return co_await upstreamJoiningFetch(
      std::move(track), std::move(fetch), *range, std::move(consumer));
}

folly::coro::Task<Publisher::FetchResult> MoQProxy::startLocalReplay(
    const std::shared_ptr<MoQProxyTrack>& track,
    RequestID requestID,
    GroupOrder groupOrder,
    MoQReplayPlan plan,
    std::shared_ptr<FetchConsumer> consumer) {
  auto handle = std::make_shared<MoQLocalFetchHandle>(FetchOk{
      .requestID = requestID,
      .groupOrder = groupOrder,
      .endOfTrack = plan.endOfTrack,
      .endLocation = plan.endLocation,
      .extensions = track->trackExtensions()});
  // The replay owns the plan but never the handle, so dropping the handle
  // cancels it.
  auto token = folly::cancellation_token_merge(
      handle->getToken(),
      shutdown_.getToken(),
      co_await folly::coro::co_current_cancellation_token);
  // Awaited separately: GCC 11 destroys a closure that shares a
  // full-expression with co_await twice.
  auto executor = co_await folly::coro::co_current_executor;
  co_withExecutor(
      executor,
      folly::coro::co_withCancellation(
          std::move(token),
          folly::coro::co_invoke(
              [stats = joinStats_,
               replay = runReplay(
                   std::move(plan),
                   std::move(consumer),
                   MoQReplayConfig{
                       .stallTimeout = options_.replayStallTimeout,
                       .timekeeper = options_.timekeeper})]() mutable
                  -> folly::coro::Task<void> {
                if (co_await std::move(replay) == MoQReplayOutcome::Stalled) {
                  stats->replayStallResets++;
                }
              })))
      .start();
  co_return handle;
}

folly::coro::Task<Publisher::FetchResult> MoQProxy::upstreamJoiningFetch(
    std::shared_ptr<MoQProxyTrack> track,
    Fetch fetch,
    MoQProxyTrack::JoiningRange range,
    std::shared_ptr<FetchConsumer> consumer) {
  const auto requestID = fetch.requestID;
  // History comes from the established upstream only, never another
  // publisher generation.
  auto upstream = track->upstreamSession();
  if (!upstream) {
    co_return folly::makeUnexpected(fetchFailure(
        requestID,
        FetchErrorCode::INTERNAL_ERROR,
        "upstream for history is unavailable"));
  }
  Fetch upstreamFetch(
      requestID,
      fetch.fullTrackName,
      range.start,
      toWireFetchEnd(range.end),
      fetch.priority,
      fetch.groupOrder);
  upstreamFetch.params = fetch.params;
  auto token = folly::cancellation_token_merge(
      co_await folly::coro::co_current_cancellation_token,
      shutdown_.getToken());
  auto result = co_await folly::coro::co_awaitTry(
      folly::coro::co_withCancellation(
          token,
          upstream->fetch(std::move(upstreamFetch), std::move(consumer))));
  if (result.hasException()) {
    co_return folly::makeUnexpected(fetchFailure(
        requestID,
        FetchErrorCode::INTERNAL_ERROR,
        result.exception().what().toStdString()));
  }
  if (result->hasError()) {
    auto error = std::move(result->error());
    error.requestID = requestID;
    co_return folly::makeUnexpected(std::move(error));
  }
  auto upstreamHandle = std::move(result->value());
  if (token.isCancellationRequested()) {
    upstreamHandle->fetchCancel();
    co_return folly::makeUnexpected(
        fetchFailure(requestID, FetchErrorCode::CANCELLED, "fetch cancelled"));
  }
  co_return std::make_shared<UpstreamJoiningFetchHandle>(
      std::move(upstreamHandle), std::move(upstream));
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

  auto track = MoQProxyTrack::create(
      fullTrackName,
      upstreamProviders_,
      MoQProxyTrack::CacheConfig{
          .maxGroups = options_.maxGroupsPerTrack, .budget = budget_});
  track->setCallback(shared_from_this());
  tracks_.emplace(fullTrackName, track);
  return track;
}

std::shared_ptr<MoQProxyTrack> MoQProxy::findTrack(
    const FullTrackName& fullTrackName) const {
  auto it = tracks_.find(fullTrackName);
  return it == tracks_.end() ? nullptr : it->second;
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
  shutdown_.requestCancellation();

  auto tracks = std::move(tracks_);
  for (auto& [_, track] : tracks) {
    track->setCallback({});
    track->close();
  }
}

} // namespace moxygen
