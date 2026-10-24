/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQProxyTrack.h"

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/Result.h>
#include <utility>

#include "moxygen/MoQLocation.h"
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

  // Called after upstream SUBSCRIBE_OK for the first subscriber: its saved
  // Joining Location and its live filter both come from the upstream Largest,
  // even if early data already advanced the forwarder.
  void anchorTo(
      const SubscribeRequest& request,
      std::optional<AbsoluteLocation> largest) {
    if (!subscriber_) {
      return;
    }
    if (largest) {
      subscriber_->updateLargest(*largest);
    }
    subscriber_->range = toSubscribeRange(request, largest);
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

// Records one subgroup of the upstream subscription into the group cache,
// then forwards it. The forwarding leg keeps cache interest while the group
// stays cached, so it consumes the subgroup even with no eligible viewer.
// Once its upstream attempt is abandoned, it neither records nor forwards.
class MoQProxyTrack::SubgroupRecorder final : public SubgroupConsumer {
 public:
  // cachedLeg is null when the group is not cached; leg then only forwards.
  SubgroupRecorder(
      std::shared_ptr<const bool> attemptActive,
      std::weak_ptr<MoQGroupCache> cache,
      uint64_t group,
      uint64_t subgroup,
      std::shared_ptr<SubgroupConsumer> leg,
      std::shared_ptr<MoQForwarder::SubgroupForwarder> cachedLeg)
      : attemptActive_(std::move(attemptActive)),
        cache_(std::move(cache)),
        group_(group),
        subgroup_(subgroup),
        leg_(std::move(leg)),
        cachedLeg_(std::move(cachedLeg)),
        caching_(cachedLeg_ != nullptr) {}

  folly::Expected<folly::Unit, MoQPublishError> object(
      uint64_t objectID,
      Payload payload,
      Extensions extensions,
      bool finSubgroup) override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!record([&](MoQGroupCache& cache) {
          return cache.subgroupObject(
              group_,
              subgroup_,
              objectID,
              ObjectStatus::NORMAL,
              payload,
              extensions);
        }) ||
        (finSubgroup && !recordEnd(/*cleanFin=*/true))) {
      return malformed();
    }
    return leg()->object(
        objectID, std::move(payload), std::move(extensions), finSubgroup);
  }

  void checkpoint() override {
    if (!abandoned()) {
      leg()->checkpoint();
    }
  }

  folly::Expected<folly::Unit, MoQPublishError> beginObject(
      uint64_t objectID,
      uint64_t length,
      Payload initialPayload,
      Extensions extensions) override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!record([&](MoQGroupCache& cache) {
          return cache.beginObject(
              group_, subgroup_, objectID, length, initialPayload, extensions);
        })) {
      return malformed();
    }
    return leg()->beginObject(
        objectID, length, std::move(initialPayload), std::move(extensions));
  }

  folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
      Payload payload,
      bool finSubgroup) override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!record([&](MoQGroupCache& cache) {
          return cache.objectPayload(group_, subgroup_, payload);
        }) ||
        (finSubgroup && !recordEnd(/*cleanFin=*/true))) {
      return malformed();
    }
    // The forwarder tracks the declared length, so its IN_PROGRESS/DONE is
    // correct even when only the cache is consuming.
    return leg()->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!recordStatus(endOfGroupObjectID, ObjectStatus::END_OF_GROUP)) {
      return malformed();
    }
    return leg()->endOfGroup(endOfGroupObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!recordStatus(endOfTrackObjectID, ObjectStatus::END_OF_TRACK)) {
      return malformed();
    }
    return leg()->endOfTrackAndGroup(endOfTrackObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!recordEnd(/*cleanFin=*/true)) {
      return malformed();
    }
    return leg()->endOfSubgroup();
  }

  void reset(ResetStreamErrorCode error) override {
    if (abandoned()) {
      return;
    }
    recordEnd(/*cleanFin=*/false);
    leg()->reset(error);
  }

 private:
  // On the first callback after the attempt was abandoned, ends the leg.
  bool abandoned() {
    if (*attemptActive_) {
      return false;
    }
    if (leg_) {
      stopCaching();
      std::exchange(leg_, nullptr)->reset(ResetStreamErrorCode::CANCELLED);
      cachedLeg_.reset();
    }
    return true;
  }

  static folly::Unexpected<MoQPublishError> abandonedError() {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::CANCELLED, "upstream attempt was abandoned"));
  }

  SubgroupConsumer* leg() {
    return leg_.get();
  }

  // Returns false only for malformed input. Once this group stops being
  // cached, the leg loses cache interest and stops consuming for the cache.
  template <typename Fn>
  bool record(Fn&& fn) {
    if (!caching_) {
      return true;
    }
    auto cache = cache_.lock();
    if (!cache) {
      stopCaching();
      return true;
    }
    auto result = fn(*cache);
    if (result == MoQGroupCache::Result::Malformed) {
      stopCaching();
      return false;
    }
    if (result == MoQGroupCache::Result::NotCached &&
        !cache->isCaching(group_)) {
      stopCaching();
    }
    return true;
  }

  bool recordStatus(uint64_t objectID, ObjectStatus status) {
    return record([&](MoQGroupCache& cache) {
             return cache.subgroupObject(
                 group_, subgroup_, objectID, status, nullptr, noExtensions());
           }) &&
        recordEnd(/*cleanFin=*/true);
  }

  bool recordEnd(bool cleanFin) {
    return record([&](MoQGroupCache& cache) {
      return cache.endSubgroup(group_, subgroup_, cleanFin);
    });
  }

  void stopCaching() {
    caching_ = false;
    if (cachedLeg_) {
      cachedLeg_->setCacheInterest(false);
    }
  }

  folly::Unexpected<MoQPublishError> malformed() {
    leg()->reset(ResetStreamErrorCode::MALFORMED_TRACK);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::MALFORMED_TRACK, "malformed subgroup data"));
  }

  std::shared_ptr<const bool> attemptActive_;
  std::weak_ptr<MoQGroupCache> cache_;
  uint64_t group_;
  uint64_t subgroup_;
  std::shared_ptr<SubgroupConsumer> leg_;
  std::shared_ptr<MoQForwarder::SubgroupForwarder> cachedLeg_;
  bool caching_;
};

// The consumer for one upstream establishment attempt. It records into the
// group cache before forwarding; once disabled, late callbacks from a failed
// attempt, including on subgroups it already opened, neither record nor
// forward.
class MoQProxyTrack::Recorder final : public TrackConsumer {
 public:
  Recorder(
      std::shared_ptr<MoQForwarder> forwarder,
      std::weak_ptr<MoQGroupCache> cache)
      : forwarder_(std::move(forwarder)), cache_(std::move(cache)) {}

  void disable() {
    disabled_ = true;
    *active_ = false;
    cache_.reset();
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override {
    if (disabled_) {
      return folly::unit;
    }
    return forwarder_->setTrackAlias(alias);
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      BeginSubgroupOptions options) override {
    if (disabled_) {
      return folly::makeUnexpected(disabledError());
    }
    auto cache = cache_.lock();
    if (!cache ||
        !cache->openSubgroup(
            groupID, subgroupID, options.containsLastInGroup)) {
      auto leg =
          forwarder_->beginSubgroup(groupID, subgroupID, priority, options);
      if (leg.hasError()) {
        return leg;
      }
      return std::make_shared<SubgroupRecorder>(
          active_,
          std::weak_ptr<MoQGroupCache>(),
          groupID,
          subgroupID,
          std::move(leg.value()),
          nullptr);
    }
    auto leg = forwarder_->beginSubgroupWithCacheInterest(
        groupID, subgroupID, priority, options);
    if (leg.hasError()) {
      cache->endSubgroup(groupID, subgroupID, /*cleanFin=*/false);
      return folly::makeUnexpected(leg.error());
    }
    return std::make_shared<SubgroupRecorder>(
        active_, cache_, groupID, subgroupID, leg.value(), leg.value());
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    if (disabled_) {
      return folly::makeUnexpected(disabledError());
    }
    return forwarder_->awaitStreamCredit();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override {
    if (disabled_) {
      return folly::makeUnexpected(disabledError());
    }
    if (!recordStandalone(header, payload, /*datagram=*/false)) {
      return folly::makeUnexpected(malformedError());
    }
    return forwarder_->objectStream(header, std::move(payload), lastInGroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override {
    if (disabled_) {
      return folly::makeUnexpected(disabledError());
    }
    if (!recordStandalone(header, payload, /*datagram=*/true)) {
      return folly::makeUnexpected(malformedError());
    }
    return forwarder_->datagram(header, std::move(payload), lastInGroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override {
    if (disabled_) {
      return folly::unit;
    }
    return forwarder_->publishDone(std::move(pubDone));
  }

 private:
  bool recordStandalone(
      const ObjectHeader& header,
      const Payload& payload,
      bool datagram) {
    auto cache = cache_.lock();
    return !cache ||
        cache->standaloneObject(header, payload, datagram) !=
        MoQGroupCache::Result::Malformed;
  }

  static MoQPublishError disabledError() {
    return MoQPublishError(
        MoQPublishError::CANCELLED, "upstream attempt was abandoned");
  }

  static MoQPublishError malformedError() {
    return MoQPublishError(
        MoQPublishError::MALFORMED_TRACK, "malformed object data");
  }

  std::shared_ptr<MoQForwarder> forwarder_;
  std::weak_ptr<MoQGroupCache> cache_;
  bool disabled_{false};
  // Shared with this attempt's subgroups.
  std::shared_ptr<bool> active_{std::make_shared<bool>(true)};
};

std::shared_ptr<MoQProxyTrack> MoQProxyTrack::create(
    FullTrackName fullTrackName,
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
    CacheConfig cacheConfig) {
  auto track = std::shared_ptr<MoQProxyTrack>(new MoQProxyTrack(
      std::move(fullTrackName),
      std::move(upstreamProviders),
      std::move(cacheConfig)));
  track->forwarder_->setCallback(std::make_shared<ForwarderCallback>(track));
  return track;
}

MoQProxyTrack::MoQProxyTrack(
    FullTrackName fullTrackName,
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
    CacheConfig cacheConfig)
    : fullTrackName_(std::move(fullTrackName)),
      upstreamProviders_(std::move(upstreamProviders)),
      forwarder_(std::make_shared<MoQForwarder>(fullTrackName_)),
      cache_(
          cacheConfig.maxGroups > 0 && cacheConfig.budget
              ? std::make_shared<MoQGroupCache>(
                    cacheConfig.maxGroups,
                    std::move(cacheConfig.budget))
              : nullptr) {
  XCHECK(!upstreamProviders_.empty())
      << "MoQProxyTrack requires upstream providers";
  for (const auto& provider : upstreamProviders_) {
    XCHECK(provider) << "MoQProxyTrack requires non-null upstream providers";
  }
}

MoQProxyTrack::~MoQProxyTrack() {
  forwarder_->setCallback(nullptr);
  stopCaching();
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
    DownstreamPeer downstream) {
  auto self = shared_from_this();
  if (subscribeRequest.fullTrackName != fullTrackName_) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::DOES_NOT_EXIST,
            "track name does not match proxy track"});
  }
  // Subscribers are keyed on the downstream id, so an unset one would collide
  // with every other unset peer instead of naming this subscription.
  if (!consumer || downstream.sessionId == kUnsetSessionId) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::INTERNAL_ERROR,
            "subscriber session and consumer are required"});
  }
  if (state_ == State::IDLE) {
    co_return co_await handleFirstSubscription(
        std::move(subscribeRequest), std::move(consumer), downstream);
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

  co_return addSubscriber(subscribeRequest, std::move(consumer), downstream);
}

folly::coro::Task<Publisher::SubscribeResult>
MoQProxyTrack::handleFirstSubscription(
    SubscribeRequest subscribeRequest,
    std::shared_ptr<TrackConsumer> consumer,
    DownstreamPeer downstream) {
  state_ = State::CONNECTING;
  auto subscriberResult =
      addSubscriber(subscribeRequest, std::move(consumer), downstream);
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
  auto failure = co_await establishUpstream(subscribeRequest, downstream);
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
  downstreamHandle->anchorTo(subscribeRequest, upstreamLargest_);
  downstreamHandle->refreshSubscribeOk();
  state_ = State::READY;
  completeUpstreamEstablishment(std::nullopt);
  co_return subscriber;
}

Publisher::SubscribeResult MoQProxyTrack::addSubscriber(
    const SubscribeRequest& subscribeRequest,
    std::shared_ptr<TrackConsumer> consumer,
    DownstreamPeer downstream) {
  // The forwarder holds one subscription per session; a second one would
  // alias the first handle.
  if (forwarder_->findSubscriber(downstream.sessionId)) {
    return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::DUPLICATE_SUBSCRIPTION,
            "session already subscribes to this track"});
  }
  auto subEndGroup = subscribeRequest.filter.legacyEndGroup();
  if (forwarder_->largest() && subEndGroup &&
      *subEndGroup < forwarder_->largest()->group) {
    return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::INVALID_RANGE,
            "range is no longer available"});
  }

  auto subscriber = forwarder_->addSubscriber(
      downstream.sessionId, subscribeRequest, std::move(consumer));
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
    DownstreamPeer downstream) {
  std::optional<SubscribeError> lastFailure;
  for (size_t providerIndex = 0; providerIndex < upstreamProviders_.size();
       ++providerIndex) {
    const auto& upstreamProvider = upstreamProviders_[providerIndex];
    const bool hasFallbackProvider =
        providerIndex + 1 < upstreamProviders_.size();
    auto result = co_await establishWithProvider(
        upstreamProvider, subscribeRequest, downstream, hasFallbackProvider);
    if (result.hasError()) {
      if (state_ != State::CONNECTING) {
        co_return std::move(result.error());
      }
      lastFailure = std::move(result.error());
      continue;
    }

    auto establishedUpstream = std::move(result.value());
    const auto& subscribeOk = establishedUpstream.handle->subscribeOk();
    upstreamLargest_ = subscribeOk.largest;
    if (subscribeOk.largest) {
      forwarder_->updateLargest(
          subscribeOk.largest->group, subscribeOk.largest->object);
    }
    forwarder_->setExtensions(subscribeOk.extensions);
    upstreamSession_ = std::move(establishedUpstream.session);
    upstreamHandle_ = std::move(establishedUpstream.handle);
    recorder_ = std::move(establishedUpstream.recorder);
    co_return std::nullopt;
  }
  co_return lastFailure;
}

folly::coro::Task<MoQProxyTrack::UpstreamEstablishmentResult>
MoQProxyTrack::establishWithProvider(
    const std::shared_ptr<MoQUpstreamProvider>& upstreamProvider,
    const SubscribeRequest& subscribeRequest,
    DownstreamPeer downstream,
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
  if (upstreamSession->sessionId() == downstream.sessionId) {
    co_return makeUpstreamFailure(
        subscribeRequest, "upstream and downstream sessions are the same");
  }

  auto upstreamRequest = subscribeRequest;
  upstreamRequest.priority = kDefaultPriority;
  upstreamRequest.groupOrder = GroupOrder::Default;
  upstreamRequest.filter = LocationFilter::nextObject();
  upstreamRequest.forward = forwarder_->numForwardingSubscribers() > 0;

  auto upstreamVersion = upstreamSession->getNegotiatedVersion();
  if (getDraftMajorVersion(downstream.version) >= 18 ||
      (upstreamVersion && getDraftMajorVersion(*upstreamVersion) >= 18)) {
    // TrackRequestParamKey::RENDEZVOUS_TIMEOUT changes meaning across
    // versions. The rendezvous timeout was introduced in draft 18, and isn't
    // propagated end-to-end (it's one hop only). In versions below 18,
    // TrackRequestParamKey::RENDEZVOUS_TIMEOUT means MAX_CACHE_DURATION.
    upstreamRequest.params.eraseAllParamsOfType(
        TrackRequestParamKey::RENDEZVOUS_TIMEOUT);
  }

  // One recorder per attempt, so a failed attempt's late callbacks can be
  // cut off without touching the next one.
  auto recorder = std::make_shared<Recorder>(forwarder_, cache_);
  auto abandonAttempt = folly::makeGuard([this, recorder] {
    recorder->disable();
    if (cache_) {
      cache_->clear();
    }
  });
  auto subscribeResult = co_await folly::coro::co_awaitTry(
      upstreamSession->subscribe(std::move(upstreamRequest), recorder));
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

  abandonAttempt.dismiss();
  co_return EstablishedUpstream{
      .session = std::move(upstreamSession),
      .handle = std::move(upstreamHandle),
      .recorder = std::move(recorder),
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
  stopCaching();
  if (auto handle = std::move(upstreamHandle_)) {
    handle->unsubscribe();
  }
  upstreamSession_.reset();
  notifyNoSubscribers();
}

void MoQProxyTrack::onUpstreamPublishDone() {
  // Publishing ended, but already received data can still serve a joining
  // FETCH, and a miss still needs this upstream for history.
  upstreamHandle_.reset();
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
  stopCaching();
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

void MoQProxyTrack::stopCaching() {
  if (auto recorder = std::move(recorder_)) {
    recorder->disable();
  }
  if (cache_) {
    cache_->shutdown();
  }
}

folly::Expected<MoQProxyTrack::JoiningRange, FetchError>
MoQProxyTrack::resolveJoiningFetch(
    SessionId sessionId,
    RequestID fetchRequestID,
    const JoiningFetch& joining) const {
  auto subscriber = forwarder_->findSubscriber(sessionId);
  if (!subscriber || !joining.joiningRequestID ||
      subscriber->requestID != *joining.joiningRequestID) {
    return folly::makeUnexpected(
        FetchError{
            fetchRequestID,
            FetchErrorCode::INVALID_JOINING_REQUEST_ID,
            "no such subscription on this track"});
  }
  if (!subscriber->shouldForward) {
    return folly::makeUnexpected(
        FetchError{
            fetchRequestID,
            FetchErrorCode::INVALID_RANGE,
            "joining FETCH requires a forwarding subscription"});
  }
  // The saved Joining Location; never the track's current Largest.
  const auto& joiningLocation = subscriber->subscribeOk().largest;
  if (!joiningLocation) {
    return folly::makeUnexpected(
        FetchError{
            fetchRequestID,
            FetchErrorCode::INVALID_RANGE,
            "no objects had been published"});
  }
  AbsoluteLocation start{0, 0};
  if (joining.fetchType == FetchType::ABSOLUTE_JOINING) {
    start.group = joining.joiningStart;
    if (*joiningLocation < start) {
      return folly::makeUnexpected(
          FetchError{
              fetchRequestID,
              FetchErrorCode::INVALID_RANGE,
              "joining start is past the Joining Location"});
    }
  } else {
    // The drafts leave a relative offset past group 0 unspecified; the proxy
    // starts at group 0.
    start.group = joining.joiningStart > joiningLocation->group
        ? 0
        : joiningLocation->group - joining.joiningStart;
  }
  auto end = joiningLocation->next();
  if (!end) {
    return folly::makeUnexpected(
        FetchError{
            fetchRequestID,
            FetchErrorCode::INVALID_RANGE,
            "Joining Location has no representable successor"});
  }
  return JoiningRange{start, *end};
}

const Extensions& MoQProxyTrack::trackExtensions() const {
  return forwarder_->extensions();
}

} // namespace moxygen
