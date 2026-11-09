/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQProxyTrack.h"

#include <fmt/core.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Result.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/WithCancellation.h>
#include <folly/logging/xlog.h>
#include <utility>

#include "moxygen/MoQLocation.h"
#include "moxygen/MoQSession.h"
#include "moxygen/events/MoQExecutor.h"
#include "moxygen/proxy/MoQFetchCacheWriter.h"
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

folly::Expected<MoQProxyTrack::JoiningRange, FetchError>
resolveJoiningFetchRange(
    const JoiningFetch& joining,
    const std::optional<AbsoluteLocation>& largest,
    AbsoluteLocation end) {
  if (!largest) {
    return folly::makeUnexpected(
        FetchError{
            RequestID(0),
            FetchErrorCode::INVALID_RANGE,
            "upstream subscription has no Joining Location"});
  }
  auto start = AbsoluteLocation{0, 0};
  if (joining.fetchType == FetchType::ABSOLUTE_JOINING) {
    start.group = joining.joiningStart;
  } else if (joining.joiningStart <= largest->group) {
    start.group = largest->group - joining.joiningStart;
  }
  auto expectedEnd = largest->next();
  if (start <= *largest && expectedEnd && !(end < start) &&
      !(*expectedEnd < end)) {
    return MoQProxyTrack::JoiningRange{start, end};
  }
  return folly::makeUnexpected(
      FetchError{
          RequestID(0),
          FetchErrorCode::INVALID_RANGE,
          "invalid upstream joining FETCH range"});
}

// Another upstream may still serve the track. A session that closes for any
// reason, including a crash, idle timeout or transport reset, ends its
// subscriptions with SESSION_CLOSED.
bool isUpstreamLoss(PublishDoneStatusCode statusCode) {
  return statusCode == PublishDoneStatusCode::GOING_AWAY ||
      statusCode == PublishDoneStatusCode::SESSION_CLOSED;
}

std::chrono::milliseconds millisecondsSince(
    std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);
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

  // Saves the upstream SUBSCRIBE_OK's Largest Object for a pending subscriber.
  void setLargestObject(std::optional<AbsoluteLocation> largest) {
    if (!subscriber_) {
      return;
    }
    if (largest) {
      subscriber_->updateLargest(*largest);
    }
    subscriber_->range = toSubscribeRange(subscriber_->filter, largest);
  }

  // Copies the finalized subscriber state into the returned SUBSCRIBE_OK.
  void finalizeSubscribeOk() {
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
  // replacement is set when the attempt replaces a lost upstream.
  SubgroupRecorder(
      std::shared_ptr<const bool> attemptActive,
      std::weak_ptr<Recorder> replacement,
      std::weak_ptr<MoQGroupCache> cache,
      uint64_t group,
      uint64_t subgroup,
      std::shared_ptr<SubgroupConsumer> leg,
      std::shared_ptr<MoQForwarder::SubgroupForwarder> cachedLeg)
      : attemptActive_(std::move(attemptActive)),
        replacement_(std::move(replacement)),
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
    resume();
    return leg_->object(
        objectID, std::move(payload), std::move(extensions), finSubgroup);
  }

  void checkpoint() override {
    if (!abandoned()) {
      leg_->checkpoint();
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
    resume();
    return leg_->beginObject(
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
    return leg_->objectPayload(std::move(payload), finSubgroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
      uint64_t endOfGroupObjectID) override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!recordStatus(endOfGroupObjectID, ObjectStatus::END_OF_GROUP)) {
      return malformed();
    }
    resume();
    return leg_->endOfGroup(endOfGroupObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
      uint64_t endOfTrackObjectID) override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!recordStatus(endOfTrackObjectID, ObjectStatus::END_OF_TRACK)) {
      return malformed();
    }
    resume();
    return leg_->endOfTrackAndGroup(endOfTrackObjectID);
  }

  folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
    if (abandoned()) {
      return abandonedError();
    }
    if (!recordEnd(/*cleanFin=*/true)) {
      return malformed();
    }
    return leg_->endOfSubgroup();
  }

  void reset(ResetStreamErrorCode error) override {
    if (abandoned()) {
      return;
    }
    recordEnd(/*cleanFin=*/false);
    leg_->reset(error);
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

  void resume();

  // Cache consistency failures disable caching without interrupting delivery.
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
      return true;
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
    leg_->reset(ResetStreamErrorCode::MALFORMED_TRACK);
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::MALFORMED_TRACK, "malformed subgroup data"));
  }

  std::shared_ptr<const bool> attemptActive_;
  std::weak_ptr<Recorder> replacement_;
  std::weak_ptr<MoQGroupCache> cache_;
  uint64_t group_;
  uint64_t subgroup_;
  std::shared_ptr<SubgroupConsumer> leg_;
  std::shared_ptr<MoQForwarder::SubgroupForwarder> cachedLeg_;
  bool caching_;
  bool resumed_{false};
};

// The consumer for one upstream establishment attempt. It records into the
// group cache before forwarding; once disabled, late callbacks from a failed
// attempt, including on subgroups it already opened, neither record nor
// forward. Once the track takes over its upstream's loss, it refuses new data,
// while subgroups it already opened finish.
class MoQProxyTrack::Recorder final
    : public TrackConsumer,
      public std::enable_shared_from_this<Recorder> {
 public:
  Recorder(
      std::shared_ptr<MoQForwarder> forwarder,
      std::weak_ptr<MoQGroupCache> cache,
      std::weak_ptr<MoQProxyTrack> track)
      : forwarder_(std::move(forwarder)),
        cache_(std::move(cache)),
        track_(std::move(track)) {}

  void disable() {
    disabled_ = true;
    *active_ = false;
    cache_.reset();
  }

  void markLost() {
    lost_ = true;
  }

  bool lost() const {
    return lost_;
  }

  // Its upstream sent a GOAWAY, on the session or on this subscription's
  // request stream.
  bool wentAway() const {
    return wentAway_;
  }

  // Some of its data reached the forwarder.
  bool forwarded() const {
    return forwarded_;
  }

  // As a replacement, some of its objects reached the forwarder.
  bool resumed() const {
    return resumed_;
  }

  // A replacement's first object that reaches the forwarder resumes the
  // viewers; a stream header alone doesn't.
  void resume(uint64_t group) {
    if (!arbiter_ || std::exchange(resumed_, true)) {
      return;
    }
    if (auto track = track_.lock()) {
      track->onReplacementResumed(*this, group);
    }
  }

  // Makes this a replacement for a lost upstream whose last group was
  // lastGroup: it admits only later groups, through an arbiter, and keeps its
  // track alias to itself, since aliases are hop-local. The lost upstream has
  // ended, so the arbiter needs only where each subgroup and datagram starts.
  // It stays for the attempt's life: a later stream that starts mid-subgroup
  // is refused too. Below draft 18 an upstream can't mark a subgroup as
  // starting mid-subgroup.
  void replaceUpstream(
      std::optional<uint64_t> lastGroup,
      bool upstreamMarksFirstObject) {
    arbiter_.emplace(
        MoQUpstreamHandoffArbiter::OldProgress{.highestGroup = lastGroup});
    arbiter_->onOldEnded();
    upstreamMarksFirstObject_ = upstreamMarksFirstObject;
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override {
    if (disabled_ || arbiter_) {
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
    if (disabled_ || lost_) {
      return folly::makeUnexpected(endedError());
    }
    if (arbiter_ &&
        !admit(
            arbiter_->onBeginSubgroup(
                MoQUpstreamHandoffArbiter::Source::Replacement,
                groupID,
                subgroupID,
                upstreamMarksFirstObject_ && !options.beginsWithFirstObject),
            /*datagram=*/false)) {
      return folly::makeUnexpected(refusedError());
    }
    forwarded_ = true;
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
          replacementSelf(),
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
        active_,
        replacementSelf(),
        cache_,
        groupID,
        subgroupID,
        leg.value(),
        leg.value());
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    if (disabled_) {
      return folly::makeUnexpected(endedError());
    }
    return forwarder_->awaitStreamCredit();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override {
    if (disabled_ || lost_) {
      return folly::makeUnexpected(endedError());
    }
    // An object stream is a subgroup of its own, which starts with its object.
    if (arbiter_ &&
        !admit(
            arbiter_->onBeginSubgroup(
                MoQUpstreamHandoffArbiter::Source::Replacement,
                header.group,
                header.subgroup),
            /*datagram=*/false)) {
      return folly::makeUnexpected(refusedError());
    }
    forwarded_ = true;
    if (!recordStandalone(header, payload, /*datagram=*/false)) {
      return folly::makeUnexpected(malformedError());
    }
    resume(header.group);
    return forwarder_->objectStream(header, std::move(payload), lastInGroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup) override {
    if (disabled_ || lost_) {
      return folly::makeUnexpected(endedError());
    }
    if (arbiter_ &&
        !admit(
            arbiter_->onDatagram(
                MoQUpstreamHandoffArbiter::Source::Replacement,
                header.group,
                lastInGroup),
            /*datagram=*/true)) {
      return folly::makeUnexpected(refusedError());
    }
    forwarded_ = true;
    if (!recordStandalone(header, payload, /*datagram=*/true)) {
      return folly::makeUnexpected(malformedError());
    }
    resume(header.group);
    return forwarder_->datagram(header, std::move(payload), lastInGroup);
  }

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override {
    if (disabled_ || lost_) {
      return folly::unit;
    }
    if (auto track = track_.lock();
        track && track->takeOverUpstreamEnd(*this, pubDone)) {
      return folly::unit;
    }
    // SESSION_CLOSED is MoQSession's local marker for a closed session.
    // Forwarded, it would tell a moxygen viewer that its own session closed,
    // and other peers don't know the code.
    if (pubDone.statusCode == PublishDoneStatusCode::SESSION_CLOSED) {
      pubDone.statusCode = PublishDoneStatusCode::GOING_AWAY;
    }
    return forwarder_->publishDone(std::move(pubDone));
  }

  // A request-stream GOAWAY for this subscription (draft 18+); the track's
  // watch on the session passes session GOAWAYs here too.
  void goaway(Goaway goaway) override {
    if (disabled_ || lost_) {
      return;
    }
    wentAway_ = true;
    if (auto track = track_.lock()) {
      track->onUpstreamGoaway(goaway);
    }
  }

 private:
  // Counts what a replacement refuses.
  bool admit(MoQUpstreamHandoffArbiter::Admit admitted, bool datagram) {
    if (admitted == MoQUpstreamHandoffArbiter::Admit::Accept) {
      return true;
    }
    if (auto track = track_.lock()) {
      track->onReplacementRefused(admitted, datagram);
    }
    return false;
  }

  // What its subgroups resume the viewers through.
  std::weak_ptr<Recorder> replacementSelf() {
    return arbiter_ ? weak_from_this() : std::weak_ptr<Recorder>();
  }

  bool recordStandalone(
      const ObjectHeader& header,
      const Payload& payload,
      bool datagram) {
    auto cache = cache_.lock();
    if (cache) {
      (void)cache->standaloneObject(header, payload, datagram);
    }
    return true;
  }

  MoQPublishError endedError() const {
    return MoQPublishError(
        MoQPublishError::CANCELLED,
        lost_ ? "upstream went away" : "upstream attempt was abandoned");
  }

  static MoQPublishError malformedError() {
    return MoQPublishError(
        MoQPublishError::MALFORMED_TRACK, "malformed object data");
  }

  static MoQPublishError refusedError() {
    return MoQPublishError(
        MoQPublishError::CANCELLED, "group belongs to the previous upstream");
  }

  std::shared_ptr<MoQForwarder> forwarder_;
  std::weak_ptr<MoQGroupCache> cache_;
  std::weak_ptr<MoQProxyTrack> track_;
  bool disabled_{false};
  bool lost_{false};
  bool wentAway_{false};
  bool forwarded_{false};
  // Shared with this attempt's subgroups.
  std::shared_ptr<bool> active_{std::make_shared<bool>(true)};
  std::optional<MoQUpstreamHandoffArbiter> arbiter_;
  bool upstreamMarksFirstObject_{true};
  bool resumed_{false};
};

void MoQProxyTrack::SubgroupRecorder::resume() {
  if (std::exchange(resumed_, true)) {
    return;
  }
  if (auto replacement = replacement_.lock()) {
    replacement->resume(group_);
  }
}

std::shared_ptr<MoQProxyTrack> MoQProxyTrack::create(
    FullTrackName fullTrackName,
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
    CacheConfig cacheConfig,
    UpstreamConfig upstreamConfig) {
  auto track = std::shared_ptr<MoQProxyTrack>(new MoQProxyTrack(
      std::move(fullTrackName),
      std::move(upstreamProviders),
      std::move(cacheConfig),
      std::move(upstreamConfig)));
  track->forwarder_->setCallback(std::make_shared<ForwarderCallback>(track));
  return track;
}

MoQProxyTrack::MoQProxyTrack(
    FullTrackName fullTrackName,
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
    CacheConfig cacheConfig,
    UpstreamConfig upstreamConfig)
    : fullTrackName_(std::move(fullTrackName)),
      upstreamProviders_(std::move(upstreamProviders)),
      forwarder_(std::make_shared<MoQForwarder>(fullTrackName_)),
      cache_(
          cacheConfig.maxGroups > 0 && cacheConfig.budget
              ? std::make_shared<MoQGroupCache>(
                    cacheConfig.maxGroups,
                    std::move(cacheConfig.budget))
              : nullptr),
      goaways_(
          upstreamConfig.goaways ? std::move(upstreamConfig.goaways)
                                 : std::make_shared<MoQUpstreamGoaways>()),
      switchStats_(
          upstreamConfig.switchStats ? std::move(upstreamConfig.switchStats)
                                     : std::make_shared<MoQProxySwitchStats>()),
      switchOptions_(upstreamConfig.switchOptions),
      timekeeper_(std::move(upstreamConfig.timekeeper)),
      random_(
          upstreamConfig.random
              ? std::move(upstreamConfig.random)
              : std::make_shared<std::mt19937_64>(
                    upstreamConfig.switchOptions.randomSeed != 0
                        ? upstreamConfig.switchOptions.randomSeed
                        : std::random_device{}())) {
  XCHECK(!upstreamProviders_.empty())
      << "MoQProxyTrack requires upstream providers";
  for (const auto& provider : upstreamProviders_) {
    XCHECK(provider) << "MoQProxyTrack requires non-null upstream providers";
  }
}

MoQProxyTrack::~MoQProxyTrack() {
  forwarder_->setCallback(nullptr);
  stopCaching();
  if (auto handle = std::move(upstream_.handle)) {
    handle->unsubscribe();
  }
  upstream_.session.reset();
  if (!viewersEnded_ && !forwarder_->empty()) {
    forwarder_->publishDone(
        PublishDone{
            RequestID(0),
            PublishDoneStatusCode::GOING_AWAY,
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
    const bool rangePending = !forwarder_->largest();
    auto subscriber =
        addSubscriber(subscribeRequest, std::move(consumer), downstream);
    if (subscriber.hasError()) {
      co_return subscriber;
    }
    auto downstreamHandle =
        std::static_pointer_cast<DownstreamSubscriptionHandle>(
            subscriber.value());
    if (rangePending) {
      pendingSubscribers_.push_back(downstreamHandle);
    }
    co_await upstreamSubscriptionReadyPromise_.getFuture();
    if (upstreamSubscriptionFailure_) {
      co_return folly::makeUnexpected(makeSubscribeError(
          subscribeRequest.requestID, *upstreamSubscriptionFailure_));
    }
    if (state_ != State::READY && state_ != State::SWITCHING) {
      co_return folly::makeUnexpected(
          SubscribeError{
              subscribeRequest.requestID,
              SubscribeErrorCode::DOES_NOT_EXIST,
              "upstream subscription is no longer available"});
    }
    // Copy the finalized subscriber state into the downstream response.
    downstreamHandle->finalizeSubscribeOk();
    co_return subscriber;
  }

  if (upstreamSubscriptionFailure_) {
    co_return folly::makeUnexpected(makeSubscribeError(
        subscribeRequest.requestID, *upstreamSubscriptionFailure_));
  }
  // A viewer that arrives while the track switches upstreams sees what the
  // others see.
  if (state_ != State::READY && state_ != State::SWITCHING) {
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
  auto downstreamHandle =
      std::static_pointer_cast<DownstreamSubscriptionHandle>(subscriber);
  pendingSubscribers_.push_back(downstreamHandle);
  executor_ = co_await folly::coro::co_current_executor;
  upstreamRequest_ = subscribeRequest;
  upstreamPeer_ = downstream;
  auto upstream = co_await establishUpstream(
      subscribeRequest, downstream, establishing_.getToken());
  if (upstream.hasError()) {
    auto& failure = upstream.error();
    if (!upstreamSubscriptionFailure_) {
      completeUpstreamEstablishment(
          UpstreamEstablishmentFailure{
              failure.errorCode,
              failure.reasonPhrase,
          });
    }
    auto returnedFailure = upstreamSubscriptionFailure_
        ? makeSubscribeError(
              subscribeRequest.requestID, *upstreamSubscriptionFailure_)
        : std::move(failure);
    // Roll back the subscriber registered before upstream establishment.
    subscriber->unsubscribe();
    co_return folly::makeUnexpected(std::move(returnedFailure));
  }
  adoptFirstUpstream(std::move(upstream.value()));
  downstreamHandle->finalizeSubscribeOk();
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

folly::coro::Task<folly::Unit> MoQProxyTrack::awaitUpstreamSubscription() {
  auto self = shared_from_this();
  co_return co_await upstreamSubscriptionReadyPromise_.getFuture();
}

std::shared_ptr<MoQProxyTrack::SharedFetchState>
MoQProxyTrack::saveJoiningFetchForUpstream(
    Fetch upstreamFetch,
    std::shared_ptr<FetchConsumer> consumer) {
  if (joiningFetchRequest_) {
    return nullptr;
  }
  joiningFetchRequest_ = std::move(upstreamFetch);
  joiningFetchConsumer_ = std::move(consumer);
  joiningFetchExposed_ = false;
  joiningFetchStatus_ = std::make_shared<SharedFetchState>();
  return joiningFetchStatus_;
}

void MoQProxyTrack::trySendJoiningFetchToCurrentUpstream() {
  if (!joiningFetchRequest_ || !upstream_.session ||
      (joiningFetchStatus_ && joiningFetchStatus_->accepted.isFulfilled()) ||
      (joiningFetchAttempt_ && joiningFetchAttempt_->id == upstreamAttempt_)) {
    return;
  }
  auto version = upstream_.session->getNegotiatedVersion();
  if (!version || useFillFetch(*version)) {
    return;
  }
  joiningFetchAttempt_.emplace(UpstreamJoiningFetchAttempt{});
  joiningFetchAttempt_->id = upstreamAttempt_;
  auto attempt = upstreamAttempt_;
  auto session = upstream_.session;
  auto cancellation = joiningFetchAttempt_->cancellation.getToken();
  auto fetch = *joiningFetchRequest_;
  joiningFetchAttempt_->writer = std::make_shared<MoQFetchCacheWriter>(
      cache_, [](bool) {}, fetch.groupOrder);
  auto writer = joiningFetchAttempt_->writer;
  auto consumer = std::make_shared<MoQFetchCacheTee>(
      writer,
      joiningFetchConsumer_,
      [weak = weak_from_this(), attempt] {
        if (auto self = weak.lock()) {
          self->joiningFetchFinished(attempt);
        }
      },
      [weak = weak_from_this(), attempt](bool exposesResponse) {
        auto self = weak.lock();
        if (!self || !self->joiningFetchAttempt_ ||
            self->joiningFetchAttempt_->id != attempt ||
            self->joiningFetchAttempt_->finished) {
          return false;
        }
        self->joiningFetchExposed_ |= exposesResponse;
        return true;
      });
  auto executor = session->getExecutor();
  auto fetchTask =
      [](std::shared_ptr<MoQProxyTrack> self,
         uint64_t attempt,
         std::shared_ptr<MoQSession> session,
         Fetch fetch,
         std::shared_ptr<FetchConsumer> consumer) -> folly::coro::Task<void> {
    auto result = co_await folly::coro::co_awaitTry(
        session->fetch(std::move(fetch), std::move(consumer)));
    if (result.hasException()) {
      self->joiningFetchFailed(
          attempt,
          FetchError{
              RequestID(0),
              FetchErrorCode::INTERNAL_ERROR,
              result.exception().what().toStdString()});
      co_return;
    }
    if (result->hasError()) {
      auto error = std::move(result->error());
      error.requestID = RequestID(0);
      self->joiningFetchFailed(attempt, std::move(error));
      co_return;
    }
    if (!result->value()) {
      self->joiningFetchFailed(
          attempt,
          FetchError{
              RequestID(0),
              FetchErrorCode::INTERNAL_ERROR,
              "upstream returned a null joining FETCH handle"});
      co_return;
    }
    self->joiningFetchAccepted(attempt, std::move(result->value()));
  }(shared_from_this(),
                                                  attempt,
                                                  std::move(session),
                                                  std::move(fetch),
                                                  std::move(consumer));
  folly::coro::co_withExecutor(
      std::move(executor),
      folly::coro::co_withCancellation(cancellation, std::move(fetchTask)))
      .start();
}

void MoQProxyTrack::cancelJoiningFetch(
    const std::shared_ptr<SharedFetchState>& fetch) {
  if (fetch != joiningFetchStatus_) {
    return;
  }
  joiningFetchRequest_.reset();
  cancelJoiningFetchAttempt(upstreamAttempt_);
  joiningFetchConsumer_.reset();
  failJoiningFetch(
      FetchError{RequestID(0), FetchErrorCode::CANCELLED, "fetch cancelled"});
}

void MoQProxyTrack::joiningFetchAccepted(
    uint64_t attempt,
    std::shared_ptr<Publisher::FetchHandle> handle) {
  if (!joiningFetchAttempt_ || joiningFetchAttempt_->id != attempt) {
    handle->fetchCancel();
    return;
  }
  joiningFetchAttempt_->handle = std::move(handle);
  resolveJoiningFetchAttempt(attempt);
}

void MoQProxyTrack::joiningFetchFailed(uint64_t attempt, FetchError error) {
  if (!joiningFetchAttempt_ || joiningFetchAttempt_->id != attempt) {
    return;
  }
  joiningFetchAttempt_->error = std::move(error);
  joiningFetchAttempt_->finished = true;
  joiningFetchAttempt_->writer->invalidate();
  resolveJoiningFetchAttempt(attempt);
}

void MoQProxyTrack::joiningFetchFinished(uint64_t attempt) {
  if (!joiningFetchAttempt_ || joiningFetchAttempt_->id != attempt) {
    return;
  }
  joiningFetchAttempt_->finished = true;
  resolveJoiningFetchAttempt(attempt);
}

void MoQProxyTrack::resolveJoiningFetchAttempt(uint64_t attempt) {
  if (!joiningFetchStatus_ || !joiningFetchAttempt_ ||
      joiningFetchAttempt_->id != attempt ||
      !joiningFetchAttempt_->subscriptionAccepted) {
    return;
  }
  auto& status = *joiningFetchStatus_;
  auto& operation = *joiningFetchAttempt_;
  if (!status.accepted.isFulfilled()) {
    if (operation.error) {
      status.error = operation.error;
    } else if (operation.handle) {
      status.fetchOk = operation.handle->fetchOk();
      status.fetchHandle = operation.handle;
      status.session = upstream_.session;
      auto range = resolveJoiningFetchRange(
          std::get<moxygen::JoiningFetch>(joiningFetchRequest_->args),
          upstreamLargest_,
          operation.handle->fetchOk().endLocation);
      if (range.hasValue()) {
        status.range = std::move(range.value());
      } else {
        status.error = std::move(range.error());
      }
    } else {
      return;
    }
    status.accepted.setValue(folly::unit);
    if (status.range) {
      (void)operation.writer->setRange(status.range->start, status.range->end);
    }
    if (status.error) {
      if (auto handle = std::exchange(operation.handle, nullptr)) {
        handle->fetchCancel();
      }
      operation.writer->invalidate();
      operation.finished = true;
    }
  }
  if (status.finished.isFulfilled() || !operation.finished) {
    return;
  }
  status.finished.setValue(folly::unit);
  operation.handle.reset();
  joiningFetchConsumer_.reset();
}

void MoQProxyTrack::cancelJoiningFetchAttempt(uint64_t attempt) {
  if (!joiningFetchAttempt_ || joiningFetchAttempt_->id != attempt) {
    return;
  }
  joiningFetchAttempt_->finished = true;
  joiningFetchAttempt_->cancellation.requestCancellation();
  if (auto handle = std::exchange(joiningFetchAttempt_->handle, nullptr)) {
    handle->fetchCancel();
  }
  joiningFetchAttempt_->writer->abort();
  joiningFetchAttempt_.reset();
}

void MoQProxyTrack::failJoiningFetch(FetchError error) {
  if (!joiningFetchStatus_) {
    return;
  }
  if (joiningFetchAttempt_) {
    joiningFetchAttempt_->cancellation.requestCancellation();
    if (auto handle = std::exchange(joiningFetchAttempt_->handle, nullptr)) {
      handle->fetchCancel();
    }
    joiningFetchAttempt_->writer->abort();
  }
  if (!joiningFetchStatus_->accepted.isFulfilled()) {
    joiningFetchStatus_->error = std::move(error);
    joiningFetchStatus_->accepted.setValue(folly::unit);
  }
  if (!joiningFetchStatus_->finished.isFulfilled()) {
    joiningFetchStatus_->finished.setValue(folly::unit);
  }
}

folly::coro::Task<MoQProxyTrack::UpstreamEstablishmentResult>
MoQProxyTrack::establishUpstream(
    SubscribeRequest subscribeRequest,
    DownstreamPeer downstream,
    folly::CancellationToken cancelled) {
  auto self = shared_from_this();
  std::optional<SubscribeError> lastFailure;
  for (size_t providerIndex = 0; providerIndex < upstreamProviders_.size();
       ++providerIndex) {
    const auto& upstreamProvider = upstreamProviders_[providerIndex];
    const bool hasFallbackProvider =
        providerIndex + 1 < upstreamProviders_.size();
    auto result = co_await establishWithProvider(
        upstreamProvider,
        subscribeRequest,
        downstream,
        hasFallbackProvider,
        cancelled,
        lostSessions_);
    if (result.hasValue() && result->recorder->lost()) {
      // Its upstream went away between its SUBSCRIBE_OK and here. What it
      // forwarded stays, so later attempts replace it.
      lostSessions_.push_back(result->session->sessionId());
      result = makeUpstreamFailure(
          subscribeRequest,
          SubscribeErrorCode::GOING_AWAY,
          "upstream went away while establishing the subscription");
    }
    if (result.hasValue() || cancelled.isCancellationRequested() ||
        (joiningFetchExposed_ && state_ == State::CONNECTING)) {
      co_return result;
    }
    lastFailure = std::move(result.error());
  }
  XCHECK(lastFailure) << "a proxy track has at least one upstream provider";
  co_return folly::makeUnexpected(std::move(*lastFailure));
}

void MoQProxyTrack::adoptFirstUpstream(UpstreamAttempt attempt) {
  XDCHECK(state_ == State::CONNECTING);
  const auto& subscribeOk = attempt.handle->subscribeOk();
  upstreamLargest_ = subscribeOk.largest;
  if (subscribeOk.largest) {
    forwarder_->updateLargest(
        subscribeOk.largest->group, subscribeOk.largest->object);
  }
  forwarder_->setExtensions(subscribeOk.extensions);
  upstream_ = std::move(attempt);
  const bool joiningUnavailable = joiningFetchRequest_ && !joiningFetchAttempt_;
  setLargestObjectForPendingSubscribers();
  state_ = State::READY;
  if (joiningUnavailable) {
    joiningFetchRequest_.reset();
    failJoiningFetch(
        FetchError{
            RequestID(0),
            FetchErrorCode::NOT_SUPPORTED,
            "upstream does not support joining FETCH"});
  } else if (
      joiningFetchAttempt_ && joiningFetchAttempt_->id == upstreamAttempt_) {
    joiningFetchAttempt_->subscriptionAccepted = true;
    resolveJoiningFetchAttempt(upstreamAttempt_);
  }
  completeUpstreamEstablishment(std::nullopt);
}

folly::coro::Task<MoQProxyTrack::UpstreamEstablishmentResult>
MoQProxyTrack::establishWithProvider(
    std::shared_ptr<MoQUpstreamProvider> upstreamProvider,
    SubscribeRequest subscribeRequest,
    DownstreamPeer downstream,
    bool hasFallbackProvider,
    folly::CancellationToken cancelled,
    std::vector<SessionId> lostSessions) {
  // The provider and the SUBSCRIBE stop early once the track stops waiting.
  const auto stopWaiting = folly::cancellation_token_merge(
      cancelled, co_await folly::coro::co_current_cancellation_token);
  auto sessionResult = co_await folly::coro::co_awaitTry(
      folly::coro::co_withCancellation(
          stopWaiting,
          upstreamProvider->getSession(
              fullTrackName_, subscribeRequest.params, hasFallbackProvider)));
  if (cancelled.isCancellationRequested()) {
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
  if (std::find(
          lostSessions.begin(),
          lostSessions.end(),
          upstreamSession->sessionId()) != lostSessions.end()) {
    co_return makeUpstreamFailure(
        subscribeRequest,
        "upstream provider returned the session of a lost upstream");
  }

  // One recorder per attempt, so a failed attempt's late callbacks can be
  // cut off without touching the next one.
  auto recorder =
      std::make_shared<Recorder>(forwarder_, cache_, weak_from_this());
  // Watched before subscribing, so a GOAWAY that arrives while the SUBSCRIBE
  // is in flight still reaches the track.
  establishingGoawayWatch_ = goaways_->watch(
      *upstreamSession,
      [weakRecorder = std::weak_ptr<Recorder>(recorder)](const Goaway& goaway) {
        if (auto recorder = weakRecorder.lock()) {
          recorder->goaway(goaway);
        }
      });
  // Only the first subscription sends the joining FETCH saved for it, so only
  // it stands in as the upstream while it subscribes.
  const bool first = state_ == State::CONNECTING;
  uint64_t attempt = 0;
  if (first) {
    upstream_.session = upstreamSession;
    attempt = ++upstreamAttempt_;
  }

  auto upstreamRequest = subscribeRequest;
  upstreamRequest.priority = kDefaultPriority;
  upstreamRequest.groupOrder.reset();
  const bool replacement = !lostSessions.empty();
  upstreamRequest.filter = LocationFilter::nextObject();
  // The upstream subscription is shared, so the fill stays off it.
  upstreamRequest.fill.reset();
  upstreamRequest.forward = forwarder_->numForwardingSubscribers() > 0;

  auto upstreamVersion = upstreamSession->getNegotiatedVersion();
  const bool upstreamDraft18 =
      upstreamVersion && getDraftMajorVersion(*upstreamVersion) >= 18;
  if (getDraftMajorVersion(downstream.version) >= 18 || upstreamDraft18) {
    // TrackRequestParamKey::RENDEZVOUS_TIMEOUT changes meaning across
    // versions. The rendezvous timeout was introduced in draft 18, and isn't
    // propagated end-to-end (it's one hop only). In versions below 18,
    // TrackRequestParamKey::RENDEZVOUS_TIMEOUT means MAX_CACHE_DURATION.
    upstreamRequest.params.eraseAllParamsOfType(
        TrackRequestParamKey::RENDEZVOUS_TIMEOUT);
  }

  if (replacement) {
    recorder->replaceUpstream(highestForwardedGroup(), upstreamDraft18);
  }
  auto abandonAttempt =
      folly::makeGuard([this, recorder, replacement, first, attempt] {
        if (first) {
          cancelJoiningFetchAttempt(attempt);
        }
        establishingGoawayWatch_.reset();
        recorder->disable();
        if (first && upstreamAttempt_ == attempt) {
          upstream_.session.reset();
        }
        // A replacement adds to what the track already forwarded and cached.
        if (cache_ && !replacement) {
          cache_->clear();
        }
      });
  auto subscribeTask = folly::coro::co_withCancellation(
      stopWaiting,
      upstreamSession->subscribe(std::move(upstreamRequest), recorder));
  folly::Try<Publisher::SubscribeResult> subscribeResult;
  if (first && joiningFetchRequest_) {
    auto subscribeFuture =
        folly::coro::co_withExecutor(
            upstreamSession->getExecutor(), std::move(subscribeTask))
            .start();
    trySendJoiningFetchToCurrentUpstream();
    subscribeResult =
        co_await folly::coro::co_awaitTry(std::move(subscribeFuture));
  } else {
    subscribeResult =
        co_await folly::coro::co_awaitTry(std::move(subscribeTask));
  }
  if (cancelled.isCancellationRequested()) {
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
  co_return UpstreamAttempt{
      .session = std::move(upstreamSession),
      .handle = std::move(upstreamHandle),
      .recorder = std::move(recorder),
      .goawayWatch = std::move(establishingGoawayWatch_),
  };
}

void MoQProxyTrack::setLargestObjectForPendingSubscribers() {
  for (auto& pending : pendingSubscribers_) {
    if (auto handle = pending.lock()) {
      handle->setLargestObject(upstreamLargest_);
    }
  }
  pendingSubscribers_.clear();
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
  if (failure) {
    joiningFetchRequest_.reset();
    failJoiningFetch(
        FetchError{RequestID(0), failure->errorCode, failure->reasonPhrase});
    pendingSubscribers_.clear();
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
  cancelEstablishment();
  stopCaching();
  if (auto handle = std::move(upstream_.handle)) {
    handle->unsubscribe();
  }
  upstream_.goawayWatch.reset();
  upstream_.session.reset();
  notifyNoSubscribers();
}

void MoQProxyTrack::cancelEstablishment() {
  establishing_.requestCancellation();
  establishingGoawayWatch_.reset();
  switchDeadline_.requestCancellation();
  switchStartedAt_.reset();
}

void MoQProxyTrack::onUpstreamPublishDone() {
  viewersEnded_ = true;
  // Publishing ended, but already received data can still serve a joining
  // FETCH, and a miss still needs this upstream for history.
  upstream_.handle.reset();
  upstream_.goawayWatch.reset();
  lostUpstream_.reset();
  // An attempt being established ended the track, the switch gave up, or a
  // replacement ended the track before the switch completed.
  cancelEstablishment();
  if (state_ == State::CONNECTING) {
    // The upstream ended the attempt before the track resumed with its
    // SUBSCRIBE_OK.
    completeUpstreamEstablishment(
        UpstreamEstablishmentFailure{
            SubscribeErrorCode::DOES_NOT_EXIST,
            "upstream ended while establishing the subscription"});
  }
  if (state_ != State::CLOSED) {
    state_ = State::DRAINING;
  }
}

// Only counted and logged here: the upstream keeps serving until it ends, and
// its Recorder remembers that it went away.
void MoQProxyTrack::onUpstreamGoaway(const Goaway& goaway) {
  ++switchStats_->upstreamGoaways;
  XLOG(DBG1) << "Upstream GOAWAY for " << fullTrackName_
             << " timeout=" << goaway.timeout << "ms"
             << (goaway.newSessionUri.empty()
                     ? ""
                     : ", ignoring newSessionUri=" + goaway.newSessionUri);
}

void MoQProxyTrack::onUpstreamLost(const PublishDone& publishDone) {
  ++switchStats_->upstreamsLost;
  XLOG(DBG1) << "Upstream lost for " << fullTrackName_
             << " status=" << folly::to_underlying(publishDone.statusCode)
             << " reason=" << publishDone.reasonPhrase;
}

bool MoQProxyTrack::takeOverUpstreamEnd(
    Recorder& recorder,
    const PublishDone& done) {
  if (!isUpstreamLoss(done.statusCode)) {
    return false;
  }
  onUpstreamLost(done);
  if (&recorder == upstream_.recorder.get()) {
    if (state_ != State::READY) {
      return false;
    }
    recorder.markLost();
    startLostSwitch(recorder);
    return true;
  }
  if (state_ != State::CONNECTING && state_ != State::SWITCHING) {
    return false;
  }
  // An attempt the track hasn't adopted yet: establishUpstream moves on.
  // Viewers already wait on it once it forwarded data.
  recorder.markLost();
  if (recorder.forwarded()) {
    noteLostUpstream(recorder);
  }
  return true;
}

void MoQProxyTrack::startLostSwitch(const Recorder& lost) {
  state_ = State::SWITCHING;
  noteLostUpstream(lost);
  if (switchStartedAt_) {
    XLOG(INFO) << "Replacement upstream lost for " << fullTrackName_
               << " before it forwarded data, switching again";
  } else {
    ++switchStats_->lostSwitches.started;
    XLOG(INFO) << "Upstream lost for " << fullTrackName_
               << ", switching to a replacement";
    switchStartedAt_ = std::chrono::steady_clock::now();
    lostSessions_.clear();
    switchDeadline_ = folly::CancellationSource();
    folly::coro::co_withExecutor(
        switchExecutor(), enforceSwitchDeadline(switchDeadline_.getToken()))
        .start();
  }
  lostSessions_.push_back(upstream_.session->sessionId());
  releaseUpstream();
  establishing_ = folly::CancellationSource();
  folly::coro::co_withExecutor(
      switchExecutor(), findReplacement(establishing_.getToken()))
      .start();
}

// Lets go of upstream_, which ended. Subgroups it already opened still
// finish.
void MoQProxyTrack::releaseUpstream() {
  upstream_.recorder.reset();
  upstream_.handle.reset();
  upstream_.goawayWatch.reset();
  upstream_.session.reset();
}

// A gap still open keeps when and how it opened.
void MoQProxyTrack::noteLostUpstream(const Recorder& lost) {
  if (lostUpstream_) {
    return;
  }
  lostUpstream_ = LostUpstream{
      .lostAt = std::chrono::steady_clock::now(),
      .lastGroup = highestForwardedGroup(),
      .afterGoaway = lost.wentAway()};
}

// A MoQ executor's own keep-alive token doesn't keep it alive; the one it
// hands out can.
folly::Executor::KeepAlive<> MoQProxyTrack::switchExecutor() const {
  if (auto* moqExecutor = dynamic_cast<MoQExecutor*>(executor_)) {
    return moqExecutor->getKeepAlive();
  }
  return folly::getKeepAliveToken(executor_);
}

// Runs until a replacement is adopted, or the switch stops.
folly::coro::Task<void> MoQProxyTrack::findReplacement(
    folly::CancellationToken cancelled) {
  auto self = shared_from_this();
  for (auto backoff = switchOptions_.retryBackoff;;
       backoff = std::min(backoff * 2, switchOptions_.maxRetryBackoff)) {
    auto attempt =
        co_await establishUpstream(*upstreamRequest_, upstreamPeer_, cancelled);
    if (attempt.hasValue()) {
      adoptReplacement(std::move(attempt.value()));
      co_return;
    }
    co_await folly::coro::co_awaitTry(
        folly::coro::co_withCancellation(
            cancelled,
            folly::coro::sleep(jittered(backoff), timekeeper_.get())));
    if (cancelled.isCancellationRequested()) {
      co_return;
    }
  }
}

// Runs on the track's executor, like adoption, so a switch either completes
// or fails.
folly::coro::Task<void> MoQProxyTrack::enforceSwitchDeadline(
    folly::CancellationToken stopped) {
  auto self = shared_from_this();
  co_await folly::coro::co_awaitTry(
      folly::coro::co_withCancellation(
          stopped,
          folly::coro::sleep(switchOptions_.deadline, timekeeper_.get())));
  if (!stopped.isCancellationRequested()) {
    failSwitch();
  }
}

void MoQProxyTrack::adoptReplacement(UpstreamAttempt attempt) {
  XDCHECK(state_ == State::SWITCHING);
  upstream_ = std::move(attempt);
  state_ = State::READY;
  if (upstream_.recorder->resumed()) {
    completeSwitch();
  }
}

void MoQProxyTrack::completeSwitch() {
  if (!switchStartedAt_) {
    return;
  }
  auto& counters = switchStats_->lostSwitches;
  ++counters.completed;
  counters.duration +=
      millisecondsSince(*std::exchange(switchStartedAt_, std::nullopt));
  switchDeadline_.requestCancellation();
}

void MoQProxyTrack::failSwitch() {
  ++switchStats_->lostSwitches.failed;
  XLOG(WARN) << "No replacement upstream forwarded data for " << fullTrackName_
             << " by the switch deadline";
  switchStartedAt_.reset();
  cancelEstablishment();
  // A replacement the track adopted that never forwarded anything.
  if (auto recorder = std::move(upstream_.recorder)) {
    recorder->disable();
  }
  if (auto handle = std::move(upstream_.handle)) {
    handle->unsubscribe();
  }
  upstream_.goawayWatch.reset();
  upstream_.session.reset();
  forwarder_->publishDone(
      PublishDone{
          RequestID(0),
          PublishDoneStatusCode::GOING_AWAY,
          0,
          "no replacement upstream"});
}

std::chrono::milliseconds MoQProxyTrack::jittered(
    std::chrono::milliseconds delay) {
  std::uniform_real_distribution<double> factor(0.8, 1.2);
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      delay * factor(*random_));
}

std::optional<uint64_t> MoQProxyTrack::highestForwardedGroup() const {
  auto largest = forwarder_->largest();
  return largest ? std::optional<uint64_t>(largest->group) : std::nullopt;
}

void MoQProxyTrack::onReplacementRefused(
    MoQUpstreamHandoffArbiter::Admit admitted,
    bool datagram) {
  ++(datagram ? switchStats_->refusedDatagrams : switchStats_->refusedStreams);
  if (admitted == MoQUpstreamHandoffArbiter::Admit::RefuseWithGap) {
    ++switchStats_->midSubgroupGaps;
  }
}

void MoQProxyTrack::onReplacementResumed(
    const Recorder& recorder,
    uint64_t firstGroup) {
  if (auto lost = std::exchange(lostUpstream_, std::nullopt)) {
    switchStats_->lostResumeTime += millisecondsSince(lost->lostAt);
    auto& gaps = lost->afterGoaway ? switchStats_->lateReplacementGaps
                                   : switchStats_->lostGaps;
    ++gaps.gaps;
    // The replacement admits only groups after the lost upstream's last.
    if (lost->lastGroup && firstGroup > *lost->lastGroup) {
      gaps.groups += firstGroup - *lost->lastGroup;
    }
  }
  if (&recorder == upstream_.recorder.get()) {
    completeSwitch();
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
  cancelEstablishment();
  stopCaching();
  if (auto handle = std::move(upstream_.handle)) {
    handle->unsubscribe();
  }
  upstream_.goawayWatch.reset();
  upstream_.session.reset();

  if (forwarder_->empty()) {
    state_ = State::CLOSED;
    notifyNoSubscribers();
    return;
  }
  if (!viewersEnded_) {
    viewersEnded_ = true;
    forwarder_->publishDone(
        PublishDone{
            RequestID(0),
            PublishDoneStatusCode::GOING_AWAY,
            0,
            "proxy track closed"});
  }
}

void MoQProxyTrack::stopCaching() {
  auto closed = FetchError{
      RequestID(0), FetchErrorCode::INTERNAL_ERROR, "proxy track closed"};
  joiningFetchRequest_.reset();
  if (auto consumer = std::exchange(joiningFetchConsumer_, nullptr)) {
    consumer->reset(ResetStreamErrorCode::CANCELLED);
  }
  joiningFetchExposed_ = false;
  cancelJoiningFetchAttempt(upstreamAttempt_);
  failJoiningFetch(closed);
  if (auto recorder = std::move(upstream_.recorder)) {
    recorder->disable();
  }
  if (cache_) {
    cache_->shutdown();
  }
}

folly::Expected<std::optional<MoQProxyTrack::JoiningRange>, FetchError>
MoQProxyTrack::resolveJoiningFetch(
    SessionId downstreamSessionId,
    RequestID fetchRequestID,
    const JoiningFetch& joining) const {
  auto subscriber = forwarder_->findSubscriber(downstreamSessionId);
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
    if (!upstreamSubscriptionPromiseResolved_) {
      return std::optional<JoiningRange>{};
    }
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
  return std::optional<JoiningRange>{JoiningRange{start, *end}};
}

bool MoQProxyTrack::hasEstablishedUpstream() const {
  return state_ == State::READY && upstream_.handle;
}

const Extensions& MoQProxyTrack::trackExtensions() const {
  return forwarder_->extensions();
}

} // namespace moxygen
