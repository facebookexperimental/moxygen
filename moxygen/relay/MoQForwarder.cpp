/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQForwarder.h"
#include "moxygen/MoQTrackProperties.h"

namespace moxygen {

namespace {
// Returns true if error should tombstone subgroup rather than remove
// subscription. Soft errors like CANCELLED (from STOP_SENDING or delivery
// timeout) should only prevent reopening the subgroup, not terminate the
// entire subscription.
bool isSoftError(const MoQPublishError& err) {
  switch (err.code) {
    case MoQPublishError::CANCELLED: // STOP_SENDING, stream reset, delivery
                                     // timeout
      return true;
    case MoQPublishError::API_ERROR:       // Programming error
    case MoQPublishError::WRITE_ERROR:     // Transport broken
    case MoQPublishError::BLOCKED:         // Shouldn't happen for subscribe
    case MoQPublishError::TOO_FAR_BEHIND:  // Will result in unsubscribe anyway
    case MoQPublishError::MALFORMED_TRACK: // Protocol violation
    default:
      return false;
  }
}
} // namespace

// Template implementations

template <typename Fn>
folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::forEachSubscriber(
    Fn&& fn) {
  for (auto subscriberIt = subscribers_.begin();
       subscriberIt != subscribers_.end();) {
    auto sub = subscriberIt->second;
    subscriberIt++;
    fn(sub);
  }
  // Check if empty after iteration - subscribers may have been removed in loop
  if (subscribers_.empty()) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "No subscribers"));
  }
  return folly::unit;
}

template <typename Fn>
folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::forEachSubscriberSubgroup(
    Fn&& fn,
    bool makeNew,
    const std::string& callsite) {
  if (!forwarder_) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "Forwarder detached"));
  }
  bool anyForwarded = false;
  forwarder_->forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!forwarder_->largest_) {
      return;
    }
    auto subgroupConsumerIt = sub->subgroups.find(identifier_);
    if (subgroupConsumerIt != sub->subgroups.end()) {
      // Entry exists: once we committed a beginObject to this consumer, its
      // object continuation must still reach it. Bypass the "future" branch
      // of checkRange so that a late-arriving range.start mutation (e.g. via
      // onPublishOk with LocationType::LargestObject) cannot strand the
      // consumer mid-object. Still honor past-end so finished subscriptions
      // are retired. See moxygen#168.
      if (forwarder_->checkPastEnd(*sub)) {
        return;
      }
      if (!subgroupConsumerIt->second) {
        // Tombstoned - skip this subscriber for this subgroup
        XLOG(DBG2) << "Skipping tombstoned subgroup for sub=" << sub.get();
        return;
      }
      if (!sub->checkShouldForward()) {
        // If we're attempting to send anything on an existing subgroup when
        // forward == false, then we reset the stream, so that we don't end
        // up with "holes" in the subgroup. If, at some point in the future,
        // we set forward = true, then we'll create a new stream for the
        // subgroup.
        subgroupConsumerIt->second->reset(
            ResetStreamErrorCode::INTERNAL_ERROR);
        closeSubgroupForSubscriber(
            sub, "SubgroupForwarder::forEachSubscriberSubgroup");
      } else {
        anyForwarded = true;
        fn(sub, subgroupConsumerIt->second);
      }
    } else {
      // No consumer yet: full range check gates new subgroup creation.
      if (!forwarder_->checkRange(*sub)) {
        return;
      }
      if (!sub->checkShouldForward()) {
        // If shouldForward == false, we shouldn't be creating any
        // subgroups.
        return;
      }
      if (!makeNew) {
        XLOG(DBG2) << "skipping creating subgroup for sub=" << sub.get();
        return;
      }
      XCHECK(sub->trackConsumer);
      XLOG(DBG2) << "Making new subgroup for consumer=" << sub->trackConsumer
                 << " " << callsite;
      auto res = sub->trackConsumer->beginSubgroup(
          identifier_.group,
          identifier_.subgroup,
          priority_,
          containsLastInGroup_);
      if (res.hasError()) {
        forwarder_->removeSubscriberOnError(*sub, res.error(), callsite);
      } else {
        auto emplaceRes = sub->subgroups.emplace(identifier_, res.value());
        subgroupConsumerIt = emplaceRes.first;
        anyForwarded = true;
        fn(sub, subgroupConsumerIt->second);
      }
    }
  });
  // Check if empty after iteration - subscribers may have been removed in loop
  // Also check if any subscriber was actually forwarded to
  if (!forwarder_ || forwarder_->subscribers_.empty() || !anyForwarded) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::CANCELLED, "No subscribers"));
  }
  return folly::unit;
}

// MoQForwarder implementation

MoQForwarder::MoQForwarder(
    FullTrackName ftn,
    std::optional<AbsoluteLocation> largest)
    : fullTrackName_(std::move(ftn)), largest_(std::move(largest)) {}

MoQForwarder::~MoQForwarder() {
  for (auto& [id, subgroupForwarder] : subgroups_) {
    subgroupForwarder->detach();
  }
}

void MoQForwarder::setDeliveryTimeout(uint64_t timeout) {
  extensions_.insertMutableExtension(
      Extension{kDeliveryTimeoutExtensionType, timeout});
}

void MoQForwarder::setExtensions(Extensions extensions) {
  extensions_ = std::move(extensions);
  auto groupOrder = getPublisherGroupOrder(extensions_);
  if (groupOrder) {
    groupOrder_ = *groupOrder;
  }
  // Update existing subscribers (typically at most the first subscriber
  // added before the upstream SubscribeOk arrived)
  for (auto& [_, sub] : subscribers_) {
    sub->setExtensions(extensions_);
  }
}

void MoQForwarder::setLargest(AbsoluteLocation largest) {
  largest_ = largest;
}

void MoQForwarder::setCallback(std::shared_ptr<Callback> callback) {
  callback_ = std::move(callback);
}

std::shared_ptr<MoQForwarder::Subscriber> MoQForwarder::addSubscriber(
    std::shared_ptr<MoQSession> session,
    const SubscribeRequest& subReq,
    std::shared_ptr<TrackConsumer> consumer) {
  if (draining_) {
    XLOG(ERR) << "addSubscriber called on draining track";
    return nullptr;
  }
  auto sessionPtr = session.get();
  auto trackAlias = trackAlias_.value_or(subReq.requestID.value);
  XCHECK(consumer);
  consumer->setTrackAlias(trackAlias);
  auto subscriber = std::make_shared<MoQForwarder::Subscriber>(
      *this,
      SubscribeOk{
          subReq.requestID,
          trackAlias,
          std::chrono::milliseconds(0),
          MoQSession::resolveGroupOrder(groupOrder_, subReq.groupOrder),
          largest_,
          extensions_},
      std::move(session),
      subReq.requestID,
      toSubscribeRange(subReq, largest_),
      std::move(consumer),
      subReq.forward);
  // If the session already has a subscriber (e.g. from a prior
  // addSubscriber(session, forward) call), emplace is a no-op. Return the
  // existing in-map entry and only increment the forwarding count when a new
  // entry was actually inserted.
  auto [it, inserted] = subscribers_.emplace(sessionPtr, subscriber);
  if (inserted && subReq.forward) {
    addForwardingSubscriber();
  }
  return it->second;
}

std::shared_ptr<MoQForwarder::Subscriber> MoQForwarder::addSubscriber(
    std::shared_ptr<MoQSession> session,
    bool forward) {
  if (draining_) {
    XLOG(ERR) << "addSubscriber called on draining track";
    return nullptr;
  }
  auto sessionPtr = session.get();
  auto subscriber = std::make_shared<MoQForwarder::Subscriber>(
      *this,
      SubscribeOk{
          RequestID(0),
          trackAlias_.value_or(TrackAlias(0)),
          std::chrono::milliseconds(0),
          groupOrder_,
          largest_,
          extensions_},
      std::move(session),
      RequestID(0),
      SubscribeRange{{0, 0}, kLocationMax},
      nullptr,
      forward);
  auto [it, inserted] = subscribers_.emplace(sessionPtr, subscriber);
  if (inserted && forward) {
    addForwardingSubscriber();
  }
  return it->second;
}

folly::Expected<SubscribeRange, FetchError> MoQForwarder::resolveJoiningFetch(
    const std::shared_ptr<MoQSession>& session,
    const JoiningFetch& joining) const {
  auto subIt = subscribers_.find(session.get());
  if (subIt == subscribers_.end()) {
    XLOG(ERR) << "Session not found";
    return folly::makeUnexpected(
        FetchError{
            RequestID(0),
            FetchErrorCode::TRACK_NOT_EXIST,
            "Session has no active subscribe"});
  }
  if (subIt->second->requestID != joining.joiningRequestID) {
    XLOG(ERR) << joining.joiningRequestID
              << " does not name a Subscribe "
                 " for this track";
    return folly::makeUnexpected(
        FetchError{
            RequestID(0),
            FetchErrorCode::INTERNAL_ERROR,
            "Incorrect RequestID for Track"});
  }
  if (!subIt->second->subscribeOk().largest) {
    // No content exists, fetch error
    // Relay caller verifies upstream SubscribeOK has been processed before
    // calling resolveJoiningFetch()
    return folly::makeUnexpected(
        FetchError{RequestID(0), FetchErrorCode::INTERNAL_ERROR, "No largest"});
  }
  CHECK(
      joining.fetchType == FetchType::RELATIVE_JOINING ||
      joining.fetchType == FetchType::ABSOLUTE_JOINING);
  auto& largest = *subIt->second->subscribeOk().largest;
  if (joining.fetchType == FetchType::RELATIVE_JOINING) {
    AbsoluteLocation start{largest};
    start.group -=
        (start.group >= joining.joiningStart) ? joining.joiningStart : 0;
    start.object = 0;
    return SubscribeRange{start, {largest.group, largest.object + 1}};
  } else {
    // ABSOLUTE_JOINING
    AbsoluteLocation start{joining.joiningStart, 0};
    return SubscribeRange{start, {largest.group, largest.object + 1}};
  }
}

void MoQForwarder::drainSubscriber(
    const std::shared_ptr<MoQSession>& session,
    PublishDone pubDone,
    const std::string& callsite) {
  XLOG(DBG1) << __func__ << " from " << callsite
             << " session=" << session.get();
  auto subIt = subscribers_.find(session.get());
  if (subIt == subscribers_.end()) {
    XLOG(WARNING) << "Session not found in drainSubscriber from " << callsite
                  << " sess=" << session;
    return;
  }

  auto& subscriber = *subIt->second;

  // Forward the publishDone message WITHOUT resetting subgroups
  pubDone.requestID = subscriber.requestID;
  if (subscriber.trackConsumer) {
    subscriber.trackConsumer->publishDone(std::move(pubDone));
  }

  // If no open subgroups, delegate to removeSubscriberIt for cleanup
  if (subscriber.subgroups.empty()) {
    // Pass std::nullopt for pubDone since we already forwarded it above
    removeSubscriberIt(subIt, std::nullopt, callsite);
    return;
  }

  // Otherwise, mark receivedPublishDone and wait for subgroups to close
  subscriber.receivedPublishDone_ = true;
  XLOG(DBG1) << "Subscriber " << &subscriber << " is draining with "
             << subscriber.subgroups.size() << " open subgroups";
}

void MoQForwarder::removeSubscriber(
    const std::shared_ptr<MoQSession>& session,
    std::optional<PublishDone> pubDone,
    const std::string& callsite) {
  XLOG(DBG1) << __func__ << " from " << callsite
             << " session=" << session.get();
  auto subIt = subscribers_.find(session.get());
  if (subIt == subscribers_.end()) {
    XLOG(WARNING) << "Session not found in removeSubscriber from " << callsite
                  << " sess=" << session;
    return;
  }
  removeSubscriberIt(subIt, std::move(pubDone), callsite);
}

void MoQForwarder::checkAndFireOnEmpty() {
  if (subscribers_.empty()) {
    if (subgroups_.empty()) {
      if (callback_) {
        callback_->onEmpty(this);
      }
    } else {
      XLOG(DBG4) << subgroups_.size() << " subgroups remaining";
    }
  }
}

void MoQForwarder::removeSubscriberIt(
    folly::F14FastMap<MoQSession*, std::shared_ptr<Subscriber>>::iterator subIt,
    std::optional<PublishDone> pubDone,
    const std::string& callsite) {
  auto& subscriber = *subIt->second;
  XLOG(DBG1) << "Resetting open subgroups for subscriber=" << &subscriber;
  for (auto& subgroup : subscriber.subgroups) {
    // Skip tombstoned subgroups (nullptr entries)
    if (subgroup.second) {
      subgroup.second->reset(ResetStreamErrorCode::CANCELLED);
    }
  }
  if (pubDone && subscriber.trackConsumer) {
    pubDone->requestID = subscriber.requestID;
    subscriber.trackConsumer->publishDone(std::move(*pubDone));
  }

  if (subscriber.shouldForward) {
    if (subscribers_.size() == 1) {
      // don't trigger a forwardUpdated callback here, we will trigger onEmpty
      forwardingSubscribers_--;
    } else {
      removeForwardingSubscriber();
    }
  }
  subscribers_.erase(subIt);
  XLOG(DBG1) << "subscribers_.size()=" << subscribers_.size();
  checkAndFireOnEmpty();
}

void MoQForwarder::updateLargest(uint64_t group, uint64_t object) {
  AbsoluteLocation now{group, object};
  if (!largest_ || now > *largest_) {
    // Clear any outstanding NEW_GROUP_REQUEST once the upstream group advances.
    if (outstandingNewGroupRequest_.has_value() &&
        (!largest_ || group > largest_->group)) {
      outstandingNewGroupRequest_.reset();
    }
    largest_ = now;
  }
}

bool MoQForwarder::checkRange(const Subscriber& sub) {
  XCHECK(largest_);
  if (*largest_ < sub.range.start) {
    // future
    return false;
  } else if (*largest_ > sub.range.end) {
    // now past, send publishDone
    // TOOD: maybe this is too early for a relay.
    XLOG(DBG4) << "removeSubscriber from checkRange";
    removeSubscriber(
        sub.session,
        PublishDone{
            sub.requestID,
            PublishDoneStatusCode::SUBSCRIPTION_ENDED,
            0, // filled in by session
            ""},
        "checkRange");
    return false;
  }
  return true;
}

bool MoQForwarder::checkPastEnd(const Subscriber& sub) {
  XCHECK(largest_);
  if (*largest_ > sub.range.end) {
    XLOG(DBG4) << "removeSubscriber from checkPastEnd";
    removeSubscriber(
        sub.session,
        PublishDone{
            sub.requestID,
            PublishDoneStatusCode::SUBSCRIPTION_ENDED,
            0, // filled in by session
            ""},
        "checkPastEnd");
    return true;
  }
  return false;
}

void MoQForwarder::removeSubscriberOnError(
    const Subscriber& sub,
    const MoQPublishError& err,
    const std::string& callsite) {
  XLOG(ERR) << "Removing subscriber after error in " << callsite
            << " err=" << err.what();
  removeSubscriber(
      sub.session,
      PublishDone{
          sub.requestID,
          PublishDoneStatusCode::INTERNAL_ERROR,
          0, // filled in by session
          err.what()},
      callsite);
}

void MoQForwarder::handleSubgroupError(
    Subscriber& sub,
    const SubgroupIdentifier& subgroupId,
    const MoQPublishError& err,
    const std::string& callsite) {
  XLOG(DBG1) << "Handling subgroup error in " << callsite
             << " err=" << err.what() << " group=" << subgroupId.group
             << " subgroup=" << subgroupId.subgroup;

  if (isSoftError(err)) {
    // Tombstone the subgroup by setting to nullptr - don't reopen it.
    // Per the SubgroupConsumer API contract, returning an error implicitly
    // resets the consumer, so no explicit reset() call is needed.
    auto it = sub.subgroups.find(subgroupId);
    if (it != sub.subgroups.end() && it->second) {
      it->second = nullptr; // Tombstone marker
      XLOG(DBG1) << "Tombstoned subgroup for subscriber";
    }
  } else {
    // Hard error - remove the entire subscription
    removeSubscriberOnError(sub, err, callsite);
  }
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::setTrackAlias(
    TrackAlias alias) {
  trackAlias_ = alias;
  return folly::unit;
}

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
MoQForwarder::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority,
    bool containsLastInGroup) {
  updateLargest(groupID, 0);
  SubgroupIdentifier subgroupIdentifier({groupID, subgroupID});

  // Check if a SubgroupForwarder already exists for this (group, subgroup).
  // This can happen if a publisher opens multiple streams with the same
  // group/subgroup (e.g., after a network issue or client bug).
  // Reset any active downstream consumers and replace the forwarder.
  // If no consumers were active (all already stop_sending'd), return CANCELLED
  // to propagate the signal back to the publisher.
  auto existingIt = subgroups_.find(subgroupIdentifier);
  if (existingIt != subgroups_.end()) {
    bool anyReset = false;
    for (auto& [sess, sub] : subscribers_) {
      auto it = sub->subgroups.find(subgroupIdentifier);
      if (it != sub->subgroups.end() && it->second) {
        it->second->reset(ResetStreamErrorCode::CANCELLED);
        sub->subgroups.erase(it);
        anyReset = true;
      }
    }
    existingIt->second->detach();
    subgroups_.erase(existingIt);
    if (!anyReset) {
      XLOG(WARN) << "beginSubgroup: duplicate group=" << groupID
                 << " subgroup=" << subgroupID
                 << " - no active consumers, returning CANCELLED";
      return folly::makeUnexpected(MoQPublishError(
          MoQPublishError::CANCELLED,
          "duplicate subgroup, no active consumers"));
    }
    XLOG(WARN) << "beginSubgroup: duplicate group=" << groupID
               << " subgroup=" << subgroupID << " - resetting active consumers";
  }

  auto subgroupForwarder = std::make_shared<SubgroupForwarder>(
      *this, groupID, subgroupID, priority, containsLastInGroup);
  auto res = forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    // Skip if tombstoned - subscriber already sent stop_sending for this
    // subgroup and should not receive it again.
    if (sub->subgroups.count(subgroupIdentifier)) {
      return;
    }
    auto sgRes = sub->trackConsumer->beginSubgroup(
        groupID, subgroupID, priority, containsLastInGroup);
    if (sgRes.hasError()) {
      removeSubscriberOnError(*sub, sgRes.error(), "beginSubgroup");
    } else {
      sub->subgroups[subgroupIdentifier] = sgRes.value();
    }
  });
  if (res.hasError()) {
    return folly::makeUnexpected(res.error());
  }
  subgroups_.emplace(subgroupIdentifier, subgroupForwarder);
  return subgroupForwarder;
}

folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
MoQForwarder::awaitStreamCredit() {
  return folly::makeSemiFuture();
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::objectStream(
    const ObjectHeader& header,
    Payload payload,
    bool lastInGroup) {
  updateLargest(header.group, header.id);
  return forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    sub->trackConsumer->objectStream(header, maybeClone(payload), lastInGroup)
        .onError([this, sub](const auto& err) {
          removeSubscriberOnError(*sub, err, "objectStream");
        });
  });
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::datagram(
    const ObjectHeader& header,
    Payload payload,
    bool lastInGroup) {
  updateLargest(header.group, header.id);
  return forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    sub->trackConsumer->datagram(header, maybeClone(payload), lastInGroup)
        .onError([this, sub](const auto& err) {
          removeSubscriberOnError(*sub, err, "datagram");
        });
  });
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::publishDone(
    PublishDone pubDone) {
  XLOG(DBG1) << __func__ << " pubDone reason=" << pubDone.reasonPhrase;
  draining_ = true;
  forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    drainSubscriber(
        sub->session,
        PublishDone{
            sub->requestID,
            pubDone.statusCode,
            0, // filled in by session
            pubDone.reasonPhrase},
        "publishDone");
  });
  // Cleanup/draining operation - succeed even with no subscribers
  return folly::unit;
}

void MoQForwarder::addForwardingSubscriber() {
  if (forwardingSubscribers_++ == 0 && callback_) {
    callback_->forwardChanged(this);
  }
}

void MoQForwarder::removeForwardingSubscriber() {
  if (--forwardingSubscribers_ == 0 && callback_) {
    callback_->forwardChanged(this);
  }
}

void MoQForwarder::tryProcessNewGroupRequest(
    const Parameters& params,
    bool fire) {
  auto value =
      getFirstIntParam(params, TrackRequestParamKey::NEW_GROUP_REQUEST);
  if (!value.has_value()) {
    return;
  }
  // the track must support dynamic groups.
  if (!getPublisherDynamicGroups(extensions_).value_or(false)) {
    return;
  }
  // non-zero value <= LargestGroup means the new group is already
  // available — do not send upstream.
  if (*value != 0 && largest_.has_value() && *value <= largest_->group) {
    return;
  }
  // already have an outstanding request with >= value.
  if (outstandingNewGroupRequest_.has_value() &&
      *outstandingNewGroupRequest_ >= *value) {
    return;
  }
  outstandingNewGroupRequest_ = *value;
  if (fire && callback_) {
    callback_->newGroupRequested(this, *value);
  }
}

Payload MoQForwarder::maybeClone(const Payload& payload) {
  return payload ? payload->clone() : nullptr;
}

// MoQForwarder::Subscriber implementation

MoQForwarder::Subscriber::Subscriber(
    MoQForwarder& f,
    SubscribeOk ok,
    std::shared_ptr<MoQSession> s,
    RequestID sid,
    SubscribeRange r,
    std::shared_ptr<TrackConsumer> tc,
    bool shouldForwardIn)
    : SubscriptionHandle(std::move(ok)),
      session(std::move(s)),
      requestID(sid),
      range(r),
      trackConsumer(std::move(tc)),
      forwarder(f),
      shouldForward(shouldForwardIn) {}

void MoQForwarder::Subscriber::setPublisherGroupOrder(
    GroupOrder pubGroupOrder) {
  subscribeOk_->groupOrder =
      MoQSession::resolveGroupOrder(pubGroupOrder, subscribeOk_->groupOrder);
}

void MoQForwarder::Subscriber::updateLargest(AbsoluteLocation largest) {
  subscribeOk_->largest = largest;
}

void MoQForwarder::Subscriber::setExtensions(Extensions extensions) {
  subscribeOk_->extensions = std::move(extensions);
  auto groupOrder = getPublisherGroupOrder(*subscribeOk_);
  if (groupOrder) {
    subscribeOk_->groupOrder =
        MoQSession::resolveGroupOrder(*groupOrder, subscribeOk_->groupOrder);
  }
}

PublishRequest MoQForwarder::Subscriber::getPublishRequest() const {
  return PublishRequest{
      RequestID(0),
      forwarder.fullTrackName(),
      TrackAlias(0),
      forwarder.groupOrder(),
      forwarder.largest(),
      shouldForward,
      forwarder.extensions()};
}

void MoQForwarder::Subscriber::setParam(const TrackRequestParameter& param) {
  for (size_t i = 0; i < subscribeOk_->params.size(); i++) {
    const auto& existingParam = subscribeOk_->params.at(i);
    if (existingParam.key == param.key) {
      subscribeOk_->params.modifyParam(
          i, param.asString, param.asUint64, param.asAuthToken);
      return;
    }
  }
  auto result = subscribeOk_->params.insertParam(param);
  if (result.hasError()) {
    XLOG(ERR) << "setParam: param not allowed for SubscribeOk, key="
              << param.key;
  }
}

void MoQForwarder::Subscriber::updateForwardState(bool newForward) {
  auto wasForwarding = shouldForward;
  shouldForward = newForward;
  if (shouldForward && !wasForwarding) {
    forwarder.addForwardingSubscriber();
  } else if (wasForwarding && !shouldForward) {
    forwarder.removeForwardingSubscriber();
  }
}

void MoQForwarder::Subscriber::onPublishOk(const PublishOk& pubOk) {
  // Update subscriber range from PUBLISH_OK
  std::optional<AbsoluteLocation> end;
  if (pubOk.endGroup) {
    end = AbsoluteLocation{*pubOk.endGroup, 0};
  }
  range =
      toSubscribeRange(pubOk.start, end, pubOk.locType, forwarder.largest());

  updateForwardState(pubOk.forward);

  // Handle NEW_GROUP_REQUEST forwarding if present
  forwarder.tryProcessNewGroupRequest(pubOk.params);
}

folly::coro::Task<folly::Expected<RequestOk, RequestError>>
MoQForwarder::Subscriber::requestUpdate(RequestUpdate requestUpdate) {
  // Validation:
  // - Start location can be updated
  // - End location can increase or decrease
  // - For bounded subscriptions (endGroup > 0), end must be >= start
  // - Forward state is optional and only updated if explicitly provided
  // - A New Group can be requested

  // Only update start if provided
  if (requestUpdate.start.has_value()) {
    range.start = *requestUpdate.start;
  }

  // Only update end if provided
  if (requestUpdate.endGroup.has_value()) {
    AbsoluteLocation newEnd{*requestUpdate.endGroup, 0};

    // Validate: for bounded end, the end must not be less than the start
    if (*requestUpdate.endGroup > 0 && range.start >= newEnd) {
      XLOG(ERR) << "Invalid requestUpdate: end Location " << newEnd
                << " is less than start " << range.start;
      session->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      co_return folly::makeUnexpected(RequestError(
          requestUpdate.requestID,
          RequestErrorCode::INVALID_RANGE,
          "End Location is less than start"));
    }
    range.end = newEnd;
  }

  // Only update forward state if explicitly provided (per draft 15+)
  if (requestUpdate.forward.has_value()) {
    updateForwardState(*requestUpdate.forward);
  }

  // Only update new group request if provided
  forwarder.tryProcessNewGroupRequest(requestUpdate.params);

  co_return RequestOk{.requestID = requestUpdate.requestID};
}

void MoQForwarder::Subscriber::unsubscribe() {
  XLOG(DBG4) << "unsubscribe sess=" << this;
  forwarder.removeSubscriber(session, std::nullopt, "unsubscribe");
}

bool MoQForwarder::Subscriber::checkShouldForward() {
  if (!shouldForward) {
    XLOG(DBG6) << "shouldForward is false of requestID " << requestID;
  }
  return shouldForward;
}

// MoQForwarder::SubgroupForwarder implementation

MoQForwarder::SubgroupForwarder::SubgroupForwarder(
    MoQForwarder& forwarder,
    uint64_t group,
    uint64_t subgroup,
    Priority priority,
    bool containsLastInGroup)
    : forwarder_(&forwarder),
      identifier_{group, subgroup},
      priority_(priority),
      containsLastInGroup_(containsLastInGroup) {}

void MoQForwarder::SubgroupForwarder::detach() {
  forwarder_ = nullptr;
}

void MoQForwarder::SubgroupForwarder::updateLargest(
    uint64_t group,
    uint64_t object) {
  if (forwarder_) {
    forwarder_->updateLargest(group, object);
  }
}

void MoQForwarder::SubgroupForwarder::closeSubgroupForSubscriber(
    const std::shared_ptr<Subscriber>& sub,
    const std::string& callsite) {
  sub->subgroups.erase(identifier_);
  // If this subscriber is draining and this was the last subgroup, remove it
  if (sub->shouldRemove()) {
    forwarder_->removeSubscriber(sub->session, std::nullopt, callsite);
  }
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::removeSubgroupAndCheckEmpty() {
  if (!forwarder_) {
    return folly::unit;
  }
  forwarder_->subgroups_.erase(identifier_);
  forwarder_->checkAndFireOnEmpty();
  return folly::unit;
}

template <typename T>
folly::Expected<T, MoQPublishError>
MoQForwarder::SubgroupForwarder::cleanupOnError(
    const folly::Expected<T, MoQPublishError>& result) {
  if (result.hasError()) {
    XLOG(DBG1) << "Removing subgroup after error: " << result.error().what();
    removeSubgroupAndCheckEmpty();
  }
  return result;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::object(
    uint64_t objectID,
    Payload payload,
    Extensions extensions,
    bool finSubgroup) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  updateLargest(identifier_.group, objectID);
  auto res = forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer
            ->object(objectID, maybeClone(payload), extensions, finSubgroup)
            .onError([this, sub](const auto& err) {
              forwarder_->handleSubgroupError(
                  *sub, identifier_, err, "SubgroupForwarder::object");
            });
        if (finSubgroup) {
          closeSubgroupForSubscriber(sub, "SubgroupForwarder::object");
        }
      });
  if (finSubgroup) {
    return removeSubgroupAndCheckEmpty();
  }
  // Data operation - propagate CANCELLED if no subscribers
  return cleanupOnError(res);
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::beginObject(
    uint64_t objectID,
    uint64_t length,
    Payload initialPayload,
    Extensions extensions) {
  // TODO: use a shared class for object publish state validation
  updateLargest(identifier_.group, objectID);
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  auto payloadLength =
      (initialPayload) ? initialPayload->computeChainDataLength() : 0;
  if (length > payloadLength) {
    currentObjectLength_ = length - payloadLength;
  }
  return cleanupOnError(forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer
            ->beginObject(
                objectID, length, maybeClone(initialPayload), extensions)
            .onError([this, sub](const auto& err) {
              forwarder_->handleSubgroupError(
                  *sub, identifier_, err, "SubgroupForwarder::beginObject");
            });
      }));
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfGroup(uint64_t endOfGroupObjectID) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  updateLargest(identifier_.group, endOfGroupObjectID);
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfGroup(endOfGroupObjectID)
            .onError([this, sub](const auto& err) {
              forwarder_->handleSubgroupError(
                  *sub, identifier_, err, "SubgroupForwarder::endOfGroup");
            });
        closeSubgroupForSubscriber(sub, "SubgroupForwarder::endOfGroup");
      });
  // Cleanup operation - succeed even with no subscribers
  return removeSubgroupAndCheckEmpty();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfTrackAndGroup(
    uint64_t endOfTrackObjectID) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  updateLargest(identifier_.group, endOfTrackObjectID);
  forEachSubscriberSubgroup([&](const std::shared_ptr<Subscriber>& sub,
                                const std::shared_ptr<SubgroupConsumer>&
                                    subgroupConsumer) {
    subgroupConsumer->endOfTrackAndGroup(endOfTrackObjectID)
        .onError([this, sub](const auto& err) {
          forwarder_->handleSubgroupError(
              *sub, identifier_, err, "SubgroupForwarder::endOfTrackAndGroup");
        });
    closeSubgroupForSubscriber(sub, "SubgroupForwarder::endOfTrackAndGroup");
  });
  // Cleanup operation - succeed even with no subscribers
  return removeSubgroupAndCheckEmpty();
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfSubgroup() {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfSubgroup().onError([this, sub](const auto& err) {
          forwarder_->handleSubgroupError(
              *sub, identifier_, err, "SubgroupForwarder::endOfSubgroup");
        });
        closeSubgroupForSubscriber(sub, "SubgroupForwarder::endOfSubgroup");
      },
      /*makeNew=*/false,
      "endOfSubgroup");
  // Cleanup operation - succeed even with no subscribers
  return removeSubgroupAndCheckEmpty();
}

void MoQForwarder::SubgroupForwarder::reset(ResetStreamErrorCode error) {
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->reset(error);
        closeSubgroupForSubscriber(sub, "SubgroupForwarder::reset");
      });
  removeSubgroupAndCheckEmpty();
}

folly::Expected<ObjectPublishStatus, MoQPublishError>
MoQForwarder::SubgroupForwarder::objectPayload(
    Payload payload,
    bool finSubgroup) {
  if (!currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Haven't started publishing object"));
  }
  auto payloadLength = (payload) ? payload->computeChainDataLength() : 0;
  if (payloadLength > *currentObjectLength_) {
    return folly::makeUnexpected(
        MoQPublishError(MoQPublishError::API_ERROR, "Payload exceeded length"));
  }
  *currentObjectLength_ -= payloadLength;
  auto res = forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        XCHECK(subgroupConsumer);
        subgroupConsumer->objectPayload(maybeClone(payload), finSubgroup)
            .onError([this, sub](const auto& err) {
              forwarder_->handleSubgroupError(
                  *sub, identifier_, err, "SubgroupForwarder::objectPayload");
            });
        if (finSubgroup) {
          closeSubgroupForSubscriber(sub, "SubgroupForwarder::objectPayload");
        }
      },
      /*makeNew=*/false);
  // Check if object is complete and subgroup should be closed
  if (*currentObjectLength_ == 0) {
    currentObjectLength_.reset();
    if (finSubgroup) {
      removeSubgroupAndCheckEmpty();
      return ObjectPublishStatus::DONE;
    }
    // Data operation - propagate CANCELLED if no subscribers
    auto cleanupRes = cleanupOnError(res);
    if (cleanupRes.hasError()) {
      return folly::makeUnexpected(cleanupRes.error());
    }
    return ObjectPublishStatus::DONE;
  }

  // Data operation, propagate CANCELLED if no subscribers
  auto cleanupRes = cleanupOnError(res);
  if (cleanupRes.hasError()) {
    return folly::makeUnexpected(cleanupRes.error());
  }
  return ObjectPublishStatus::IN_PROGRESS;
}

} // namespace moxygen
