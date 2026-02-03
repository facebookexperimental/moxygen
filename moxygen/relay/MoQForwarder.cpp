/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQForwarder.h"

namespace moxygen {

// Template implementations

template <typename Fn>
folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::forEachSubscriber(
    Fn&& fn) {
  for (auto subscriberIt = subscribers_.begin();
       subscriberIt != subscribers_.end();) {
    const auto& sub = subscriberIt->second;
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
  forwarder_.forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (forwarder_.largest_ && forwarder_.checkRange(*sub)) {
      auto subgroupConsumerIt = sub->subgroups.find(identifier_);
      if (subgroupConsumerIt == sub->subgroups.end()) {
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
            identifier_.group, identifier_.subgroup, priority_);
        if (res.hasError()) {
          forwarder_.removeSubscriberOnError(*sub, res.error(), callsite);
        } else {
          auto emplaceRes = sub->subgroups.emplace(identifier_, res.value());
          subgroupConsumerIt = emplaceRes.first;
        }
      }
      if (!sub->checkShouldForward()) {
        // If we're attempting to send anything on an existing subgroup when
        // forward == false, then we reset the stream, so that we don't end
        // up with "holes" in the subgroup. If, at some point in the future,
        // we set forward = true, then we'll create a new stream for the
        // subgroup.
        subgroupConsumerIt->second->reset(ResetStreamErrorCode::INTERNAL_ERROR);
        closeSubgroupForSubscriber(
            sub, "SubgroupForwarder::forEachSubscriberSubgroup");
      } else {
        fn(sub, subgroupConsumerIt->second);
      }
    }
  });
  // Check if empty after iteration - subscribers may have been removed in loop
  if (forwarder_.subscribers_.empty()) {
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

void MoQForwarder::setDeliveryTimeout(uint64_t timeout) {
  upstreamDeliveryTimeout_ = std::chrono::milliseconds(timeout);
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
          largest_},
      std::move(session),
      subReq.requestID,
      toSubscribeRange(subReq, largest_),
      std::move(consumer),
      subReq.forward);
  if (upstreamDeliveryTimeout_.count() > 0) {
    subscriber->setParam(
        {folly::to_underlying(TrackRequestParamKey::DELIVERY_TIMEOUT),
         static_cast<uint64_t>(upstreamDeliveryTimeout_.count())});
  }
  subscribers_.emplace(sessionPtr, subscriber);
  if (subReq.forward) {
    addForwardingSubscriber();
  }
  return subscriber;
}

std::shared_ptr<MoQForwarder::Subscriber> MoQForwarder::addSubscriber(
    std::shared_ptr<MoQSession> session,
    const PublishRequest& pub) {
  if (draining_) {
    XLOG(ERR) << "addSubscriber called on draining track";
    return nullptr;
  }
  auto sessionPtr = session.get();
  auto subscriber = std::make_shared<MoQForwarder::Subscriber>(
      *this,
      SubscribeOk{
          pub.requestID,
          pub.trackAlias,
          std::chrono::milliseconds(0),
          pub.groupOrder,
          largest_},
      std::move(session),
      pub.requestID,
      SubscribeRange{{0, 0}, kLocationMax},
      nullptr,
      pub.forward);
  subscribers_.emplace(sessionPtr, subscriber);
  if (pub.forward) {
    addForwardingSubscriber();
  }
  return subscriber;
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
  subscriber.trackConsumer->publishDone(std::move(pubDone));

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
    subgroup.second->reset(ResetStreamErrorCode::CANCELLED);
  }
  if (pubDone) {
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

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::setTrackAlias(
    TrackAlias alias) {
  trackAlias_ = alias;
  return folly::unit;
}

folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
MoQForwarder::beginSubgroup(
    uint64_t groupID,
    uint64_t subgroupID,
    Priority priority) {
  updateLargest(groupID, 0);
  auto subgroupForwarder =
      std::make_shared<SubgroupForwarder>(*this, groupID, subgroupID, priority);
  SubgroupIdentifier subgroupIdentifier({groupID, subgroupID});
  auto res = forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    auto sgRes =
        sub->trackConsumer->beginSubgroup(groupID, subgroupID, priority);
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
    Payload payload) {
  updateLargest(header.group, header.id);
  return forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    sub->trackConsumer->objectStream(header, maybeClone(payload))
        .onError([this, sub](const auto& err) {
          removeSubscriberOnError(*sub, err, "objectStream");
        });
  });
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::datagram(
    const ObjectHeader& header,
    Payload payload) {
  updateLargest(header.group, header.id);
  return forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    sub->trackConsumer->datagram(header, maybeClone(payload))
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

folly::coro::Task<folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>>
MoQForwarder::Subscriber::subscribeUpdate(SubscribeUpdate subscribeUpdate) {
  // Validation:
  // - Start location can be updated
  // - End location can increase or decrease
  // - For bounded subscriptions (endGroup > 0), end must be >= start
  // - Forward state is optional and only updated if explicitly provided

  // Only update start if provided
  if (subscribeUpdate.start.has_value()) {
    range.start = *subscribeUpdate.start;
  }

  // Only update end if provided
  if (subscribeUpdate.endGroup.has_value()) {
    AbsoluteLocation newEnd{*subscribeUpdate.endGroup, 0};

    // Validate: for bounded end, the end must not be less than the start
    if (*subscribeUpdate.endGroup > 0 && range.start >= newEnd) {
      XLOG(ERR) << "Invalid subscribeUpdate: end Location " << newEnd
                << " is less than start " << range.start;
      session->close(SessionCloseErrorCode::PROTOCOL_VIOLATION);
      co_return folly::makeUnexpected(SubscribeUpdateError(
          subscribeUpdate.requestID,
          RequestErrorCode::INVALID_RANGE,
          "End Location is less than start"));
    }
    range.end = newEnd;
  }

  auto wasForwarding = shouldForward;
  // Only update forward state if explicitly provided (per draft 15+)
  if (subscribeUpdate.forward.has_value()) {
    shouldForward = *subscribeUpdate.forward;
  }

  if (shouldForward && !wasForwarding) {
    forwarder.addForwardingSubscriber();
  } else if (wasForwarding && !shouldForward) {
    forwarder.removeForwardingSubscriber();
  }

  co_return SubscribeUpdateOk{.requestID = subscribeUpdate.requestID};
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
    Priority priority)
    : forwarder_(forwarder),
      identifier_{group, subgroup},
      priority_(priority) {}

void MoQForwarder::SubgroupForwarder::closeSubgroupForSubscriber(
    const std::shared_ptr<Subscriber>& sub,
    const std::string& callsite) {
  sub->subgroups.erase(identifier_);
  // If this subscriber is draining and this was the last subgroup, remove it
  if (sub->shouldRemove()) {
    forwarder_.removeSubscriber(sub->session, std::nullopt, callsite);
  }
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::removeSubgroupAndCheckEmpty() {
  forwarder_.subgroups_.erase(identifier_);
  forwarder_.checkAndFireOnEmpty();
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
  forwarder_.updateLargest(identifier_.group, objectID);
  auto res = forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer
            ->object(objectID, maybeClone(payload), extensions, finSubgroup)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::object");
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
  forwarder_.updateLargest(identifier_.group, objectID);
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
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::beginObject");
            });
      }));
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfGroup(uint64_t endOfGroupObjectID) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  forwarder_.updateLargest(identifier_.group, endOfGroupObjectID);
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfGroup(endOfGroupObjectID)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::endOfGroup");
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
  forwarder_.updateLargest(identifier_.group, endOfTrackObjectID);
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfTrackAndGroup(endOfTrackObjectID)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::endOfTrackAndGroup");
            });
        closeSubgroupForSubscriber(
            sub, "SubgroupForwarder::endOfTrackAndGroup");
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
          forwarder_.removeSubscriberOnError(
              *sub, err, "SubgroupForwarder::endOfSubgroup");
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
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::objectPayload");
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
