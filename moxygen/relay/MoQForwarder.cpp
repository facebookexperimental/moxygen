/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/relay/MoQForwarder.h"

namespace moxygen {

// MoQForwarder implementation

MoQForwarder::MoQForwarder(
    FullTrackName ftn,
    folly::Optional<AbsoluteLocation> largest)
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
  auto sessionPtr = session.get();
  auto trackAlias =
      subReq.trackAlias.value_or(trackAlias_.value_or(subReq.requestID.value));
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
          {}},
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
  auto sessionPtr = session.get();
  auto subscriber = std::make_shared<MoQForwarder::Subscriber>(
      *this,
      SubscribeOk{
          pub.requestID,
          pub.trackAlias,
          std::chrono::milliseconds(0),
          pub.groupOrder,
          largest_,
          {}},
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

void MoQForwarder::removeSession(
    const std::shared_ptr<MoQSession>& session,
    folly::Optional<SubscribeDone> subDone) {
  XLOG(DBG1) << __func__ << " session=" << session.get();
  auto subIt = subscribers_.find(session.get());
  if (subIt == subscribers_.end()) {
    // ?
    XLOG(ERR) << "Session not found";
    return;
  }
  subscribeDone(*subIt->second, subDone);
  if (subIt->second->shouldForward) {
    if (subscribers_.size() == 1) {
      // don't trigger a forwardUpdated callback here, we will trigger onEmpty
      forwardingSubscribers_--;
    } else {
      removeForwardingSubscriber();
    }
  }
  subscribers_.erase(subIt);
  XLOG(DBG1) << "subscribers_.size()=" << subscribers_.size();
  if (subscribers_.empty() && callback_) {
    callback_->onEmpty(this);
  }
}

void MoQForwarder::subscribeDone(
    Subscriber& subscriber,
    folly::Optional<SubscribeDone> subDone) {
  // TODO: Resetting subgroups here is too aggressive
  XLOG(DBG1) << "Resetting open subgroups for subscriber=" << &subscriber;
  for (auto& subgroup : subscriber.subgroups) {
    subgroup.second->reset(ResetStreamErrorCode::CANCELLED);
  }
  if (subDone) {
    subDone->requestID = subscriber.requestID;
    subscriber.trackConsumer->subscribeDone(*subDone);
  }
}

void MoQForwarder::forEachSubscriber(
    std::function<void(const std::shared_ptr<Subscriber>&)> fn) {
  for (auto subscriberIt = subscribers_.begin();
       subscriberIt != subscribers_.end();) {
    const auto& sub = subscriberIt->second;
    subscriberIt++;
    fn(sub);
  }
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
    // now past, send subscribeDone
    // TOOD: maybe this is too early for a relay.
    XLOG(DBG4) << "removeSession from checkRange";
    removeSession(
        sub.session,
        SubscribeDone{
            sub.requestID,
            SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
            0, // filled in by session
            ""});
    return false;
  }
  return true;
}

void MoQForwarder::removeSubscriberOnError(
    const Subscriber& sub,
    const MoQPublishError& err,
    const std::string& where) {
  XLOG(ERR) << "Removing subscriber after error in " << where
            << " err=" << err.what();
  removeSession(
      sub.session,
      SubscribeDone{
          sub.requestID,
          SubscribeDoneStatusCode::INTERNAL_ERROR,
          0, // filled in by session
          err.what()});
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
  forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    auto res = sub->trackConsumer->beginSubgroup(groupID, subgroupID, priority);
    if (res.hasError()) {
      removeSubscriberOnError(*sub, res.error(), "beginSubgroup");
    } else {
      sub->subgroups[subgroupIdentifier] = res.value();
    }
  });
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
  forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    sub->trackConsumer->objectStream(header, maybeClone(payload))
        .onError([this, sub](const auto& err) {
          removeSubscriberOnError(*sub, err, "objectStream");
        });
  });
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::groupNotExists(
    uint64_t groupID,
    uint64_t subgroup,
    Priority pri,
    Extensions extensions) {
  updateLargest(groupID, 0);
  forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    sub->trackConsumer->groupNotExists(groupID, subgroup, pri, extensions)
        .onError([this, sub](const auto& err) {
          removeSubscriberOnError(*sub, err, "groupNotExists");
        });
  });
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::datagram(
    const ObjectHeader& header,
    Payload payload) {
  updateLargest(header.group, header.id);
  forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (!checkRange(*sub) || !sub->checkShouldForward()) {
      return;
    }
    sub->trackConsumer->datagram(header, maybeClone(payload))
        .onError([this, sub](const auto& err) {
          removeSubscriberOnError(*sub, err, "datagram");
        });
  });
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError> MoQForwarder::subscribeDone(
    SubscribeDone subDone) {
  XLOG(DBG1) << __func__ << " subDone reason=" << subDone.reasonPhrase;
  forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    removeSession(
        sub->session,
        SubscribeDone{
            sub->requestID,
            subDone.statusCode,
            0, // filled in by session
            subDone.reasonPhrase});
  });
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
  subscribeOk_->params.insertParam(param);
}

void MoQForwarder::Subscriber::subscribeUpdate(
    SubscribeUpdate subscribeUpdate) {
  // TODO: Validate update subscription range conforms to SUBSCRIBE_UPDATE
  // rules
  // If it moved end before largest, then the next published object will
  // generate SUBSCRIBE_DONE
  range.start = subscribeUpdate.start;
  range.end = {subscribeUpdate.endGroup, 0};
  auto wasForwarding = shouldForward;
  // Only update forward state if explicitly provided (per draft 15+)
  if (subscribeUpdate.forward.hasValue()) {
    shouldForward = *subscribeUpdate.forward;
  }
  if (shouldForward && !wasForwarding) {
    forwarder.addForwardingSubscriber();
  } else if (wasForwarding && !shouldForward) {
    forwarder.removeForwardingSubscriber();
  }
}

void MoQForwarder::Subscriber::unsubscribe() {
  XLOG(DBG4) << "unsubscribe sess=" << this;
  forwarder.removeSession(session);
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

void MoQForwarder::SubgroupForwarder::forEachSubscriberSubgroup(
    std::function<void(
        const std::shared_ptr<Subscriber>& sub,
        const std::shared_ptr<SubgroupConsumer>&)> fn) {
  forwarder_.forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
    if (forwarder_.largest_ && forwarder_.checkRange(*sub)) {
      auto subgroupConsumerIt = sub->subgroups.find(identifier_);
      if (subgroupConsumerIt == sub->subgroups.end()) {
        if (!sub->checkShouldForward()) {
          // If shouldForward == false, we shouldn't be creating any
          // subgroups.
          return;
        }
        auto res = sub->trackConsumer->beginSubgroup(
            identifier_.group, identifier_.subgroup, priority_);
        if (res.hasError()) {
          forwarder_.removeSubscriberOnError(
              *sub,
              res.error(),
              "SubgroupForwarder::forEachSubscriberSubgroup");
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
        sub->subgroups.erase(identifier_);
      } else {
        fn(sub, subgroupConsumerIt->second);
      }
    }
  });
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
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer
            ->object(objectID, maybeClone(payload), extensions, finSubgroup)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::object");
            });
        if (finSubgroup) {
          sub->subgroups.erase(identifier_);
        }
      });
  if (finSubgroup) {
    forwarder_.subgroups_.erase(identifier_);
  }
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::objectNotExists(
    uint64_t objectID,
    Extensions extensions,
    bool finSubgroup) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  forwarder_.updateLargest(identifier_.group, objectID);
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->objectNotExists(objectID, extensions, finSubgroup)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::objectNotExists");
            });
        if (finSubgroup) {
          sub->subgroups.erase(identifier_);
        }
      });
  if (finSubgroup) {
    forwarder_.subgroups_.erase(identifier_);
  }
  return folly::unit;
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
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer
            ->beginObject(
                objectID, length, maybeClone(initialPayload), extensions)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::beginObject");
            });
      });
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfGroup(
    uint64_t endOfGroupObjectID,
    Extensions extensions) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  forwarder_.updateLargest(identifier_.group, endOfGroupObjectID);
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfGroup(endOfGroupObjectID, extensions)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::endOfGroup");
            });
        sub->subgroups.erase(identifier_);
      });
  forwarder_.subgroups_.erase(identifier_);
  return folly::unit;
}

folly::Expected<folly::Unit, MoQPublishError>
MoQForwarder::SubgroupForwarder::endOfTrackAndGroup(
    uint64_t endOfTrackObjectID,
    Extensions extensions) {
  if (currentObjectLength_) {
    return folly::makeUnexpected(MoQPublishError(
        MoQPublishError::API_ERROR, "Still publishing previous object"));
  }
  forwarder_.updateLargest(identifier_.group, endOfTrackObjectID);
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->endOfTrackAndGroup(endOfTrackObjectID, extensions)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::endOfTrackAndGroup");
            });
        sub->subgroups.erase(identifier_);
      });
  forwarder_.subgroups_.erase(identifier_);
  return folly::unit;
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
        sub->subgroups.erase(identifier_);
      });
  forwarder_.subgroups_.erase(identifier_);
  return folly::unit;
}

void MoQForwarder::SubgroupForwarder::reset(ResetStreamErrorCode error) {
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->reset(error);
        sub->subgroups.erase(identifier_);
      });
  forwarder_.subgroups_.erase(identifier_);
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
  forEachSubscriberSubgroup(
      [&](const std::shared_ptr<Subscriber>& sub,
          const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
        subgroupConsumer->objectPayload(maybeClone(payload), finSubgroup)
            .onError([this, sub](const auto& err) {
              forwarder_.removeSubscriberOnError(
                  *sub, err, "SubgroupForwarder::objectPayload");
            });
        if (finSubgroup) {
          sub->subgroups.erase(identifier_);
        }
      });
  if (*currentObjectLength_ == 0) {
    currentObjectLength_.reset();
    if (finSubgroup) {
      forwarder_.subgroups_.erase(identifier_);
    }
    return ObjectPublishStatus::DONE;
  }
  return ObjectPublishStatus::IN_PROGRESS;
}

} // namespace moxygen
