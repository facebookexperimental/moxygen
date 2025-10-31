/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQLocation.h"
#include "moxygen/MoQSession.h"

#include <folly/container/F14Set.h>
#include <folly/hash/Hash.h>
#include <folly/io/async/EventBase.h>

namespace moxygen {

class MoQForwarder : public TrackConsumer {
 public:
  explicit MoQForwarder(
      FullTrackName ftn,
      folly::Optional<AbsoluteLocation> largest = folly::none)
      : fullTrackName_(std::move(ftn)), largest_(std::move(largest)) {}

  const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  GroupOrder groupOrder() const {
    return groupOrder_;
  }

  void setGroupOrder(GroupOrder order) {
    groupOrder_ = order;
  }

  void setDeliveryTimeout(uint64_t timeout) {
    upstreamDeliveryTimeout_ = std::chrono::milliseconds(timeout);
  }

  std::chrono::milliseconds upstreamDeliveryTimeout() const {
    return upstreamDeliveryTimeout_;
  }

  void setLargest(AbsoluteLocation largest) {
    largest_ = largest;
  }

  folly::Optional<AbsoluteLocation> largest() {
    return largest_;
  }

  class Callback {
   public:
    virtual ~Callback() = default;
    virtual void onEmpty(MoQForwarder*) = 0;
  };

  void setCallback(std::shared_ptr<Callback> callback) {
    callback_ = std::move(callback);
  }

  struct SubgroupIdentifier {
    uint64_t group;
    uint64_t subgroup;
    struct hash {
      size_t operator()(const SubgroupIdentifier& id) const {
        return folly::hash::hash_combine(id.group, id.subgroup);
      }
    };
    bool operator==(const SubgroupIdentifier& other) const {
      return group == other.group && subgroup == other.subgroup;
    }
  };
  class SubgroupForwarder;
  struct Subscriber : public Publisher::SubscriptionHandle {
    using SubgroupConsumerMap = folly::F14FastMap<
        SubgroupIdentifier,
        std::shared_ptr<SubgroupConsumer>,
        SubgroupIdentifier::hash>;

    Subscriber(
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

    // This method is for a relay to fixup the publisher group order of the
    // first subscriber if it was added before the upstream SubscribeOK.
    void setPublisherGroupOrder(GroupOrder pubGroupOrder) {
      subscribeOk_->groupOrder = MoQSession::resolveGroupOrder(
          pubGroupOrder, subscribeOk_->groupOrder);
    }

    void updateLargest(AbsoluteLocation largest) {
      subscribeOk_->largest = largest;
    }

    // Updates the params of the subscribeOk
    // updates existing param if key matches, otherwise adds new param
    void setParam(const TrackRequestParameter& param) {
      for (auto& existingParam : subscribeOk_->params) {
        if (existingParam.key == param.key) {
          existingParam = param;
          return;
        }
      }
      subscribeOk_->params.push_back(param);
    }

    void subscribeUpdate(SubscribeUpdate subscribeUpdate) override {
      // TODO: Validate update subscription range conforms to SUBSCRIBE_UPDATE
      // rules
      // If it moved end before largest, then the next published object will
      // generate SUBSCRIBE_DONE
      range.start = subscribeUpdate.start;
      range.end = {subscribeUpdate.endGroup, 0};
      shouldForward = subscribeUpdate.forward;
    }

    void unsubscribe() override {
      forwarder.removeSession(session);
    }

    bool checkShouldForward() {
      if (!shouldForward) {
        XLOG(DBG6) << "shouldForward is false of requestID " << requestID;
      }
      return shouldForward;
    }

    std::shared_ptr<MoQSession> session;
    RequestID requestID;
    SubscribeRange range;
    std::shared_ptr<TrackConsumer> trackConsumer;
    // Stores the SubgroupConsumer for this subscriber for all currently
    // publishing subgroups.  Having this state here makes it easy to remove
    // a Subscriber and all open subgroups.
    SubgroupConsumerMap subgroups;
    MoQForwarder& forwarder;
    bool shouldForward;
  };

  [[nodiscard]] bool empty() const {
    return subscribers_.empty();
  }

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      const SubscribeRequest& subReq,
      std::shared_ptr<TrackConsumer> consumer) {
    auto sessionPtr = session.get();
    auto subscriber = std::make_shared<MoQForwarder::Subscriber>(
        *this,
        SubscribeOk{
            subReq.requestID,
            subReq.trackAlias.value_or(
                trackAlias_.value_or(TrackAlias(subReq.requestID.value))),
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
           "",
           static_cast<uint64_t>(upstreamDeliveryTimeout_.count()),
           {}});
    }
    subscribers_.emplace(sessionPtr, subscriber);
    return subscriber;
  }

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
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
    return subscriber;
  }

  folly::Expected<SubscribeRange, FetchError> resolveJoiningFetch(
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
          FetchError{
              RequestID(0), FetchErrorCode::INTERNAL_ERROR, "No largest"});
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

  void removeSession(
      const std::shared_ptr<MoQSession>& session,
      folly::Optional<SubscribeDone> subDone = folly::none) {
    XLOG(DBG1) << __func__ << " session=" << session.get();
    auto subIt = subscribers_.find(session.get());
    if (subIt == subscribers_.end()) {
      // ?
      XLOG(ERR) << "Session not found";
      return;
    }
    subscribeDone(*subIt->second, subDone);
    subscribers_.erase(subIt);
    XLOG(DBG1) << "subscribers_.size()=" << subscribers_.size();
    if (subscribers_.empty() && callback_) {
      callback_->onEmpty(this);
    }
  }

  void subscribeDone(
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

  void forEachSubscriber(
      std::function<void(const std::shared_ptr<Subscriber>&)> fn) {
    for (auto subscriberIt = subscribers_.begin();
         subscriberIt != subscribers_.end();) {
      const auto& sub = subscriberIt->second;
      subscriberIt++;
      fn(sub);
    }
  }

  void updateLargest(uint64_t group, uint64_t object = 0) {
    AbsoluteLocation now{group, object};
    if (!largest_ || now > *largest_) {
      largest_ = now;
    }
  }

  bool checkRange(const Subscriber& sub) {
    XCHECK(largest_);
    if (*largest_ < sub.range.start) {
      // future
      return false;
    } else if (*largest_ > sub.range.end) {
      // now past, send subscribeDone
      // TOOD: maybe this is too early for a relay.
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

  void removeSession(const Subscriber& sub, const MoQPublishError& err) {
    removeSession(
        sub.session,
        SubscribeDone{
            sub.requestID,
            SubscribeDoneStatusCode::INTERNAL_ERROR,
            0, // filled in by session
            err.what()});
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override {
    trackAlias_ = alias;
    return folly::unit;
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override {
    updateLargest(groupID, 0);
    auto subgroupForwarder = std::make_shared<SubgroupForwarder>(
        *this, groupID, subgroupID, priority);
    SubgroupIdentifier subgroupIdentifier({groupID, subgroupID});
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      if (!checkRange(*sub) || !sub->checkShouldForward()) {
        return;
      }
      auto res =
          sub->trackConsumer->beginSubgroup(groupID, subgroupID, priority);
      if (res.hasError()) {
        removeSession(*sub, res.error());
      } else {
        sub->subgroups[subgroupIdentifier] = res.value();
      }
    });
    subgroups_.emplace(subgroupIdentifier, subgroupForwarder);
    return subgroupForwarder;
  }

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override {
    return folly::makeSemiFuture();
  }

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload) override {
    updateLargest(header.group, header.id);
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      if (!checkRange(*sub) || !sub->checkShouldForward()) {
        return;
      }
      sub->trackConsumer->objectStream(header, maybeClone(payload))
          .onError([this, sub](const auto& err) { removeSession(*sub, err); });
    });
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroup,
      Priority pri,
      Extensions extensions) override {
    updateLargest(groupID, 0);
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      if (!checkRange(*sub) || !sub->checkShouldForward()) {
        return;
      }
      sub->trackConsumer->groupNotExists(groupID, subgroup, pri, extensions)
          .onError([this, sub](const auto& err) { removeSession(*sub, err); });
    });
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override {
    updateLargest(header.group, header.id);
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      if (!checkRange(*sub) || !sub->checkShouldForward()) {
        return;
      }
      sub->trackConsumer->datagram(header, maybeClone(payload))
          .onError([this, sub](const auto& err) { removeSession(*sub, err); });
    });
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override {
    XLOG(DBG1) << __func__ << " subDone reason=" << subDone.reasonPhrase;
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      removeSession(sub->session, subDone);
    });
    return folly::unit;
  }

  class SubgroupForwarder : public SubgroupConsumer {
    folly::Optional<uint64_t> currentObjectLength_;
    MoQForwarder& forwarder_;
    SubgroupIdentifier identifier_;
    Priority priority_;

    void forEachSubscriberSubgroup(
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
              forwarder_.removeSession(*sub, res.error());
            } else {
              auto emplaceRes =
                  sub->subgroups.emplace(identifier_, res.value());
              subgroupConsumerIt = emplaceRes.first;
            }
          }
          if (!sub->checkShouldForward()) {
            // If we're attempting to send anything on an existing subgroup when
            // forward == false, then we reset the stream, so that we don't end
            // up with "holes" in the subgroup. If, at some point in the future,
            // we set forward = true, then we'll create a new stream for the
            // subgroup.
            subgroupConsumerIt->second->reset(
                ResetStreamErrorCode::INTERNAL_ERROR);
            sub->subgroups.erase(identifier_);
          } else {
            fn(sub, subgroupConsumerIt->second);
          }
        }
      });
    }

   public:
    SubgroupForwarder(
        MoQForwarder& forwarder,
        uint64_t group,
        uint64_t subgroup,
        Priority priority)
        : forwarder_(forwarder),
          identifier_{group, subgroup},
          priority_(priority) {}

    folly::Expected<folly::Unit, MoQPublishError> object(
        uint64_t objectID,
        Payload payload,
        Extensions extensions,
        bool finSubgroup) override {
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
                  forwarder_.removeSession(*sub, err);
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

    folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
        uint64_t objectID,
        Extensions extensions,
        bool finSubgroup = false) override {
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
                  forwarder_.removeSession(*sub, err);
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

    folly::Expected<folly::Unit, MoQPublishError> beginObject(
        uint64_t objectID,
        uint64_t length,
        Payload initialPayload,
        Extensions extensions) override {
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
                  forwarder_.removeSession(*sub, err);
                });
          });
      return folly::unit;
    }

    folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
        uint64_t endOfGroupObjectID,
        Extensions extensions) override {
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
                  forwarder_.removeSession(*sub, err);
                });
            sub->subgroups.erase(identifier_);
          });
      forwarder_.subgroups_.erase(identifier_);
      return folly::unit;
    }

    folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
        uint64_t endOfTrackObjectID,
        Extensions extensions) override {
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
                  forwarder_.removeSession(*sub, err);
                });
            sub->subgroups.erase(identifier_);
          });
      forwarder_.subgroups_.erase(identifier_);
      return folly::unit;
    }

    folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override {
      if (currentObjectLength_) {
        return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::API_ERROR, "Still publishing previous object"));
      }
      forEachSubscriberSubgroup(
          [&](const std::shared_ptr<Subscriber>& sub,
              const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
            subgroupConsumer->endOfSubgroup().onError(
                [this, sub](const auto& err) {
                  forwarder_.removeSession(*sub, err);
                });
            sub->subgroups.erase(identifier_);
          });
      forwarder_.subgroups_.erase(identifier_);
      return folly::unit;
    }

    void reset(ResetStreamErrorCode error) override {
      forEachSubscriberSubgroup(
          [&](const std::shared_ptr<Subscriber>& sub,
              const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
            subgroupConsumer->reset(error);
            sub->subgroups.erase(identifier_);
          });
      forwarder_.subgroups_.erase(identifier_);
    }

    folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
        Payload payload,
        bool finSubgroup = false) override {
      if (!currentObjectLength_) {
        return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::API_ERROR, "Haven't started publishing object"));
      }
      auto payloadLength = (payload) ? payload->computeChainDataLength() : 0;
      if (payloadLength > *currentObjectLength_) {
        return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::API_ERROR, "Payload exceeded length"));
      }
      *currentObjectLength_ -= payloadLength;
      forEachSubscriberSubgroup(
          [&](const std::shared_ptr<Subscriber>& sub,
              const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
            subgroupConsumer->objectPayload(maybeClone(payload), finSubgroup)
                .onError([this, sub](const auto& err) {
                  forwarder_.removeSession(*sub, err);
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
  };

 private:
  static Payload maybeClone(const Payload& payload) {
    return payload ? payload->clone() : nullptr;
  }

  FullTrackName fullTrackName_;
  folly::Optional<TrackAlias> trackAlias_;
  folly::F14FastMap<MoQSession*, std::shared_ptr<Subscriber>> subscribers_;
  folly::F14FastMap<
      SubgroupIdentifier,
      std::shared_ptr<SubgroupForwarder>,
      SubgroupIdentifier::hash>
      subgroups_;
  GroupOrder groupOrder_{GroupOrder::OldestFirst};
  folly::Optional<AbsoluteLocation> largest_;
  // This should eventually be a vector of params that can be cascaded e2e
  std::chrono::milliseconds upstreamDeliveryTimeout_{};
  std::shared_ptr<Callback> callback_;
};

} // namespace moxygen
