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
      folly::Optional<AbsoluteLocation> latest = folly::none)
      : fullTrackName_(std::move(ftn)), latest_(std::move(latest)) {}

  void setGroupOrder(GroupOrder order) {
    groupOrder_ = order;
  }

  void setLatest(AbsoluteLocation latest) {
    latest_ = latest;
  }

  folly::Optional<AbsoluteLocation> latest() {
    return latest_;
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
        SubscribeID sid,
        TrackAlias ta,
        SubscribeRange r,
        std::shared_ptr<TrackConsumer> tc)
        : SubscriptionHandle(std::move(ok)),
          session(std::move(s)),
          subscribeID(sid),
          trackAlias(ta),
          range(r),
          trackConsumer(std::move(tc)),
          forwarder(f) {}

    // This method is for a relay to fixup the publisher group order of the
    // first subscriber if it was added before the upstream SubscribeOK.
    void setPublisherGroupOrder(GroupOrder pubGroupOrder) {
      subscribeOk_->groupOrder = MoQSession::resolveGroupOrder(
          pubGroupOrder, subscribeOk_->groupOrder);
    }

    void updateLatest(AbsoluteLocation latest) {
      subscribeOk_->latest = latest;
    }

    void subscribeUpdate(SubscribeUpdate subscribeUpdate) override {
      // TODO: Validate update subscription range conforms to SUBSCRIBE_UPDATE
      // rules
      // If it moved end before latest, then the next published object will
      // generate SUBSCRIBE_DONE
      range.start = subscribeUpdate.start;
      range.end = subscribeUpdate.end;
    }

    void unsubscribe() override {
      forwarder.removeSession(
          session,
          {subscribeID,
           SubscribeDoneStatusCode::UNSUBSCRIBED,
           0, // filled in by session
           "",
           forwarder.latest()});
    }

    std::shared_ptr<MoQSession> session;
    SubscribeID subscribeID;
    TrackAlias trackAlias;
    SubscribeRange range;
    std::shared_ptr<TrackConsumer> trackConsumer;
    // Stores the SubgroupConsumer for this subscriber for all currently
    // publishing subgroups.  Having this state here makes it easy to remove
    // a Subscriber and all open subgroups.
    SubgroupConsumerMap subgroups;
    MoQForwarder& forwarder;
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
            subReq.subscribeID,
            std::chrono::milliseconds(0),
            MoQSession::resolveGroupOrder(groupOrder_, subReq.groupOrder),
            latest_,
            {}},
        std::move(session),
        subReq.subscribeID,
        subReq.trackAlias,
        toSubscribeRange(subReq, latest_),
        std::move(consumer));
    subscribers_.emplace(sessionPtr, subscriber);
    return subscriber;
  }

  folly::Expected<StandaloneFetch, std::string> join(
      const std::shared_ptr<MoQSession>& session,
      const JoiningFetch& joining) const {
    auto subIt = subscribers_.find(session.get());
    if (subIt == subscribers_.end()) {
      XLOG(ERR) << "Session not found";
      return folly::makeUnexpected("Session has no active subscribe");
    }
    if (subIt->second->subscribeID != joining.joiningSubscribeID) {
      XLOG(ERR) << joining.joiningSubscribeID
                << " does not name a Subscribe "
                   " for this track";
      return folly::makeUnexpected("Incorrect SubscribeID for Track");
    }
    if (!subIt->second->subscribeOk().latest) {
      // No content exists, fetch error
      // Relay caller verifies upstream SubscribeOK has been processed before
      // calling join()
      return folly::makeUnexpected("No latest");
    }
    auto& latest = *subIt->second->subscribeOk().latest;
    AbsoluteLocation start{latest};
    start.group -= (start.group >= joining.precedingGroupOffset)
        ? joining.precedingGroupOffset
        : 0;
    start.object = 0;
    return StandaloneFetch{start, {latest.group, latest.object + 1}};
  }

  void removeSession(const std::shared_ptr<MoQSession>& session) {
    removeSession(
        session,
        {SubscribeID(0),
         SubscribeDoneStatusCode::GOING_AWAY,
         0, // filled in by session
         "byebyebye",
         latest_});
  }

  void removeSession(
      const std::shared_ptr<MoQSession>& session,
      SubscribeDone subDone) {
    auto subIt = subscribers_.find(session.get());
    if (subIt == subscribers_.end()) {
      // ?
      XLOG(ERR) << "Session not found";
      return;
    }
    subDone.subscribeID = subIt->second->subscribeID;
    subscribeDone(*subIt->second, subDone);
    subscribers_.erase(subIt);
    XLOG(DBG1) << "subscribers_.size()=" << subscribers_.size();
    if (subscribers_.empty() && callback_) {
      callback_->onEmpty(this);
    }
  }

  void subscribeDone(Subscriber& subscriber, SubscribeDone subDone) {
    // TODO: Resetting subgroups here is too aggressive
    XLOG(DBG1) << "Resetting open subgroups for subscriber=" << &subscriber;
    for (auto& subgroup : subscriber.subgroups) {
      subgroup.second->reset(ResetStreamErrorCode::CANCELLED);
    }
    subscriber.trackConsumer->subscribeDone(subDone);
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

  void updateLatest(uint64_t group, uint64_t object = 0) {
    AbsoluteLocation now{group, object};
    if (!latest_ || now > *latest_) {
      latest_ = now;
    }
  }

  bool checkRange(const Subscriber& sub) {
    XCHECK(latest_);
    if (*latest_ < sub.range.start) {
      // future
      return false;
    } else if (*latest_ > sub.range.end) {
      // now past, send subscribeDone
      // TOOD: maybe this is too early for a relay.
      removeSession(
          sub.session,
          {sub.subscribeID,
           SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
           0, // filled in by session
           "",
           sub.range.end});
      return false;
    }
    return true;
  }

  void removeSession(const Subscriber& sub, const MoQPublishError& err) {
    removeSession(
        sub.session,
        {sub.subscribeID,
         SubscribeDoneStatusCode::INTERNAL_ERROR,
         0, // filled in by session
         err.what(),
         sub.range.end});
  }

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override {
    updateLatest(groupID, 0);
    auto subgroupForwarder = std::make_shared<SubgroupForwarder>(
        *this, groupID, subgroupID, priority);
    SubgroupIdentifier subgroupIdentifier({groupID, subgroupID});
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      if (!checkRange(*sub)) {
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
    updateLatest(header.group, header.id);
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      if (!checkRange(*sub)) {
        return;
      }
      sub->trackConsumer->objectStream(header, maybeClone(payload))
          .onError([this, sub](const auto& err) { removeSession(*sub, err); });
    });
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError>
  groupNotExists(uint64_t groupID, uint64_t subgroup, Priority pri) override {
    updateLatest(groupID, 0);
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      if (!checkRange(*sub)) {
        return;
      }
      sub->trackConsumer->groupNotExists(groupID, subgroup, pri)
          .onError([this, sub](const auto& err) { removeSession(*sub, err); });
    });
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override {
    updateLatest(header.group, header.id);
    forEachSubscriber([&](const std::shared_ptr<Subscriber>& sub) {
      if (!checkRange(*sub)) {
        return;
      }
      sub->trackConsumer->datagram(header, maybeClone(payload))
          .onError([this, sub](const auto& err) { removeSession(*sub, err); });
    });
    return folly::unit;
  }

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override {
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
        if (forwarder_.latest_ && forwarder_.checkRange(*sub)) {
          auto subgroupConsumerIt = sub->subgroups.find(identifier_);
          if (subgroupConsumerIt == sub->subgroups.end()) {
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
          fn(sub, subgroupConsumerIt->second);
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

    folly::Expected<folly::Unit, MoQPublishError>
    object(uint64_t objectID, Payload payload, bool finSubgroup) override {
      if (currentObjectLength_) {
        return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::API_ERROR, "Still publishing previous object"));
      }
      forwarder_.updateLatest(identifier_.group, objectID);
      forEachSubscriberSubgroup(
          [&](const std::shared_ptr<Subscriber>& sub,
              const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
            subgroupConsumer->object(objectID, maybeClone(payload), finSubgroup)
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
        bool finSubgroup = false) override {
      if (currentObjectLength_) {
        return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::API_ERROR, "Still publishing previous object"));
      }
      forwarder_.updateLatest(identifier_.group, objectID);
      forEachSubscriberSubgroup(
          [&](const std::shared_ptr<Subscriber>& sub,
              const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
            subgroupConsumer->objectNotExists(objectID, finSubgroup)
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
        Payload initialPayload) override {
      // TODO: use a shared class for object publish state validation
      forwarder_.updateLatest(identifier_.group, objectID);
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
                ->beginObject(objectID, length, maybeClone(initialPayload))
                .onError([this, sub](const auto& err) {
                  forwarder_.removeSession(*sub, err);
                });
          });
      return folly::unit;
    }

    folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
        uint64_t endOfGroupObjectID) override {
      if (currentObjectLength_) {
        return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::API_ERROR, "Still publishing previous object"));
      }
      forwarder_.updateLatest(identifier_.group, endOfGroupObjectID);
      forEachSubscriberSubgroup(
          [&](const std::shared_ptr<Subscriber>& sub,
              const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
            subgroupConsumer->endOfGroup(endOfGroupObjectID)
                .onError([this, sub](const auto& err) {
                  forwarder_.removeSession(*sub, err);
                });
            sub->subgroups.erase(identifier_);
          });
      forwarder_.subgroups_.erase(identifier_);
      return folly::unit;
    }

    folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
        uint64_t endOfTrackObjectID) override {
      if (currentObjectLength_) {
        return folly::makeUnexpected(MoQPublishError(
            MoQPublishError::API_ERROR, "Still publishing previous object"));
      }
      forwarder_.updateLatest(identifier_.group, endOfTrackObjectID);
      forEachSubscriberSubgroup(
          [&](const std::shared_ptr<Subscriber>& sub,
              const std::shared_ptr<SubgroupConsumer>& subgroupConsumer) {
            subgroupConsumer->endOfTrackAndGroup(endOfTrackObjectID)
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
  folly::F14FastMap<MoQSession*, std::shared_ptr<Subscriber>> subscribers_;
  folly::F14FastMap<
      SubgroupIdentifier,
      std::shared_ptr<SubgroupForwarder>,
      SubgroupIdentifier::hash>
      subgroups_;
  GroupOrder groupOrder_{GroupOrder::OldestFirst};
  folly::Optional<AbsoluteLocation> latest_;
  std::shared_ptr<Callback> callback_;
};

} // namespace moxygen
