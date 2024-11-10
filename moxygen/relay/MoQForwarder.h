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

class MoQForwarder {
 public:
  explicit MoQForwarder(
      FullTrackName ftn,
      folly::Optional<AbsoluteLocation> latest = folly::none)
      : fullTrackName_(std::move(ftn)), latest_(std::move(latest)) {}

  void setGroupOrder(GroupOrder order) {
    groupOrder_ = order;
  }

  GroupOrder groupOrder() const {
    return groupOrder_;
  }

  void setLatest(AbsoluteLocation latest) {
    latest_ = latest;
  }

  folly::Optional<AbsoluteLocation> latest() {
    return latest_;
  }

  void setFinAfterEnd(bool finAfterEnd) {
    finAfterEnd_ = finAfterEnd;
  }

  struct Subscriber {
    std::shared_ptr<MoQSession> session;
    SubscribeID subscribeID;
    TrackAlias trackAlias;
    SubscribeRange range;

    struct hash {
      std::uint64_t operator()(const Subscriber& subscriber) const {
        return folly::hash::hash_combine(
            subscriber.session.get(),
            subscriber.subscribeID.value,
            subscriber.trackAlias.value,
            subscriber.range.start.group,
            subscriber.range.start.object,
            subscriber.range.end.group,
            subscriber.range.end.object);
      }
    };
    bool operator==(const Subscriber& other) const {
      return session == other.session && subscribeID == other.subscribeID &&
          trackAlias == other.trackAlias &&
          (range.start <=> other.range.start) ==
          std::strong_ordering::equivalent &&
          (range.end <=> other.range.end) == std::strong_ordering::equivalent;
    }
  };

  [[nodiscard]] bool empty() const {
    return subscribers_.empty();
  }

  void addSubscriber(Subscriber sub) {
    subscribers_.emplace(std::move(sub));
  }

  void addSubscriber(
      std::shared_ptr<MoQSession> session,
      SubscribeID subscribeID,
      TrackAlias trackAlias,
      const SubscribeRequest& sub) {
    subscribers_.emplace(Subscriber(
        {std::move(session),
         subscribeID,
         trackAlias,
         toSubscribeRange(sub, latest_)}));
  }

  bool updateSubscriber(const SubscribeUpdate& subscribeUpdate) {
    folly::F14NodeSet<Subscriber, Subscriber::hash>::iterator it =
        subscribers_.begin();
    for (; it != subscribers_.end();) {
      if (subscribeUpdate.subscribeID == it->subscribeID) {
        break;
      }
    }
    if (it == subscribers_.end()) {
      // subscribeID not found
      return false;
    }
    // Not implemented: Validation about subscriptions
    Subscriber subscriber = *it;
    subscribers_.erase(it);
    subscriber.range.start = subscribeUpdate.start;
    subscriber.range.end = subscribeUpdate.end;
    subscribers_.emplace(std::move(subscriber));
    return true;
  }

  void removeSession(
      const std::shared_ptr<MoQSession>& session,
      folly::Optional<SubscribeID> subID = folly::none) {
    // The same session could have multiple subscriptions, remove all of them
    // TODO: This shouldn't need to be a linear search
    for (auto it = subscribers_.begin(); it != subscribers_.end();) {
      if (it->session.get() == session.get() &&
          (!subID || *subID == it->subscribeID)) {
        if (subID) {
          it->session->subscribeDone(
              {*subID,
               SubscribeDoneStatusCode::UNSUBSCRIBED,
               "byebyebye",
               latest_});
        } // else assume the session went away ungracefully
        XLOG(DBG1) << "Removing session from forwarder";
        it = subscribers_.erase(it);
      } else {
        it++;
      }
    }
    XLOG(DBG1) << "subscribers_.size()=" << subscribers_.size();
  }

  void publish(
      ObjectHeader objHeader,
      std::unique_ptr<folly::IOBuf> payload,
      uint64_t payloadOffset = 0,
      bool eom = true,
      bool streamPerObject = false) {
    AbsoluteLocation now{objHeader.group, objHeader.id};
    if (!latest_ || now > *latest_) {
      latest_ = now;
    }
    for (auto it = subscribers_.begin(); it != subscribers_.end();) {
      auto& sub = *it;
      if (sub.range.start > now) {
        // future subscriber
        it++;
        continue;
      }
      auto evb = sub.session->getEventBase();
      if (sub.range.end < now) {
        // subscription over
        if (finAfterEnd_) {
          evb->runImmediatelyOrRunInEventBaseThread([session = sub.session,
                                                     subId = sub.subscribeID,
                                                     now,
                                                     trackName =
                                                         fullTrackName_] {
            session->subscribeDone(
                {subId, SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, "", now});
          });
        }
        it = subscribers_.erase(it);
      } else {
        evb->runImmediatelyOrRunInEventBaseThread(
            [session = sub.session,
             subId = sub.subscribeID,
             trackAlias = sub.trackAlias,
             objHeader,
             payloadOffset,
             buf = (payload) ? payload->clone() : nullptr,
             eom,
             streamPerObject]() mutable {
              objHeader.trackIdentifier = trackAlias;
              if (objHeader.status != ObjectStatus::NORMAL) {
                session->publishStatus(objHeader, subId);
              } else if (streamPerObject) {
                session->publishStreamPerObject(
                    objHeader, subId, payloadOffset, std::move(buf), eom);
              } else {
                session->publish(
                    objHeader, subId, payloadOffset, std::move(buf), eom);
              }
            });
        it++;
      }
    }
  }

  void error(SubscribeDoneStatusCode errorCode, std::string reasonPhrase) {
    for (auto sub : subscribers_) {
      sub.session->subscribeDone(
          {sub.subscribeID, errorCode, reasonPhrase, latest_});
    }
    subscribers_.clear();
  }

 private:
  FullTrackName fullTrackName_;
  folly::F14NodeSet<Subscriber, Subscriber::hash> subscribers_;
  GroupOrder groupOrder_{GroupOrder::OldestFirst};
  folly::Optional<AbsoluteLocation> latest_;
  bool finAfterEnd_{true};
};

} // namespace moxygen
