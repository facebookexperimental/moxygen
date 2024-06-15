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
  explicit MoQForwarder(FullTrackName ftn) : fullTrackName_(std::move(ftn)) {}

  void setFinAfterEnd(bool finAfterEnd) {
    finAfterEnd_ = finAfterEnd;
  }

  struct Subscriber {
    std::shared_ptr<MoQSession> session;
    uint64_t subscribeID;
    uint64_t trackAlias;
    AbsoluteLocation start;
    AbsoluteLocation end;

    struct hash {
      std::uint64_t operator()(const Subscriber& subscriber) const {
        return folly::hash::hash_combine(
            subscriber.session.get(),
            subscriber.subscribeID,
            subscriber.trackAlias,
            subscriber.start.group,
            subscriber.start.object,
            subscriber.end.group,
            subscriber.end.object);
      }
    };
    bool operator==(const Subscriber& other) const {
      return session == other.session && subscribeID == other.subscribeID &&
          trackAlias == other.trackAlias &&
          (start <=> other.start) == std::strong_ordering::equivalent &&
          (end <=> other.end) == std::strong_ordering::equivalent;
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
      uint64_t subscribeID,
      uint64_t trackAlias,
      const SubscribeRequest& sub) {
    auto start = toAbsolulte(
        sub.startGroup, sub.startObject, current_.group, current_.object);
    auto end = toAbsolulte(
        sub.endGroup, sub.endObject, current_.group, current_.object);
    subscribers_.emplace(
        Subscriber({std::move(session), subscribeID, trackAlias, start, end}));
  }

  void removeSession(
      const std::shared_ptr<MoQSession>& session,
      folly::Optional<uint64_t> subID = folly::none) {
    // The same session could have multiple subscriptions, remove all of them
    for (auto it = subscribers_.begin(); it != subscribers_.end();) {
      if (it->session.get() == session.get() &&
          (!subID || *subID == it->subscribeID)) {
        if (subID) {
          it->session->subscribeDone(
              {*subID,
               SubscribeDoneStatusCode::UNSUBSCRIBED,
               "byebyebye",
               GroupAndObject({current_.group, current_.object})});
        } // else assume the session went away ungracefully
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
      bool eom = true) {
    AbsoluteLocation now{objHeader.group, objHeader.id};
    if (now > current_) {
      current_ = now;
    }
    for (auto it = subscribers_.begin(); it != subscribers_.end();) {
      auto& sub = *it;
      if (sub.start > now) {
        // future subscriber
        it++;
        continue;
      }
      auto evb = sub.session->getEventBase();
      if (sub.end < now) {
        // subscription over
        if (finAfterEnd_) {
          evb->runImmediatelyOrRunInEventBaseThread(
              [session = sub.session,
               subId = sub.subscribeID,
               now,
               trackName = fullTrackName_] {
                session->subscribeDone(
                    {subId,
                     SubscribeDoneStatusCode::SUBSCRIPTION_ENDED,
                     "",
                     GroupAndObject({now.group, now.object})});
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
             eom]() mutable {
              objHeader.subscribeID = subId;
              objHeader.trackAlias = trackAlias;
              session->publish(objHeader, payloadOffset, std::move(buf), eom);
            });
        it++;
      }
    }
  }

  void error(SubscribeDoneStatusCode errorCode, std::string reasonPhrase) {
    for (auto sub : subscribers_) {
      sub.session->subscribeDone(
          {sub.subscribeID,
           errorCode,
           reasonPhrase,
           GroupAndObject({current_.group, current_.object})});
    }
    subscribers_.clear();
  }

 private:
  FullTrackName fullTrackName_;
  folly::F14NodeSet<Subscriber, Subscriber::hash> subscribers_;
  AbsoluteLocation current_{0, 0};
  bool finAfterEnd_{true};
};

} // namespace moxygen
