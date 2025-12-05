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
      folly::Optional<AbsoluteLocation> largest = folly::none);

  const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  GroupOrder groupOrder() const {
    return groupOrder_;
  }

  void setGroupOrder(GroupOrder order) {
    groupOrder_ = order;
  }

  void setDeliveryTimeout(uint64_t timeout);

  std::chrono::milliseconds upstreamDeliveryTimeout() const {
    return upstreamDeliveryTimeout_;
  }

  void setLargest(AbsoluteLocation largest);

  folly::Optional<AbsoluteLocation> largest() {
    return largest_;
  }

  class Callback {
   public:
    virtual ~Callback() = default;
    virtual void onEmpty(MoQForwarder*) = 0;
    virtual void forwardChanged(MoQForwarder*) {}
  };

  void setCallback(std::shared_ptr<Callback> callback);

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
        bool shouldForwardIn);

    // This method is for a relay to fixup the publisher group order of the
    // first subscriber if it was added before the upstream SubscribeOK.
    void setPublisherGroupOrder(GroupOrder pubGroupOrder);

    void updateLargest(AbsoluteLocation largest);

    // Updates the params of the subscribeOk
    // updates existing param if key matches, otherwise adds new param
    void setParam(const TrackRequestParameter& param);

    void subscribeUpdate(SubscribeUpdate subscribeUpdate) override;

    void unsubscribe() override;

    bool checkShouldForward();

    // Returns true if this subscriber is draining and all subgroups are closed
    bool shouldRemove() const {
      return receivedSubscribeDone_ && subgroups.empty();
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
    bool receivedSubscribeDone_{false};
  };

  [[nodiscard]] bool empty() const {
    return subscribers_.empty();
  }

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      const SubscribeRequest& subReq,
      std::shared_ptr<TrackConsumer> consumer);

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      const PublishRequest& pub);

  folly::Expected<SubscribeRange, FetchError> resolveJoiningFetch(
      const std::shared_ptr<MoQSession>& session,
      const JoiningFetch& joining) const;

  // Gracefully drains a subscriber - forwards subscribeDone but doesn't reset
  // open subgroups. Calls removeSubscriber() if no subgroups are open.
  void drainSubscriber(
      const std::shared_ptr<MoQSession>& session,
      SubscribeDone subDone,
      const std::string& callsite);

  // Immediately removes a session - resets all open subgroups and removes
  // from subscribers map
  void removeSubscriber(
      const std::shared_ptr<MoQSession>& session,
      folly::Optional<SubscribeDone> subDone,
      const std::string& callsite);

  void forEachSubscriber(
      std::function<void(const std::shared_ptr<Subscriber>&)> fn);

  void updateLargest(uint64_t group, uint64_t object = 0);

  bool checkRange(const Subscriber& sub);

  void removeSubscriberOnError(
      const Subscriber& sub,
      const MoQPublishError& err,
      const std::string& callsite /*for logging*/);

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override;

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(uint64_t groupID, uint64_t subgroupID, Priority priority)
      override;

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override;

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError> groupNotExists(
      uint64_t groupID,
      uint64_t subgroup,
      Priority pri,
      Extensions extensions) override;

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError> subscribeDone(
      SubscribeDone subDone) override;

  class SubgroupForwarder : public SubgroupConsumer {
    folly::Optional<uint64_t> currentObjectLength_;
    MoQForwarder& forwarder_;
    SubgroupIdentifier identifier_;
    Priority priority_;

    void forEachSubscriberSubgroup(
        std::function<void(
            const std::shared_ptr<Subscriber>& sub,
            const std::shared_ptr<SubgroupConsumer>&)> fn,
        bool makeNew = true,
        const std::string& callsite = "");

    // Helper to erase subgroup from subscriber and remove subscriber if
    // draining
    void closeSubgroupForSubscriber(
        const std::shared_ptr<Subscriber>& sub,
        const std::string& callsite);

   public:
    SubgroupForwarder(
        MoQForwarder& forwarder,
        uint64_t group,
        uint64_t subgroup,
        Priority priority);

    folly::Expected<folly::Unit, MoQPublishError> object(
        uint64_t objectID,
        Payload payload,
        Extensions extensions,
        bool finSubgroup) override;

    folly::Expected<folly::Unit, MoQPublishError> objectNotExists(
        uint64_t objectID,
        Extensions extensions,
        bool finSubgroup = false) override;

    folly::Expected<folly::Unit, MoQPublishError> beginObject(
        uint64_t objectID,
        uint64_t length,
        Payload initialPayload,
        Extensions extensions) override;

    folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
        uint64_t endOfGroupObjectID,
        Extensions extensions) override;

    folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
        uint64_t endOfTrackObjectID,
        Extensions extensions) override;

    folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override;

    void reset(ResetStreamErrorCode error) override;

    folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
        Payload payload,
        bool finSubgroup = false) override;
  };

  void addForwardingSubscriber();

  void removeForwardingSubscriber();

  uint64_t numForwardingSubscribers() const {
    return forwardingSubscribers_;
  }

 private:
  static Payload maybeClone(const Payload& payload);

  // Helper that removes a subscriber given an iterator (avoids lookup)
  void removeSubscriberIt(
      folly::F14FastMap<MoQSession*, std::shared_ptr<Subscriber>>::iterator
          subIt,
      folly::Optional<SubscribeDone> subDone,
      const std::string& callsite);

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
  uint64_t forwardingSubscribers_{0};
  bool draining_{false};
};

} // namespace moxygen
