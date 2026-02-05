/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQLocation.h"
#include "moxygen/MoQSession.h"

#include <folly/container/F14Set.h>
#include <folly/experimental/coro/Task.h>
#include <folly/hash/Hash.h>
#include <folly/io/async/EventBase.h>

namespace moxygen {

class MoQForwarder : public TrackConsumer,
                     public std::enable_shared_from_this<MoQForwarder> {
 public:
  explicit MoQForwarder(
      FullTrackName ftn,
      folly::Executor* executor,
      std::optional<AbsoluteLocation> largest = std::nullopt,
      size_t maxSubscribersPerForwarder = 50);

  const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  GroupOrder groupOrder() const {
    return groupOrder_;
  }

  void setGroupOrder(GroupOrder order) {
    groupOrder_ = order;
  }

  void updateLargest(uint64_t group, uint64_t object = 0);

  void setLargest(AbsoluteLocation largest);

  void setDeliveryTimeout(uint64_t timeout);

  std::chrono::milliseconds upstreamDeliveryTimeout() const {
    return upstreamDeliveryTimeout_;
  }

  std::optional<AbsoluteLocation> largest() {
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
  struct Subscriber : public Publisher::SubscriptionHandle {
    using SubgroupConsumerMap = folly::F14FastMap<
        SubgroupIdentifier,
        std::shared_ptr<SubgroupConsumer>,
        SubgroupIdentifier::hash>;

    Subscriber(
        MoQForwarder* f,
        SubscribeOk ok,
        std::shared_ptr<MoQSession> s,
        RequestID sid,
        SubscribeRange r,
        std::shared_ptr<TrackConsumer> tc,
        bool shouldForwardIn);

    // This method is for a relay to fixup the publisher group order of the
    // first subscriber if it was added before the upstream SubscribeOK.
    void setPublisherGroupOrder(GroupOrder pubGroupOrder);

    void setSubscribeOkLargest(AbsoluteLocation largest);

    // Updates the params of the subscribeOk
    // updates existing param if key matches, otherwise adds new param
    void setParam(const TrackRequestParameter& param);

    folly::coro::Task<folly::Expected<SubscribeUpdateOk, SubscribeUpdateError>>
    subscribeUpdate(SubscribeUpdate subscribeUpdate) override;

    void unsubscribe() override;

    bool checkShouldForward();

    // Subscriber can be removed when:
    // 1. It's been marked as draining (publishDone sent)
    // 2. All subgroups have been closed (no pending data)
    bool shouldRemoveSubscriber() const {
      return receivedPublishDone_ && subgroups.empty();
    }

    std::shared_ptr<MoQSession> session;
    RequestID requestID;
    SubscribeRange range;
    std::shared_ptr<TrackConsumer> trackConsumer;
    // Stores the SubgroupConsumer for this subscriber for all currently
    // publishing subgroups.  Having this state here makes it easy to remove
    // a Subscriber and all open subgroups.
    SubgroupConsumerMap subgroups;
    MoQForwarder* forwarder;
    bool shouldForward;
    bool receivedPublishDone_{false};
  };

  [[nodiscard]] bool empty() const;

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      const SubscribeRequest& subReq,
      std::shared_ptr<TrackConsumer> consumer);

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      const PublishRequest& pub);

  folly::coro::Task<std::shared_ptr<Subscriber>> addSubscriberAsync(
      std::shared_ptr<MoQSession> session,
      const SubscribeRequest& subReq,
      std::shared_ptr<TrackConsumer> consumer,
      folly::Executor* subscriberExecutor);

  folly::coro::Task<std::shared_ptr<Subscriber>> addSubscriberAsync(
      std::shared_ptr<MoQSession> session,
      const PublishRequest& pub,
      folly::Executor* subscriberExecutor);

  folly::Expected<SubscribeRange, FetchError> resolveJoiningFetch(
      const std::shared_ptr<MoQSession>& session,
      const JoiningFetch& joining) const;

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

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload) override;

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override;

  void addForwardingSubscriber();

  void removeForwardingSubscriber();

  uint64_t numForwardingSubscribers() const {
    return forwardingSubscribersAtomic_->load(std::memory_order_relaxed);
  }

  uint64_t totalSubscribers() const {
    return totalSubscribers_->load(std::memory_order_relaxed);
  }

  // Gracefully drains a subscriber - forwards publishDone but doesn't reset
  // open subgroups. Calls removeSubscriber() if no subgroups are open.
  void drainSubscriber(
      const std::shared_ptr<MoQSession>& session,
      PublishDone pubDone,
      const std::string& callsite);

  // Immediately removes a session - resets all open subgroups and removes
  // from subscribers map
  void removeSubscriber(
      const std::shared_ptr<MoQSession>& session,
      std::optional<PublishDone> pubDone,
      const std::string& callsite);

 private:
  // Private constructor for creating child forwarders
  MoQForwarder(MoQForwarder* parent, folly::Executor* executor);

  std::shared_ptr<MoQForwarder> createChildForwarder(
      folly::Executor* childExecutor);

  folly::coro::Task<std::shared_ptr<Subscriber>> addSubscriberInternal(
      std::shared_ptr<MoQSession> session,
      SubscribeOk subscribeOk,
      RequestID requestID,
      SubscribeRange range,
      std::shared_ptr<TrackConsumer> consumer,
      folly::Executor* subscriberExecutor,
      bool shouldForward);

  bool hasCapacity() const {
    return subscribers_.size() < maxSubscribersPerForwarder_;
  }

  bool allChildrenEmpty() const;

  void onChildEmptied(MoQForwarder* child);

  // Invokes callback on root's executor (or directly if already on root's
  // executor)
  void invokeCallback(std::function<void(Callback*, MoQForwarder*)> fn);

  template <typename Fn>
  folly::Expected<folly::Unit, MoQPublishError> forEachSubscriber(
      Fn&& fn,
      std::optional<AbsoluteLocation> location = std::nullopt);

  bool checkRange(const Subscriber& sub);

  void removeSubscriberOnError(
      const Subscriber& sub,
      const MoQPublishError& err,
      const std::string& callsite /*for logging*/);

  // Helper that checks if both subscribers_ and subgroups_ are empty and
  // fires onEmpty callback if so
  void checkAndFireOnEmpty();

  // Helper that removes a subscriber given an iterator (avoids lookup)
  void removeSubscriberIt(
      folly::F14FastMap<MoQSession*, std::shared_ptr<Subscriber>>::iterator
          subIt,
      std::optional<PublishDone> pubDone,
      const std::string& where);

  // Thread-local executor cache support
  struct ExecutorForwarderPair {
    folly::Executor* executor;
    MoQForwarder* forwarder; // First forwarder for this executor
  };

  // Static thread-local storage
  static folly::ThreadLocal<std::vector<ExecutorForwarderPair>>
      executorForwarderCache_;

  // Helper to register this forwarder for its executor on current thread
  void registerForCurrentThread();

  // Helper to get the first forwarder for a given executor from thread-local
  // cache
  static MoQForwarder* currentForwarderForExecutor(folly::Executor* exec);

  // Helper to find subscriber for a session, using executor-aware search
  std::shared_ptr<Subscriber> findSubscriberForSession(
      const std::shared_ptr<MoQSession>& session) const;

  class SubgroupForwarder
      : public SubgroupConsumer,
        public std::enable_shared_from_this<SubgroupForwarder> {
    std::optional<uint64_t> currentObjectLength_;
    MoQForwarder& forwarder_;
    SubgroupIdentifier identifier_;
    Priority priority_;

    template <typename Fn>
    folly::Expected<folly::Unit, MoQPublishError> forEachSubscriberSubgroup(
        Fn&& fn,
        std::optional<AbsoluteLocation> location = std::nullopt,
        bool makeNew = true,
        const std::string& callsite = "");

    // Helper to erase subgroup from subscriber and remove subscriber if
    // draining
    static void closeSubgroupForSubscriber(
        const std::shared_ptr<Subscriber>& sub,
        const SubgroupIdentifier& identifier,
        const std::string& callsite);

    // Removes this subgroup from the forwarder and checks if forwarder is empty
    folly::Expected<folly::Unit, MoQPublishError> removeSubgroupAndCheckEmpty();

    // Removes subgroup if result contains error, otherwise returns result
    // unchanged
    template <typename T>
    folly::Expected<T, MoQPublishError> cleanupOnError(
        const folly::Expected<T, MoQPublishError>& result);

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

    folly::Expected<folly::Unit, MoQPublishError> beginObject(
        uint64_t objectID,
        uint64_t length,
        Payload initialPayload,
        Extensions extensions) override;

    folly::Expected<folly::Unit, MoQPublishError> endOfGroup(
        uint64_t endOfGroupObjectID) override;

    folly::Expected<folly::Unit, MoQPublishError> endOfTrackAndGroup(
        uint64_t endOfTrackObjectID) override;

    folly::Expected<folly::Unit, MoQPublishError> endOfSubgroup() override;

    void reset(ResetStreamErrorCode error) override;

    folly::Expected<ObjectPublishStatus, MoQPublishError> objectPayload(
        Payload payload,
        bool finSubgroup = false) override;
  };

 private:
  FullTrackName fullTrackName_;
  std::optional<TrackAlias> trackAlias_;
  folly::F14FastMap<MoQSession*, std::shared_ptr<Subscriber>> subscribers_;
  folly::F14FastMap<
      SubgroupIdentifier,
      std::shared_ptr<SubgroupForwarder>,
      SubgroupIdentifier::hash>
      subgroups_; // ROOT-ONLY: Subgroups only created on root
  GroupOrder groupOrder_{GroupOrder::OldestFirst};
  std::optional<AbsoluteLocation> largest_;
  // This should eventually be a vector of params that can be cascaded e2e
  std::chrono::milliseconds upstreamDeliveryTimeout_{};
  std::shared_ptr<Callback> callback_;
  bool draining_{false};

  // Tree structure for scalable multi-executor support
  size_t maxSubscribersPerForwarder_;
  folly::Executor* executor_;
  MoQForwarder* parent_{nullptr};
  MoQForwarder* root_{nullptr};

  // Child forwarders
  // Root: map of executor -> child forwarder (multiple executors supported)
  // Non-root: at most one child (overflow on same executor)
  folly::F14FastMap<folly::Executor*, std::shared_ptr<MoQForwarder>>
      nextForwarders_;

  // Atomic counters (root owns storage, children point to root's)
  std::atomic<uint64_t>* pendingOps_;
  std::atomic<uint64_t>* totalSubscribers_;
  std::atomic<uint64_t>* forwardingSubscribersAtomic_;

  // Storage for root forwarder
  std::atomic<uint64_t> pendingOpsStorage_{0};
  std::atomic<uint64_t> totalSubscribersStorage_{0};
  std::atomic<uint64_t> forwardingSubscribersStorage_{0};
};

} // namespace moxygen
