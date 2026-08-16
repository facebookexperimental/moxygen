/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQLocation.h"
#include "moxygen/MoQSession.h"

#include <folly/Executor.h>
#include <folly/container/F14Set.h>
#include <folly/hash/Hash.h>

namespace moxygen {

class MoQForwarder : public TrackConsumer {
 public:
  explicit MoQForwarder(
      FullTrackName ftn,
      std::optional<AbsoluteLocation> largest = std::nullopt);

  ~MoQForwarder() override;
  MoQForwarder(const MoQForwarder&) = delete;
  MoQForwarder& operator=(const MoQForwarder&) = delete;
  MoQForwarder(MoQForwarder&&) = delete;
  MoQForwarder& operator=(MoQForwarder&&) = delete;

  const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  GroupOrder groupOrder() const {
    return groupOrder_;
  }

  // Deprecated: use setExtensions instead, which extracts group order
  // internally
  void setGroupOrder(GroupOrder order) {
    groupOrder_ = order;
  }

  // Deprecated: delivery timeout is now carried in extensions
  void setDeliveryTimeout(uint64_t timeout);

  void setExtensions(Extensions extensions);

  const Extensions& extensions() const {
    return extensions_;
  }

  // Extract the NEW_GROUP_REQUEST param from `params`, check if it should
  // be forwarded upstream, record it as outstanding, and optionally fire
  // the newGroupRequested callback.  Pass fire=false when the NGR already
  // rides an outgoing SUBSCRIBE and no extra REQUEST_UPDATE is needed.
  void tryProcessNewGroupRequest(const Parameters& params, bool fire = true);

  void setLargest(AbsoluteLocation largest);

  std::optional<AbsoluteLocation> largest() {
    return largest_;
  }

  class Callback {
   public:
    virtual ~Callback() = default;
    virtual void onEmpty(MoQForwarder*) = 0;
    // Fires when the forwarder's source terminates (publishDone), before its
    // subscribers are drained. Distinct from onEmpty (last subscriber left):
    // this signals the publisher/upstream is gone, which bounds the forwarder's
    // lifetime in any owning registry.
    virtual void onPublishDone(MoQForwarder*) {}
    virtual void forwardChanged(MoQForwarder* fwd, bool /*forward*/) {
      forwardChanged(fwd);
    }
    virtual void forwardChanged(MoQForwarder*) {}
    // This fires whenever an unseen NGR is received
    virtual void newGroupRequested(MoQForwarder*, uint64_t /*group*/) {}
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

    // Deprecated: setExtensions now internally resolves group order
    void setPublisherGroupOrder(GroupOrder pubGroupOrder);

    void updateLargest(AbsoluteLocation largest);

    // Deprecated: track properties are now carried in extensions
    void setParam(const TrackRequestParameter& param);

    // Deprecated: MoQForwarder::setExtensions now updates all subscribers
    void setExtensions(Extensions extensions);

    // Constructs a PublishRequest from the forwarder's track-level state.
    // requestID and trackAlias are placeholders (overwritten by MoQSession).
    PublishRequest getPublishRequest() const;

    // Process PUBLISH_OK response, updating range, forward flag, and handling
    // NEW_GROUP_REQUEST forwarding via callback
    void onPublishOk(const PublishOk& pubOk);

    folly::coro::Task<folly::Expected<RequestOk, RequestError>> requestUpdate(
        RequestUpdate requestUpdate) override;

    void unsubscribe() override;

    bool checkShouldForward();

    // Returns true if this subscriber is draining and all subgroups are closed
    bool shouldRemove() const {
      return receivedPublishDone_ && subgroups.empty();
    }

    std::shared_ptr<MoQSession> session;
    // Key used in MoQForwarder::subscribers_: session.get() for session
    // subscribers, executor pointer for channel subscribers.
    const void* mapKey{nullptr};
    RequestID requestID;
    SubscribeRange range;
    std::shared_ptr<TrackConsumer> trackConsumer;
    // Stores the SubgroupConsumer for this subscriber for all currently
    // publishing subgroups.  Having this state here makes it easy to remove
    // a Subscriber and all open subgroups.
    SubgroupConsumerMap subgroups;
    MoQForwarder* forwarder;
    bool shouldForward;
    bool passive{false};
    bool pinned{false};
    bool receivedPublishDone_{false};
    bool isPinned() const {
      return pinned;
    }

    void detach() {
      forwarder = nullptr;
    }

   private:
    // Updates shouldForward and keeps forwardingSubscribers_ in sync,
    // firing forwardChanged when the count crosses zero.  Shared by
    // onPublishOk and requestUpdate.
    void updateForwardState(bool newForward);
  };

  [[nodiscard]] bool empty() const {
    return subscribers_.empty();
  }

  std::shared_ptr<Subscriber> getSubscriber(MoQSession* session) const {
    auto it = subscribers_.find(static_cast<const void*>(session));
    return it != subscribers_.end() ? it->second : nullptr;
  }

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      const SubscribeRequest& subReq,
      std::shared_ptr<TrackConsumer> consumer);

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      bool forward);

  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      const PublishRequest& pub) {
    return addSubscriber(std::move(session), pub.forward);
  }

  // Add a subscriber with an explicit consumer and optional passive flag.
  // Passive subscribers receive objects but do not count toward
  // forwardingSubscribers_, so they do not affect the forwardChanged callback
  // or onEmpty firing.  Use passive=true for internal consumers (e.g. cache)
  // that should not influence the relay's upstream subscription lifecycle.
  std::shared_ptr<MoQForwarder::Subscriber> addSubscriber(
      std::shared_ptr<MoQSession> session,
      bool forward,
      std::shared_ptr<TrackConsumer> consumer,
      bool passive = false);

  // Add a channel subscriber: a cross-exec filter routing to a per-thread
  // local forwarder.  `exec` is the subscriber iothread's executor — used as
  // the unique map key so only one cross-exec filter per executor is added.
  // Returns the Subscriber handle; call removeChannelSubscriber(handle) when
  // the local forwarder drains.
  //
  // passive=true marks the channel subscriber as not counting toward
  // forwardingSubscribers_ or blocking onEmpty (see addSubscriber). Use it for
  // the relay's own internal chain (top-N/termination/cache) attached below a
  // local-forwarder primary, so the primary's onEmpty still fires once the last
  // real cross-exec subscriber leaves.
  std::shared_ptr<MoQForwarder::Subscriber> addChannelSubscriber(
      folly::Executor* exec,
      bool forward,
      std::shared_ptr<TrackConsumer> consumer,
      bool passive = false);

  // Remove a channel subscriber added via addChannelSubscriber().
  void removeChannelSubscriber(
      const std::shared_ptr<MoQForwarder::Subscriber>& handle,
      std::optional<PublishDone> pubDone = std::nullopt);

  // Remove a channel subscriber by its executor key (avoids needing the handle).
  void removeChannelSubscriberByExec(
      folly::Executor* exec,
      std::optional<PublishDone> pubDone = std::nullopt);

  folly::Expected<SubscribeRange, FetchError> resolveJoiningFetch(
      const std::shared_ptr<MoQSession>& session,
      const JoiningFetch& joining) const;

  // Gracefully drains a subscriber - forwards publishDone but doesn't reset
  // open subgroups. Calls removeSubscriber() if no subgroups are open.
  void drainSubscriber(
      const std::shared_ptr<MoQSession>& session,
      PublishDone pubDone,
      const std::string& callsite);

  // Same as drainSubscriber but looks up by mapKey rather than session pointer.
  // Use this for channel subscribers (keyed by executor, session is null).
  void drainSubscriberByKey(
      const void* mapKey,
      PublishDone pubDone,
      const std::string& callsite);

  // Immediately removes a session - resets all open subgroups and removes
  // from subscribers map
  void removeSubscriber(
      const std::shared_ptr<MoQSession>& session,
      std::optional<PublishDone> pubDone,
      const std::string& callsite);

  template <typename Fn>
  folly::Expected<folly::Unit, MoQPublishError> forEachSubscriber(Fn&& fn);

  void updateLargest(uint64_t group, uint64_t object = 0);

  bool checkRange(const Subscriber& sub);

  // Returns true if largest_ has advanced past sub.range.end. As a side
  // effect this also publishDone's the subscriber; that retirement
  // probably belongs elsewhere (TODO).
  bool checkPastEnd(const Subscriber& sub);

  void removeSubscriberOnError(
      const Subscriber& sub,
      const MoQPublishError& err,
      const std::string& callsite /*for logging*/);

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override;

  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      BeginSubgroupOptions options = {}) override;

  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override;

  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override;

  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override;

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override;

  class SubgroupForwarder : public SubgroupConsumer {
    std::optional<uint64_t> currentObjectLength_;
    MoQForwarder* forwarder_;
    SubgroupIdentifier identifier_;
    Priority priority_;
    TrackConsumer::BeginSubgroupOptions options_;
    // Set only when upstream marked this stream as starting with the original
    // first object for the logical subgroup.
    std::optional<uint64_t> firstObjectId_;

    template <typename Fn>
    folly::Expected<folly::Unit, MoQPublishError> forEachSubscriberSubgroup(
        Fn&& fn,
        bool makeNew = true,
        const std::string& callsite = "",
        bool beginsWithFirstObjectForNewSubgroups = false);

    // Helper to erase subgroup from subscriber and remove subscriber if
    // draining
    void closeSubgroupForSubscriber(
        const std::shared_ptr<Subscriber>& sub,
        const std::string& callsite);

    // Removes this subgroup from the forwarder and checks if forwarder is empty
    folly::Expected<folly::Unit, MoQPublishError> removeSubgroupAndCheckEmpty();

    // Removes subgroup if result contains error, otherwise returns result
    // unchanged
    template <typename T>
    folly::Expected<T, MoQPublishError> cleanupOnError(
        const folly::Expected<T, MoQPublishError>& result);

    // Updates largest on the forwarder (no-op if detached)
    void updateLargest(uint64_t group, uint64_t object);

    bool startsWithFirstObject(uint64_t objectID);

   public:
    SubgroupForwarder(
        MoQForwarder& forwarder,
        uint64_t group,
        uint64_t subgroup,
        Priority priority,
        TrackConsumer::BeginSubgroupOptions options = {});

    // Detach from the owning MoQForwarder (called from MoQForwarder destructor)
    void detach();

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

  void addForwardingSubscriber();

  void removeForwardingSubscriber();

  uint64_t numForwardingSubscribers() const {
    return forwardingSubscribers_;
  }

  size_t subscriberCount() const {
    return subscribers_.size();
  }

  uint64_t totalGroupsReceived() const {
    return totalGroupsReceived_;
  }

  uint64_t totalObjectsReceived() const {
    return totalObjectsReceived_;
  }

 private:
  static Payload maybeClone(const Payload& payload);

  // Helper that checks if both subscribers_ and subgroups_ are empty and
  // fires onEmpty callback if so
  void checkAndFireOnEmpty();

  // onEmpty may destroy this forwarder, so a live OnEmptyGuard defers it until
  // iteration unwinds; the outermost guard re-checks emptiness and fires once.
  uint32_t deferOnEmptyDepth_{0};
  bool onEmptyPending_{false};
  struct OnEmptyGuard {
    explicit OnEmptyGuard(MoQForwarder* forwarder) : forwarder_(forwarder) {
      if (forwarder_) {
        forwarder_->deferOnEmptyDepth_++;
      }
    }
    ~OnEmptyGuard() {
      if (forwarder_ && --forwarder_->deferOnEmptyDepth_ == 0 &&
          forwarder_->onEmptyPending_) {
        forwarder_->onEmptyPending_ = false;
        forwarder_->checkAndFireOnEmpty();
      }
    }
    OnEmptyGuard(const OnEmptyGuard&) = delete;
    OnEmptyGuard& operator=(const OnEmptyGuard&) = delete;
    MoQForwarder* forwarder_;
  };

  // Helper that removes a subscriber given an iterator (avoids lookup)
  void removeSubscriberIt(
      folly::F14FastMap<const void*, std::shared_ptr<Subscriber>>::iterator
          subIt,
      std::optional<PublishDone> pubDone,
      const std::string& callsite);

  // Helper that looks up by mapKey and removes (used internally where a
  // Subscriber reference is available but no session pointer)
  void removeSubscriberByKey(
      const void* key,
      std::optional<PublishDone> pubDone,
      const std::string& callsite);

  // Handles errors on a subgroup for a specific subscriber.
  // Soft errors (CANCELLED - from STOP_SENDING or delivery timeout) tombstone
  // the subgroup by setting it to nullptr, preventing reopening but keeping
  // the subscription alive. Hard errors remove the entire subscription.
  void handleSubgroupError(
      Subscriber& sub,
      const SubgroupIdentifier& subgroupId,
      const MoQPublishError& err,
      const std::string& callsite);

  FullTrackName fullTrackName_;
  std::optional<TrackAlias> trackAlias_;
  // Keyed by const void*: session.get() for session subscribers,
  // executor pointer for channel subscribers (cross-exec filters).
  folly::F14FastMap<const void*, std::shared_ptr<Subscriber>> subscribers_;
  folly::F14FastMap<
      SubgroupIdentifier,
      std::shared_ptr<SubgroupForwarder>,
      SubgroupIdentifier::hash>
      subgroups_;
  GroupOrder groupOrder_{GroupOrder::OldestFirst};
  std::optional<AbsoluteLocation> largest_;
  Extensions extensions_;
  // The NEW_GROUP_REQUEST value most recently forwarded upstream; cleared when
  // the upstream Largest Group advances (indicating the request was fulfilled).
  std::optional<uint64_t> outstandingNewGroupRequest_{};
  std::shared_ptr<Callback> callback_;
  // Increments totalObjectsReceived_ and, when the group changes,
  // totalGroupsReceived_.  Call once per incoming object regardless of delivery
  // mode (subgroup stream, objectStream, datagram).
  void countReceivedObject(uint64_t groupID);

  uint64_t forwardingSubscribers_{0};
  uint32_t passiveCount_{0};
  uint64_t totalGroupsReceived_{0};
  uint64_t totalObjectsReceived_{0};
  // NOTE: counts distinct group transitions, not distinct group IDs.
  // If subgroups for a group arrive interleaved with another group (e.g. under
  // NewestFirst delivery or due to retransmission), a group may be counted more
  // than once.  This is a best-effort counter for diagnostics only.
  uint64_t lastGroupSeen_{std::numeric_limits<uint64_t>::max()};
  bool draining_{false};
};

} // namespace moxygen
