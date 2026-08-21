/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Function.h>
#include <folly/coro/SharedPromise.h>
#include "moxygen/MoQSession.h"
#include "moxygen/relay/MoQCache.h"
#include "moxygen/relay/MoQForwarder.h"
#include "moxygen/util/TimedBaton.h"

#include <folly/container/F14Map.h>

namespace moxygen {

class MoQRelay : public Publisher,
                 public Subscriber,
                 public std::enable_shared_from_this<MoQRelay>,
                 public MoQForwarder::Callback {
 public:
  explicit MoQRelay(
      size_t maxCachedTracks = kDefaultMaxCachedTracks,
      size_t maxCachedGroupsPerTrack = kDefaultMaxCachedGroupsPerTrack) {
    if (maxCachedTracks > 0) {
      cache_ =
          std::make_unique<MoQCache>(maxCachedTracks, maxCachedGroupsPerTrack);
    }
  }

  void setAllowedNamespacePrefix(TrackNamespace allowed) {
    allowedNamespacePrefix_ = std::move(allowed);
  }

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subReq,
      std::shared_ptr<TrackConsumer> consumer) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override;

  folly::coro::Task<SubscribeNamespaceResult> subscribeNamespace(
      SubscribeNamespace subAnn,
      std::shared_ptr<NamespacePublishHandle> namespacePublishHandle) override;

  folly::coro::Task<SubscribeTracksResult> subscribeTracks(
      SubscribeTracks subTracks,
      std::shared_ptr<PublishBlockedHandle> publishBlockedHandle =
          nullptr) override;

  folly::coro::Task<Subscriber::PublishNamespaceResult> publishNamespace(
      PublishNamespace ann,
      std::shared_ptr<Subscriber::PublishNamespaceCallback>) override;

  PublishResult publish(
      PublishRequest pubReq,
      std::shared_ptr<Publisher::SubscriptionHandle> handle = nullptr) override;

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Processing goaway uri=" << goaway.newSessionUri;
  }

  folly::coro::Task<Publisher::TrackStatusResult> trackStatus(
      TrackStatus req) override;

  std::shared_ptr<MoQSession> findPublishNamespaceSession(
      const TrackNamespace& ns);

  // Wrapper for compatibility - returns single session as vector
  std::vector<std::shared_ptr<MoQSession>> findPublishNamespaceSessions(
      const TrackNamespace& ns) {
    auto session = findPublishNamespaceSession(ns);
    if (session) {
      return {session};
    }
    return {};
  }

  // Test accessor: check if a publish exists and return node/publish state
  struct PublishState {
    bool nodeExists{false};                       // true if tree node exists
    std::shared_ptr<MoQSession> session{nullptr}; // publish session if exists
  };
  PublishState findPublishState(const FullTrackName& ftn);

 private:
  class NamespaceSubscription;
  class TracksSubscription;
  class TerminationFilter;

  void unsubscribeNamespace(
      const TrackNamespace& prefix,
      std::shared_ptr<MoQSession> session);

  // Draft 18+
  void unsubscribeTracks(
      const TrackNamespace& prefix,
      std::shared_ptr<MoQSession> session);

  void onPublishDoneImpl(const FullTrackName& ftn);

  static bool emptyNamespaceAllowed();

  struct NamespaceNode : public Subscriber::PublishNamespaceHandle {
    explicit NamespaceNode(MoQRelay& relay, NamespaceNode* parent = nullptr)
        : relay_(relay), parent_(parent) {}

    void publishNamespaceDone() override {
      relay_.publishNamespaceDone(trackNamespace_, this);
    }

    folly::coro::Task<RequestUpdateResult> requestUpdate(
        RequestUpdate reqUpdate) override {
      co_return folly::makeUnexpected(
          RequestError{
              reqUpdate.requestID,
              RequestErrorCode::NOT_SUPPORTED,
              "REQUEST_UPDATE not supported for relay PUBLISH_NAMESPACE"});
    }

    // Helper to check if THIS node (excluding children) has content
    bool hasLocalSessions() const {
      return !publishes.empty() || !sessions.empty() ||
          !namespacesPublished.empty() || sourceSession != nullptr;
    }

    // Check if node should be kept (has content OR non-empty children)
    bool shouldKeep() const {
      return hasLocalSessions() || activeChildCount_ > 0;
    }

    using Subscriber::PublishNamespaceHandle::setPublishNamespaceOk;

    TrackNamespace trackNamespace_;
    folly::F14FastMap<std::string, std::shared_ptr<NamespaceNode>> children;

    // Maps a track name to a the session performing the PUBLISH
    folly::F14FastMap<std::string, std::shared_ptr<MoQSession>> publishes;

    struct NamespaceSubscriberInfo {
      bool forward{true};
      SubscribeNamespaceOptions options{SubscribeNamespaceOptions::BOTH};
      // Handle for sending NAMESPACE / NAMESPACE_DONE on the bidi stream
      // (draft 16+). Null for draft <= 15.
      std::shared_ptr<Publisher::NamespacePublishHandle> namespacePublishHandle;
      // Draft 18+ only: handle for SUBSCRIBE_TRACKS response-stream followups.
      std::shared_ptr<Publisher::PublishBlockedHandle> publishBlockedHandle;
      // The namespace prefix this subscriber used for SUBSCRIBE_NAMESPACE
      TrackNamespace trackNamespacePrefix;
    };

    // Sessions with a SUBSCRIBE_NAMESPACE here, with their preferences
    folly::F14FastMap<std::shared_ptr<MoQSession>, NamespaceSubscriberInfo>
        sessions;
    // All active PUBLISH_NAMESPACEs for this node (includes prefix sessions)
    folly::F14FastMap<
        std::shared_ptr<MoQSession>,
        std::shared_ptr<PublishNamespaceHandle>>
        namespacesPublished;
    // The session that PUBLISH_NAMESPACEd this node
    std::shared_ptr<MoQSession> sourceSession;
    std::shared_ptr<PublishNamespaceCallback> publishNamespaceCallback;

    MoQRelay& relay_;

    // Pruning support: parent pointer and active child count
    NamespaceNode* parent_{nullptr}; // back link (raw pointer, parent owns us)
    size_t activeChildCount_{0};     // count of children with content

    friend class MoQRelay;

    void incrementActiveChildren();
    void decrementActiveChildren();
    void tryPruneChild(const std::string& childKey);
  };

  // We maintain two separate prefix trees, one for SUBSCRIBE_NAMESPACE,
  // and one for SUBSCRIBE_TRACKS. In the SUBSCRIBE_TRACKS tree, only
  // `children` and `sessions` are populated.
  NamespaceNode publishNamespaceRoot_{*this};
  NamespaceNode tracksSubscriberRoot_{*this};
  std::shared_ptr<NamespaceNode> findNamespaceNode(
      const TrackNamespace& ns,
      bool createMissingNodes = false,
      std::vector<std::pair<
          std::shared_ptr<MoQSession>,
          NamespaceNode::NamespaceSubscriberInfo>>* sessions = nullptr) {
    return findInTree(publishNamespaceRoot_, ns, createMissingNodes, sessions);
  }

  // Draft 18+: same lookup, but in the tracks-subscriber tree.
  std::shared_ptr<NamespaceNode> findTracksSubscriberNode(
      const TrackNamespace& ns,
      bool createMissingNodes = false,
      std::vector<std::pair<
          std::shared_ptr<MoQSession>,
          NamespaceNode::NamespaceSubscriberInfo>>* sessions = nullptr) {
    return findInTree(tracksSubscriberRoot_, ns, createMissingNodes, sessions);
  }

  std::shared_ptr<NamespaceNode> findInTree(
      NamespaceNode& root,
      const TrackNamespace& ns,
      bool createMissingNodes,
      std::vector<std::pair<
          std::shared_ptr<MoQSession>,
          NamespaceNode::NamespaceSubscriberInfo>>* sessions);

  // Generic per-session prefix-overlap check within a subscriber tree
  // (SUBSCRIBE_NAMESPACE or SUBSCRIBE_TRACKS).
  bool hasOverlappingSubscription(
      const NamespaceNode& root,
      const TrackNamespace& prefix,
      std::shared_ptr<MoQSession> session) const;
  bool hasSessionInSubtree(
      const NamespaceNode& node,
      std::shared_ptr<MoQSession> session) const;

  // Draft §10.9.2: move an established SUBSCRIBE_NAMESPACE / SUBSCRIBE_TRACKS
  // registration to a new Track Namespace Prefix within its subscriber tree,
  // enforcing the per-session overlap restriction. Returns PREFIX_OVERLAP and
  // leaves the original registration intact if the new prefix overlaps another
  // active subscription of the same type in the session.
  folly::Expected<folly::Unit, RequestErrorCode> updateSubscriptionPrefix(
      NamespaceNode& root,
      const TrackNamespace& oldPrefix,
      const TrackNamespace& newPrefix,
      const std::shared_ptr<MoQSession>& session);
  // `forward`, when set, updates the Forwarding State applied to future
  // matching subscriptions (it is applied before backfilling newly-matched
  // tracks so they honor the new state); existing subscriptions are unaffected.
  folly::Expected<folly::Unit, RequestErrorCode> updateTracksSubscriptionPrefix(
      const TrackNamespace& oldPrefix,
      const TrackNamespace& newPrefix,
      const std::shared_ptr<MoQSession>& session,
      std::optional<bool> forward = std::nullopt);
  folly::Expected<folly::Unit, RequestErrorCode>
  updateNamespaceSubscriptionPrefix(
      const TrackNamespace& oldPrefix,
      const TrackNamespace& newPrefix,
      const std::shared_ptr<MoQSession>& session);
  // Selects which subscriber tree a prefix update applies to.
  enum class PrefixUpdateKind { Tracks, Namespace };
  // Shared REQUEST_UPDATE handling for the SUBSCRIBE_TRACKS and
  // SUBSCRIBE_NAMESPACE handles: extracts and decodes the
  // TRACK_NAMESPACE_PREFIX parameter, closes the session on a malformed tuple
  // (§2.4.1), and moves the registration (which backfills newly-matched
  // content). For SUBSCRIBE_TRACKS it also applies the FORWARD parameter, which
  // updates the Forwarding State on future matching subscriptions (existing
  // subscriptions are unaffected) and may appear with or without a prefix
  // change. Returns the (possibly unchanged) prefix on success or a per-request
  // error to relay back to the peer.
  folly::Expected<TrackNamespace, RequestError> updatePrefixFromRequest(
      const RequestUpdate& reqUpdate,
      const TrackNamespace& currentPrefix,
      const std::shared_ptr<MoQSession>& session,
      PrefixUpdateKind kind);

  struct RelaySubscription {
    RelaySubscription(
        std::shared_ptr<MoQForwarder> f,
        std::shared_ptr<MoQSession> u)
        : forwarder(std::move(f)), upstream(std::move(u)) {}

    std::shared_ptr<MoQForwarder> forwarder;
    std::shared_ptr<MoQSession> upstream;
    RequestID requestID{0};
    std::shared_ptr<Publisher::SubscriptionHandle> handle;
    folly::coro::SharedPromise<folly::Unit> promise;
    bool isPublish{false};
  };

  folly::coro::Task<Publisher::SubscribeResult>
  subscribeToExistingRelaySubscription(
      SubscribeRequest subReq,
      std::shared_ptr<TrackConsumer> consumer,
      std::shared_ptr<MoQSession> downstreamSession);

  folly::coro::Task<Publisher::SubscribeResult>
  subscribeToFirstRelaySubscription(
      SubscribeRequest subReq,
      std::shared_ptr<TrackConsumer> consumer,
      std::shared_ptr<MoQSession> downstreamSession,
      std::shared_ptr<MoQSession> upstreamSession);

  void onEmpty(MoQForwarder* forwarder) override;
  void forwardChanged(MoQForwarder* forwarder) override;
  void newGroupRequested(MoQForwarder* forwarder, uint64_t group) override;

  folly::coro::Task<void> publishNamespaceToSession(
      std::shared_ptr<MoQSession> session,
      PublishNamespace ann,
      std::shared_ptr<NamespaceNode> nodePtr);

  folly::coro::Task<void> publishToSession(
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<MoQForwarder> forwarder,
      bool forward,
      TrackNamespace trackNamespacePrefix = {},
      std::shared_ptr<Publisher::PublishBlockedHandle> publishBlockedHandle =
          nullptr);

  // Emit PUBLISH to `session` for every already-published track matching
  // `prefix`. Tracks under `skipUnderPrefix` are skipped -- they are already
  // being forwarded to this session via a prior prefix, so re-publishing would
  // emit a duplicate PUBLISH. Used by the initial SUBSCRIBE_TRACKS (no skip)
  // and by REQUEST_UPDATE prefix moves (skip the outgoing prefix's subtree).
  void publishExistingMatchingTracks(
      const std::shared_ptr<MoQSession>& session,
      const TrackNamespace& prefix,
      bool forward,
      const std::shared_ptr<Publisher::PublishBlockedHandle>&
          publishBlockedHandle,
      const TrackNamespace* skipUnderPrefix = nullptr);

  // Namespace-tree analog of publishExistingMatchingTracks: replays existing
  // PUBLISH_NAMESPACEs and matching PUBLISHes under `prefix` to a
  // SUBSCRIBE_NAMESPACE subscriber, honoring `options` and draft version.
  // Tracks/namespaces under `skipUnderPrefix` are skipped -- already delivered
  // via a prior prefix. Used by the initial SUBSCRIBE_NAMESPACE (no skip) and
  // by REQUEST_UPDATE prefix moves (skip the outgoing prefix's subtree).
  void publishExistingMatchingNamespaces(
      const std::shared_ptr<MoQSession>& session,
      const TrackNamespace& prefix,
      bool forward,
      SubscribeNamespaceOptions options,
      const std::shared_ptr<Publisher::NamespacePublishHandle>&
          namespacePublishHandle,
      RequestID requestID,
      uint64_t negotiatedVersion,
      const TrackNamespace* skipUnderPrefix = nullptr);

  void publishBlockedToTracksSubscriber(
      const TrackNamespace& trackNamespacePrefix,
      const FullTrackName& ftn,
      const std::shared_ptr<Publisher::PublishBlockedHandle>&
          publishBlockedHandle) const;

  folly::coro::Task<void> doSubscribeUpdate(
      std::shared_ptr<Publisher::SubscriptionHandle> handle,
      bool forward);

  folly::coro::Task<void> doNewGroupRequestUpdate(
      std::shared_ptr<Publisher::SubscriptionHandle> handle,
      uint64_t newGroupRequestValue);

  void publishNamespaceDone(
      const TrackNamespace& trackNamespace,
      NamespaceNode* node);

  struct PendingRendezvous {
    SubscribeRequest subReq;
    std::shared_ptr<TrackConsumer> consumer;
    std::shared_ptr<MoQSession> downstreamSession;

    // The baton is signaled when one of the following happens:
    // (1) A peer PUBLISHes the exact track
    // (2) A peer PUBLISH_NAMESPACEs a namespace that contains the track
    TimedBaton baton;
  };

  struct PendingRendezvousNode {
    using WaiterList = std::vector<std::shared_ptr<PendingRendezvous>>;

    bool empty() const {
      return children.empty() && waitersByTrack.empty();
    }

    folly::F14FastMap<std::string, std::unique_ptr<PendingRendezvousNode>>
        children;
    folly::F14FastMap<std::string, WaiterList> waitersByTrack;
  };

  void addPendingRendezvous(
      const FullTrackName& ftn,
      const std::shared_ptr<PendingRendezvous>& waiter);
  PendingRendezvousNode& findOrCreatePendingRendezvousNode(
      const TrackNamespace& ns);

  void erasePendingRendezvous(
      const FullTrackName& ftn,
      const std::shared_ptr<PendingRendezvous>& waiter);

  void erasePendingRendezvousFromNode(
      PendingRendezvousNode& node,
      const FullTrackName& ftn,
      size_t namespaceIndex,
      const std::shared_ptr<PendingRendezvous>& waiter);
  static void wakePendingRendezvousSubtree(PendingRendezvousNode& node);

  void applyFnAtNamespaceNode(
      const TrackNamespace& ns,
      folly::FunctionRef<void(PendingRendezvousNode&)> onNode);

  void wakePendingRendezvousForTrack(const FullTrackName& ftn);

  void wakePendingRendezvousUnderNamespace(const TrackNamespace& ns);

  TrackNamespace allowedNamespacePrefix_;
  folly::F14FastMap<FullTrackName, RelaySubscription, FullTrackName::hash>
      subscriptions_;
  PendingRendezvousNode pendingRendezvousRoot_;

  std::shared_ptr<TrackConsumer> getSubscribeWriteback(
      const FullTrackName& ftn,
      std::shared_ptr<TrackConsumer> consumer);
  std::unique_ptr<MoQCache> cache_;
};

} // namespace moxygen
