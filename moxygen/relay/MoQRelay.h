/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/SharedPromise.h>
#include "moxygen/MoQSession.h"
#include "moxygen/relay/MoQCache.h"
#include "moxygen/relay/MoQForwarder.h"

#include <folly/container/F14Set.h>

namespace moxygen {

class MoQRelay : public Publisher,
                 public Subscriber,
                 public std::enable_shared_from_this<MoQRelay>,
                 public MoQForwarder::Callback {
 public:
  explicit MoQRelay(bool enableCache) {
    if (enableCache) {
      cache_ = std::make_unique<MoQCache>();
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

  folly::coro::Task<SubscribeAnnouncesResult> subscribeAnnounces(
      SubscribeAnnounces subAnn) override;

  folly::coro::Task<Subscriber::AnnounceResult> announce(
      Announce ann,
      std::shared_ptr<Subscriber::AnnounceCallback>) override;

  PublishResult publish(
      PublishRequest pubReq,
      std::shared_ptr<Publisher::SubscriptionHandle> handle = nullptr) override;

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Processing goaway uri=" << goaway.newSessionUri;
  }

  std::shared_ptr<MoQSession> findAnnounceSession(const TrackNamespace& ns);

  // Wrapper for compatibility - returns single session as vector
  std::vector<std::shared_ptr<MoQSession>> findAnnounceSessions(
      const TrackNamespace& ns) {
    auto session = findAnnounceSession(ns);
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
  class AnnouncesSubscription;
  class TerminationFilter;

  void unsubscribeAnnounces(
      const TrackNamespace& prefix,
      std::shared_ptr<MoQSession> session);

  void onPublishDone(const FullTrackName& ftn);

  struct AnnounceNode : public Subscriber::AnnounceHandle {
    explicit AnnounceNode(MoQRelay& relay, AnnounceNode* parent = nullptr)
        : relay_(relay), parent_(parent) {}

    void unannounce() override {
      relay_.unannounce(trackNamespace_, this);
    }

    // Helper to check if THIS node (excluding children) has content
    bool hasLocalSessions() const {
      return !publishes.empty() || !sessions.empty() ||
          !announcements.empty() || sourceSession != nullptr;
    }

    // Check if node should be kept (has content OR non-empty children)
    bool shouldKeep() const {
      return hasLocalSessions() || activeChildCount_ > 0;
    }

    using Subscriber::AnnounceHandle::setAnnounceOk;

    TrackNamespace trackNamespace_;
    folly::F14FastMap<std::string, std::shared_ptr<AnnounceNode>> children;

    // Maps a track name to a the session performing the PUBLISH
    folly::F14FastMap<std::string, std::shared_ptr<MoQSession>> publishes;
    // Sessions with a SUBSCRIBE_ANNOUNCES here, with their forward preference
    // Key: session, Value: forward (true = forward data, false = don't forward)
    folly::F14FastMap<std::shared_ptr<MoQSession>, bool> sessions;
    // All active ANNOUNCEs for this node (includes prefix sessions)
    folly::
        F14FastMap<std::shared_ptr<MoQSession>, std::shared_ptr<AnnounceHandle>>
            announcements;
    // The session that ANNOUNCEd this node
    std::shared_ptr<MoQSession> sourceSession;
    std::shared_ptr<AnnounceCallback> announceCallback;

    MoQRelay& relay_;

    // Pruning support: parent pointer and active child count
    AnnounceNode* parent_{nullptr}; // back link (raw pointer, parent owns us)
    size_t activeChildCount_{0};    // count of children with content

    friend class MoQRelay;

    void incrementActiveChildren();
    void decrementActiveChildren();
    void tryPruneChild(const std::string& childKey);
  };

  AnnounceNode announceRoot_{*this};
  enum class MatchType { Exact, Prefix };
  std::shared_ptr<AnnounceNode> findNamespaceNode(
      const TrackNamespace& ns,
      bool createMissingNodes = false,
      MatchType matchType = MatchType::Exact,
      std::vector<std::pair<std::shared_ptr<MoQSession>, bool>>* sessions =
          nullptr);

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

  void onEmpty(MoQForwarder* forwarder) override;
  void forwardChanged(MoQForwarder* forwarder) override;

  folly::coro::Task<void> announceToSession(
      std::shared_ptr<MoQSession> session,
      Announce ann,
      std::shared_ptr<AnnounceNode> nodePtr);

  folly::coro::Task<void> publishToSession(
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<MoQForwarder> forwarder,
      PublishRequest pub,
      bool forward);

  folly::coro::Task<void> doSubscribeUpdate(
      std::shared_ptr<Publisher::SubscriptionHandle> handle,
      bool forward);

  void unannounce(const TrackNamespace& trackNamespace, AnnounceNode* node);

  TrackNamespace allowedNamespacePrefix_;
  folly::F14FastMap<FullTrackName, RelaySubscription, FullTrackName::hash>
      subscriptions_;

  std::shared_ptr<TrackConsumer> getSubscribeWriteback(
      const FullTrackName& ftn,
      std::shared_ptr<TrackConsumer> consumer);
  std::unique_ptr<MoQCache> cache_;
};

} // namespace moxygen
