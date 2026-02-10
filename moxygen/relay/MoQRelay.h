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

  folly::coro::Task<SubscribeNamespaceResult> subscribeNamespace(
      SubscribeNamespace subAnn,
      std::shared_ptr<NamespacePublishHandle> namespacePublishHandle) override;

  folly::coro::Task<Subscriber::PublishNamespaceResult> publishNamespace(
      PublishNamespace ann,
      std::shared_ptr<Subscriber::PublishNamespaceCallback>) override;

  PublishResult publish(
      PublishRequest pubReq,
      std::shared_ptr<Publisher::SubscriptionHandle> handle = nullptr) override;

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Processing goaway uri=" << goaway.newSessionUri;
  }

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
  class TerminationFilter;

  void unsubscribeNamespace(
      const TrackNamespace& prefix,
      std::shared_ptr<MoQSession> session);

  void onPublishDone(const FullTrackName& ftn);

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
    // Sessions with a SUBSCRIBE_NAMESPACE here, with their forward preference
    // Key: session, Value: forward (true = forward data, false = don't forward)
    folly::F14FastMap<std::shared_ptr<MoQSession>, bool> sessions;
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

  NamespaceNode publishNamespaceRoot_{*this};
  enum class MatchType { Exact, Prefix };
  std::shared_ptr<NamespaceNode> findNamespaceNode(
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

  folly::coro::Task<void> publishNamespaceToSession(
      std::shared_ptr<MoQSession> session,
      PublishNamespace ann,
      std::shared_ptr<NamespaceNode> nodePtr);

  folly::coro::Task<void> publishToSession(
      std::shared_ptr<MoQSession> session,
      std::shared_ptr<MoQForwarder> forwarder,
      PublishRequest pub,
      bool forward);

  folly::coro::Task<void> doSubscribeUpdate(
      std::shared_ptr<Publisher::SubscriptionHandle> handle,
      bool forward);

  void publishNamespaceDone(
      const TrackNamespace& trackNamespace,
      NamespaceNode* node);

  TrackNamespace allowedNamespacePrefix_;
  folly::F14FastMap<FullTrackName, RelaySubscription, FullTrackName::hash>
      subscriptions_;

  std::shared_ptr<TrackConsumer> getSubscribeWriteback(
      const FullTrackName& ftn,
      std::shared_ptr<TrackConsumer> consumer);
  std::unique_ptr<MoQCache> cache_;
};

} // namespace moxygen
