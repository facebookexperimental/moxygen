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

  PublishResult publish(
      PublishRequest pubReq,
      std::shared_ptr<SubscriptionHandle> handle = nullptr) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override;

  folly::coro::Task<SubscribeAnnouncesResult> subscribeAnnounces(
      SubscribeAnnounces subAnn) override;

  folly::coro::Task<Subscriber::AnnounceResult> announce(
      Announce ann,
      std::shared_ptr<Subscriber::AnnounceCallback>) override;

  void removeSession(const std::shared_ptr<MoQSession>& session);

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Processing goaway uri=" << goaway.newSessionUri;
    removeSession(MoQSession::getRequestSession());
  }

  class RelaySubscriptionHandle : public Publisher::SubscriptionHandle {
   public:
    explicit RelaySubscriptionHandle() : Publisher::SubscriptionHandle() {}

    // To Be Implemented
    void unsubscribe() override {}

    // To Be Implemented
    void subscribeUpdate(SubscribeUpdate update) override {}
  };

 private:
  class AnnouncesSubscription;
  void unsubscribeAnnounces(
      const TrackNamespace& prefix,
      std::shared_ptr<MoQSession> session);

  struct AnnounceNode : public Subscriber::AnnounceHandle {
    explicit AnnounceNode(MoQRelay& relay) : relay_(relay) {}

    void unannounce() override {
      relay_.unannounce(announceOk().trackNamespace, this);
    }

    using Subscriber::AnnounceHandle::setAnnounceOk;

    folly::F14FastMap<std::string, std::shared_ptr<AnnounceNode>> children;
    // Sessions with a SUBSCRIBE_ANNOUNCES here
    folly::F14FastSet<std::shared_ptr<MoQSession>> sessions;
    // All active ANNOUNCEs for this node (includes prefix sessions)
    folly::
        F14FastMap<std::shared_ptr<MoQSession>, std::shared_ptr<AnnounceHandle>>
            announcements;
    // The session that ANNOUNCEd this node
    std::shared_ptr<MoQSession> sourceSession;

    // Forward For PUBLISH on this node, map is (trackName -> MoQForwarder)
    folly::F14FastMap<std::string, std::shared_ptr<MoQForwarder>>
        publishForwarders_;

    MoQRelay& relay_;
  };

  AnnounceNode announceRoot_{*this};
  enum class MatchType { Exact, Prefix };
  std::shared_ptr<AnnounceNode> findNamespaceNode(
      const TrackNamespace& ns,
      bool createMissingNodes = false,
      MatchType matchType = MatchType::Exact,
      std::vector<std::shared_ptr<MoQSession>>* sessions = nullptr);
  std::shared_ptr<MoQSession> findAnnounceSession(const TrackNamespace& ns);

  // Searches prefix announce tree starting at node, and checks each node to see
  // if it has a MoQForwarder (Active PUBLISH) and returns them in a vector
  std::vector<std::shared_ptr<MoQForwarder>> getAllPublishForwardersStartingAt(
      const AnnounceNode& node);

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
  };

  void onEmpty(MoQForwarder* forwarder) override;

  folly::coro::Task<void> announceToSession(
      std::shared_ptr<MoQSession> session,
      Announce ann,
      std::shared_ptr<AnnounceNode> nodePtr);

  void unannounce(const TrackNamespace& trackNamespace, AnnounceNode* node);

  TrackNamespace allowedNamespacePrefix_;
  folly::F14FastMap<FullTrackName, RelaySubscription, FullTrackName::hash>
      subscriptions_;

  std::shared_ptr<TrackConsumer> getSubscribeWriteback(
      const FullTrackName& ftn,
      std::shared_ptr<TrackConsumer> consumer) {
    return cache_ ? cache_->getSubscribeWriteback(ftn, std::move(consumer))
                  : consumer;
  }
  std::unique_ptr<MoQCache> cache_;
};

} // namespace moxygen
