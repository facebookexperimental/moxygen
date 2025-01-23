/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQSession.h"
#include "moxygen/relay/MoQForwarder.h"

#include <folly/container/F14Set.h>

namespace moxygen {

class MoQRelay : public Publisher,
                 public std::enable_shared_from_this<MoQRelay>,
                 public MoQForwarder::Callback {
 public:
  void setAllowedNamespacePrefix(TrackNamespace allowed) {
    allowedNamespacePrefix_ = std::move(allowed);
  }

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subReq,
      std::shared_ptr<TrackConsumer> consumer) override;

  folly::coro::Task<SubscribeAnnouncesResult> subscribeAnnounces(
      SubscribeAnnounces subAnn) override;

  void onAnnounce(Announce&& ann, std::shared_ptr<MoQSession> session);
  void onUnannounce(
      Unannounce&& ann,
      const std::shared_ptr<MoQSession>& session);

  void removeSession(const std::shared_ptr<MoQSession>& session);

  void goaway(Goaway goaway) override {
    XLOG(INFO) << "Processing goaway uri=" << goaway.newSessionUri;
    removeSession(MoQSession::getRequestSession());
  }

 private:
  class AnnouncesSubscription;
  void unsubscribeAnnounces(
      const TrackNamespace& prefix,
      std::shared_ptr<MoQSession> session);

  struct AnnounceNode {
    folly::F14NodeMap<std::string, AnnounceNode> children;
    folly::F14FastSet<std::shared_ptr<MoQSession>> sessions;
    std::shared_ptr<MoQSession> sourceSession;
  };
  AnnounceNode announceRoot_;
  AnnounceNode* findNamespaceNode(
      const TrackNamespace& ns,
      bool createMissingNodes,
      std::vector<std::shared_ptr<MoQSession>>* sessions = nullptr);
  std::shared_ptr<MoQSession> findAnnounceSession(const TrackNamespace& ns);

  struct RelaySubscription {
    std::shared_ptr<MoQForwarder> forwarder;
    std::shared_ptr<MoQSession> upstream;
    SubscribeID subscribeID;
    std::shared_ptr<Publisher::SubscriptionHandle> handle;
  };

  void onEmpty(MoQForwarder* forwarder) override;

  TrackNamespace allowedNamespacePrefix_;
  folly::F14FastMap<FullTrackName, RelaySubscription, FullTrackName::hash>
      subscriptions_;
};

} // namespace moxygen
