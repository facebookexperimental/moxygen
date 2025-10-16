/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/container/F14Map.h>
#include <moxygen/MoQSession.h>

namespace moxygen {

/**
 * MoQRelaySession extends MoQSession with full announcement functionality.
 *
 * This subclass provides real implementations of announce() and
 * subscribeAnnounces() methods, along with proper announcement state
 * management. It should be used in relay servers and any applications that need
 * to handle announcements.
 *
 * The base MoQSession returns NOT_SUPPORTED for announcement operations, making
 * it suitable for simple clients that only subscribe to tracks.
 */
class MoQRelaySession : public MoQSession {
 public:
  // Inherit all base constructors
  using MoQSession::MoQSession;

  // Static factory for creating relay sessions in clients
  static std::function<std::shared_ptr<MoQSession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>,
      std::shared_ptr<MoQExecutor>)>
  createRelaySessionFactory();

  // Override cleanup method for proper inheritance pattern
  void cleanup() override;

  // Override announcement methods with real implementations
  folly::coro::Task<Subscriber::AnnounceResult> announce(
      Announce ann,
      std::shared_ptr<AnnounceCallback> announceCallback = nullptr) override;

  folly::coro::Task<Publisher::SubscribeAnnouncesResult> subscribeAnnounces(
      SubscribeAnnounces subAnn) override;

 private:
  // Forward declarations for inner classes
  class SubscriberAnnounceCallback;
  class PublisherAnnounceHandle;
  class SubscribeAnnouncesHandle;

  // Internal announcement handling methods
  folly::coro::Task<void> handleSubscribeAnnounces(SubscribeAnnounces sa);
  void subscribeAnnouncesOk(const SubscribeAnnouncesOk& saOk);
  void unsubscribeAnnounces(const UnsubscribeAnnounces& unsubAnn);

  folly::coro::Task<void> handleAnnounce(Announce announce);
  void announceOk(const AnnounceOk& annOk);
  void announceCancel(const AnnounceCancel& annCan);
  void unannounce(const Unannounce& unannounce);

  // Override all incoming announcement message handlers
  void onAnnounce(Announce ann) override;
  void onAnnounceOk(AnnounceOk annOk) override;
  void onAnnounceError(AnnounceError announceError) override;
  void onAnnounceCancel(AnnounceCancel announceCancel) override;
  void onUnannounce(Unannounce unAnn) override;
  void onSubscribeAnnounces(SubscribeAnnounces sa) override;
  void onSubscribeAnnouncesOk(SubscribeAnnouncesOk saOk) override;
  void onSubscribeAnnouncesError(
      SubscribeAnnouncesError subscribeAnnouncesError) override;
  void onUnsubscribeAnnounces(UnsubscribeAnnounces unsub) override;

  // Announcement-specific types (moved from base class)
  struct PendingAnnounce {
    TrackNamespace trackNamespace;
    folly::coro::Promise<folly::Expected<AnnounceOk, AnnounceError>> promise;
    std::shared_ptr<AnnounceCallback> callback;
  };

  // Announcement state management (restored from base class)
  folly::F14FastMap<
      TrackNamespace,
      std::shared_ptr<Subscriber::AnnounceHandle>,
      TrackNamespace::hash>
      subscriberAnnounces_;
  folly::F14FastMap<
      TrackNamespace,
      std::shared_ptr<Subscriber::AnnounceCallback>,
      TrackNamespace::hash>
      publisherAnnounces_;
  folly::F14FastMap<
      TrackNamespace,
      std::shared_ptr<Publisher::SubscribeAnnouncesHandle>,
      TrackNamespace::hash>
      subscribeAnnounces_;

  // Extended PendingRequestState for announcement support
  class MoQRelayPendingRequestState;
};

} // namespace moxygen
