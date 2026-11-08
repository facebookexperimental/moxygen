/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/Expected.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Task.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "moxygen/Publisher.h"
#include "moxygen/proxy/MoQGroupCache.h"
#include "moxygen/proxy/MoQUpstreamGoaways.h"
#include "moxygen/proxy/MoQUpstreamProvider.h"

namespace moxygen {

class MoQForwarder;
class MoQSession;
class TrackConsumer;

struct MoQProxyTrackCacheConfig {
  // Newest group IDs kept, including the active group; 0 disables caching.
  size_t maxGroups{0};
  std::shared_ptr<MoQCacheBudget> budget;
};

// Counters for an upstream going away, summed over a proxy's tracks. Plain
// counters: the tracks share the proxy's executor.
struct MoQProxySwitchStats {
  // Each session or request-stream GOAWAY, once per track it reaches: a session
  // GOAWAY and a request-stream GOAWAY reaching one track count two.
  uint64_t upstreamGoaways{0};
  // Upstream subscriptions that ended because their relay went away or their
  // session closed, rather than because the track ended.
  uint64_t upstreamsLost{0};
};

// Shared by the tracks of one proxy; a null member gets a per-track default.
// Tracks that can share an upstream session must share goaways: a session's
// GOAWAYs reach only the registry that watched it last.
struct MoQProxyTrackUpstreamConfig {
  std::shared_ptr<MoQUpstreamGoaways> goaways;
  std::shared_ptr<MoQProxySwitchStats> switchStats;
};

class MoQProxyTrack : public std::enable_shared_from_this<MoQProxyTrack> {
 public:
  // The peer a subscription is served to. MoQProxyTrack never holds the
  // session, so it is handed the facts it needs.
  struct DownstreamPeer {
    SessionId sessionId;
    uint64_t version{0};
  };

  class Callback {
   public:
    virtual ~Callback() = default;
    virtual void onNoSubscribers(MoQProxyTrack* track) = 0;
  };

  using CacheConfig = MoQProxyTrackCacheConfig;
  using UpstreamConfig = MoQProxyTrackUpstreamConfig;

  static std::shared_ptr<MoQProxyTrack> create(
      FullTrackName fullTrackName,
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
      CacheConfig cacheConfig = {},
      UpstreamConfig upstreamConfig = {});

  ~MoQProxyTrack();

  MoQProxyTrack(const MoQProxyTrack&) = delete;
  MoQProxyTrack& operator=(const MoQProxyTrack&) = delete;

  const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  void setCallback(std::weak_ptr<Callback> callback) {
    callback_ = std::move(callback);
  }

  folly::coro::Task<Publisher::SubscribeResult> subscribe(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer,
      DownstreamPeer downstream);

  // The range a joining FETCH covers: from its start through the saved
  // Joining Location of its subscription, as an exclusive end.
  struct JoiningRange {
    AbsoluteLocation start;
    AbsoluteLocation end;
  };
  folly::Expected<JoiningRange, FetchError> resolveJoiningFetch(
      SessionId sessionId,
      RequestID fetchRequestID,
      const JoiningFetch& joining) const;

  const std::shared_ptr<MoQGroupCache>& groupCache() const {
    return cache_;
  }
  folly::coro::Task<folly::Unit> awaitUpstreamSubscription();
  // Still set after the upstream ends publishing, for historical fallback.
  const std::shared_ptr<MoQSession>& upstreamSession() const {
    return upstream_.session;
  }
  const Extensions& trackExtensions() const;

  void close();

 private:
  enum class State { IDLE, CONNECTING, READY, DRAINING, CLOSED };

  struct UpstreamEstablishmentFailure {
    SubscribeErrorCode errorCode;
    std::string reasonPhrase;
  };

  class Recorder;
  class SubgroupRecorder;
  class DownstreamSubscriptionHandle;

  // One upstream subscription and what serves it.
  struct UpstreamAttempt {
    std::shared_ptr<MoQSession> session;
    std::shared_ptr<Publisher::SubscriptionHandle> handle;
    // Records the attempt's data into cache_ before forwarding it.
    std::shared_ptr<Recorder> recorder;
    // Set while handle is; reports session GOAWAYs.
    MoQUpstreamGoaways::Watch goawayWatch;
  };

  using UpstreamEstablishmentResult =
      folly::Expected<UpstreamAttempt, SubscribeError>;

  class ForwarderCallback;

  MoQProxyTrack(
      FullTrackName fullTrackName,
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
      CacheConfig cacheConfig,
      UpstreamConfig upstreamConfig);

  folly::coro::Task<Publisher::SubscribeResult> handleFirstSubscription(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer,
      DownstreamPeer downstream);

  Publisher::SubscribeResult addSubscriber(
      const SubscribeRequest& subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer,
      DownstreamPeer downstream);

  // Take their arguments by value, and establishUpstream holds the track, so a
  // caller may run it detached from its frame. establishWithProvider runs only
  // inside establishUpstream.
  folly::coro::Task<UpstreamEstablishmentResult> establishUpstream(
      SubscribeRequest subscribeRequest,
      DownstreamPeer downstream,
      folly::CancellationToken cancelled);

  folly::coro::Task<UpstreamEstablishmentResult> establishWithProvider(
      std::shared_ptr<MoQUpstreamProvider> upstreamProvider,
      SubscribeRequest subscribeRequest,
      DownstreamPeer downstream,
      bool hasFallbackProvider,
      folly::CancellationToken cancelled);

  // Also gives the track the attempt's Largest and extensions, which only the
  // first upstream may do.
  void adoptFirstUpstream(UpstreamAttempt attempt);

  SubscribeError makeSubscribeError(
      RequestID requestID,
      const UpstreamEstablishmentFailure& failure) const;

  void completeUpstreamEstablishment(
      std::optional<UpstreamEstablishmentFailure> failure);
  void onForwarderEmpty();
  void onUpstreamPublishDone();
  void onUpstreamGoaway(const Goaway& goaway);
  void onUpstreamLost(const PublishDone& publishDone);
  void notifyNoSubscribers();
  void stopCaching();
  void setLargestObjectForPendingSubscribers();

  FullTrackName fullTrackName_;
  std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders_;
  std::shared_ptr<MoQForwarder> forwarder_;
  std::shared_ptr<MoQGroupCache> cache_;
  std::shared_ptr<MoQUpstreamGoaways> goaways_;
  std::shared_ptr<MoQProxySwitchStats> switchStats_;
  UpstreamAttempt upstream_;
  // Cancels establishing upstream_ once the track stops waiting for it. A
  // cancelled source stays cancelled, so each later establishment needs a
  // fresh one.
  folly::CancellationSource establishing_;
  // The Largest in the upstream SUBSCRIBE_OK, which anchors the first
  // subscriber even if early data advanced the forwarder.
  std::optional<AbsoluteLocation> upstreamLargest_;
  std::vector<std::weak_ptr<DownstreamSubscriptionHandle>> pendingSubscribers_;
  std::weak_ptr<Callback> callback_;
  folly::coro::SharedPromise<folly::Unit> upstreamSubscriptionReadyPromise_;
  std::optional<UpstreamEstablishmentFailure> upstreamSubscriptionFailure_;
  State state_{State::IDLE};
  bool upstreamSubscriptionPromiseResolved_{false};
  bool noSubscribersNotificationSent_{false};
  // The forwarder already sent viewers PUBLISH_DONE. It keeps the ones still
  // finishing a subgroup, and another publishDone would send it to them again.
  bool viewersEnded_{false};
};

} // namespace moxygen
