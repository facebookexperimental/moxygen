/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/Executor.h>
#include <folly/Expected.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Task.h>
#include <chrono>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "moxygen/Publisher.h"
#include "moxygen/proxy/MoQGroupCache.h"
#include "moxygen/proxy/MoQUpstreamGoaways.h"
#include "moxygen/proxy/MoQUpstreamHandoffArbiter.h"
#include "moxygen/proxy/MoQUpstreamProvider.h"

namespace folly {
class Timekeeper;
} // namespace folly

namespace moxygen {

class MoQForwarder;
class MoQFetchCacheWriter;
class MoQSession;
class FetchConsumer;
class TrackConsumer;

struct MoQProxyTrackCacheConfig {
  // Newest group IDs kept, including the active group; 0 disables caching.
  size_t maxGroups{0};
  std::shared_ptr<MoQCacheBudget> budget;
};

// How a track moves to a replacement upstream when its upstream goes away.
struct MoQProxySwitchOptions {
  // How long viewers wait, from losing the upstream, for a replacement the
  // track adopted to forward data, before their subscriptions end with
  // GOING_AWAY.
  std::chrono::milliseconds deadline{std::chrono::seconds(10)};
  // The wait before trying the providers again once they all failed, doubled
  // per pass up to maxRetryBackoff, each with +/-20% jitter.
  std::chrono::milliseconds retryBackoff{100};
  std::chrono::milliseconds maxRetryBackoff{1000};
  // Seeds the jitter; 0 seeds it randomly.
  uint64_t randomSeed{0};
};

// A switch completes once a replacement the track adopted has forwarded an
// object; a stream header alone doesn't count.
struct MoQProxySwitchCounters {
  uint64_t started{0};
  uint64_t completed{0};
  // None by the deadline; viewers got PUBLISH_DONE(GOING_AWAY).
  uint64_t failed{0};
  // From losing the upstream to completing, summed over completed switches.
  std::chrono::milliseconds duration{0};
};

// Where viewers may have missed groups because the track resumed from another
// upstream.
struct MoQProxyGapCounters {
  uint64_t gaps{0};
  // From the last group forwarded before each gap, which may be unfinished, up
  // to the first group after it, exclusive, summed. Group IDs can skip, so
  // this counts IDs the origin never used too. A gap with nothing forwarded
  // before it adds none.
  uint64_t groups{0};
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
  // Switches that started because the upstream was lost. A replacement lost
  // before the switch completes doesn't start another.
  MoQProxySwitchCounters lostSwitches;
  // Replacement data refused before it reached the cache or viewers: subgroup
  // and object streams, and datagrams.
  uint64_t refusedStreams{0};
  uint64_t refusedDatagrams{0};
  // Gaps a lost upstream left: after its GOAWAY, which a replacement
  // established in time would have avoided, or without warning.
  MoQProxyGapCounters lateReplacementGaps;
  MoQProxyGapCounters lostGaps;
  // Replacement streams that started mid-subgroup in a group the replacement
  // already owned, so that group reached viewers without them.
  uint64_t midSubgroupGaps{0};
  // From losing an upstream to the first object forwarded from its replacement,
  // summed.
  std::chrono::milliseconds lostResumeTime{0};
};

// Shared by the tracks of one proxy; a null member gets a per-track default.
// Tracks that can share an upstream session must share goaways: a session's
// GOAWAYs reach only the registry that watched it last.
struct MoQProxyTrackUpstreamConfig {
  std::shared_ptr<MoQUpstreamGoaways> goaways;
  std::shared_ptr<MoQProxySwitchStats> switchStats;
  MoQProxySwitchOptions switchOptions;
  // Times the switch deadline and retry backoff; null uses folly's default.
  std::shared_ptr<folly::Timekeeper> timekeeper;
  // Null seeds a per-track generator from switchOptions.randomSeed.
  std::shared_ptr<std::mt19937_64> random;
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

  using JoiningRange = MoQGroupCache::Range;

  struct SharedFetchState {
    folly::coro::SharedPromise<folly::Unit> accepted;
    folly::coro::SharedPromise<folly::Unit> finished;
    std::optional<JoiningRange> range;
    std::optional<FetchError> error;
    std::optional<FetchOk> fetchOk;
    std::weak_ptr<Publisher::FetchHandle> fetchHandle;
    std::weak_ptr<MoQSession> session;
  };

  std::shared_ptr<SharedFetchState> saveJoiningFetchForUpstream(
      Fetch upstreamFetch,
      std::shared_ptr<FetchConsumer> consumer);
  void trySendJoiningFetchToCurrentUpstream();
  void cancelJoiningFetch(const std::shared_ptr<SharedFetchState>& fetch);

  folly::Expected<std::optional<JoiningRange>, FetchError> resolveJoiningFetch(
      SessionId downstreamSessionId,
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
  bool hasEstablishedUpstream() const;
  const Extensions& trackExtensions() const;

  void close();

 private:
  // SWITCHING looks for a replacement for a lost upstream while viewers stay
  // subscribed.
  enum class State { IDLE, CONNECTING, READY, SWITCHING, DRAINING, CLOSED };

  struct UpstreamEstablishmentFailure {
    SubscribeErrorCode errorCode;
    std::string reasonPhrase;
  };

  class Recorder;
  class SubgroupRecorder;
  class DownstreamSubscriptionHandle;

  struct UpstreamJoiningFetchAttempt {
    uint64_t id{0};
    folly::CancellationSource cancellation;
    std::optional<FetchError> error;
    std::shared_ptr<MoQFetchCacheWriter> writer;
    std::shared_ptr<Publisher::FetchHandle> handle;
    bool subscriptionAccepted{false};
    bool finished{false};
  };

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
  // inside establishUpstream. Once an upstream was lost, attempts are
  // replacements: they refuse lostSessions_, subscribe from the next group
  // start, and admit data through a handoff arbiter, so viewers never get a
  // group twice. An attempt whose upstream goes away before the track adopts
  // it fails, and later attempts replace it.
  folly::coro::Task<UpstreamEstablishmentResult> establishUpstream(
      SubscribeRequest subscribeRequest,
      DownstreamPeer downstream,
      folly::CancellationToken cancelled);

  folly::coro::Task<UpstreamEstablishmentResult> establishWithProvider(
      std::shared_ptr<MoQUpstreamProvider> upstreamProvider,
      SubscribeRequest subscribeRequest,
      DownstreamPeer downstream,
      bool hasFallbackProvider,
      folly::CancellationToken cancelled,
      std::vector<SessionId> lostSessions);

  // Also gives the track the attempt's Largest and extensions, which only the
  // first upstream may do.
  void adoptFirstUpstream(UpstreamAttempt attempt);

  // Whether the track takes over an upstream PUBLISH_DONE instead of passing
  // it to viewers: a lost upstream it can replace.
  bool takeOverUpstreamEnd(Recorder& recorder, const PublishDone& done);
  void startLostSwitch(const Recorder& lost);
  void releaseUpstream();
  void noteLostUpstream(const Recorder& lost);
  folly::Executor::KeepAlive<> switchExecutor() const;
  folly::coro::Task<void> findReplacement(folly::CancellationToken cancelled);
  folly::coro::Task<void> enforceSwitchDeadline(
      folly::CancellationToken stopped);
  void adoptReplacement(UpstreamAttempt attempt);
  void completeSwitch();
  void failSwitch();
  std::chrono::milliseconds jittered(std::chrono::milliseconds delay);
  std::optional<uint64_t> highestForwardedGroup() const;
  void onReplacementRefused(
      MoQUpstreamHandoffArbiter::Admit admitted,
      bool datagram);
  void onReplacementResumed(const Recorder& recorder, uint64_t firstGroup);

  SubscribeError makeSubscribeError(
      RequestID requestID,
      const UpstreamEstablishmentFailure& failure) const;

  void completeUpstreamEstablishment(
      std::optional<UpstreamEstablishmentFailure> failure);
  // Stops waiting for the attempt being established, and for its GOAWAYs,
  // and stops the switch deadline.
  void cancelEstablishment();
  void onForwarderEmpty();
  void onUpstreamPublishDone();
  void onUpstreamGoaway(const Goaway& goaway);
  void onUpstreamLost(const PublishDone& publishDone);
  void notifyNoSubscribers();
  void stopCaching();
  void joiningFetchAccepted(
      uint64_t attempt,
      std::shared_ptr<Publisher::FetchHandle> handle);
  void joiningFetchFailed(uint64_t attempt, FetchError error);
  void joiningFetchFinished(uint64_t attempt);
  void resolveJoiningFetchAttempt(uint64_t attempt);
  void cancelJoiningFetchAttempt(uint64_t attempt);
  void failJoiningFetch(FetchError error);
  void setLargestObjectForPendingSubscribers();

  FullTrackName fullTrackName_;
  std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders_;
  std::shared_ptr<MoQForwarder> forwarder_;
  std::shared_ptr<MoQGroupCache> cache_;
  std::shared_ptr<MoQUpstreamGoaways> goaways_;
  std::shared_ptr<MoQProxySwitchStats> switchStats_;
  MoQProxySwitchOptions switchOptions_;
  std::shared_ptr<folly::Timekeeper> timekeeper_;
  std::shared_ptr<std::mt19937_64> random_;
  UpstreamAttempt upstream_;
  // The GOAWAY watch of the upstream attempt being established, which the
  // track stops when it stops waiting for that attempt.
  MoQUpstreamGoaways::Watch establishingGoawayWatch_;
  // Cancels establishing an upstream, the first or a replacement, once the
  // track stops waiting for it. A cancelled source stays cancelled, so each
  // later establishment needs a fresh one.
  folly::CancellationSource establishing_;
  // The Largest in the upstream SUBSCRIBE_OK, which anchors the first
  // subscriber even if early data advanced the forwarder.
  std::optional<AbsoluteLocation> upstreamLargest_;
  std::vector<std::weak_ptr<DownstreamSubscriptionHandle>> pendingSubscribers_;
  std::optional<Fetch> joiningFetchRequest_;
  std::shared_ptr<FetchConsumer> joiningFetchConsumer_;
  bool joiningFetchExposed_{false};
  std::shared_ptr<SharedFetchState> joiningFetchStatus_;
  std::optional<UpstreamJoiningFetchAttempt> joiningFetchAttempt_;
  uint64_t upstreamAttempt_{0};
  // The first subscription's request and peer, which replacements reuse.
  std::optional<SubscribeRequest> upstreamRequest_;
  DownstreamPeer upstreamPeer_;
  // Where the first subscription ran; switches run there too.
  folly::Executor* executor_{nullptr};
  // Sessions of the upstreams lost since the current switch started, or
  // while the first subscription was establishing; no attempt uses them.
  std::vector<SessionId> lostSessions_;
  // Set until the current switch completes or fails.
  std::optional<std::chrono::steady_clock::time_point> switchStartedAt_;
  // Stops the current switch's deadline.
  folly::CancellationSource switchDeadline_;
  // A lost upstream whose viewers wait for a replacement's first data.
  struct LostUpstream {
    std::chrono::steady_clock::time_point lostAt;
    std::optional<uint64_t> lastGroup;
    bool afterGoaway{false};
  };
  std::optional<LostUpstream> lostUpstream_;
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
