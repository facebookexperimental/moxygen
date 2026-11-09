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
#include <functional>
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
  // GOING_AWAY. On a GOAWAY, how long the track looks for a replacement, from
  // the GOAWAY, and no longer than its timeout.
  std::chrono::milliseconds deadline{std::chrono::seconds(10)};
  // On a GOAWAY, the track looks for a replacement after a random delay of up
  // to a quarter of the GOAWAY's timeout, and at most this, so the tracks of a
  // session going away don't all subscribe again at once.
  std::chrono::milliseconds maxStartDelay{std::chrono::seconds(1)};
  // How long the upstream that went away must stay caught up with its
  // replacement before the track lets it go, since a stream for one of its
  // groups can still arrive late.
  std::chrono::milliseconds reorderGrace{200};
  // If the upstream that went away hasn't caught up within two group
  // durations of the adopted replacement forwarding its first object, and no
  // sooner than minTail, the replacement takes over and the old upstream's
  // unfinished groups reach viewers cut short. That happens at least
  // tailMargin before the GOAWAY's timeout, or once a reorder grace that
  // runs then ends without a handover. Until the track has seen two groups
  // start, a group lasts defaultGroupDuration.
  std::chrono::milliseconds minTail{std::chrono::seconds(1)};
  std::chrono::milliseconds tailMargin{500};
  std::chrono::milliseconds defaultGroupDuration{std::chrono::seconds(2)};
  // The wait before trying the providers again once they all failed, doubled
  // per pass up to maxRetryBackoff, each with +/-20% jitter.
  std::chrono::milliseconds retryBackoff{100};
  std::chrono::milliseconds maxRetryBackoff{1000};
  // Seeds the jitter; 0 seeds it randomly.
  uint64_t randomSeed{0};
};

struct MoQProxySwitchCounters {
  uint64_t started{0};
  uint64_t completed{0};
  uint64_t failed{0};
  // From the switch starting to completing, summed over completed switches.
  std::chrono::milliseconds duration{0};
};

// Where a replacement found on a GOAWAY started, against the groups of the
// upstream that went away.
struct MoQProxyHandoffCounters {
  // At or before a group the old upstream had opened, so the replacement's
  // copies of those groups were refused.
  uint64_t behind{0};
  // After them, and the old upstream opened no group before it, as when it
  // starts at the old upstream's next group.
  uint64_t equal{0};
  // Later: the old upstream filled groups before it.
  uint64_t ahead{0};
};

// Graceful switches that let go of the upstream that went away before it
// caught up with its replacement.
struct MoQProxyCutoffCounters {
  uint64_t cutoffs{0};
  // The groups it had opened and not finished, which reached viewers cut
  // short, summed. Groups it never opened, and object streams, which each end
  // with their object, aren't counted.
  uint64_t unfinishedGroups{0};
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
  // Switches that started because the upstream was lost. One completes once a
  // replacement the track adopted has forwarded an object, since a stream
  // header alone doesn't count, or at the deadline when the adopted one has
  // sent nothing but has nothing newer than the track's last group, or no
  // viewer forwards, as on a quiet track. It fails at the deadline otherwise,
  // which ends viewers' subscriptions with GOING_AWAY. A replacement lost
  // before the switch completes doesn't start another.
  MoQProxySwitchCounters lostSwitches;
  // Switches that started because the upstream sent a GOAWAY, while it went on
  // serving. One completes once the track lets go of that upstream, which has
  // caught up with the replacement or ended, or which, like the replacement,
  // has had nothing to send for the reorder grace, and fails when it has no
  // adopted replacement once the deadline passed; the upstream then serves
  // until it goes. One whose upstream is lost before an adopted replacement
  // forwarded an object carries on as a lost switch, counted there. That one,
  // and one stopped by the upstream ending the track or by the track closing,
  // neither completes nor fails here.
  MoQProxySwitchCounters gracefulSwitches;
  // Completed graceful switches whose replacement delivered data before the
  // old upstream ended.
  MoQProxyHandoffCounters handoffs;
  // Graceful switches whose old upstream was lost, or that the tail timer let
  // go of, before it caught up. The tail timer's hand over to the replacement
  // and complete.
  MoQProxyCutoffCounters lostCutoffs;
  MoQProxyCutoffCounters tailCutoffs;
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
  // What the track measures group durations by; null reads the steady clock.
  std::function<std::chrono::steady_clock::time_point()> clock;
  // Null seeds a per-track generator from switchOptions.randomSeed.
  std::shared_ptr<std::mt19937_64> random;
};

// How far apart a track's groups start, smoothed: each new spacing weighs an
// eighth.
class MoQGroupDurationEstimate {
 public:
  void onGroupStart(std::chrono::steady_clock::time_point at);
  const std::optional<std::chrono::milliseconds>& duration() const {
    return duration_;
  }

 private:
  std::optional<std::chrono::steady_clock::time_point> lastStart_;
  std::optional<std::chrono::milliseconds> duration_;
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
  // For history, a session other than those already tried: the upstream's, or
  // that of the replacement a switch on a GOAWAY holds. While a switch runs
  // and has neither, this waits for one. Null once no switch runs. The caller
  // holds the track while it waits.
  folly::coro::Task<std::shared_ptr<MoQSession>> awaitUpstreamSession(
      std::vector<std::shared_ptr<MoQSession>> tried);
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
  // inside establishUpstream. Once an upstream was lost or went away, attempts
  // are replacements: they refuse lostSessions_, subscribe from the next group
  // start, and admit data through a handoff arbiter, so viewers never get a
  // group twice. An attempt whose upstream is lost before the track adopts it
  // fails, and later attempts replace it; one whose upstream sent a GOAWAY is
  // adopted, and switched from.
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
  // Runs a switch task, such as findReplacement, on switchExecutor(), holding
  // the track until it ends.
  void runDetached(folly::coro::Task<void> task);
  folly::coro::Task<void> findReplacement(folly::CancellationToken cancelled);
  folly::coro::Task<void> enforceSwitchDeadline(
      std::chrono::milliseconds deadline,
      folly::CancellationToken stopped);
  void adoptReplacement(UpstreamAttempt attempt);
  void completeSwitch();
  void failSwitch();
  // It has sent nothing, not even data its arbiter refused, and the viewers
  // have nothing newer to wait for from it: its Largest is no later than the
  // track's, or no viewer forwards. A quiet track, such as the catalog, may
  // publish nothing for minutes.
  bool hasNothingNewer(const UpstreamAttempt& attempt) const;
  std::shared_ptr<MoQUpstreamHandoffArbiter> arbiterAfterForwarded() const;
  static void unsubscribe(UpstreamAttempt attempt);

  // A switch on a GOAWAY: the old upstream, upstream_, serves until a
  // replacement takes over.
  void switchIfGoingAway();
  void startGracefulSwitch(
      const Goaway& goaway,
      std::chrono::steady_clock::time_point receivedAt);
  std::chrono::milliseconds startDelay(
      std::optional<std::chrono::milliseconds> timeout);
  folly::coro::Task<void> findReplacementAfter(
      std::chrono::milliseconds delay,
      folly::CancellationToken cancelled,
      folly::CancellationToken skipDelay);
  void holdReplacement(UpstreamAttempt attempt);
  void onHandoverProgress(const Recorder& old);
  void checkHandoff();
  // The adopted replacement has forwarded an object.
  bool replacementReady() const;
  // The held replacement has sent nothing, its Largest is the track's, and
  // old has no group open: neither has anything to send.
  bool quietHandoffReady() const;
  folly::coro::Task<void> handOverAfterGrace(
      uint64_t seen,
      folly::CancellationToken stopped);
  void promoteReplacement(
      std::optional<uint64_t> continuedGroup = std::nullopt);
  void countHandoff(const MoQUpstreamHandoffArbiter& arbiter);
  void onGracefulDeadline();
  bool takeOverOldUpstreamLoss(Recorder& old);
  void countCutoff(MoQProxyCutoffCounters& counters);
  void onGroupStarted();
  void startTail();
  std::chrono::milliseconds tailDuration() const;
  folly::coro::Task<void> letGoOfOldAfterTail(
      std::chrono::milliseconds tail,
      folly::CancellationToken stopped);
  void onTailTimer();
  void dropReplacement(Recorder& replacement);
  // Wakes awaitUpstreamSession, whose outcome depends on state_, upstream_,
  // graceful_ and switchStartedAt_: each change to them that can end its wait
  // calls this. The wait resumes on the executor once the change is done.
  void onSwitchProgress();
  bool switchRuns() const;
  void onReplacementGone(const Recorder& replacement);
  void renewHandoff();
  // Drops a replacement waiting for the old upstream to catch up.
  void endGracefulSwitch();
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
  void onUpstreamGoaway(const Recorder& recorder, const Goaway& goaway);
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
  std::function<std::chrono::steady_clock::time_point()> clock_;
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
  // Sessions of the upstreams lost or going away since the current switch
  // started, or lost while the first subscription was establishing; no attempt
  // uses them.
  std::vector<SessionId> lostSessions_;
  // Set until the current switch completes or fails.
  std::optional<std::chrono::steady_clock::time_point> switchStartedAt_;
  // Stops the current switch's deadline.
  folly::CancellationSource switchDeadline_;
  // Cuts the current graceful switch's start delay short.
  folly::CancellationSource startDelay_;
  struct GracefulSwitch {
    // Shared by the old upstream and the replacement attempts, until one that
    // froze its split is gone.
    std::shared_ptr<MoQUpstreamHandoffArbiter> arbiter;
    std::chrono::steady_clock::time_point startedAt;
    // When the GOAWAY's timeout runs out, if it has one.
    std::optional<std::chrono::steady_clock::time_point> expiresAt;
    // The replacement the track adopted, which takes over once the old
    // upstream has caught up with it.
    std::optional<UpstreamAttempt> replacement;
    // Stops the reorder grace, while it runs.
    std::optional<folly::CancellationSource> grace;
    // Stops the tail timer, from the replacement's first forwarded object.
    std::optional<folly::CancellationSource> tail;
    bool deadlinePassed{false};
    // The old upstream's end, held for a replacement still subscribing.
    std::optional<PublishDone> oldEnd;
  };
  std::optional<GracefulSwitch> graceful_;
  // A lost upstream whose viewers wait for a replacement's first data.
  struct LostUpstream {
    std::chrono::steady_clock::time_point lostAt;
    std::optional<uint64_t> lastGroup;
    bool afterGoaway{false};
  };
  std::optional<LostUpstream> lostUpstream_;
  MoQGroupDurationEstimate groupDuration_;
  std::weak_ptr<Callback> callback_;
  folly::coro::SharedPromise<folly::Unit> upstreamSubscriptionReadyPromise_;
  folly::coro::SharedPromise<folly::Unit> switchProgress_;
  std::optional<UpstreamEstablishmentFailure> upstreamSubscriptionFailure_;
  State state_{State::IDLE};
  bool upstreamSubscriptionPromiseResolved_{false};
  bool noSubscribersNotificationSent_{false};
  // The forwarder already sent viewers PUBLISH_DONE. It keeps the ones still
  // finishing a subgroup, and another publishDone would send it to them again.
  bool viewersEnded_{false};
};

} // namespace moxygen
