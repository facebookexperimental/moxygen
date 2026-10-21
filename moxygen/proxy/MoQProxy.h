/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/container/F14Map.h>
#include <folly/futures/Future.h>
#include <chrono>
#include <memory>
#include <vector>

#include "moxygen/Publisher.h"
#include "moxygen/Subscriber.h"
#include "moxygen/proxy/MoQGroupCache.h"
#include "moxygen/proxy/MoQProxyTrack.h"
#include "moxygen/proxy/MoQUpstreamProvider.h"

namespace moxygen {

struct MoQProxyOptions {
  // Newest group IDs cached per track, including the active group. Zero
  // disables the cache, so every joining FETCH goes upstream.
  size_t maxGroupsPerTrack{3};
  // Accounted bytes shared by every track's cache and accepted replays.
  // Admission is first come, first served; overload only causes misses.
  size_t maxCacheBytes{10 * 1024 * 1024};
  // Absolute extra wait for objects still arriving locally. Provisional.
  std::chrono::milliseconds cacheCompletionWait{100};
  // Longest time a local replay may go without an accepted write.
  // Provisional; must be positive.
  std::chrono::milliseconds replayStallTimeout{std::chrono::seconds(5)};
  // Times the bounded waits above; null uses folly's default.
  std::shared_ptr<folly::Timekeeper> timekeeper;
};

// Counters for telling a working cache from one that only passes warm tests.
struct MoQProxyJoinStats {
  uint64_t immediateHits{0};
  uint64_t hitsAfterWait{0};
  // History not locally available, or the cache is disabled.
  uint64_t fallbacksMiss{0};
  uint64_t fallbacksWaitExpired{0};
  // The budget could not charge the replay plan.
  uint64_t fallbacksPlanRejected{0};
  uint64_t replayStallResets{0};
  std::chrono::milliseconds completionWaitTime{0};
};

// One instance, its tracks and its cache budget run on a single event loop.
class MoQProxy : public Publisher,
                 public Subscriber,
                 public std::enable_shared_from_this<MoQProxy>,
                 public MoQProxyTrack::Callback {
 public:
  static std::shared_ptr<MoQProxy> create(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
      MoQProxyOptions options = {});

  ~MoQProxy() override;

  MoQProxy(const MoQProxy&) = delete;
  MoQProxy& operator=(const MoQProxy&) = delete;
  MoQProxy(MoQProxy&&) = delete;
  MoQProxy& operator=(MoQProxy&&) = delete;

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override;

  Subscriber::PublishResult publish(
      PublishRequest publishRequest,
      std::shared_ptr<Publisher::SubscriptionHandle> handle = nullptr) override;

  void close();

  const std::shared_ptr<MoQCacheBudget>& cacheBudget() const {
    return budget_;
  }

  const MoQProxyJoinStats& joinStats() const {
    return *joinStats_;
  }

  std::shared_ptr<MoQProxyTrack> findTrack(
      const FullTrackName& fullTrackName) const;

 private:
  class LateBoundPublishConsumer;
  struct PublishAttempt {
    PublishOk ok;
    std::shared_ptr<TrackConsumer> consumer;
    std::shared_ptr<MoQSession> session;
  };

  using PublishReplyTask =
      folly::coro::Task<folly::Expected<PublishOk, PublishError>>;
  using PublishAttemptResult = folly::Expected<PublishAttempt, RequestError>;

  MoQProxy(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders,
      MoQProxyOptions options);

  static PublishReplyTask forwardPublish(
      std::shared_ptr<MoQProxy> self,
      PublishRequest publishRequest,
      std::shared_ptr<Publisher::SubscriptionHandle> handle,
      std::shared_ptr<LateBoundPublishConsumer> consumer,
      SessionId downstreamSessionId);

  folly::coro::Task<FetchResult> forwardStandaloneFetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer);

  folly::coro::Task<FetchResult> joiningFetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer);

  folly::coro::Task<FetchResult> startLocalReplay(
      const std::shared_ptr<MoQProxyTrack>& track,
      RequestID requestID,
      GroupOrder groupOrder,
      MoQReplayPlan plan,
      std::shared_ptr<FetchConsumer> consumer);

  folly::coro::Task<FetchResult> upstreamJoiningFetch(
      std::shared_ptr<MoQProxyTrack> track,
      Fetch fetch,
      MoQProxyTrack::JoiningRange range,
      std::shared_ptr<FetchConsumer> consumer);

  static folly::coro::Task<PublishAttemptResult> publishToUpstream(
      std::shared_ptr<MoQSession> upstreamSession,
      SessionId downstreamSessionId,
      PublishRequest publishRequest,
      std::shared_ptr<Publisher::SubscriptionHandle> handle);
  static void cancelPublishAttempt(PublishAttempt& attempt);

  template <
      typename Result,
      typename Operation,
      typename Cleanup,
      typename... Args>
  folly::coro::Task<folly::Expected<Result, RequestError>> tryUpstreams(
      RequestID requestID,
      const FullTrackName& fullTrackName,
      const TrackRequestParameters& params,
      Operation operation,
      Cleanup cleanup,
      Args... args);

  std::shared_ptr<MoQProxyTrack> getOrCreateTrack(
      const FullTrackName& fullTrackName);

  void onNoSubscribers(MoQProxyTrack* track) override;

  std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders_;
  MoQProxyOptions options_;
  std::shared_ptr<MoQCacheBudget> budget_;
  // Shared with detached replays, which can outlive the proxy.
  std::shared_ptr<MoQProxyJoinStats> joinStats_{
      std::make_shared<MoQProxyJoinStats>()};
  // Cancels cache waits and local replays when the proxy closes.
  folly::CancellationSource shutdown_;
  folly::F14FastMap<
      FullTrackName,
      std::shared_ptr<MoQProxyTrack>,
      FullTrackName::hash>
      tracks_;
  bool closed_{false};
};

} // namespace moxygen
