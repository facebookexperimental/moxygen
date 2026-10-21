/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/coro/Task.h>
#include <folly/futures/Future.h>
#include <chrono>
#include <memory>

#include "moxygen/MoQConsumers.h"
#include "moxygen/Publisher.h"
#include "moxygen/proxy/MoQGroupCache.h"

namespace moxygen {

// Waits while the range can still complete from ingress that is open now.
// Returns Ready or Miss as soon as the cache knows; AwaitIngress means maxWait
// passed or the caller was cancelled first. A null timekeeper uses folly's
// default.
folly::coro::Task<MoQGroupCache::Coverage> awaitCacheCoverage(
    std::shared_ptr<MoQGroupCache> cache,
    AbsoluteLocation start,
    AbsoluteLocation end,
    std::chrono::milliseconds maxWait,
    folly::Timekeeper* timekeeper = nullptr);

struct MoQReplayConfig {
  // Longest wait for the consumer to accept the next write before the replay
  // is reset. Each object is one write, so this must cover draining the
  // largest object.
  std::chrono::milliseconds stallTimeout{std::chrono::seconds(5)};
  // Writes between yields, so a large plan cannot starve live forwarding.
  size_t writesPerYield{16};
  // Null uses folly's default. Shared, since a replay can outlive its caller.
  std::shared_ptr<folly::Timekeeper> timekeeper;
};

enum class MoQReplayOutcome { Finished, Cancelled, Stalled, Failed };

// Writes a replay plan to a FETCH consumer, waiting for readiness before
// every write. Resets the consumer on cancellation, stall, or a write error,
// and finishes or resets exactly once.
folly::coro::Task<MoQReplayOutcome> runReplay(
    MoQReplayPlan plan,
    std::shared_ptr<FetchConsumer> consumer,
    MoQReplayConfig config);

// The handle for a FETCH served from the group cache. Destroying it, or
// fetchCancel(), stops the replay.
class MoQLocalFetchHandle : public Publisher::FetchHandle {
 public:
  explicit MoQLocalFetchHandle(FetchOk ok)
      : Publisher::FetchHandle(std::move(ok)) {}
  ~MoQLocalFetchHandle() override {
    source_.requestCancellation();
  }

  MoQLocalFetchHandle(const MoQLocalFetchHandle&) = delete;
  MoQLocalFetchHandle& operator=(const MoQLocalFetchHandle&) = delete;
  MoQLocalFetchHandle(MoQLocalFetchHandle&&) = delete;
  MoQLocalFetchHandle& operator=(MoQLocalFetchHandle&&) = delete;

  void fetchCancel() override {
    source_.requestCancellation();
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate requestUpdate) override;

  folly::CancellationToken getToken() const {
    return source_.getToken();
  }

 private:
  folly::CancellationSource source_;
};

} // namespace moxygen
