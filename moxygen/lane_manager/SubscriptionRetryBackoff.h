/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Random.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Task.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <utility>

namespace moxygen::lane_manager::detail {

inline constexpr std::chrono::milliseconds kInitialSubscriptionRetryWindow{100};
inline constexpr std::chrono::milliseconds kMaxSubscriptionRetryWindow{30000};
inline constexpr std::chrono::seconds kStableSubscriptionInterval{10};

inline folly::coro::Task<void> waitForSubscriptionRetry(
    std::chrono::milliseconds remaining) {
  constexpr auto kMaxSleep =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::hours(24));
  while (remaining > std::chrono::milliseconds(0)) {
    const auto part = std::min(remaining, kMaxSleep);
    co_await folly::coro::sleep(part);
    remaining -= part;
  }
}

// A zero window starts a new sequence. The caller handles Retry Interval = 0.
template <class RNG = folly::ThreadLocalPRNG>
std::chrono::milliseconds nextSubscriptionRetryDelay(
    std::chrono::milliseconds& window,
    std::optional<std::chrono::milliseconds> retryInterval = std::nullopt,
    RNG&& rng = RNG()) {
  using Duration = std::chrono::milliseconds;
  if (window < kInitialSubscriptionRetryWindow) {
    window = kInitialSubscriptionRetryWindow;
  } else if (window >= kMaxSubscriptionRetryWindow / 2) {
    window = kMaxSubscriptionRetryWindow;
  } else {
    window *= 2;
  }
  const auto minimum = retryInterval && *retryInterval > Duration(1)
      ? *retryInterval - Duration(1)
      : Duration(0);
  const auto spread = std::min(
      {kMaxSubscriptionRetryWindow,
       std::max(window, minimum / 4),
       Duration::max() - minimum});
  const Duration jitter(
      folly::Random::rand64(
          1,
          static_cast<uint64_t>(spread.count()) + 1,
          std::forward<RNG>(rng)));
  return minimum + jitter;
}

class SubscriptionRetryBackoff {
 public:
  using Clock = std::chrono::steady_clock;

  void onActive(Clock::time_point now) {
    activeSince_ = now;
  }

  void onInactive(Clock::time_point now) {
    if (activeSince_ && now - *activeSince_ >= kStableSubscriptionInterval) {
      window_ = std::chrono::milliseconds(0);
    }
    activeSince_.reset();
  }

  template <class RNG = folly::ThreadLocalPRNG>
  std::chrono::milliseconds nextDelay(
      std::optional<std::chrono::milliseconds> retryInterval = std::nullopt,
      RNG&& rng = RNG()) {
    return nextSubscriptionRetryDelay(
        window_, retryInterval, std::forward<RNG>(rng));
  }

 private:
  std::chrono::milliseconds window_{0};
  std::optional<Clock::time_point> activeSince_;
};

} // namespace moxygen::lane_manager::detail
