/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/SubscriptionRetryBackoff.h"

#include <folly/portability/GTest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <optional>
#include <random>

namespace moxygen::lane_manager { namespace {

using namespace std::chrono_literals;
using Duration = std::chrono::milliseconds;
using detail::nextSubscriptionRetryDelay;
using detail::SubscriptionRetryBackoff;

TEST(SubscriptionRetryBackoffTest, AcceptedFlappingKeepsTheSequence) {
  SubscriptionRetryBackoff backoff;
  SubscriptionRetryBackoff::Clock::time_point now{};
  std::mt19937_64 rng(47);
  std::mt19937_64 reference(47);
  for (uint64_t bound :
       {100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 30000, 30000}) {
    SCOPED_TRACE(bound);
    backoff.onActive(now);
    now += 1ms;
    backoff.onInactive(now);

    const Duration expected(
        std::uniform_int_distribution<uint64_t>(1, bound)(reference));
    EXPECT_EQ(backoff.nextDelay(std::nullopt, rng), expected);
    now += expected;
  }
}

TEST(SubscriptionRetryBackoffTest, OnlyContinuousActiveTimeResetsTheSequence) {
  SubscriptionRetryBackoff backoff;
  SubscriptionRetryBackoff::Clock::time_point now{};
  std::mt19937_64 rng(53);
  std::mt19937_64 reference(53);

  backoff.onActive(now);
  backoff.onInactive(now + 9999ms);
  EXPECT_EQ(
      backoff.nextDelay(std::nullopt, rng),
      Duration(std::uniform_int_distribution<uint64_t>(1, 100)(reference)));

  now += 1h;
  backoff.onInactive(now);
  EXPECT_EQ(
      backoff.nextDelay(std::nullopt, rng),
      Duration(std::uniform_int_distribution<uint64_t>(1, 200)(reference)));

  backoff.onActive(now);
  backoff.onInactive(now + 9999ms);
  EXPECT_EQ(
      backoff.nextDelay(std::nullopt, rng),
      Duration(std::uniform_int_distribution<uint64_t>(1, 400)(reference)));

  now += 1min;
  backoff.onActive(now);
  backoff.onInactive(now + 10s);
  EXPECT_EQ(
      backoff.nextDelay(std::nullopt, rng),
      Duration(std::uniform_int_distribution<uint64_t>(1, 100)(reference)));
  EXPECT_EQ(
      backoff.nextDelay(std::nullopt, rng),
      Duration(std::uniform_int_distribution<uint64_t>(1, 200)(reference)));
}

TEST(SubscriptionRetryBackoffTest, WindowDoublesAndStaysCapped) {
  Duration window{0};
  std::mt19937_64 rng(7);
  for (const auto expected : std::array{
           100ms,
           200ms,
           400ms,
           800ms,
           1600ms,
           3200ms,
           6400ms,
           12800ms,
           25600ms,
           30000ms}) {
    const auto delay = nextSubscriptionRetryDelay(window, std::nullopt, rng);
    EXPECT_EQ(window, expected);
    EXPECT_GE(delay, 1ms);
    EXPECT_LE(delay, expected);
  }
  for (size_t i = 0; i < 4096; ++i) {
    const auto delay = nextSubscriptionRetryDelay(window, std::nullopt, rng);
    ASSERT_EQ(window, 30000ms);
    ASSERT_GE(delay, 1ms);
    ASSERT_LE(delay, 30000ms);
  }
}

TEST(SubscriptionRetryBackoffTest, UsesTheSuppliedGenerator) {
  Duration window{0};
  std::mt19937_64 rng(17);
  std::mt19937_64 reference(17);
  for (uint64_t bound : {100, 200, 400, 800, 1600, 3200}) {
    const Duration expected(
        std::uniform_int_distribution<uint64_t>(1, bound)(reference));
    EXPECT_EQ(nextSubscriptionRetryDelay(window, std::nullopt, rng), expected);
  }
  EXPECT_EQ(rng(), reference());
}

TEST(SubscriptionRetryBackoffTest, ServerMinimumKeepsItsOwnJitterSpread) {
  struct Case {
    Duration encoded;
    Duration minimum;
    uint64_t spread;
  };
  const std::array cases{
      Case{1ms, 0ms, 100},
      Case{2ms, 1ms, 100},
      Case{101ms, 100ms, 100},
      Case{4001ms, 4000ms, 1000},
      Case{120001ms, 120000ms, 30000},
      Case{600001ms, 600000ms, 30000}};
  for (const auto& test : cases) {
    std::mt19937_64 rng(29);
    std::mt19937_64 reference(29);
    for (size_t i = 0; i < 64; ++i) {
      Duration window{0};
      const Duration jitter(
          std::uniform_int_distribution<uint64_t>(1, test.spread)(reference));
      const auto delay = nextSubscriptionRetryDelay(window, test.encoded, rng);
      EXPECT_EQ(delay, test.minimum + jitter);
      EXPECT_GT(delay, test.minimum);
      EXPECT_LE(delay, test.minimum + Duration(test.spread));
      EXPECT_EQ(window, 100ms);
    }
  }
}

TEST(SubscriptionRetryBackoffTest, ZeroWindowStartsANewSequence) {
  Duration window{0};
  std::mt19937_64 rng(37);
  for (size_t i = 0; i < 16; ++i) {
    nextSubscriptionRetryDelay(window, std::nullopt, rng);
  }
  ASSERT_EQ(window, 30000ms);

  window = 0ms;
  auto reference = rng;
  const Duration expected(
      std::uniform_int_distribution<uint64_t>(1, 100)(reference));
  EXPECT_EQ(nextSubscriptionRetryDelay(window, std::nullopt, rng), expected);
  EXPECT_EQ(window, 100ms);
  nextSubscriptionRetryDelay(window, std::nullopt, rng);
  EXPECT_EQ(window, 200ms);
}

TEST(SubscriptionRetryBackoffTest, MaximumServerHintsCannotOverflow) {
  const auto maximum = Duration::max();
  for (const auto encoded :
       std::array{maximum, maximum - 1ms, maximum - 100ms}) {
    const auto minimum = encoded - 1ms;
    const auto spread = static_cast<uint64_t>((maximum - minimum).count());
    Duration window = maximum;
    std::mt19937_64 rng(43);
    std::mt19937_64 reference(43);
    const Duration jitter(
        std::uniform_int_distribution<uint64_t>(1, spread)(reference));

    const auto delay = nextSubscriptionRetryDelay(window, encoded, rng);

    EXPECT_EQ(window, 30000ms);
    EXPECT_EQ(delay, minimum + jitter);
    EXPECT_GT(delay, minimum);
    EXPECT_LE(delay, maximum);
  }
}

}} // namespace moxygen::lane_manager
