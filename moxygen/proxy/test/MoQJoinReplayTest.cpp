/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQJoinReplay.h"

#include <folly/coro/WithCancellation.h>
#include <folly/futures/ManualTimekeeper.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GTest.h>
#include <optional>

#include "moxygen/proxy/test/RecordingFetchConsumer.h"

namespace moxygen::test {
namespace {

using namespace std::chrono_literals;

Payload bytes(size_t length) {
  return length == 0 ? nullptr
                     : folly::IOBuf::copyBuffer(std::string(length, 'x'));
}

class MoQJoinReplayTest : public ::testing::Test {
 protected:
  // Caches objects [0, sizes.size()) of group 1, with the given sizes.
  MoQReplayPlan makePlan(const std::vector<size_t>& sizes) {
    cache_ = std::make_unique<MoQGroupCache>(3, budget_);
    EXPECT_TRUE(cache_->openSubgroup(1, 0, /*containsLastInGroup=*/false));
    for (uint64_t objectID = 0; objectID < sizes.size(); objectID++) {
      EXPECT_EQ(
          cache_->subgroupObject(
              1,
              0,
              objectID,
              ObjectStatus::NORMAL,
              bytes(sizes[objectID]),
              noExtensions()),
          MoQGroupCache::Result::Recorded);
    }
    auto plan =
        cache_->snapshot({1, 0}, {1, sizes.size()}, GroupOrder::OldestFirst);
    EXPECT_TRUE(plan.has_value());
    return std::move(*plan);
  }

  void startReplay(MoQReplayPlan plan) {
    MoQReplayConfig config;
    config.timekeeper = timekeeper_;
    config.stallTimeout = 5s;
    done_ = folly::coro::co_withExecutor(
                &evb_,
                folly::coro::co_withCancellation(
                    cancel_.getToken(),
                    runReplay(std::move(plan), consumer_, config)))
                .start();
    drive();
  }

  // Runs everything ready now. A suspended replay holds a keep-alive, so
  // loop() would not return.
  void drive() {
    do {
      evb_.loopOnce(EVLOOP_NONBLOCK);
    } while (evb_.getNumLoopCallbacks() > 0 ||
             evb_.getNotificationQueueSize() > 0);
  }

  std::optional<MoQReplayOutcome> outcome() {
    if (!done_.isReady()) {
      return std::nullopt;
    }
    return std::move(done_).get();
  }

  void advance(std::chrono::milliseconds delta) {
    timekeeper_->advance(delta);
    drive();
  }

  void TearDown() override {
    // Let any replay still waiting finish, so nothing outlives the loop.
    cancel_.requestCancellation();
    consumer_->makeReady();
    drive();
  }

  folly::EventBase evb_;
  std::shared_ptr<folly::ManualTimekeeper> timekeeper_{
      std::make_shared<folly::ManualTimekeeper>()};
  std::shared_ptr<MoQCacheBudget> budget_{
      std::make_shared<MoQCacheBudget>(1 << 20)};
  std::unique_ptr<MoQGroupCache> cache_;
  std::shared_ptr<RecordingFetchConsumer> consumer_{
      std::make_shared<RecordingFetchConsumer>()};
  folly::CancellationSource cancel_;
  folly::SemiFuture<MoQReplayOutcome> done_{
      folly::SemiFuture<MoQReplayOutcome>::makeEmpty()};
};

} // namespace

TEST_F(MoQJoinReplayTest, WritesPlanInOrderAndFinishesOnce) {
  startReplay(makePlan({10, 0, 20}));

  const std::vector<std::string> expected{
      "object 1/0 10", "object 1/1 0", "object 1/2 20", "endOfFetch"};
  EXPECT_EQ(consumer_->log, expected);
  EXPECT_FALSE(consumer_->resetCode.has_value());
  EXPECT_EQ(outcome(), MoQReplayOutcome::Finished);
}

TEST_F(MoQJoinReplayTest, WaitsForReadinessEvenAfterAnAcceptedWrite) {
  consumer_->readyWrites = 1;

  startReplay(makePlan({10, 10}));

  const std::vector<std::string> firstOnly{"object 1/0 10"};
  EXPECT_EQ(consumer_->log, firstOnly);
  consumer_->makeReady();
  drive();
  const std::vector<std::string> expected{
      "object 1/0 10", "object 1/1 10", "endOfFetch"};
  EXPECT_EQ(consumer_->log, expected);
}

// BLOCKED means the write was accepted: it is not repeated, and the next one
// waits for readiness.
TEST_F(MoQJoinReplayTest, BlockedWriteIsAcceptedAndPausesUntilReady) {
  consumer_->blockedWrites = 1;

  startReplay(makePlan({10, 10}));
  const std::vector<std::string> firstOnly{"object 1/0 10"};
  EXPECT_EQ(consumer_->log, firstOnly);
  consumer_->makeReady();
  drive();

  const std::vector<std::string> expected{
      "object 1/0 10", "object 1/1 10", "endOfFetch"};
  EXPECT_EQ(consumer_->log, expected);
  EXPECT_EQ(outcome(), MoQReplayOutcome::Finished);
}

TEST_F(MoQJoinReplayTest, LargeObjectIsWrittenWhole) {
  startReplay(makePlan({256 * 1024}));

  const std::vector<std::string> expected{"object 1/0 262144", "endOfFetch"};
  EXPECT_EQ(consumer_->log, expected);
  EXPECT_EQ(outcome(), MoQReplayOutcome::Finished);
}

TEST_F(MoQJoinReplayTest, StalledReplayResetsAndReleasesItsCharges) {
  consumer_->readyWrites = 1;
  startReplay(makePlan({10, 10}));
  cache_.reset();
  EXPECT_GT(budget_->used(), 0);

  advance(5s);

  EXPECT_EQ(consumer_->resetCode, ResetStreamErrorCode::CANCELLED);
  EXPECT_EQ(outcome(), MoQReplayOutcome::Stalled);
  EXPECT_EQ(budget_->used(), 0);
}

TEST_F(MoQJoinReplayTest, AcceptedWritesRefreshTheStallDeadline) {
  consumer_->readyWrites = 1;
  startReplay(makePlan({10, 10, 10}));

  advance(4s);
  consumer_->makeReady();
  consumer_->readyWrites = 2;
  drive();
  advance(4s);

  // Eight seconds in total, but never five without an accepted write.
  EXPECT_FALSE(consumer_->resetCode.has_value());
}

TEST_F(MoQJoinReplayTest, ReadinessThatNeverReturnsAfterBlockedStalls) {
  consumer_->blockedWrites = 1;
  startReplay(makePlan({10, 10}));
  EXPECT_FALSE(consumer_->resetCode.has_value());

  advance(5s);

  const std::vector<std::string> firstOnly{"object 1/0 10"};
  EXPECT_EQ(consumer_->log, firstOnly);
  EXPECT_EQ(consumer_->resetCode, ResetStreamErrorCode::CANCELLED);
  EXPECT_EQ(outcome(), MoQReplayOutcome::Stalled);
}

// A wait that ends without a change, by deadline or cancellation, must not
// leave its wakeup registered on the cache.
TEST_F(MoQJoinReplayTest, EndedCoverageWaitsLeaveNoWaiter) {
  auto cache = std::make_shared<MoQGroupCache>(3, budget_);
  ASSERT_TRUE(cache->openSubgroup(1, 0, /*containsLastInGroup=*/false));
  ASSERT_EQ(
      cache->beginObject(1, 0, 0, 10, bytes(5), noExtensions()),
      MoQGroupCache::Result::Recorded);
  auto wait = [&](std::chrono::milliseconds maxWait) {
    return folly::coro::co_withExecutor(
               &evb_,
               folly::coro::co_withCancellation(
                   cancel_.getToken(),
                   awaitCacheCoverage(
                       cache, {1, 0}, {1, 1}, maxWait, timekeeper_.get())))
        .start();
  };

  auto timedOut = wait(100ms);
  drive();
  EXPECT_EQ(cache->waiterCount(), 1);
  advance(100ms);
  ASSERT_TRUE(timedOut.isReady());
  EXPECT_EQ(std::move(timedOut).get(), MoQGroupCache::Coverage::AwaitIngress);
  EXPECT_EQ(cache->waiterCount(), 0);

  auto cancelled = wait(5s);
  drive();
  EXPECT_EQ(cache->waiterCount(), 1);
  cancel_.requestCancellation();
  drive();
  ASSERT_TRUE(cancelled.isReady());
  EXPECT_EQ(cache->waiterCount(), 0);
}

TEST_F(MoQJoinReplayTest, CancellationStopsFurtherWrites) {
  consumer_->readyWrites = 1;
  startReplay(makePlan({10, 10}));

  cancel_.requestCancellation();
  drive();
  consumer_->makeReady();
  drive();

  const std::vector<std::string> firstOnly{"object 1/0 10"};
  EXPECT_EQ(consumer_->log, firstOnly);
  EXPECT_EQ(consumer_->resetCode, ResetStreamErrorCode::CANCELLED);
  EXPECT_EQ(outcome(), MoQReplayOutcome::Cancelled);
}

TEST_F(MoQJoinReplayTest, EndOfTrackFinishesTheStream) {
  cache_ = std::make_unique<MoQGroupCache>(3, budget_);
  ASSERT_TRUE(cache_->openSubgroup(1, 0, false));
  ASSERT_EQ(
      cache_->subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, bytes(5), noExtensions()),
      MoQGroupCache::Result::Recorded);
  ASSERT_EQ(
      cache_->subgroupObject(
          1, 0, 1, ObjectStatus::END_OF_TRACK, nullptr, noExtensions()),
      MoQGroupCache::Result::Recorded);
  auto plan = cache_->snapshot({1, 0}, {1, 2}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());

  startReplay(std::move(*plan));

  const std::vector<std::string> expected{"object 1/0 5", "endOfTrack 1/1"};
  EXPECT_EQ(consumer_->log, expected);
}

// Newest first, the END_OF_TRACK marker is in the first group written. It
// must not finish the stream before the older groups are replayed.
TEST_F(MoQJoinReplayTest, NewestFirstPlanReplaysOlderGroupsAfterEndOfTrack) {
  cache_ = std::make_unique<MoQGroupCache>(3, budget_);
  ASSERT_TRUE(cache_->openSubgroup(1, 0, /*containsLastInGroup=*/true));
  ASSERT_EQ(
      cache_->subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, bytes(5), noExtensions()),
      MoQGroupCache::Result::Recorded);
  ASSERT_EQ(
      cache_->endSubgroup(1, 0, /*cleanFin=*/true),
      MoQGroupCache::Result::Recorded);
  ASSERT_TRUE(cache_->openSubgroup(2, 0, /*containsLastInGroup=*/false));
  ASSERT_EQ(
      cache_->subgroupObject(
          2, 0, 0, ObjectStatus::NORMAL, bytes(6), noExtensions()),
      MoQGroupCache::Result::Recorded);
  ASSERT_EQ(
      cache_->subgroupObject(
          2, 0, 1, ObjectStatus::END_OF_TRACK, nullptr, noExtensions()),
      MoQGroupCache::Result::Recorded);
  auto plan = cache_->snapshot({1, 0}, {2, 2}, GroupOrder::NewestFirst);
  ASSERT_TRUE(plan.has_value());
  EXPECT_TRUE(plan->endOfTrack);

  startReplay(std::move(*plan));

  const std::vector<std::string> expected{
      "object 2/0 6", "object 1/0 5", "endOfFetch"};
  EXPECT_EQ(consumer_->log, expected);
  EXPECT_EQ(outcome(), MoQReplayOutcome::Finished);
}

} // namespace moxygen::test
