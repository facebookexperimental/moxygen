/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/experimental/coro/GtestHelpers.h>
#include <folly/io/async/EventBase.h>
#include <folly/portability/GMock.h>
#include <folly/portability/GTest.h>
#include <moxygen/relay/MoQCache.h>
#include <moxygen/test/Mocks.h>
#include <moxygen/test/TestUtils.h>

using namespace testing;
namespace moxygen::test {

const FullTrackName kTestTrackName{TrackNamespace{{"foo"}}, "bar"};

Fetch getFetch(AbsoluteLocation start, AbsoluteLocation end) {
  return Fetch{0, kTestTrackName, start, end};
}

class MoQCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Code to set up test environment, if needed
    ON_CALL(*trackConsumer_, datagram(_, _)).WillByDefault(Return(folly::unit));
    cache_.clear();
  }

  void TearDown() override {
    // Code to clean up test environment, if needed
  }

  std::shared_ptr<Publisher::FetchHandle> expectUpstreamFetch(
      AbsoluteLocation start,
      AbsoluteLocation end,
      bool endOfTrack,
      AbsoluteLocation latest) {
    auto handle = std::make_shared<moxygen::MockFetchHandle>(
        FetchOk{0, GroupOrder::OldestFirst, endOfTrack, latest, {}});
    EXPECT_CALL(*upstream_, fetch(_, _))
        .WillOnce([start, end, handle, this](auto fetch, auto consumer) {
          auto [standalone, joining] = fetchType(fetch);
          EXPECT_EQ(standalone->start, start);
          EXPECT_EQ(standalone->end, end);
          upstreamFetchConsumer_ = std::move(consumer);
          return folly::coro::makeTask<Publisher::FetchResult>(handle);
        })
        .RetiresOnSaturation();
    return handle;
  }

  void expectUpstreamFetch(const FetchError& err) {
    EXPECT_CALL(*upstream_, fetch(_, _))
        .WillOnce(Return(folly::coro::makeTask<Publisher::FetchResult>(
            folly::makeUnexpected(err))));
  }

  void populateCacheRange(
      AbsoluteLocation start,
      AbsoluteLocation end,
      uint64_t objectsPerGroup = 10,
      uint64_t objectIncrement = 1,
      uint64_t groupIncrement = 1,
      bool endOfGroup = false) {
    auto writeback =
        cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);
    while (start < end) {
      writeback->datagram(
          ObjectHeader(TrackAlias(0), start.group, 0, start.object, 0, 100),
          makeBuf(100));
      start.object += objectIncrement;
      if (start.object >= objectsPerGroup) {
        if (endOfGroup) {
          writeback->datagram(
              ObjectHeader(
                  TrackAlias(0),
                  start.group,
                  0,
                  start.object,
                  0,
                  ObjectStatus::END_OF_GROUP),
              nullptr);
        }
        start.group += groupIncrement;
        start.object = 0;
      }
    }
  }

  void serverCacheRangeFromUpstream(
      AbsoluteLocation start,
      AbsoluteLocation end,
      uint64_t objectsPerGroup = 10,
      uint64_t objectIncrement = 1,
      uint64_t groupIncrement = 1,
      bool endOfGroup = false) {
    while (start < end) {
      upstreamFetchConsumer_->object(
          start.group, 0, start.object, makeBuf(100));
      start.object += objectIncrement;
      if (start.object >= objectsPerGroup) {
        if (endOfGroup) {
          upstreamFetchConsumer_->endOfGroup(start.group, 0, start.object);
        }
        start.group += groupIncrement;
        start.object = 0;
      }
    }
    upstreamFetchConsumer_->endOfFetch();
  }

  void expectFetchObjects(
      AbsoluteLocation start,
      AbsoluteLocation end,
      bool endOfFetch,
      uint64_t objectsPerGroup = 10,
      uint64_t objectIncrement = 1,
      uint64_t groupIncrement = 1,
      bool endOfGroup = false) {
    testing::InSequence enforceOrder;
    while (start < end) {
      EXPECT_CALL(*consumer_, object(_, _, _, _, _, _))
          .WillOnce([start](auto group, auto, auto object, auto, auto, auto) {
            EXPECT_EQ(group, start.group);
            EXPECT_EQ(object, start.object);
            return folly::unit;
          })
          .RetiresOnSaturation();
      start.object += objectIncrement;
      if (start.object >= objectsPerGroup) {
        if (endOfGroup) {
          EXPECT_CALL(*consumer_, endOfGroup(_, _, _, _, _))
              .WillOnce([start](auto group, auto, auto object, auto, auto) {
                EXPECT_EQ(group, start.group);
                EXPECT_EQ(object, start.object);
                return folly::unit;
              })
              .RetiresOnSaturation();
        }
        start.group += groupIncrement;
        start.object = 0;
      }
    }
    if (endOfFetch) {
      EXPECT_CALL(*consumer_, endOfFetch()).WillOnce(Return(folly::unit));
    }
  }

  MoQCache cache_;
  std::shared_ptr<MockPublisher> upstream_{std::make_shared<MockPublisher>()};
  std::shared_ptr<moxygen::FetchConsumer> upstreamFetchConsumer_;
  std::shared_ptr<moxygen::MockFetchConsumer> consumer_{
      std::make_shared<moxygen::MockFetchConsumer>()};
  std::shared_ptr<NiceMock<moxygen::MockTrackConsumer>> trackConsumer_{
      std::make_shared<NiceMock<moxygen::MockTrackConsumer>>()};
};

CO_TEST_F(MoQCacheTest, TestFetchAllHit) {
  populateCacheRange({0, 0}, {0, 10});
  expectFetchObjects({0, 0}, {0, 10}, false);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 9}));
}
CO_TEST_F(MoQCacheTest, TestFetchAllHitEOG) {
  populateCacheRange({0, 0}, {0, 10}, 10, 1, 1, true);
  expectFetchObjects({0, 0}, {0, 10}, false, 10, 1, 1, true);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {1, 0}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 10}));
}

CO_TEST_F(MoQCacheTest, TestFetchMissUpstreamError) {
  // Test case for fetch with complete cache miss when no track is present
  expectUpstreamFetch(
      FetchError{0, FetchErrorCode::TRACK_NOT_EXIST, "not exist"});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasError());
}

CO_TEST_F(MoQCacheTest, TestFetchMissNoTrackUpstreamCompleteHit) {
  // Test case for fetch when no track is present, full range served by upstream
  expectUpstreamFetch({0, 0}, {0, 10}, 0, AbsoluteLocation{1, 0});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {0, 10}, true);
  serverCacheRangeFromUpstream({0, 0}, {0, 10});
}
CO_TEST_F(MoQCacheTest, TestFetchMissUpstreamCompleteHit) {
  // Test case for fetch when track is present, but no overlap
  populateCacheRange({0, 0}, {0, 1});
  expectUpstreamFetch({0, 1}, {0, 10}, 0, AbsoluteLocation{1, 0});
  auto res =
      co_await cache_.fetch(getFetch({0, 1}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 1}, {0, 10}, true);
  serverCacheRangeFromUpstream({0, 1}, {0, 10});
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginning) {
  populateCacheRange({0, 0}, {0, 5});
  expectFetchObjects({0, 0}, {0, 10}, true);
  expectUpstreamFetch({0, 5}, {0, 10}, 0, AbsoluteLocation{0, 9});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 9}));
  serverCacheRangeFromUpstream({0, 5}, {0, 10});
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitEnd) {
  populateCacheRange({0, 5}, {0, 10});
  expectFetchObjects({0, 0}, {0, 10}, false);
  expectUpstreamFetch({0, 0}, {0, 5}, 0, AbsoluteLocation{0, 9});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 9}));
  serverCacheRangeFromUpstream({0, 0}, {0, 5});
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningEnd) {
  populateCacheRange({0, 0}, {0, 2});
  populateCacheRange({0, 6}, {0, 8});
  populateCacheRange({0, 9}, {0, 10});
  expectFetchObjects({0, 0}, {0, 10}, false);
  expectUpstreamFetch({0, 2}, {0, 6}, 0, AbsoluteLocation{0, 9});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 9}));
  co_await folly::coro::co_reschedule_on_current_executor;
  expectUpstreamFetch({0, 8}, {0, 9}, 0, AbsoluteLocation{0, 9});
  serverCacheRangeFromUpstream({0, 2}, {0, 6});

  co_await folly::coro::co_reschedule_on_current_executor;
  serverCacheRangeFromUpstream({0, 8}, {0, 9});
}

CO_TEST_F(MoQCacheTest, TestFetchWriteback) {
  // Test case for fetch + writeback

  expectUpstreamFetch({0, 0}, {0, 10}, 0, AbsoluteLocation{1, 0});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {0, 10}, true);
  serverCacheRangeFromUpstream({0, 0}, {0, 10});

  co_await folly::coro::co_reschedule_on_current_executor;

  expectFetchObjects({0, 0}, {0, 10}, false);
  res = co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
}

CO_TEST_F(MoQCacheTest, TestFetchPopulatesNotExist) {
  // Test case for fetch populating OBJECT_NOT_EXIST, GROUP_NOT_EXIST
  expectUpstreamFetch({0, 0}, {0, 10}, 0, AbsoluteLocation{1, 0});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {0, 10}, true, 10, 2, 2);
  serverCacheRangeFromUpstream({0, 0}, {0, 10}, 10, 2, 2);

  co_await folly::coro::co_reschedule_on_current_executor;

  expectFetchObjects({0, 0}, {0, 10}, false, 10, 2, 2);
  res = co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  co_return;
}

CO_TEST_F(MoQCacheTest, TestNonLiveTrackFetchCompletelySatisfiedByCache) {
  // Test case for non-live track where fetch is completely satisfied by cache
  co_return;
}

CO_TEST_F(MoQCacheTest, TestPartialFetchHitsMissingBeginning) {
  // Test case for partial fetch hits with missing beginning
  co_return;
}

CO_TEST_F(MoQCacheTest, TestPartialFetchHitsMissingMiddleMultipleMisses) {
  // Test case for partial fetch hits with missing middle and multiple misses
  co_return;
}

CO_TEST_F(MoQCacheTest, TestPartialFetchHitsMissingEnd) {
  // Test case for partial fetch hits with missing end
  co_return;
}

CO_TEST_F(MoQCacheTest, TestFetchOnLiveTrack) {
  // Test case for fetch on a live track
  co_return;
}

CO_TEST_F(MoQCacheTest, TestFetchEndBeyondEndOfTrack) {
  // Test case for fetch end beyond the end of track
  co_return;
}

CO_TEST_F(MoQCacheTest, TestFetchWaitsForFetchInProgress) {
  // Test case for fetch waiting for a fetch in progress
  co_return;
}

CO_TEST_F(MoQCacheTest, TestUpstreamFetchError) {
  // Test case for upstream fetch error
  co_return;
}

CO_TEST_F(MoQCacheTest, TestUpstreamResetStream) {
  // Test case for upstream resetting the stream
  co_return;
}

CO_TEST_F(MoQCacheTest, TestConsumerObjectBlocked) {
  // Test case for consumer->object BLOCKED
  co_return;
}

CO_TEST_F(MoQCacheTest, TestAwaitFails) {
  // Test case for await fails
  co_return;
}

CO_TEST_F(MoQCacheTest, TestConsumerObjectFailsForAnotherReason) {
  // Test case for consumer->object fails for another reason (e.g., cancel)
  co_return;
}

} // namespace moxygen::test
