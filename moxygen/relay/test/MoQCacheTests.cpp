/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/coro/Collect.h>
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

  folly::SemiFuture<std::shared_ptr<FetchConsumer>> expectUpstreamFetch(
      AbsoluteLocation start,
      AbsoluteLocation end,
      bool endOfTrack,
      AbsoluteLocation latest) {
    auto [promise, future] =
        folly::makePromiseContract<std::shared_ptr<FetchConsumer>>();
    EXPECT_CALL(*upstream_, fetch(_, _))
        .WillOnce([start,
                   end,
                   endOfTrack,
                   latest,
                   promise = std::move(promise),
                   this](auto fetch, auto consumer) mutable {
          auto [standalone, joining] = fetchType(fetch);
          EXPECT_EQ(standalone->start, start);
          EXPECT_EQ(standalone->end, end);
          upstreamFetchConsumer_ = std::move(consumer);
          promise.setValue(upstreamFetchConsumer_);
          auto handle = std::make_shared<moxygen::MockFetchHandle>(
              FetchOk{0, GroupOrder::OldestFirst, endOfTrack, latest, {}});
          return folly::coro::makeTask<Publisher::FetchResult>(handle);
        })
        .RetiresOnSaturation();
    return std::move(future);
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
      if (start.object >= objectsPerGroup * objectIncrement) {
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
      } else {
        writeback->datagram(
            ObjectHeader(TrackAlias(0), start.group, 0, start.object, 0, 100),
            makeBuf(100));
        start.object += objectIncrement;
      }
    }
  }

  void serveCacheRangeFromUpstream(
      AbsoluteLocation start,
      AbsoluteLocation end,
      uint64_t objectsPerGroup = 10,
      uint64_t objectIncrement = 1,
      uint64_t groupIncrement = 1,
      bool endOfGroup = false) {
    while (start < end) {
      if (start.object >= objectsPerGroup * objectIncrement) {
        if (endOfGroup) {
          upstreamFetchConsumer_->endOfGroup(start.group, 0, start.object);
        }
        start.group += groupIncrement;
        start.object = 0;
      } else {
        upstreamFetchConsumer_->object(
            start.group, 0, start.object, makeBuf(100));
        start.object += objectIncrement;
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
      bool endOfGroup = false,
      std::shared_ptr<moxygen::MockFetchConsumer> consumerIn = nullptr) {
    auto consumer = consumerIn ? std::move(consumerIn) : consumer_;
    testing::InSequence enforceOrder;
    while (start < end) {
      if (start.object >= objectsPerGroup * objectIncrement) {
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
      } else {
        EXPECT_CALL(*consumer, object(_, _, _, _, _, _))
            .WillOnce([start](auto group, auto, auto object, auto, auto, auto) {
              EXPECT_EQ(group, start.group);
              EXPECT_EQ(object, start.object);
              return folly::unit;
            })
            .RetiresOnSaturation();
        start.object += objectIncrement;
      }
    }
    if (endOfFetch) {
      EXPECT_CALL(*consumer, endOfFetch()).WillOnce(Return(folly::unit));
    }
  }

  MoQCache cache_;
  std::shared_ptr<StrictMock<MockPublisher>> upstream_{
      std::make_shared<StrictMock<MockPublisher>>()};
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
  populateCacheRange({0, 0}, {0, 11}, 10, 1, 1, true);
  expectFetchObjects({0, 0}, {0, 11}, false, 10, 1, 1, true);
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
  serveCacheRangeFromUpstream({0, 0}, {0, 10});
}

CO_TEST_F(MoQCacheTest, TestFetchMissUpstreamCompleteHit) {
  // Test case for fetch when track is present, but no overlap
  populateCacheRange({0, 0}, {0, 1});
  expectUpstreamFetch({0, 1}, {0, 10}, 0, AbsoluteLocation{1, 0});
  auto res =
      co_await cache_.fetch(getFetch({0, 1}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 1}, {0, 10}, true);
  serveCacheRangeFromUpstream({0, 1}, {0, 10});
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
  serveCacheRangeFromUpstream({0, 5}, {0, 10});
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
  serveCacheRangeFromUpstream({0, 0}, {0, 5});
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
  serveCacheRangeFromUpstream({0, 2}, {0, 6});

  co_await folly::coro::co_reschedule_on_current_executor;
  serveCacheRangeFromUpstream({0, 8}, {0, 9});
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningWholeGroup) {
  populateCacheRange({0, 0}, {0, 10});
  expectFetchObjects({0, 0}, {0, 11}, true, 10, 1, 1, true);
  expectUpstreamFetch({0, 10}, {1, 0}, 0, AbsoluteLocation{0, 10});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {1, 0}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 10}));
  serveCacheRangeFromUpstream({0, 10}, {0, 11}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchWriteback) {
  // Test case for fetch + writeback

  expectUpstreamFetch({0, 0}, {0, 10}, 0, AbsoluteLocation{1, 0});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {0, 10}, true);
  serveCacheRangeFromUpstream({0, 0}, {0, 10});

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
  serveCacheRangeFromUpstream({0, 0}, {0, 10}, 10, 2, 2);

  co_await folly::coro::co_reschedule_on_current_executor;

  expectFetchObjects({0, 0}, {0, 10}, false, 10, 2, 2);
  res = co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  co_return;
}

CO_TEST_F(MoQCacheTest, TestFetchOnLiveTrackNoObjects) {
  // Test case for fetch when track is live with no objects
  auto writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);

  expectUpstreamFetch({0, 0}, {0, 10}, 0, AbsoluteLocation{1, 0});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {0, 10}, true);
  serveCacheRangeFromUpstream({0, 0}, {0, 10});
}

CO_TEST_F(MoQCacheTest, TestFetchOnLiveTrackPastLargest) {
  // Test case for fetch when track is live with objects
  // Fetch 0,0-10, largest seen object on live is 0,0
  // FETCH_OK largest = 0,0, receive object 0,0
  populateCacheRange({0, 0}, {0, 1});
  auto writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);

  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 0}));
  expectFetchObjects({0, 0}, {0, 1}, false);
}

CO_TEST_F(MoQCacheTest, TestFetchEndBeyondEndOfTrack) {
  // Test case for fetch end beyond the end of track
  populateCacheRange({0, 0}, {0, 5});
  auto writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);
  writeback->datagram(
      ObjectHeader(
          TrackAlias(0), 0, 0, 5, 0, ObjectStatus::END_OF_TRACK_AND_GROUP),
      nullptr);
  writeback.reset();
  expectFetchObjects({0, 0}, {0, 5}, false);
  EXPECT_CALL(*consumer_, endOfTrackAndGroup(0, 0, 5, _))
      .WillOnce(Return(folly::unit));

  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 5}));
}

CO_TEST_F(MoQCacheTest, TestFetchWaitsForFetchInProgress) {
  // Test case for fetch waiting for a fetch in progress
  expectUpstreamFetch({0, 0}, {0, 10}, 0, AbsoluteLocation{1, 0})
      .via(co_await folly::coro::co_current_executor)
      .thenTry([this](auto) { serveCacheRangeFromUpstream({0, 0}, {0, 10}); });
  auto consumer2{std::make_shared<moxygen::MockFetchConsumer>()};
  expectFetchObjects({0, 0}, {0, 10}, true);
  expectFetchObjects({0, 0}, {0, 10}, false, 10, 1, 1, false, consumer2);

  auto [res1, res2] = co_await folly::coro::collectAll(
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_),
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer2, upstream_));
  EXPECT_TRUE(res1.hasValue());
  EXPECT_TRUE(res2.hasValue());
}

CO_TEST_F(MoQCacheTest, TestFetchWaitsForFetchInProgressMultiple) {
  // Test case for fetch waiting for a fetch in progress
  populateCacheRange({0, 1}, {0, 2});
  {
    auto exec = co_await folly::coro::co_current_executor;
    InSequence enforceOrder;
    expectUpstreamFetch({0, 0}, {0, 1}, 0, AbsoluteLocation{1, 0})
        .via(exec)
        .thenTry([this](auto) { serveCacheRangeFromUpstream({0, 0}, {0, 1}); });
    expectUpstreamFetch({0, 2}, {0, 10}, 0, AbsoluteLocation{1, 0})
        .via(exec)
        .thenTry([this, exec](auto) {
          upstreamFetchConsumer_->object(0, 0, 2, makeBuf(100));
          exec->add([this] { serveCacheRangeFromUpstream({0, 3}, {0, 10}); });
        });
  }
  auto consumer2{std::make_shared<moxygen::MockFetchConsumer>()};
  expectFetchObjects({0, 0}, {0, 10}, true);
  expectFetchObjects({0, 0}, {0, 10}, false, 10, 1, 1, false, consumer2);

  auto [res1, res2] = co_await folly::coro::collectAll(
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_),
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer2, upstream_));
  EXPECT_TRUE(res1.hasValue());
  EXPECT_TRUE(res2.hasValue());
}

CO_TEST_F(MoQCacheTest, TestFetchWaitsForFetchInProgressError) {
  // Test case for fetch waiting for a fetch in progress.  The fetcher
  // receives a reset and the waiter gets a miss and goes upstream
  {
    InSequence enforceOrder;
    expectUpstreamFetch({0, 0}, {0, 10}, 0, AbsoluteLocation{1, 0})
        .via(co_await folly::coro::co_current_executor)
        .thenTry([this](auto) {
          upstreamFetchConsumer_->reset(ResetStreamErrorCode::INTERNAL_ERROR);
        });
    expectUpstreamFetch(
        FetchError{0, FetchErrorCode::INTERNAL_ERROR, "borked"});
  }
  auto consumer2{std::make_shared<StrictMock<moxygen::MockFetchConsumer>>()};
  EXPECT_CALL(*consumer_, reset(_));
  EXPECT_CALL(*consumer2, reset(_));
  auto [res1, res2] = co_await folly::coro::collectAll(
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_),
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer2, upstream_));
  EXPECT_TRUE(res1.hasValue());
  EXPECT_TRUE(res2.hasError());
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
  populateCacheRange({0, 0}, {0, 2});
  {
    InSequence enforceOrder;
    EXPECT_CALL(*consumer_, object(0, 0, 0, _, _, _))
        .WillOnce([](auto, auto, auto, auto, auto, auto) {
          return folly::makeUnexpected(
              MoQPublishError(MoQPublishError::BLOCKED));
        });
    EXPECT_CALL(*consumer_, awaitReadyToConsume())
        .WillOnce(Return(folly::makeSemiFuture(folly::unit)));
    expectFetchObjects({0, 1}, {0, 2}, false);
  }
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 2}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(
      res.value()->fetchOk().latestGroupAndObject, (AbsoluteLocation{0, 1}));
}

CO_TEST_F(MoQCacheTest, TestAwaitFails) {
  // Test case for consumer->object BLOCKED, but await fails.  This results
  // in FETCH_ERROR
  populateCacheRange({0, 0}, {0, 2});
  EXPECT_CALL(*consumer_, object(0, 0, 0, _, _, _))
      .WillOnce([](auto, auto, auto, auto, auto, auto) {
        return folly::makeUnexpected(MoQPublishError(MoQPublishError::BLOCKED));
      });
  EXPECT_CALL(*consumer_, awaitReadyToConsume())
      .WillOnce(Return(
          folly::makeUnexpected(MoQPublishError(MoQPublishError::CANCELLED))));
  EXPECT_CALL(*consumer_, reset(_));
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 2}), consumer_, upstream_);
  EXPECT_TRUE(res.hasError());
}

CO_TEST_F(MoQCacheTest, TestConsumerObjectFailsForAnotherReason) {
  // Test case for consumer->object fails for another reason (e.g., cancel)
  populateCacheRange({0, 0}, {0, 2});
  EXPECT_CALL(*consumer_, object(0, 0, 0, _, _, _))
      .WillOnce([](auto, auto, auto, auto, auto, auto) {
        return folly::makeUnexpected(
            MoQPublishError(MoQPublishError::CANCELLED));
      });
  EXPECT_CALL(*consumer_, reset(_));
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 2}), consumer_, upstream_);
  EXPECT_TRUE(res.hasError());
}

} // namespace moxygen::test
