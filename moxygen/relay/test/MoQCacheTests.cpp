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

Fetch getFetch(
    AbsoluteLocation start,
    AbsoluteLocation end,
    GroupOrder order = GroupOrder::Default) {
  return Fetch{0, kTestTrackName, start, end, kDefaultPriority, order};
}

class MoQCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Code to set up test environment, if needed
    ON_CALL(*trackConsumer_, setTrackAlias(_))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*trackConsumer_, datagram(_, _)).WillByDefault(Return(folly::unit));
    ON_CALL(*trackConsumer_, objectStream(_, _))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*trackConsumer_, groupNotExists(_, _, _, _))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*trackConsumer_, subscribeDone(_))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*trackConsumer_, beginSubgroup(_, _, _))
        .WillByDefault(Return(makeSubgroupConsumer()));
    cache_.clear();
  }

  std::shared_ptr<MockSubgroupConsumer> makeSubgroupConsumer() {
    auto subgroupConsumer = std::make_shared<NiceMock<MockSubgroupConsumer>>();
    ON_CALL(*subgroupConsumer, object(_, _, _, _))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*subgroupConsumer, objectNotExists(_, _, _))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*subgroupConsumer, endOfSubgroup())
        .WillByDefault(Return(folly::unit));
    ON_CALL(*subgroupConsumer, checkpoint()).WillByDefault(Return());
    ON_CALL(*subgroupConsumer, beginObject(_, _, _, _))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*subgroupConsumer, objectPayload(_, _))
        .WillByDefault(Return(ObjectPublishStatus{}));
    ON_CALL(*subgroupConsumer, endOfGroup(_, _))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*subgroupConsumer, endOfTrackAndGroup(_, _))
        .WillByDefault(Return(folly::unit));
    ON_CALL(*subgroupConsumer, reset(_)).WillByDefault(Return());
    return subgroupConsumer;
  }

  void TearDown() override {
    // Code to clean up test environment, if needed
  }

  folly::SemiFuture<std::shared_ptr<FetchConsumer>> expectUpstreamFetch(
      AbsoluteLocation start,
      AbsoluteLocation end,
      bool endOfTrack,
      AbsoluteLocation largest,
      GroupOrder order = GroupOrder::OldestFirst) {
    auto [p, future] =
        folly::makePromiseContract<std::shared_ptr<FetchConsumer>>();
    EXPECT_CALL(*upstream_, fetch(_, _))
        .WillOnce([start,
                   end,
                   endOfTrack,
                   largest,
                   order,
                   promise = std::move(p),
                   this](auto fetch, auto consumer) mutable {
          auto [standalone, joining] = fetchType(fetch);
          EXPECT_EQ(standalone->start, start);
          EXPECT_EQ(standalone->end, end);
          upstreamFetchConsumer_ = std::move(consumer);
          promise.setValue(upstreamFetchConsumer_);
          upstreamFetchHandle_ = std::make_shared<moxygen::MockFetchHandle>(
              FetchOk{0, order, endOfTrack, largest, {}});
          return folly::coro::makeTask<Publisher::FetchResult>(
              upstreamFetchHandle_);
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
                  start.group, 0, start.object, 0, ObjectStatus::END_OF_GROUP),
              nullptr);
        }
        start.group += groupIncrement;
        start.object = 0;
      } else {
        writeback->datagram(
            ObjectHeader(start.group, 0, start.object, 0, 100), makeBuf(100));
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
      bool endOfGroup = false,
      bool endOfFetch = true) {
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
    if (endOfFetch) {
      upstreamFetchConsumer_->endOfFetch();
    }
  }

  void serveCacheRangeFromUpstreamDescending(
      AbsoluteLocation end,
      AbsoluteLocation start,
      uint64_t objectsPerGroup = 10,
      uint64_t objectIncrement = 1,
      uint64_t groupDecrement = 1,
      bool endOfGroup = false,
      bool endOfFetch = true) {
    // For descending groups, we start from the highest group (start.group)
    // and work down to the lowest group (end.group)
    AbsoluteLocation current = start;
    current.object = 0; // Always start from object 0 in each group

    while (current.group >= end.group) {
      // Check if we've served all objects in the current group
      if (current.object >= objectsPerGroup * objectIncrement ||
          current == start) {
        // Send endOfGroup if requested (but not for the first group)
        if (endOfGroup && current.group != start.group) {
          upstreamFetchConsumer_->endOfGroup(current.group, 0, current.object);
        }

        // Check if we're at the final group before decrementing
        if (current.group == end.group) {
          // Exit after processing all objects in the final group
          break;
        }

        // Move to the next lower group
        current.group -= groupDecrement;
        if (current.group == end.group) {
          current.object = end.object;
        } else {
          current.object = 0; // Reset to object 0 for the new group
        }
      } else {
        // Serve the current object
        upstreamFetchConsumer_->object(
            current.group, 0, current.object, makeBuf(100));
        current.object += objectIncrement;
      }
    }

    // Send endOfFetch if requested
    if (endOfFetch) {
      upstreamFetchConsumer_->endOfFetch();
    }
  }

  void expectFetchObjectsDescending(
      AbsoluteLocation end,
      AbsoluteLocation start,
      bool endOfFetch,
      uint64_t objectsPerGroup = 10,
      uint64_t objectIncrement = 1,
      int64_t groupDecrement = 1,
      bool endOfGroup = false,
      std::shared_ptr<moxygen::MockFetchConsumer> consumerIn = nullptr) {
    auto consumer = consumerIn ? std::move(consumerIn) : consumer_;
    testing::InSequence enforceOrder;

    // For descending groups, process each group from start.group down to
    // end.group Within each group, always start from object 0 and go ascending
    AbsoluteLocation current = start;
    current.object = 0; // Always start from object 0 in each group

    while (current.group >= end.group) {
      if (current.object >= objectsPerGroup * objectIncrement) {
        if (endOfGroup && current.group != start.group) {
          EXPECT_CALL(*consumer_, endOfGroup(_, _, _, _, _))
              .WillOnce(
                  [current](auto group, auto, auto object, const auto&, auto) {
                    EXPECT_EQ(group, current.group);
                    EXPECT_EQ(object, current.object);
                    return folly::unit;
                  })
              .RetiresOnSaturation();
        }

        // Check if we're at the final group before decrementing
        if (current.group == end.group) {
          // Exit after processing all objects in the final group
          break;
        }
        current.group -= groupDecrement;
        if (current.group == end.group) {
          current.object = end.object;
        } else {
          current.object = 0; // Reset to object 0 for the new group
        }
      } else {
        if (current < start) {
          EXPECT_CALL(*consumer, object(_, _, _, _, _, _))
              .WillOnce(
                  [current](
                      auto group, auto, auto object, auto, const auto&, auto) {
                    EXPECT_EQ(group, current.group);
                    EXPECT_EQ(object, current.object);
                    return folly::unit;
                  })
              .RetiresOnSaturation();
          current.object += objectIncrement;
        } else {
          current.object = 0;
          current.group -= groupDecrement;
        }
      }
    }
    if (endOfFetch) {
      EXPECT_CALL(*consumer, endOfFetch()).WillOnce(Return(folly::unit));
    }
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
  std::shared_ptr<moxygen::MockFetchHandle> upstreamFetchHandle_;
  std::shared_ptr<moxygen::FetchConsumer> upstreamFetchConsumer_;
  std::shared_ptr<moxygen::MockFetchConsumer> consumer_{
      std::make_shared<StrictMock<moxygen::MockFetchConsumer>>()};
  std::shared_ptr<NiceMock<moxygen::MockTrackConsumer>> trackConsumer_{
      std::make_shared<NiceMock<moxygen::MockTrackConsumer>>()};
};

CO_TEST_F(MoQCacheTest, TestFetchAllHit) {
  populateCacheRange({0, 0}, {0, 10});
  expectFetchObjects({0, 0}, {0, 10}, false);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 10}));
}
CO_TEST_F(MoQCacheTest, TestFetchAllHitEOG) {
  populateCacheRange({0, 0}, {0, 11}, 10, 1, 1, true);
  expectFetchObjects({0, 0}, {0, 11}, false, 10, 1, 1, true);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 0}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 0}));
}

CO_TEST_F(MoQCacheTest, TestFetchMissUpstreamError) {
  // Test case for fetch with complete cache miss when no track is present
  expectUpstreamFetch(
      FetchError{0, FetchErrorCode::TRACK_NOT_EXIST, "not exist"});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasError());
}

CO_TEST_F(MoQCacheTest, TestFetchMissTailUpstreamError) {
  // Test case for fetch with complete cache miss when no track is present
  populateCacheRange({0, 0}, {0, 1});
  expectUpstreamFetch(
      FetchError{0, FetchErrorCode::TRACK_NOT_EXIST, "not exist"});
  expectFetchObjects({0, 0}, {0, 1}, false);
  EXPECT_CALL(*consumer_, reset(_));
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
  expectUpstreamFetch({0, 5}, {0, 10}, 0, AbsoluteLocation{0, 10});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 10}));
  serveCacheRangeFromUpstream({0, 5}, {0, 10});
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitEnd) {
  populateCacheRange({0, 5}, {0, 10});
  expectFetchObjects({0, 0}, {0, 10}, false);
  expectUpstreamFetch({0, 0}, {0, 5}, 0, AbsoluteLocation{0, 9});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 10}));
  co_await folly::coro::co_reschedule_on_current_executor;
  serveCacheRangeFromUpstream({0, 0}, {0, 5});
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningEnd) {
  populateCacheRange({0, 0}, {0, 2});
  populateCacheRange({0, 6}, {0, 8});
  populateCacheRange({0, 9}, {0, 10});
  expectFetchObjects({0, 0}, {0, 10}, false);
  expectUpstreamFetch({0, 2}, {0, 6}, 0, AbsoluteLocation{0, 6});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 10}));
  co_await folly::coro::co_reschedule_on_current_executor;
  expectUpstreamFetch({0, 8}, {0, 9}, 0, AbsoluteLocation{0, 9});
  serveCacheRangeFromUpstream({0, 2}, {0, 6});

  co_await folly::coro::co_reschedule_on_current_executor;
  serveCacheRangeFromUpstream({0, 8}, {0, 9});
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningWholeGroup) {
  populateCacheRange({0, 0}, {0, 10});
  expectFetchObjects({0, 0}, {0, 11}, true, 10, 1, 1, true);
  expectUpstreamFetch({0, 10}, {0, 0}, 0, AbsoluteLocation{0, 10});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 0}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 10}));
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
  expectUpstreamFetch({0, 0}, {2, 10}, 0, AbsoluteLocation{1, 0});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {2, 10}, true, 10, 2, 2);
  serveCacheRangeFromUpstream({0, 0}, {2, 10}, 10, 2, 2);

  co_await folly::coro::co_reschedule_on_current_executor;

  expectFetchObjects({0, 0}, {2, 10}, true, 10, 2, 2);
  res = co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
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
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 1}));
  expectFetchObjects({0, 0}, {0, 1}, false);
}

CO_TEST_F(MoQCacheTest, TestFetchEndBeyondEndOfTrack) {
  // Test case for fetch end beyond the end of track
  populateCacheRange({0, 0}, {0, 5});
  auto writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);
  writeback->datagram(
      ObjectHeader(0, 0, 5, 0, ObjectStatus::END_OF_TRACK), nullptr);
  writeback.reset();
  expectFetchObjects({0, 0}, {0, 5}, false);
  EXPECT_CALL(*consumer_, endOfTrackAndGroup(0, 0, 5, _))
      .WillOnce(Return(folly::unit));

  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_TRUE(res.value()->fetchOk().endOfTrack);
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 5}));
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
  expectFetchObjects({0, 0}, {0, 10}, false);
  expectFetchObjects({0, 0}, {0, 10}, true, 10, 1, 1, false, consumer2);

  auto [res1, res2] = co_await folly::coro::collectAll(
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_),
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer2, upstream_));
  EXPECT_TRUE(res1.hasValue());
  EXPECT_TRUE(res2.hasValue());
  //
}

CO_TEST_F(MoQCacheTest, TestFetchWaitsForFetchInProgressMiddle) {
  // Test case for fetch waiting for a fetch in progress in the middle
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch({0, 0}, {0, 10}, 0, AbsoluteLocation{1, 0})
      .via(co_await folly::coro::co_current_executor)
      .thenTry([this, exec](auto) {
        serveCacheRangeFromUpstream({0, 0}, {0, 6}, 10, 1, 1, false, false);
        exec->add([this] { serveCacheRangeFromUpstream({0, 6}, {0, 10}); });
      });
  auto consumer2{std::make_shared<moxygen::MockFetchConsumer>()};
  expectFetchObjects({0, 0}, {0, 10}, true);
  expectFetchObjects({0, 5}, {0, 6}, false, 10, 1, 1, false, consumer2);

  auto [res1, res2] = co_await folly::coro::collectAll(
      cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_),
      cache_.fetch(getFetch({0, 5}, {0, 6}), consumer2, upstream_));
  EXPECT_TRUE(res1.hasValue());
  EXPECT_TRUE(res2.hasValue());
}

CO_TEST_F(MoQCacheTest, TestFetchWaitsForFetchInProgressError) {
  // Test case for fetch waiting for a fetch in progress.  The fetcher
  // receives a reset and the waiter gets a miss and goes upstream.
  // FETCH_OK is sent immediately.
  populateCacheRange({0, 9}, {0, 10});
  {
    InSequence enforceOrder;
    expectUpstreamFetch({0, 0}, {0, 9}, 0, AbsoluteLocation{0, 9})
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
  EXPECT_TRUE(res2.hasValue());
}

CO_TEST_F(MoQCacheTest, TestFetchWaitsForFetchInProgressErrorNeedsFetchOK) {
  // Test case for fetch waiting for a fetch in progress.  The fetcher
  // receives a reset and the waiter gets a miss and goes upstream
  // FETCH_OK has not been sent
  populateCacheRange({0, 1}, {0, 2});
  {
    InSequence enforceOrder;
    expectUpstreamFetch({0, 0}, {0, 1}, 0, AbsoluteLocation{1, 0})
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
  EXPECT_TRUE(res1.hasError());
  EXPECT_TRUE(res2.hasError());
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
        .WillOnce(Return(uint64_t(0)));
    expectFetchObjects({0, 1}, {0, 2}, false);
  }
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 2}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 2}));
}

CO_TEST_F(MoQCacheTest, TestAwaitFails) {
  // Test case for consumer->object BLOCKED, but await fails.  This results
  // in FETCH_ERROR
  populateCacheRange({0, 0}, {0, 1});
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
  populateCacheRange({0, 0}, {0, 1});
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

CO_TEST_F(MoQCacheTest, TestFetchCancel) {
  // Test case for invoking fetchCancel on FetchHandle
  populateCacheRange({0, 0}, {0, 1});
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch({0, 1}, {0, 10}, 0, AbsoluteLocation{1, 0})
      .via(exec)
      .thenTry([this, exec](auto) {
        // Simulate a delay before serving the range
        EXPECT_CALL(*upstreamFetchHandle_, fetchCancel()).WillOnce([this] {
          upstreamFetchConsumer_->reset(ResetStreamErrorCode::CANCELLED);
          upstreamFetchConsumer_.reset();
        });
        exec->add([this] {
          EXPECT_EQ(upstreamFetchConsumer_, nullptr);
          // serveCacheRangeFromUpstream({0, 1}, {0, 2}, 10, 1, 1, false,
          // false);
        });
      });

  expectFetchObjects({0, 0}, {0, 1}, false);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 10}), consumer_, upstream_);

  EXPECT_CALL(*consumer_, reset(_));
  co_await folly::coro::co_reschedule_on_current_executor;
  if (res.hasValue()) {
    auto fetchHandle = res.value();
    fetchHandle->fetchCancel(); // Invoke fetchCancel on the FetchHandle
  }

  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{1, 0}));
}
CO_TEST_F(MoQCacheTest, TestFetchPopulatesNotExistObjectsAndGroups) {
  // Test case for fetch populating OBJECT_NOT_EXIST and GROUP_NOT_EXIST
  auto writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);

  writeback->objectStream(
      ObjectHeader(0, 0, 0, 0, ObjectStatus::OBJECT_NOT_EXIST), nullptr);
  writeback->objectStream(
      ObjectHeader(0, 0, 1, 0, ObjectStatus::END_OF_GROUP), nullptr);

  // Simulate GROUP_NOT_EXIST using groupNotExist method
  writeback->groupNotExists(1, 0, 0);
  writeback.reset();

  EXPECT_CALL(*consumer_, endOfGroup(0, 0, 1, _, false))
      .WillOnce(Return(folly::unit));
  EXPECT_CALL(*consumer_, endOfFetch()).WillOnce(Return(folly::unit));
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {1, 0}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{1, 0}));
}

TEST_F(MoQCacheTest, TestInvalidCacheUpdateFails) {
  // Populate the cache with OBJECT_NOT_EXIST for objects 0,0 1,0 2,0
  auto writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);
  for (int i = 0; i < 5; ++i) {
    writeback->objectStream(
        ObjectHeader(i, 0, 0, 0, ObjectStatus::OBJECT_NOT_EXIST), nullptr);
  }
  writeback->objectStream(ObjectHeader(5, 0, 0, 0, 10), makeBuf(10));

  writeback->objectStream(
      ObjectHeader(6, 0, 0, 0, ObjectStatus::END_OF_TRACK), nullptr);

  writeback.reset();

  // Attempt to overwrite the objects with normal objects using datagram
  writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);

  // Attempt to overwrite missing objects with normal objects using objectStream
  auto result =
      writeback->objectStream(ObjectHeader(0, 0, 0, 0, 100), makeBuf(100));
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);

  result = writeback->datagram(ObjectHeader(1, 0, 0, 0, 100), makeBuf(100));
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);

  result = writeback->beginSubgroup(2, 0, 0).value()->endOfGroup(0);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);

  result = writeback->beginSubgroup(3, 0, 0).value()->beginObject(
      0, 100, makeBuf(100));
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);

  result = writeback->beginSubgroup(4, 0, 0).value()->endOfTrackAndGroup(0);
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);

  // Payload size changed
  result = writeback->objectStream(ObjectHeader(5, 0, 0, 0, 20), makeBuf(20));
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);

  // Beyond End of track
  result = writeback->objectStream(ObjectHeader(7, 0, 0, 0, 20), makeBuf(20));
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);

  // End of track not largest
  result = writeback->objectStream(
      ObjectHeader(5, 0, 1, 0, ObjectStatus::END_OF_TRACK), makeBuf(20));
  EXPECT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::API_ERROR);

  // Test the rest of the writeback APIs while we're here
  result = writeback->beginSubgroup(5, 0, 0).value()->endOfSubgroup();
  EXPECT_FALSE(result.hasError());

  writeback->beginSubgroup(6, 0, 0).value()->checkpoint();
  writeback->beginSubgroup(7, 0, 0).value()->reset(
      ResetStreamErrorCode::CANCELLED);
  writeback->subscribeDone(
      {RequestID(0), SubscribeDoneStatusCode::SUBSCRIPTION_ENDED, 0, ""});

  writeback.reset();
}

CO_TEST_F(MoQCacheTest, TestUpstreamFetchPartialWriteAndReset) {
  // Initiate an upstream fetch for one object
  expectUpstreamFetch({0, 0}, {0, 1}, 0, AbsoluteLocation{0, 0})
      .via(co_await folly::coro::co_current_executor)
      .thenTry([this](auto) {
        // Partially write the object using beginObject
        upstreamFetchConsumer_->beginObject(0, 0, 0, 100, makeBuf(50));
        // Followed by a reset
        upstreamFetchConsumer_->reset(ResetStreamErrorCode::CANCELLED);
      });

  // Expect the consumer to reset
  EXPECT_CALL(*consumer_, beginObject(0, 0, 0, 100, _, _))
      .WillOnce(Return(folly::unit));
  EXPECT_CALL(*consumer_, reset(_));

  // Perform the fetch
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 1}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());

  co_await folly::coro::co_reschedule_on_current_executor;

  // A subsequent fetch for the same object goes upstream and succeeds
  expectUpstreamFetch({0, 0}, {0, 1}, 0, AbsoluteLocation{0, 1});
  expectFetchObjects({0, 0}, {0, 1}, true);

  res = co_await cache_.fetch(getFetch({0, 0}, {0, 1}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 1}));

  co_await folly::coro::co_reschedule_on_current_executor;
  serveCacheRangeFromUpstream({0, 0}, {0, 1});
}

CO_TEST_F(MoQCacheTest, TestUpstreamServesObjectNotExist) {
  // Test case for upstream serving OBJECT_NOT_EXIST
  populateCacheRange({0, 0}, {0, 1});

  // Expect upstream fetch to be called with the specified range
  expectUpstreamFetch({0, 1}, {0, 2}, 0, AbsoluteLocation{0, 1})
      .via(co_await folly::coro::co_current_executor)
      .thenTry([this](auto) {
        // Serve OBJECT_NOT_EXIST for the first object
        upstreamFetchConsumer_->objectNotExists(0, 0, 1, noExtensions(), true);
      });

  EXPECT_CALL(*consumer_, endOfFetch()).WillOnce(Return(folly::unit));
  // Perform the fetch
  auto res =
      co_await cache_.fetch(getFetch({0, 1}, {0, 2}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 1}));
}

CO_TEST_F(MoQCacheTest, TestUpstreamServesGroupNotExist) {
  // Test case for upstream serving GROUP_NOT_EXIST
  populateCacheRange({0, 0}, {0, 1});
  // Expect upstream fetch to be called with the specified range
  expectUpstreamFetch({1, 0}, {1, 1}, 0, AbsoluteLocation{1, 0})
      .via(co_await folly::coro::co_current_executor)
      .thenTry([this](auto) {
        // Serve GROUP_NOT_EXIST for the second group
        upstreamFetchConsumer_->groupNotExists(1, 0, noExtensions(), true);
      });

  EXPECT_CALL(*consumer_, endOfFetch()).WillOnce(Return(folly::unit));
  // Perform the fetch
  auto res =
      co_await cache_.fetch(getFetch({1, 0}, {1, 1}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{1, 0}));
}

CO_TEST_F(MoQCacheTest, TestUpstreamServesEndOfTrackAndGroup) {
  // Test case for upstream serving END_OF_TRACK

  // Expect upstream fetch to be called with the specified range
  expectUpstreamFetch({0, 0}, {2, 1}, 0, AbsoluteLocation{2, 0})
      .via(co_await folly::coro::co_current_executor)
      .thenTry([this](auto) {
        upstreamFetchConsumer_->objectNotExists(0, 0, 0);
        upstreamFetchConsumer_->checkpoint();
        upstreamFetchConsumer_->groupNotExists(1, 0);
        upstreamFetchConsumer_->endOfTrackAndGroup(2, 0, 0);
      });

  // Expect the consumer to handle the served statuses
  EXPECT_CALL(*consumer_, checkpoint());
  EXPECT_CALL(*consumer_, endOfTrackAndGroup(2, 0, 0, _))
      .WillOnce(Return(folly::unit));
  // Perform the fetch
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 1}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 0}));
}

CO_TEST_F(MoQCacheTest, TestPopulateObjectUsingBeginObjectAndObjectPayload) {
  // Test case for populating an object using beginObject and objectPayload

  // Create a writeback for the test track
  auto writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);

  // Begin an object with a specific size
  auto subgroupConsumer = writeback->beginSubgroup(0, 0, 0).value();
  subgroupConsumer->beginObject(0, 100, makeBuf(50));

  // Provide the payload for the object
  auto status = subgroupConsumer->objectPayload(makeBuf(50), false);
  EXPECT_FALSE(status.hasError());

  // End the object and the subgroup
  subgroupConsumer->endOfTrackAndGroup(0);
  writeback.reset();

  // Verify that the object was populated correctly
  expectFetchObjects({0, 0}, {0, 1}, false);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 1}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 1}));
}

CO_TEST_F(MoQCacheTest, TestUpstreamFetchUsingBeginObjectAndObjectPayload) {
  // Test case for upstream fetch using beginObject and objectPayload

  // Expect upstream fetch to be called with the specified range
  expectUpstreamFetch({0, 0}, {0, 1}, 0, AbsoluteLocation{0, 0})
      .via(co_await folly::coro::co_current_executor)
      .thenTry([this](auto) {
        // Begin an object with a specific size
        upstreamFetchConsumer_->beginObject(0, 0, 0, 100, makeBuf(50));
        auto status = upstreamFetchConsumer_->objectPayload(makeBuf(50), false);
        EXPECT_FALSE(status.hasError());
        upstreamFetchConsumer_->beginObject(0, 0, 1, 100, nullptr);
        status = upstreamFetchConsumer_->objectPayload(makeBuf(100), true);
        EXPECT_FALSE(status.hasError());
      });

  // Expect the consumer to handle the served object
  EXPECT_CALL(*consumer_, beginObject(0, 0, 0, 100, _, _))
      .WillOnce(Return(folly::unit));
  EXPECT_CALL(*consumer_, objectPayload(_, false))
      .WillOnce(Return(ObjectPublishStatus::DONE));
  EXPECT_CALL(*consumer_, beginObject(0, 0, 1, 100, _, _))
      .WillOnce(Return(folly::unit));
  EXPECT_CALL(*consumer_, objectPayload(_, true))
      .WillOnce(Return(ObjectPublishStatus::DONE));
  // Perform the fetch
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 1}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 0}));
}

CO_TEST_F(MoQCacheTest, TestPopulateCacheWithBeginSubgroupAndFetch) {
  // Create a writeback for the test track
  auto writeback = cache_.getSubscribeWriteback(kTestTrackName, trackConsumer_);

  // Begin a subgroup and populate it with objects
  auto subgroupConsumer = writeback->beginSubgroup(0, 0, 0).value();
  subgroupConsumer->object(0, makeBuf(100));
  subgroupConsumer->objectNotExists(1);
  subgroupConsumer->endOfSubgroup();
  writeback.reset();

  // Verify that the objects were populated correctly
  expectFetchObjects({0, 0}, {0, 1}, true);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 2}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 2}));
}

CO_TEST_F(MoQCacheTest, TestUpstreamReturnsNoObjectsError) {
  // Insert one object in the cache
  populateCacheRange({0, 0}, {0, 1});

  // Expect upstream fetch to return FetchError::NO_OBJECTS
  expectUpstreamFetch(FetchError{0, FetchErrorCode::NO_OBJECTS, "no objects"});

  // Perform the fetch
  auto res =
      co_await cache_.fetch(getFetch({0, 1}, {0, 2}), consumer_, upstream_);
  EXPECT_TRUE(res.hasError());
  EXPECT_EQ(res.error().errorCode, FetchErrorCode::NO_OBJECTS);
}

CO_TEST_F(MoQCacheTest, TestUpstreamReturnsNoObjectsTail) {
  // Insert one object in the cache
  populateCacheRange({0, 0}, {0, 1});

  // Expect upstream fetch to return FetchError::NO_OBJECTS
  expectUpstreamFetch(FetchError{0, FetchErrorCode::NO_OBJECTS, "no objects"});

  expectFetchObjects({0, 0}, {0, 1}, true);
  // Perform the fetch
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 2}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 2}));
}

CO_TEST_F(MoQCacheTest, TestFetchWithCacheGapAndUpstreamNoObjects) {
  // Populate the cache with a gap {0,0}, {0,2}
  populateCacheRange({0, 0}, {0, 3}, 10, 2);

  // Expect upstream fetch to return FetchError::NO_OBJECTS for the gap
  expectUpstreamFetch(FetchError{0, FetchErrorCode::NO_OBJECTS, "no objects"});

  // Expect the consumer to receive the objects from the cache
  expectFetchObjects({0, 0}, {0, 3}, false, 10, 2);

  // Perform the fetch
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 3}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 3}));
}

// Unit tests for cache hits, cache miss, and partial hits/misses spanning
// groups

CO_TEST_F(MoQCacheTest, TestFetchAllHitAcrossGroups) {
  // Populate two groups fully in cache: group 0 [0,0-0,10), group 1 [1,0-1,10)
  populateCacheRange({0, 0}, {2, 10}, 10, 1, 1, true);
  // Expect all objects to be served from cache, no upstream
  expectFetchObjects({0, 0}, {2, 10}, false, 10, 1, 1, true);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 10}));
}

CO_TEST_F(MoQCacheTest, TestFetchAllMissAcrossGroups) {
  // Test case for fetch when track is present, but no overlap
  // populateCacheRange({0, 0}, {0, 1});
  expectUpstreamFetch({0, 0}, {2, 10}, 0, AbsoluteLocation{2, 10});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {2, 10}, true, 10, 1, 1, true);
  serveCacheRangeFromUpstream({0, 0}, {2, 10}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningAcrossGroups) {
  populateCacheRange({0, 0}, {1, 5}, 10, 1, 1, true);
  expectUpstreamFetch({1, 5}, {2, 6}, 0, AbsoluteLocation{2, 6});
  expectFetchObjects({0, 0}, {1, 5}, false, 10, 1, 1, true);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 6}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 6}));
  expectFetchObjects({1, 5}, {2, 6}, true, 10, 1, 1, true);
  serveCacheRangeFromUpstream({1, 5}, {2, 6}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitEndAcrossGroups) {
  populateCacheRange({1, 5}, {2, 10}, 10, 1, 1, true);
  expectFetchObjects({0, 0}, {2, 10}, false, 10, 1, 1, true);
  expectUpstreamFetch({0, 0}, {1, 5}, 0, AbsoluteLocation{2, 9});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 10}));
  co_await folly::coro::co_reschedule_on_current_executor;
  serveCacheRangeFromUpstream({0, 0}, {1, 5}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitMiddleAcrossGroups) {
  // Cache: group 0 [0,0-0,10), group 2 [2,0-2,5)
  populateCacheRange({0, 0}, {0, 8});
  populateCacheRange({2, 2}, {2, 5});
  // Upstream needed for [0,10-2,0)
  expectFetchObjects({0, 0}, {2, 5}, false, 10, 1, 1, true);
  expectUpstreamFetch({0, 8}, {2, 2}, 0, AbsoluteLocation{2, 2});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 5}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 5}));
  co_await folly::coro::co_reschedule_on_current_executor;
  serveCacheRangeFromUpstream({0, 8}, {2, 2}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchMissSingleGroupBoundary) {
  // Cache: group 0 [0,0-0,5)
  populateCacheRange({0, 0}, {0, 5});
  // Upstream needed for [0,5-1,3)
  expectFetchObjects({0, 0}, {1, 3}, true);
  expectUpstreamFetch({0, 5}, {1, 3}, 0, AbsoluteLocation{1, 3});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {1, 3}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{1, 3}));
  serveCacheRangeFromUpstream({0, 5}, {1, 3});
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningEndAcrossGroups) {
  // Cache: group 0 [0,0-0,2), group 1 [1,6-1,8), group 2 [2,9-2,10)
  populateCacheRange({0, 0}, {0, 2});
  populateCacheRange({1, 6}, {1, 8});
  populateCacheRange({2, 9}, {2, 10});
  expectFetchObjects({0, 0}, {2, 10}, false, 10, 1, 1, true);
  expectUpstreamFetch({0, 2}, {1, 6}, 0, AbsoluteLocation{1, 6});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 10}));
  co_await folly::coro::co_reschedule_on_current_executor;
  expectUpstreamFetch({1, 8}, {2, 9}, 0, AbsoluteLocation{2, 9});
  serveCacheRangeFromUpstream({0, 2}, {1, 6}, 10, 1, 1, true);

  co_await folly::coro::co_reschedule_on_current_executor;
  serveCacheRangeFromUpstream({1, 8}, {2, 9}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningWholeGroupAcrossGroups) {
  // Cache: group 0 [0,0-0,10), group 1 [1,0-1,10)
  populateCacheRange({0, 0}, {1, 10}, 10, 1, 1, true);
  expectFetchObjects({0, 0}, {2, 0}, true, 10, 1, 1, true);
  expectUpstreamFetch({1, 10}, {2, 0}, 0, AbsoluteLocation{1, 10});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 0}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{1, 10}));
  serveCacheRangeFromUpstream({1, 10}, {2, 0}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchMissNoTrackUpstreamCompleteHitAcrossGroups) {
  // Test case for fetch when no track is present, full range served by upstream
  // across groups
  expectUpstreamFetch({0, 0}, {2, 10}, 0, AbsoluteLocation{2, 10});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {2, 10}, true, 10, 1, 1, true);
  serveCacheRangeFromUpstream({0, 0}, {2, 10}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchAllHitEOGAcrossGroups) {
  // Test case for fetch all hit with end of group across multiple groups
  populateCacheRange({0, 0}, {2, 11}, 10, 1, 1, true);
  expectFetchObjects({0, 0}, {2, 11}, false, 10, 1, 1, true);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 0}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 0}));
}

CO_TEST_F(MoQCacheTest, TestFetchWritebackAcrossGroups) {
  // Test case for fetch + writeback across groups
  expectUpstreamFetch({0, 0}, {2, 10}, 0, AbsoluteLocation{2, 10});
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjects({0, 0}, {2, 10}, true, 10, 1, 1, true);
  serveCacheRangeFromUpstream({0, 0}, {2, 10}, 10, 1, 1, true);

  co_await folly::coro::co_reschedule_on_current_executor;

  expectFetchObjects({0, 0}, {2, 10}, false, 10, 1, 1, true);
  res = co_await cache_.fetch(getFetch({0, 0}, {2, 10}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
}

CO_TEST_F(MoQCacheTest, TestFetchRangeExactlyAtGroupBoundary) {
  // Test fetch ranges ending exactly at group boundaries like [0,0-1,0)
  populateCacheRange({0, 0}, {2, 0}, 10, 1, 1, true);

  expectFetchObjects({0, 0}, {2, 0}, false, 10, 1, 1, true);
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {1, 0}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{1, 0}));
}

CO_TEST_F(MoQCacheTest, TestFetchAllMissAcrossGroupsDesc) {
  expectUpstreamFetch(
      {0, 0}, {2, 10}, 0, AbsoluteLocation{2, 10}, GroupOrder::NewestFirst);
  auto res = co_await cache_.fetch(
      getFetch({0, 0}, {2, 10}, GroupOrder::NewestFirst), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  expectFetchObjectsDescending({0, 0}, {2, 10}, true, 10, 1, 1, true);
  serveCacheRangeFromUpstreamDescending({0, 0}, {2, 10}, 10, 1, 1, true);
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningAcrossGroupsDesc) {
  populateCacheRange({0, 0}, {1, 5}, 10, 1, 1, true);
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch(
      {2, 0}, {2, 6}, 0, AbsoluteLocation{2, 6}, GroupOrder::NewestFirst)
      .via(exec)
      .thenTry([this, exec](const auto&) {
        expectFetchObjectsDescending({1, 0}, {2, 6}, false, 10, 1, 1, true);
        serveCacheRangeFromUpstream({2, 0}, {2, 6}, 10, 1, 1, false);

        expectUpstreamFetch(
            {1, 5}, {1, 0}, 0, AbsoluteLocation{1, 0}, GroupOrder::NewestFirst)
            .via(exec)
            .thenTry([this](const auto&) {
              serveCacheRangeFromUpstream({1, 5}, {2, 0}, 10, 1, 1, true);
              expectFetchObjects({0, 0}, {1, 0}, false, 10, 1, 1, true);
            });
      });
  auto res = co_await cache_.fetch(
      getFetch({0, 0}, {2, 6}, GroupOrder::NewestFirst), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 6}));
}

CO_TEST_F(MoQCacheTest, TestFetchPartialHitBeginningGroupBoundaryDesc) {
  populateCacheRange({0, 0}, {1, 0}, 10, 1, 1, true);
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch(
      {1, 0}, {2, 6}, 0, AbsoluteLocation{2, 6}, GroupOrder::NewestFirst)
      .via(exec)
      .thenTry([this](const auto&) {
        expectFetchObjectsDescending({1, 0}, {2, 6}, false, 10, 1, 1, true);
        serveCacheRangeFromUpstreamDescending({1, 0}, {2, 6}, 10, 1, 1, true);
        expectFetchObjects({0, 0}, {1, 0}, false, 10, 1, 1, true);
      });
  auto res = co_await cache_.fetch(
      getFetch({0, 0}, {2, 6}, GroupOrder::NewestFirst), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{2, 6}));
}

CO_TEST_F(MoQCacheTest, TestFetchMissTailGroupDesc) {
  populateCacheRange({3, 0}, {3, 5}, 10, 1, 1, false);
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch(
      {1, 0}, {2, 0}, 0, AbsoluteLocation{2, 0}, GroupOrder::NewestFirst)
      .via(exec)
      .thenTry([this](const auto&) {
        expectFetchObjectsDescending({1, 0}, {3, 0}, true, 10, 1, 1, true);
        serveCacheRangeFromUpstreamDescending({1, 0}, {3, 0}, 10, 1, 1, true);
      });
  expectFetchObjects({3, 0}, {3, 5}, false, 10, 1, 1, true);
  auto res = co_await cache_.fetch(
      getFetch({1, 0}, {3, 5}, GroupOrder::NewestFirst), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{3, 5}));
}

CO_TEST_F(MoQCacheTest, TestFetchMissTailObjectsDesc) {
  populateCacheRange({3, 0}, {3, 5});
  populateCacheRange({1, 0}, {1, 5});
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch(
      {2, 0}, {2, 0}, 0, AbsoluteLocation{2, 0}, GroupOrder::NewestFirst)
      .via(exec)
      .thenTry([this, exec](const auto&) {
        expectFetchObjectsDescending({2, 0}, {3, 0}, false, 10, 1, 1, true);
        serveCacheRangeFromUpstreamDescending({2, 0}, {3, 0}, 10, 1, 1, true);

        expectFetchObjects({1, 0}, {1, 5}, false, 10, 1, 1, false);
        expectUpstreamFetch(
            {1, 5}, {1, 0}, 0, AbsoluteLocation{1, 0}, GroupOrder::NewestFirst)
            .via(exec)
            .thenTry([this](const auto&) {
              expectFetchObjects({1, 5}, {2, 0}, true, 10, 1, 1, true);
              serveCacheRangeFromUpstream({1, 5}, {2, 0}, 10, 1, 1, true);
            });
      });
  expectFetchObjects({3, 0}, {3, 5}, false, 10, 1, 1, false);
  auto res = co_await cache_.fetch(
      getFetch({1, 0}, {3, 5}, GroupOrder::NewestFirst), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{3, 5}));
}

CO_TEST_F(MoQCacheTest, TestFetchPartialMissTailObjectsDesc) {
  populateCacheRange({3, 0}, {3, 5});
  populateCacheRange({1, 0}, {1, 5});
  populateCacheRange({1, 9}, {2, 0}, 10, 1, 1, true);
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch(
      {2, 0}, {2, 0}, 0, AbsoluteLocation{2, 0}, GroupOrder::NewestFirst)
      .via(exec)
      .thenTry([this, exec](const auto&) {
        expectFetchObjectsDescending({2, 0}, {3, 0}, false, 10, 1, 1, true);
        serveCacheRangeFromUpstreamDescending({2, 0}, {3, 0}, 10, 1, 1, true);

        expectFetchObjects({1, 0}, {1, 5}, false, 10, 1, 1, false);
        expectUpstreamFetch(
            {1, 5}, {1, 9}, 0, AbsoluteLocation{1, 9}, GroupOrder::NewestFirst)
            .via(exec)
            .thenTry([this](const auto&) {
              expectFetchObjects({1, 5}, {1, 9}, false, 10, 1, 1, false);
              serveCacheRangeFromUpstream({1, 5}, {1, 9}, 10, 1, 1, false);
              expectFetchObjects({1, 9}, {2, 0}, false, 10, 1, 1, true);
            });
      });
  expectFetchObjects({3, 0}, {3, 5}, false, 10, 1, 1, false);
  auto res = co_await cache_.fetch(
      getFetch({1, 0}, {3, 5}, GroupOrder::NewestFirst), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{3, 5}));
}

CO_TEST_F(MoQCacheTest, TestFetchPartialMissThreeUpstreamFetchesDesc) {
  populateCacheRange({5, 0}, {5, 5});
  populateCacheRange({1, 7}, {2, 0}, 10, 1, 1, true);
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch(
      {5, 5}, {5, 8}, 0, AbsoluteLocation{5, 8}, GroupOrder::NewestFirst)
      .via(exec)
      .thenTry([this, exec](const auto&) {
        expectFetchObjects({5, 5}, {5, 8}, false, 10, 1, 1, false);
        serveCacheRangeFromUpstream({5, 5}, {5, 8}, 10, 1, 1, false);

        expectUpstreamFetch(
            {2, 0}, {4, 0}, 0, AbsoluteLocation{4, 0}, GroupOrder::NewestFirst)
            .via(exec)
            .thenTry([this, exec](const auto&) {
              expectFetchObjectsDescending(
                  {2, 0}, {5, 0}, false, 10, 1, 1, true);
              serveCacheRangeFromUpstreamDescending(
                  {2, 0}, {5, 0}, 10, 1, 1, true);

              expectUpstreamFetch(
                  {1, 3},
                  {1, 7},
                  0,
                  AbsoluteLocation{1, 7},
                  GroupOrder::NewestFirst)
                  .via(exec)
                  .thenTry([this](const auto&) {
                    expectFetchObjects({1, 3}, {1, 7}, false, 10, 1, 1, false);
                    serveCacheRangeFromUpstream(
                        {1, 3}, {1, 7}, 10, 1, 1, false);
                    expectFetchObjects({1, 7}, {2, 0}, false, 10, 1, 1, true);
                  });
            });
      });
  expectFetchObjects({5, 0}, {5, 5}, false, 10, 1, 1, false);
  auto res = co_await cache_.fetch(
      getFetch({1, 3}, {5, 8}, GroupOrder::NewestFirst), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{5, 8}));
}

CO_TEST_F(MoQCacheTest, TestFetchPartialMissTwoUpstreamFetchesTailDesc) {
  populateCacheRange({4, 0}, {4, 3});
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch(
      {4, 3}, {4, 8}, 0, AbsoluteLocation{4, 8}, GroupOrder::NewestFirst)
      .via(exec)
      .thenTry([this, exec](const auto&) {
        expectFetchObjects({4, 4}, {4, 8}, false, 10, 1, 1, false);
        serveCacheRangeFromUpstream({4, 4}, {4, 8}, 10, 1, 1, false);

        expectUpstreamFetch(
            {1, 3}, {3, 0}, 0, AbsoluteLocation{3, 0}, GroupOrder::NewestFirst)
            .via(exec)
            .thenTry([this, exec](const auto&) {
              expectFetchObjectsDescending(
                  {1, 3}, {4, 0}, true, 10, 1, 1, true);
              serveCacheRangeFromUpstreamDescending(
                  {1, 3}, {4, 0}, 10, 1, 1, true);
            });
      });
  expectFetchObjects({4, 0}, {4, 3}, false, 10, 1, 1, false);
  auto res = co_await cache_.fetch(
      getFetch({1, 3}, {4, 8}, GroupOrder::NewestFirst), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{4, 8}));
}

CO_TEST_F(MoQCacheTest, TestFetchPartialMissTwoUpstreamFetchesTail) {
  populateCacheRange({0, 5}, {0, 7});
  auto exec = co_await folly::coro::co_current_executor;
  expectUpstreamFetch({0, 0}, {0, 5}, 0, AbsoluteLocation{0, 5})
      .via(exec)
      .thenTry([this, exec](const auto&) {
        expectFetchObjects({0, 0}, {0, 5}, false, 10, 1, 1, false);
        serveCacheRangeFromUpstream({0, 0}, {0, 5}, 10, 1, 1, false);
        expectFetchObjects({0, 5}, {0, 7}, false, 10, 1, 1, false);
        expectUpstreamFetch({0, 7}, {0, 9}, 0, AbsoluteLocation{0, 9})
            .via(exec)
            .thenTry([this, exec](const auto&) {
              expectFetchObjects({0, 7}, {0, 9}, true, 10, 1, 1, true);
              serveCacheRangeFromUpstream({0, 7}, {0, 9}, 10, 1, 1, true);
            });
      });
  auto res =
      co_await cache_.fetch(getFetch({0, 0}, {0, 9}), consumer_, upstream_);
  EXPECT_TRUE(res.hasValue());
  EXPECT_EQ(res.value()->fetchOk().endLocation, (AbsoluteLocation{0, 9}));
}

} // namespace moxygen::test
