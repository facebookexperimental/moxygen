/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GTest.h>
#include <moxygen/util/LocationIntervalSet.h>

using namespace moxygen;

class LocationIntervalSetTest : public ::testing::Test {
 protected:
  LocationIntervalSet set_;
};

TEST_F(LocationIntervalSetTest, EmptySet) {
  EXPECT_TRUE(set_.empty());
  EXPECT_EQ(set_.size(), 0);
  EXPECT_FALSE(set_.contains({0, 0}));
  EXPECT_FALSE(set_.findIntervalEnd({0, 0}).has_value());
}

TEST_F(LocationIntervalSetTest, InsertSinglePosition) {
  set_.insert({0, 7});

  EXPECT_EQ(set_.size(), 1);
  EXPECT_FALSE(set_.contains({0, 6}));
  EXPECT_TRUE(set_.contains({0, 7}));
  EXPECT_FALSE(set_.contains({0, 8}));

  // Adjacent single-position inserts merge.
  set_.insert({0, 8});
  EXPECT_EQ(set_.size(), 1);
  EXPECT_TRUE(set_.contains({0, 8}));
  EXPECT_EQ(set_.findIntervalEnd({0, 7}), (AbsoluteLocation{0, 8}));
}

TEST_F(LocationIntervalSetTest, SingleInterval) {
  set_.insert({0, 5}, {0, 10});

  EXPECT_FALSE(set_.empty());
  EXPECT_EQ(set_.size(), 1);

  // Before interval
  EXPECT_FALSE(set_.contains({0, 4}));
  // In interval
  EXPECT_TRUE(set_.contains({0, 5}));
  EXPECT_TRUE(set_.contains({0, 7}));
  EXPECT_TRUE(set_.contains({0, 10}));
  // After interval
  EXPECT_FALSE(set_.contains({0, 11}));
}

TEST_F(LocationIntervalSetTest, FindIntervalEnd) {
  set_.insert({0, 5}, {0, 10});

  EXPECT_FALSE(set_.findIntervalEnd({0, 4}).has_value());
  EXPECT_EQ(set_.findIntervalEnd({0, 5}), (AbsoluteLocation{0, 10}));
  EXPECT_EQ(set_.findIntervalEnd({0, 7}), (AbsoluteLocation{0, 10}));
  EXPECT_EQ(set_.findIntervalEnd({0, 10}), (AbsoluteLocation{0, 10}));
  EXPECT_FALSE(set_.findIntervalEnd({0, 11}).has_value());
}

TEST_F(LocationIntervalSetTest, FindInterval) {
  set_.insert({0, 5}, {0, 10});

  // Before interval
  EXPECT_FALSE(set_.findInterval({0, 4}).has_value());

  // At start of interval
  auto result = set_.findInterval({0, 5});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 5}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 10}));

  // In middle of interval
  result = set_.findInterval({0, 7});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 5}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 10}));

  // At end of interval
  result = set_.findInterval({0, 10});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 5}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 10}));

  // After interval
  EXPECT_FALSE(set_.findInterval({0, 11}).has_value());
}

TEST_F(LocationIntervalSetTest, FindIntervalMultiple) {
  set_.insert({0, 0}, {0, 5});
  set_.insert({0, 10}, {0, 15});

  // First interval
  auto result = set_.findInterval({0, 3});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 0}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 5}));

  // Gap between intervals
  EXPECT_FALSE(set_.findInterval({0, 7}).has_value());

  // Second interval
  result = set_.findInterval({0, 12});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 10}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 15}));
}

TEST_F(LocationIntervalSetTest, DisjointIntervals) {
  set_.insert({0, 0}, {0, 5});
  set_.insert({0, 10}, {0, 15});

  EXPECT_EQ(set_.size(), 2);

  EXPECT_TRUE(set_.contains({0, 0}));
  EXPECT_TRUE(set_.contains({0, 5}));
  EXPECT_FALSE(set_.contains({0, 6}));
  EXPECT_FALSE(set_.contains({0, 9}));
  EXPECT_TRUE(set_.contains({0, 10}));
  EXPECT_TRUE(set_.contains({0, 15}));
  EXPECT_FALSE(set_.contains({0, 16}));
}

TEST_F(LocationIntervalSetTest, MergeOverlapping) {
  set_.insert({0, 0}, {0, 10});
  set_.insert({0, 5}, {0, 15});

  EXPECT_EQ(set_.size(), 1);
  EXPECT_TRUE(set_.contains({0, 0}));
  EXPECT_TRUE(set_.contains({0, 15}));
  EXPECT_EQ(set_.findIntervalEnd({0, 0}), (AbsoluteLocation{0, 15}));
}

TEST_F(LocationIntervalSetTest, MergeAdjacent) {
  set_.insert({0, 0}, {0, 5});
  set_.insert({0, 6}, {0, 10});

  // Should merge because {0, 5} and {0, 6} are adjacent
  EXPECT_EQ(set_.size(), 1);
  EXPECT_EQ(set_.findIntervalEnd({0, 0}), (AbsoluteLocation{0, 10}));
}

TEST_F(LocationIntervalSetTest, MergeMultiple) {
  set_.insert({0, 0}, {0, 5});
  set_.insert({0, 10}, {0, 15});
  set_.insert({0, 20}, {0, 25});

  EXPECT_EQ(set_.size(), 3);

  // Insert interval that spans all three
  set_.insert({0, 3}, {0, 22});

  EXPECT_EQ(set_.size(), 1);
  EXPECT_EQ(set_.findIntervalEnd({0, 0}), (AbsoluteLocation{0, 25}));
}

TEST_F(LocationIntervalSetTest, CrossGroupInterval) {
  // Interval spanning multiple groups
  set_.insert({0, 100}, {2, 50});

  EXPECT_TRUE(set_.contains({0, 100}));
  EXPECT_TRUE(set_.contains({0, 1000}));
  EXPECT_TRUE(set_.contains({1, 0}));
  EXPECT_TRUE(set_.contains({1, 500}));
  EXPECT_TRUE(set_.contains({2, 0}));
  EXPECT_TRUE(set_.contains({2, 50}));
  EXPECT_FALSE(set_.contains({2, 51}));
  EXPECT_FALSE(set_.contains({0, 99}));
}

TEST_F(LocationIntervalSetTest, LargeGap) {
  // Test O(1) insertion of large gap (like 2^60 groups)
  constexpr uint64_t kLargeGroup = 1ULL << 60;

  set_.insert({1, 0}, {kLargeGroup - 1, kEightByteLimit});

  EXPECT_EQ(set_.size(), 1);
  EXPECT_FALSE(set_.contains({0, 0}));
  EXPECT_TRUE(set_.contains({1, 0}));
  EXPECT_TRUE(set_.contains({kLargeGroup / 2, 0}));
  EXPECT_TRUE(set_.contains({kLargeGroup - 1, 0}));
  EXPECT_FALSE(set_.contains({kLargeGroup, 0}));
}

TEST_F(LocationIntervalSetTest, Clear) {
  set_.insert({0, 0}, {0, 10});
  EXPECT_FALSE(set_.empty());

  set_.clear();
  EXPECT_TRUE(set_.empty());
  EXPECT_FALSE(set_.contains({0, 5}));
}

TEST_F(LocationIntervalSetTest, InsertInvalidInterval) {
  // end < start should be ignored
  set_.insert({0, 10}, {0, 5});
  EXPECT_TRUE(set_.empty());
}

TEST_F(LocationIntervalSetTest, AdjacentAcrossGroups) {
  // {0, kEightByteLimit} is adjacent to {1, 0}
  set_.insert({0, 0}, {0, kEightByteLimit});
  set_.insert({1, 0}, {1, 10});

  // Should merge
  EXPECT_EQ(set_.size(), 1);
  EXPECT_EQ(set_.findIntervalEnd({0, 0}), (AbsoluteLocation{1, 10}));
}

TEST_F(LocationIntervalSetTest, InsertSameIntervalTwice) {
  set_.insert({0, 5}, {0, 10});
  set_.insert({0, 5}, {0, 10});

  EXPECT_EQ(set_.size(), 1);
}

TEST_F(LocationIntervalSetTest, InsertSubInterval) {
  set_.insert({0, 0}, {0, 20});
  set_.insert({0, 5}, {0, 10}); // Subset

  EXPECT_EQ(set_.size(), 1);
  EXPECT_EQ(set_.findIntervalEnd({0, 0}), (AbsoluteLocation{0, 20}));
}

TEST_F(LocationIntervalSetTest, InsertSuperInterval) {
  set_.insert({0, 5}, {0, 10});
  set_.insert({0, 0}, {0, 20}); // Superset

  EXPECT_EQ(set_.size(), 1);
  EXPECT_EQ(set_.findIntervalEnd({0, 0}), (AbsoluteLocation{0, 20}));
}

// Tests for remove()

TEST_F(LocationIntervalSetTest, RemoveFromEmpty) {
  set_.remove({0, 0}, {0, 10});
  EXPECT_TRUE(set_.empty());
}

TEST_F(LocationIntervalSetTest, RemoveNoOverlap) {
  set_.insert({0, 5}, {0, 10});
  set_.remove({0, 15}, {0, 20});
  EXPECT_EQ(set_.size(), 1);
  EXPECT_TRUE(set_.contains({0, 7}));
}

TEST_F(LocationIntervalSetTest, RemoveEntireInterval) {
  set_.insert({0, 5}, {0, 10});
  set_.remove({0, 5}, {0, 10});
  EXPECT_TRUE(set_.empty());
}

TEST_F(LocationIntervalSetTest, RemoveSuperset) {
  set_.insert({0, 5}, {0, 10});
  set_.remove({0, 0}, {0, 20});
  EXPECT_TRUE(set_.empty());
}

TEST_F(LocationIntervalSetTest, RemoveTrimHead) {
  set_.insert({0, 5}, {0, 10});
  set_.remove({0, 5}, {0, 7});
  EXPECT_EQ(set_.size(), 1);
  EXPECT_FALSE(set_.contains({0, 7}));
  EXPECT_TRUE(set_.contains({0, 8}));
  EXPECT_TRUE(set_.contains({0, 10}));
}

TEST_F(LocationIntervalSetTest, RemoveTrimTail) {
  set_.insert({0, 5}, {0, 10});
  set_.remove({0, 8}, {0, 10});
  EXPECT_EQ(set_.size(), 1);
  EXPECT_TRUE(set_.contains({0, 5}));
  EXPECT_TRUE(set_.contains({0, 7}));
  EXPECT_FALSE(set_.contains({0, 8}));
}

TEST_F(LocationIntervalSetTest, RemoveSplitInterval) {
  set_.insert({0, 0}, {0, 20});
  set_.remove({0, 5}, {0, 10});
  EXPECT_EQ(set_.size(), 2);
  EXPECT_TRUE(set_.contains({0, 0}));
  EXPECT_TRUE(set_.contains({0, 4}));
  EXPECT_FALSE(set_.contains({0, 5}));
  EXPECT_FALSE(set_.contains({0, 10}));
  EXPECT_TRUE(set_.contains({0, 11}));
  EXPECT_TRUE(set_.contains({0, 20}));
}

TEST_F(LocationIntervalSetTest, RemoveSpanMultiple) {
  set_.insert({0, 0}, {0, 5});
  set_.insert({0, 10}, {0, 15});
  set_.insert({0, 20}, {0, 25});
  set_.remove({0, 3}, {0, 22});
  EXPECT_EQ(set_.size(), 2);
  EXPECT_TRUE(set_.contains({0, 0}));
  EXPECT_TRUE(set_.contains({0, 2}));
  EXPECT_FALSE(set_.contains({0, 3}));
  EXPECT_FALSE(set_.contains({0, 22}));
  EXPECT_TRUE(set_.contains({0, 23}));
  EXPECT_TRUE(set_.contains({0, 25}));
}

TEST_F(LocationIntervalSetTest, RemoveAtZero) {
  set_.insert({0, 0}, {0, 10});
  set_.remove({0, 0}, {0, 5});
  EXPECT_EQ(set_.size(), 1);
  EXPECT_FALSE(set_.contains({0, 0}));
  EXPECT_TRUE(set_.contains({0, 6}));
}

TEST_F(LocationIntervalSetTest, RemoveAtMax) {
  set_.insert({0, kEightByteLimit - 5}, {0, kEightByteLimit});
  set_.remove({0, kEightByteLimit - 2}, {0, kEightByteLimit});
  EXPECT_EQ(set_.size(), 1);
  EXPECT_TRUE(set_.contains({0, kEightByteLimit - 5}));
  EXPECT_TRUE(set_.contains({0, kEightByteLimit - 3}));
  EXPECT_FALSE(set_.contains({0, kEightByteLimit - 2}));
}

TEST_F(LocationIntervalSetTest, RemoveSinglePoint) {
  set_.insert({0, 0}, {0, 10});
  set_.remove({0, 5}, {0, 5});
  EXPECT_EQ(set_.size(), 2);
  EXPECT_TRUE(set_.contains({0, 4}));
  EXPECT_FALSE(set_.contains({0, 5}));
  EXPECT_TRUE(set_.contains({0, 6}));
}

// Tests for findNextInterval()

TEST_F(LocationIntervalSetTest, FindNextIntervalBeforeAll) {
  set_.insert({0, 10}, {0, 20});
  auto result = set_.findNextInterval({0, 0});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 10}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 20}));
}

TEST_F(LocationIntervalSetTest, FindNextIntervalInsideCurrent) {
  set_.insert({0, 0}, {0, 10});
  set_.insert({0, 20}, {0, 30});
  auto result = set_.findNextInterval({0, 5});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 20}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 30}));
}

TEST_F(LocationIntervalSetTest, FindNextIntervalBetween) {
  set_.insert({0, 0}, {0, 5});
  set_.insert({0, 20}, {0, 30});
  auto result = set_.findNextInterval({0, 10});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 20}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 30}));
}

TEST_F(LocationIntervalSetTest, FindNextIntervalAfterAll) {
  set_.insert({0, 0}, {0, 10});
  auto result = set_.findNextInterval({0, 15});
  EXPECT_FALSE(result.has_value());
}

TEST_F(LocationIntervalSetTest, FindNextIntervalAtEndOfCurrent) {
  set_.insert({0, 0}, {0, 10});
  set_.insert({0, 20}, {0, 30});
  auto result = set_.findNextInterval({0, 10});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 20}));
}

TEST_F(LocationIntervalSetTest, FindNextIntervalEmpty) {
  auto result = set_.findNextInterval({0, 0});
  EXPECT_FALSE(result.has_value());
}

TEST_F(LocationIntervalSetTest, FindNextIntervalAtLocStart) {
  set_.insert({0, 5}, {0, 10});
  auto result = set_.findNextInterval({0, 4});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 5}));
}

// Tests for findPrevInterval()

TEST_F(LocationIntervalSetTest, FindPrevIntervalAfterAll) {
  set_.insert({0, 0}, {0, 10});
  auto result = set_.findPrevInterval({0, 20});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 0}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 10}));
}

TEST_F(LocationIntervalSetTest, FindPrevIntervalInsideCurrent) {
  set_.insert({0, 0}, {0, 10});
  set_.insert({0, 20}, {0, 30});
  auto result = set_.findPrevInterval({0, 25});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 0}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 10}));
}

TEST_F(LocationIntervalSetTest, FindPrevIntervalBetween) {
  set_.insert({0, 0}, {0, 5});
  set_.insert({0, 20}, {0, 30});
  auto result = set_.findPrevInterval({0, 10});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 0}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 5}));
}

TEST_F(LocationIntervalSetTest, FindPrevIntervalBeforeAll) {
  set_.insert({0, 10}, {0, 20});
  auto result = set_.findPrevInterval({0, 5});
  EXPECT_FALSE(result.has_value());
}

TEST_F(LocationIntervalSetTest, FindPrevIntervalEmpty) {
  auto result = set_.findPrevInterval({0, 0});
  EXPECT_FALSE(result.has_value());
}

TEST_F(LocationIntervalSetTest, FindPrevIntervalAtStartOfFirst) {
  set_.insert({0, 0}, {0, 10});
  auto result = set_.findPrevInterval({0, 0});
  EXPECT_FALSE(result.has_value());
}

TEST_F(LocationIntervalSetTest, FindPrevIntervalMultiple) {
  set_.insert({0, 0}, {0, 5});
  set_.insert({0, 10}, {0, 15});
  set_.insert({0, 20}, {0, 25});
  auto result = set_.findPrevInterval({0, 22});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->first, (AbsoluteLocation{0, 10}));
  EXPECT_EQ(result->second, (AbsoluteLocation{0, 15}));
}

// Tests for AbsoluteLocation helper methods

TEST(AbsoluteLocationTest, PrevGroupEnd) {
  // Normal case: group > 0
  auto result = AbsoluteLocation{5, 10}.prevGroupEnd();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->group, 4);
  EXPECT_EQ(result->object, kEightByteLimit);

  // Edge case: group == 0 (no previous group)
  result = AbsoluteLocation{0, 10}.prevGroupEnd();
  EXPECT_FALSE(result.has_value());

  // Object value doesn't affect result
  result = AbsoluteLocation{3, 0}.prevGroupEnd();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->group, 2);
  EXPECT_EQ(result->object, kEightByteLimit);
}

TEST(AbsoluteLocationTest, PrevInGroup) {
  // Normal case: object > 0
  auto result = AbsoluteLocation{5, 10}.prevInGroup();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->group, 5);
  EXPECT_EQ(result->object, 9);

  // Edge case: object == 0 (no previous in group)
  result = AbsoluteLocation{5, 0}.prevInGroup();
  EXPECT_FALSE(result.has_value());

  // Group 0, object > 0
  result = AbsoluteLocation{0, 5}.prevInGroup();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->group, 0);
  EXPECT_EQ(result->object, 4);
}
