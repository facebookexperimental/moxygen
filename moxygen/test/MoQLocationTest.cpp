/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>
#include <moxygen/MoQLocation.h>

using namespace moxygen;

namespace {
SubscribeRequest getRequest(
    LocationType locType,
    folly::Optional<AbsoluteLocation> start = folly::none,
    uint64_t endGroup = 0) {
  return SubscribeRequest::make(
      FullTrackName(),
      0,
      GroupOrder::OldestFirst,
      true,
      locType,
      start,
      endGroup,
      {});
}
} // namespace

TEST(Location, LatestObject) {
  auto range = toSubscribeRange(
      getRequest(LocationType::LatestObject), AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 78);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, NextGroupStart) {
  auto range = toSubscribeRange(
      getRequest(LocationType::NextGroupStart), AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 20);
  EXPECT_EQ(range.start.object, 0);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, LatestGroup) {
  auto range = toSubscribeRange(
      getRequest(LocationType::LatestGroup), AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 0);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, AbsoluteStart) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteStart, AbsoluteLocation({1, 2})),
      AbsoluteLocation({1, 1}));
  EXPECT_EQ(range.start.group, 1);
  EXPECT_EQ(range.start.object, 2);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, AbsoluteStartBehindLatest) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteStart, AbsoluteLocation({1, 2})),
      AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 78);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, AbsoluteRange) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteRange, AbsoluteLocation({1, 2}), 3),
      AbsoluteLocation({1, 1}));
  EXPECT_EQ(range.start.group, 1);
  EXPECT_EQ(range.start.object, 2);
  EXPECT_EQ(range.end.group, 3);
  EXPECT_EQ(range.end.object, 0);
}

TEST(Location, AbsoluteRangeBehindLatest) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteRange, AbsoluteLocation({1, 2}), 3),
      AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 78);
  EXPECT_EQ(range.end.group, 3);
  EXPECT_EQ(range.end.object, 0);
}

TEST(Location, Compare) {
  AbsoluteLocation loc1 = {1, 2};
  AbsoluteLocation loc2 = {1, 3};
  AbsoluteLocation loc3 = {2, 1};
  EXPECT_TRUE(loc1 == loc1);
  EXPECT_TRUE(loc1 < loc2);
  EXPECT_TRUE(loc2 > loc1);
  EXPECT_TRUE(loc2 < loc3);
  EXPECT_TRUE(loc3 > loc2);
}
