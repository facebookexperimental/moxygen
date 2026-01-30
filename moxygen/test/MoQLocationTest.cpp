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
    std::optional<AbsoluteLocation> start = std::nullopt,
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

TEST(Location, LargestObject) {
  auto range = toSubscribeRange(
      getRequest(LocationType::LargestObject), AbsoluteLocation({19, 77}));
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

TEST(Location, LargestGroup) {
  auto range = toSubscribeRange(
      getRequest(LocationType::LargestGroup), AbsoluteLocation({19, 77}));
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

TEST(Location, AbsoluteStartBehindLargest) {
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

TEST(Location, AbsoluteRangeBehindLargest) {
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
