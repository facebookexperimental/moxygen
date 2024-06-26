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
    folly::Optional<AbsoluteLocation> end = folly::none) {
  return SubscribeRequest{0, 0, FullTrackName(), locType, start, end, {}};
}
} // namespace

TEST(Location, LatestObject) {
  auto range = toSubscribeRange(
      getRequest(LocationType::LatestObject), AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 77);
  EXPECT_EQ(range.end <=> kLocationMax, std::strong_ordering::equivalent);
}

TEST(Location, LatestGroup) {
  auto range = toSubscribeRange(
      getRequest(LocationType::LatestGroup), AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 0);
  EXPECT_EQ(range.end <=> kLocationMax, std::strong_ordering::equivalent);
}

TEST(Location, AbsoluteStart) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteStart, AbsoluteLocation({1, 2})),
      AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 1);
  EXPECT_EQ(range.start.object, 2);
  EXPECT_EQ(range.end <=> kLocationMax, std::strong_ordering::equivalent);
}

TEST(Location, AbsoluteRange) {
  auto range = toSubscribeRange(
      getRequest(
          LocationType::AbsoluteRange,
          AbsoluteLocation({1, 2}),
          AbsoluteLocation({3, 4})),
      AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 1);
  EXPECT_EQ(range.start.object, 2);
  EXPECT_EQ(range.end.group, 3);
  EXPECT_EQ(range.end.object, 4);
}

TEST(Location, Compare) {
  AbsoluteLocation loc1 = {1, 2};
  AbsoluteLocation loc2 = {1, 3};
  AbsoluteLocation loc3 = {2, 1};
  EXPECT_EQ(loc1 <=> loc1, std::strong_ordering::equivalent);
  EXPECT_TRUE(loc1 < loc2);
  EXPECT_TRUE(loc2 > loc1);
  EXPECT_TRUE(loc2 < loc3);
  EXPECT_TRUE(loc3 > loc2);
}
