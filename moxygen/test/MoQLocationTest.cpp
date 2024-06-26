/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>
#include <moxygen/MoQLocation.h>

using namespace moxygen;

TEST(Location, LatestObject) {
  auto abs = toAbsolute(LocationType::LatestObject, folly::none, 19, 77);
  EXPECT_EQ(abs.group, 19);
  EXPECT_EQ(abs.object, 77);
}

TEST(Location, LatestGroup) {
  auto abs = toAbsolute(LocationType::LatestGroup, folly::none, 19, 77);
  EXPECT_EQ(abs.group, 19);
  EXPECT_EQ(abs.object, 0);
}

TEST(Location, AbsoluteStart) {
  auto abs =
      toAbsolute(LocationType::AbsoluteStart, AbsoluteLocation({1, 2}), 19, 77);
  EXPECT_EQ(abs.group, 1);
  EXPECT_EQ(abs.object, 2);
}

TEST(Location, AbsoluteRange) {
  auto abs =
      toAbsolute(LocationType::AbsoluteRange, AbsoluteLocation({1, 2}), 19, 77);
  EXPECT_EQ(abs.group, 1);
  EXPECT_EQ(abs.object, 2);
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
