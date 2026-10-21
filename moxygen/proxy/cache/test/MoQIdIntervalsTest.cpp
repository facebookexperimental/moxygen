/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/cache/MoQIdIntervals.h"

#include <folly/portability/GTest.h>

namespace moxygen::test {

TEST(MoQIdIntervalsTest, InsertMergesOverlappingAndTouchingRanges) {
  MoQIdIntervals intervals;
  intervals.insert({.start = 2, .end = 4});
  intervals.insert({.start = 6, .end = 8});
  EXPECT_EQ(intervals.endContaining(3), 4);
  EXPECT_EQ(intervals.endContaining(4), std::nullopt);
  EXPECT_EQ(intervals.nextStartAfter(4), 6);

  intervals.insert({.start = 4, .end = 6});

  EXPECT_EQ(intervals.endContaining(2), 8);
  EXPECT_EQ(intervals.nextStartAfter(2), std::nullopt);
  EXPECT_EQ(intervals.lastEnd(), 8);
}

TEST(MoQIdIntervalsTest, EraseEndingByKeepsRangesThatEndLater) {
  MoQIdIntervals intervals;
  intervals.insert({.start = 0, .end = 2});
  intervals.insert({.start = 5, .end = 9});

  intervals.eraseEndingBy(5);

  EXPECT_EQ(intervals.endContaining(0), std::nullopt);
  EXPECT_EQ(intervals.endContaining(5), 9);
}

} // namespace moxygen::test
