/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/cache/MoQPriorGapRanges.h"

#include <folly/portability/GTest.h>

namespace moxygen::test {
namespace {

Extensions gaps(uint64_t priorObjects, uint64_t priorGroups) {
  Extensions extensions;
  extensions.insertMutableExtension(
      Extension{kPriorObjectIdGapExtensionType, priorObjects});
  extensions.insertMutableExtension(
      Extension{kPriorGroupIdGapExtensionType, priorGroups});
  return extensions;
}

} // namespace

TEST(MoQPriorGapRangesTest, GapsCoverTheIDsJustBefore) {
  auto ranges = decodePriorGapRanges(/*groupID=*/5, /*objectID=*/4, gaps(3, 2));

  EXPECT_TRUE(ranges.valid);
  EXPECT_EQ(ranges.objects, (MoQIdRange{.start = 1, .end = 4}));
  EXPECT_EQ(ranges.groups, (MoQIdRange{.start = 3, .end = 5}));
}

TEST(MoQPriorGapRangesTest, ZeroGapsProveNothing) {
  auto ranges = decodePriorGapRanges(5, 4, gaps(0, 0));

  EXPECT_TRUE(ranges.valid);
  EXPECT_EQ(ranges.objects, std::nullopt);
  EXPECT_EQ(ranges.groups, std::nullopt);
}

TEST(MoQPriorGapRangesTest, GapBelowIDZeroIsInvalid) {
  EXPECT_FALSE(decodePriorGapRanges(5, 1, gaps(2, 0)).valid);
  EXPECT_FALSE(decodePriorGapRanges(1, 4, gaps(0, 2)).valid);
}

} // namespace moxygen::test
