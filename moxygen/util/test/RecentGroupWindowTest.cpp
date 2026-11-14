/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/util/RecentGroupWindow.h>

#include <folly/portability/GTest.h>

namespace moxygen::test {

TEST(RecentGroupWindowTest, InterleavedAndReorderedGroupsCountOnce) {
  RecentGroupWindow window;
  uint64_t admitted = 0;
  for (uint64_t group : {5, 4, 5, 6, 4, 6}) {
    admitted += window.admit(group) ? 1 : 0;
  }
  EXPECT_EQ(admitted, 3);
  // A group counts again once kSize later groups pass it.
  for (uint64_t group = 10; group < 10 + RecentGroupWindow::kSize; ++group) {
    EXPECT_TRUE(window.admit(group));
  }
  EXPECT_TRUE(window.admit(5));
  EXPECT_FALSE(window.admit(5));
}

} // namespace moxygen::test
