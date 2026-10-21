/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/cache/MoQCacheBudget.h"

#include <folly/portability/GTest.h>

namespace moxygen::test {

TEST(MoQCacheBudgetTest, ReserveFailsBeyondCapacity) {
  auto budget = std::make_shared<MoQCacheBudget>(100);
  auto held = MoQCacheCharge::reserve(budget, 60);
  ASSERT_TRUE(held.has_value());

  EXPECT_FALSE(MoQCacheCharge::reserve(budget, 50).has_value());
  EXPECT_EQ(budget->used(), 60);
}

TEST(MoQCacheBudgetTest, ChargeIsReleasedByWhicheverOwnerDropsIt) {
  auto budget = std::make_shared<MoQCacheBudget>(100);
  auto first = MoQCacheCharge::reserve(budget, 60);
  auto second = MoQCacheCharge::reserve(budget, 30);
  ASSERT_TRUE(first.has_value() && second.has_value());

  // Assigning over a charge releases the one it held.
  *first = std::move(*second);
  EXPECT_EQ(budget->used(), 30);
  EXPECT_EQ(second->bytes(), 0);

  first.reset();
  EXPECT_EQ(budget->used(), 0);
}

} // namespace moxygen::test
