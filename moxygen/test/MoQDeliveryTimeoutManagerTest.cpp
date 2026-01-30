/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GTest.h>
#include <moxygen/events/MoQDeliveryTimeoutManager.h>

using namespace moxygen;
using namespace std::chrono_literals;

class MoQDeliveryTimeoutManagerTest : public ::testing::Test {
 protected:
  MoQDeliveryTimeoutManager manager_;
};

// Test 1: Verify that effective timeout is min(upstream, downstream)
TEST_F(MoQDeliveryTimeoutManagerTest, EffectiveTimeoutIsMinimum) {
  manager_.setUpstreamTimeout(1000ms);
  manager_.setDownstreamTimeout(500ms);

  auto effective = manager_.getEffectiveTimeout();
  ASSERT_TRUE(effective.has_value());
  EXPECT_EQ(effective.value(), 500ms);

  // Swap values - effective should now be 1000ms
  manager_.setUpstreamTimeout(500ms);
  manager_.setDownstreamTimeout(1000ms);

  effective = manager_.getEffectiveTimeout();
  ASSERT_TRUE(effective.has_value());
  EXPECT_EQ(effective.value(), 500ms);
}

// Test 2: Verify callback is invoked when effective timeout changes
TEST_F(MoQDeliveryTimeoutManagerTest, CallbackInvokedOnChange) {
  int callbackCount = 0;
  std::optional<std::chrono::milliseconds> lastValue;

  manager_.setOnChangeCallback(
      [&](const std::optional<std::chrono::milliseconds>& newTimeout) {
        callbackCount++;
        lastValue = newTimeout;
      });

  // First set should trigger callback
  manager_.setUpstreamTimeout(1000ms);
  EXPECT_EQ(callbackCount, 1);
  ASSERT_TRUE(lastValue.has_value());
  EXPECT_EQ(lastValue.value(), 1000ms);

  // Setting downstream lower should trigger callback with new min
  manager_.setDownstreamTimeout(500ms);
  EXPECT_EQ(callbackCount, 2);
  ASSERT_TRUE(lastValue.has_value());
  EXPECT_EQ(lastValue.value(), 500ms);

  // Setting downstream higher (but still < upstream) should change effective
  manager_.setDownstreamTimeout(800ms);
  EXPECT_EQ(callbackCount, 3); // Changed from 500ms to 800ms
  ASSERT_TRUE(lastValue.has_value());
  EXPECT_EQ(lastValue.value(), 800ms);

  // Setting downstream even higher (above upstream) should change effective to
  // upstream
  manager_.setDownstreamTimeout(1500ms);
  EXPECT_EQ(callbackCount, 4); // Changed from 800ms to 1000ms
  ASSERT_TRUE(lastValue.has_value());
  EXPECT_EQ(lastValue.value(), 1000ms);
}

// Test 3: Verify callback is NOT invoked when effective timeout doesn't change
TEST_F(MoQDeliveryTimeoutManagerTest, CallbackNotInvokedWhenNoChange) {
  int callbackCount = 0;

  manager_.setOnChangeCallback(
      [&](const std::optional<std::chrono::milliseconds>&) {
        callbackCount++;
      });

  manager_.setUpstreamTimeout(1000ms);
  manager_.setDownstreamTimeout(500ms);
  EXPECT_EQ(callbackCount, 2); // Two changes so far

  // Setting upstream to higher value shouldn't change effective (still 500ms)
  manager_.setUpstreamTimeout(2000ms);
  EXPECT_EQ(callbackCount, 2); // No new callback

  // Setting downstream to same value shouldn't trigger callback
  manager_.setDownstreamTimeout(500ms);
  EXPECT_EQ(callbackCount, 2); // No new callback
}

// Test 4: Verify behavior when only one timeout source is set
TEST_F(MoQDeliveryTimeoutManagerTest, SingleTimeoutSource) {
  int callbackCount = 0;
  std::optional<std::chrono::milliseconds> lastValue;

  manager_.setOnChangeCallback(
      [&](const std::optional<std::chrono::milliseconds>& newTimeout) {
        callbackCount++;
        lastValue = newTimeout;
      });

  // Only upstream set
  manager_.setUpstreamTimeout(1000ms);
  EXPECT_EQ(callbackCount, 1);
  ASSERT_TRUE(lastValue.has_value());
  EXPECT_EQ(lastValue.value(), 1000ms);

  auto effective = manager_.getEffectiveTimeout();
  ASSERT_TRUE(effective.has_value());
  EXPECT_EQ(effective.value(), 1000ms);
}

// Test 5: Verify behavior when no timeout sources are set
TEST_F(MoQDeliveryTimeoutManagerTest, NoTimeoutSources) {
  int callbackCount = 0;

  manager_.setOnChangeCallback(
      [&](const std::optional<std::chrono::milliseconds>&) {
        callbackCount++;
      });

  // No timeouts set - effective should be none
  auto effective = manager_.getEffectiveTimeout();
  EXPECT_FALSE(effective.has_value());

  // No callback should have been invoked
  EXPECT_EQ(callbackCount, 0);
}
