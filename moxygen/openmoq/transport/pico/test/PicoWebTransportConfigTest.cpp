/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/portability/GTest.h>
#include <gmock/gmock.h>

#include <moxygen/openmoq/transport/pico/MoQPicoServerBase.h>

using namespace moxygen;
using namespace testing;

namespace {

class PicoWebTransportConfigTest : public Test {
 protected:
  void SetUp() override {}
};

// Test default configuration values
TEST_F(PicoWebTransportConfigTest, DefaultConfig) {
  PicoWebTransportConfig config;

  EXPECT_FALSE(config.enableWebTransport);
  EXPECT_TRUE(config.enableRawMoQ);
  EXPECT_EQ(config.wtEndpoint, "/moq");
  EXPECT_EQ(config.wtMaxSessions, 100u);
}

// Test custom configuration
TEST_F(PicoWebTransportConfigTest, CustomConfig) {
  PicoWebTransportConfig config;
  config.enableWebTransport = true;
  config.enableRawMoQ = false;
  config.wtEndpoint = "/custom-moq";
  config.wtMaxSessions = 50;

  EXPECT_TRUE(config.enableWebTransport);
  EXPECT_FALSE(config.enableRawMoQ);
  EXPECT_EQ(config.wtEndpoint, "/custom-moq");
  EXPECT_EQ(config.wtMaxSessions, 50u);
}

// Test both protocols enabled
TEST_F(PicoWebTransportConfigTest, BothProtocolsEnabled) {
  PicoWebTransportConfig config;
  config.enableWebTransport = true;
  config.enableRawMoQ = true;

  EXPECT_TRUE(config.enableWebTransport);
  EXPECT_TRUE(config.enableRawMoQ);
}

// Test neither protocol enabled (edge case)
TEST_F(PicoWebTransportConfigTest, NeitherProtocolEnabled) {
  PicoWebTransportConfig config;
  config.enableWebTransport = false;
  config.enableRawMoQ = false;

  // This is a valid configuration (though not useful)
  EXPECT_FALSE(config.enableWebTransport);
  EXPECT_FALSE(config.enableRawMoQ);
}

// Test endpoint path variations
TEST_F(PicoWebTransportConfigTest, EndpointPathVariations) {
  PicoWebTransportConfig config;

  // Root path
  config.wtEndpoint = "/";
  EXPECT_EQ(config.wtEndpoint, "/");

  // Nested path
  config.wtEndpoint = "/api/v1/moq";
  EXPECT_EQ(config.wtEndpoint, "/api/v1/moq");

  // Empty path (edge case)
  config.wtEndpoint = "";
  EXPECT_EQ(config.wtEndpoint, "");
}

// Test max sessions boundary values
TEST_F(PicoWebTransportConfigTest, MaxSessionsBoundaries) {
  PicoWebTransportConfig config;

  // Minimum
  config.wtMaxSessions = 1;
  EXPECT_EQ(config.wtMaxSessions, 1u);

  // Large value
  config.wtMaxSessions = 10000;
  EXPECT_EQ(config.wtMaxSessions, 10000u);

  // Zero (disable sessions - edge case)
  config.wtMaxSessions = 0;
  EXPECT_EQ(config.wtMaxSessions, 0u);
}

} // namespace
