/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/mlog/MLogger.h>
#include <moxygen/mlog/MLoggerFactory.h>
#include <moxygen/mlog/SamplingMLoggerFactory.h>

#include <folly/portability/GTest.h>
#include <memory>

namespace moxygen {

// ---------------------------------------------------------------------------
// Minimal spy factory: counts createMLogger() calls, returns real loggers
// ---------------------------------------------------------------------------

class SpyMLoggerFactory : public MLoggerFactory {
 public:
  int callCount{0};

  std::shared_ptr<MLogger> createMLogger() override;
};

// Minimal no-op MLogger for testing — outputLogs() does nothing
class NullMLogger : public MLogger {
 public:
  explicit NullMLogger() : MLogger(VantagePoint::SERVER) {}
  void outputLogs() override {}
};

std::shared_ptr<MLogger> SpyMLoggerFactory::createMLogger() {
  ++callCount;
  return std::make_shared<NullMLogger>();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

class SamplingMLoggerFactoryTest : public ::testing::Test {
 protected:
  std::shared_ptr<SpyMLoggerFactory> inner_ =
      std::make_shared<SpyMLoggerFactory>();
};

// rate <= 0: always returns nullptr, inner never called
TEST_F(SamplingMLoggerFactoryTest, ZeroRate_AlwaysReturnsNull) {
  constexpr float kZeroRate = 0.0f;
  constexpr int kBasicTrials = 100;
  SamplingMLoggerFactory factory(inner_, kZeroRate);
  for (int i = 0; i < kBasicTrials; ++i) {
    EXPECT_EQ(factory.createMLogger(), nullptr);
  }
  EXPECT_EQ(inner_->callCount, 0);
}

// rate >= 1.0: always returns a logger, inner always called
TEST_F(SamplingMLoggerFactoryTest, FullRate_AlwaysReturnsLogger) {
  constexpr float kFullRate = 1.0f;
  constexpr int kBasicTrials = 100;
  SamplingMLoggerFactory factory(inner_, kFullRate);
  for (int i = 0; i < kBasicTrials; ++i) {
    EXPECT_NE(factory.createMLogger(), nullptr);
  }
  EXPECT_EQ(inner_->callCount, kBasicTrials);
}

} // namespace moxygen
