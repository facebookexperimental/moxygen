/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/mlog/MLogger.h>
#include <moxygen/mlog/MLoggerFactory.h>
#include <moxygen/mlog/SamplingMLoggerFactory.h>

#include <folly/portability/GTest.h>
#include <limits>
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
  // Large enough that the tolerances below sit several standard deviations
  // away from the expected rate, so the statistical tests do not flake.
  static constexpr int kSamplingTrials = 20000;

  // Fraction of createMLogger() calls that produced a logger.
  double measureRate(float sampleRate) {
    SamplingMLoggerFactory factory(inner_, sampleRate);
    int logged = 0;
    for (int i = 0; i < kSamplingTrials; ++i) {
      if (factory.createMLogger() != nullptr) {
        ++logged;
      }
    }
    return static_cast<double>(logged) / kSamplingTrials;
  }

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

// A rate whose reciprocal is not an integer is still honored.
TEST_F(SamplingMLoggerFactoryTest, NonReciprocalRate_MatchesRequestedRate) {
  EXPECT_NEAR(measureRate(0.7f), 0.7, 0.03);
}

// The low rates this factory exists to serve, e.g. logging 1% of sessions.
TEST_F(SamplingMLoggerFactoryTest, LowRate_MatchesRequestedRate) {
  EXPECT_NEAR(measureRate(0.01f), 0.01, 0.005);
}

// Rates outside [0, 1] saturate rather than wrapping or aborting.
TEST_F(SamplingMLoggerFactoryTest, OutOfRangeRates_Saturate) {
  EXPECT_EQ(measureRate(-0.5f), 0.0);
  EXPECT_EQ(measureRate(1.5f), 1.0);
}

// NaN is not a usable rate; it must disable logging, not sample arbitrarily.
TEST_F(SamplingMLoggerFactoryTest, NanRate_NeverLogs) {
  EXPECT_EQ(measureRate(std::numeric_limits<float>::quiet_NaN()), 0.0);
}

} // namespace moxygen
