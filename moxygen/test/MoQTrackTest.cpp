// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "moxygen/moqtest/Types.h"
#include "moxygen/moqtest/Utils.h"

namespace {

class MoQTrackTest : public testing::Test {
 public:
 protected:
  MoQTrackTest() {}

  moxygen::MoQTestParameters track_;
};

} // namespace

TEST_F(MoQTrackTest, testValidateTrackNamespaceAsDefault) {
  EXPECT_NO_THROW(moxygen::validateMoQTestParameters(&track_));
}

TEST_F(MoQTrackTest, testInvalidForwardPreference) {
  track_.forwardingPreference = moxygen::ForwardingPreference(4);
  auto validateResult = moxygen::validateMoQTestParameters(&track_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testStartGroupGreaterThanLastGroup) {
  track_.startGroup = 4;
  track_.lastGroupInTrack = 3;
  auto validateResult = moxygen::validateMoQTestParameters(&track_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testStartObjectGreaterThanLastObject) {
  track_.startObject = 4;
  track_.lastObjectInTrack = 3;
  auto validateResult = moxygen::validateMoQTestParameters(&track_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testLastGroupGreaterThanAllowedMaximum) {
  track_.lastGroupInTrack = static_cast<uint64_t>(pow(2, 62));
  auto validateResult = moxygen::validateMoQTestParameters(&track_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testLastObjectGreaterThanAllowedMaximum) {
  track_.lastObjectInTrack = 10000;
  auto validateResult = moxygen::validateMoQTestParameters(&track_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testGroupIncrementAsZero) {
  track_.groupIncrement = 0;
  auto validateResult = moxygen::validateMoQTestParameters(&track_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testObjectIncrementAsZero) {
  track_.objectIncrement = 0;
  auto validateResult = moxygen::validateMoQTestParameters(&track_);
  EXPECT_TRUE(validateResult.hasError());
}
