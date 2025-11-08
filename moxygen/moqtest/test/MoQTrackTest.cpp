/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <folly/portability/GTest.h>
#include "moxygen/moqtest/Types.h"
#include "moxygen/moqtest/Utils.h"

namespace {

class MoQTrackTest : public testing::Test {
 public:
  void CreatDefaultTrackNamespace() {
    track_.trackNamespace = {
        "moq-test-00",
        "0",
        "0",
        "0",
        "10",
        "1",
        "1",
        "1",
        "1",
        "1",
        "1",
        "1",
        "0",
        "-1",
        "-1",
        "0"};
  }

  void CreateDefaultMoQTestParameters() {
    params_.forwardingPreference = moxygen::ForwardingPreference(0);
    params_.startGroup = 0;
    params_.startObject = 0;
    params_.lastGroupInTrack = 10;
    params_.lastObjectInTrack = 1;
    params_.objectsPerGroup = 1;
    params_.sizeOfObjectZero = 1;
    params_.sizeOfObjectGreaterThanZero = 1;
    params_.objectFrequency = 1;
    params_.groupIncrement = 1;
    params_.objectIncrement = 1;
    params_.sendEndOfGroupMarkers = false;
    params_.testIntegerExtension = -1;
    params_.testVariableExtension = -1;
    params_.publisherDeliveryTimeout = 0;
  }

  moxygen::MoQTestParameters params_;
  moxygen::TrackNamespace track_;
};

} // namespace

// MoQTestParameters Validation Function Tests
TEST_F(MoQTrackTest, testValidateTrackNamespaceAsDefault) {
  EXPECT_NO_THROW(moxygen::validateMoQTestParameters(&params_));
}

TEST_F(MoQTrackTest, testInvalidForwardPreference) {
  params_.forwardingPreference = moxygen::ForwardingPreference(4);
  auto validateResult = moxygen::validateMoQTestParameters(&params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testStartGroupGreaterThanLastGroup) {
  params_.startGroup = 4;
  params_.lastGroupInTrack = 3;
  auto validateResult = moxygen::validateMoQTestParameters(&params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testStartObjectGreaterThanLastObject) {
  params_.startObject = 4;
  params_.lastObjectInTrack = 3;
  auto validateResult = moxygen::validateMoQTestParameters(&params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testLastGroupGreaterThanAllowedMaximum) {
  params_.lastGroupInTrack = static_cast<uint64_t>(pow(2, 62));
  auto validateResult = moxygen::validateMoQTestParameters(&params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testLastObjectGreaterThanAllowedMaximum) {
  params_.lastObjectInTrack = 10000;
  auto validateResult = moxygen::validateMoQTestParameters(&params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testGroupIncrementAsZero) {
  params_.groupIncrement = 0;
  auto validateResult = moxygen::validateMoQTestParameters(&params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testObjectIncrementAsZero) {
  params_.objectIncrement = 0;
  auto validateResult = moxygen::validateMoQTestParameters(&params_);
  EXPECT_TRUE(validateResult.hasError());
}

// Convert TrackNamespace to MoQTestParameters Tests
TEST_F(MoQTrackTest, testConvertTrackNamespaceToMoQTestParameters) {
  CreatDefaultTrackNamespace();
  auto params = moxygen::convertTrackNamespaceToMoqTestParam(&track_);
  ASSERT_FALSE(params.hasError());
  EXPECT_EQ(
      params.value().forwardingPreference, moxygen::ForwardingPreference(0));
  EXPECT_EQ(params.value().startGroup, 0);
  EXPECT_EQ(params.value().startObject, 0);
  EXPECT_EQ(params.value().lastGroupInTrack, 10);
  EXPECT_EQ(params.value().lastObjectInTrack, 1);
  EXPECT_EQ(params.value().objectsPerGroup, 1);
  EXPECT_EQ(params.value().sizeOfObjectZero, 1);
  EXPECT_EQ(params.value().sizeOfObjectGreaterThanZero, 1);
  EXPECT_EQ(params.value().objectFrequency, 1);
  EXPECT_EQ(params.value().groupIncrement, 1);
  EXPECT_EQ(params.value().objectIncrement, 1);
  EXPECT_EQ(params.value().sendEndOfGroupMarkers, false);
  EXPECT_EQ(params.value().testIntegerExtension, -1);
  EXPECT_EQ(params.value().testVariableExtension, -1);
  EXPECT_EQ(params.value().publisherDeliveryTimeout, 0);
}

TEST_F(
    MoQTrackTest,
    testConvertTrackNamespaceToMoQTestParametersWithInvalidProtocol) {
  CreatDefaultTrackNamespace();
  track_.trackNamespace[0] = "moq-test-01";
  auto params = moxygen::convertTrackNamespaceToMoqTestParam(&track_);
  EXPECT_TRUE(params.hasError());
}

TEST_F(MoQTrackTest, testConversionGivenTrackNamespaceWithInvalidLength) {
  CreatDefaultTrackNamespace();
  track_.trackNamespace.resize(15);
  auto params = moxygen::convertTrackNamespaceToMoqTestParam(&track_);
  EXPECT_TRUE(params.hasError());
}

TEST_F(MoQTrackTest, testConversionWithInvalidEndParams) {
  CreatDefaultTrackNamespace();
  track_.trackNamespace[1] = "4";
  auto params = moxygen::convertTrackNamespaceToMoqTestParam(&track_);
  EXPECT_TRUE(params.hasError());
}

TEST_F(MoQTrackTest, testConversionWithTrackNamespaceHavingNonDigitValues) {
  CreatDefaultTrackNamespace();
  track_.trackNamespace[1] = "a";
  auto params = moxygen::convertTrackNamespaceToMoqTestParam(&track_);
  EXPECT_TRUE(params.hasError());
}

// Test Conversion of MoQTestParameters to TrackNamespace
TEST_F(MoQTrackTest, testConvertMoQTestParametersToTrackNamespace) {
  CreateDefaultMoQTestParameters();
  auto track = moxygen::convertMoqTestParamToTrackNamespace(&params_);
  ASSERT_FALSE(track.hasError());
  EXPECT_EQ(track.value().trackNamespace.size(), 16);
  EXPECT_EQ(track.value().trackNamespace[0], "moq-test-00");
  EXPECT_EQ(track.value().trackNamespace[1], "0");
  EXPECT_EQ(track.value().trackNamespace[2], "0");
  EXPECT_EQ(track.value().trackNamespace[3], "0");
  EXPECT_EQ(track.value().trackNamespace[4], "10");
  EXPECT_EQ(track.value().trackNamespace[5], "1");
  EXPECT_EQ(track.value().trackNamespace[6], "1");
  EXPECT_EQ(track.value().trackNamespace[7], "1");
  EXPECT_EQ(track.value().trackNamespace[8], "1");
  EXPECT_EQ(track.value().trackNamespace[9], "1");
  EXPECT_EQ(track.value().trackNamespace[10], "1");
  EXPECT_EQ(track.value().trackNamespace[11], "1");
  EXPECT_EQ(track.value().trackNamespace[12], "0");
  EXPECT_EQ(track.value().trackNamespace[13], "-1");
  EXPECT_EQ(track.value().trackNamespace[14], "-1");
  EXPECT_EQ(track.value().trackNamespace[15], "0");
}

TEST_F(
    MoQTrackTest,
    testConvertMoQTestParametersToTrackNamespaceWithInvalidParams) {
  CreateDefaultMoQTestParameters();
  params_.lastObjectInTrack = 2;
  auto track = moxygen::convertMoqTestParamToTrackNamespace(&params_);
  EXPECT_TRUE(track.hasError());
}
