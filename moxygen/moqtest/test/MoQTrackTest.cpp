/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
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

  // Groups 4, 6 and 8, each holding objects 3, 6 and 9, so the track skips IDs
  // at the start of the grid and between every step.
  void CreateGappyMoQTestParameters() {
    CreateDefaultMoQTestParameters();
    params_.startGroup = 4;
    params_.lastGroupInTrack = 8;
    params_.groupIncrement = 2;
    params_.startObject = 3;
    params_.lastObjectInTrack = 9;
    params_.objectsPerGroup = 3;
    params_.objectIncrement = 3;
    params_.testIntegerExtension = 1;
  }

  moxygen::MoQTestParameters params_;
  moxygen::TrackNamespace track_;
};

moxygen::Extension GroupGap(uint64_t gap) {
  return moxygen::Extension(moxygen::kPriorGroupIdGapExtensionType, gap);
}

moxygen::Extension ObjectGap(uint64_t gap) {
  return moxygen::Extension(moxygen::kPriorObjectIdGapExtensionType, gap);
}

} // namespace

// MoQTestParameters Validation Function Tests
TEST_F(MoQTrackTest, testValidateTrackNamespaceAsDefault) {
  EXPECT_NO_THROW(moxygen::validateMoQTestParameters(params_));
}

TEST_F(MoQTrackTest, testInvalidForwardPreference) {
  params_.forwardingPreference = moxygen::ForwardingPreference(4);
  auto validateResult = moxygen::validateMoQTestParameters(params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testStartGroupGreaterThanLastGroup) {
  params_.startGroup = 4;
  params_.lastGroupInTrack = 3;
  auto validateResult = moxygen::validateMoQTestParameters(params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testStartObjectGreaterThanLastObject) {
  params_.startObject = 4;
  params_.lastObjectInTrack = 3;
  auto validateResult = moxygen::validateMoQTestParameters(params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testLastGroupGreaterThanAllowedMaximum) {
  params_.lastGroupInTrack = static_cast<uint64_t>(pow(2, 62));
  auto validateResult = moxygen::validateMoQTestParameters(params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testLastObjectGreaterThanAllowedMaximum) {
  params_.lastObjectInTrack = 10000;
  auto validateResult = moxygen::validateMoQTestParameters(params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testGroupIncrementAsZero) {
  params_.groupIncrement = 0;
  auto validateResult = moxygen::validateMoQTestParameters(params_);
  EXPECT_TRUE(validateResult.hasError());
}

TEST_F(MoQTrackTest, testObjectIncrementAsZero) {
  params_.objectIncrement = 0;
  auto validateResult = moxygen::validateMoQTestParameters(params_);
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
  auto track = moxygen::convertMoqTestParamToTrackNamespace(params_);
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
  auto track = moxygen::convertMoqTestParamToTrackNamespace(params_);
  EXPECT_TRUE(track.hasError());
}

// FETCH Window Tests.  The default parameters describe groups 0-10, each
// carrying objects 0 and 1.
TEST_F(MoQTrackTest, testFetchWindowWithoutARequestCoversTheWholeTrack) {
  CreateDefaultMoQTestParameters();
  auto window = moxygen::resolveFetchWindow(params_);
  ASSERT_FALSE(window.empty());
  EXPECT_EQ(window.first, moxygen::kLocationMin);
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{10, 1}));
  EXPECT_TRUE(window.endOfTrack);
}

TEST_F(MoQTrackTest, testDefaultFetchWindowIsEmpty) {
  // The generators bound their loops on the window, so the default must not
  // select object {0, 0}.
  moxygen::MoQTestFetchWindow window;
  EXPECT_TRUE(window.empty());
}

TEST_F(MoQTrackTest, testFetchWindowNarrowsToTheRequestedRange) {
  CreateDefaultMoQTestParameters();
  // End object 1 is exclusive, so the window stops at object 0 of group 5.
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({2, 1}, {5, 1}));
  ASSERT_FALSE(window.empty());
  EXPECT_EQ(window.first, (moxygen::AbsoluteLocation{2, 1}));
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{5, 0}));
  EXPECT_FALSE(window.endOfTrack);
  // Groups between the boundaries still carry the track's full object range.
  EXPECT_EQ(window.firstObjectIn(3), 0);
  EXPECT_EQ(window.lastObjectIn(3), 1);
}

TEST_F(MoQTrackTest, testFetchWindowInsideASingleGroup) {
  CreateDefaultMoQTestParameters();
  params_.objectsPerGroup = 5;
  params_.lastObjectInTrack = 5;
  // Both boundaries land in group 2, so firstObjectIn and lastObjectIn have to
  // narrow the same group from opposite ends.
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({2, 1}, {2, 4}));
  ASSERT_FALSE(window.empty());
  EXPECT_EQ(window.first, (moxygen::AbsoluteLocation{2, 1}));
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{2, 3}));
  EXPECT_EQ(window.firstObjectIn(2), 1);
  EXPECT_EQ(window.lastObjectIn(2), 3);
}

TEST_F(MoQTrackTest, testFetchWindowWithEndObjectZeroTakesTheWholeEndGroup) {
  CreateDefaultMoQTestParameters();
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({1, 0}, {3, 0}));
  ASSERT_FALSE(window.empty());
  EXPECT_EQ(window.first, (moxygen::AbsoluteLocation{1, 0}));
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{3, 1}));
}

TEST_F(MoQTrackTest, testFetchWindowIsEmptyPastTheEndOfTheTrack) {
  CreateDefaultMoQTestParameters();
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({20, 0}, {30, 0}));
  EXPECT_TRUE(window.empty());
}

TEST_F(MoQTrackTest, testFetchWindowIsEmptyBeforeTheStartOfTheTrack) {
  CreateDefaultMoQTestParameters();
  params_.startGroup = 5;
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({0, 0}, {3, 0}));
  EXPECT_TRUE(window.empty());
}

TEST_F(MoQTrackTest, testFetchWindowStartRollsForwardPastAGroupsLastObject) {
  CreateDefaultMoQTestParameters();
  // Group 2 has no object 5, so the window opens on the whole of group 3.
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({2, 5}, {6, 0}));
  ASSERT_FALSE(window.empty());
  EXPECT_EQ(window.first, (moxygen::AbsoluteLocation{3, 0}));
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{6, 1}));
}

TEST_F(MoQTrackTest, testFetchWindowEndRollsBackBeforeAGroupsFirstObject) {
  CreateDefaultMoQTestParameters();
  // Objects start at 2, so an end of {4, 2} is exclusive of every object group
  // 4 carries and the window closes on the whole of group 3.
  params_.startObject = 2;
  params_.objectsPerGroup = 2;
  params_.lastObjectInTrack = 4;
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({1, 2}, {4, 2}));
  ASSERT_FALSE(window.empty());
  EXPECT_EQ(window.first, (moxygen::AbsoluteLocation{1, 2}));
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{3, 4}));
}

TEST_F(MoQTrackTest, testFetchWindowSnapsOntoTheObjectIncrementGrid) {
  CreateDefaultMoQTestParameters();
  // Objects 0, 2, 4 and 6 of each group.
  params_.objectsPerGroup = 3;
  params_.objectIncrement = 2;
  params_.lastObjectInTrack = 6;
  params_.lastGroupInTrack = 4;
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({1, 1}, {2, 4}));
  ASSERT_FALSE(window.empty());
  EXPECT_EQ(window.first, (moxygen::AbsoluteLocation{1, 2}));
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{2, 2}));
}

TEST_F(MoQTrackTest, testFetchWindowSnapsOntoTheGroupIncrementGrid) {
  CreateDefaultMoQTestParameters();
  // Groups 0, 2, 4, 6, 8 and 10, each carrying objects 0, 2, 4 and 6.
  params_.objectsPerGroup = 3;
  params_.objectIncrement = 2;
  params_.lastObjectInTrack = 6;
  params_.groupIncrement = 2;
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({3, 1}, {7, 4}));
  ASSERT_FALSE(window.empty());
  // Groups 3 and 7 aren't generated, so the window covers 4 through 6 whole.
  EXPECT_EQ(window.first, (moxygen::AbsoluteLocation{4, 0}));
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{6, 6}));
  EXPECT_FALSE(window.endOfTrack);
}

TEST_F(MoQTrackTest, testWholeTrackFetchResolvesToEveryObject) {
  CreateDefaultMoQTestParameters();
  params_.groupIncrement = 3;
  auto window =
      moxygen::resolveFetchWindow(params_, moxygen::wholeTrackFetch(params_));
  ASSERT_FALSE(window.empty());
  EXPECT_EQ(window.first, moxygen::kLocationMin);
  // Group 10 isn't generated, so the track really ends on group 9.
  EXPECT_EQ(window.last, (moxygen::AbsoluteLocation{9, 1}));
  EXPECT_TRUE(window.endOfTrack);
}

TEST_F(MoQTrackTest, testExpectedObjectsCoverTheWindowInclusive) {
  CreateDefaultMoQTestParameters();
  auto window = moxygen::resolveFetchWindow(
      params_, moxygen::StandaloneFetch({2, 1}, {4, 1}));
  const std::set<std::pair<uint64_t, uint64_t>> expected{
      {2, 1}, {3, 0}, {3, 1}, {4, 0}};
  EXPECT_EQ(moxygen::expectedObjectsIn(params_, window), expected);
}

TEST_F(MoQTrackTest, testExpectedObjectsAreEmptyForAnEmptyWindow) {
  CreateDefaultMoQTestParameters();
  EXPECT_TRUE(
      moxygen::expectedObjectsIn(params_, moxygen::MoQTestFetchWindow{})
          .empty());
}

TEST_F(MoQTrackTest, testFetchEndLocationEchoesARequestInsideTheTrack) {
  CreateDefaultMoQTestParameters();
  EXPECT_EQ(
      moxygen::fetchEndLocation(
          params_, moxygen::StandaloneFetch({1, 0}, {4, 1})),
      (moxygen::AbsoluteLocation{4, 1}));
}

TEST_F(MoQTrackTest, testFetchEndLocationClampsToTheEndOfTheTrack) {
  CreateDefaultMoQTestParameters();
  // Groups 0-10 carrying objects 0 and 1, so the track stops at {10, 2}.
  EXPECT_EQ(
      moxygen::fetchEndLocation(
          params_, moxygen::StandaloneFetch({1, 0}, {30, 0})),
      (moxygen::AbsoluteLocation{10, 2}));
  // An end object of 0 asks for the whole of group 10, which also overshoots.
  EXPECT_EQ(
      moxygen::fetchEndLocation(
          params_, moxygen::StandaloneFetch({1, 0}, {10, 0})),
      (moxygen::AbsoluteLocation{10, 2}));
}

TEST_F(MoQTrackTest, testFetchEndLocationNeverPrecedesTheRequestedStart) {
  CreateDefaultMoQTestParameters();
  // A receiver must close the session over an End Location below the start, so
  // a request wholly past the track reports a zero-length range instead.
  EXPECT_EQ(
      moxygen::fetchEndLocation(
          params_, moxygen::StandaloneFetch({50, 0}, {60, 0})),
      (moxygen::AbsoluteLocation{50, 0}));
}

TEST_F(MoQTrackTest, testFetchEndLocationIsEmptyBeforeTheStartOfTheTrack) {
  CreateDefaultMoQTestParameters();
  params_.startGroup = 5;
  const moxygen::StandaloneFetch fetch({0, 0}, {3, 0});
  // Nothing is delivered, so the End Location has to say so rather than report
  // an end the track never reaches.
  EXPECT_TRUE(moxygen::resolveFetchWindow(params_, fetch).empty());
  EXPECT_EQ(
      moxygen::fetchEndLocation(params_, fetch),
      (moxygen::AbsoluteLocation{0, 0}));
}

TEST_F(MoQTrackTest, testParseLocation) {
  auto parsed = moxygen::parseLocation("4,7");
  ASSERT_TRUE(parsed.hasValue());
  EXPECT_EQ(parsed.value(), (moxygen::AbsoluteLocation{4, 7}));

  EXPECT_TRUE(moxygen::parseLocation("4").hasError());
  EXPECT_TRUE(moxygen::parseLocation("4,7,9").hasError());
  EXPECT_TRUE(moxygen::parseLocation("four,7").hasError());
  EXPECT_TRUE(moxygen::parseLocation("-1,7").hasError());
  EXPECT_TRUE(moxygen::parseLocation("").hasError());
}

// Gap extension tests
TEST_F(MoQTrackTest, testADenseTrackHasNoGaps) {
  CreateDefaultMoQTestParameters();
  params_.testIntegerExtension = 1;
  EXPECT_EQ(moxygen::priorGroupGap(params_, 0), 0);
  EXPECT_EQ(moxygen::priorGroupGap(params_, 7), 0);
  EXPECT_EQ(moxygen::priorObjectGap(params_, 0), 0);
  EXPECT_EQ(moxygen::priorObjectGap(params_, 1), 0);
  EXPECT_TRUE(moxygen::getGapExtensions(params_, 7, 1).empty());
}

TEST_F(MoQTrackTest, testAnIncrementIsAGap) {
  CreateDefaultMoQTestParameters();
  params_.groupIncrement = 3;
  params_.objectIncrement = 2;
  // The first group and object of a grid that starts at 0 skip nothing.
  EXPECT_EQ(moxygen::priorGroupGap(params_, 0), 0);
  EXPECT_EQ(moxygen::priorObjectGap(params_, 0), 0);
  EXPECT_EQ(moxygen::priorGroupGap(params_, 3), 2);
  EXPECT_EQ(moxygen::priorObjectGap(params_, 2), 1);
}

TEST_F(MoQTrackTest, testALeadingOffsetIsAGap) {
  CreateDefaultMoQTestParameters();
  params_.startGroup = 5;
  params_.lastGroupInTrack = 7;
  params_.startObject = 2;
  params_.lastObjectInTrack = 4;
  params_.objectsPerGroup = 4;
  // Groups below startGroup and objects below startObject never exist, so the
  // offset is a real gap even with an increment of 1.
  EXPECT_EQ(moxygen::priorGroupGap(params_, 5), 5);
  EXPECT_EQ(moxygen::priorGroupGap(params_, 6), 0);
  EXPECT_EQ(moxygen::priorObjectGap(params_, 2), 2);
  EXPECT_EQ(moxygen::priorObjectGap(params_, 3), 0);
}

TEST_F(MoQTrackTest, testGapExtensionsCarryTheGroupGapOnlyOnTheFirstObject) {
  CreateGappyMoQTestParameters();
  const std::vector<moxygen::Extension> firstObjectOfFirstGroup{
      GroupGap(4), ObjectGap(3)};
  const std::vector<moxygen::Extension> firstObjectOfLaterGroup{
      GroupGap(1), ObjectGap(3)};
  const std::vector<moxygen::Extension> laterObject{ObjectGap(2)};
  EXPECT_EQ(moxygen::getGapExtensions(params_, 4, 3), firstObjectOfFirstGroup);
  EXPECT_EQ(moxygen::getGapExtensions(params_, 4, 6), laterObject);
  EXPECT_EQ(moxygen::getGapExtensions(params_, 6, 3), firstObjectOfLaterGroup);
  EXPECT_EQ(moxygen::getGapExtensions(params_, 6, 9), laterObject);
}

TEST_F(MoQTrackTest, testGapExtensionsNeedADeclaredTestExtension) {
  CreateGappyMoQTestParameters();
  params_.testIntegerExtension = -1;
  params_.testVariableExtension = -1;
  EXPECT_TRUE(moxygen::getGapExtensions(params_, 4, 3).empty());

  // Either declaration is enough: one extension already puts the extension bit
  // in the track's headers.
  params_.testVariableExtension = 1;
  EXPECT_FALSE(moxygen::getGapExtensions(params_, 4, 3).empty());
}

TEST_F(MoQTrackTest, testValidateGapExtensionsAcceptsTheExactSet) {
  CreateGappyMoQTestParameters();
  moxygen::Extensions extensions(
      moxygen::getExtensions(params_.testIntegerExtension, -1),
      {GroupGap(4), ObjectGap(3)});
  EXPECT_TRUE(moxygen::validateGapExtensions(extensions, params_, 4, 3));
}

TEST_F(MoQTrackTest, testValidateGapExtensionsRejectsAnythingElse) {
  CreateGappyMoQTestParameters();
  const moxygen::Extensions missing({}, {GroupGap(4)});
  const moxygen::Extensions extra(
      {}, {GroupGap(4), ObjectGap(3), ObjectGap(3)});
  const moxygen::Extensions wrongValue({}, {GroupGap(3), ObjectGap(3)});
  const moxygen::Extensions mutableSection({GroupGap(4), ObjectGap(3)}, {});
  EXPECT_FALSE(moxygen::validateGapExtensions(missing, params_, 4, 3));
  EXPECT_FALSE(moxygen::validateGapExtensions(extra, params_, 4, 3));
  EXPECT_FALSE(moxygen::validateGapExtensions(wrongValue, params_, 4, 3));
  EXPECT_FALSE(moxygen::validateGapExtensions(mutableSection, params_, 4, 3));
}

TEST_F(MoQTrackTest, testFetchForwardingPreferenceOnlyRemapsDatagram) {
  using moxygen::ForwardingPreference;
  // A datagram object carries no subgroup, so over a fetch stream the track is
  // shaped like one subgroup per group.  Everything else is unchanged.
  EXPECT_EQ(
      moxygen::fetchForwardingPreference(ForwardingPreference::DATAGRAM),
      ForwardingPreference::ONE_SUBGROUP_PER_GROUP);
  for (auto preference :
       {ForwardingPreference::ONE_SUBGROUP_PER_GROUP,
        ForwardingPreference::ONE_SUBGROUP_PER_OBJECT,
        ForwardingPreference::TWO_SUBGROUPS_PER_GROUP}) {
    EXPECT_EQ(moxygen::fetchForwardingPreference(preference), preference);
  }
}
