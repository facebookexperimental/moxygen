// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include <gtest/gtest.h>
#include "moxygen/MoQFramer.h"
#include "moxygen/tools/moqperf/MoQPerfParams.h"
#include "moxygen/tools/moqperf/MoQPerfUtils.h"

namespace {

class MoQPerfTest : public testing::Test {
 public:
  moxygen::MoQPerfParams CreateDefaultMoQPerfParameters() {
    return moxygen::MoQPerfParams{
        .numObjectsPerSubgroup = 1,
        .numSubgroupsPerGroup = 10,
        .objectSize = 100,
        .sendEndOfGroupMarkers = false,
        .request = moxygen::RequestType(0),
    };
  }

  moxygen::TrackNamespace CreatDefaultTrackNamespace() {
    moxygen::TrackNamespace track;
    track.trackNamespace = {"1", "10", "100", "0", "0"};
    return track;
  }
};

} // namespace

TEST_F(MoQPerfTest, TestConvertMoQPerfParamsToTrackNamespace) {
  moxygen::MoQPerfParams params = CreateDefaultMoQPerfParameters();
  auto track = moxygen::convertMoQPerfParamsToTrackNamespace(params);
  std::vector<std::string> expected = {"1", "10", "100", "0", "0"};
  EXPECT_EQ(track.trackNamespace, expected);
}

TEST_F(MoQPerfTest, TestConvertTrackNamespaceToMoQPerfParams) {
  moxygen::TrackNamespace track = CreatDefaultTrackNamespace();
  auto params = moxygen::convertTrackNamespaceToMoQPerfParams(track);
  EXPECT_EQ(params.numObjectsPerSubgroup, 1);
  EXPECT_EQ(params.numSubgroupsPerGroup, 10);
  EXPECT_EQ(params.objectSize, 100);
  EXPECT_EQ(params.sendEndOfGroupMarkers, false);
  EXPECT_EQ(params.request, moxygen::RequestType(0));
}

TEST_F(MoQPerfTest, TestValidateMoQPerfParamsWithValidParams) {
  moxygen::MoQPerfParams params = CreateDefaultMoQPerfParameters();
  EXPECT_TRUE(moxygen::validateMoQPerfParams(params));
}

TEST_F(MoQPerfTest, TestValidateMoQPerfParamsWithInvalidParams) {
  moxygen::MoQPerfParams params = CreateDefaultMoQPerfParameters();
  params.numObjectsPerSubgroup = 0;
  EXPECT_FALSE(moxygen::validateMoQPerfParams(params));
  params.numObjectsPerSubgroup = 1;
  params.numSubgroupsPerGroup = 0;
  EXPECT_FALSE(moxygen::validateMoQPerfParams(params));
  params.numSubgroupsPerGroup = 1;
  params.objectSize = 0;
  EXPECT_FALSE(moxygen::validateMoQPerfParams(params));
  params.objectSize = 1;
  params.request = moxygen::RequestType(2);
  EXPECT_FALSE(moxygen::validateMoQPerfParams(params));
}
