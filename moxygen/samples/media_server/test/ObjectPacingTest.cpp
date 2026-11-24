/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/PublishLoop.h>
#include <moxygen/samples/media_server/sources/Fmp4MediaSource.h>

#include <folly/portability/GTest.h>

#include <chrono>

using namespace std::chrono_literals;

namespace moxygen::media_server::test {
namespace {

using Clock = std::chrono::steady_clock;

const Clock::time_point kWindowStart = Clock::time_point{} + 100s;

MediaObject pacedObject(
    Clock::time_point windowStart,
    uint64_t index,
    size_t count,
    std::chrono::milliseconds publishDelay = 0ms) {
  MediaObject object;
  object.publishAt = pacedReleaseTime(windowStart, 1s, index, count);
  object.publishDelay = publishDelay;
  return object;
}

} // namespace

TEST(ObjectPacingTest, ObjectsAreSpreadEvenlyOverTheGroup) {
  EXPECT_EQ(pacedReleaseTime(kWindowStart, 1s, 0, 30), kWindowStart);
  EXPECT_EQ(pacedReleaseTime(kWindowStart, 1s, 15, 30), kWindowStart + 500ms);
  EXPECT_EQ(
      pacedReleaseTime(kWindowStart, 1s, 29, 30), kWindowStart + 966666666ns);
}

TEST(ObjectPacingTest, ATrackOpenedMidwayStaysOnTheSharedWindow) {
  // Audio opened with the window; video opened 500ms into it. Both are paced
  // from the window start, not from when their subgroup opened.
  const auto audioOpened = kWindowStart;
  const auto videoOpened = kWindowStart + 500ms;
  for (uint64_t index = 0; index < 30; ++index) {
    const auto expected = pacedReleaseTime(kWindowStart, 1s, index, 30);
    EXPECT_EQ(
        objectReleaseTime(audioOpened, pacedObject(kWindowStart, index, 30)),
        expected);
    EXPECT_EQ(
        objectReleaseTime(videoOpened, pacedObject(kWindowStart, index, 30)),
        expected);
  }
}

TEST(ObjectPacingTest, ObjectsAlreadyDueWhenATrackOpensLateGoOutAtOnce) {
  const auto videoOpened = kWindowStart + 500ms;
  // Object 10 of 30 was due at 333ms: its release time is already past, so
  // the publish loop does not wait for it.
  EXPECT_LT(
      objectReleaseTime(videoOpened, pacedObject(kWindowStart, 10, 30)),
      videoOpened);
  EXPECT_GT(
      objectReleaseTime(videoOpened, pacedObject(kWindowStart, 20, 30)),
      videoOpened);
}

TEST(ObjectPacingTest, AGroupEndsBeforeTheNextOneStarts) {
  // Relative to when the subgroup opened, a track opened 500ms late would
  // release its last object at 1466ms, into the next group. Anchored to the
  // window, it is due before the next window starts.
  const auto videoOpened = kWindowStart + 500ms;
  const auto nextWindowStart = kWindowStart + 1s;
  EXPECT_LT(
      objectReleaseTime(videoOpened, pacedObject(kWindowStart, 29, 30)),
      nextWindowStart);
  EXPECT_EQ(
      objectReleaseTime(nextWindowStart, pacedObject(nextWindowStart, 0, 30)),
      nextWindowStart);
}

TEST(ObjectPacingTest, AHoldDelaysPacedObjectsByItsDuration) {
  EXPECT_EQ(
      objectReleaseTime(
          kWindowStart, pacedObject(kWindowStart, 15, 30, 2000ms)),
      kWindowStart + 2500ms);
}

TEST(ObjectPacingTest, AnUnpacedObjectIsReleasedRelativeToItsSubgroup) {
  const auto opened = kWindowStart + 500ms;
  MediaObject object;
  EXPECT_EQ(objectReleaseTime(opened, object), opened);
  object.publishDelay = 2000ms;
  EXPECT_EQ(objectReleaseTime(opened, object), opened + 2000ms);
}

} // namespace moxygen::media_server::test
