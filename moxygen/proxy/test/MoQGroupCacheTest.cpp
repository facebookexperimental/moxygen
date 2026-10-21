/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQGroupCache.h"

#include <folly/portability/GTest.h>

namespace moxygen::test {
namespace {

using Coverage = MoQGroupCache::Coverage;
using Result = MoQGroupCache::Result;

constexpr size_t kLargeBudget = 1 << 20;

Payload bytes(size_t length) {
  return length == 0 ? nullptr
                     : folly::IOBuf::copyBuffer(std::string(length, 'x'));
}

std::shared_ptr<MoQCacheBudget> makeBudget(size_t capacity = kLargeBudget) {
  return std::make_shared<MoQCacheBudget>(capacity);
}

// Records objects [0, count) of a group on one subgroup, optionally proving
// the group's end with the header flag and a clean FIN.
void recordGroup(
    MoQGroupCache& cache,
    uint64_t group,
    uint64_t count,
    bool finishWithEnd = true) {
  ASSERT_TRUE(
      cache.openSubgroup(group, 0, /*containsLastInGroup=*/finishWithEnd));
  for (uint64_t objectID = 0; objectID < count; objectID++) {
    ASSERT_EQ(
        cache.subgroupObject(
            group,
            0,
            objectID,
            ObjectStatus::NORMAL,
            bytes(10),
            noExtensions()),
        Result::Recorded);
  }
  if (finishWithEnd) {
    ASSERT_EQ(cache.endSubgroup(group, 0, /*cleanFin=*/true), Result::Recorded);
  }
}

Coverage wholeGroups(
    const MoQGroupCache& cache,
    uint64_t firstGroup,
    uint64_t lastGroup) {
  return cache.inspect({firstGroup, 0}, {lastGroup + 1, 0});
}

std::vector<AbsoluteLocation> locations(const MoQReplayPlan& plan) {
  std::vector<AbsoluteLocation> result;
  result.reserve(plan.entries.size());
  for (const auto& entry : plan.entries) {
    result.push_back(entry.location);
  }
  return result;
}

Extensions withExtension(uint64_t type, uint64_t value) {
  Extensions extensions;
  extensions.insertMutableExtension(Extension{type, value});
  return extensions;
}

} // namespace

// --- Retention ---

TEST(MoQGroupCacheTest, KeepsNewestDistinctGroupIDs) {
  MoQGroupCache cache(3, makeBudget());

  recordGroup(cache, 10, 1);
  recordGroup(cache, 20, 1);
  recordGroup(cache, 30, 1);
  recordGroup(cache, 40, 1);

  EXPECT_EQ(cache.residentGroups(), 3);
  EXPECT_FALSE(cache.isCaching(10));
  EXPECT_TRUE(cache.isCaching(20));
  EXPECT_TRUE(cache.isCaching(40));
  EXPECT_EQ(cache.retiredThrough(), 10);
}

TEST(MoQGroupCacheTest, ZeroGroupsDisablesCaching) {
  MoQGroupCache cache(0, makeBudget());

  EXPECT_FALSE(cache.openSubgroup(1, 0, false));
  EXPECT_EQ(cache.inspect({1, 0}, {1, 1}), Coverage::Miss);
}

TEST(MoQGroupCacheTest, SingleGroupKeepsOnlyTheNewest) {
  MoQGroupCache cache(1, makeBudget());

  recordGroup(cache, 5, 2);
  recordGroup(cache, 6, 2);

  EXPECT_FALSE(cache.isCaching(5));
  EXPECT_EQ(wholeGroups(cache, 6, 6), Coverage::Ready);
}

TEST(MoQGroupCacheTest, RetiredGroupsCannotBeRecreated) {
  MoQGroupCache cache(2, makeBudget());
  recordGroup(cache, 10, 1);
  recordGroup(cache, 20, 1);
  recordGroup(cache, 30, 1);

  // 10 was retired; 15 is older than every resident group with no room.
  EXPECT_FALSE(cache.openSubgroup(10, 1, false));
  EXPECT_FALSE(cache.openSubgroup(15, 0, false));
  EXPECT_EQ(
      cache.standaloneObject(ObjectHeader(10, 0, 5), bytes(1), true),
      Result::NotCached);
  EXPECT_EQ(cache.residentGroups(), 2);
}

TEST(MoQGroupCacheTest, ByteRetirementAdvancesWatermarkPastEmptySlots) {
  // Room for a little more than two small groups.
  auto budget = makeBudget(1200);
  MoQGroupCache cache(3, budget);
  recordGroup(cache, 1, 1);
  recordGroup(cache, 2, 1);

  // A large object in group 3 retires groups 1 and 2 to make room, leaving
  // empty slots that late callbacks must not refill.
  ASSERT_TRUE(cache.openSubgroup(3, 0, false));
  EXPECT_EQ(
      cache.subgroupObject(
          3, 0, 0, ObjectStatus::NORMAL, bytes(600), noExtensions()),
      Result::Recorded);

  EXPECT_EQ(cache.retiredThrough(), 2);
  EXPECT_FALSE(cache.openSubgroup(2, 1, false));
  EXPECT_EQ(cache.residentGroups(), 1);
}

TEST(MoQGroupCacheTest, TracksSharingABudgetRetainIndependently) {
  auto budget = makeBudget();
  MoQGroupCache first(1, budget);
  MoQGroupCache second(1, budget);

  recordGroup(first, 7, 1);
  recordGroup(second, 7, 2);
  recordGroup(second, 8, 2);

  EXPECT_TRUE(first.isCaching(7));
  auto plan = first.snapshot({7, 0}, {8, 0}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());
  const std::vector<AbsoluteLocation> expected{{7, 0}};
  EXPECT_EQ(locations(*plan), expected);
}

// --- Accounted budget ---

TEST(MoQGroupCacheTest, OversizedObjectIsNotCachedAndHoldsNoCharge) {
  auto budget = makeBudget(1000);
  MoQGroupCache cache(3, budget);
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));

  auto result = cache.beginObject(1, 0, 0, 5000, bytes(10), noExtensions());

  EXPECT_EQ(result, Result::NotCached);
  EXPECT_FALSE(cache.isCaching(1));
  EXPECT_EQ(budget->used(), 0);
}

TEST(MoQGroupCacheTest, ZeroLengthObjectsAreCharged) {
  auto budget = makeBudget();
  MoQGroupCache cache(3, budget);
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));
  const auto afterGroup = budget->used();

  ASSERT_EQ(
      cache.subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, nullptr, noExtensions()),
      Result::Recorded);

  EXPECT_GT(budget->used(), afterGroup);
}

TEST(MoQGroupCacheTest, PartialObjectReservesDeclaredLengthUpFront) {
  auto budget = makeBudget();
  MoQGroupCache cache(3, budget);
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));

  ASSERT_EQ(
      cache.beginObject(1, 0, 0, 1000, bytes(10), noExtensions()),
      Result::Recorded);

  EXPECT_GE(budget->used(), 1000);
}

TEST(MoQGroupCacheTest, SnapshotKeepsChargesUntilReleased) {
  auto budget = makeBudget();
  auto cache = std::make_unique<MoQGroupCache>(1, budget);
  recordGroup(*cache, 1, 3);
  auto plan = cache->snapshot({1, 0}, {2, 0}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());

  recordGroup(*cache, 2, 1);
  cache.reset();

  // Group 1 was evicted and the cache destroyed, yet the plan still owns it.
  EXPECT_GE(budget->used(), 30);
  plan.reset();
  EXPECT_EQ(budget->used(), 0);
}

TEST(MoQGroupCacheTest, ReplayPlanMetadataIsCharged) {
  auto budget = makeBudget();
  MoQGroupCache cache(3, budget);
  recordGroup(cache, 1, 3);
  const auto beforePlan = budget->used();

  auto plan = cache.snapshot({1, 0}, {2, 0}, GroupOrder::OldestFirst);

  ASSERT_TRUE(plan.has_value());
  EXPECT_GT(budget->used(), beforePlan);
}

TEST(MoQGroupCacheTest, SnapshotFailsWhenPlanCannotBeCharged) {
  auto budget = makeBudget();
  MoQGroupCache cache(3, budget);
  recordGroup(cache, 1, 3);
  auto filler =
      MoQCacheCharge::reserve(budget, budget->capacity() - budget->used());
  ASSERT_TRUE(filler.has_value());

  EXPECT_FALSE(cache.snapshot({1, 0}, {2, 0}, GroupOrder::OldestFirst));
}

// --- Overload ---

TEST(MoQGroupCacheTest, OverloadIsFirstComeFirstServedWithoutFairShare) {
  // Room for the first track's object plus a little metadata.
  auto budget = makeBudget(2300);
  MoQGroupCache first(3, budget);
  MoQGroupCache second(3, budget);
  ASSERT_TRUE(first.openSubgroup(1, 0, false));
  ASSERT_EQ(
      first.subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, bytes(1500), noExtensions()),
      Result::Recorded);

  // The second track cannot evict the first one's data, so it caches nothing.
  ASSERT_TRUE(second.openSubgroup(1, 0, false));
  auto result = second.subgroupObject(
      1, 0, 0, ObjectStatus::NORMAL, bytes(1500), noExtensions());

  EXPECT_EQ(result, Result::NotCached);
  EXPECT_LE(budget->used(), budget->capacity());
  EXPECT_TRUE(first.isCaching(1));
  EXPECT_EQ(second.inspect({1, 0}, {1, 1}), Coverage::Miss);
}

// --- Group end evidence ---

TEST(MoQGroupCacheTest, EndOfGroupStatusProvesTheTail) {
  MoQGroupCache cache(3, makeBudget());
  recordGroup(cache, 1, 3, /*finishWithEnd=*/false);
  ASSERT_EQ(
      cache.subgroupObject(
          1, 0, 3, ObjectStatus::END_OF_GROUP, nullptr, noExtensions()),
      Result::Recorded);
  ASSERT_EQ(cache.endSubgroup(1, 0, /*cleanFin=*/true), Result::Recorded);

  EXPECT_EQ(wholeGroups(cache, 1, 1), Coverage::Ready);
}

TEST(MoQGroupCacheTest, LastInGroupFlagWithCleanFinProvesTheTail) {
  MoQGroupCache cache(3, makeBudget());

  recordGroup(cache, 1, 3, /*finishWithEnd=*/true);

  EXPECT_EQ(wholeGroups(cache, 1, 1), Coverage::Ready);
  auto plan = cache.snapshot({1, 0}, {2, 0}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());
  const std::vector<AbsoluteLocation> expected{{1, 0}, {1, 1}, {1, 2}};
  EXPECT_EQ(locations(*plan), expected);
}

TEST(MoQGroupCacheTest, LastInGroupFlagWithoutFinIsNotProof) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, /*containsLastInGroup=*/true));
  ASSERT_EQ(
      cache.subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, bytes(1), noExtensions()),
      Result::Recorded);

  EXPECT_EQ(wholeGroups(cache, 1, 1), Coverage::AwaitIngress);
}

TEST(MoQGroupCacheTest, ResetStreamIsNotProof) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, /*containsLastInGroup=*/true));
  ASSERT_EQ(
      cache.subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, bytes(1), noExtensions()),
      Result::Recorded);

  ASSERT_EQ(cache.endSubgroup(1, 0, /*cleanFin=*/false), Result::Recorded);

  EXPECT_EQ(wholeGroups(cache, 1, 1), Coverage::Miss);
}

TEST(MoQGroupCacheTest, FinWithoutLastInGroupFlagIsNotProof) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, /*containsLastInGroup=*/false));
  ASSERT_EQ(
      cache.subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, bytes(1), noExtensions()),
      Result::Recorded);

  ASSERT_EQ(cache.endSubgroup(1, 0, /*cleanFin=*/true), Result::Recorded);

  EXPECT_EQ(wholeGroups(cache, 1, 1), Coverage::Miss);
}

TEST(MoQGroupCacheTest, NewerGroupStartingIsNotProof) {
  MoQGroupCache cache(3, makeBudget());
  recordGroup(cache, 1, 2, /*finishWithEnd=*/false);
  ASSERT_EQ(cache.endSubgroup(1, 0, /*cleanFin=*/true), Result::Recorded);

  recordGroup(cache, 2, 1);

  EXPECT_EQ(cache.inspect({1, 0}, {2, 1}), Coverage::Miss);
}

TEST(
    MoQGroupCacheTest,
    TailEvidenceDoesNotHideAMissingObjectOnAnotherSubgroup) {
  MoQGroupCache cache(3, makeBudget());
  // Subgroup 0 carries the even objects and the group's end.
  ASSERT_TRUE(cache.openSubgroup(1, 0, /*containsLastInGroup=*/true));
  ASSERT_TRUE(cache.openSubgroup(1, 1, /*containsLastInGroup=*/false));
  for (uint64_t objectID : {0, 2}) {
    ASSERT_EQ(
        cache.subgroupObject(
            1, 0, objectID, ObjectStatus::NORMAL, bytes(1), noExtensions()),
        Result::Recorded);
  }
  ASSERT_EQ(cache.endSubgroup(1, 0, /*cleanFin=*/true), Result::Recorded);

  // Object 1 is still due on subgroup 1.
  EXPECT_EQ(wholeGroups(cache, 1, 1), Coverage::AwaitIngress);
  ASSERT_EQ(cache.endSubgroup(1, 1, /*cleanFin=*/false), Result::Recorded);
  EXPECT_EQ(wholeGroups(cache, 1, 1), Coverage::Miss);
}

// --- Coverage ---

TEST(MoQGroupCacheTest, CompletePrefixOfActiveGroupIsReady) {
  MoQGroupCache cache(3, makeBudget());
  recordGroup(cache, 5, 3, /*finishWithEnd=*/false);

  EXPECT_EQ(cache.inspect({5, 0}, {5, 3}), Coverage::Ready);
}

TEST(MoQGroupCacheTest, PartiallyReceivedObjectAwaitsThenCompletes) {
  MoQGroupCache cache(3, makeBudget());
  recordGroup(cache, 5, 2, /*finishWithEnd=*/false);
  ASSERT_EQ(
      cache.beginObject(5, 0, 2, 20, bytes(5), noExtensions()),
      Result::Recorded);
  EXPECT_EQ(cache.inspect({5, 0}, {5, 3}), Coverage::AwaitIngress);

  ASSERT_EQ(cache.objectPayload(5, 0, bytes(15)), Result::Recorded);

  EXPECT_EQ(cache.inspect({5, 0}, {5, 3}), Coverage::Ready);
  auto plan = cache.snapshot({5, 2}, {5, 3}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());
  ASSERT_EQ(plan->entries.size(), 1);
  EXPECT_EQ(plan->entries[0].object->payload->computeChainDataLength(), 20);
}

TEST(MoQGroupCacheTest, SubgroupHeaderBeforeFirstObjectAwaits) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(6, 0, false));

  EXPECT_EQ(cache.inspect({6, 0}, {6, 1}), Coverage::AwaitIngress);
}

TEST(MoQGroupCacheTest, MissingObjectPassedByEveryStreamIsAMiss) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));
  for (uint64_t objectID : {0, 2}) {
    ASSERT_EQ(
        cache.subgroupObject(
            1, 0, objectID, ObjectStatus::NORMAL, bytes(1), noExtensions()),
        Result::Recorded);
  }

  EXPECT_EQ(cache.inspect({1, 0}, {1, 3}), Coverage::Miss);
}

TEST(MoQGroupCacheTest, PriorObjectGapProvesNonexistence) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));
  ASSERT_EQ(
      cache.subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, bytes(1), noExtensions()),
      Result::Recorded);
  ASSERT_EQ(
      cache.subgroupObject(
          1,
          0,
          4,
          ObjectStatus::NORMAL,
          bytes(1),
          withExtension(kPriorObjectIdGapExtensionType, 3)),
      Result::Recorded);

  EXPECT_EQ(cache.inspect({1, 0}, {1, 5}), Coverage::Ready);
  auto plan = cache.snapshot({1, 0}, {1, 5}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());
  const std::vector<AbsoluteLocation> expected{{1, 0}, {1, 4}};
  EXPECT_EQ(locations(*plan), expected);
}

TEST(MoQGroupCacheTest, PriorGroupGapProvesNonexistentGroups) {
  MoQGroupCache cache(3, makeBudget());
  recordGroup(cache, 7, 1);
  ASSERT_TRUE(cache.openSubgroup(10, 0, false));
  ASSERT_EQ(
      cache.subgroupObject(
          10,
          0,
          0,
          ObjectStatus::NORMAL,
          bytes(1),
          withExtension(kPriorGroupIdGapExtensionType, 2)),
      Result::Recorded);

  // Groups 8 and 9 do not exist; nothing proves anything about group 6.
  EXPECT_EQ(cache.inspect({8, 0}, {10, 1}), Coverage::Ready);
  EXPECT_EQ(cache.inspect({7, 0}, {10, 1}), Coverage::Ready);
  EXPECT_EQ(cache.inspect({6, 0}, {10, 1}), Coverage::Miss);
}

// Once a group is proven not to exist, later data for it is not cached or
// replayed.
TEST(MoQGroupCacheTest, GroupsProvenAbsentAreNeverRecorded) {
  MoQGroupCache cache(4, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(10, 0, true));
  ASSERT_EQ(
      cache.subgroupObject(
          10,
          0,
          0,
          ObjectStatus::NORMAL,
          bytes(1),
          withExtension(kPriorGroupIdGapExtensionType, 2)),
      Result::Recorded);
  ASSERT_EQ(cache.endSubgroup(10, 0, /*cleanFin=*/true), Result::Recorded);

  EXPECT_FALSE(cache.openSubgroup(8, 0, true));
  EXPECT_FALSE(cache.openSubgroup(9, 0, true));
  EXPECT_EQ(cache.inspect({8, 0}, {10, 1}), Coverage::Ready);
  auto plan = cache.snapshot({8, 0}, {10, 1}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());
  const std::vector<AbsoluteLocation> expected{{10, 0}};
  EXPECT_EQ(locations(*plan), expected);
}

TEST(MoQGroupCacheTest, GapOverAStartedGroupIsMalformed) {
  MoQGroupCache cache(4, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(8, 0, false));
  ASSERT_TRUE(cache.openSubgroup(10, 0, true));

  EXPECT_EQ(
      cache.subgroupObject(
          10,
          0,
          0,
          ObjectStatus::NORMAL,
          bytes(1),
          withExtension(kPriorGroupIdGapExtensionType, 2)),
      Result::Malformed);
  EXPECT_FALSE(cache.isCaching(10));
  ASSERT_EQ(
      cache.subgroupObject(
          8, 0, 0, ObjectStatus::NORMAL, bytes(1), noExtensions()),
      Result::Recorded);
  EXPECT_EQ(cache.inspect({8, 0}, {8, 1}), Coverage::Ready);
  EXPECT_EQ(cache.inspect({9, 0}, {9, 1}), Coverage::Miss);
}

TEST(MoQGroupCacheTest, GapOverAnObjectStillArrivingIsMalformed) {
  MoQGroupCache cache(4, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));
  ASSERT_TRUE(cache.openSubgroup(1, 1, false));
  ASSERT_EQ(
      cache.beginObject(1, 0, 2, 10, bytes(5), noExtensions()),
      Result::Recorded);

  EXPECT_EQ(
      cache.subgroupObject(
          1,
          1,
          4,
          ObjectStatus::NORMAL,
          bytes(1),
          withExtension(kPriorObjectIdGapExtensionType, 3)),
      Result::Malformed);
  EXPECT_FALSE(cache.isCaching(1));
}

TEST(MoQGroupCacheTest, InvalidGapIsMalformed) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));

  auto result = cache.subgroupObject(
      1,
      0,
      2,
      ObjectStatus::NORMAL,
      bytes(1),
      withExtension(kPriorObjectIdGapExtensionType, 3));

  EXPECT_EQ(result, Result::Malformed);
  EXPECT_FALSE(cache.isCaching(1));
}

TEST(MoQGroupCacheTest, LostDatagramIsAMiss) {
  MoQGroupCache cache(3, makeBudget());
  for (uint64_t objectID : {0, 2}) {
    ASSERT_EQ(
        cache.standaloneObject(
            ObjectHeader(1, 0, objectID), bytes(1), /*datagram=*/true),
        Result::Recorded);
  }

  EXPECT_EQ(cache.inspect({1, 0}, {1, 2}), Coverage::Miss);
  EXPECT_EQ(cache.inspect({1, 0}, {1, 1}), Coverage::Ready);
}

TEST(MoQGroupCacheTest, ConflictingDuplicateIsMalformed) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));
  ASSERT_EQ(
      cache.subgroupObject(
          1, 0, 0, ObjectStatus::NORMAL, bytes(5), noExtensions()),
      Result::Recorded);
  ASSERT_TRUE(cache.openSubgroup(1, 1, false));

  auto result = cache.subgroupObject(
      1, 1, 0, ObjectStatus::NORMAL, bytes(6), noExtensions());

  EXPECT_EQ(result, Result::Malformed);
  EXPECT_EQ(cache.inspect({1, 0}, {1, 1}), Coverage::Miss);
}

TEST(MoQGroupCacheTest, PayloadBeyondDeclaredLengthIsMalformed) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));
  ASSERT_EQ(
      cache.beginObject(1, 0, 0, 4, bytes(2), noExtensions()),
      Result::Recorded);

  EXPECT_EQ(cache.objectPayload(1, 0, bytes(3)), Result::Malformed);
}

TEST(MoQGroupCacheTest, NewestFirstSnapshotReversesGroupsOnly) {
  MoQGroupCache cache(3, makeBudget());
  recordGroup(cache, 1, 2);
  recordGroup(cache, 2, 2);

  auto plan = cache.snapshot({1, 0}, {3, 0}, GroupOrder::NewestFirst);

  ASSERT_TRUE(plan.has_value());
  const std::vector<AbsoluteLocation> expected{{2, 0}, {2, 1}, {1, 0}, {1, 1}};
  EXPECT_EQ(locations(*plan), expected);
}

TEST(MoQGroupCacheTest, EndOfTrackIsReportedOnlyWhenInsideTheRange) {
  MoQGroupCache cache(3, makeBudget());
  recordGroup(cache, 1, 1, /*finishWithEnd=*/false);
  ASSERT_EQ(
      cache.subgroupObject(
          1, 0, 1, ObjectStatus::END_OF_TRACK, nullptr, noExtensions()),
      Result::Recorded);

  auto inside = cache.snapshot({1, 0}, {1, 2}, GroupOrder::OldestFirst);
  auto before = cache.snapshot({1, 0}, {1, 1}, GroupOrder::OldestFirst);

  ASSERT_TRUE(inside.has_value());
  ASSERT_TRUE(before.has_value());
  EXPECT_TRUE(inside->endOfTrack);
  EXPECT_FALSE(before->endOfTrack);
}

// --- Waiting ---

TEST(MoQGroupCacheTest, WaitersWakeOnCompletionNotOnFragments) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(1, 0, false));
  ASSERT_EQ(
      cache.beginObject(1, 0, 0, 10, bytes(2), noExtensions()),
      Result::Recorded);
  int wakes = 0;
  cache.addWaiter([&] { wakes++; });

  ASSERT_EQ(cache.objectPayload(1, 0, bytes(3)), Result::Recorded);
  EXPECT_EQ(wakes, 0);
  ASSERT_EQ(cache.objectPayload(1, 0, bytes(5)), Result::Recorded);
  EXPECT_EQ(wakes, 1);
}

TEST(MoQGroupCacheTest, ShutdownWakesWaitersAndMisses) {
  MoQGroupCache cache(3, makeBudget());
  recordGroup(cache, 1, 1);
  int wakes = 0;
  cache.addWaiter([&] { wakes++; });

  cache.shutdown();

  EXPECT_EQ(wakes, 1);
  EXPECT_EQ(wholeGroups(cache, 1, 1), Coverage::Miss);
  EXPECT_FALSE(cache.openSubgroup(2, 0, false));
}

} // namespace moxygen::test
