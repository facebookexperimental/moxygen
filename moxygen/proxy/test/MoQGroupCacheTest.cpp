/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQGroupCache.h"
#include "moxygen/proxy/MoQFetchCacheWriter.h"

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

std::shared_ptr<MoQFetchCacheWriter> fetchWriter(
    const std::shared_ptr<MoQGroupCache>& cache,
    AbsoluteLocation start,
    AbsoluteLocation end,
    bool& cacheable) {
  auto writer = std::make_shared<MoQFetchCacheWriter>(
      cache, start, end, [&](bool result) { cacheable = result; });
  writer->setRange(start, end);
  return writer;
}

} // namespace

TEST(MoQGroupCacheTest, WritesCompleteAndFragmentedFetchObjects) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  bool cacheable = false;
  auto writer = fetchWriter(cache, {7, 0}, {7, 3}, cacheable);

  EXPECT_TRUE(writer->object(7, 0, 0, bytes(3), noExtensions(), false, false)
                  .hasValue());
  EXPECT_TRUE(
      writer->beginObject(7, 0, 1, 5, bytes(2), noExtensions()).hasValue());
  auto status = writer->objectPayload(bytes(3), false);
  ASSERT_TRUE(status.hasValue());
  EXPECT_EQ(status.value(), ObjectPublishStatus::DONE);
  EXPECT_TRUE(writer->endOfFetch().hasValue());

  EXPECT_TRUE(cacheable);
  EXPECT_EQ(cache->inspect({7, 0}, {7, 3}), Coverage::Ready);
  auto plan = cache->snapshot({7, 0}, {7, 3}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(locations(*plan), (std::vector<AbsoluteLocation>{{7, 0}, {7, 1}}));
}

TEST(MoQGroupCacheTest, CompletesFetchedGapsAcrossGroups) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  bool cacheable = false;
  auto writer = std::make_shared<MoQFetchCacheWriter>(
      cache, [&](bool result) { cacheable = result; });

  EXPECT_TRUE(writer->object(10, 0, 0, bytes(1), noExtensions(), false, false)
                  .hasValue());
  EXPECT_TRUE(writer->endOfGroup(10, 0, 2, false).hasValue());
  EXPECT_EQ(wholeGroups(*cache, 10, 10), Coverage::Ready);
  EXPECT_FALSE(cacheable);
  EXPECT_TRUE(writer->endOfTrackAndGroup(12, 0, 0).hasValue());

  EXPECT_TRUE(writer->setRange({10, 0}, {12, 1}));
  EXPECT_TRUE(cacheable);
  EXPECT_EQ(cache->inspect({10, 0}, {12, 1}), Coverage::Ready);
}

TEST(MoQGroupCacheTest, CompletesGroupFromFetchAndSubscription) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  ASSERT_TRUE(cache->openSubgroup(11, 1, true));

  std::optional<bool> cacheable;
  auto writer = std::make_shared<MoQFetchCacheWriter>(
      cache, [&](bool result) { cacheable = result; });
  ASSERT_TRUE(writer->object(11, 0, 0, bytes(1), noExtensions(), false, false)
                  .hasValue());
  EXPECT_EQ(wholeGroups(*cache, 11, 11), Coverage::AwaitIngress);

  ASSERT_EQ(
      cache->subgroupObject(
          11, 1, 1, ObjectStatus::NORMAL, bytes(1), noExtensions()),
      Result::Recorded);
  ASSERT_EQ(cache->endSubgroup(11, 1, true), Result::Recorded);

  EXPECT_EQ(wholeGroups(*cache, 11, 11), Coverage::Ready);
  auto plan = cache->snapshot({11, 0}, {12, 0}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(
      locations(*plan), (std::vector<AbsoluteLocation>{{11, 0}, {11, 1}}));
  EXPECT_FALSE(cacheable.has_value());
}

TEST(MoQGroupCacheTest, FailedFetchKeepsCompletedGroups) {
  auto cache = std::make_shared<MoQGroupCache>(4, makeBudget());
  bool cacheable = true;
  auto writer = fetchWriter(cache, {10, 0}, {12, 0}, cacheable);
  ASSERT_TRUE(writer->object(10, 0, 0, bytes(1), noExtensions(), false, false)
                  .hasValue());
  ASSERT_TRUE(
      writer->beginObject(11, 0, 0, 2, bytes(1), noExtensions()).hasValue());
  ASSERT_TRUE(cache->openSubgroup(20, 0, true));

  writer->abort();

  EXPECT_FALSE(cacheable);
  EXPECT_EQ(wholeGroups(*cache, 10, 10), Coverage::Ready);
  EXPECT_NE(wholeGroups(*cache, 11, 11), Coverage::Ready);
  EXPECT_EQ(
      cache->subgroupObject(
          20, 0, 0, ObjectStatus::NORMAL, bytes(1), noExtensions()),
      Result::Recorded);
  EXPECT_EQ(cache->endSubgroup(20, 0, true), Result::Recorded);
  EXPECT_EQ(wholeGroups(*cache, 20, 20), Coverage::Ready);
}

TEST(MoQGroupCacheTest, FailedFetchPreservesConcurrentWriterData) {
  auto cache = std::make_shared<MoQGroupCache>(4, makeBudget());
  bool failedCacheable = true;
  bool survivorCacheable = false;
  auto failed = fetchWriter(cache, {20, 0}, {21, 0}, failedCacheable);
  auto survivor = fetchWriter(cache, {10, 0}, {12, 0}, survivorCacheable);
  ASSERT_TRUE(failed->object(20, 0, 0, bytes(1), noExtensions(), false, false)
                  .hasValue());
  ASSERT_TRUE(survivor->object(10, 0, 0, bytes(1), noExtensions(), false, false)
                  .hasValue());

  failed->invalidate();

  EXPECT_FALSE(failedCacheable);
  EXPECT_EQ(cache->inspect({20, 0}, {20, 1}), Coverage::Ready);
  ASSERT_TRUE(survivor->object(10, 0, 1, bytes(1), noExtensions(), false, false)
                  .hasValue());
  ASSERT_TRUE(survivor->object(11, 0, 0, bytes(1), noExtensions(), true, false)
                  .hasValue());
  ASSERT_TRUE(survivorCacheable);
  auto plan = cache->snapshot({10, 0}, {12, 0}, GroupOrder::OldestFirst);
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(
      locations(*plan),
      (std::vector<AbsoluteLocation>{{10, 0}, {10, 1}, {11, 0}}));
}

TEST(MoQGroupCacheTest, AbortAfterCleanFetchRejectsLateRange) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  std::optional<bool> cacheable;
  auto writer = std::make_shared<MoQFetchCacheWriter>(
      cache, [&](bool result) { cacheable = result; });
  ASSERT_TRUE(writer->object(7, 0, 0, bytes(1), noExtensions(), true, false)
                  .hasValue());
  ASSERT_FALSE(cacheable.has_value());

  writer->abort();

  ASSERT_TRUE(cacheable.has_value());
  EXPECT_FALSE(*cacheable);
  EXPECT_FALSE(writer->setRange({7, 0}, {8, 0}));
  EXPECT_EQ(wholeGroups(*cache, 7, 7), Coverage::Miss);
}

TEST(MoQGroupCacheTest, ValidatesDataThatPrecedesFetchOk) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  std::optional<bool> cacheable;
  auto writer = std::make_shared<MoQFetchCacheWriter>(
      cache, [&](bool result) { cacheable = result; });

  EXPECT_TRUE(writer->object(7, 0, 0, bytes(1), noExtensions(), true, false)
                  .hasValue());
  EXPECT_FALSE(cacheable.has_value());
  EXPECT_TRUE(writer->setRange({7, 0}, {7, 1}));

  ASSERT_TRUE(cacheable.has_value());
  EXPECT_TRUE(*cacheable);
}

TEST(MoQGroupCacheTest, RejectsFetchOkRangeThatExcludesEarlyData) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  std::optional<bool> cacheable;
  auto writer = std::make_shared<MoQFetchCacheWriter>(
      cache, [&](bool result) { cacheable = result; });

  ASSERT_TRUE(writer->object(7, 0, 0, bytes(1), noExtensions(), true, false)
                  .hasValue());
  EXPECT_FALSE(writer->setRange({8, 0}, {8, 1}));

  ASSERT_TRUE(cacheable.has_value());
  EXPECT_FALSE(*cacheable);
  EXPECT_EQ(cache->inspect({7, 0}, {7, 1}), Coverage::Ready);
}

TEST(MoQGroupCacheTest, RejectsDataOutsideRequestedBoundsBeforeFetchOk) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  std::optional<bool> cacheable;
  auto writer = std::make_shared<MoQFetchCacheWriter>(
      cache, AbsoluteLocation{8, 0}, AbsoluteLocation{9, 0}, [&](bool result) {
        cacheable = result;
      });

  EXPECT_TRUE(writer->object(7, 0, 0, bytes(1), noExtensions(), false, false)
                  .hasValue());
  EXPECT_TRUE(writer->object(8, 0, 0, bytes(1), noExtensions(), true, false)
                  .hasValue());
  EXPECT_FALSE(writer->setRange({8, 0}, {9, 0}));

  EXPECT_EQ(cache->residentGroups(), 0);
  ASSERT_TRUE(cacheable.has_value());
  EXPECT_FALSE(*cacheable);
}

TEST(MoQGroupCacheTest, FetchCacheConflictIsPropagated) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  ASSERT_EQ(
      cache->standaloneObject(ObjectHeader(7, 0, 0), bytes(1), false),
      Result::Recorded);
  bool cacheable = true;
  auto writer = fetchWriter(cache, {7, 0}, {7, 1}, cacheable);

  auto result = writer->object(7, 0, 0, bytes(2), noExtensions(), true, false);

  ASSERT_TRUE(result.hasError());
  EXPECT_EQ(result.error().code, MoQPublishError::MALFORMED_TRACK);
  EXPECT_FALSE(cacheable);
}

TEST(MoQGroupCacheTest, OverlappingFetchUsesCachedObject) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  ASSERT_EQ(
      cache->standaloneObject(ObjectHeader(7, 0, 0), bytes(1), false),
      Result::Recorded);
  bool cacheable = true;
  auto writer = fetchWriter(cache, {7, 0}, {7, 1}, cacheable);

  ASSERT_TRUE(writer->object(7, 0, 0, bytes(1), noExtensions(), true, false)
                  .hasValue());

  EXPECT_TRUE(cacheable);
  EXPECT_EQ(cache->inspect({7, 0}, {7, 1}), Coverage::Ready);
}

TEST(MoQGroupCacheTest, CompleteFetchNeedsTheWholeRangeResident) {
  auto cache = std::make_shared<MoQGroupCache>(1, makeBudget());
  bool cacheable = false;
  auto writer = fetchWriter(cache, {7, 0}, {9, 0}, cacheable);
  ASSERT_TRUE(writer->object(7, 0, 0, bytes(1), noExtensions(), false, false)
                  .hasValue());
  ASSERT_TRUE(writer->object(8, 0, 0, bytes(1), noExtensions(), true, false)
                  .hasValue());

  ASSERT_TRUE(cacheable);
  EXPECT_EQ(cache->completeFetch({7, 0}, {9, 0}), Result::NotCached);
}

TEST(MoQGroupCacheTest, EmptyFetchRangeIsComplete) {
  MoQGroupCache cache(1, makeBudget());

  EXPECT_EQ(cache.completeFetch({7, 0}, {7, 0}), Result::Recorded);
}

TEST(MoQGroupCacheTest, IncompleteFetchedObjectIsNotReplayable) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  bool cacheable = true;
  auto writer = fetchWriter(cache, {7, 0}, {7, 1}, cacheable);

  EXPECT_TRUE(
      writer->beginObject(7, 0, 0, 5, bytes(2), noExtensions()).hasValue());
  EXPECT_TRUE(writer->endOfFetch().hasError());

  EXPECT_FALSE(cacheable);
  EXPECT_NE(cache->inspect({7, 0}, {7, 1}), Coverage::Ready);
}

TEST(MoQGroupCacheTest, FetchedObjectsDoNotCompleteLiveSubgroups) {
  auto cache = std::make_shared<MoQGroupCache>(3, makeBudget());
  ASSERT_TRUE(cache->openSubgroup(7, 0, /*containsLastInGroup=*/true));
  bool cacheable = false;
  auto writer = fetchWriter(cache, {7, 0}, {7, 1}, cacheable);

  EXPECT_TRUE(writer->object(7, 0, 0, bytes(1), noExtensions(), true, false)
                  .hasValue());
  EXPECT_EQ(cache->endSubgroup(7, 0, /*cleanFin=*/true), Result::Recorded);

  EXPECT_TRUE(cacheable);
  EXPECT_EQ(cache->inspect({7, 0}, {8, 0}), Coverage::Miss);
}

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

TEST(MoQGroupCacheTest, MissingRangesExcludeCachedObjects) {
  MoQGroupCache cache(3, makeBudget());
  for (uint64_t objectID : {0, 2}) {
    ASSERT_EQ(
        cache.standaloneObject(
            ObjectHeader(7, 0, objectID), bytes(1), /*datagram=*/true),
        Result::Recorded);
  }
  recordGroup(cache, 9, 1);

  EXPECT_EQ(
      cache.missingRanges({7, 0}, {10, 0}),
      (std::vector<MoQGroupCache::Range>{{{7, 1}, {7, 2}}, {{7, 3}, {9, 0}}}));
}

TEST(MoQGroupCacheTest, MissingRangesExcludeKnownGroupTail) {
  MoQGroupCache cache(3, makeBudget());
  ASSERT_TRUE(cache.openSubgroup(7, 0, false));
  ASSERT_EQ(
      cache.subgroupObject(
          7, 0, 0, ObjectStatus::NORMAL, bytes(1), noExtensions()),
      Result::Recorded);
  ASSERT_EQ(
      cache.subgroupObject(
          7, 0, 2, ObjectStatus::END_OF_GROUP, nullptr, noExtensions()),
      Result::Recorded);

  EXPECT_EQ(
      cache.missingRanges({7, 0}, {8, 0}),
      (std::vector<MoQGroupCache::Range>{{{7, 1}, {7, 2}}}));
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
  EXPECT_EQ(
      cache.missingRanges({1, 0}, {1, 1}),
      (std::vector<MoQGroupCache::Range>{{{1, 0}, {1, 1}}}));
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
