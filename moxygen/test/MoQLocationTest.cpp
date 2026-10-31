/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/logging/xlog.h>
#include <folly/portability/GTest.h>
#include <moxygen/MoQLocation.h>

using namespace moxygen;

namespace {
SubscribeRequest getRequest(
    LocationType locType,
    std::optional<AbsoluteLocation> start = std::nullopt,
    uint64_t endGroup = 0) {
  return SubscribeRequest::make(
      FullTrackName(),
      0,
      GroupOrder::OldestFirst,
      true,
      locType,
      start,
      endGroup,
      {});
}
} // namespace

TEST(Location, LargestObject) {
  auto range = toSubscribeRange(
      getRequest(LocationType::LargestObject), AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 78);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, NextGroupStart) {
  auto range = toSubscribeRange(
      getRequest(LocationType::NextGroupStart), AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 20);
  EXPECT_EQ(range.start.object, 0);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, LargestGroup) {
  auto range = toSubscribeRange(
      getRequest(LocationType::LargestGroup), AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 0);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, AbsoluteStart) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteStart, AbsoluteLocation({1, 2})),
      AbsoluteLocation({1, 1}));
  EXPECT_EQ(range.start.group, 1);
  EXPECT_EQ(range.start.object, 2);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, AbsoluteStartBehindLargest) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteStart, AbsoluteLocation({1, 2})),
      AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 78);
  EXPECT_EQ(range.end, kLocationMax);
}

TEST(Location, AbsoluteRange) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteRange, AbsoluteLocation({1, 2}), 3),
      AbsoluteLocation({1, 1}));
  EXPECT_EQ(range.start.group, 1);
  EXPECT_EQ(range.start.object, 2);
  EXPECT_EQ(range.end.group, 3);
  EXPECT_EQ(range.end.object, kEightByteLimit);
}

TEST(Location, AbsoluteRangeBehindLargest) {
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteRange, AbsoluteLocation({1, 2}), 3),
      AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 78);
  EXPECT_EQ(range.end.group, 3);
  EXPECT_EQ(range.end.object, kEightByteLimit);
}

TEST(Location, AbsoluteRangeEndGroupZero) {
  // endGroup=0 is valid for AbsoluteRange; old code checked endGroup > 0
  // which would leave end as nullopt and trigger XCHECK failure
  auto range = toSubscribeRange(
      getRequest(LocationType::AbsoluteRange, AbsoluteLocation({0, 0}), 0),
      std::nullopt);
  EXPECT_EQ(range.start.group, 0);
  EXPECT_EQ(range.start.object, 0);
  EXPECT_EQ(range.end.group, 0);
  EXPECT_EQ(range.end.object, kEightByteLimit);
}

TEST(Location, NonRangeWithNonZeroEndGroup) {
  // endGroup field should be ignored for non-range location types
  auto range = toSubscribeRange(
      getRequest(LocationType::LargestGroup, std::nullopt, 5),
      AbsoluteLocation({19, 77}));
  EXPECT_EQ(range.start.group, 19);
  EXPECT_EQ(range.start.object, 0);
  EXPECT_EQ(range.end, kLocationMax);
}

namespace {
// One case per row of the draft-22 Location Filter table.
struct ResolveCase {
  std::string name;
  LocationFilter filter;
  std::optional<AbsoluteLocation> largest;
  AbsoluteLocation subscribeStart;
  AbsoluteLocation subscribeEnd;
  AbsoluteLocation fetchStart;
  AbsoluteLocation fetchEnd;
};

} // namespace

TEST(LocationFilter, Resolve) {
  const std::vector<ResolveCase> cases{
      // No filter: unfiltered, and a fetch runs to Largest Object.
      ResolveCase{
          "none",
          LocationFilter::none(),
          AbsoluteLocation{19, 77},
          {19, 78},
          kLocationMax,
          {0, 0},
          {19, 77}},
      ResolveCase{
          "noneNoLargest",
          LocationFilter::none(),
          std::nullopt,
          {0, 0},
          kLocationMax,
          {0, 0},
          kLocationMax},
      // Relative: {Largest.group + 1 - StartGroup, 0}.
      ResolveCase{
          "relativeNextGroup",
          LocationFilter::relativeStart(0),
          AbsoluteLocation{19, 77},
          {20, 0},
          kLocationMax,
          {20, 0},
          {19, 77}},
      ResolveCase{
          "relativeCurrentGroup",
          LocationFilter::relativeStart(1),
          AbsoluteLocation{19, 77},
          {19, 0},
          kLocationMax,
          {19, 0},
          {19, 77}},
      ResolveCase{
          "relativeThreeGroupsBack",
          LocationFilter::relativeStart(3),
          AbsoluteLocation{19, 77},
          {17, 0},
          kLocationMax,
          {17, 0},
          {19, 77}},
      // A relative start below group 0 clamps at 0.
      ResolveCase{
          "relativeClampsAtZero",
          LocationFilter::relativeStart(9),
          AbsoluteLocation{2, 5},
          {0, 0},
          kLocationMax,
          {0, 0},
          {2, 5}},
      // Two zero fields: Next Object.
      ResolveCase{
          "nextObject",
          LocationFilter::nextObject(),
          AbsoluteLocation{19, 77},
          {19, 78},
          kLocationMax,
          {19, 78},
          {19, 77}},
      ResolveCase{
          "nextObjectNoLargest",
          LocationFilter::nextObject(),
          std::nullopt,
          {0, 0},
          kLocationMax,
          {0, 0},
          kLocationMax},
      // Absolute start, open ended.
      ResolveCase{
          "absoluteStartAhead",
          LocationFilter::absoluteStart({30, 2}),
          AbsoluteLocation{19, 77},
          {30, 2},
          kLocationMax,
          {30, 2},
          {19, 77}},
      // A subscription cannot start behind Largest Object; a fetch can.
      ResolveCase{
          "absoluteStartBehind",
          LocationFilter::absoluteStart({1, 2}),
          AbsoluteLocation{19, 77},
          {19, 78},
          kLocationMax,
          {1, 2},
          {19, 77}},
      // Start plus end group: through the last object of that group.
      ResolveCase{
          "rangeToEndOfGroup",
          LocationFilter::range({1, 2}, 3),
          AbsoluteLocation{1, 1},
          {1, 2},
          {3, kEightByteLimit},
          {1, 2},
          {1, 1}},
      // Start plus end location.
      ResolveCase{
          "rangeToEndObject",
          LocationFilter::range({1, 2}, {3, 4}),
          AbsoluteLocation{1, 1},
          {1, 2},
          {3, 4},
          {1, 2},
          {1, 1}},
      // An end beyond Largest Object is clamped for a fetch only.
      ResolveCase{
          "rangeClampedForFetch",
          LocationFilter::range({1, 0}, {9, 9}),
          AbsoluteLocation{4, 4},
          {4, 5},
          {9, 9},
          {1, 0},
          {4, 4}}};

  for (const auto& c : cases) {
    SCOPED_TRACE(c.name);
    auto sub = c.filter.resolve(c.largest, /*isFetch=*/false);
    EXPECT_EQ(sub.start, c.subscribeStart) << "subscribe start";
    EXPECT_EQ(sub.end, c.subscribeEnd) << "subscribe end";

    auto fetch = c.filter.resolve(c.largest, /*isFetch=*/true);
    EXPECT_EQ(fetch.start, c.fetchStart) << "fetch start";
    EXPECT_EQ(fetch.end, c.fetchEnd) << "fetch end";
  }
}

TEST(LocationFilter, FromFieldsRejectsTooManyFields) {
  std::array<uint64_t, LocationFilter::kMaxFields> fields{1, 2, 3, 4};
  EXPECT_TRUE(LocationFilter::fromFields(fields, 4).hasValue());
  auto res = LocationFilter::fromFields(fields, 5);
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error(), ErrorCode::PROTOCOL_VIOLATION);
}

TEST(LocationFilter, FromFieldsRejectsEndGroupOverflow) {
  // Only a sum past 2^64-1 is a violation. Two varints cannot reach it, so
  // this case arrives from a direct caller rather than from the wire.
  std::array<uint64_t, LocationFilter::kMaxFields> fields{
      1, 0, std::numeric_limits<uint64_t>::max(), 0};
  auto res = LocationFilter::fromFields(fields, 3);
  ASSERT_TRUE(res.hasError());
  EXPECT_EQ(res.error(), ErrorCode::PROTOCOL_VIOLATION);

  // A sum past the varint limit is legal, and the end group saturates there.
  fields[2] = kEightByteLimit;
  auto saturated = LocationFilter::fromFields(fields, 3);
  ASSERT_TRUE(saturated.hasValue());
  EXPECT_EQ(saturated->fields()[2], kEightByteLimit - 1);

  // The same delta is fine from group 0.
  fields[0] = 0;
  EXPECT_TRUE(LocationFilter::fromFields(fields, 3).hasValue());
}

// The end group is delta encoded, so it round-trips through the wire form.
TEST(LocationFilter, FieldsRoundTrip) {
  const std::vector<LocationFilter> filters{
      LocationFilter::none(),
      LocationFilter::relativeStart(3),
      LocationFilter::nextObject(),
      LocationFilter::absoluteStart({10, 20}),
      LocationFilter::range({10, 20}, 30),
      LocationFilter::range({10, 20}, {30, 4})};
  for (const auto& filter : filters) {
    auto res = LocationFilter::fromFields(filter.fields(), filter.fieldCount());
    ASSERT_TRUE(res.hasValue()) << filter.describe();
    EXPECT_EQ(*res, filter) << filter.describe();
  }
}

TEST(LocationFilter, LegacyRoundTrip) {
  const std::vector<LocationFilter> filters{
      LocationFilter::none(),
      LocationFilter::relativeStart(0),
      LocationFilter::relativeStart(1),
      LocationFilter::nextObject(),
      LocationFilter::absoluteStart({10, 20}),
      LocationFilter::range({10, 20}, 30)};
  for (const auto& filter : filters) {
    EXPECT_EQ(fromSubscriptionFilter(toSubscriptionFilter(filter)), filter)
        << filter.describe();
  }
}

// Drafts below 22 can carry an end group behind the start; the endpoint has to
// see that rather than have it normalized away.
TEST(LocationFilter, EndBeforeStartSurvives) {
  auto filter = LocationFilter::range({5, 10}, 3);
  EXPECT_EQ(filter.legacyEndGroup(), 3);
  EXPECT_EQ(fromSubscriptionFilter(toSubscriptionFilter(filter)), filter);
}

// A StandaloneFetch end is the wire End Location: object 0 means all of the
// end group. Both directions have to honour that or the range shifts a group.
TEST(LocationFilter, FetchEndLocationRoundTrip) {
  const std::vector<StandaloneFetch> ranges{
      StandaloneFetch({5, 0}, {10, 0}), // groups 5-10
      StandaloneFetch({5, 0}, {5, 0}),  // one whole group
      StandaloneFetch({0, 0}, {0, 0}),
      StandaloneFetch({2, 0}, {2, 10}), // group 2, objects 0-9
      StandaloneFetch({7, 1}, {10, 5})};
  for (const auto& range : ranges) {
    SCOPED_TRACE(range.start.describe() + "-" + range.end.describe());
    auto filter = toLocationFilter(range);
    auto [start, end] = filter.fetchRange(std::nullopt);
    EXPECT_EQ(start, range.start);
    EXPECT_EQ(end, range.end);
  }
}

TEST(LocationFilter, FetchRangeIsHalfOpen) {
  auto [start, end] =
      LocationFilter::range({1, 2}, {3, 4}).fetchRange(AbsoluteLocation{9, 9});
  EXPECT_EQ(start, (AbsoluteLocation{1, 2}));
  EXPECT_EQ(end, (AbsoluteLocation{3, 5}));
}

TEST(Location, Compare) {
  AbsoluteLocation loc1 = {1, 2};
  AbsoluteLocation loc2 = {1, 3};
  AbsoluteLocation loc3 = {2, 1};
  EXPECT_TRUE(loc1 == loc1);
  EXPECT_TRUE(loc1 < loc2);
  EXPECT_TRUE(loc2 > loc1);
  EXPECT_TRUE(loc2 < loc3);
  EXPECT_TRUE(loc3 > loc2);
}

TEST(Location, Next) {
  // Normal increment
  EXPECT_EQ((AbsoluteLocation{0, 0}.next()), (AbsoluteLocation{0, 1}));
  EXPECT_EQ((AbsoluteLocation{5, 100}.next()), (AbsoluteLocation{5, 101}));

  // Object overflow -> next group
  EXPECT_EQ(
      (AbsoluteLocation{5, kEightByteLimit}.next()), (AbsoluteLocation{6, 0}));

  // Max location has no successor
  EXPECT_FALSE(kLocationMax.next().has_value());
}

TEST(Location, Prev) {
  // Normal decrement
  EXPECT_EQ((AbsoluteLocation{0, 1}.prev()), (AbsoluteLocation{0, 0}));
  EXPECT_EQ((AbsoluteLocation{5, 100}.prev()), (AbsoluteLocation{5, 99}));

  // Object underflow -> previous group
  EXPECT_EQ(
      (AbsoluteLocation{5, 0}.prev()), (AbsoluteLocation{4, kEightByteLimit}));

  // Min location has no predecessor
  EXPECT_FALSE(kLocationMin.prev().has_value());
}

TEST(Location, NextInGroup) {
  EXPECT_EQ((AbsoluteLocation{5, 10}.nextInGroup()), (AbsoluteLocation{5, 11}));
  EXPECT_FALSE(
      (AbsoluteLocation{5, kEightByteLimit}.nextInGroup().has_value()));
}

TEST(Location, NextGroup) {
  EXPECT_EQ((AbsoluteLocation{5, 10}.nextGroup()), (AbsoluteLocation{6, 0}));
  EXPECT_FALSE((AbsoluteLocation{kEightByteLimit, 0}.nextGroup().has_value()));
}

TEST(Location, PrevGroup) {
  EXPECT_EQ((AbsoluteLocation{5, 10}.prevGroup()), (AbsoluteLocation{4, 0}));
  EXPECT_FALSE((AbsoluteLocation{0, 10}.prevGroup().has_value()));
}
