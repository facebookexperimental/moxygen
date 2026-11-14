/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>
#include <folly/portability/GTest.h>
#include <moxygen/MoQTimestamp.h>

#include <limits>

using namespace moxygen;

namespace {

constexpr uint64_t kU64Max = std::numeric_limits<uint64_t>::max();
constexpr int64_t kI64Max = std::numeric_limits<int64_t>::max();
constexpr int64_t kI64Min = std::numeric_limits<int64_t>::min();

ObjectHeader objectWith(
    uint64_t group,
    uint64_t object,
    const std::vector<Extension>& extensions = {},
    const std::vector<Extension>& immutable = {}) {
  return ObjectHeader(
      group,
      0,
      object,
      kDefaultPriority,
      ObjectStatus::NORMAL,
      Extensions(extensions, immutable));
}

Extension objectTimestamp(uint64_t value) {
  return Extension{kObjectTimestampPropertyType, value};
}

} // namespace

TEST(MoQTimestamp, ZigZag) {
  EXPECT_EQ(zigzagEncode(0), 0);
  EXPECT_EQ(zigzagEncode(-1), 1);
  EXPECT_EQ(zigzagEncode(1), 2);
  EXPECT_EQ(zigzagEncode(-2), 3);
  EXPECT_EQ(zigzagEncode(kI64Max), kU64Max - 1);
  EXPECT_EQ(zigzagEncode(kI64Min), kU64Max);
  for (int64_t v : {int64_t(0), int64_t(-1), int64_t(1), kI64Max, kI64Min}) {
    EXPECT_EQ(zigzagDecode(zigzagEncode(v)), v);
  }
}

TEST(MoQTimestamp, MappingRoundTrip) {
  TimestampMapping mapping{7, kU64Max, 60060, 1001};
  auto parsed = TimestampMapping::parse(*mapping.encode());
  ASSERT_TRUE(parsed.hasValue());
  EXPECT_EQ(*parsed, mapping);
}

TEST(MoQTimestamp, MappingMalformed) {
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&queue, 16);
  for (int i = 0; i < 3; i++) {
    appender.writeBE<uint8_t>(1);
  }
  EXPECT_TRUE(TimestampMapping::parse(*queue.front()).hasError());

  appender.writeBE<uint8_t>(1);
  appender.writeBE<uint8_t>(1);
  EXPECT_TRUE(TimestampMapping::parse(*queue.front()).hasError());

  auto truncated = TimestampMapping{1, 2, 3, kU64Max}.encode();
  truncated->coalesce();
  truncated->trimEnd(1);
  EXPECT_TRUE(TimestampMapping::parse(*truncated).hasError());

  EXPECT_TRUE(TimestampMapping::parse(*folly::IOBuf::create(0)).hasError());
}

TEST(MoQTimestamp, FixedCadence) {
  // 30000/1001 fps, 60 Objects per Group.
  TimestampMapping mapping{0, 0, 60060, 1001};
  EXPECT_EQ(mapping.timestamp(2, 3).value(), 2 * 60060 + 3 * 1001);
  EXPECT_EQ(mapping.timestamp(2, 3, -5).value(), 2 * 60060 + 3 * 1001 - 5);
  EXPECT_EQ(mapping.locate(2 * 60060 + 3 * 1001), (AbsoluteLocation{2, 3}));
  EXPECT_EQ(
      mapping.locate(2 * 60060 + 3 * 1001 + 1000), (AbsoluteLocation{2, 3}));
}

TEST(MoQTimestamp, GroupIdsAsTimestamps) {
  // Group IDs are the timestamps.
  TimestampMapping mapping{0, 0, 1, 0};
  constexpr uint64_t kGroup = 1767225600000000;
  EXPECT_EQ(mapping.timestamp(kGroup, 5).value(), kGroup);
  EXPECT_EQ(mapping.locate(kGroup), (AbsoluteLocation{kGroup, 0}));
}

TEST(MoQTimestamp, GroupsBeforeBase) {
  TimestampMapping mapping{10, 1000, 100, 10};
  EXPECT_EQ(mapping.timestamp(8, 1).value(), 810);
  EXPECT_EQ(mapping.locate(810), (AbsoluteLocation{8, 1}));
  EXPECT_EQ(mapping.locate(0), (AbsoluteLocation{0, 0}));
  EXPECT_EQ(mapping.locate(5), (AbsoluteLocation{0, 0}));
  // Before Group 0.
  TimestampMapping late{1, 500, 100, 10};
  EXPECT_EQ(late.locate(0), std::nullopt);
  EXPECT_EQ(TimestampMapping{}.locate(0), std::nullopt);
}

TEST(MoQTimestamp, NegativeMappedTimestamp) {
  TimestampMapping mapping{10, 0, 100, 0};
  EXPECT_TRUE(mapping.timestamp(9, 0).hasError());
  EXPECT_EQ(mapping.timestamp(9, 0, 150).value(), 50);
  EXPECT_EQ(mapping.timestamp(9, 0, 100).value(), 0);
  EXPECT_TRUE(mapping.timestamp(10, 0, -1).hasError());
  EXPECT_EQ(mapping.correction(9, 0, 50).value(), 150);
}

TEST(MoQTimestamp, Overflow) {
  TimestampMapping mapping{0, 0, kU64Max, 0};
  EXPECT_EQ(mapping.timestamp(1, 0).value(), kU64Max);
  EXPECT_TRUE(mapping.timestamp(2, 0).hasError());
  EXPECT_TRUE(mapping.timestamp(1, 0, 1).hasError());
  EXPECT_EQ(mapping.timestamp(1, 0, -1).value(), kU64Max - 1);
}

TEST(MoQTimestamp, OverflowCancels) {
  // The Group term before the base brings the base plus the Object term back
  // under 2^64.
  TimestampMapping mapping{5, kU64Max - 10, 100, 50};
  EXPECT_EQ(mapping.timestamp(4, 1).value(), kU64Max - 60);
  EXPECT_EQ(mapping.timestamp(4, 1, 60).value(), kU64Max);
  EXPECT_TRUE(mapping.timestamp(4, 1, 61).hasError());
  EXPECT_EQ(mapping.correction(4, 1, kU64Max - 61).value(), -1);
  EXPECT_EQ(mapping.correction(4, 1, kU64Max).value(), 60);

  // The two products of 2^64 cancel.
  constexpr uint64_t k = uint64_t{1} << 32;
  TimestampMapping products{k, 0, k, k};
  EXPECT_EQ(products.timestamp(0, k).value(), 0);
  EXPECT_EQ(products.correction(0, k, 0).value(), 0);
  EXPECT_EQ(products.timestamp(0, k + 1).value(), k);
  EXPECT_TRUE(products.timestamp(0, k - 1).hasError());
  // Both products on one side, each near 2^128.
  TimestampMapping huge{0, kU64Max, kU64Max, kU64Max};
  EXPECT_TRUE(huge.timestamp(kU64Max, kU64Max).hasError());
  EXPECT_TRUE(huge.correction(kU64Max, kU64Max, 0).hasError());
}

TEST(MoQTimestamp, CorrectionRange) {
  TimestampMapping mapping{0, uint64_t(1) << 63, 0, 0};
  EXPECT_EQ(mapping.correction(0, 0, 0).value(), kI64Min);
  EXPECT_EQ(mapping.timestamp(0, 0, kI64Min).value(), 0);
  EXPECT_TRUE(TimestampMapping{}.correction(0, 0, kU64Max).hasError());
}

TEST(MoQTimestamp, TrackPropertiesAbsent) {
  auto info = TrackTimestampInfo::fromTrackProperties(Extensions());
  ASSERT_TRUE(info.hasValue());
  EXPECT_FALSE(info->has_value());
}

TEST(MoQTimestamp, TrackPropertiesRoundTrip) {
  TrackTimestampInfo info;
  info.timescale = 30000;
  info.clockId = 0x1A3F5C9E07B2D461;
  info.origin = 12345;
  info.mapping = TimestampMapping{0, 0, 60060, 1001};
  for (bool immutable : {false, true}) {
    Extensions properties;
    info.addTo(properties, immutable);
    auto parsed = TrackTimestampInfo::fromTrackProperties(properties);
    ASSERT_TRUE(parsed.hasValue());
    ASSERT_TRUE(parsed->has_value());
    EXPECT_EQ(**parsed, info);
  }
}

TEST(MoQTimestamp, TrackPropertiesMalformed) {
  auto expectMalformed = [](const Extensions& properties) {
    EXPECT_TRUE(TrackTimestampInfo::fromTrackProperties(properties).hasError());
  };
  expectMalformed(Extensions({Extension{kClockIdPropertyType, 1}}, {}));
  expectMalformed(Extensions({Extension{kTimescalePropertyType, 0}}, {}));
  expectMalformed(Extensions(
      {Extension{kTimescalePropertyType, 1000}},
      {Extension{kTimescalePropertyType, 1000}}));
  expectMalformed(Extensions({objectTimestamp(5)}, {}));
  expectMalformed(Extensions(
      {Extension{kTimescalePropertyType, 1000},
       Extension{kTimestampMappingPropertyType, folly::IOBuf::copyBuffer("x")}},
      {}));
}

TEST(MoQTimestamp, ExplicitObjectTimestamp) {
  auto info = TrackTimestampInfo::wallClockMicros();
  EXPECT_EQ(info.objectTimestamp(objectWith(1, 1)).value(), std::nullopt);
  EXPECT_EQ(
      info.objectTimestamp(objectWith(1, 1, {objectTimestamp(42)})).value(),
      42);
  EXPECT_EQ(
      info.objectTimestamp(objectWith(1, 1, {}, {objectTimestamp(42)})).value(),
      42);
  EXPECT_TRUE(
      info.objectTimestamp(
              objectWith(1, 1, {objectTimestamp(1)}, {objectTimestamp(1)}))
          .hasError());
  EXPECT_TRUE(
      info.objectTimestamp(
              objectWith(1, 1, {Extension{kTimescalePropertyType, 1000}}))
          .hasError());

  auto property = info.objectProperty(1, 1, 42);
  ASSERT_TRUE(property.hasValue());
  ASSERT_TRUE(property->has_value());
  EXPECT_EQ((*property)->intValue, 42);
}

TEST(MoQTimestamp, MappedObjectTimestamp) {
  TrackTimestampInfo info;
  info.timescale = 30000;
  info.mapping = TimestampMapping{0, 0, 60060, 1001};
  const uint64_t onSchedule = 2 * 60060 + 3 * 1001;
  EXPECT_EQ(info.objectTimestamp(objectWith(2, 3)).value(), onSchedule);
  EXPECT_EQ(info.objectProperty(2, 3, onSchedule).value(), std::nullopt);

  auto property = info.objectProperty(2, 3, onSchedule - 7);
  ASSERT_TRUE(property.hasValue());
  ASSERT_TRUE(property->has_value());
  EXPECT_EQ((*property)->intValue, zigzagEncode(-7));
  EXPECT_EQ(
      info.objectTimestamp(objectWith(2, 3, {**property})).value(),
      onSchedule - 7);
}

TEST(MoQTimestamp, WallClock) {
  auto micros = TrackTimestampInfo::wallClockMicros();
  auto when = std::chrono::system_clock::time_point(
      std::chrono::seconds(1767225600) + std::chrono::microseconds(123456));
  auto ticks = micros.fromWallClock(when);
  ASSERT_TRUE(ticks.has_value());
  EXPECT_EQ(*ticks, 1767225600123456);
  EXPECT_EQ(micros.toWallClock(*ticks), when);

  TrackTimestampInfo video;
  video.timescale = 90000;
  auto videoTicks = video.fromWallClock(when);
  ASSERT_TRUE(videoTicks.has_value());
  EXPECT_EQ(*videoTicks, uint64_t(1767225600) * 90000 + 11111);

  EXPECT_EQ(TrackTimestampInfo{}.fromWallClock(when), std::nullopt);

  // The conversions do not apply to TAI or to a shared clock.
  for (uint64_t clockId : {uint64_t{1}, kMaxReservedClockId + 1}) {
    TrackTimestampInfo other = micros;
    other.clockId = clockId;
    EXPECT_EQ(other.fromWallClock(when), std::nullopt);
    EXPECT_EQ(other.toWallClock(5), std::nullopt);
  }
}

TEST(MoQTimestamp, OriginOnPosixTime) {
  TrackTimestampInfo info = TrackTimestampInfo::wallClockMicros();
  info.origin = 1767225600000000;
  Extensions properties;
  info.addTo(properties);
  auto parsed = TrackTimestampInfo::fromTrackProperties(properties);
  ASSERT_TRUE(parsed.hasValue());
  ASSERT_TRUE(parsed->has_value());
  EXPECT_EQ(**parsed, info);
  EXPECT_TRUE((*parsed)->isWallClock());

  auto when = std::chrono::system_clock::time_point(
      std::chrono::seconds(1767225600) + std::chrono::microseconds(5));
  EXPECT_EQ(info.fromWallClock(when), 5);
  EXPECT_EQ(info.toWallClock(5), when);
}
