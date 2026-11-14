/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQTypes.h>

#include <folly/Expected.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>

namespace moxygen {

// Timestamp Properties for MOQT (draft-frindell-moq-timestamp-01). A missing
// CLOCK_ID means POSIX time. Any present one is treated as a non-wall clock.

// Code points are provisional.
constexpr uint64_t kTimescalePropertyType = 0x2C7A51E0;
constexpr uint64_t kClockIdPropertyType = 0x3E8D2B70;
constexpr uint64_t kTimestampOriginPropertyType = 0x31B49A6E;
constexpr uint64_t kTimestampMappingPropertyType = 0x27F308C5;
constexpr uint64_t kObjectTimestampPropertyType = 0x2D1A;

// CLOCK_IDs up to this value are reserved for the "MOQ Clock IDs" registry.
constexpr uint64_t kMaxReservedClockId = 63;

// Every error means the Track is malformed.
struct TimestampError {
  std::string reason;
};

uint64_t zigzagEncode(int64_t value);
int64_t zigzagDecode(uint64_t value);

struct TimestampMapping {
  uint64_t baseGroup{0};
  uint64_t baseTimestamp{0};
  uint64_t groupMultiplier{0};
  uint64_t objectMultiplier{0};

  // Always the draft-17+ MoQT varint, whatever the session's version.
  std::unique_ptr<folly::IOBuf> encode() const;
  static folly::Expected<TimestampMapping, TimestampError> parse(
      const folly::IOBuf& value);

  // mapped_timestamp + correction. Fails outside [0, 2^64-1].
  folly::Expected<uint64_t, TimestampError>
  timestamp(uint64_t group, uint64_t object, int64_t correction = 0) const;

  // The correction that makes (group, object) carry `timestamp`. Fails when
  // it does not fit in an int64.
  folly::Expected<int64_t, TimestampError>
  correction(uint64_t group, uint64_t object, uint64_t timestamp) const;

  // Estimated Location of `timestamp`. nullopt when the Group Multiplier is 0
  // or the Group is out of range.
  std::optional<AbsoluteLocation> locate(uint64_t timestamp) const;

  bool operator==(const TimestampMapping&) const = default;
};

struct TrackTimestampInfo {
  uint64_t timescale{0};
  std::optional<uint64_t> clockId;
  uint64_t origin{0};
  std::optional<TimestampMapping> mapping;

  // A wall-clock Track with explicit timestamps in microseconds.
  static TrackTimestampInfo wallClockMicros();

  // nullopt when the Track does not carry these Properties.
  static folly::Expected<std::optional<TrackTimestampInfo>, TimestampError>
  fromTrackProperties(const Extensions& properties);

  void addTo(Extensions& properties, bool immutable = false) const;

  // The OBJECT_TIMESTAMP Property for an Object at `timestamp`, or nullopt
  // when a mapped Track needs no correction.
  folly::Expected<std::optional<Extension>, TimestampError>
  objectProperty(uint64_t group, uint64_t object, uint64_t timestamp) const;

  // The Object's timestamp, or nullopt when it has none.
  folly::Expected<std::optional<uint64_t>, TimestampError> objectTimestamp(
      const ObjectHeader& header) const;

  bool isWallClock() const {
    return !clockId.has_value();
  }

  std::optional<uint64_t> clockTime(uint64_t timestamp) const;

  // Wall-clock conversions. nullopt on a Track that is not on wall clock or
  // when the value is out of range.
  std::optional<std::chrono::system_clock::time_point> toWallClock(
      uint64_t timestamp) const;
  std::optional<uint64_t> fromWallClock(
      std::chrono::system_clock::time_point when) const;

  bool operator==(const TrackTimestampInfo&) const = default;
};

} // namespace moxygen
