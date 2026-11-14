/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQTimestamp.h>

#include <moxygen/MoQVarint.h>

#include <folly/CppAttributes.h>
#include <folly/io/Cursor.h>
#include <folly/io/IOBufQueue.h>

#include <limits>

namespace moxygen {

namespace {

constexpr uint64_t kNanosPerSecond = 1'000'000'000;

folly::Unexpected<TimestampError> malformed(std::string reason) {
  return folly::makeUnexpected(TimestampError{std::move(reason)});
}

bool isTrackPropertyType(uint64_t type) {
  return type == kTimescalePropertyType || type == kClockIdPropertyType ||
      type == kTimestampOriginPropertyType ||
      type == kTimestampMappingPropertyType;
}

// Counts across both lists so a Property repeated in each is caught.
const Extension* FOLLY_NULLABLE
find(const Extensions& extensions, uint64_t type, size_t& count) {
  const Extension* first = nullptr;
  count = 0;
  for (const auto* list :
       {&extensions.getMutableExtensions(),
        &extensions.getImmutableExtensions()}) {
    for (const auto& ext : *list) {
      if (ext.type == type) {
        if (!first) {
          first = &ext;
        }
        ++count;
      }
    }
  }
  return first;
}

// An unsigned 128-bit value, wide enough for any product of two uint64_t.
struct Wide {
  uint64_t hi{0};
  uint64_t lo{0};

  static Wide product(uint64_t a, uint64_t b) {
    constexpr uint64_t kLow32 = 0xFFFFFFFF;
    const uint64_t ll = (a & kLow32) * (b & kLow32);
    const uint64_t lh = (a & kLow32) * (b >> 32);
    const uint64_t hl = (a >> 32) * (b & kLow32);
    const uint64_t hh = (a >> 32) * (b >> 32);
    const uint64_t mid = (ll >> 32) + (lh & kLow32) + (hl & kLow32);
    return {
        hh + (lh >> 32) + (hl >> 32) + (mid >> 32),
        (mid << 32) | (ll & kLow32)};
  }

  // false on overflow past 2^128-1.
  bool add(const Wide& other) {
    uint64_t newLo = 0;
    uint64_t newHi = 0;
    const bool carry = __builtin_add_overflow(lo, other.lo, &newLo);
    if (__builtin_add_overflow(hi, other.hi, &newHi) ||
        __builtin_add_overflow(newHi, uint64_t{carry}, &newHi)) {
      return false;
    }
    hi = newHi;
    lo = newLo;
    return true;
  }

  bool operator<(const Wide& other) const {
    return hi != other.hi ? hi < other.hi : lo < other.lo;
  }

  // Requires *this >= other.
  Wide minus(const Wide& other) const {
    return {hi - other.hi - (lo < other.lo ? 1 : 0), lo - other.lo};
  }
};

// An exact sum of signed terms, so that large ones cancel. A side exceeds 2^128
// only if it includes both products, which puts the total out of range.
class SignedSum {
 public:
  void add(const Wide& term, bool negative) {
    if (!(negative ? negative_ : positive_).add(term)) {
      overflow_ = true;
    }
  }

  void add(uint64_t term, bool negative) {
    add(Wide{0, term}, negative);
  }

  void addCorrection(int64_t correction) {
    // Negating in unsigned arithmetic keeps INT64_MIN representable.
    add(correction < 0 ? 0 - static_cast<uint64_t>(correction)
                       : static_cast<uint64_t>(correction),
        correction < 0);
  }

  struct Total {
    uint64_t magnitude{0};
    bool negative{false};
  };

  // nullopt when the total's magnitude exceeds 2^64-1.
  std::optional<Total> total() const {
    if (overflow_) {
      return std::nullopt;
    }
    const bool negative = positive_ < negative_;
    const Wide magnitude =
        negative ? negative_.minus(positive_) : positive_.minus(negative_);
    if (magnitude.hi != 0) {
      return std::nullopt;
    }
    return Total{magnitude.lo, negative};
  }

 private:
  Wide positive_;
  Wide negative_;
  bool overflow_{false};
};

// Adds the mapped timestamp's terms to `sum`, negated when `subtract` is set.
void addMapped(
    SignedSum& sum,
    const TimestampMapping& m,
    uint64_t group,
    uint64_t object,
    bool subtract) {
  const bool beforeBase = group < m.baseGroup;
  const uint64_t groupDelta =
      beforeBase ? m.baseGroup - group : group - m.baseGroup;
  sum.add(m.baseTimestamp, subtract);
  sum.add(Wide::product(groupDelta, m.groupMultiplier), beforeBase != subtract);
  sum.add(Wide::product(object, m.objectMultiplier), subtract);
}

// value * numerator / denominator, exact when the product fits in 64 bits.
std::optional<uint64_t>
scale(uint64_t value, uint64_t numerator, uint64_t denominator) {
  uint64_t whole = 0;
  uint64_t fraction = 0;
  if (__builtin_mul_overflow(value / denominator, numerator, &whole)) {
    return std::nullopt;
  }
  uint64_t remainder = value % denominator;
  if (__builtin_mul_overflow(remainder, numerator, &fraction)) {
    fraction = static_cast<uint64_t>(
        static_cast<long double>(remainder) * numerator / denominator);
  } else {
    fraction /= denominator;
  }
  uint64_t result = 0;
  if (__builtin_add_overflow(whole, fraction, &result)) {
    return std::nullopt;
  }
  return result;
}

} // namespace

uint64_t zigzagEncode(int64_t value) {
  return (static_cast<uint64_t>(value) << 1) ^
      static_cast<uint64_t>(value >> 63);
}

int64_t zigzagDecode(uint64_t value) {
  return static_cast<int64_t>((value >> 1) ^ (0 - (value & 1)));
}

std::unique_ptr<folly::IOBuf> TimestampMapping::encode() const {
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender appender(&queue, 4 * 9);
  for (auto value :
       {baseGroup, baseTimestamp, groupMultiplier, objectMultiplier}) {
    encodeMoQVarint(value, appender);
  }
  return queue.move();
}

folly::Expected<TimestampMapping, TimestampError> TimestampMapping::parse(
    const folly::IOBuf& value) {
  folly::io::Cursor cursor(&value);
  TimestampMapping mapping;
  for (auto* field :
       {&mapping.baseGroup,
        &mapping.baseTimestamp,
        &mapping.groupMultiplier,
        &mapping.objectMultiplier}) {
    auto decoded = decodeMoQVarint(cursor, cursor.totalLength());
    if (!decoded) {
      return malformed("TIMESTAMP_MAPPING has fewer than four integers");
    }
    *field = decoded->first;
  }
  if (!cursor.isAtEnd()) {
    return malformed("TIMESTAMP_MAPPING has trailing bytes");
  }
  return mapping;
}

folly::Expected<uint64_t, TimestampError> TimestampMapping::timestamp(
    uint64_t group,
    uint64_t object,
    int64_t correction) const {
  SignedSum sum;
  addMapped(sum, *this, group, object, false);
  sum.addCorrection(correction);
  auto total = sum.total();
  if (!total) {
    return malformed("Object timestamp exceeds 2^64-1");
  }
  if (total->negative) {
    return malformed("Object timestamp is negative");
  }
  return total->magnitude;
}

folly::Expected<int64_t, TimestampError> TimestampMapping::correction(
    uint64_t group,
    uint64_t object,
    uint64_t timestamp) const {
  SignedSum sum;
  sum.add(timestamp, false);
  addMapped(sum, *this, group, object, true);
  constexpr auto kMax =
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
  auto total = sum.total();
  if (!total || total->magnitude > kMax + total->negative) {
    return malformed("correction does not fit in an int64");
  }
  return total->negative ? static_cast<int64_t>(0 - total->magnitude)
                         : static_cast<int64_t>(total->magnitude);
}

std::optional<AbsoluteLocation> TimestampMapping::locate(
    uint64_t timestamp) const {
  if (groupMultiplier == 0) {
    return std::nullopt;
  }
  uint64_t group = 0;
  uint64_t offset = 0;
  if (timestamp >= baseTimestamp) {
    uint64_t delta = timestamp - baseTimestamp;
    if (__builtin_add_overflow(baseGroup, delta / groupMultiplier, &group)) {
      return std::nullopt;
    }
    offset = delta % groupMultiplier;
  } else {
    // floor() of a negative quotient rounds away from zero.
    uint64_t delta = baseTimestamp - timestamp;
    uint64_t remainder = delta % groupMultiplier;
    uint64_t groups = delta / groupMultiplier + (remainder != 0);
    if (groups > baseGroup) {
      return std::nullopt;
    }
    group = baseGroup - groups;
    offset = remainder != 0 ? groupMultiplier - remainder : 0;
  }
  uint64_t object = objectMultiplier != 0 ? offset / objectMultiplier : 0;
  return AbsoluteLocation{group, object};
}

TrackTimestampInfo TrackTimestampInfo::wallClockMicros() {
  TrackTimestampInfo info;
  info.timescale = 1'000'000;
  return info;
}

folly::Expected<std::optional<TrackTimestampInfo>, TimestampError>
TrackTimestampInfo::fromTrackProperties(const Extensions& properties) {
  size_t count = 0;
  find(properties, kObjectTimestampPropertyType, count);
  if (count != 0) {
    return malformed("OBJECT_TIMESTAMP is not a Track Property");
  }
  size_t timescaleCount = 0;
  size_t clockIdCount = 0;
  size_t originCount = 0;
  size_t mappingCount = 0;
  const auto* timescale =
      find(properties, kTimescalePropertyType, timescaleCount);
  const auto* clockId = find(properties, kClockIdPropertyType, clockIdCount);
  const auto* origin =
      find(properties, kTimestampOriginPropertyType, originCount);
  const auto* mapping =
      find(properties, kTimestampMappingPropertyType, mappingCount);
  if (!timescale && !clockId && !origin && !mapping) {
    return std::nullopt;
  }
  if (timescaleCount > 1 || clockIdCount > 1 || originCount > 1 ||
      mappingCount > 1) {
    return malformed("timestamp Track Property appears more than once");
  }
  if (!timescale || timescale->intValue == 0) {
    return malformed("TIMESCALE is missing or 0");
  }
  TrackTimestampInfo info;
  info.timescale = timescale->intValue;
  if (clockId) {
    info.clockId = clockId->intValue;
  }
  if (origin) {
    info.origin = origin->intValue;
  }
  if (mapping) {
    if (!mapping->arrayValue) {
      return malformed("TIMESTAMP_MAPPING is empty");
    }
    auto parsed = TimestampMapping::parse(*mapping->arrayValue);
    if (parsed.hasError()) {
      return folly::makeUnexpected(std::move(parsed.error()));
    }
    info.mapping = *parsed;
  }
  return info;
}

void TrackTimestampInfo::addTo(Extensions& properties, bool immutable) const {
  auto insert = [&](Extension&& ext) {
    if (immutable) {
      properties.insertImmutableExtension(std::move(ext));
    } else {
      properties.insertMutableExtension(std::move(ext));
    }
  };
  insert(Extension{kTimescalePropertyType, timescale});
  if (clockId) {
    insert(Extension{kClockIdPropertyType, *clockId});
  }
  if (origin != 0) {
    insert(Extension{kTimestampOriginPropertyType, origin});
  }
  if (mapping) {
    insert(Extension{kTimestampMappingPropertyType, mapping->encode()});
  }
}

folly::Expected<std::optional<Extension>, TimestampError>
TrackTimestampInfo::objectProperty(
    uint64_t group,
    uint64_t object,
    uint64_t timestamp) const {
  if (!mapping) {
    return Extension{kObjectTimestampPropertyType, timestamp};
  }
  auto correction = mapping->correction(group, object, timestamp);
  if (correction.hasError()) {
    return folly::makeUnexpected(std::move(correction.error()));
  }
  if (*correction == 0) {
    return std::nullopt;
  }
  return Extension{kObjectTimestampPropertyType, zigzagEncode(*correction)};
}

folly::Expected<std::optional<uint64_t>, TimestampError>
TrackTimestampInfo::objectTimestamp(const ObjectHeader& header) const {
  const Extension* property = nullptr;
  for (const auto* list :
       {&header.extensions.getMutableExtensions(),
        &header.extensions.getImmutableExtensions()}) {
    for (const auto& ext : *list) {
      if (isTrackPropertyType(ext.type)) {
        return malformed("timestamp Track Property on an Object");
      }
      if (ext.type == kObjectTimestampPropertyType) {
        if (property) {
          return malformed("OBJECT_TIMESTAMP appears more than once");
        }
        property = &ext;
      }
    }
  }
  if (!mapping) {
    if (!property) {
      return std::nullopt;
    }
    return property->intValue;
  }
  int64_t correction = property ? zigzagDecode(property->intValue) : 0;
  auto timestamp = mapping->timestamp(header.group, header.id, correction);
  if (timestamp.hasError()) {
    return folly::makeUnexpected(std::move(timestamp.error()));
  }
  return *timestamp;
}

std::optional<uint64_t> TrackTimestampInfo::clockTime(
    uint64_t timestamp) const {
  uint64_t result = 0;
  if (__builtin_add_overflow(origin, timestamp, &result)) {
    return std::nullopt;
  }
  return result;
}

std::optional<std::chrono::system_clock::time_point>
TrackTimestampInfo::toWallClock(uint64_t timestamp) const {
  if (!isWallClock() || timescale == 0) {
    return std::nullopt;
  }
  auto ticks = clockTime(timestamp);
  if (!ticks) {
    return std::nullopt;
  }
  auto nanos = scale(*ticks, kNanosPerSecond, timescale);
  if (!nanos ||
      *nanos > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return std::nullopt;
  }
  return std::chrono::system_clock::time_point(
      std::chrono::duration_cast<std::chrono::system_clock::duration>(
          std::chrono::nanoseconds(static_cast<int64_t>(*nanos))));
}

std::optional<uint64_t> TrackTimestampInfo::fromWallClock(
    std::chrono::system_clock::time_point when) const {
  auto sinceEpoch = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        when.time_since_epoch())
                        .count();
  if (!isWallClock() || timescale == 0 || sinceEpoch < 0) {
    return std::nullopt;
  }
  auto ticks =
      scale(static_cast<uint64_t>(sinceEpoch), timescale, kNanosPerSecond);
  if (!ticks || *ticks < origin) {
    return std::nullopt;
  }
  return *ticks - origin;
}

} // namespace moxygen
