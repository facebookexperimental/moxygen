/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace moxygen {

// Fixed exponential latency bucket upper bounds, in milliseconds (inclusive).
// Chosen to resolve the single-digit-ms range where end-to-end latency normally
// lives while still capturing multi-second tails.
inline constexpr std::array<uint64_t, 13> kLatencyBucketsMs = {
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096};

// Plain (non-atomic) value type holding a fixed-boundary latency histogram,
// shaped for Prometheus export. Used to accumulate/merge snapshots and format
// output on a single thread. Live cross-thread accumulation lives in the client
// as atomics; snapshotLatencyHist() converts those into one of these.
//
// buckets_[i] counts observations in bucket i; buckets_[kNumBounds] is the +Inf
// overflow bucket (values above the last explicit bound).
class LatencyHistogram {
 public:
  static constexpr size_t kNumBounds = kLatencyBucketsMs.size();
  static constexpr size_t kNumBuckets = kNumBounds + 1;

  // Bucket a value falls into (kNumBounds == +Inf overflow). Single source of
  // truth so the atomic accumulator and addValue() agree.
  static size_t bucketIndex(uint64_t latencyMs) {
    for (size_t i = 0; i < kNumBounds; ++i) {
      if (latencyMs <= kLatencyBucketsMs[i]) {
        return i;
      }
    }
    return kNumBounds;
  }

  void addValue(uint64_t latencyMs) {
    ++count_;
    sum_ += latencyMs;
    ++buckets_[bucketIndex(latencyMs)];
  }

  void merge(const LatencyHistogram& other) {
    count_ += other.count_;
    sum_ += other.sum_;
    for (size_t i = 0; i < kNumBuckets; ++i) {
      buckets_[i] += other.buckets_[i];
    }
  }

  void addRawBucket(size_t i, uint64_t n) {
    buckets_[i] += n;
  }
  void addSum(uint64_t s) {
    sum_ += s;
  }
  void addCount(uint64_t c) {
    count_ += c;
  }

  // out[i] = cumulative count of observations <= kLatencyBucketsMs[i].
  // out[kNumBounds] = total count (the +Inf bucket).
  std::array<uint64_t, kNumBuckets> cumulative() const {
    std::array<uint64_t, kNumBuckets> out{};
    uint64_t running = 0;
    for (size_t i = 0; i < kNumBounds; ++i) {
      running += buckets_[i];
      out[i] = running;
    }
    out[kNumBounds] = count_;
    return out;
  }

  uint64_t sum() const {
    return sum_;
  }
  uint64_t count() const {
    return count_;
  }

 private:
  std::array<uint64_t, kNumBuckets> buckets_{};
  uint64_t sum_{0};
  uint64_t count_{0};
};

} // namespace moxygen
