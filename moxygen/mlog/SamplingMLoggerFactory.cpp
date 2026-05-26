/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/mlog/SamplingMLoggerFactory.h"

namespace moxygen {

SamplingMLoggerFactory::SamplingMLoggerFactory(
    std::shared_ptr<MLoggerFactory> inner,
    float sampleRate)
    : inner_(std::move(inner)), sampleRate_(sampleRate) {
  // Validate and normalize sampleRate to valid range [0.0, 1.0]
  if (sampleRate_ <= 0.0f) {
    sampleRate_ = 0.0f;
    bucketSize_ = 0; // Sentinel: no sampling
  } else if (sampleRate_ >= 1.0f) {
    sampleRate_ = 1.0f;
    bucketSize_ = 1; // All sessions logged
  } else {
    // bucketSize = how many sessions per one logged session, e.g. 0.01 -> 100
    bucketSize_ = static_cast<uint32_t>(1.0f / sampleRate_);
    if (bucketSize_ == 0) {
      bucketSize_ = 1; // guard against rounding to zero for very high rates
    }
  }
}

std::shared_ptr<MLogger> SamplingMLoggerFactory::createMLogger() {
  if (bucketSize_ == 0) {
    // No sampling: sampleRate was <= 0
    return nullptr;
  }
  if (bucketSize_ == 1) {
    // Log all: sampleRate was >= 1
    return inner_->createMLogger();
  }
  // folly::Random::oneIn() is thread-safe and bucketSize_ is always > 1 here.
  if (folly::Random::oneIn(bucketSize_)) {
    return inner_->createMLogger();
  }
  return nullptr;
}

} // namespace moxygen
