/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Random.h>
#include <memory>
#include <moxygen/mlog/MLoggerFactory.h>

namespace moxygen {

// Wraps a MLoggerFactory and applies probabilistic sampling.
// For each createMLogger() call, returns a logger with probability sampleRate,
// or nullptr otherwise (meaning the session is not logged).
//
// THREAD SAFETY: Uses folly::Random::oneIn() which relies on ThreadLocalPRNG
// and is safe for concurrent calls. 
class SamplingMLoggerFactory : public MLoggerFactory {
 public:
  SamplingMLoggerFactory(
      std::shared_ptr<MLoggerFactory> inner,
      float sampleRate);

  std::shared_ptr<MLogger> createMLogger() override;

 private:
  std::shared_ptr<MLoggerFactory> inner_;
  float sampleRate_;
  uint32_t bucketSize_; // ceil(1 / sampleRate_)
};

} // namespace moxygen
