/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/mlog/MLoggerFactory.h>
#include <memory>

namespace moxygen {

// Wraps a MLoggerFactory and applies probabilistic sampling.
// For each createMLogger() call, returns a logger with probability sampleRate,
// or nullptr otherwise (meaning the session is not logged). A rate <= 0 never
// logs and a rate >= 1 always logs; NaN is treated as "never".
//
// THREAD SAFETY: Uses folly::Random, which relies on ThreadLocalPRNG and is
// safe for concurrent calls.
class SamplingMLoggerFactory : public MLoggerFactory {
 public:
  SamplingMLoggerFactory(
      std::shared_ptr<MLoggerFactory> inner,
      float sampleRate);

  std::shared_ptr<MLogger> createMLogger() override;

 private:
  std::shared_ptr<MLoggerFactory> inner_;
  float sampleRate_;
};

} // namespace moxygen
