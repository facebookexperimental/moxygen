/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/mlog/SamplingMLoggerFactory.h"

#include <folly/Random.h>

namespace moxygen {

SamplingMLoggerFactory::SamplingMLoggerFactory(
    std::shared_ptr<MLoggerFactory> inner,
    float sampleRate)
    : inner_(std::move(inner)), sampleRate_(sampleRate) {}

std::shared_ptr<MLogger> SamplingMLoggerFactory::createMLogger() {
  // randDouble01() yields [0, 1), so a rate of 0 never logs and a rate of 1
  // always logs. A NaN rate fails the comparison and never logs.
  if (folly::Random::randDouble01() < sampleRate_) {
    return inner_->createMLogger();
  }
  return nullptr;
}

} // namespace moxygen
