/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/cache/MoQCacheBudget.h"

#include <folly/logging/xlog.h>
#include <utility>

namespace moxygen {

bool MoQCacheBudget::tryReserve(size_t bytes) {
  if (bytes > capacity_ - used_) {
    return false;
  }
  used_ += bytes;
  return true;
}

void MoQCacheBudget::release(size_t bytes) {
  XCHECK_GE(used_, bytes);
  used_ -= bytes;
}

std::optional<MoQCacheCharge> MoQCacheCharge::reserve(
    const std::shared_ptr<MoQCacheBudget>& budget,
    size_t bytes) {
  if (!budget || !budget->tryReserve(bytes)) {
    return std::nullopt;
  }
  return MoQCacheCharge(budget, bytes);
}

MoQCacheCharge::MoQCacheCharge(MoQCacheCharge&& other) noexcept
    : budget_(std::move(other.budget_)),
      bytes_(std::exchange(other.bytes_, 0)) {}

MoQCacheCharge& MoQCacheCharge::operator=(MoQCacheCharge&& other) noexcept {
  if (this != &other) {
    releaseCharge();
    budget_ = std::move(other.budget_);
    bytes_ = std::exchange(other.bytes_, 0);
  }
  return *this;
}

MoQCacheCharge::~MoQCacheCharge() {
  releaseCharge();
}

void MoQCacheCharge::releaseCharge() {
  if (budget_) {
    budget_->release(bytes_);
  }
  budget_.reset();
  bytes_ = 0;
}

} // namespace moxygen
