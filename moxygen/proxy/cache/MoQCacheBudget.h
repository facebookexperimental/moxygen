/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstddef>
#include <memory>
#include <optional>

namespace moxygen {

// Accounted bytes shared by every group cache and accepted replay of one
// proxy. Not thread-safe: a proxy and its tracks run on one event loop.
class MoQCacheBudget {
 public:
  explicit MoQCacheBudget(size_t capacity) : capacity_(capacity) {}

  bool tryReserve(size_t bytes);
  void release(size_t bytes);

  size_t used() const {
    return used_;
  }
  size_t capacity() const {
    return capacity_;
  }

 private:
  size_t capacity_;
  size_t used_{0};
};

// One reservation against a MoQCacheBudget, released on destruction.
class MoQCacheCharge {
 public:
  MoQCacheCharge() = default;
  static std::optional<MoQCacheCharge> reserve(
      const std::shared_ptr<MoQCacheBudget>& budget,
      size_t bytes);

  MoQCacheCharge(MoQCacheCharge&& other) noexcept;
  MoQCacheCharge& operator=(MoQCacheCharge&& other) noexcept;
  MoQCacheCharge(const MoQCacheCharge&) = delete;
  MoQCacheCharge& operator=(const MoQCacheCharge&) = delete;
  ~MoQCacheCharge();

  size_t bytes() const {
    return bytes_;
  }

 private:
  MoQCacheCharge(std::shared_ptr<MoQCacheBudget> budget, size_t bytes)
      : budget_(std::move(budget)), bytes_(bytes) {}
  void releaseCharge();

  std::shared_ptr<MoQCacheBudget> budget_;
  size_t bytes_{0};
};

} // namespace moxygen
