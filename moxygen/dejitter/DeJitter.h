/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>
#include <folly/logging/xlog.h>
#include <cstdint>
#include <map>

namespace moxygen::dejitter {

template <class T>
class DeJitter {
 public:
  enum class GapType : uint8_t {
    NO_GAP = 0x0,
    GAP = 0x1,
    ARRIVED_LATE = 0x2,
    FILLING_BUFFER = 0x3,
    INTERNAL_ERROR = 0xff,
  };
  struct GapInfo {
    GapType gapType;
    uint64_t gapSize;
    GapInfo() : gapType(GapType::NO_GAP), gapSize(0) {}
    GapInfo(GapType gapType, uint64_t gapSize)
        : gapType(gapType), gapSize(gapSize) {}
  };

  DeJitter(uint64_t bufferSizeMs, uint64_t avgItemSizeMs) {
    bufferSize_ = bufferSizeMs / avgItemSizeMs;
    CHECK_GT(bufferSize_, 0);
  }
  explicit DeJitter(uint64_t bufferSize) : bufferSize_(bufferSize) {
    CHECK_GT(bufferSize_, 0);
  }

  size_t size() const {
    return buffer_.size();
  }

  // Assuming pos in monotically increasing
  inline std::tuple<folly::Optional<T>, typename DeJitter<T>::GapInfo>
  insertItem(uint64_t pos, T item) {
    // Arrived late
    if (lastSent_.has_value() && pos <= lastSent_.value()) {
      return std::make_tuple(
          folly::none,
          GapInfo{DeJitter<T>::GapType::ARRIVED_LATE, lastSent_.value() - pos});
    }

    // Add to buffer
    buffer_.emplace(std::make_pair(pos, std::move(item)));
    if (buffer_.size() <= bufferSize_) {
      return std::make_tuple(
          folly::none, GapInfo{DeJitter<T>::GapType::FILLING_BUFFER, 0});
    }

    // Check next item
    folly::Optional<uint64_t> minVal;
    for (auto it = buffer_.begin(); it != buffer_.end(); it++) {
      if (!minVal.has_value() || it->first < minVal.value()) {
        minVal = it->first;
      }
      if (lastSent_.has_value()) {
        if (it->first == lastSent_.value() + 1) {
          lastSent_ = it->first;
          auto v = std::make_tuple(std::move(it->second), GapInfo{});
          buffer_.erase(it);
          return v;
        }
      }
    }
    if (!minVal.has_value()) {
      // Should never happen
      return std::make_tuple(
          folly::none, GapInfo{DeJitter<T>::GapType::INTERNAL_ERROR, 0});
    }

    auto v = std::move(buffer_[minVal.value()]);
    uint64_t gapSize = 0;
    if (lastSent_.has_value()) {
      gapSize = minVal.value() - lastSent_.value_or(0) - 1;
    }
    lastSent_ = minVal.value();
    buffer_.erase(minVal.value());
    // At start there is NO gat
    auto gap = (gapSize > 0) ? DeJitter<T>::GapType::GAP
                             : DeJitter<T>::GapType::NO_GAP;
    return std::make_tuple(std::move(v), GapInfo{gap, gapSize});
  }

 private:
  std::map<uint64_t, T> buffer_;
  uint64_t bufferSize_{0};
  folly::Optional<uint64_t> lastSent_;
};

} // namespace moxygen::dejitter
