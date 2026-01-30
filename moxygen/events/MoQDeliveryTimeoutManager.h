/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/logging/xlog.h>
#include <algorithm>
#include <chrono>
#include <functional>
#include <optional>

namespace moxygen {

/**
 * Manages delivery timeout from multiple sources and computes the effective
 * timeout as min(upstream, downstream).
 *
 * When the effective timeout changes, automatically invokes a registered
 * callback to notify the owner.
 */
class MoQDeliveryTimeoutManager {
 public:
  // Callback type invoked when effective timeout changes
  using OnChangeCallback =
      std::function<void(std::optional<std::chrono::milliseconds>)>;

  MoQDeliveryTimeoutManager() = default;

  // Set callback to be invoked when effective timeout changes
  void setOnChangeCallback(OnChangeCallback callback) {
    onChangeCallback_ = std::move(callback);
  }

  // Set upstream timeout (automatically triggers callback if effective changes)
  void setUpstreamTimeout(std::chrono::milliseconds timeout) {
    upstreamTimeout_ = timeout;
    notifyIfChanged();
  }

  // Set downstream timeout (automatically triggers callback if effective
  // changes)
  void setDownstreamTimeout(std::chrono::milliseconds timeout) {
    XLOG(DBG6)
        << "MoQDeliveryTimeoutManager::setDownstreamTimeout: SETTING downstream timeout"
        << " timeout=" << timeout.count() << "ms"
        << " previousDownstream="
        << (downstreamTimeout_.has_value()
                ? std::to_string(downstreamTimeout_->count()) + "ms"
                : "none");
    downstreamTimeout_ = timeout;
    notifyIfChanged();
  }

  // Get effective timeout as min of available sources
  std::optional<std::chrono::milliseconds> getEffectiveTimeout() const {
    if (upstreamTimeout_ && downstreamTimeout_) {
      return std::min(*upstreamTimeout_, *downstreamTimeout_);
    }
    if (upstreamTimeout_) {
      return upstreamTimeout_;
    }
    if (downstreamTimeout_) {
      return downstreamTimeout_;
    }
    return std::nullopt;
  }

 private:
  // Individual timeout sources
  std::optional<std::chrono::milliseconds> upstreamTimeout_;
  std::optional<std::chrono::milliseconds> downstreamTimeout_;

  // Cached effective timeout for change detection
  mutable std::optional<std::chrono::milliseconds> lastEffectiveTimeout_;

  // Callback invoked when effective timeout changes
  OnChangeCallback onChangeCallback_;

  // Check if effective timeout changed and notify callback
  void notifyIfChanged() {
    auto current = getEffectiveTimeout();
    if (current != lastEffectiveTimeout_) {
      XLOG(DBG6)
          << "MoQDeliveryTimeoutManager::notifyIfChanged: EFFECTIVE TIMEOUT CHANGED"
          << " previous="
          << (lastEffectiveTimeout_.has_value()
                  ? std::to_string(lastEffectiveTimeout_->count()) + "ms"
                  : "none")
          << " current="
          << (current.has_value() ? std::to_string(current->count()) + "ms"
                                  : "none")
          << " - INVOKING CALLBACK";
      lastEffectiveTimeout_ = current;
      if (onChangeCallback_) {
        onChangeCallback_(current);
      }
    } else {
      XLOG(DBG6)
          << "MoQDeliveryTimeoutManager::notifyIfChanged: Effective timeout unchanged"
          << " current="
          << (current.has_value() ? std::to_string(current->count()) + "ms"
                                  : "none")
          << " - NO CALLBACK";
    }
  }
};

} // namespace moxygen
