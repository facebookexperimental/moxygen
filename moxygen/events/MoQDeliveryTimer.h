/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>
#include <quic/common/events/QuicTimer.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/events/MoQExecutor.h>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>

// Forward declarations
namespace moxygen {

class ObjectTimerCallback;

// Callback type for session close functionality
using StreamResetCallback = std::function<void(ResetStreamErrorCode)>;

/*
 * Stream scoped delivery timeout manager used to track per object delivery
 * timeout.
 */
class MoQDeliveryTimer {
 public:
  MoQDeliveryTimer(
      std::shared_ptr<MoQExecutor> exec,
      std::chrono::milliseconds deliveryTimeout,
      StreamResetCallback streamResetCallback)
      : exec_(std::move(exec)),
        streamResetCallback_(std::move(streamResetCallback)),
        deliveryTimeout_(deliveryTimeout) {
    XLOG(DBG3) << "Setting delivery timeout to " << deliveryTimeout_.count()
               << "ms";
  }
  ~MoQDeliveryTimer();

  /*
   * Sets the delivery timeout.
   */
  void setDeliveryTimeout(std::chrono::milliseconds timeout);

  /**
   * Start the delivery timeout timer
   */
  void startTimer(uint64_t objectId, std::chrono::microseconds srtt);

  /**
   * Cancel the active delivery timeout timer for a specific object
   */
  void cancelTimer(uint64_t objectId);

  /**
   * Cancel all active delivery timeout timers for a specific stream
   */
  void cancelAllTimers();

 private:
  friend class ObjectTimerCallback;

  /*
   * Calculates the effective timeout that will be used for object timers
   */
  std::chrono::milliseconds calculateTimeout(std::chrono::microseconds srtt);

  std::shared_ptr<MoQExecutor> exec_;
  StreamResetCallback streamResetCallback_;
  // (objectId -> timer)
  folly::F14FastMap<uint64_t, std::unique_ptr<ObjectTimerCallback>>
      objectTimers_;
  std::chrono::milliseconds deliveryTimeout_;
};

class ObjectTimerCallback : public quic::QuicTimerCallback {
 public:
  ObjectTimerCallback(uint64_t objectId, MoQDeliveryTimer& owner)
      : objectId_(objectId), owner_(owner) {}

  void timeoutExpired() noexcept override;

  // Don't invoke callback when timer is cancelled
  void callbackCanceled() noexcept override {
    XLOG(DBG6) << "MoQDeliveryTimer: ObjectID " << objectId_
               << " timer cancelled";
  }

 private:
  uint64_t objectId_;
  MoQDeliveryTimer& owner_;
};

} // namespace moxygen
