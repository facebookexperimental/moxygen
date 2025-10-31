/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>
#include <folly/container/F14Map.h>
#include <quic/common/events/QuicTimer.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/events/MoQExecutor.h>
#include <chrono>
#include <functional>
#include <memory>

// Forward declarations
namespace moxygen {

class ObjectTimerCallback;

// Callback type for session close functionality
using StreamResetCallback = std::function<void(ResetStreamErrorCode)>;

// Callback type for getting current RTT
using RttGetterCallback = std::function<std::chrono::microseconds()>;

/*
 * Stream scoped delivery timeout manager used to track per object delivery
 * timeout.
 */
class MoQDeliveryTimer {
 public:
  MoQDeliveryTimer(
      std::shared_ptr<MoQExecutor> exec,
      std::chrono::milliseconds deliveryTimeout,
      RttGetterCallback rttGetter = nullptr)
      : exec_(std::move(exec)),
        rttGetter_(std::move(rttGetter)),
        deliveryTimeout_(deliveryTimeout) {
    XLOG(DBG3) << "Setting delivery timeout to " << deliveryTimeout_.count()
               << "ms";
  }
  ~MoQDeliveryTimer();

  /*
   * Sets the callback to be called when a stream is reset if timed-out.
   */
  void setStreamResetCallback(StreamResetCallback streamResetCallback);

  /*
   * Sets the delivery timeout.
   */
  void setDeliveryTimeout(std::chrono::milliseconds timeout);

  /*
   * Sets the RTT getter callback.
   */
  void setRttGetter(RttGetterCallback rttGetter);

  /**
   * Start the delivery timeout timer
   */
  void startTimer(uint64_t objectId);

  /**
   * Cancel the active delivery timeout timer for a specific object
   */
  void cancelTimer(uint64_t objectId);

  /**
   * Cancel all active delivery timeout timers for a specific stream
   */
  void cancelAllTimers();

 private:
  /*
   * Calculates the effective timeout that will be used for object timers
   */
  std::chrono::milliseconds calculateTimeout();

  std::shared_ptr<MoQExecutor> exec_;
  StreamResetCallback streamResetCallback_;
  RttGetterCallback rttGetter_;
  // (objectId -> timer)
  folly::F14FastMap<uint64_t, std::unique_ptr<ObjectTimerCallback>>
      objectTimers_;
  std::chrono::milliseconds deliveryTimeout_;
};

class ObjectTimerCallback : public quic::QuicTimerCallback {
 public:
  ObjectTimerCallback(
      uint64_t objectId,
      StreamResetCallback streamResetCallback)
      : objectId_(objectId),
        streamResetCallback_(std::move(streamResetCallback)) {}
  void timeoutExpired() noexcept override;

 private:
  uint64_t objectId_;
  StreamResetCallback streamResetCallback_;
};

} // namespace moxygen
