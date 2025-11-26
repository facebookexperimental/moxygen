/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/events/MoQDeliveryTimer.h>

namespace moxygen {

MoQDeliveryTimer::~MoQDeliveryTimer() {
  cancelAllTimers();
}

void MoQDeliveryTimer::setDeliveryTimeout(std::chrono::milliseconds timeout) {
  if (timeout.count() == 0) {
    XLOG(DBG6)
        << "MoQDeliveryTimer::setDeliveryTimeout: Ignoring delivery timeout update of 0ms";
    return;
  }
  XLOG(DBG6) << "MoQDeliveryTimer::setDeliveryTimeout: TIMER VALUE UPDATED"
             << " timeout=" << timeout.count() << "ms";
  deliveryTimeout_ = timeout;
}

void MoQDeliveryTimer::startTimer(
    uint64_t objId,
    std::chrono::microseconds srtt) {
  XCHECK(exec_) << "MoQDeliveryTimer::startTimer: exec_ is not set";

  auto callback = std::make_unique<ObjectTimerCallback>(objId, *this);
  auto* callbackPtr = callback.get();
  auto [it, inserted] = objectTimers_.emplace(objId, std::move(callback));

  if (inserted) {
    auto timeout = calculateTimeout(srtt);
    exec_->scheduleTimeout(callbackPtr, timeout);
  } else {
    // ObjectID already sent, this is an internal error (?)
    XLOG(ERR) << "MoQDeliveryTimer::startTimer: Internal Error::ObjectID "
              << objId << " already has a timer.";
    streamResetCallback_(ResetStreamErrorCode::INTERNAL_ERROR);
  }
}

std::chrono::milliseconds MoQDeliveryTimer::calculateTimeout(
    std::chrono::microseconds srtt) {
  // Calculate timeout: deliveryTimeout + rtt/2 (one-way latency)
  std::chrono::milliseconds timeout;

  if (srtt.count() > 0) {
    // RTT available: deliveryTimeout + one-way latency (rtt/2)
    // Rounded up
    auto oneWayLatency =
        std::chrono::ceil<std::chrono::milliseconds>(srtt / 2.0);
    timeout = deliveryTimeout_ + oneWayLatency;

    XLOG(DBG6)
        << "Using RTT-adjusted timeout: " << timeout.count()
        << "ms (base: " << deliveryTimeout_.count()
        << "ms + one-way latency: " << oneWayLatency.count() << "ms, srtt: "
        << std::chrono::duration_cast<std::chrono::milliseconds>(srtt).count()
        << "ms)";
  } else {
    // RTT is 0 or not available: fall back to doubling
    timeout = deliveryTimeout_ * 2;
    XLOG(DBG6) << "RTT not available, using fallback timeout (2x)"
               << timeout.count() << "ms";
  }
  return timeout;
}

void MoQDeliveryTimer::cancelTimer(uint64_t objId) {
  auto timerIt = objectTimers_.find(objId);
  if (timerIt == objectTimers_.end()) {
    // Could not find the timer. This is normally possible when timer has
    // started but pendingDeliveries_ does not have the object yet, because the
    // last part has not been sent. At that point, if the stream is reset, the
    // timer is canceled. If we still end up getting ack, we end up here

    // MoQSession::onByteEventCanceled() will call cancelTimer() potentially
    // all offsets.  eg: if we register timers for 0, 1, 2 and 0 is cancelled.
    // We still get onByteEventCanceled for 1 and 2.
    XLOG(DBG5) << "MoQDeliveryTimer::cancelTimer: ERROR: ObjectID " << objId
               << " does not have a timer";
    return;
  }
  objectTimers_.erase(objId);
}

void MoQDeliveryTimer::cancelAllTimers() {
  // Cancel all timers for objects in this stream
  // The destructors will call cancelImpl() without invoking callbacks
  XLOG(DBG6) << "MoQDeliveryTimer::cancelAllTimers: Canceling all "
             << objectTimers_.size() << " pending timers";
  objectTimers_.clear();
  XLOG(DBG2) << "MoQDeliveryTimer::cancelAllTimers: ALL TIMERS CANCELED";
}

void ObjectTimerCallback::timeoutExpired() noexcept {
  XLOG(DBG6) << "MoQDeliveryTimer::timeoutExpired: ObjectID " << objectId_
             << " timed out.";
  if (owner_.streamResetCallback_) {
    owner_.streamResetCallback_(ResetStreamErrorCode::DELIVERY_TIMEOUT);
  }
}

} // namespace moxygen
