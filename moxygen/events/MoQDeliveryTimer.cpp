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

void MoQDeliveryTimer::setStreamResetCallback(
    StreamResetCallback streamResetCallback) {
  streamResetCallback_ = std::move(streamResetCallback);
}

void MoQDeliveryTimer::setDeliveryTimeout(std::chrono::milliseconds timeout) {
  if (timeout.count() == 0) {
    XLOG(DBG2) << "Ignoring delivery timeout update of 0ms";
    return;
  }
  XLOG(DBG3) << "Setting delivery timeout to " << timeout.count() << "ms";
  deliveryTimeout_ = timeout;
}

void MoQDeliveryTimer::startTimer(uint64_t objId) {
  XCHECK(exec_) << "MoQDeliveryTimer::exec_ is not set";

  if (objectTimers_.find(objId) != objectTimers_.end()) {
    // ObjectID already sent, this is an internal error (?)
    XLOG(ERR) << "Internal Error::ObjectID " << objId
              << " already has a timer.";
    streamResetCallback_(ResetStreamErrorCode::INTERNAL_ERROR);
  } else {
    auto callback =
        std::make_unique<ObjectTimerCallback>(objId, streamResetCallback_);
    auto* callbackPtr = callback.get();
    objectTimers_.emplace(objId, std::move(callback));

    // We assume that deliveryTimeout includes one-way latency, hence we x 2
    // for an estimate. This wiil change once RTT is available.
    auto rttCutoff = deliveryTimeout_ * 2;
    exec_->scheduleTimeout(callbackPtr, rttCutoff);
  }
}

void MoQDeliveryTimer::cancelTimer(uint64_t objId) {
  auto timerIt = objectTimers_.find(objId);
  if (timerIt != objectTimers_.end()) {
    objectTimers_.erase(objId);
  } else {
    // Could not find the timer, log error
    XLOG(ERR) << "Internal Error::ObjectID " << objId
              << " does not have a timer";
  }
}

void MoQDeliveryTimer::cancelAllTimers() {
  // Cancel all timers for objects in this stream
  XLOG(DBG3) << "Canceling all " << objectTimers_.size() << " pending timers";
  for (auto& [objId, timer] : objectTimers_) {
    timer->cancelTimerCallback();
  }
  objectTimers_.clear();
}

void ObjectTimerCallback::timeoutExpired() noexcept {
  XLOG(DBG3) << "ObjectID " << objectId_ << " timed out.";
  if (streamResetCallback_) {
    streamResetCallback_(ResetStreamErrorCode::DELIVERY_TIMEOUT);
  }
}

} // namespace moxygen
