/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/events/MoQFollyExecutorImpl.h>

namespace moxygen {

void MoQFollyExecutorImpl::add(folly::Func func) {
  getBackingEventBase()->add(std::move(func));
}

void MoQFollyExecutorImpl::scheduleTimeout(
    quic::QuicTimerCallback* callback,
    std::chrono::milliseconds timeout) {
  quic::FollyQuicEventBase::scheduleTimeout(callback, timeout);
}

} // namespace moxygen
