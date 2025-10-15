/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/events/MoQLibevExecutorImpl.h>

namespace moxygen {

MoQLibevExecutorImpl::MoQLibevExecutorImpl(
    std::unique_ptr<quic::LibevQuicEventBase::EvLoopWeak> loop)
    : quic::LibevQuicEventBase(std::move(loop)) {}

void MoQLibevExecutorImpl::add(folly::Func func) {
  runInLoop(std::move(func).asStdFunction(), /*thisIteration=*/false);
}

void MoQLibevExecutorImpl::scheduleTimeout(
    quic::QuicTimerCallback* callback,
    std::chrono::milliseconds timeout) {
  quic::LibevQuicEventBase::scheduleTimeout(callback, timeout);
}

} // namespace moxygen
