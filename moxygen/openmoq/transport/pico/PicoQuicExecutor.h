/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Function.h>
#include <memory>
#include <moxygen/events/MoQExecutor.h>
#include <quic/common/events/QuicEventBase.h>

namespace moxygen {

/**
 * PicoQuicExecutor - Executor implementation for picoquic event loop
 *
 * This executor integrates with picoquic's packet loop callbacks to:
 * - Execute tasks queued via add()
 * - Fire timers scheduled via scheduleTimeout()
 * - Set epoll timeout to 0 when work is pending
 *
 * Usage:
 *   auto executor = std::make_shared<PicoQuicExecutor>();
 *   // Pass PicoQuicExecutor::getLoopCallback() to picoquic_packet_loop
 *   // Pass executor.get() as loop_callback_ctx
 */
class PicoQuicExecutor : public MoQExecutor {
public:
  PicoQuicExecutor();
  ~PicoQuicExecutor() override;

  // folly::Executor interface
  void add(folly::Func f) override;

  // MoQExecutor interface
  void scheduleTimeout(quic::QuicTimerCallback *callback,
                       std::chrono::milliseconds timeout) override;

  // Get the loop callback function to pass to picoquic_packet_loop.
  // Returns a picoquic_packet_loop_cb_fn (defined in picoquic_packet_loop.h).
  // Declared as void* to avoid exposing picoquic headers; callers in .cpp
  // files that include picoquic can reinterpret_cast.
  static void *getLoopCallback();

  // Process work - called from the packet loop callback
  void drainTasks();
  void processExpiredTimers(uint64_t currentTime);
  bool hasPendingWork() const;
  int64_t getNextTimeoutDelta(uint64_t currentTime) const;

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace moxygen
