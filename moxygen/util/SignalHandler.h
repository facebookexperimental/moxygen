/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/logging/xlog.h>
#include <signal.h>
#include <functional>

namespace moxygen {

/**
 * SignalHandler - Signal handler for MoQ servers
 *
 * Handles SIGINT and SIGTERM signals, executing an optional cleanup callback
 * and terminating the event loop. Always restores default signal handlers
 * to allow forced termination with a second signal.
 */
class SignalHandler : public folly::AsyncSignalHandler {
 public:
  explicit SignalHandler(
      folly::EventBase* evb,
      std::function<void(int)> cleanup_fn = nullptr)
      : AsyncSignalHandler(evb), cleanup_fn_(std::move(cleanup_fn)) {
    registerSignalHandler(SIGINT);
    registerSignalHandler(SIGTERM);
  }

  void signalReceived(int signum) noexcept override {
    XLOG(INFO) << "Received signal " << signum;

    if (!stopped_) {
      stopped_ = true;

      // Execute custom cleanup
      if (cleanup_fn_) {
        cleanup_fn_(signum);
      }

      // Terminate event loop
      getEventBase()->terminateLoopSoon();

      // Unregister handlers
      unregisterSignalHandler(SIGINT);
      unregisterSignalHandler(SIGTERM);

      // Restore defaults (allows Ctrl-C twice to force quit)
      signal(SIGINT, SIG_DFL);
      signal(SIGTERM, SIG_DFL);
    }
  }

 private:
  std::function<void(int)> cleanup_fn_;
  bool stopped_{false};
};

} // namespace moxygen
