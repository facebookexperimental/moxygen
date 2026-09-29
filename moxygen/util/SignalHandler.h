/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/logging/xlog.h>
#include <signal.h>
#include <functional>
#include <utility>

namespace moxygen {

/**
 * SignalHandler - Signal handler for MoQ servers
 *
 * Handles SIGINT and SIGTERM signals, executing an optional cleanup callback
 * and terminating the event loop if terminateLoop is set. Always restores
 * default signal handlers to allow forced termination with a second signal.
 */
class SignalHandler : public folly::AsyncSignalHandler {
 public:
  explicit SignalHandler(
      folly::EventBase* evb,
      std::function<void(int)> cleanup_fn = nullptr,
      bool terminateLoop = true)
      : AsyncSignalHandler(evb),
        cleanup_fn_(std::move(cleanup_fn)),
        terminateLoop_(terminateLoop) {
    registerSignalHandler(SIGINT);
    registerSignalHandler(SIGTERM);
  }

  // Stops handling signals. While registered, this keeps evb.loop() running.
  void unregister() {
    if (std::exchange(registered_, false)) {
      unregisterSignalHandler(SIGINT);
      unregisterSignalHandler(SIGTERM);
    }
  }

  void signalReceived(int signum) noexcept override {
    XLOG(INFO) << "Received signal " << signum;

    if (!stopped_) {
      stopped_ = true;

      // Execute custom cleanup
      if (cleanup_fn_) {
        cleanup_fn_(signum);
      }

      if (terminateLoop_) {
        getEventBase()->terminateLoopSoon();
      }

      unregister();

      // Restore defaults (allows Ctrl-C twice to force quit)
      signal(SIGINT, SIG_DFL);
      signal(SIGTERM, SIG_DFL);
    }
  }

 private:
  std::function<void(int)> cleanup_fn_;
  bool terminateLoop_;
  bool stopped_{false};
  bool registered_{true};
};

} // namespace moxygen
