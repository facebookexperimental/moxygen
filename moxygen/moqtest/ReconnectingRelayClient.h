/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/CancellationToken.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/Task.h>
#include <folly/logging/xlog.h>
#include "moxygen/relay/MoQRelayClient.h"

#include <algorithm>
#include <chrono>
#include <functional>

namespace moxygen {

// Keeps a namespace published on a relay across disconnects, by running a
// fresh MoQRelayClient per session.  A conformance run outlives any one
// session: relays do close us, and every test after that fails until we are
// published again.
class ReconnectingRelayClient {
 public:
  // Returns a client whose MoQ setup is complete, or null if the attempt
  // failed.
  using ConnectFn =
      std::function<folly::coro::Task<std::unique_ptr<MoQClientBase>>()>;

  explicit ReconnectingRelayClient(
      ConnectFn connect,
      std::chrono::milliseconds minBackoff = std::chrono::seconds(1),
      std::chrono::milliseconds maxBackoff = std::chrono::seconds(30))
      : connect_(std::move(connect)),
        minBackoff_(minBackoff),
        maxBackoff_(maxBackoff) {}

  // Connects, publishes `namespaces`, and holds the session open until it
  // ends, then starts over on a fresh one.  Returns only once stopped.
  folly::coro::Task<void> run(
      std::shared_ptr<Publisher> publisher,
      std::vector<TrackNamespace> namespaces) {
    auto callerToken = co_await folly::coro::co_current_cancellation_token;
    auto token =
        folly::cancellation_token_merge(stopSource_.getToken(), callerToken);
    auto backoff = minBackoff_;
    while (!token.isCancellationRequested()) {
      auto connected = co_await folly::coro::co_awaitTry(
          folly::coro::co_withCancellation(token, connect_()));
      if (token.isCancellationRequested()) {
        // A stop raced the connect; drop whatever it returned.
        break;
      }
      if (connected.hasException()) {
        XLOG(ERR) << "Relay connect failed: "
                  << folly::exceptionStr(connected.exception());
      } else if (connected.value()) {
        auto sessionStart = std::chrono::steady_clock::now();
        client_ =
            std::make_unique<MoQRelayClient>(std::move(connected.value()));
        // Publishes the namespaces, then pings until the session ends.
        co_await folly::coro::co_withCancellation(
            token, client_->run(publisher, namespaces));
        resetClient();
        if (token.isCancellationRequested()) {
          break;
        }
        if (std::chrono::steady_clock::now() - sessionStart >= kHealthy) {
          // The session lasted long enough to be useful, so treat the relay as
          // healthy and retry promptly.
          backoff = minBackoff_;
        }
        XLOG(INFO) << "Relay session ended, reconnecting in " << backoff.count()
                   << "ms";
      }
      auto slept = co_await folly::coro::co_awaitTry(
          folly::coro::co_withCancellation(token, folly::coro::sleep(backoff)));
      if (slept.hasException()) {
        break;
      }
      backoff = std::min(backoff * 2, maxBackoff_);
    }
    resetClient();
  }

  // Ends the loop.  Draining is what makes a close we asked for look different
  // from the relay dropping us, and it is also what wakes MoQRelayClient::run
  // out of its keepalive sleep.
  void stop() {
    stopSource_.requestCancellation();
    if (auto session = getSession()) {
      session->drain();
    }
  }

  std::shared_ptr<MoQSession> getSession() const {
    return client_ ? client_->getSession() : nullptr;
  }

 private:
  // MoQRelayClient::run outlives the session it was built on, so a session
  // that just ended must not be reused for the next attempt.
  void resetClient() {
    if (client_) {
      client_->shutdown();
      client_.reset();
    }
  }

  // A session shorter than one keepalive interval never proved anything, so
  // only longer ones reset the backoff.
  static constexpr std::chrono::seconds kHealthy{30};

  ConnectFn connect_;
  std::chrono::milliseconds minBackoff_;
  std::chrono::milliseconds maxBackoff_;
  std::unique_ptr<MoQRelayClient> client_;
  folly::CancellationSource stopSource_;
};

} // namespace moxygen
