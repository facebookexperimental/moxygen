/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/samples/media_server/MoQBroadcastDispatcher.h>
#include <moxygen/samples/media_server/MoQMediaServer.h>

#include <folly/SocketAddress.h>

#include <memory>
#include <string>

namespace moxygen::media_server {

struct MediaListenerOptions {
  bool quic{true};
  bool qmux{true};
  std::string cert;
  std::string key;
  // Use the built-in insecure certificate instead of cert/key.
  bool insecure{false};
};

// The QUIC/WebTransport and QMUX listeners of one media server. Both hand their
// sessions to the same dispatcher and run on its EventBase.
struct MediaListeners {
  MediaListeners() = default;
  MediaListeners(MediaListeners&&) = default;
  // Stops the listeners being replaced: MoQServer does not stop itself when
  // destroyed.
  MediaListeners& operator=(MediaListeners&& other);
  ~MediaListeners() {
    stop();
  }

  // Must not be called on the dispatcher's EventBase. Idempotent.
  void stop();

  // Null when that listener is disabled or stopped.
  std::shared_ptr<MoQMediaServer> quic;
  std::shared_ptr<MoQMediaQmuxServer> qmux;
  // The dispatcher's EventBase, which stop() must not run on.
  folly::EventBase* evb{nullptr};
};

// Both listeners bind the same port number (UDP for QUIC, TCP for QMUX). With
// port 0 the number is picked to be free for both. Throws if a listener fails
// to start, after stopping any already started.
MediaListeners startMediaListeners(
    std::shared_ptr<MoQBroadcastDispatcher> dispatcher,
    const folly::SocketAddress& addr,
    const MediaListenerOptions& options);

} // namespace moxygen::media_server
