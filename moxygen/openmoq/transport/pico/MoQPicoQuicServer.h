/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/SocketAddress.h>
#include <moxygen/openmoq/transport/pico/MoQPicoServerBase.h>
#include <memory>
#include <string>

namespace moxygen {

/**
 * MoQPicoQuicServer - MoQ server using picoquic transport (thread-based).
 *
 * Creates a background thread running the picoquic packet loop.
 * For an EventBase-integrated alternative see MoQPicoQuicEventBaseServer.
 *
 * Supports two connection modes (configured via PicoWebTransportConfig):
 * - Raw MoQ: Direct MoQ over QUIC (ALPN: moqt-16, moqt-15, moq-00)
 * - WebTransport: MoQ over WebTransport over HTTP/3 (ALPN: h3)
 */
class MoQPicoQuicServer : public MoQPicoServerBase {
 public:
  MoQPicoQuicServer(
      std::string cert,
      std::string key,
      std::string endpoint,
      std::string versions = "",
      PicoWebTransportConfig wtConfig = {});

  MoQPicoQuicServer(const MoQPicoQuicServer&) = delete;
  MoQPicoQuicServer(MoQPicoQuicServer&&) = delete;
  MoQPicoQuicServer& operator=(const MoQPicoQuicServer&) = delete;
  MoQPicoQuicServer& operator=(MoQPicoQuicServer&&) = delete;
  ~MoQPicoQuicServer() override;

  /**
   * Start the server on the specified address.
   * Creates a background thread running the picoquic packet loop.
   */
  void start(const folly::SocketAddress& addr) override;

  /**
   * Stop the server and wait for the packet loop thread to finish.
   */
  void stop() override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace moxygen
