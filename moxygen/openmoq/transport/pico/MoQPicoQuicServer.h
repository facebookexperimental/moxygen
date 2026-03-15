/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/SocketAddress.h>
#include <memory>
#include <moxygen/MoQServerBase.h>
#include <string>

namespace moxygen {

/**
 * MoQPicoQuicServer - MoQ server using picoquic transport
 *
 * This server creates WebTransport sessions over raw QUIC using picoquic,
 * as opposed to MoQServer which uses HTTP/3 WebTransport over proxygen.
 */
class MoQPicoQuicServer : public MoQServerBase {
public:
  MoQPicoQuicServer(
      std::string cert,
      std::string key,
      std::string endpoint,
      std::string versions = "");

  MoQPicoQuicServer(const MoQPicoQuicServer &) = delete;
  MoQPicoQuicServer(MoQPicoQuicServer &&) = delete;
  MoQPicoQuicServer &operator=(const MoQPicoQuicServer &) = delete;
  MoQPicoQuicServer &operator=(MoQPicoQuicServer &&) = delete;
  ~MoQPicoQuicServer() override;

  /**
   * Start the server on the specified address.
   * This will create a background thread running the picoquic packet loop.
   */
  void start(const folly::SocketAddress &addr);

  /**
   * Stop the server and wait for the packet loop thread to finish.
   */
  void stop();

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

} // namespace moxygen
