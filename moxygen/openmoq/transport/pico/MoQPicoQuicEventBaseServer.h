/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Executor.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/EventBase.h>
#include <moxygen/events/MoQExecutor.h>
#include <moxygen/openmoq/transport/pico/MoQPicoServerBase.h>
#include <memory>
#include <string>

namespace moxygen {

/**
 * MoQPicoQuicEventBaseServer - MoQ server using picoquic on a folly::EventBase.
 *
 * Runs picoquic I/O directly on a caller-supplied EventBase rather than in a
 * dedicated background thread. This allows picoquic sessions to share an event
 * loop with H3 connections or other EventBase work.
 *
 * The caller supplies a KeepAlive<EventBase>. The server creates a
 * MoQFollyExecutorImpl internally; sessions get real shared ownership of
 * the executor and it lives until the last session releases it.
 *
 * Usage:
 *   folly::EventBase evb;
 *   MoQPicoQuicEventBaseServer server(
 *       cert, key, endpoint, folly::getKeepAliveToken(&evb));
 *   server.start(addr);
 *   evb.loop();
 */
class MoQPicoQuicEventBaseServer : public MoQPicoServerBase {
 public:
  MoQPicoQuicEventBaseServer(
      std::string cert,
      std::string key,
      std::string endpoint,
      folly::Executor::KeepAlive<folly::EventBase> evb,
      std::string versions = "",
      PicoWebTransportConfig wtConfig = {});

  MoQPicoQuicEventBaseServer(const MoQPicoQuicEventBaseServer&) = delete;
  MoQPicoQuicEventBaseServer(MoQPicoQuicEventBaseServer&&) = delete;
  MoQPicoQuicEventBaseServer& operator=(const MoQPicoQuicEventBaseServer&) =
      delete;
  MoQPicoQuicEventBaseServer& operator=(MoQPicoQuicEventBaseServer&&) = delete;
  ~MoQPicoQuicEventBaseServer() override;

  /**
   * Bind the UDP socket and start receiving.
   * Must be called from the EventBase thread (or before evb.loop()).
   */
  void start(const folly::SocketAddress& addr) override;

  /**
   * Stop the server: cancel the wake timer, pause socket reads, free picoquic.
   * Must be called from the EventBase thread.
   */
  void stop() override;

 protected:
  void onWebTransportCreated(PicoWebTransportBase& wt) noexcept override;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

  folly::Executor::KeepAlive<folly::EventBase> evb_;
};

} // namespace moxygen
