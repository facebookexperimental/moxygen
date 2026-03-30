/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Executor.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/EventBase.h>
#include <memory>
#include <moxygen/MoQSession.h>
#include <moxygen/events/MoQExecutor.h>
#include <string>

// Forward declarations to avoid exposing picoquic.h
typedef struct st_picoquic_quic_t picoquic_quic_t;
typedef struct st_picoquic_cnx_t picoquic_cnx_t;

namespace moxygen {

/**
 * MoQPicoQuicEventBaseClient - MoQ client using picoquic on a folly::EventBase.
 *
 * Initiates an outgoing QUIC connection and establishes a MoQ session over it,
 * running entirely on a caller-supplied EventBase.
 *
 * Unlike MoQPicoQuicEventBaseServer there is no base class shared with a
 * thread-based variant — clients are expected to always run on an EventBase.
 * The picoquic callback is simpler than the server's because the connection
 * is created explicitly rather than arriving unexpectedly.
 *
 * Usage:
 *   class MyClient : public MoQPicoQuicEventBaseClient {
 *     void onSession(std::shared_ptr<MoQSession> session) override {
 *       session->setPublishHandler(...);
 *       // kick off subscribe / publish work
 *     }
 *   };
 *
 *   folly::EventBase evb;
 *   MyClient client("moq-relay.example.com",
 *                   folly::getKeepAliveToken(&evb));
 *   client.connect(serverAddr, "h3");
 *   evb.loop();
 */
class MoQPicoQuicEventBaseClient {
 public:
  /**
   * certRootFile: path to a CA bundle for server certificate verification,
   * or empty to skip verification (useful for testing).
   */
  MoQPicoQuicEventBaseClient(
      std::string endpoint,
      folly::Executor::KeepAlive<folly::EventBase> evb,
      std::string certRootFile = "");

  MoQPicoQuicEventBaseClient(const MoQPicoQuicEventBaseClient&) = delete;
  MoQPicoQuicEventBaseClient(MoQPicoQuicEventBaseClient&&) = delete;
  MoQPicoQuicEventBaseClient& operator=(
      const MoQPicoQuicEventBaseClient&) = delete;
  MoQPicoQuicEventBaseClient& operator=(
      MoQPicoQuicEventBaseClient&&) = delete;
  virtual ~MoQPicoQuicEventBaseClient();

  /**
   * Bind a local socket (ephemeral port) and initiate the QUIC connection to
   * addr. onSession() is called asynchronously when the MoQ session is ready.
   * Must be called from the EventBase thread (or before evb.loop()).
   *
   * sni:  TLS server name indication (usually the server hostname)
   * alpn: the QUIC ALPN to negotiate (e.g. "moqt-draft-07")
   */
  void connect(const folly::SocketAddress& addr,
               std::string sni,
               std::string alpn);

  /**
   * Close the connection and free all resources.
   * Must be called from the EventBase thread.
   */
  void close();

  /**
   * Called when the MoQ session is established and ready to use.
   * Override to configure handlers, start subscriptions, etc.
   */
  virtual void onSession(std::shared_ptr<MoQSession> session) = 0;

 protected:
  /**
   * Override to create a custom MoQSession subclass.
   * The default creates a plain MoQSession.
   */
  virtual std::shared_ptr<MoQSession> createSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      std::shared_ptr<MoQExecutor> executor);

  folly::EventBase* getEventBase() const;

  std::string endpoint_;

 private:
  // Called from picoCallback via friend when picoquic_callback_ready fires.
  void onConnectionReady(void* cnx);
  // Called from picoCallback via friend on connection close
  // (pre- or post-session).
  void onConnectionClosed();
  // Idempotent teardown: stops the handler, frees picoquic state.
  void doCleanup();

  friend struct MoQPicoQuicEventBaseClientCallbacks;

  struct Impl;
  std::unique_ptr<Impl> impl_;

  folly::Executor::KeepAlive<folly::EventBase> evb_;
  std::shared_ptr<MoQExecutor> executor_; // created in connect()
  std::string certRootFile_;
  std::string sni_;
  std::string alpn_;
};

} // namespace moxygen
