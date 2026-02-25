/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/MoQPicoQuicServer.h"
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQVersions.h>
#include <moxygen/MoQSession.h>
#include <moxygen/mlog/MLogger.h>
#include <moxygen/openmoq/transport/pico/PicoQuicExecutor.h>
#include <moxygen/openmoq/transport/pico/PicoQuicWebTransport.h>
#include <picoquic.h>
#include <picoquic_bbr.h>
#include <picoquic_packet_loop.h>
#include <thread>

namespace moxygen {

// Per-connection context stored in picoquic's callback_ctx
struct ConnectionContext {
  // Magic value to identify this as a ConnectionContext vs server pointer
  static constexpr uint32_t kMagic = 0xC099EC71; // "CONNECT1"
  uint32_t magic{kMagic};

  std::shared_ptr<PicoQuicWebTransport> webTransport;
  std::shared_ptr<MoQSession> moqSession;
  MoQPicoQuicServer *server;
};

struct MoQPicoQuicServer::Impl {
  // Picoquic callback - static function that routes to instance methods
  static int picoCallback(picoquic_cnx_t *cnx, uint64_t stream_id,
                          uint8_t *bytes, size_t length,
                          picoquic_call_back_event_t fin_or_event,
                          void *callback_ctx, void *v_stream_ctx);

  // ALPN selection callback for picoquic
  static size_t alpnSelectCallback(picoquic_quic_t *quic,
                                   picoquic_iovec_t *list, size_t count);

  // Handle new connection event
  void onNewConnection(MoQPicoQuicServer *server, picoquic_cnx_t *cnx);

  // Server parameters
  std::string cert_;
  std::string key_;
  std::vector<std::string> supportedAlpns_;

  // Picoquic context
  picoquic_quic_t *quic_{nullptr};

  // Network thread context and parameters
  picoquic_packet_loop_param_t loopParam_{};
  picoquic_network_thread_ctx_t *networkThreadCtx_{nullptr};

  // Server state
  folly::SocketAddress serverAddr_;
  std::atomic<bool> running_{false};

  // Executor for all sessions
  std::shared_ptr<PicoQuicExecutor> executor_;
};

MoQPicoQuicServer::MoQPicoQuicServer(
    std::string cert,
    std::string key,
    std::string endpoint,
    std::string versions)
    : MoQServerBase(std::move(endpoint)), impl_(std::make_unique<Impl>()) {
  impl_->cert_ = std::move(cert);
  impl_->key_ = std::move(key);
  impl_->supportedAlpns_ = getMoqtProtocols(versions, true);
}

MoQPicoQuicServer::~MoQPicoQuicServer() { stop(); }

void MoQPicoQuicServer::start(const folly::SocketAddress &addr) {
  if (impl_->running_.exchange(true)) {
    XLOG(WARN) << "Server already running";
    return;
  }

  impl_->serverAddr_ = addr;

  // Create the executor that will be shared by all sessions
  impl_->executor_ = std::make_shared<PicoQuicExecutor>();

  // Create the QUIC context
  uint64_t current_time = picoquic_current_time();

  XLOG(INFO) << "Supported ALPNs: "
             << folly::join(", ", impl_->supportedAlpns_);

  // Pass NULL as default ALPN - we'll use the selection callback instead
  impl_->quic_ =
      picoquic_create(100, // max_connections
                      impl_->cert_.c_str(), impl_->key_.c_str(),
                      nullptr, // cert_store_filename
                      nullptr, // default_alpn (NULL to use selection callback)
                      Impl::picoCallback,
                      this,    // callback_ctx
                      nullptr, // cnx_id_callback
                      nullptr, // cnx_id_callback_ctx
                      nullptr, // reset_seed
                      current_time,
                      nullptr, // simulated_time
                      nullptr, // ticket_file_name
                      nullptr, // ticket_encryption_key
                      0);      // ticket_encryption_key_length

  if (impl_->quic_ == nullptr) {
    XLOG(ERR)
        << "Failed to create picoquic context (check cert/key paths: cert="
        << impl_->cert_ << ", key=" << impl_->key_ << ")";
    impl_->executor_.reset();
    impl_->running_ = false;
    return;
  }

  // Set ALPN selection callback to handle multiple ALPNs
  picoquic_set_alpn_select_fn_v2(impl_->quic_, Impl::alpnSelectCallback);

  // Configure picoquic settings
  picoquic_set_cookie_mode(impl_->quic_, 2);
  picoquic_set_default_congestion_algorithm(impl_->quic_,
                                            picoquic_bbr_algorithm);

  XLOG(INFO) << "Starting MoQPicoQuicServer on "
             << impl_->serverAddr_.describe() << " with "
             << impl_->supportedAlpns_.size() << " supported ALPNs";

  // Set up packet loop parameters
  impl_->loopParam_ = {};
  impl_->loopParam_.local_port =
      static_cast<uint16_t>(impl_->serverAddr_.getPort());

  // Start the network thread using picoquic's network thread API
  int ret = 0;
  impl_->networkThreadCtx_ = picoquic_start_network_thread(
      impl_->quic_, &impl_->loopParam_,
      reinterpret_cast<picoquic_packet_loop_cb_fn>(
          PicoQuicExecutor::getLoopCallback()),
      impl_->executor_.get(), &ret);

  if (impl_->networkThreadCtx_ == nullptr) {
    XLOG(ERR) << "Failed to start network thread, ret=" << ret;
    picoquic_free(impl_->quic_);
    impl_->quic_ = nullptr;
    impl_->running_ = false;
    return;
  }

  XLOG(INFO) << "MoQPicoQuicServer network thread started";
}

void MoQPicoQuicServer::stop() {
  if (!impl_->running_.exchange(false)) {
    return;
  }

  XLOG(INFO) << "Stopping MoQPicoQuicServer";

  // Stop the network thread cleanly
  if (impl_->networkThreadCtx_) {
    picoquic_delete_network_thread(impl_->networkThreadCtx_);
    impl_->networkThreadCtx_ = nullptr;
  }

  // Free the picoquic context
  // Note: picoquic_free will close all connections, which will trigger close
  // callbacks
  if (impl_->quic_) {
    picoquic_free(impl_->quic_);
    impl_->quic_ = nullptr;
  }

  XLOG(INFO) << "MoQPicoQuicServer stopped";
}

size_t MoQPicoQuicServer::Impl::alpnSelectCallback(picoquic_quic_t *quic,
                                                   picoquic_iovec_t *list,
                                                   size_t count) {
  auto *server = static_cast<MoQPicoQuicServer *>(
      picoquic_get_default_callback_context(quic));
  const auto &supportedAlpns = server->impl_->supportedAlpns_;

  // First pass: build list of client-proposed ALPNs
  std::vector<std::string> clientAlpns;
  for (size_t i = 0; i < count; i++) {
    if (list[i].base && list[i].len > 0) {
      clientAlpns.emplace_back(reinterpret_cast<const char *>(list[i].base),
                               list[i].len);
    }
  }
  XLOG(DBG4) << "Client proposed ALPNs: " << folly::join(", ", clientAlpns);

  // Find the first ALPN from our preference list that the client supports
  for (const auto &ourAlpn : supportedAlpns) {
    for (size_t i = 0; i < clientAlpns.size(); i++) {
      if (clientAlpns[i] == ourAlpn) {
        XLOG(DBG1) << "Selected ALPN: " << ourAlpn << " (index " << i << ")";
        return i;
      }
    }
  }

  XLOG(WARN) << "No common ALPN found between client and server";
  return count; // Return invalid index
}

int MoQPicoQuicServer::Impl::picoCallback(
    picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes, size_t length,
    picoquic_call_back_event_t fin_or_event, void *callback_ctx,
    void *v_stream_ctx) {

  // Log ALL callbacks to debug missing stream data
  XLOG(DBG6) << "MoQPicoQuicServer::picoCallback: event=" << fin_or_event
             << " stream_id=" << stream_id << " length=" << length
             << " callback_ctx=" << callback_ctx;

  // For the ready/almost_ready/ALPN events, callback_ctx is the server
  // For all other events, it's the ConnectionContext
  if (fin_or_event == picoquic_callback_ready ||
      fin_or_event == picoquic_callback_almost_ready ||
      fin_or_event == picoquic_callback_request_alpn_list ||
      fin_or_event == picoquic_callback_set_alpn) {
    auto *server = static_cast<MoQPicoQuicServer *>(callback_ctx);
    if (!server) {
      XLOG(ERR) << "picoCallback: server is null on event " << fin_or_event
                << "! This will return "
                << "PICOQUIC_ERROR_UNEXPECTED_ERROR (1051)";
      return PICOQUIC_ERROR_UNEXPECTED_ERROR;
    }

    // Only initialize on ready, not almost_ready or ALPN events
    if (fin_or_event == picoquic_callback_ready) {
      XLOG(DBG1) << "New connection ready";
      server->impl_->onNewConnection(server, cnx);
    } else {
      XLOG(DBG2) << "Connection event: " << fin_or_event;
    }
    return 0;
  }

  // For all other events, get the ConnectionContext
  auto *ctx = static_cast<ConnectionContext *>(callback_ctx);
  if (!ctx) {
    // Connection might be closing before we set context
    return 0;
  }

  // Verify this is actually a ConnectionContext, not the server pointer
  // (connection can close before we create the ConnectionContext)
  if (ctx->magic != ConnectionContext::kMagic) {
    XLOG(DBG1) << "Connection closing before ConnectionContext "
               << "created, event=" << fin_or_event;
    return 0;
  }

  switch (fin_or_event) {
  case picoquic_callback_close:
  case picoquic_callback_application_close:
  case picoquic_callback_stateless_reset: {
    // Connection closed - clean up context
    XLOG(DBG1) << "Connection closed, event=" << fin_or_event;
    if (ctx->moqSession) {
      ctx->moqSession->onSessionEnd(folly::none);
    }
    // Clear magic before deletion to catch use-after-free
    ctx->magic = 0xDEADBEEF;
    delete ctx; // Free the connection context
    return 0;
  }

  default: {
    // Forward all stream events to PicoQuicWebTransport callback
    XLOG(DBG6) << "Forwarding event " << fin_or_event
               << " to PicoQuicWebTransport"
               << " webTransport=" << (void *)ctx->webTransport.get();
    if (ctx->webTransport) {
      return ctx->webTransport->handlePicoEvent(
          cnx, stream_id, bytes, length,
          static_cast<int>(fin_or_event), v_stream_ctx);
    }
    XLOG(WARN) << "webTransport is null, cannot forward event " << fin_or_event;
    return 0;
  }
  }
}

void MoQPicoQuicServer::Impl::onNewConnection(MoQPicoQuicServer *server,
                                               picoquic_cnx_t *cnx) {
  // Get local and peer addresses
  struct sockaddr *local_addr_ptr = nullptr;
  struct sockaddr *peer_addr_ptr = nullptr;

  picoquic_get_peer_addr(cnx, &peer_addr_ptr);
  picoquic_get_local_addr(cnx, &local_addr_ptr);

  folly::SocketAddress localSockAddr;
  folly::SocketAddress peerSockAddr;

  if (local_addr_ptr) {
    localSockAddr.setFromSockaddr(local_addr_ptr);
  }
  if (peer_addr_ptr) {
    peerSockAddr.setFromSockaddr(peer_addr_ptr);
  }

  XLOG(DBG1) << "New connection from " << peerSockAddr.describe();

  // Create PicoQuicWebTransport
  auto webTransport =
      std::make_shared<PicoQuicWebTransport>(cnx, localSockAddr, peerSockAddr);

  // Create MoQSession using the shared executor
  // Note: createSession takes shared_ptr by value, so it copies
  auto moqSession = server->createSession(webTransport, executor_);

  // Set the WebTransport handler to the MoQSession
  webTransport->setHandler(moqSession.get());

  // Get the negotiated ALPN from picoquic
  const char *alpn = picoquic_tls_get_negotiated_alpn(cnx);
  if (alpn) {
    XLOG(DBG1) << "Setting MoQ version from negotiated ALPN: " << alpn;
    moqSession->validateAndSetVersionFromAlpn(alpn);
  } else {
    XLOG(WARN) << "No ALPN was negotiated for connection";
  }

  // Create connection context and set it as the callback context
  auto *ctx = new ConnectionContext{
      .webTransport = webTransport, .moqSession = moqSession, .server = server};

  // Update the connection's callback context to our ConnectionContext
  // IMPORTANT: This must happen before any other picoquic events can fire
  XLOG(DBG4) << "Setting connection callback context from server "
             << "to ConnectionContext";
  picoquic_set_callback(cnx, picoCallback, ctx);
  XLOG(DBG4) << "Connection callback context updated successfully";

  // Start handling the session in a coroutine scheduled on the executor
  folly::coro::co_withExecutor(executor_.get(),
                               server->handleClientSession(moqSession))
      .start();
}

} // namespace moxygen
