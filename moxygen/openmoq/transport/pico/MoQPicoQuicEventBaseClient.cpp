/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/MoQPicoQuicEventBaseClient.h"
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/MoQSession.h>
#include <moxygen/MoQVersions.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/openmoq/transport/pico/PicoConnectionContext.h>
#include <moxygen/openmoq/transport/pico/PicoQuicSocketHandler.h>
#include <picoquic.h>
#include <picoquic_bbr.h>

namespace moxygen {

// ---------------------------------------------------------------------------
// Friend bridge: lets the anonymous-namespace picoCallback call private
// methods on MoQPicoQuicEventBaseClient.
// ---------------------------------------------------------------------------
struct MoQPicoQuicEventBaseClientCallbacks {
  static void onConnectionReady(MoQPicoQuicEventBaseClient* client, void* cnx) {
    client->onConnectionReady(cnx);
  }
  static void onConnectionClosed(MoQPicoQuicEventBaseClient* client) {
    client->onConnectionClosed();
  }
};

namespace {

static int clientPicoCallback(picoquic_cnx_t* cnx,
                               uint64_t stream_id,
                               uint8_t* bytes,
                               size_t length,
                               picoquic_call_back_event_t fin_or_event,
                               void* callback_ctx,
                               void* v_stream_ctx) {
  XLOG(DBG6) << "clientPicoCallback: event=" << fin_or_event
             << " stream_id=" << stream_id << " length=" << length;

  // callback_ctx is MoQPicoQuicEventBaseClient* until connection is ready,
  // then switches to PicoConnectionContext*. Distinguish them by the magic
  // field: a valid PicoConnectionContext has kMagic as its first word.
  auto* tagged = static_cast<PicoConnectionContext*>(callback_ctx);
  bool isState = callback_ctx && tagged->magic == PicoConnectionContext::kMagic;

  if (!isState) {
    // Still in setup phase — callback_ctx is the client pointer.
    auto* client = static_cast<MoQPicoQuicEventBaseClient*>(callback_ctx);
    if (!client) {
      return PICOQUIC_ERROR_UNEXPECTED_ERROR;
    }

    switch (fin_or_event) {
      case picoquic_callback_ready:
        XLOG(DBG1) << "Client connection ready";
        MoQPicoQuicEventBaseClientCallbacks::onConnectionReady(client, cnx);
        break;
      case picoquic_callback_close:
      case picoquic_callback_application_close:
      case picoquic_callback_stateless_reset:
        XLOG(DBG1) << "Client connection closed before ready, event="
                   << fin_or_event;
        MoQPicoQuicEventBaseClientCallbacks::onConnectionClosed(client);
        break;
      default:
        break;
    }
    return 0;
  }

  // Connection is established — delegate to shared dispatch.
  XLOG(DBG6) << "Forwarding event " << fin_or_event
             << " to PicoQuicWebTransport";
  return dispatchConnectionEvent(
      tagged, cnx, stream_id, bytes, length, fin_or_event, v_stream_ctx);
}

} // namespace

// ---------------------------------------------------------------------------

struct MoQPicoQuicEventBaseClient::Impl {
  picoquic_quic_t* quic{nullptr};
  picoquic_cnx_t* cnx{nullptr};
  std::unique_ptr<PicoQuicSocketHandler> handler;
  std::atomic<bool> running{false};
};

MoQPicoQuicEventBaseClient::MoQPicoQuicEventBaseClient(
    std::string endpoint,
    folly::Executor::KeepAlive<folly::EventBase> evb,
    std::string certRootFile)
    : endpoint_(std::move(endpoint)),
      impl_(std::make_unique<Impl>()),
      evb_(std::move(evb)),
      certRootFile_(std::move(certRootFile)) {}

MoQPicoQuicEventBaseClient::~MoQPicoQuicEventBaseClient() {
  doCleanup();
}

void MoQPicoQuicEventBaseClient::connect(const folly::SocketAddress& addr,
                                          std::string sni,
                                          std::string alpn) {
  if (impl_->running.exchange(true)) {
    XLOG(WARN) << "Client already connecting";
    return;
  }

  sni_ = std::move(sni);
  alpn_ = std::move(alpn);

  uint64_t currentTime = picoquic_current_time();

  // Clients pass NULL for cert/key (no client certificate).
  // cert_root_file is optional — pass NULL to skip server cert verification.
  const char* certRoot =
      certRootFile_.empty() ? nullptr : certRootFile_.c_str();

  impl_->quic = picoquic_create(
      1,        // max_nb_connections (client typically has one)
      nullptr,  // cert_file_name — not needed for client
      nullptr,  // key_file_name  — not needed for client
      certRoot, // cert_root_file — for server certificate verification
      alpn_.c_str(),
      clientPicoCallback,
      this, // callback_ctx (this until connection is ready)
      nullptr,
      nullptr,
      nullptr,
      currentTime,
      nullptr,
      nullptr,
      nullptr,
      0);

  if (!impl_->quic) {
    XLOG(ERR) << "Failed to create picoquic client context";
    impl_->running = false;
    return;
  }

  picoquic_set_default_congestion_algorithm(impl_->quic,
                                            picoquic_bbr_algorithm);

  // Initiate the outgoing connection before starting the socket handler so
  // that the initial rescheduleTimer() call in start() sees the connection
  // with next_wake_time=now and immediately schedules a drain.
  struct sockaddr_storage ss;
  addr.getAddress(&ss);

  impl_->cnx = picoquic_create_client_cnx(
      impl_->quic,
      reinterpret_cast<struct sockaddr*>(&ss),
      currentTime,
      0,              // preferred_version (0 = default)
      sni_.c_str(),
      alpn_.c_str(),
      clientPicoCallback,
      this);

  if (!impl_->cnx) {
    XLOG(ERR) << "Failed to create client connection to " << addr.describe();
    picoquic_free(impl_->quic);
    impl_->quic = nullptr;
    impl_->running = false;
    return;
  }

  // Bind to an ephemeral local port and start the socket handler after creating
  // the cnx so that rescheduleTimer() in start() sees next_wake_time=now and
  // immediately drains the Initial packet.
  folly::SocketAddress localAddr(
      addr.getFamily() == AF_INET6 ? "::" : "0.0.0.0", 0);
  if (!executor_) {
    executor_ = std::make_shared<MoQFollyExecutorImpl>(evb_.get());
  }
  impl_->handler = std::make_unique<PicoQuicSocketHandler>(
      evb_.get(), impl_->quic);
  impl_->handler->start(localAddr);

  // Tell picoquic our actual bound address so it populates addrFrom correctly
  // in picoquic_prepare_next_packet_ex.
  folly::SocketAddress boundAddr = impl_->handler->boundAddress();
  struct sockaddr_storage boundSS;
  boundAddr.getAddress(&boundSS);
  picoquic_set_local_addr(
      impl_->cnx, reinterpret_cast<struct sockaddr*>(&boundSS));

  XLOG(INFO) << "MoQPicoQuicEventBaseClient connecting to "
             << addr.describe() << " alpn=" << alpn_;
}

void MoQPicoQuicEventBaseClient::close() {
  if (!impl_->running.exchange(false)) {
    return;
  }

  XLOG(INFO) << "Closing MoQPicoQuicEventBaseClient";

  if (!impl_->cnx || !impl_->handler) {
    // Connection never established or already torn down; clean up immediately.
    doCleanup();
    return;
  }

  // Start CONN_CLOSE drain; safe to call if closeSession() already did this.
  picoquic_close(impl_->cnx, 0);

  // Leave the handler alive for 3xPTO: picoquic retransmits CONN_CLOSE on
  // the timer and responds to peer packets. Destructor calls doCleanup().
}

void MoQPicoQuicEventBaseClient::doCleanup() {
  if (impl_->handler) {
    // Stop I/O before freeing quic. stopped_ flag makes this idempotent so
    // ~PicoQuicSocketHandler won't re-enter drainOutgoing() on freed memory.
    impl_->handler->stop();
  }
  if (impl_->quic) {
    // Free quic before resetting the handler so that any close callbacks fired
    // synchronously by picoquic_free() still have a valid handler raw pointer
    // (captured in the onConnectionClosedCallback_ lambda).
    picoquic_free(impl_->quic);
    impl_->quic = nullptr;
    impl_->cnx = nullptr;
  }
  impl_->handler.reset();
  executor_.reset();
  evb_ = {}; // release KeepAlive so EVB destructor doesn't spin
}

folly::EventBase* MoQPicoQuicEventBaseClient::getEventBase() const {
  return evb_.get();
}

std::shared_ptr<MoQSession> MoQPicoQuicEventBaseClient::createSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    std::shared_ptr<MoQExecutor> executor) {
  return std::make_shared<MoQSession>(wt, executor);
}

void MoQPicoQuicEventBaseClient::onConnectionReady(void* vcnx) {
  auto* cnx = static_cast<picoquic_cnx_t*>(vcnx);

  struct sockaddr* local_addr_ptr = nullptr;
  struct sockaddr* peer_addr_ptr = nullptr;
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

  XLOG(DBG1) << "Client connection ready, peer=" << peerSockAddr.describe();

  auto webTransport = std::make_shared<PicoQuicWebTransport>(
      cnx, localSockAddr, peerSockAddr);
  webTransport->setUpdateWakeTimeoutCallback(
      [handler = impl_->handler.get()] { handler->updateWakeTimeout(); });
  webTransport->setOnConnectionClosedCallback(
      [handler = impl_->handler.get()] { handler->closeMaybeDeferred(); });

  auto moqSession = createSession(webTransport, executor_);
  webTransport->setHandler(moqSession.get());

  const char* alpn = picoquic_tls_get_negotiated_alpn(cnx);
  if (alpn) {
    XLOG(DBG1) << "Negotiated ALPN: " << alpn;
    moqSession->validateAndSetVersionFromAlpn(alpn);
  }

  // Switch callback_ctx from `this` to a PicoConnectionContext so subsequent
  // events route through the shared dispatch.
  auto* ctx = new PicoConnectionContext{
      .webTransport = webTransport, .moqSession = moqSession};
  picoquic_set_callback(cnx, clientPicoCallback, ctx);

  // Start the session, complete the MoQ SETUP exchange, then notify the
  // subclass. Doing this in a coroutine ensures onSession() is not called
  // until SERVER_SETUP has been received and peerMaxRequestID is set.
  moqSession->start();
  folly::coro::co_withExecutor(
      executor_.get(),
      [](MoQPicoQuicEventBaseClient* self,
         std::shared_ptr<MoQSession> moqSession)
      -> folly::coro::Task<void> {
        const uint32_t kDefaultMaxRequestID = 100;
        ClientSetup clientSetup{
            .supportedVersions = getSupportedLegacyVersions()};
        clientSetup.params.insertParam(Parameter(
            folly::to_underlying(SetupKey::MAX_REQUEST_ID),
            kDefaultMaxRequestID));
        try {
          auto serverSetup = co_await moqSession->setup(std::move(clientSetup));
          XLOG(DBG1) << "MoQ SETUP complete, selected version="
                     << serverSetup.selectedVersion;
        } catch (const std::exception& ex) {
          XLOG(ERR) << "MoQ SETUP failed: " << ex.what();
          co_return;
        }
        self->onSession(std::move(moqSession));
      }(this, std::move(moqSession)))
      .start();
}

void MoQPicoQuicEventBaseClient::onConnectionClosed() {
  XLOG(DBG1) << "Client connection closed before session was established";
  impl_->cnx = nullptr;
}

} // namespace moxygen
