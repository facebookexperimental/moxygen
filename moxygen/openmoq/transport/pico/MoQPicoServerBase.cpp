/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/MoQPicoServerBase.h"
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/MoQSession.h>
#include <moxygen/MoQVersions.h>
#include <moxygen/openmoq/transport/pico/PicoConnectionContext.h>
#include <picoquic.h>
#include <picoquic_bbr.h>

namespace moxygen {

// Bridge struct declared as friend in MoQPicoServerBase. Allows the
// anonymous-namespace callbacks to call private methods.
struct MoQPicoServerBaseCallbacks {
  static void onNewConnection(MoQPicoServerBase* server, void* cnx) {
    server->onNewConnectionImpl(cnx);
  }
  static const std::string& getVersions(MoQPicoServerBase* server) {
    return server->versions_;
  }
};

namespace {

static size_t alpnSelectCallback(picoquic_quic_t* quic,
                                 picoquic_iovec_t* list,
                                 size_t count) {
  auto* server = static_cast<MoQPicoServerBase*>(
      picoquic_get_default_callback_context(quic));
  auto supportedAlpns =
      getMoqtProtocols(MoQPicoServerBaseCallbacks::getVersions(server), true);

  std::vector<std::string> clientAlpns;
  for (size_t i = 0; i < count; i++) {
    if (list[i].base && list[i].len > 0) {
      clientAlpns.emplace_back(reinterpret_cast<const char*>(list[i].base),
                               list[i].len);
    }
  }
  XLOG(DBG4) << "Client proposed ALPNs: " << folly::join(", ", clientAlpns);

  for (const auto& ourAlpn : supportedAlpns) {
    for (size_t i = 0; i < clientAlpns.size(); i++) {
      if (clientAlpns[i] == ourAlpn) {
        XLOG(DBG1) << "Selected ALPN: " << ourAlpn << " (index " << i << ")";
        return i;
      }
    }
  }

  XLOG(WARN) << "No common ALPN found between client and server";
  return count;
}

static int picoCallback(picoquic_cnx_t* cnx,
                        uint64_t stream_id,
                        uint8_t* bytes,
                        size_t length,
                        picoquic_call_back_event_t fin_or_event,
                        void* callback_ctx,
                        void* v_stream_ctx) {
  XLOG(DBG6) << "picoCallback: event=" << fin_or_event
             << " stream_id=" << stream_id << " length=" << length;

  if (fin_or_event == picoquic_callback_ready ||
      fin_or_event == picoquic_callback_almost_ready ||
      fin_or_event == picoquic_callback_request_alpn_list ||
      fin_or_event == picoquic_callback_set_alpn) {
    auto* server = static_cast<MoQPicoServerBase*>(callback_ctx);
    if (!server) {
      XLOG(ERR) << "picoCallback: server is null on event " << fin_or_event;
      return PICOQUIC_ERROR_UNEXPECTED_ERROR;
    }
    if (fin_or_event == picoquic_callback_ready) {
      XLOG(DBG1) << "New connection ready";
      MoQPicoServerBaseCallbacks::onNewConnection(server, cnx);
    } else {
      XLOG(DBG2) << "Connection event: " << fin_or_event;
    }
    return 0;
  }

  auto* ctx = static_cast<PicoConnectionContext*>(callback_ctx);
  if (!ctx) {
    return 0;
  }
  if (ctx->magic != PicoConnectionContext::kMagic) {
    XLOG(DBG1) << "Connection closing before context created, event="
               << fin_or_event;
    return 0;
  }

  XLOG(DBG6) << "Forwarding event " << fin_or_event << " to PicoQuicWebTransport";
  return dispatchConnectionEvent(
      ctx, cnx, stream_id, bytes, length, fin_or_event, v_stream_ctx);
}

} // namespace

// ---------------------------------------------------------------------------

MoQPicoServerBase::MoQPicoServerBase(std::string cert,
                                     std::string key,
                                     std::string endpoint,
                                     std::string versions)
    : MoQServerBase(std::move(endpoint)),
      cert_(std::move(cert)),
      key_(std::move(key)),
      versions_(std::move(versions)) {}

MoQPicoServerBase::~MoQPicoServerBase() {
  destroyQuicContext();
}

bool MoQPicoServerBase::createQuicContext() {
  auto supportedAlpns = getMoqtProtocols(versions_, true);
  XLOG(INFO) << "Supported ALPNs: " << folly::join(", ", supportedAlpns);

  uint64_t current_time = picoquic_current_time();
  quic_ = picoquic_create(
      100,
      cert_.c_str(),
      key_.c_str(),
      nullptr, // cert_store_filename
      nullptr, // default_alpn (NULL — use ALPN selection callback)
      picoCallback,
      this,    // callback_ctx
      nullptr, // cnx_id_callback
      nullptr, // cnx_id_callback_ctx
      nullptr, // reset_seed
      current_time,
      nullptr, // simulated_time
      nullptr, // ticket_file_name
      nullptr, // ticket_encryption_key
      0);      // ticket_encryption_key_length

  if (quic_ == nullptr) {
    XLOG(ERR) << "Failed to create picoquic context (cert=" << cert_
              << ", key=" << key_ << ")";
    return false;
  }

  picoquic_set_alpn_select_fn_v2(quic_, alpnSelectCallback);
  picoquic_set_cookie_mode(quic_, 2);
  picoquic_set_default_congestion_algorithm(quic_, picoquic_bbr_algorithm);
  return true;
}

void MoQPicoServerBase::destroyQuicContext() {
  if (quic_) {
    // picoquic_free will close all connections, which will trigger close
    // callbacks
    picoquic_free(quic_);
    quic_ = nullptr;
  }
}

void MoQPicoServerBase::onNewConnectionImpl(void* vcnx) {
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

  XLOG(DBG1) << "New connection from " << peerSockAddr.describe();

  auto webTransport =
      std::make_shared<PicoQuicWebTransport>(cnx, localSockAddr, peerSockAddr);
  onWebTransportCreated(*webTransport);

  auto moqSession = createSession(webTransport, executor_);
  webTransport->setHandler(moqSession.get());

  const char* alpn = picoquic_tls_get_negotiated_alpn(cnx);
  if (alpn) {
    XLOG(DBG1) << "Setting MoQ version from negotiated ALPN: " << alpn;
    moqSession->validateAndSetVersionFromAlpn(alpn);
  } else {
    XLOG(WARN) << "No ALPN was negotiated for connection";
  }

  auto* ctx = new PicoConnectionContext{
      .webTransport = webTransport, .moqSession = moqSession};

  XLOG(DBG4) << "Setting connection callback context";
  picoquic_set_callback(cnx, picoCallback, ctx);

  folly::coro::co_withExecutor(executor_.get(), handleClientSession(moqSession))
      .start();
}

} // namespace moxygen
