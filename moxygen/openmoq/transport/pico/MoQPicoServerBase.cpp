/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/MoQPicoServerBase.h"
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <h3zero_common.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/MoQSession.h>
#include <moxygen/MoQVersions.h>
#include <moxygen/openmoq/transport/pico/PicoConnectionContext.h>
#include <moxygen/openmoq/transport/pico/PicoH3WebTransport.h>
#include <moxygen/openmoq/transport/pico/PicoProtocolDispatcher.h>
#include <pico_webtransport.h>
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
  static const PicoWebTransportConfig& getWTConfig(MoQPicoServerBase* server) {
    return server->wtConfig_;
  }
  static h3zero_callback_ctx_t* getOrCreateH3Ctx(
      MoQPicoServerBase* server,
      picoquic_cnx_t* cnx) {
    auto it = server->h3Contexts_.find(cnx);
    if (it != server->h3Contexts_.end()) {
      return it->second;
    }
    // Create per-connection h3zero context using same params as template
    picohttp_server_parameters_t serverParams = {};
    serverParams.web_folder = nullptr;
    serverParams.path_table = server->wtPathTable_.get();
    serverParams.path_table_nb = 1;

    auto* h3Ctx = h3zero_callback_create_context(&serverParams);
    if (h3Ctx) {
      h3Ctx->settings.h3_datagram = 1;
      // wtMaxSessions limits concurrent WT sessions per QUIC connection.
      // h3zero enforces this in the WT layer; we use it for flow control.
      h3Ctx->settings.webtransport_max_sessions =
          server->wtConfig_.wtMaxSessions;
      server->h3Contexts_[cnx] = h3Ctx;
      XLOG(DBG1) << "Created per-connection h3Ctx for cnx=" << (void*)cnx;
    }
    return h3Ctx;
  }
  static void removeH3Ctx(MoQPicoServerBase* server, picoquic_cnx_t* cnx) {
    auto it = server->h3Contexts_.find(cnx);
    if (it != server->h3Contexts_.end()) {
      XLOG(DBG1) << "Removing per-connection h3Ctx for cnx=" << (void*)cnx;
      h3zero_callback_delete_context(cnx, it->second);
      server->h3Contexts_.erase(it);
    }
  }
  static int onWebTransportConnect(
      MoQPicoServerBase* server,
      picoquic_cnx_t* cnx,
      h3zero_stream_ctx_t* streamCtx) {
    return server->onWebTransportConnectImpl(cnx, streamCtx);
  }
  static std::shared_ptr<MoQExecutor> getExecutor(MoQPicoServerBase* server) {
    return server->executor_;
  }
};

namespace {

// Build list of supported ALPNs based on configuration.
// Order matters: server prefers earlier entries. Put MOQT ALPNs first so
// non-browser clients get moqt-* preference; h3 last for browser fallback.
static std::vector<std::string> buildSupportedAlpns(
    const std::string& versions,
    const PicoWebTransportConfig& wtConfig) {
  std::vector<std::string> alpns;
  if (wtConfig.enableQuicTransport) {
    auto moqtAlpns = getMoqtProtocols(versions, true);
    alpns.insert(alpns.end(), moqtAlpns.begin(), moqtAlpns.end());
  }
  if (wtConfig.enableWebTransport) {
    alpns.push_back("h3");
  }
  return alpns;
}

static size_t alpnSelectCallback(
    picoquic_quic_t* quic,
    picoquic_iovec_t* list,
    size_t count) {
  auto* server = static_cast<MoQPicoServerBase*>(
      picoquic_get_default_callback_context(quic));
  const auto& supportedAlpns = buildSupportedAlpns(
      MoQPicoServerBaseCallbacks::getVersions(server),
      MoQPicoServerBaseCallbacks::getWTConfig(server));

  std::vector<std::string> clientAlpns;
  for (size_t i = 0; i < count; i++) {
    if (list[i].base && list[i].len > 0) {
      clientAlpns.emplace_back(
          reinterpret_cast<const char*>(list[i].base), list[i].len);
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

static int picoCallback(
    picoquic_cnx_t* cnx,
    uint64_t stream_id,
    uint8_t* bytes,
    size_t length,
    picoquic_call_back_event_t fin_or_event,
    void* callback_ctx,
    void* v_stream_ctx) {
  XLOG(DBG6) << "picoCallback: event=" << fin_or_event
             << " stream_id=" << stream_id << " length=" << length;

  if (fin_or_event == picoquic_callback_request_alpn_list ||
      fin_or_event == picoquic_callback_set_alpn) {
    // request_alpn_list is not fired when alpn_select_fn_v2 is registered —
    // alpnSelectCallback handles selection directly. set_alpn is informational;
    // we read the negotiated ALPN via picoquic_tls_get_negotiated_alpn in
    // almost_ready instead. Both are no-ops here.
    XLOG(DBG2) << "Connection event: " << fin_or_event;
    return 0;
  }

  if (fin_or_event == picoquic_callback_almost_ready) {
    // Switch to h3zero_callback EARLY for WebTransport connections
    // so h3zero receives almost_ready and ready events properly
    auto* server = static_cast<MoQPicoServerBase*>(callback_ctx);
    if (!server) {
      XLOG(ERR) << "picoCallback: server is null on almost_ready";
      return PICOQUIC_ERROR_UNEXPECTED_ERROR;
    }

    const char* alpn = picoquic_tls_get_negotiated_alpn(cnx);
    auto protocol = PicoProtocolDispatcher::getProtocol(alpn);

    XLOG(DBG1) << "Connection almost_ready, ALPN: " << (alpn ? alpn : "(null)")
               << " -> Protocol: "
               << (protocol ? PicoProtocolDispatcher::protocolName(*protocol)
                            : "none");

    if (!protocol) {
      XLOG(ERR) << "No recognized ALPN on almost_ready, closing connection";
      return PICOQUIC_ERROR_UNEXPECTED_ERROR;
    }

    if (*protocol == PicoProtocolType::WebTransportH3) {
      // Create per-connection h3zero context and switch to h3zero_callback
      auto* h3Ctx = MoQPicoServerBaseCallbacks::getOrCreateH3Ctx(server, cnx);
      if (!h3Ctx) {
        XLOG(ERR) << "Failed to create h3Ctx for WebTransport connection";
        return PICOQUIC_ERROR_UNEXPECTED_ERROR;
      }
      XLOG(DBG1) << "Switching to h3zero_callback for WebTransport, cnx="
                 << (void*)cnx << " h3Ctx=" << (void*)h3Ctx;
      picoquic_set_callback(cnx, h3zero_callback, h3Ctx);
      // Forward almost_ready event to h3zero
      return h3zero_callback(
          cnx, stream_id, bytes, length, fin_or_event, h3Ctx, v_stream_ctx);
    }
    return 0;
  }

  if (fin_or_event == picoquic_callback_ready) {
    auto* server = static_cast<MoQPicoServerBase*>(callback_ctx);
    if (!server) {
      XLOG(ERR) << "picoCallback: server is null on ready event";
      return PICOQUIC_ERROR_UNEXPECTED_ERROR;
    }

    // Determine protocol based on negotiated ALPN
    const char* alpn = picoquic_tls_get_negotiated_alpn(cnx);
    auto protocol = PicoProtocolDispatcher::getProtocol(alpn);

    XLOG(DBG1) << "Negotiated ALPN: " << (alpn ? alpn : "(null)")
               << " -> Protocol: "
               << (protocol ? PicoProtocolDispatcher::protocolName(*protocol)
                            : "none");

    if (!protocol) {
      XLOG(ERR) << "No recognized ALPN on ready, closing connection";
      return PICOQUIC_ERROR_UNEXPECTED_ERROR;
    }

    // WebTransport connections should have been switched to h3zero_callback
    // in almost_ready. If we get here with h3 ALPN, something is wrong.
    XCHECK(*protocol != PicoProtocolType::WebTransportH3)
        << "WebTransport connection reached picoCallback on ready event";

    if (*protocol == PicoProtocolType::Quic) {
      // Route to QUIC transport handler (existing path)
      MoQPicoServerBaseCallbacks::onNewConnection(server, cnx);
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

  XLOG(DBG6) << "Forwarding event " << fin_or_event << " to Pico*WebTransport";
  return dispatchConnectionEvent(
      ctx, cnx, stream_id, bytes, length, fin_or_event, v_stream_ctx);
}

// WebTransport path callback - invoked by h3zero when a CONNECT request
// is received on the configured endpoint (e.g., /moq)
static int wtPathCallback(
    picoquic_cnx_t* cnx,
    uint8_t* bytes,
    size_t length,
    picohttp_call_back_event_t event,
    h3zero_stream_ctx_t* streamCtx,
    void* pathAppCtx) {
  auto* server = static_cast<MoQPicoServerBase*>(pathAppCtx);
  if (!server) {
    XLOG(ERR) << "wtPathCallback: server is null";
    return -1;
  }

  XLOG(DBG3) << "wtPathCallback: event=" << static_cast<int>(event)
             << " stream=" << (streamCtx ? streamCtx->stream_id : 0)
             << " length=" << length;

  switch (event) {
    case picohttp_callback_connect:
      // Browser sent CONNECT request - accept WebTransport session
      return MoQPicoServerBaseCallbacks::onWebTransportConnect(
          server, cnx, streamCtx);

    case picohttp_callback_connect_refused:
      // Client-side callback - shouldn't happen on server
      XLOG(WARN) << "Unexpected picohttp_callback_connect_refused on server";
      break;

    case picohttp_callback_connect_accepted:
      // Client-side callback - shouldn't happen on server
      XLOG(WARN) << "Unexpected picohttp_callback_connect_accepted on server";
      break;

    default:
      return dispatchH3Event(
          cnx, bytes, length, static_cast<int>(event), streamCtx);
  }

  return 0;
}

} // namespace

// ---------------------------------------------------------------------------

MoQPicoServerBase::MoQPicoServerBase(
    std::string cert,
    std::string key,
    std::string endpoint,
    std::string versions,
    PicoWebTransportConfig wtConfig)
    : MoQServerBase(std::move(endpoint)),
      cert_(std::move(cert)),
      key_(std::move(key)),
      versions_(std::move(versions)),
      wtConfig_(std::move(wtConfig)) {}

MoQPicoServerBase::~MoQPicoServerBase() {
  destroyH3Zero();
  destroyQuicContext();
}

bool MoQPicoServerBase::createQuicContext() {
  XLOG(DBG1) << "Supported ALPNs: "
             << folly::join(", ", buildSupportedAlpns(versions_, wtConfig_));

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

  // Initialize h3zero for WebTransport if enabled
  if (wtConfig_.enableWebTransport) {
    if (!initH3Zero()) {
      XLOG(ERR) << "Failed to initialize h3zero for WebTransport";
      destroyQuicContext();
      return false;
    }
    // Enable WebTransport transport parameters
    picowt_set_default_transport_parameters(quic_);
    XLOG(DBG1) << "WebTransport enabled on endpoint: " << wtConfig_.wtEndpoint;
  }

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
    XLOG(DBG1) << "Setting MOQT version from negotiated ALPN: " << alpn;
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

bool MoQPicoServerBase::initH3Zero() {
  // Create path table for WebTransport endpoint
  // We need to keep this alive for the lifetime of the server
  // TODO: Support multiple endpoints by changing wtEndpoint to vector and
  // sizing this array accordingly. For now, single endpoint suffices.
  wtPathTable_ = std::make_unique<picohttp_server_path_item_t[]>(2);

  // Configure the WebTransport endpoint path
  // The callback will be invoked when a CONNECT request is received
  // Note: This does exact path matching; consider wildcard support for
  // path-based routing (e.g., /moq/relay-name).
  wtPathTable_[0].path = wtConfig_.wtEndpoint.c_str();
  wtPathTable_[0].path_length = wtConfig_.wtEndpoint.size();
  wtPathTable_[0].path_callback = wtPathCallback;
  wtPathTable_[0].path_app_ctx = this;

  // Null terminator for path table
  wtPathTable_[1].path = nullptr;
  wtPathTable_[1].path_length = 0;
  wtPathTable_[1].path_callback = nullptr;
  wtPathTable_[1].path_app_ctx = nullptr;

  // Note: We create per-connection h3zero contexts in getOrCreateH3Ctx()
  // using these same path table settings. The wtPathTable_ is shared
  // read-only across all connections.

  XLOG(DBG1) << "h3zero initialized with WebTransport endpoint: "
             << wtConfig_.wtEndpoint;

  return true;
}

void MoQPicoServerBase::destroyH3Zero() {
  // h3Contexts_ entries have been handed to h3zero via picoquic_set_callback.
  // h3zero owns each context and frees it when the connection closes
  // (picoquic_callback_close → h3zero_callback_delete_context). Calling
  // h3zero_callback_delete_context here would double-free already-closed
  // connections. Just clear the map and let destroyQuicContext/picoquic_free
  // drive the remaining close callbacks.
  h3Contexts_.clear();
  wtPathTable_.reset();
}

int MoQPicoServerBase::onWebTransportConnectImpl(
    picoquic_cnx_t* cnx,
    h3zero_stream_ctx_t* streamCtx) {
  // TODO: Add dynamic authority/path matching for relay deployments where
  // multiple services share a single server (e.g., by authority or path
  // prefix).
  XLOG(DBG1) << "WebTransport CONNECT received on stream "
             << streamCtx->stream_id << " cnx=" << (void*)cnx;

  // Get addresses
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

  XLOG(DBG1) << "Accepting WebTransport session from "
             << peerSockAddr.describe();

  // Get per-connection h3zero context
  auto h3CtxIt = h3Contexts_.find(cnx);
  if (h3CtxIt == h3Contexts_.end()) {
    XLOG(ERR) << "No h3Ctx found for cnx=" << (void*)cnx;
    return -1;
  }
  auto* h3Ctx = h3CtxIt->second;

  // Create PicoH3WebTransport adapter
  auto webTransport = std::make_shared<PicoH3WebTransport>(
      cnx, h3Ctx, streamCtx, localSockAddr, peerSockAddr);
  onWebTransportCreated(*webTransport);

  // Create MoQSession
  auto moqSession = createSession(webTransport, executor_);
  webTransport->setHandler(moqSession.get());

  // Negotiate MOQT version via WebTransport protocol negotiation.
  // The client sends wt-available-protocols, we select from our supported
  // versions. Note: picowt_select_wt_protocol expects ALPN format (e.g.
  // "moqt-16, moqt-15") not draft numbers, so we convert versions_ first.
  auto alpnProtocols = getMoqtProtocols(versions_, /*useStandard=*/true);
  std::string alpnList = folly::join(", ", alpnProtocols);

  // Debug: log what the client offered
  const char* clientProtos = reinterpret_cast<const char*>(
      streamCtx->ps.stream_state.header.wt_available_protocols);
  XLOG(DBG1) << "WT protocol negotiation: client offers ["
             << (clientProtos ? clientProtos : "NULL") << "], server offers ["
             << alpnList << "]";

  int wtProtoRet = picowt_select_wt_protocol(streamCtx, alpnList.c_str());
  if (wtProtoRet == 0 && streamCtx->ps.stream_state.wt_protocol) {
    const char* selectedProto = streamCtx->ps.stream_state.wt_protocol;
    XLOG(DBG1) << "WebTransport selected protocol: " << selectedProto;
    moqSession->validateAndSetVersionFromAlpn(selectedProto);
  } else {
    XLOG(ERR)
        << "No compatible WT protocol - client didn't offer matching version";
    return -1;
  }

  // Store session context on the control stream. Data streams inherit it via
  // path_callback in PicoH3WebTransport::markStreamActiveImpl, so all stream
  // events dispatch directly without a server-level map lookup.
  auto* sessionCtx = new PicoH3SessionContext{
      .webTransport = webTransport, .moqSession = moqSession};
  streamCtx->path_callback_ctx = sessionCtx;

  // NOTE: h3zero automatically sends 200 response and sets is_upgraded=1
  // when we return 0 from this callback (see h3zero_common.c:1138).
  // Do NOT send duplicate response here!

  // Register stream prefix so new WT streams are routed to wtPathCallback.
  // Pass sessionCtx so data streams receive it as pathAppCtx, distinguishing
  // them from the control stream (which uses pathAppCtx == server).
  int ret = h3zero_declare_stream_prefix(
      h3Ctx, streamCtx->stream_id, wtPathCallback, sessionCtx);
  if (ret != 0) {
    XLOG(ERR) << "Failed to declare stream prefix: " << ret;
    streamCtx->path_callback_ctx = nullptr;
    delete sessionCtx;
    return ret;
  }
  XLOG(DBG2) << "Registered stream prefix for control stream "
             << streamCtx->stream_id;

  // Start handling the MOQT session
  folly::coro::co_withExecutor(executor_.get(), handleClientSession(moqSession))
      .start();

  XLOG(DBG1) << "WebTransport session accepted on stream "
             << streamCtx->stream_id << " cnx=" << (void*)cnx;
  return 0;
}

} // namespace moxygen
