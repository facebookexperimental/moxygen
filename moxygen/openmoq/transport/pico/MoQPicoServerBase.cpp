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
#include <moxygen/openmoq/transport/pico/PicoH3WebTransport.h>
#include <moxygen/openmoq/transport/pico/PicoProtocolDispatcher.h>
#include <picoquic.h>
#include <picoquic_bbr.h>
#include <h3zero_common.h>
#include <pico_webtransport.h>

namespace moxygen {

// Bridge struct declared as friend in MoQPicoServerBase. Allows the
// anonymous-namespace callbacks to call private methods.
struct MoQPicoServerBaseCallbacks {
  static void onNewConnection(MoQPicoServerBase* server, void* cnx) {
    server->onNewConnectionImpl(cnx);
  }
  static void onNewWebTransportConnection(MoQPicoServerBase* server, void* cnx) {
    server->onNewWebTransportConnectionImpl(cnx);
  }
  static const std::string& getVersions(MoQPicoServerBase* server) {
    return server->versions_;
  }
  static const PicoWebTransportConfig& getWTConfig(MoQPicoServerBase* server) {
    return server->wtConfig_;
  }
  static h3zero_callback_ctx_t* getH3CtxTemplate(MoQPicoServerBase* server) {
    return server->h3CtxTemplate_;
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
      h3Ctx->settings.webtransport_max_sessions = server->wtConfig_.wtMaxSessions;
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
  static int onWebTransportEvent(
      MoQPicoServerBase* server,
      picoquic_cnx_t* cnx,
      uint8_t* bytes,
      size_t length,
      int event,
      h3zero_stream_ctx_t* streamCtx) {
    return server->onWebTransportEventImpl(cnx, bytes, length, event, streamCtx);
  }
  static std::shared_ptr<MoQExecutor> getExecutor(MoQPicoServerBase* server) {
    return server->executor_;
  }
};

namespace {

static size_t alpnSelectCallback(picoquic_quic_t* quic,
                                 picoquic_iovec_t* list,
                                 size_t count) {
  auto* server = static_cast<MoQPicoServerBase*>(
      picoquic_get_default_callback_context(quic));
  const auto& wtConfig = MoQPicoServerBaseCallbacks::getWTConfig(server);

  // Build list of supported ALPNs based on configuration
  std::vector<std::string> supportedAlpns;

  // Add h3 first if WebTransport is enabled (higher priority for browsers)
  if (wtConfig.enableWebTransport) {
    supportedAlpns.push_back("h3");
  }

  // Add raw MoQ ALPNs if enabled
  if (wtConfig.enableRawMoQ) {
    auto moqtAlpns =
        getMoqtProtocols(MoQPicoServerBaseCallbacks::getVersions(server), true);
    supportedAlpns.insert(
        supportedAlpns.end(), moqtAlpns.begin(), moqtAlpns.end());
  }

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

  if (fin_or_event == picoquic_callback_request_alpn_list ||
      fin_or_event == picoquic_callback_set_alpn) {
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
               << PicoProtocolDispatcher::protocolName(protocol);

    if (protocol == PicoProtocolType::WebTransport) {
      // Create per-connection h3zero context and switch to h3zero_callback
      auto* h3Ctx = MoQPicoServerBaseCallbacks::getOrCreateH3Ctx(server, cnx);
      if (h3Ctx) {
        XLOG(DBG1) << "Switching to h3zero_callback for WebTransport, cnx="
                   << (void*)cnx << " h3Ctx=" << (void*)h3Ctx;
        picoquic_set_callback(cnx, h3zero_callback, h3Ctx);
        // Forward almost_ready event to h3zero
        return h3zero_callback(cnx, stream_id, bytes, length, fin_or_event, h3Ctx, v_stream_ctx);
      }
    }
    return 0;
  }

  if (fin_or_event == picoquic_callback_ready) {
    auto* server = static_cast<MoQPicoServerBase*>(callback_ctx);
    if (!server) {
      XLOG(ERR) << "picoCallback: server is null on ready event";
      return PICOQUIC_ERROR_UNEXPECTED_ERROR;
    }

    XLOG(DBG1) << "New connection ready";

    // Determine protocol based on negotiated ALPN
    const char* alpn = picoquic_tls_get_negotiated_alpn(cnx);
    auto protocol = PicoProtocolDispatcher::getProtocol(alpn);

    XLOG(DBG1) << "Negotiated ALPN: " << (alpn ? alpn : "(null)")
               << " -> Protocol: "
               << PicoProtocolDispatcher::protocolName(protocol);

    if (protocol == PicoProtocolType::WebTransport) {
      // WebTransport was switched to h3zero_callback in almost_ready.
      // This shouldn't happen, but forward to h3zero just in case.
      auto* h3Ctx = MoQPicoServerBaseCallbacks::getOrCreateH3Ctx(server, cnx);
      if (h3Ctx) {
        return h3zero_callback(cnx, stream_id, bytes, length, fin_or_event, h3Ctx, v_stream_ctx);
      }
    } else {
      // Route to raw MoQ handler (existing path)
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

  XLOG(DBG6) << "Forwarding event " << fin_or_event << " to PicoQuicWebTransport";
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
      XLOG(WARN) << "WebTransport CONNECT refused";
      break;

    case picohttp_callback_connect_accepted:
      XLOG(DBG1) << "WebTransport CONNECT accepted";
      break;

    default:
      // Forward other events to the WebTransport adapter
      return MoQPicoServerBaseCallbacks::onWebTransportEvent(
          server, cnx, bytes, length, static_cast<int>(event), streamCtx);
  }

  return 0;
}

} // namespace

// ---------------------------------------------------------------------------

MoQPicoServerBase::MoQPicoServerBase(std::string cert,
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
  // Build and log supported ALPNs
  std::vector<std::string> supportedAlpns;
  if (wtConfig_.enableWebTransport) {
    supportedAlpns.push_back("h3");
  }
  if (wtConfig_.enableRawMoQ) {
    auto moqtAlpns = getMoqtProtocols(versions_, true);
    supportedAlpns.insert(
        supportedAlpns.end(), moqtAlpns.begin(), moqtAlpns.end());
  }
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

  // Initialize h3zero for WebTransport if enabled
  if (wtConfig_.enableWebTransport) {
    if (!initH3Zero()) {
      XLOG(ERR) << "Failed to initialize h3zero for WebTransport";
      destroyQuicContext();
      return false;
    }
    // Enable WebTransport transport parameters
    picowt_set_default_transport_parameters(quic_);
    XLOG(INFO) << "WebTransport enabled on endpoint: " << wtConfig_.wtEndpoint;
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

void MoQPicoServerBase::onNewWebTransportConnectionImpl(void* vcnx) {
  // NOTE: This function is no longer called in the normal flow.
  // WebTransport connections switch to h3zero_callback during set_alpn,
  // so h3zero handles almost_ready and ready events directly.
  // MoQSession creation happens in onWebTransportConnectImpl when
  // the browser sends CONNECT.
  auto* cnx = static_cast<picoquic_cnx_t*>(vcnx);
  XLOG(WARN) << "onNewWebTransportConnectionImpl called unexpectedly";
  (void)cnx;
}

bool MoQPicoServerBase::initH3Zero() {
  // Create path table for WebTransport endpoint
  // We need to keep this alive for the lifetime of the server
  wtPathTable_ = std::make_unique<picohttp_server_path_item_t[]>(2);

  // Configure the WebTransport endpoint path
  // The callback will be invoked when a CONNECT request is received
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
  // Clean up all per-connection h3zero contexts
  for (auto& [cnx, h3Ctx] : h3Contexts_) {
    XLOG(DBG2) << "Cleaning up h3Ctx for cnx=" << (void*)cnx;
    h3zero_callback_delete_context(cnx, h3Ctx);
  }
  h3Contexts_.clear();
  wtPathTable_.reset();
}

int MoQPicoServerBase::onWebTransportConnectImpl(
    picoquic_cnx_t* cnx,
    h3zero_stream_ctx_t* streamCtx) {
  XLOG(DBG1) << "WebTransport CONNECT received on stream " << streamCtx->stream_id
             << " cnx=" << (void*)cnx;

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

  XLOG(DBG1) << "Accepting WebTransport session from " << peerSockAddr.describe();

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
  onH3WebTransportCreated(*webTransport);

  // Create MoQSession
  auto moqSession = createSession(webTransport, executor_);
  webTransport->setHandler(moqSession.get());

  // Negotiate MoQ version via WebTransport protocol negotiation.
  // The client sends wt-available-protocols, we select from our supported versions.
  int wtProtoRet = picowt_select_wt_protocol(streamCtx, versions_.c_str());
  if (wtProtoRet == 0 && streamCtx->ps.stream_state.wt_protocol) {
    const char* selectedProto = streamCtx->ps.stream_state.wt_protocol;
    XLOG(DBG1) << "WebTransport selected protocol: " << selectedProto;
    moqSession->validateAndSetVersionFromAlpn(selectedProto);
  } else {
    // No protocol match or client didn't send wt-available-protocols.
    // Default to latest version for compatibility with older clients.
    XLOG(DBG1) << "No WT protocol negotiated, defaulting to moqt-16";
    moqSession->validateAndSetVersionFromAlpn("moqt-16");
  }

  // Set path_callback_ctx to the server pointer so h3zero passes it for
  // events like provide_data and free. Must be MoQPicoServerBase*, NOT
  // PicoH3WebTransport* - we look up the WebTransport adapter via wtSessions_.
  streamCtx->path_callback_ctx = this;

  // Store in wtSessions_ map for lifecycle management and event routing.
  // Key is (cnx, control stream ID) since stream IDs are per-connection.
  wtSessions_[{cnx, streamCtx->stream_id}] = {webTransport, moqSession};

  // Note: onNewSession will be called by handleClientSession()

  // NOTE: h3zero automatically sends 200 response and sets is_upgraded=1
  // when we return 0 from this callback (see h3zero_common.c:1138).
  // Do NOT send duplicate response here!

  // Register stream prefix so new WT streams are routed to our callback
  // The prefix is the control stream ID - h3zero uses this to find our callback
  // when new bidi/uni streams arrive with this session ID in their header
  int ret = h3zero_declare_stream_prefix(
      h3Ctx, streamCtx->stream_id, wtPathCallback, this);
  if (ret != 0) {
    XLOG(ERR) << "Failed to declare stream prefix: " << ret;
    return ret;
  }
  XLOG(DBG2) << "Registered stream prefix for control stream " << streamCtx->stream_id;

  // Start handling the MoQ session
  folly::coro::co_withExecutor(executor_.get(), handleClientSession(moqSession))
      .start();

  XLOG(DBG1) << "WebTransport session accepted on stream " << streamCtx->stream_id
             << " cnx=" << (void*)cnx;
  return 0;
}

int MoQPicoServerBase::onWebTransportEventImpl(
    picoquic_cnx_t* cnx,
    uint8_t* bytes,
    size_t length,
    int event,
    h3zero_stream_ctx_t* streamCtx) {
  // Find the WebTransport adapter for this stream
  PicoH3WebTransport* wt = nullptr;

  if (!streamCtx) {
    return 0;
  }

  uint64_t streamId = streamCtx->stream_id;

  XLOG(DBG2) << "onWebTransportEventImpl: cnx=" << (void*)cnx
             << " event=" << event << " stream=" << streamId;

  // Check if this stream ID is a control stream (key in wtSessions_).
  // For control streams, we manually set path_callback_ctx to PicoH3WebTransport*.
  // For data streams, h3zero sets path_callback_ctx to the stream_prefix function_ctx
  // (which is MoQPicoServerBase*), so we must NOT use it as PicoH3WebTransport*.
  // Key is (cnx, stream ID) since stream IDs are per-connection.
  auto controlIt = wtSessions_.find({cnx, streamId});
  if (controlIt != wtSessions_.end()) {
    // This IS a control stream - use the stored WebTransport adapter
    wt = controlIt->second.webTransport.get();
    XLOG(DBG3) << "onWebTransportEventImpl: control stream " << streamId
               << " cnx=" << (void*)cnx << " wt=" << (void*)wt;
  } else {
    // This is a data stream - find the session by control_stream_id
    uint64_t controlStreamId = streamCtx->ps.stream_state.control_stream_id;
    XLOG(DBG2) << "onWebTransportEventImpl: data stream " << streamId
               << " cnx=" << (void*)cnx << " has control_stream_id=" << controlStreamId;
    auto it = wtSessions_.find({cnx, controlStreamId});
    if (it != wtSessions_.end()) {
      wt = it->second.webTransport.get();
      XLOG(DBG2) << "onWebTransportEventImpl: found session for control_stream_id="
                 << controlStreamId << " cnx=" << (void*)cnx << " wt=" << (void*)wt;
    } else {
      XLOG(WARN) << "onWebTransportEventImpl: no session found for cnx=" << (void*)cnx
                 << " control_stream_id=" << controlStreamId
                 << " (wtSessions_ size=" << wtSessions_.size() << ")";
    }
  }

  if (!wt) {
    XLOG(DBG4) << "No WebTransport adapter found for event " << event;
    return 0;
  }

  return wt->handleWtEvent(cnx, bytes, length, event, streamCtx);
}

} // namespace moxygen
