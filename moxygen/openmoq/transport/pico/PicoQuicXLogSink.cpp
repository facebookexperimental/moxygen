/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/PicoQuicXLogSink.h"

#include <folly/Range.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <string>

extern "C" {
#include "picoquic.h"
#include "picoquic_unified_log.h"
}

namespace moxygen::openmoq::pico {
namespace {

// ─── helpers ─────────────────────────────────────────────────────────────────

// Hex-encode a connection ID into a short string (for log line prefixes).
std::string cidToHex(const picoquic_connection_id_t* cid) {
  if (!cid || cid->id_len == 0) {
    return "<empty>";
  }
  return folly::hexlify(folly::ByteRange{cid->id, cid->id_len});
}

// Stable short connection identifier (first 8 hex chars of initial CID).
std::string cnxShort(picoquic_cnx_t* cnx) {
  if (!cnx) {
    return "<null>";
  }
  picoquic_connection_id_t cid = picoquic_get_initial_cnxid(cnx);
  std::string hex = cidToHex(&cid);
  return hex.size() > 8 ? hex.substr(0, 8) : hex;
}

// Format a printf-style message into a stack buffer; safe for log lines.
// Truncates with "..." if the message is longer than the buffer.
std::string vformat(const char* fmt, va_list args) {
  char buf[1024];
  int n = vsnprintf(buf, sizeof(buf), fmt, args);
  if (n <= 0) {
    return {};
  }
  if (static_cast<size_t>(n) >= sizeof(buf)) {
    constexpr const char kEllipsis[] = "...";
    constexpr size_t kEllipsisLen = sizeof(kEllipsis) - 1;
    memcpy(buf + sizeof(buf) - kEllipsisLen - 1, kEllipsis, kEllipsisLen);
    buf[sizeof(buf) - 1] = '\0';
    return buf;
  }
  return {buf, static_cast<size_t>(n)};
}

// ─── per-context callbacks ───────────────────────────────────────────────────
// Each per-context callback receives `void* log_param` — the value passed to
// picoquic_register_log_functions(). We pass nullptr (no per-quic state), so
// log_param is unused.

extern "C" void xlogLogQuicAppMessage(
    picoquic_quic_t* /*quic*/,
    void* /*log_param*/,
    const picoquic_connection_id_t* cid,
    const char* fmt,
    va_list args) {
  XLOG(INFO) << "[cid=" << cidToHex(cid) << "] " << vformat(fmt, args);
}

extern "C" void xlogLogQuicPdu(
    picoquic_quic_t* /*quic*/,
    void* /*log_param*/,
    int receiving,
    uint64_t /*current_time*/,
    uint64_t cid64,
    const struct sockaddr* /*addr_peer*/,
    const struct sockaddr* /*addr_local*/,
    size_t packet_length) {
  XLOG(DBG3) << "[cid64=" << cid64 << "] " << (receiving ? "<- " : "-> ")
             << "stray pdu len=" << packet_length;
}

extern "C" void xlogLogQuicClose(
    picoquic_quic_t* /*quic*/,
    void* /*log_param*/) {
  // Nothing to release — the XLog sink owns no per-context state.
}

// ─── per-connection callbacks ────────────────────────────────────────────────
// Each per-cnx callback receives `void* log_ctx` — the value xlogLogNewConnection
// seeded into picoquic's per-cnx log_ctx slot. We currently set log_ctx to the
// cnx pointer itself as a non-null sentinel (NULL would tell the dispatcher to
// skip this cnx). A follow-up can attach per-cnx state (e.g. MoQ session-id
// tagger) by allocating a small struct in xlogLogNewConnection and freeing it
// in xlogLogCloseConnection.

extern "C" void xlogLogAppMessage(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    const char* fmt,
    va_list args) {
  XLOG(INFO) << "[cnx=" << cnxShort(cnx) << "] " << vformat(fmt, args);
}

extern "C" void xlogLogPdu(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    int receiving,
    uint64_t /*current_time*/,
    const struct sockaddr* /*addr_peer*/,
    const struct sockaddr* /*addr_local*/,
    size_t packet_length,
    uint64_t /*unique_path_id*/,
    unsigned char /*ecn*/) {
  XLOG(DBG3) << "[cnx=" << cnxShort(cnx) << "] " << (receiving ? "<- " : "-> ")
             << "pdu len=" << packet_length;
}

extern "C" void xlogLogPacket(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    picoquic_path_t* /*path*/,
    int receiving,
    uint64_t /*current_time*/,
    struct st_picoquic_packet_header_t* /*ph*/,
    const uint8_t* /*bytes*/,
    size_t bytes_max) {
  XLOG(DBG3) << "[cnx=" << cnxShort(cnx) << "] " << (receiving ? "<- " : "-> ")
             << "pkt len=" << bytes_max;
}

extern "C" void xlogLogDroppedPacket(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    picoquic_path_t* /*path*/,
    struct st_picoquic_packet_header_t* /*ph*/,
    size_t packet_size,
    int err,
    uint64_t /*current_time*/) {
  XLOG(DBG1) << "[cnx=" << cnxShort(cnx) << "] "
             << "dropped pkt size=" << packet_size << " err=" << err;
}

extern "C" void xlogLogBufferedPacket(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    picoquic_path_t* /*path*/,
    picoquic_packet_type_enum ptype,
    uint64_t /*current_time*/) {
  XLOG(DBG2) << "[cnx=" << cnxShort(cnx) << "] "
             << "buffered pkt type=" << static_cast<int>(ptype);
}

extern "C" void xlogLogOutgoingPacket(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    picoquic_path_t* /*path*/,
    uint8_t* /*bytes*/,
    uint64_t sequence_number,
    size_t /*pn_length*/,
    size_t length,
    uint8_t* /*send_buffer*/,
    size_t /*send_length*/,
    uint64_t /*current_time*/) {
  XLOG(DBG3) << "[cnx=" << cnxShort(cnx) << "] "
             << "out pkt seq=" << sequence_number << " len=" << length;
}

extern "C" void xlogLogPacketLost(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    picoquic_path_t* /*path*/,
    picoquic_packet_type_enum ptype,
    uint64_t sequence_number,
    char const* trigger,
    picoquic_connection_id_t* /*dcid*/,
    size_t packet_size,
    uint64_t /*current_time*/) {
  XLOG(DBG1) << "[cnx=" << cnxShort(cnx) << "] "
             << "lost pkt type=" << static_cast<int>(ptype)
             << " seq=" << sequence_number << " sz=" << packet_size
             << " trigger=" << (trigger ? trigger : "?");
}

extern "C" void xlogLogNegotiatedAlpn(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    int is_local,
    uint8_t const* sni,
    size_t sni_len,
    uint8_t const* alpn,
    size_t alpn_len,
    const ptls_iovec_t* /*alpn_list*/,
    size_t /*alpn_count*/) {
  std::string sniStr = (sni && sni_len > 0)
      ? std::string(reinterpret_cast<const char*>(sni), sni_len)
      : "";
  std::string alpnStr = (alpn && alpn_len > 0)
      ? std::string(reinterpret_cast<const char*>(alpn), alpn_len)
      : "";
  XLOG(DBG1) << "[cnx=" << cnxShort(cnx) << "] "
             << "ALPN negotiated (is_local=" << is_local << " sni=" << sniStr
             << " alpn=" << alpnStr << ")";
}

extern "C" void xlogLogTransportExtension(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    int is_local,
    size_t param_length,
    uint8_t* /*params*/) {
  XLOG(DBG2) << "[cnx=" << cnxShort(cnx) << "] "
             << "transport extension is_local=" << is_local
             << " param_len=" << param_length;
}

extern "C" void xlogLogTlsTicket(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    uint8_t* /*ticket*/,
    uint16_t ticket_length) {
  XLOG(DBG2) << "[cnx=" << cnxShort(cnx) << "] "
             << "TLS ticket sz=" << ticket_length;
}

// Lifecycle hook for seeding the per-cnx ctx. NULL *log_ctx tells picoquic's
// dispatcher to skip every subsequent callback for this cnx — we opt in by
// stashing a non-null sentinel (the cnx pointer itself).
extern "C" void xlogLogNewConnection(
    picoquic_cnx_t* cnx,
    void* /*log_param*/,
    void** log_ctx) {
  if (log_ctx) {
    *log_ctx = cnx;
  }
  XLOG(DBG1) << "[cnx=" << cnxShort(cnx) << "] new connection";
}

extern "C" void xlogLogCloseConnection(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/) {
  // No per-cnx allocation today, so nothing to free. If log_ctx becomes a
  // heap-allocated state struct in the future, free it here.
  XLOG(DBG1) << "[cnx=" << cnxShort(cnx) << "] close connection";
}

extern "C" void xlogLogCcDump(
    picoquic_cnx_t* cnx,
    void* /*log_ctx*/,
    picoquic_path_t* /*path*/,
    uint64_t /*current_time*/) {
  // Minimal cc-state snapshot. Detailed cc fields are accessible via opaque
  // path/cnx getters; a follow-up can extend this to include rtt, cwnd,
  // ssthresh once we decide which getters are stable across picoquic versions.
  XLOG(DBG2) << "[cnx=" << cnxShort(cnx) << "] cc snapshot";
}

// ─── the unified-logging struct (process-lifetime) ───────────────────────────

// Non-const because picoquic_register_log_functions() takes a non-const
// pointer (the runtime uses the address as a slot-lookup key for the
// companion picoquic_get_log_params() getter). Process-lifetime storage —
// never mutated at runtime.
//
// log_flush is intentionally nullptr: folly XLOG's async handler drains
// continuously and sync_level=WARN already escalates important events to
// synchronous emit on the calling thread. A process-wide LoggerDB flush at
// every picoquic "sensitive event" call site would crater throughput for
// negligible durability gain. picoquic's dispatcher gates on
// (log_flush != NULL), so a null field is the explicit opt-out signal.
picoquic_unified_logging_t kXLogBackend = {
    // Per-context functions
    .log_quic_app_message = xlogLogQuicAppMessage,
    .log_quic_pdu = xlogLogQuicPdu,
    .log_quic_close = xlogLogQuicClose,
    // Per-connection functions
    .log_app_message = xlogLogAppMessage,
    .log_pdu = xlogLogPdu,
    .log_packet = xlogLogPacket,
    .log_dropped_packet = xlogLogDroppedPacket,
    .log_buffered_packet = xlogLogBufferedPacket,
    .log_outgoing_packet = xlogLogOutgoingPacket,
    .log_packet_lost = xlogLogPacketLost,
    .log_negotiated_alpn = xlogLogNegotiatedAlpn,
    .log_transport_extension = xlogLogTransportExtension,
    .log_picotls_ticket = xlogLogTlsTicket,
    .log_new_connection = xlogLogNewConnection,
    .log_close_connection = xlogLogCloseConnection,
    .log_cc_dump = xlogLogCcDump,
    .log_flush = nullptr,
};

} // namespace

void installPicoQuicXLogSink(picoquic_quic_t* quic) {
  if (!quic) {
    XLOG(ERR) << "installPicoQuicXLogSink: null quic context";
    return;
  }
  // Register into the first free slot of picoquic's log-functions table.
  // params=nullptr — this sink owns no per-quic state (logging config is
  // global to folly's LoggerDB). Returns 0 on success, -1 on null args or
  // full table.
  picoquic_register_log_functions(quic, &kXLogBackend, /*params=*/nullptr);
}

} // namespace moxygen::openmoq::pico
