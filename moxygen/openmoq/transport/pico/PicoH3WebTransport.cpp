/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/PicoH3WebTransport.h"
#include <folly/logging/xlog.h>
#include <h3zero_common.h>
#include <pico_webtransport.h>
#include <picoquic.h>

namespace moxygen {

PicoH3WebTransport::PicoH3WebTransport(
    picoquic_cnx_t* cnx,
    h3zero_callback_ctx_t* h3Ctx,
    h3zero_stream_ctx_t* controlStreamCtx,
    const folly::SocketAddress& localAddr,
    const folly::SocketAddress& peerAddr)
    : PicoWebTransportBase(
          cnx,
          false /* isClient - server side */,
          localAddr,
          peerAddr),
      h3Ctx_(h3Ctx),
      controlStreamCtx_(controlStreamCtx) {
  // Store control stream context
  streamContexts_[controlStreamCtx_->stream_id] = controlStreamCtx_;

  XLOG(DBG1) << "PicoH3WebTransport created, control stream="
             << controlStreamCtx_->stream_id;
}

PicoH3WebTransport::~PicoH3WebTransport() {
  // Base class destructor handles cleanup
}

uint64_t PicoH3WebTransport::getControlStreamId() const {
  return controlStreamCtx_ ? controlStreamCtx_->stream_id : 0;
}

// Transport-specific implementations

folly::Expected<uint64_t, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::createStreamImpl(bool bidi) {
  // Create WebTransport stream via h3zero
  auto* streamCtx = picowt_create_local_stream(
      cnx_, bidi ? 1 : 0, h3Ctx_, controlStreamCtx_->stream_id);

  if (!streamCtx) {
    XLOG(ERR) << "Failed to create WebTransport " << (bidi ? "bidir" : "unidir")
              << " stream";
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Copy path_callback from control stream so h3zero routes JIT callbacks to us
  streamCtx->path_callback = controlStreamCtx_->path_callback;
  streamCtx->path_callback_ctx = controlStreamCtx_->path_callback_ctx;

  uint64_t streamId = streamCtx->stream_id;
  streamContexts_[streamId] = streamCtx;

  XLOG(DBG2) << "Created WebTransport " << (bidi ? "bidir" : "unidir")
             << " stream: " << streamId;

  return streamId;
}

void PicoH3WebTransport::markStreamActiveImpl(uint64_t streamId) {
  auto it = streamContexts_.find(streamId);
  if (it == streamContexts_.end()) {
    XLOG(DBG2) << "markStreamActiveImpl: no context for stream " << streamId;
    return;
  }

  XLOG(DBG5) << "markStreamActiveImpl: stream=" << streamId;
  int ret = picoquic_mark_active_stream(cnx_, streamId, 1, it->second);
  if (ret != 0) {
    XLOG(WARN) << "Failed to mark stream " << streamId
               << " as active, error=" << ret;
  }
}

void PicoH3WebTransport::markDatagramActiveImpl() {
  XCHECK(cnx_);
  XCHECK(controlStreamCtx_);
  h3zero_set_datagram_ready(cnx_, controlStreamCtx_->stream_id);
}

void PicoH3WebTransport::resetStreamImpl(uint64_t streamId, uint32_t error) {
  auto it = streamContexts_.find(streamId);
  if (it != streamContexts_.end()) {
    picowt_reset_stream(cnx_, it->second, error);
    if (it->second->is_h3) {
      it->second->ps.stream_state.is_fin_sent = 1;
      maybeDeleteStream(it->second);
    }
  }
}

void PicoH3WebTransport::stopSendingImpl(uint64_t streamId, uint32_t error) {
  XCHECK(cnx_);
  picoquic_stop_sending(cnx_, streamId, error);
}

uint8_t* PicoH3WebTransport::getDatagramBuffer(
    uint8_t* context,
    size_t length,
    bool keepPolling) {
  // h3zero_provide_datagram_buffer maps cleanly onto the three-case contract:
  //   length > 0              → allocate and return write buffer; keepPolling
  //                             controls whether h3zero polls this session
  //                             again after this packet (normal send path).
  //   length == 0, keepPolling=true  → defer: sets application_ready so h3zero
  //                             re-polls next packet (datagram didn't fit).
  //   length == 0, keepPolling=false → stop: application_ready stays 0, h3zero
  //                             stops polling (queue drained or peer can't
  //                             recv).
  return h3zero_provide_datagram_buffer(context, length, keepPolling ? 1 : 0);
}

size_t PicoH3WebTransport::getMaxDatagramPayload() const {
  size_t base = PicoWebTransportBase::getMaxDatagramPayload();
  // h3zero prefixes each WT datagram with the quarter-stream-ID varint.
  // A stream ID varint is at most 8 bytes; subtract conservatively so we
  // don't defer a datagram that can never fit in any packet.
  constexpr size_t kMaxStreamIdPrefixLen = 8;
  return base > kMaxStreamIdPrefixLen ? base - kMaxStreamIdPrefixLen : 0;
}

void PicoH3WebTransport::sendCloseImpl(uint32_t errorCode) {
  XCHECK(cnx_);
  XCHECK(controlStreamCtx_);
  picowt_send_close_session_message(
      cnx_, controlStreamCtx_, errorCode, nullptr);
}

// H3zero event handling

int PicoH3WebTransport::handleWtEvent(
    picoquic_cnx_t* /*cnx*/,
    uint8_t* bytes,
    size_t length,
    int wtEvent,
    h3zero_stream_ctx_t* streamCtx) {
  auto event = static_cast<picohttp_call_back_event_t>(wtEvent);

  XLOG(DBG5) << "handleWtEvent: event=" << wtEvent
             << " stream=" << (streamCtx ? streamCtx->stream_id : 0)
             << " length=" << length;

  switch (event) {
    case picohttp_callback_post:
      // picohttp_callback_post is for the initial CONNECT request body.
      // For WebTransport, this contains capsule data that h3zero handles
      // internally, so we ignore it here. Only the control stream gets this.
      XLOG(DBG5) << "Ignoring picohttp_callback_post on control stream";
      break;

    case picohttp_callback_post_data:
      // Data received on a WebTransport stream
      if (streamCtx) {
        onStreamData(streamCtx, bytes, length, false);
      }
      break;

    case picohttp_callback_post_fin:
      // FIN received on a WebTransport stream
      if (streamCtx) {
        onStreamData(streamCtx, bytes, length, true);
        if (streamCtx->is_h3) {
          streamCtx->ps.stream_state.is_fin_received = 1;
        }
        maybeDeleteStream(streamCtx);
      }
      break;

    case picohttp_callback_post_datagram:
      // Datagram received
      onReceiveDatagramCommon(bytes, length);
      break;

    case picohttp_callback_provide_data:
      // Ready to send data on a stream - JIT callback
      if (streamCtx) {
        bool finSent = onJitProvideData(streamCtx->stream_id, bytes, length);
        if (finSent && streamCtx->is_h3) {
          streamCtx->ps.stream_state.is_fin_sent = 1;
          maybeDeleteStream(streamCtx);
        }
      }
      break;

    case picohttp_callback_provide_datagram:
      // Ready to send datagram - delegate to shared base implementation
      onJitProvideDatagram(bytes, length);
      break;

    case picohttp_callback_reset:
      // Stream was reset by peer. Do not erase from streamContexts_ here —
      // picohttp_callback_free is the authoritative signal that h3zero is
      // done with this stream and will fire after reset.
      if (streamCtx) {
        if (streamCtx->is_h3) {
          streamCtx->ps.stream_state.is_fin_received = 1;
        }
        onStreamResetCommon(streamCtx->stream_id, 0);
        maybeDeleteStream(streamCtx);
      }
      break;

    case picohttp_callback_stop_sending:
      // Peer sent STOP_SENDING
      if (streamCtx) {
        onStopSendingCommon(streamCtx->stream_id, 0);
      }
      break;

    case picohttp_callback_deregister:
      // Stream prefix removed: session is closing. Notify but don't delete
      // yet — picohttp_callback_free still fires for each stream in the splay
      // tree (including the control stream).
      onSessionCloseCommon(0);
      deregistered_ = true;
      break;

    case picohttp_callback_free:
      // Individual stream being freed by h3zero. Once all streams are gone
      // and we've been deregistered, signal the caller to delete sessionCtx.
      if (streamCtx) {
        streamContexts_.erase(streamCtx->stream_id);
        if (controlStreamCtx_ &&
            streamCtx->stream_id == controlStreamCtx_->stream_id) {
          controlStreamCtx_ = nullptr;
        }
      }
      if (deregistered_ && streamContexts_.empty()) {
        return kDeleteCtx;
      }
      break;

    default:
      XLOG(DBG2) << "Unhandled WebTransport event: " << wtEvent;
      break;
  }

  return 0;
}

void PicoH3WebTransport::maybeDeleteStream(
    h3zero_stream_ctx_t* streamCtx) noexcept {
  if (streamCtx->is_h3 && streamCtx->ps.stream_state.is_fin_received &&
      streamCtx->ps.stream_state.is_fin_sent) {
    XLOG(DBG5) << "maybeDeleteStream: deleting stream " << streamCtx->stream_id;
    h3zero_delete_stream(cnx_, h3Ctx_, streamCtx);
    // picohttp_callback_free will fire and erase from streamContexts_
  }
}

void PicoH3WebTransport::onStreamData(
    h3zero_stream_ctx_t* streamCtx,
    uint8_t* bytes,
    size_t length,
    bool fin) {
  uint64_t streamId = streamCtx->stream_id;

  XLOG(DBG5) << "onStreamData: stream=" << streamId << " length=" << length
             << " fin=" << fin;

  // Handle control stream capsules (DRAIN_SESSION, CLOSE_SESSION, etc.)
  if (controlStreamCtx_ && streamId == controlStreamCtx_->stream_id) {
    if (bytes && length > 0) {
      // Parse WebTransport capsules
      picowt_capsule_t capsule = {};
      int ret = picowt_receive_capsule(
          cnx_, controlStreamCtx_, bytes, bytes + length, &capsule);
      if (ret != 0) {
        XLOG(ERR) << "Failed to parse WebTransport capsule: " << ret
                  << ", tearing down session";
        onSessionCloseCommon(static_cast<uint32_t>(ret));
        return;
      }
      if (capsule.h3_capsule.is_stored) {
        uint64_t capsuleType = capsule.h3_capsule.capsule_type;
        if (capsuleType == picowt_capsule_close_webtransport_session ||
            capsuleType == picowt_capsule_drain_webtransport_session) {
          XLOG(DBG1) << "WebTransport session close/drain requested, error="
                     << capsule.error_code;
          onSessionCloseCommon(capsule.error_code);
        } else {
          XLOG(DBG2) << "Unhandled WT capsule type: 0x" << std::hex
                     << capsuleType;
        }
      }
      picowt_release_capsule(&capsule);
    }
    return;
  }

  // Track stream context
  if (streamContexts_.find(streamId) == streamContexts_.end()) {
    streamContexts_[streamId] = streamCtx;
    XLOG(DBG5) << "Tracking new stream context for stream " << streamId;
  }

  // Delegate to base class common handler
  onStreamDataCommon(streamId, bytes, length, fin);
}

} // namespace moxygen
