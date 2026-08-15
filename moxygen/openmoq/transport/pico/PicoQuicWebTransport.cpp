/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/PicoQuicWebTransport.h"
#include <folly/logging/xlog.h>
#include <picoquic.h>

namespace moxygen {

// File-local picoquic callback that delegates to the PicoQuicWebTransport
// instance. Used as the function pointer passed to picoquic_set_callback().
static int picoCallback(
    picoquic_cnx_t* cnx,
    uint64_t stream_id,
    uint8_t* bytes,
    size_t length,
    picoquic_call_back_event_t fin_or_event,
    void* callback_ctx,
    void* v_stream_ctx) {
  auto* self = static_cast<PicoQuicWebTransport*>(callback_ctx);
  if (!self) {
    return -1;
  }
  return self->handlePicoEvent(
      cnx,
      stream_id,
      bytes,
      length,
      static_cast<int>(fin_or_event),
      v_stream_ctx);
}

PicoQuicWebTransport::PicoQuicWebTransport(
    picoquic_cnx_t* cnx,
    const folly::SocketAddress& localAddr,
    const folly::SocketAddress& peerAddr)
    : PicoWebTransportBase(cnx, picoquic_is_client(cnx), localAddr, peerAddr) {
  // Set the callback context for picoquic
  picoquic_set_callback(cnx_, moxygen::picoCallback, this);
}

PicoQuicWebTransport::~PicoQuicWebTransport() {
  // Ensure callback is cleared even if closeSession wasn't called
  clearPicoquicCallback();
}

// Transport-specific implementations

folly::Expected<uint64_t, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::createStreamImpl(bool bidi) {
  // Get next local stream ID from picoquic
  uint64_t streamId =
      picoquic_get_next_local_stream_id(cnx_, bidi ? 0 : 1 /* is_unidir */);

  // Reserve the stream ID in picoquic by setting app stream context
  int ret = picoquic_set_app_stream_ctx(cnx_, streamId, nullptr);
  if (ret != 0) {
    XLOG(ERR) << "Failed to reserve stream ID " << streamId
              << " in picoquic, error=" << ret;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  return streamId;
}

void PicoQuicWebTransport::markStreamActiveImpl(uint64_t streamId) {
  XCHECK(cnx_);
  // Mark stream as active in picoquic
  // Stream context is nullptr - we use callback context instead
  int ret = picoquic_mark_active_stream(cnx_, streamId, 1, nullptr);
  if (ret != 0) {
    XLOG(WARN) << "Failed to mark stream " << streamId
               << " as active, error=" << ret;
  }
}

void PicoQuicWebTransport::markDatagramActiveImpl() {
  XCHECK(cnx_);
  XLOG(DBG4) << "markDatagramActiveImpl: marking datagram as ready";
  int ret = picoquic_mark_datagram_ready(cnx_, 1);
  if (ret != 0) {
    XLOG(WARN) << "Failed to mark datagram as ready, error=" << ret;
  }
}

void PicoQuicWebTransport::resetStreamImpl(uint64_t streamId, uint32_t error) {
  XCHECK(cnx_);
  int ret = picoquic_reset_stream(cnx_, streamId, error);
  if (ret != 0) {
    XLOG(WARN) << "Failed to reset stream " << streamId << " error=" << error
               << " picoquic_ret=" << ret;
  }
}

void PicoQuicWebTransport::stopSendingImpl(uint64_t streamId, uint32_t error) {
  XCHECK(cnx_);
  int ret = picoquic_stop_sending(cnx_, streamId, error);
  if (ret != 0) {
    XLOG(WARN) << "Failed to stop sending on stream " << streamId
               << " error=" << error << " picoquic_ret=" << ret;
  }
}

void PicoQuicWebTransport::sendCloseImpl(uint32_t errorCode) {
  XCHECK(cnx_);
  int ret = picoquic_close(cnx_, errorCode);
  if (ret != 0) {
    XLOG(WARN) << "picoquic_close failed with error=" << ret;
  }
}

void PicoQuicWebTransport::onSessionClosedImpl() {
  // Signal the owner (if set) to stop shared I/O after this drain cycle.
  if (auto cb = std::exchange(onConnectionClosedCallback_, nullptr)) {
    cb();
  }
  // Clear picoquic callback to prevent further events
  clearPicoquicCallback();
}

// Picoquic event handling

int PicoQuicWebTransport::handlePicoEvent(
    picoquic_cnx_t* /*cnx*/,
    uint64_t stream_id,
    uint8_t* bytes,
    size_t length,
    int fin_or_event_int,
    void* /*v_stream_ctx*/) {
  auto fin_or_event = static_cast<picoquic_call_back_event_t>(fin_or_event_int);

  XLOG(DBG6) << "picoCallback: event=" << fin_or_event
             << " stream_id=" << stream_id << " length=" << length;

  switch (fin_or_event) {
    case picoquic_callback_stream_data:
    case picoquic_callback_stream_fin:
      onStreamDataCommon(
          stream_id,
          bytes,
          length,
          fin_or_event == picoquic_callback_stream_fin);
      break;

    case picoquic_callback_datagram:
      XLOG(DBG4) << "picoCallback: datagram received, length=" << length;
      onReceiveDatagramCommon(bytes, length);
      break;

    case picoquic_callback_stream_reset:
      // length contains error code
      onStreamResetCommon(stream_id, length);
      break;

    case picoquic_callback_stop_sending:
      // length contains error code
      onStopSendingCommon(stream_id, length);
      break;

    case picoquic_callback_close:
    case picoquic_callback_application_close:
      onSessionCloseCommon(static_cast<uint32_t>(length));
      break;

    case picoquic_callback_prepare_to_send: {
      // JIT callback - picoquic is ready to send data on this stream
      onJitProvideData(stream_id, bytes, length);
      break;
    }

    case picoquic_callback_prepare_datagram:
      // JIT callback - picoquic is ready to send a datagram
      onJitProvideDatagram(bytes, length);
      break;

    case picoquic_callback_stream_gap:
      XLOG(DBG2) << "Stream gap on stream " << stream_id;
      break;

    default:
      // Other events we don't handle for now
      break;
  }

  return 0;
}

uint8_t* PicoQuicWebTransport::getDatagramBuffer(
    uint8_t* context,
    size_t length,
    bool keepPolling) {
  if (length == 0) {
    // length==0 is the defer/stop path. picoquic_provide_datagram_buffer_ex
    // with picoquic_datagram_active_any_path defers to the next packet;
    // picoquic_datagram_not_active cancels the pending datagram-ready signal
    // set by markDatagramActiveImpl (queue drained or peer can't receive).
    picoquic_provide_datagram_buffer_ex(
        context,
        0,
        keepPolling ? picoquic_datagram_active_any_path
                    : picoquic_datagram_not_active);
    return nullptr;
  }
  // Normal send path. keepPolling is not passed here — picoquic re-polls as
  // long as picoquic_mark_datagram_ready is set (done in
  // markDatagramActiveImpl). The base class issues a length==0 /
  // keepPolling=false call once the queue drains to cancel the ready signal.
  return picoquic_provide_datagram_buffer(context, length);
}

void PicoQuicWebTransport::clearPicoquicCallback() {
  if (cnx_) {
    // Clear the callback to prevent further callbacks to this object
    picoquic_set_callback(cnx_, nullptr, nullptr);
    cnx_ = nullptr;
  }
}

} // namespace moxygen
