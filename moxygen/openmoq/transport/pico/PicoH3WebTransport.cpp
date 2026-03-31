/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/PicoH3WebTransport.h"
#include <folly/logging/xlog.h>
#include <picoquic.h>
#include <h3zero_common.h>
#include <pico_webtransport.h>

namespace moxygen {

PicoH3WebTransport::PicoH3WebTransport(
    picoquic_cnx_t* cnx,
    h3zero_callback_ctx_t* h3Ctx,
    h3zero_stream_ctx_t* controlStreamCtx,
    const folly::SocketAddress& localAddr,
    const folly::SocketAddress& peerAddr)
    : cnx_(cnx),
      h3Ctx_(h3Ctx),
      controlStreamCtx_(controlStreamCtx),
      localAddr_(localAddr),
      peerAddr_(peerAddr),
      egressCallback_(this),
      ingressCallback_(this) {
  // Configure WtStreamManager flow control limits
  // We set all limits to max() because h3zero/picoquic handles flow control
  proxygen::detail::WtStreamManager::WtConfig wtConfig;
  wtConfig.selfMaxStreamsBidi = std::numeric_limits<uint64_t>::max();
  wtConfig.selfMaxStreamsUni = std::numeric_limits<uint64_t>::max();
  wtConfig.selfMaxConnData = std::numeric_limits<uint64_t>::max();
  wtConfig.selfMaxStreamDataBidi = std::numeric_limits<uint64_t>::max();
  wtConfig.selfMaxStreamDataUni = std::numeric_limits<uint64_t>::max();

  wtConfig.peerMaxStreamsBidi = std::numeric_limits<uint64_t>::max();
  wtConfig.peerMaxStreamsUni = std::numeric_limits<uint64_t>::max();
  wtConfig.peerMaxConnData = std::numeric_limits<uint64_t>::max();
  wtConfig.peerMaxStreamDataBidi = std::numeric_limits<uint64_t>::max();
  wtConfig.peerMaxStreamDataUni = std::numeric_limits<uint64_t>::max();

  // Server-side WebTransport (browsers connect to us)
  streamManager_ = std::make_unique<proxygen::detail::WtStreamManager>(
      proxygen::detail::WtDir::Server,
      wtConfig,
      egressCallback_,
      ingressCallback_,
      priorityQueue_);

  // Store control stream context
  streamContexts_[controlStreamCtx_->stream_id] = controlStreamCtx_;

  XLOG(DBG1) << "PicoH3WebTransport created, control stream="
             << controlStreamCtx_->stream_id;
}

PicoH3WebTransport::~PicoH3WebTransport() {
  handler_ = nullptr;
  if (!sessionClosed_) {
    closeSession(folly::none);
  }
}

uint64_t PicoH3WebTransport::getControlStreamId() const {
  return controlStreamCtx_ ? controlStreamCtx_->stream_id : 0;
}

int PicoH3WebTransport::handleWtEvent(
    picoquic_cnx_t* cnx,
    uint8_t* bytes,
    size_t length,
    int wtEvent,
    h3zero_stream_ctx_t* streamCtx) {
  auto event = static_cast<picohttp_call_back_event_t>(wtEvent);

  XLOG(DBG4) << "handleWtEvent: event=" << wtEvent
             << " stream=" << (streamCtx ? streamCtx->stream_id : 0)
             << " length=" << length;

  switch (event) {
    case picohttp_callback_post:
      // Initial POST/CONNECT body on control stream - ignore
      // WebTransport capsule data on control stream is handled by h3zero
      XLOG(DBG4) << "Ignoring picohttp_callback_post on control stream";
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
      }
      break;

    case picohttp_callback_post_datagram:
      // Datagram received
      onReceiveDatagram(bytes, length);
      break;

    case picohttp_callback_provide_data:
      // Ready to send data on a stream - process egress queue
      processEgressEvents();
      break;

    case picohttp_callback_provide_datagram:
      // Ready to send datagram
      if (!datagramQueue_.empty()) {
        // TODO: Send datagram via h3zero API (picowt_prepare_datagram)
        datagramQueue_.pop_front();
      }
      break;

    case picohttp_callback_reset:
      // Stream was reset by peer
      if (streamCtx) {
        onStreamReset(streamCtx->stream_id, 0);
      }
      break;

    case picohttp_callback_stop_sending:
      // Peer sent STOP_SENDING
      if (streamCtx) {
        onStopSending(streamCtx->stream_id, 0);
      }
      break;

    case picohttp_callback_deregister:
    case picohttp_callback_free:
      // Session is being closed
      onSessionClose(0, nullptr);
      break;

    default:
      XLOG(DBG2) << "Unhandled WebTransport event: " << wtEvent;
      break;
  }

  return 0;
}

void PicoH3WebTransport::onStreamData(
    h3zero_stream_ctx_t* streamCtx,
    uint8_t* bytes,
    size_t length,
    bool fin) {
  uint64_t streamId = streamCtx->stream_id;

  XLOG(DBG4) << "onStreamData: stream=" << streamId << " length=" << length
             << " fin=" << fin;

  // Handle control stream capsules (DRAIN_SESSION, CLOSE_SESSION, etc.)
  // These are WebTransport session management capsules, not MoQ data.
  if (controlStreamCtx_ && streamId == controlStreamCtx_->stream_id) {
    if (bytes && length > 0) {
      // Parse WebTransport capsules
      picowt_capsule_t capsule = {};
      int ret = picowt_receive_capsule(
          cnx_, controlStreamCtx_, bytes, bytes + length, &capsule);
      if (ret != 0) {
        XLOG(ERR) << "Failed to parse WebTransport capsule: " << ret;
      } else if (capsule.h3_capsule.is_stored) {
        XLOG(DBG1) << "Received WT capsule type: 0x" << std::hex
                   << capsule.h3_capsule.capsule_type;
        // Handle CLOSE/DRAIN capsules
        if (capsule.h3_capsule.capsule_type ==
                picowt_capsule_close_webtransport_session ||
            capsule.h3_capsule.capsule_type ==
                picowt_capsule_drain_webtransport_session) {
          XLOG(DBG1) << "WebTransport session close/drain requested, error="
                     << capsule.error_code;
          onSessionClose(capsule.error_code, nullptr);
        }
      }
      picowt_release_capsule(&capsule);
    }
    return;
  }

  // Track stream context
  if (streamContexts_.find(streamId) == streamContexts_.end()) {
    streamContexts_[streamId] = streamCtx;
    XLOG(DBG4) << "Tracking new stream context for stream " << streamId;
  }

  auto* readHandle = streamManager_->getOrCreateIngressHandle(streamId);
  if (!readHandle) {
    XLOG(ERR) << "Failed to get/create ingress handle for stream " << streamId;
    return;
  }

  // Create IOBuf from the data
  std::unique_ptr<folly::IOBuf> data;
  if (bytes && length > 0) {
    data = folly::IOBuf::copyBuffer(bytes, length);
  }

  // Enqueue data to WtStreamManager
  proxygen::detail::WtStreamManager::StreamData streamData{std::move(data), fin};
  auto result = streamManager_->enqueue(*readHandle, std::move(streamData));

  if (result == proxygen::detail::WtStreamManager::Result::Fail) {
    XLOG(ERR) << "Failed to enqueue data for stream " << streamId;
    return;
  }

  // Check if we need to notify handler about this new peer stream
  auto it = pendingStreamNotifications_.find(streamId);
  if (it != pendingStreamNotifications_.end()) {
    pendingStreamNotifications_.erase(it);

    if (!handler_) {
      return;
    }

    // Determine if this is a bidi or uni stream
    bool isBidi = PICOQUIC_IS_BIDIR_STREAM_ID(streamId);

    if (isBidi) {
      // For peer-initiated bidi streams, we need BOTH ingress and egress handles.
      auto bidiHandle = streamManager_->getOrCreateBidiHandle(streamId);
      if (bidiHandle.readHandle && bidiHandle.writeHandle) {
        XLOG(DBG2) << "Notifying handler of new peer bidi stream " << streamId;
        handler_->onNewBidiStream(bidiHandle);
      } else {
        XLOG(ERR) << "Failed to create bidi handle for stream " << streamId;
      }
    } else {
      XLOG(DBG2) << "Notifying handler of new peer uni stream " << streamId;
      handler_->onNewUniStream(readHandle);
    }
  }
}

void PicoH3WebTransport::onStreamReset(uint64_t streamId, uint64_t errorCode) {
  XLOG(DBG2) << "onStreamReset: stream=" << streamId << " error=" << errorCode;
  proxygen::detail::WtStreamManager::ResetStream reset{streamId, errorCode, 0};
  streamManager_->onResetStream(reset);
  streamContexts_.erase(streamId);
}

void PicoH3WebTransport::onStopSending(uint64_t streamId, uint64_t errorCode) {
  XLOG(DBG2) << "onStopSending: stream=" << streamId << " error=" << errorCode;
  proxygen::detail::WtStreamManager::StopSending stopSending{streamId, errorCode};
  streamManager_->onStopSending(stopSending);
}

void PicoH3WebTransport::onSessionClose(
    uint32_t errorCode,
    const char* errorMsg) {
  XLOG(DBG1) << "onSessionClose: error=" << errorCode
             << " msg=" << (errorMsg ? errorMsg : "");
  sessionClosed_ = true;

  // Close stream manager BEFORE calling handler
  proxygen::detail::WtStreamManager::CloseSession closeSession{errorCode, ""};
  streamManager_->onCloseSession(closeSession);

  if (auto handler = std::exchange(handler_, nullptr)) {
    handler->onSessionEnd(errorCode);
  }
}

void PicoH3WebTransport::onReceiveDatagram(uint8_t* bytes, size_t length) {
  XLOG(DBG4) << "onReceiveDatagram: length=" << length;
  if (handler_ && length > 0) {
    auto buf = folly::IOBuf::copyBuffer(bytes, length);
    handler_->onDatagram(std::move(buf));
  }
}

void PicoH3WebTransport::EgressCallback::eventsAvailable() noexcept {
  parent_->processEgressEvents();
}

void PicoH3WebTransport::IngressCallback::onNewPeerStream(
    uint64_t streamId) noexcept {
  // NOTE: This callback is invoked by WtStreamManager during BidiHandle
  // construction, BEFORE the handle is inserted into the map. We must NOT
  // call getOrCreateBidiHandle or getBidiHandle here.
  //
  // Skip the control stream - it's for WebTransport session control,
  // not application (MoQ) data.
  if (parent_->controlStreamCtx_ &&
      streamId == parent_->controlStreamCtx_->stream_id) {
    XLOG(DBG4) << "onNewPeerStream: skipping control stream " << streamId;
    return;
  }

  // Instead, track this stream and notify the handler later in onStreamData.
  XLOG(DBG2) << "onNewPeerStream: " << streamId;
  parent_->pendingStreamNotifications_.insert(streamId);
}

void PicoH3WebTransport::processEgressEvents() {
  if (!cnx_) {
    return;
  }

  XLOG(DBG4) << "processEgressEvents: processing egress events";

  auto events = streamManager_->moveEvents();

  for (auto& event : events) {
    if (auto* resetStream =
            std::get_if<proxygen::detail::WtStreamManager::ResetStream>(&event)) {
      auto it = streamContexts_.find(resetStream->streamId);
      if (it != streamContexts_.end()) {
        picowt_reset_stream(cnx_, it->second, resetStream->err);
      }
    } else if (auto* stopSending =
                   std::get_if<proxygen::detail::WtStreamManager::StopSending>(&event)) {
      if (cnx_) {
        picoquic_stop_sending(cnx_, stopSending->streamId, stopSending->err);
      }
    } else if (auto* closeSession =
                   std::get_if<proxygen::detail::WtStreamManager::CloseSession>(&event)) {
      this->closeSession(closeSession->err);
    } else if (std::get_if<proxygen::detail::WtStreamManager::DrainSession>(&event)) {
      XLOG(DBG1) << "DrainSession event received";
    } else if (auto* maxConnData =
                   std::get_if<proxygen::detail::WtStreamManager::MaxConnData>(&event)) {
      XLOG(DBG2) << "Unhandled MaxConnData event, maxData=" << maxConnData->maxData;
    } else if (auto* maxStreamData =
                   std::get_if<proxygen::detail::WtStreamManager::MaxStreamData>(&event)) {
      XLOG(DBG2) << "Unhandled MaxStreamData event, streamId="
                 << maxStreamData->streamId << " maxData=" << maxStreamData->maxData;
    } else if (auto* maxStreamsBidi =
                   std::get_if<proxygen::detail::WtStreamManager::MaxStreamsBidi>(&event)) {
      XLOG(DBG2) << "Unhandled MaxStreamsBidi event, maxStreams="
                 << maxStreamsBidi->maxStreams;
    } else if (auto* maxStreamsUni =
                   std::get_if<proxygen::detail::WtStreamManager::MaxStreamsUni>(&event)) {
      XLOG(DBG2) << "Unhandled MaxStreamsUni event, maxStreams="
                 << maxStreamsUni->maxStreams;
    } else {
      XLOG(WARN) << "Unknown event type in processEgressEvents";
    }
  }

  // Process writable streams - send all available data
  while (auto* handle = streamManager_->nextWritable()) {
    uint64_t streamId = handle->getID();
    auto it = streamContexts_.find(streamId);
    if (it == streamContexts_.end()) {
      // Stream context gone, dequeue to clear the buffer
      streamManager_->dequeue(*handle, UINT64_MAX);
      continue;
    }

    // Dequeue ALL available data for this stream
    auto streamData = streamManager_->dequeue(*handle, UINT64_MAX);
    if (streamData.data || streamData.fin) {
      // Must coalesce IOBuf chain before sending - data() only returns first buffer
      if (streamData.data) {
        streamData.data->coalesce();
      }
      size_t bytesDequeued =
          streamData.data ? streamData.data->length() : 0;
      sendStreamData(
          it->second,
          streamData.data ? streamData.data->data() : nullptr,
          bytesDequeued,
          streamData.fin);
    } else {
      // No data available, break to avoid infinite loop
      break;
    }
  }
}

void PicoH3WebTransport::sendStreamData(
    h3zero_stream_ctx_t* streamCtx,
    const uint8_t* data,
    size_t length,
    bool fin) {
  XLOG(DBG4) << "sendStreamData: stream=" << streamCtx->stream_id
             << " length=" << length << " fin=" << fin;

  int ret = picoquic_add_to_stream_with_ctx(
      cnx_, streamCtx->stream_id, data, length, fin ? 1 : 0, streamCtx);
  if (ret != 0) {
    XLOG(ERR) << "Failed to send data on stream " << streamCtx->stream_id
              << ", error=" << ret;
  }
}

// Stream creation

folly::Expected<PicoH3WebTransport::StreamWriteHandle*, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::createUniStream() {
  if (!cnx_ || sessionClosed_) {
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Create WebTransport unidirectional stream via h3zero
  auto* streamCtx = picowt_create_local_stream(
      cnx_, 0 /* is_bidir */, h3Ctx_, controlStreamCtx_->stream_id);

  if (!streamCtx) {
    XLOG(ERR) << "Failed to create WebTransport unidir stream";
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  uint64_t streamId = streamCtx->stream_id;
  streamContexts_[streamId] = streamCtx;

  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    XLOG(ERR) << "WtStreamManager failed to create egress handle for stream "
              << streamId;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  XLOG(DBG2) << "Created WebTransport unidir stream: " << streamId;
  return handle;
}

folly::Expected<PicoH3WebTransport::BidiStreamHandle, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::createBidiStream() {
  if (!cnx_ || sessionClosed_) {
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Create WebTransport bidirectional stream via h3zero
  auto* streamCtx = picowt_create_local_stream(
      cnx_, 1 /* is_bidir */, h3Ctx_, controlStreamCtx_->stream_id);

  if (!streamCtx) {
    XLOG(ERR) << "Failed to create WebTransport bidir stream";
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  uint64_t streamId = streamCtx->stream_id;
  streamContexts_[streamId] = streamCtx;

  auto handle = streamManager_->getOrCreateBidiHandle(streamId);
  if (!handle.readHandle || !handle.writeHandle) {
    XLOG(ERR) << "WtStreamManager failed to create bidi handle for stream "
              << streamId;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  XLOG(DBG2) << "Created WebTransport bidir stream: " << streamId;
  return handle;
}

folly::SemiFuture<folly::Unit> PicoH3WebTransport::awaitUniStreamCredit() {
  // h3zero handles flow control internally; always return ready
  return folly::makeSemiFuture(folly::Unit());
}

folly::SemiFuture<folly::Unit> PicoH3WebTransport::awaitBidiStreamCredit() {
  // h3zero handles flow control internally; always return ready
  return folly::makeSemiFuture(folly::Unit());
}

folly::Expected<folly::SemiFuture<PicoH3WebTransport::StreamData>, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::readStreamData(uint64_t id) {
  auto* handle = streamManager_->getOrCreateIngressHandle(id);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  return handle->readStreamData();
}

folly::Expected<PicoH3WebTransport::FCState, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::writeStreamData(
    uint64_t id,
    std::unique_ptr<folly::IOBuf> data,
    bool fin,
    ByteEventCallback* deliveryCallback) {
  // Find stream context for this stream ID
  auto it = streamContexts_.find(id);
  if (it == streamContexts_.end()) {
    XLOG(ERR) << "writeStreamData: no stream context for stream " << id;
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  // Send data directly via picoquic instead of queueing
  // This ensures complete objects are sent atomically
  if (data) {
    data->coalesce();
    size_t length = data->length();
    XLOG(DBG4) << "writeStreamData: stream=" << id << " length=" << length
               << " fin=" << fin;
    int ret = picoquic_add_to_stream_with_ctx(
        cnx_, id, data->data(), data->length(), fin ? 1 : 0, it->second);
    if (ret != 0) {
      XLOG(ERR) << "picoquic_add_to_stream_with_ctx failed: " << ret;
      return folly::makeUnexpected(ErrorCode::GENERIC_ERROR);
    }

    // Note: deliveryCallback not used - picoquic handles delivery internally
    (void)deliveryCallback;
  } else if (fin) {
    // Send FIN without data
    int ret = picoquic_add_to_stream_with_ctx(
        cnx_, id, nullptr, 0, 1, it->second);
    if (ret != 0) {
      XLOG(ERR) << "picoquic_add_to_stream_with_ctx (fin) failed: " << ret;
      return folly::makeUnexpected(ErrorCode::GENERIC_ERROR);
    }
  }

  // Notify EventBase that wake time may have decreased (data is ready to send)
  if (updateWakeTimeoutCallback_) {
    updateWakeTimeoutCallback_();
  }

  return FCState::UNBLOCKED;
}

folly::Expected<folly::Unit, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::resetStream(uint64_t streamId, uint32_t error) {
  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  auto it = streamContexts_.find(streamId);
  if (it != streamContexts_.end()) {
    picowt_reset_stream(cnx_, it->second, error);
  }
  return handle->resetStream(error);
}

folly::Expected<folly::Unit, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::setPriority(
    uint64_t streamId,
    quic::PriorityQueue::Priority priority) {
  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  return handle->setPriority(priority);
}

folly::Expected<folly::Unit, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::setPriorityQueue(
    std::unique_ptr<quic::PriorityQueue> queue) noexcept {
  // Not supported; using internal priority queue
  return folly::makeUnexpected(ErrorCode::GENERIC_ERROR);
}

folly::Expected<folly::SemiFuture<uint64_t>, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::awaitWritable(uint64_t streamId) {
  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  return handle->awaitWritable();
}

folly::Expected<folly::Unit, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::stopSending(uint64_t streamId, uint32_t error) {
  auto* handle = streamManager_->getOrCreateIngressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  if (cnx_) {
    picoquic_stop_sending(cnx_, streamId, error);
  }
  return handle->stopSending(error);
}

folly::Expected<folly::Unit, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::sendDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  if (!cnx_ || sessionClosed_) {
    return folly::makeUnexpected(ErrorCode::GENERIC_ERROR);
  }

  // Queue datagram for sending
  datagramQueue_.push_back(std::move(datagram));

  // Mark datagram ready in h3zero
  h3zero_set_datagram_ready(cnx_, controlStreamCtx_->stream_id);

  return folly::Unit();
}

const folly::SocketAddress& PicoH3WebTransport::getLocalAddress() const {
  return localAddr_;
}

const folly::SocketAddress& PicoH3WebTransport::getPeerAddress() const {
  return peerAddr_;
}

quic::TransportInfo PicoH3WebTransport::getTransportInfo() const {
  quic::TransportInfo info;
  // Basic info - could be expanded with picoquic stats
  return info;
}

folly::Expected<folly::Unit, PicoH3WebTransport::ErrorCode>
PicoH3WebTransport::closeSession(folly::Optional<uint32_t> error) {
  if (sessionClosed_) {
    return folly::unit;
  }
  sessionClosed_ = true;

  uint32_t errorCode = error.value_or(0);
  XLOG(DBG1) << "Closing WebTransport session, error=" << errorCode;

  // Shutdown stream manager
  proxygen::detail::WtStreamManager::CloseSession closeSession{errorCode, ""};
  streamManager_->shutdown(closeSession);

  if (cnx_ && controlStreamCtx_) {
    picowt_send_close_session_message(
        cnx_,
        controlStreamCtx_,
        errorCode,
        nullptr);
  }

  if (auto handler = std::exchange(handler_, nullptr)) {
    handler->onSessionEnd(error);
  }

  return folly::unit;
}

} // namespace moxygen
