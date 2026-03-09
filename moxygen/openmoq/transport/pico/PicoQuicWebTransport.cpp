/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/PicoQuicWebTransport.h"
#include <folly/logging/xlog.h>
#include <glog/logging.h>
#include <picoquic.h>

namespace moxygen {

namespace {

// RAII guard: captures picoquic's next wake time on construction and fires
// a callback on destruction if the wake time decreased (e.g. after
// mark_active_stream / mark_datagram_ready).
struct WakeTimeGuard {
  WakeTimeGuard(picoquic_cnx_t* cnx, const std::function<void()>& cb)
      : quic_(picoquic_get_quic_ctx(cnx)), cb_(cb) {
    if (cb_) {
      before_ = picoquic_get_next_wake_time(quic_, picoquic_current_time());
    }
  }
  ~WakeTimeGuard() {
    if (cb_ &&
        picoquic_get_next_wake_time(quic_, picoquic_current_time()) < before_) {
      cb_();
    }
  }

 private:
  picoquic_quic_t* quic_;
  const std::function<void()>& cb_;
  uint64_t before_{UINT64_MAX};
};

} // namespace

// File-local picoquic callback that delegates to the PicoQuicWebTransport
// instance. Used as the function pointer passed to picoquic_set_callback().
static int picoCallback(picoquic_cnx_t *cnx, uint64_t stream_id,
                        uint8_t *bytes, size_t length,
                        picoquic_call_back_event_t fin_or_event,
                        void *callback_ctx, void *v_stream_ctx) {
  auto *self = static_cast<PicoQuicWebTransport *>(callback_ctx);
  if (!self) {
    return -1;
  }
  return self->handlePicoEvent(
      cnx, stream_id, bytes, length, static_cast<int>(fin_or_event),
      v_stream_ctx);
}

PicoQuicWebTransport::PicoQuicWebTransport(
    picoquic_cnx_t *cnx, const folly::SocketAddress &localAddr,
    const folly::SocketAddress &peerAddr)
    : cnx_(cnx), localAddr_(localAddr), peerAddr_(peerAddr),
      egressCallback_(this), ingressCallback_(this) {

  // Determine if this is a client or server connection
  isClient_ = picoquic_is_client(cnx_);

  // Configure WtStreamManager flow control limits
  //
  // We set all limits to max() because picoquic handles flow control
  // internally and does NOT expose MAX_STREAMS/MAX_DATA frame reception
  // to applications via callbacks. This means:
  //
  // - WtStreamManager cannot track actual peer limits
  // - Flow control is enforced by picoquic at send time
  // - Streams become "blocked" when exceeding peer's limits
  // - Picoquic automatically sends STREAMS_BLOCKED/DATA_BLOCKED frames
  //
  // See awaitUniStreamCredit() / awaitBidiStreamCredit() for more details
  // on the implications of this design.
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

  auto dir = isClient_ ? proxygen::detail::WtDir::Client
                       : proxygen::detail::WtDir::Server;
  streamManager_ = std::make_unique<proxygen::detail::WtStreamManager>(
      dir, wtConfig, egressCallback_, ingressCallback_, priorityQueue_);

  // Set the callback context for picoquic
  picoquic_set_callback(cnx_, moxygen::picoCallback, this);
}

PicoQuicWebTransport::~PicoQuicWebTransport() {
  // Clear handler first to prevent any callbacks during destruction
  handler_ = nullptr;
  if (!sessionClosed_) {
    closeSession(folly::none);
  }
  // Ensure callback is cleared even if closeSession wasn't called
  clearPicoquicCallback();
}

folly::Expected<PicoQuicWebTransport::StreamWriteHandle *,
                PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::createUniStream() {
  if (!cnx_) {
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Get next local stream ID from picoquic
  uint64_t streamId =
      picoquic_get_next_local_stream_id(cnx_, 1 /* is_unidir */);

  // Reserve the stream ID in picoquic by setting app stream context
  // This ensures picoquic knows about the stream and will increment
  // its internal stream ID counter
  int ret = picoquic_set_app_stream_ctx(cnx_, streamId, nullptr);
  if (ret != 0) {
    XLOG(ERR) << "Failed to reserve stream ID " << streamId
              << " in picoquic, error=" << ret;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Create handle in WtStreamManager with the specific stream ID
  auto *handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    XLOG(ERR) << "WtStreamManager failed to create egress handle "
              << "for stream " << streamId;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Stream will be marked active when data is written via
  // writeStreamData()
  return handle;
}

folly::Expected<PicoQuicWebTransport::BidiStreamHandle,
                PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::createBidiStream() {
  if (!cnx_) {
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Get next local stream ID from picoquic
  uint64_t streamId =
      picoquic_get_next_local_stream_id(cnx_, 0 /* is_unidir */);

  // Reserve the stream ID in picoquic by setting app stream context
  // This ensures picoquic knows about the stream and will increment
  // its internal stream ID counter
  int ret = picoquic_set_app_stream_ctx(cnx_, streamId, nullptr);
  if (ret != 0) {
    XLOG(ERR) << "Failed to reserve stream ID " << streamId
              << " in picoquic, error=" << ret;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Create handle in WtStreamManager with the specific stream ID
  auto handle = streamManager_->getOrCreateBidiHandle(streamId);
  if (!handle.readHandle || !handle.writeHandle) {
    XLOG(ERR) << "WtStreamManager failed to create bidi handle "
              << "for stream " << streamId;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  // Stream will be marked active when data is written via
  // writeStreamData()
  return handle;
}

folly::SemiFuture<folly::Unit> PicoQuicWebTransport::awaitUniStreamCredit() {
  // NOTE: Picoquic does NOT provide callbacks for MAX_STREAMS frames!
  //
  // Picoquic's design philosophy is to handle flow control internally
  // and provide "lazy" backpressure:
  // - Applications create streams optimistically using
  //   picoquic_get_next_local_stream_id()
  // - Streams get blocked at *send time* if they exceed peer's limit
  // - Picoquic sends STREAMS_BLOCKED frames to notify the peer
  // - When peer sends MAX_STREAMS, picoquic updates internal state
  //   but does NOT notify the application
  //
  // This means we cannot properly implement stream credit tracking:
  // - We set all WtStreamManager flow control limits to max()
  // - WtStreamManager always thinks we have infinite stream credit
  // - We can't wait for credit because we don't know when it arrives
  //
  // As a result, this method always returns ready. Applications
  // will discover stream blocking when writes stop making progress.
  //
  // Possible future improvements:
  // - Add picoquic API to query max_stream_id_bidir_remote/unidir_remote
  // - Add picoquic callback for MAX_STREAMS frame reception
  // - Hook into picoquic's frame decoder (invasive)
  return folly::makeSemiFuture();
}

folly::SemiFuture<folly::Unit> PicoQuicWebTransport::awaitBidiStreamCredit() {
  // NOTE: See awaitUniStreamCredit() for explanation of why this
  // always returns ready. Picoquic does not expose MAX_STREAMS
  // frame reception to applications.
  return folly::makeSemiFuture();
}

folly::Expected<folly::SemiFuture<PicoQuicWebTransport::StreamData>,
                PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::readStreamData(uint64_t id) {
  // Delegate to stream manager
  auto *handle = streamManager_->getOrCreateIngressHandle(id);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  return handle->readStreamData();
}

folly::Expected<PicoQuicWebTransport::FCState, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::writeStreamData(uint64_t id,
                                      std::unique_ptr<folly::IOBuf> data,
                                      bool fin,
                                      ByteEventCallback *deliveryCallback) {

  size_t dataLen = data ? data->computeChainDataLength() : 0;
  XLOG(DBG4) << "writeStreamData: stream=" << id << " dataLen=" << dataLen
             << " fin=" << fin;

  auto *handle = streamManager_->getOrCreateEgressHandle(id);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  auto result = handle->writeStreamData(std::move(data), fin, deliveryCallback);

  if (result.hasValue()) {
    // Mark stream as active for JIT sending
    markStreamActive(id);
  }

  return result;
}

folly::Expected<folly::Unit, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::resetStream(uint64_t streamId, uint32_t error) {
  auto *handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  return handle->resetStream(error);
}

folly::Expected<folly::Unit, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::setPriority(uint64_t streamId,
                                  quic::PriorityQueue::Priority priority) {

  auto *handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  return handle->setPriority(priority);
}

folly::Expected<folly::Unit, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::setPriorityQueue(
    std::unique_ptr<quic::PriorityQueue> /*queue*/) noexcept {
  // PicoQuic handles priority internally, so this is a no-op
  return folly::unit;
}

folly::Expected<folly::SemiFuture<uint64_t>, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::awaitWritable(uint64_t streamId) {
  auto *handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  return handle->awaitWritable();
}

folly::Expected<folly::Unit, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::stopSending(uint64_t streamId, uint32_t error) {
  auto *handle = streamManager_->getOrCreateIngressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  return handle->stopSending(error);
}

folly::Expected<folly::Unit, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::sendDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  size_t datagramLen = datagram ? datagram->computeChainDataLength() : 0;
  XLOG(DBG4) << "sendDatagram: queuing " << datagramLen << " bytes, "
             << "queue_size=" << datagramQueue_.size();

  // Queue datagram for JIT sending
  datagramQueue_.push_back(std::move(datagram));

  // Mark datagram as active
  markDatagramActive();

  return folly::unit;
}

const folly::SocketAddress &PicoQuicWebTransport::getLocalAddress() const {
  return localAddr_;
}

const folly::SocketAddress &PicoQuicWebTransport::getPeerAddress() const {
  return peerAddr_;
}

quic::TransportInfo PicoQuicWebTransport::getTransportInfo() const {
  quic::TransportInfo info;

  if (!cnx_) {
    return info;
  }

  // Populate transport info from picoquic connection state
  info.srtt = std::chrono::microseconds(picoquic_get_rtt(cnx_));
  info.bytesSent = picoquic_get_data_sent(cnx_);
  info.bytesRecvd = picoquic_get_data_received(cnx_);

  return info;
}

folly::Expected<folly::Unit, PicoQuicWebTransport::ErrorCode>
PicoQuicWebTransport::closeSession(folly::Optional<uint32_t> error) {
  if (sessionClosed_) {
    return folly::unit;
  }

  sessionClosed_ = true;

  uint32_t errorCode = error.value_or(0);
  proxygen::detail::WtStreamManager::CloseSession closeSession{errorCode,
                                                                     ""};
  streamManager_->shutdown(closeSession);

  if (cnx_) {
    int ret = picoquic_close(cnx_, errorCode);
    if (ret != 0) {
      XLOG(WARN) << "picoquic_close failed with error=" << ret;
    }
  }

  // Do NOT clear the picoquic callback here — let the 3xPTO drain complete.
  // picoquic_callback_close will fire and route to onConnectionClose() which
  // clears it. handler_ is nulled now to prevent a double onSessionEnd.

  // Use std::exchange to prevent re-entrancy issues
  // The handler callback may destroy this object
  if (auto handler = std::exchange(handler_, nullptr)) {
    handler->onSessionEnd(error);
  }

  return folly::unit;
}

// WtStreamManager callbacks
void PicoQuicWebTransport::EgressCallback::eventsAvailable() noexcept {
  XLOG(DBG4) << "EgressCallback::eventsAvailable() called";
  parent_->processEgressEvents();
}

void PicoQuicWebTransport::IngressCallback::onNewPeerStream(
    uint64_t streamId) noexcept {
  // NOTE: This callback is invoked by WtStreamManager during BidiHandle
  // construction, BEFORE the handle is inserted into the map. We must NOT
  // call getOrCreateBidiHandle or getBidiHandle here as the handle doesn't
  // exist yet and would cause infinite recursion or return null.
  //
  // Instead, track this stream and notify the handler later in onStreamData
  // after the handle is fully created.
  parent_->pendingStreamNotifications_.insert(streamId);
}

int PicoQuicWebTransport::handlePicoEvent(picoquic_cnx_t *cnx,
                                          uint64_t stream_id, uint8_t *bytes,
                                          size_t length, int fin_or_event_int,
                                          void *v_stream_ctx) {
  auto fin_or_event =
      static_cast<picoquic_call_back_event_t>(fin_or_event_int);

  XLOG(DBG6) << "picoCallback: event=" << fin_or_event
             << " stream_id=" << stream_id << " length=" << length;

  switch (fin_or_event) {
  case picoquic_callback_stream_data:
  case picoquic_callback_stream_fin:
    onStreamData(stream_id, bytes, length,
                 fin_or_event == picoquic_callback_stream_fin);
    break;

  case picoquic_callback_datagram:
    // Receive datagram from peer
    XLOG(DBG4) << "picoCallback: datagram received, length=" << length;
    onReceiveDatagram(bytes, length);
    break;

  case picoquic_callback_stream_reset:
    // length contains error code
    onStreamReset(stream_id, length);
    break;

  case picoquic_callback_stop_sending:
    // length contains error code
    onStopSending(stream_id, length);
    break;

  case picoquic_callback_close:
  case picoquic_callback_application_close:
    onConnectionClose(length);
    break;

  case picoquic_callback_prepare_to_send: {
    // JIT callback - picoquic is ready to send data on this stream
    size_t written = 0;
    bool fin = false;
    onPrepareToSend(stream_id, bytes, length, written, fin);
    // onPrepareToSend calls picoquic_provide_stream_data_buffer internally
    break;
  }

  case picoquic_callback_prepare_datagram: {
    // JIT callback - picoquic is ready to send a datagram
    size_t written = 0;
    onPrepareDatagram(bytes, length, written);
    XLOG(DBG4) << "picoCallback: prepare_datagram, max_length=" << length
               << " written=" << written;
    // picoquic handles datagram buffer
    break;
  }

  case picoquic_callback_stream_gap:
    XLOG(DBG2) << "Stream gap on stream " << stream_id;
    break;

  default:
    // Other events we don't handle for now
    break;
  }

  return 0;
}

void PicoQuicWebTransport::onPrepareToSend(uint64_t streamId, uint8_t *context,
                                           size_t maxLength, size_t &written,
                                           bool &fin) {

  written = 0;
  fin = false;

  if (!cnx_) {
    // Connection closed, no data to send
    XLOG(ERR) << "onPrepareToSend: cnx_ is null for stream " << streamId;
    return;
  }

  // Get the write handle from stream manager
  auto *handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    // Stream doesn't exist, mark as inactive
    int ret = picoquic_mark_active_stream(cnx_, streamId, 0, nullptr);
    if (ret != 0) {
      XLOG(WARN) << "Failed to mark stream " << streamId
                 << " as inactive, error=" << ret;
    }
    return;
  }

  // Dequeue data from WtStreamManager
  auto streamData = streamManager_->dequeue(*handle, maxLength);

  // Calculate how much data we have to send
  // dequeue() already respects maxLength, so returned data is never > maxLength
  size_t dataSize = 0;
  if (streamData.data) {
    dataSize = streamData.data->computeChainDataLength();
  }

  XLOG(DBG6) << "onPrepareToSend: stream=" << streamId << " max=" << maxLength
             << " dequeued=" << dataSize << " fin=" << streamData.fin;

  // WtStreamManager sets fin=true only when returning the final chunk
  fin = streamData.fin;

  // Stream is still active if we have data or will have more data
  // If we dequeued 0 bytes and no fin, stream has no data currently
  bool is_still_active = (dataSize > 0) || fin;

  // Get buffer from picoquic to write into
  uint8_t *buffer = picoquic_provide_stream_data_buffer(context, dataSize, fin,
                                                        is_still_active);

  if (buffer == nullptr) {
    if (dataSize > 0) {
      XLOG(ERR) << "picoquic_provide_stream_data_buffer returned null "
                << "for stream " << streamId << " size=" << dataSize;
    }
    return;
  }

  // Copy data into the buffer provided by picoquic
  if (dataSize > 0 && streamData.data) {
    written = 0;
    for (auto &buf : *streamData.data) {
      size_t toWrite = std::min(buf.size(), dataSize - written);
      if (toWrite == 0) {
        break;
      }
      memcpy(buffer + written, buf.data(), toWrite);
      written += toWrite;
    }
  }

  // Mark stream inactive if we sent fin or dequeued less than requested
  // (which means no more buffered data available)
  if (fin || dataSize < maxLength) {
    int ret = picoquic_mark_active_stream(cnx_, streamId, 0, nullptr);
    if (ret != 0) {
      XLOG(WARN) << "Failed to mark stream " << streamId
                 << " as inactive, error=" << ret;
    }

    // Check if there's another writable stream in the priority queue
    if (!priorityQueue_.empty()) {
      auto nextId = priorityQueue_.peekNextScheduledID();
      if (nextId.isStreamID()) {
        uint64_t nextStreamId = nextId.asStreamID();
        XLOG(DBG4) << "onPrepareToSend: marking next writable stream "
                   << nextStreamId << " as active after stream " << streamId
                   << " became inactive";
        markStreamActive(nextStreamId);
      }
    }
  }
}

void PicoQuicWebTransport::onPrepareDatagram(uint8_t *context, size_t maxLength,
                                             size_t &written) {

  written = 0;

  if (!cnx_) {
    // Connection closed, no datagrams to send
    XLOG(WARN) << "onPrepareDatagram: cnx_ is null";
    return;
  }

  if (datagramQueue_.empty()) {
    // No datagrams to send, mark as not ready
    int ret = picoquic_mark_datagram_ready(cnx_, 0);
    if (ret != 0) {
      XLOG(WARN) << "Failed to mark datagram as not ready, error=" << ret;
    }
    return;
  }

  // Get next datagram from queue
  auto &datagram = datagramQueue_.front();

  // Check if entire datagram fits - datagrams are atomic
  size_t datagramLen = datagram->computeChainDataLength();

  XLOG(DBG4) << "onPrepareDatagram: max=" << maxLength
             << " datagram_len=" << datagramLen
             << " queue_size=" << datagramQueue_.size();

  if (datagramLen > maxLength) {
    // Datagram too large, can't send it
    // (picoquic will try again with larger buffer)
    XLOG(DBG2) << "onPrepareDatagram: datagram too large (" << datagramLen
               << " > " << maxLength << "), skipping";
    return;
  }

  // Get the actual datagram buffer from picoquic
  // This encodes the datagram frame header and returns where to write
  uint8_t *buffer = picoquic_provide_datagram_buffer(context, datagramLen);
  if (buffer == nullptr) {
    XLOG(WARN) << "picoquic_provide_datagram_buffer returned null "
               << "for length=" << datagramLen;
    return;
  }

  // Copy entire datagram to buffer
  size_t copied = 0;
  for (const auto &buf : *datagram) {
    memcpy(buffer + copied, buf.data(), buf.size());
    copied += buf.size();
  }

  written = copied;
  datagramQueue_.pop_front();

  XLOG(DBG4) << "onPrepareDatagram: sent " << written << " bytes, "
             << "queue_size=" << datagramQueue_.size();

  // If queue is now empty, mark as not ready
  if (datagramQueue_.empty()) {
    int ret = picoquic_mark_datagram_ready(cnx_, 0);
    if (ret != 0) {
      XLOG(WARN) << "Failed to mark datagram as not ready, error=" << ret;
    }
  }
}

void PicoQuicWebTransport::onReceiveDatagram(uint8_t *bytes, size_t length) {

  if (!handler_) {
    XLOG(DBG2) << "onReceiveDatagram: no handler set, dropping " << length
               << " bytes";
    return;
  }

  if (!bytes || length == 0) {
    XLOG(DBG2) << "onReceiveDatagram: empty datagram";
    return;
  }

  XLOG(DBG4) << "onReceiveDatagram: received " << length << " bytes";

  // Copy datagram data into IOBuf
  auto datagram = folly::IOBuf::copyBuffer(bytes, length);

  // Deliver to handler
  handler_->onDatagram(std::move(datagram));
}

void PicoQuicWebTransport::onStreamData(uint64_t stream_id, uint8_t *bytes,
                                        size_t length, bool fin) {
  auto *readHandle = streamManager_->getOrCreateIngressHandle(stream_id);
  if (!readHandle) {
    XLOG(ERR) << "Failed to get/create ingress handle for stream " << stream_id;
    return;
  }

  // Create IOBuf from the data
  std::unique_ptr<folly::IOBuf> data;
  if (bytes && length > 0) {
    data = folly::IOBuf::copyBuffer(bytes, length);
  }

  proxygen::detail::WtStreamManager::StreamData streamData{
      std::move(data), fin};
  auto result = streamManager_->enqueue(*readHandle, std::move(streamData));

  if (result == proxygen::detail::WtStreamManager::Result::Fail) {
    XLOG(ERR) << "Failed to enqueue data for stream " << stream_id;
    return;
  }

  // Check if we need to notify handler about this new peer stream
  auto it = pendingStreamNotifications_.find(stream_id);
  if (it != pendingStreamNotifications_.end()) {
    pendingStreamNotifications_.erase(it);

    // Check handler_ is still valid before calling
    // (may have been cleared during session teardown)
    if (!handler_) {
      return;
    }

    // Determine if this is a bidi or uni stream
    bool isBidi = PICOQUIC_IS_BIDIR_STREAM_ID(stream_id);

    if (isBidi) {
      // Use getBidiHandle (not getOrCreate) - handle already exists
      auto bidiHandle = streamManager_->getBidiHandle(stream_id);
      if (bidiHandle.readHandle && bidiHandle.writeHandle) {
        handler_->onNewBidiStream(bidiHandle);
      }
    } else {
      // Uni stream - we already have the readHandle
      handler_->onNewUniStream(readHandle);
    }
  }
}

void PicoQuicWebTransport::onStreamReset(uint64_t stream_id,
                                         uint64_t error_code) {
  proxygen::detail::WtStreamManager::ResetStream reset{stream_id,
                                                             error_code, 0};
  streamManager_->onResetStream(reset);
}

void PicoQuicWebTransport::onStopSending(uint64_t stream_id,
                                         uint64_t error_code) {
  proxygen::detail::WtStreamManager::StopSending stopSending{stream_id,
                                                                   error_code};
  streamManager_->onStopSending(stopSending);
}

void PicoQuicWebTransport::onConnectionClose(uint64_t error_code) {
  sessionClosed_ = true;

  // Close stream manager BEFORE calling handler, as the handler may
  // destroy this object
  proxygen::detail::WtStreamManager::CloseSession closeSession{error_code,
                                                                     ""};
  streamManager_->onCloseSession(closeSession);

  // Clear picoquic callback to prevent further events
  clearPicoquicCallback();

  // Signal the owner (if set) to stop shared I/O after this drain cycle.
  if (auto cb = std::exchange(onConnectionClosedCallback_, nullptr)) {
    cb();
  }

  // Use std::exchange to prevent re-entrancy issues
  // The handler callback may destroy this object
  if (auto handler = std::exchange(handler_, nullptr)) {
    handler->onSessionEnd(static_cast<uint32_t>(error_code));
  }
}

void PicoQuicWebTransport::processEgressEvents() {
  if (!cnx_) {
    // Connection already closed, ignore egress events
    XLOG(WARN) << "processEgressEvents: cnx_ is null, ignoring events";
    return;
  }

  XLOG(DBG4) << "processEgressEvents: processing egress events";

  auto events = streamManager_->moveEvents();

  for (auto &event : events) {
    if (auto *resetStream =
            std::get_if<proxygen::detail::WtStreamManager::ResetStream>(
                &event)) {
      int ret =
          picoquic_reset_stream(cnx_, resetStream->streamId, resetStream->err);
      if (ret != 0) {
        XLOG(WARN) << "Failed to reset stream " << resetStream->streamId
                   << " error=" << resetStream->err << " picoquic_ret=" << ret;
      }
    } else if (auto *stopSending = std::get_if<
                   proxygen::detail::WtStreamManager::StopSending>(
                   &event)) {
      int ret =
          picoquic_stop_sending(cnx_, stopSending->streamId, stopSending->err);
      if (ret != 0) {
        XLOG(WARN) << "Failed to stop sending on stream "
                   << stopSending->streamId << " error=" << stopSending->err
                   << " picoquic_ret=" << ret;
      }
    } else if (auto *closeSession = std::get_if<
                   proxygen::detail::WtStreamManager::CloseSession>(
                   &event)) {
      this->closeSession(closeSession->err);
    } else if (std::get_if<
                   proxygen::detail::WtStreamManager::DrainSession>(
                   &event)) {
      XLOG(DBG1) << "DrainSession event received";
    } else if (auto *maxConnData = std::get_if<
                   proxygen::detail::WtStreamManager::MaxConnData>(
                   &event)) {
      XLOG(DBG1) << "Unhandled MaxConnData event, maxData="
                 << maxConnData->maxData;
    } else if (auto *maxStreamData = std::get_if<
                   proxygen::detail::WtStreamManager::MaxStreamData>(
                   &event)) {
      XLOG(DBG1) << "Unhandled MaxStreamData event, streamId="
                 << maxStreamData->streamId
                 << " maxData=" << maxStreamData->maxData;
    } else if (auto *maxStreamsBidi = std::get_if<
                   proxygen::detail::WtStreamManager::MaxStreamsBidi>(
                   &event)) {
      XLOG(DBG1) << "Unhandled MaxStreamsBidi event, maxStreams="
                 << maxStreamsBidi->maxStreams;
    } else if (auto *maxStreamsUni = std::get_if<
                   proxygen::detail::WtStreamManager::MaxStreamsUni>(
                   &event)) {
      XLOG(DBG1) << "Unhandled MaxStreamsUni event, maxStreams="
                 << maxStreamsUni->maxStreams;
    } else {
      XLOG(ERR) << "Unknown event type in processEgressEvents";
    }
  }

  // Check for writable streams in the priority queue
  if (!priorityQueue_.empty()) {
    auto nextId = priorityQueue_.peekNextScheduledID();
    if (nextId.isStreamID()) {
      uint64_t streamId = nextId.asStreamID();
      XLOG(DBG4) << "processEgressEvents: marking writable stream "
                 << streamId << " as active";
      markStreamActive(streamId);
    }
  } else {
    XLOG(DBG6) << "processEgressEvents: no writable streams found";
  }
}

void PicoQuicWebTransport::markStreamActive(uint64_t streamId) {
  if (!cnx_) {
    XLOG(ERR) << "markStreamActive: cnx_ is null for stream " << streamId;
    return;
  }
  WakeTimeGuard guard(cnx_, updateWakeTimeoutCallback_);
  // Mark stream as active in picoquic
  // This will cause picoquic to call prepare_to_send when ready
  // Stream context is nullptr - we use callback context instead
  int ret = picoquic_mark_active_stream(cnx_, streamId, 1, nullptr);
  if (ret != 0) {
    XLOG(WARN) << "Failed to mark stream " << streamId
               << " as active, error=" << ret;
  }
}

void PicoQuicWebTransport::markDatagramActive() {
  if (!cnx_) {
    XLOG(WARN) << "markDatagramActive: cnx_ is null";
    return;
  }
  XLOG(DBG4) << "markDatagramActive: marking datagram as ready";
  WakeTimeGuard guard(cnx_, updateWakeTimeoutCallback_);
  // Mark datagram as ready in picoquic
  // This will cause picoquic to call prepare_datagram when ready
  int ret = picoquic_mark_datagram_ready(cnx_, 1);
  if (ret != 0) {
    XLOG(WARN) << "Failed to mark datagram as ready, error=" << ret;
  }
}

void PicoQuicWebTransport::clearPicoquicCallback() {
  if (cnx_) {
    // Clear the callback to prevent further callbacks to this object
    // This prevents use-after-free if picoquic tries to deliver events
    // after the transport is destroyed or being torn down
    picoquic_set_callback(cnx_, nullptr, nullptr);
    cnx_ = nullptr;
  }
}

} // namespace moxygen
