/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/PicoWebTransportBase.h"
#include <folly/logging/xlog.h>
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

PicoWebTransportBase::PicoWebTransportBase(
    picoquic_cnx_t* cnx,
    bool isClient,
    const folly::SocketAddress& localAddr,
    const folly::SocketAddress& peerAddr)
    : cnx_(cnx),
      localAddr_(localAddr),
      peerAddr_(peerAddr),
      isClient_(isClient),
      egressCallback_(this),
      ingressCallback_(this) {
  // Configure WtStreamManager flow control limits
  // We set all limits to max() because picoquic handles flow control internally
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
}

PicoWebTransportBase::~PicoWebTransportBase() {
  handler_ = nullptr;
  if (!sessionClosed_) {
    // Cannot call closeSession() here: it calls sendCloseImpl() which is pure
    // virtual and the derived class vtable is already gone by the time the base
    // destructor runs. Do the minimum safe cleanup directly.
    sessionClosed_ = true;
    proxygen::detail::WtStreamManager::CloseSession cs{0, ""};
    streamManager_->shutdown(cs);
  }
}

// Stream creation

folly::Expected<
    PicoWebTransportBase::StreamWriteHandle*,
    PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::createUniStream() {
  XCHECK(cnx_);
  if (sessionClosed_) {
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  auto streamIdResult = createStreamImpl(false /* bidi */);
  if (!streamIdResult) {
    return folly::makeUnexpected(streamIdResult.error());
  }
  uint64_t streamId = *streamIdResult;

  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    XLOG(ERR) << "WtStreamManager failed to create egress handle for stream "
              << streamId;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  if (statsCallback_) {
    statsCallback_->onStreamCreated();
  }
  return handle;
}

folly::Expected<
    PicoWebTransportBase::BidiStreamHandle,
    PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::createBidiStream() {
  XCHECK(cnx_);
  if (sessionClosed_) {
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  auto streamIdResult = createStreamImpl(true /* bidi */);
  if (!streamIdResult) {
    return folly::makeUnexpected(streamIdResult.error());
  }
  uint64_t streamId = *streamIdResult;

  auto handle = streamManager_->getOrCreateBidiHandle(streamId);
  if (!handle.readHandle || !handle.writeHandle) {
    XLOG(ERR) << "WtStreamManager failed to create bidi handle for stream "
              << streamId;
    return folly::makeUnexpected(ErrorCode::STREAM_CREATION_ERROR);
  }

  if (statsCallback_) {
    statsCallback_->onStreamCreated();
  }
  return handle;
}

folly::SemiFuture<folly::Unit> PicoWebTransportBase::awaitUniStreamCredit() {
  // Picoquic handles flow control internally; always return ready
  return folly::makeSemiFuture();
}

folly::SemiFuture<folly::Unit> PicoWebTransportBase::awaitBidiStreamCredit() {
  // Picoquic handles flow control internally; always return ready
  return folly::makeSemiFuture();
}

// Stream operations

folly::Expected<
    folly::SemiFuture<PicoWebTransportBase::StreamData>,
    PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::readStreamData(uint64_t id) {
  auto* handle = streamManager_->getOrCreateIngressHandle(id);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  return handle->readStreamData();
}

folly::Expected<PicoWebTransportBase::FCState, PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::writeStreamData(
    uint64_t id,
    std::unique_ptr<folly::IOBuf> data,
    bool fin,
    ByteEventCallback* deliveryCallback) {
  size_t dataLen = data ? data->computeChainDataLength() : 0;
  XLOG(DBG5) << "writeStreamData: stream=" << id << " dataLen=" << dataLen
             << " fin=" << fin;

  auto* handle = streamManager_->getOrCreateEgressHandle(id);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }

  auto result = handle->writeStreamData(std::move(data), fin, deliveryCallback);
  if (result.hasValue()) {
    WakeTimeGuard guard(cnx_, updateWakeTimeoutCallback_);
    markStreamActiveImpl(id);
  }
  return result;
}

folly::Expected<folly::Unit, PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::resetStream(uint64_t streamId, uint32_t error) {
  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  return handle->resetStream(error);
}

folly::Expected<folly::Unit, PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::setPriority(
    uint64_t streamId,
    quic::PriorityQueue::Priority priority) {
  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  return handle->setPriority(priority);
}

folly::Expected<folly::Unit, PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::setPriorityQueue(
    std::unique_ptr<quic::PriorityQueue> /*queue*/) noexcept {
  // Picoquic handles priority internally, this is a no-op
  return folly::unit;
}

folly::Expected<folly::SemiFuture<uint64_t>, PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::awaitWritable(uint64_t streamId) {
  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  return handle->awaitWritable();
}

folly::Expected<folly::Unit, PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::stopSending(uint64_t streamId, uint32_t error) {
  auto* handle = streamManager_->getOrCreateIngressHandle(streamId);
  if (!handle) {
    return folly::makeUnexpected(ErrorCode::INVALID_STREAM_ID);
  }
  return handle->stopSending(error);
}

folly::Expected<folly::Unit, PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::sendDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  XCHECK(cnx_);
  if (sessionClosed_) {
    return folly::makeUnexpected(ErrorCode::GENERIC_ERROR);
  }

  size_t datagramLen = datagram ? datagram->computeChainDataLength() : 0;
  XLOG(DBG4) << "sendDatagram: queuing " << datagramLen << " bytes, "
             << "queue_size=" << datagramQueue_.size();

  datagramQueue_.push_back(std::move(datagram));
  WakeTimeGuard guard(cnx_, updateWakeTimeoutCallback_);
  markDatagramActiveImpl();

  return folly::unit;
}

const folly::SocketAddress& PicoWebTransportBase::getLocalAddress() const {
  return localAddr_;
}

const folly::SocketAddress& PicoWebTransportBase::getPeerAddress() const {
  return peerAddr_;
}

quic::TransportInfo PicoWebTransportBase::getTransportInfo() const {
  quic::TransportInfo info;
  XCHECK(cnx_);
  info.srtt = std::chrono::microseconds(picoquic_get_rtt(cnx_));
  info.bytesSent = picoquic_get_data_sent(cnx_);
  info.bytesRecvd = picoquic_get_data_received(cnx_);
  return info;
}

folly::Expected<folly::Unit, PicoWebTransportBase::ErrorCode>
PicoWebTransportBase::closeSession(folly::Optional<uint32_t> error) {
  if (sessionClosed_) {
    return folly::unit;
  }
  sessionClosed_ = true;

  uint32_t errorCode = error.value_or(0);
  XLOG(DBG1) << "closeSession: error=" << errorCode;

  // Shutdown stream manager
  proxygen::detail::WtStreamManager::CloseSession closeSession{errorCode, ""};
  streamManager_->shutdown(closeSession);

  // Send close signal via subclass
  sendCloseImpl(errorCode);

  // Allow subclass cleanup
  onSessionClosedImpl();

  // Notify handler (use exchange to prevent re-entrancy)
  if (auto handler = std::exchange(handler_, nullptr)) {
    handler->onSessionEnd(error);
  }

  return folly::unit;
}

// WtStreamManager callbacks

void PicoWebTransportBase::EgressCallback::eventsAvailable() noexcept {
  XLOG(DBG4) << "EgressCallback::eventsAvailable()";
  parent_->processEgressEvents();
}

void PicoWebTransportBase::IngressCallback::onNewPeerStream(
    uint64_t streamId) noexcept {
  // NOTE: This callback is invoked by WtStreamManager during BidiHandle
  // construction, BEFORE the handle is inserted into the map. We must NOT
  // call getOrCreateBidiHandle or getBidiHandle here.
  //
  // Track this stream and notify the handler later in onStreamDataCommon.
  XLOG(DBG2) << "onNewPeerStream: " << streamId;
  parent_->pendingStreamNotifications_.insert(streamId);
  if (parent_->statsCallback_) {
    parent_->statsCallback_->onStreamCreated();
  }
}

// Egress event processing

void PicoWebTransportBase::processEgressEvents() {
  XCHECK(cnx_);

  XLOG(DBG4) << "processEgressEvents: processing egress events";
  WakeTimeGuard guard(cnx_, updateWakeTimeoutCallback_);

  auto events = streamManager_->moveEvents();

  for (auto& event : events) {
    if (auto* resetStream =
            std::get_if<proxygen::detail::WtStreamManager::ResetStream>(
                &event)) {
      resetStreamImpl(resetStream->streamId, resetStream->err);
    } else if (
        auto* stopSending =
            std::get_if<proxygen::detail::WtStreamManager::StopSending>(
                &event)) {
      stopSendingImpl(stopSending->streamId, stopSending->err);
    } else if (
        auto* closeSession =
            std::get_if<proxygen::detail::WtStreamManager::CloseSession>(
                &event)) {
      this->closeSession(closeSession->err);
    } else if (std::get_if<proxygen::detail::WtStreamManager::DrainSession>(
                   &event)) {
      XLOG(DBG1) << "DrainSession event received";
    } else if (
        auto* maxConnData =
            std::get_if<proxygen::detail::WtStreamManager::MaxConnData>(
                &event)) {
      XLOG(DBG1) << "Unhandled MaxConnData event, maxData="
                 << maxConnData->maxData;
    } else if (
        auto* maxStreamData =
            std::get_if<proxygen::detail::WtStreamManager::MaxStreamData>(
                &event)) {
      XLOG(DBG1) << "Unhandled MaxStreamData event, streamId="
                 << maxStreamData->streamId
                 << " maxData=" << maxStreamData->maxData;
    } else if (
        auto* maxStreamsBidi =
            std::get_if<proxygen::detail::WtStreamManager::MaxStreamsBidi>(
                &event)) {
      XLOG(DBG1) << "Unhandled MaxStreamsBidi event, maxStreams="
                 << maxStreamsBidi->maxStreams;
    } else if (
        auto* maxStreamsUni =
            std::get_if<proxygen::detail::WtStreamManager::MaxStreamsUni>(
                &event)) {
      XLOG(DBG1) << "Unhandled MaxStreamsUni event, maxStreams="
                 << maxStreamsUni->maxStreams;
    } else {
      XLOG(ERR) << "Unknown event type in processEgressEvents";
    }
  }

  // Check for writable streams in the priority queue and mark one active
  if (!priorityQueue_.empty()) {
    auto nextId = priorityQueue_.peekNextScheduledID();
    if (nextId.isStreamID()) {
      uint64_t streamId = nextId.asStreamID();
      XLOG(DBG5) << "processEgressEvents: marking writable stream " << streamId
                 << " as active";
      markStreamActiveImpl(streamId);
    }
  }
}

// JIT send path

bool PicoWebTransportBase::onJitProvideData(
    uint64_t streamId,
    uint8_t* picoContext,
    size_t maxLength) {
  auto* handle = streamManager_->getOrCreateEgressHandle(streamId);
  if (!handle) {
    XLOG(DBG2) << "onJitProvideData: no handle for stream " << streamId;
    picoquic_provide_stream_data_buffer(picoContext, 0, 0, 0);
    return false;
  }

  // Dequeue data from WtStreamManager (respects maxLength)
  auto streamData = streamManager_->dequeue(*handle, maxLength);

  size_t dataLen =
      streamData.data ? streamData.data->computeChainDataLength() : 0;
  bool fin = streamData.fin;
  // Stream is still active if we sent data and have more, or filled the buffer
  bool isStillActive = (dataLen > 0 && !fin) || (dataLen >= maxLength);

  XLOG(DBG6) << "onJitProvideData: stream=" << streamId
             << " dequeued=" << dataLen << " fin=" << fin
             << " isStillActive=" << isStillActive;

  // Get buffer from picoquic via JIT API
  uint8_t* buffer = picoquic_provide_stream_data_buffer(
      picoContext, dataLen, fin ? 1 : 0, isStillActive ? 1 : 0);

  if (buffer == nullptr) {
    if (dataLen > 0) {
      XLOG(ERR)
          << "picoquic_provide_stream_data_buffer returned null for stream "
          << streamId << " size=" << dataLen;
    }
    return false;
  }

  // Copy data into the buffer
  if (dataLen > 0 && streamData.data) {
    size_t written = 0;
    for (const auto& buf : *streamData.data) {
      size_t toWrite = std::min(buf.size(), dataLen - written);
      if (toWrite == 0) {
        break;
      }
      memcpy(buffer + written, buf.data(), toWrite);
      written += toWrite;
    }
  }

  // Fire delivery callback (optimistic, since picoquic lacks ACK callbacks)
  if (streamData.deliveryCallback) {
    streamData.deliveryCallback->onByteEvent(
        streamId, streamData.lastByteStreamOffset);
  }

  // If this stream is no longer active, check for next writable stream
  if (!isStillActive && !priorityQueue_.empty()) {
    auto nextId = priorityQueue_.peekNextScheduledID();
    if (nextId.isStreamID()) {
      uint64_t nextStreamId = nextId.asStreamID();
      XLOG(DBG5) << "onJitProvideData: marking next writable stream "
                 << nextStreamId << " as active after stream " << streamId;
      markStreamActiveImpl(nextStreamId);
    }
  }
  return fin;
}

// Ingress helpers

void PicoWebTransportBase::onStreamDataCommon(
    uint64_t streamId,
    uint8_t* bytes,
    size_t length,
    bool fin) {
  XLOG(DBG5) << "onStreamDataCommon: stream=" << streamId
             << " length=" << length << " fin=" << fin;
  if (fin && statsCallback_) {
    statsCallback_->onStreamClosed();
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

  proxygen::detail::WtStreamManager::StreamData streamData{
      std::move(data), fin};
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
      auto bidiHandle = streamManager_->getOrCreateBidiHandle(streamId);
      if (bidiHandle.readHandle && bidiHandle.writeHandle) {
        XLOG(DBG4) << "Notifying handler of new peer bidi stream " << streamId;
        handler_->onNewBidiStream(bidiHandle);
      }
    } else {
      XLOG(DBG4) << "Notifying handler of new peer uni stream " << streamId;
      handler_->onNewUniStream(readHandle);
    }
  }
}

void PicoWebTransportBase::onStreamResetCommon(
    uint64_t streamId,
    uint64_t errorCode) {
  XLOG(DBG2) << "onStreamResetCommon: stream=" << streamId
             << " error=" << errorCode;
  if (statsCallback_) {
    statsCallback_->onStreamReset();
  }
  proxygen::detail::WtStreamManager::ResetStream reset{streamId, errorCode, 0};
  streamManager_->onResetStream(reset);
}

void PicoWebTransportBase::onStopSendingCommon(
    uint64_t streamId,
    uint64_t errorCode) {
  XLOG(DBG2) << "onStopSendingCommon: stream=" << streamId
             << " error=" << errorCode;
  proxygen::detail::WtStreamManager::StopSending stopSending{
      streamId, errorCode};
  streamManager_->onStopSending(stopSending);
}

void PicoWebTransportBase::onSessionCloseCommon(uint32_t errorCode) {
  XLOG(DBG1) << "onSessionCloseCommon: error=" << errorCode;
  if (sessionClosed_) {
    return;
  }
  sessionClosed_ = true;

  // Shutdown stream manager directly (not via closeSession which would
  // send a close signal back to an already-closing connection)
  proxygen::detail::WtStreamManager::CloseSession closeSession{errorCode, ""};
  streamManager_->onCloseSession(closeSession);

  onSessionClosedImpl();

  if (auto handler = std::exchange(handler_, nullptr)) {
    handler->onSessionEnd(errorCode);
  }
}

size_t PicoWebTransportBase::getMaxDatagramPayload() const {
  XCHECK(cnx_);
  auto* tp = picoquic_get_transport_parameters(cnx_, 0 /* peer */);
  return tp ? static_cast<size_t>(tp->max_datagram_frame_size) : 0;
}

void PicoWebTransportBase::onJitProvideDatagram(
    uint8_t* context,
    size_t maxLength) {
  size_t peerMax = getMaxDatagramPayload();
  if (peerMax == 0) {
    // Peer advertised max_datagram_frame_size=0, meaning it does not support
    // the DATAGRAM extension (RFC 9221). Drop everything and stop polling.
    XLOG(WARN) << "onJitProvideDatagram: peer does not support datagrams, "
               << "dropping " << datagramQueue_.size() << " queued datagrams";
    datagramQueue_.clear();
    getDatagramBuffer(context, 0, /*keepPolling=*/false);
    return;
  }

  while (!datagramQueue_.empty()) {
    auto& dg = datagramQueue_.front();
    size_t dgLen = dg->computeChainDataLength();

    if (dgLen > peerMax) {
      // Datagram exceeds peer's hard limit — will never fit, drop it.
      XLOG(WARN) << "onJitProvideDatagram: dropping datagram (" << dgLen
                 << " > peer max " << peerMax << ")";
      datagramQueue_.pop_front();
      continue;
    }

    if (dgLen > maxLength) {
      // Datagram doesn't fit in this packet — defer to next packet.
      // This fixes the stuck-datagram bug where passing ready_to_send=0
      // (because queue.size()==1) would stop polling permanently.
      XLOG(DBG3) << "onJitProvideDatagram: datagram too large (" << dgLen
                 << " > " << maxLength << "), deferring";
      getDatagramBuffer(context, 0, /*keepPolling=*/true);
      return;
    }

    bool moreToSend = datagramQueue_.size() > 1;
    uint8_t* buffer = getDatagramBuffer(context, dgLen, moreToSend);
    if (!buffer) {
      XLOG(WARN) << "onJitProvideDatagram: getDatagramBuffer returned null "
                 << "for length=" << dgLen;
      return;
    }

    // Copy IOBuf chain into buffer without coalescing
    size_t offset = 0;
    for (const auto& buf : *dg) {
      memcpy(buffer + offset, buf.data(), buf.size());
      offset += buf.size();
    }
    datagramQueue_.pop_front();
    return;
  }

  // Queue empty — stop polling
  getDatagramBuffer(context, 0, /*keepPolling=*/false);
}

void PicoWebTransportBase::onReceiveDatagramCommon(
    uint8_t* bytes,
    size_t length) {
  XLOG(DBG4) << "onReceiveDatagramCommon: length=" << length;
  if (handler_ && bytes && length > 0) {
    auto buf = folly::IOBuf::copyBuffer(bytes, length);
    handler_->onDatagram(std::move(buf));
  }
}

} // namespace moxygen
