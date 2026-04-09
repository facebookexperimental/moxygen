/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/SocketAddress.h>
#include <folly/container/F14Set.h>
#include <moxygen/openmoq/transport/pico/PicoQuicStatsCallback.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <proxygen/lib/http/webtransport/WtStreamManager.h>
#include <quic/priority/HTTPPriorityQueue.h>
#include <deque>
#include <functional>
#include <memory>

// Forward declaration - avoids including picoquic.h
typedef struct st_picoquic_cnx_t picoquic_cnx_t;

namespace moxygen {

/**
 * PicoWebTransportBase - Shared base class for picoquic WebTransport adapters.
 *
 * This base class implements the proxygen::WebTransport interface and manages:
 * - WtStreamManager for stream multiplexing and flow control
 * - Egress event processing (reset, stop-sending, close)
 * - JIT (just-in-time) send path shared logic
 * - Ingress data delivery and new stream notification
 * - Priority queue scheduling
 *
 * Subclasses implement transport-specific primitives:
 * - PicoQuicWebTransport: QUIC transport (picoquic callbacks)
 * - PicoH3WebTransport: HTTP/3 WebTransport (h3zero callbacks)
 */
class PicoWebTransportBase : public proxygen::WebTransport {
 public:
  PicoWebTransportBase(
      picoquic_cnx_t* cnx,
      bool isClient,
      const folly::SocketAddress& localAddr,
      const folly::SocketAddress& peerAddr);

  ~PicoWebTransportBase() override;

  /**
   * Set the handler for incoming WebTransport events.
   */
  void setHandler(proxygen::WebTransportHandler* handler) {
    handler_ = handler;
  }

  /**
   * Set a callback invoked when picoquic's wake time may have decreased
   * (e.g. after adding data to a stream). The callback should cancel and
   * reschedule the wake timer so sends are not delayed.
   */
  void setUpdateWakeTimeoutCallback(std::function<void()> cb) {
    updateWakeTimeoutCallback_ = std::move(cb);
  }

  /**
   * Set the stats callback for transport-level events. Called directly when
   * a locally-initiated stream is created (createUniStream / createBidiStream).
   * Non-owning: the callback object must outlive this WebTransport instance.
   */
  void setStatsCallback(PicoQuicStatsCallback* cb) {
    statsCallback_ = cb;
  }

  // proxygen::WebTransport interface - implemented in base class
  folly::Expected<StreamWriteHandle*, ErrorCode> createUniStream() override;
  folly::Expected<BidiStreamHandle, ErrorCode> createBidiStream() override;
  folly::SemiFuture<folly::Unit> awaitUniStreamCredit() override;
  folly::SemiFuture<folly::Unit> awaitBidiStreamCredit() override;

  folly::Expected<folly::SemiFuture<StreamData>, ErrorCode> readStreamData(
      uint64_t id) override;

  folly::Expected<FCState, ErrorCode> writeStreamData(
      uint64_t id,
      std::unique_ptr<folly::IOBuf> data,
      bool fin,
      ByteEventCallback* deliveryCallback) override;

  folly::Expected<folly::Unit, ErrorCode> resetStream(
      uint64_t streamId,
      uint32_t error) override;

  folly::Expected<folly::Unit, ErrorCode> setPriority(
      uint64_t streamId,
      quic::PriorityQueue::Priority priority) override;

  folly::Expected<folly::Unit, ErrorCode> setPriorityQueue(
      std::unique_ptr<quic::PriorityQueue> queue) noexcept override;

  folly::Expected<folly::SemiFuture<uint64_t>, ErrorCode> awaitWritable(
      uint64_t streamId) override;

  folly::Expected<folly::Unit, ErrorCode> stopSending(
      uint64_t streamId,
      uint32_t error) override;

  folly::Expected<folly::Unit, ErrorCode> sendDatagram(
      std::unique_ptr<folly::IOBuf> datagram) override;

  const folly::SocketAddress& getLocalAddress() const override;
  const folly::SocketAddress& getPeerAddress() const override;
  quic::TransportInfo getTransportInfo() const override;

  folly::Expected<folly::Unit, ErrorCode> closeSession(
      folly::Optional<uint32_t> error = folly::none) override;

 protected:
  // Transport-specific primitives - subclasses must implement these

  /**
   * Create a new local stream. Return stream ID on success.
   * Subclass should reserve the stream with picoquic/h3zero.
   */
  virtual folly::Expected<uint64_t, ErrorCode> createStreamImpl(bool bidi) = 0;

  /**
   * Mark stream as having data to send.
   * Subclass should call picoquic_mark_active_stream with appropriate context.
   */
  virtual void markStreamActiveImpl(uint64_t streamId) = 0;

  /**
   * Mark datagram as ready to send.
   * Subclass should call picoquic_mark_datagram_ready or h3zero equivalent.
   */
  virtual void markDatagramActiveImpl() = 0;

  /**
   * Send RESET_STREAM to peer.
   * Subclass should call picoquic_reset_stream or picowt_reset_stream.
   */
  virtual void resetStreamImpl(uint64_t streamId, uint32_t error) = 0;

  /**
   * Send STOP_SENDING to peer.
   * Subclass should call picoquic_stop_sending.
   */
  virtual void stopSendingImpl(uint64_t streamId, uint32_t error) = 0;

  /**
   * Send session close signal to peer (QUIC close or WT close capsule).
   */
  virtual void sendCloseImpl(uint32_t errorCode) = 0;

  /**
   * Return the maximum datagram payload size the peer will accept.
   * Used by onJitProvideDatagram to drop datagrams that can never be sent.
   * Default uses picoquic_get_transport_parameters; H3 overrides to subtract
   * the stream ID prefix encoding overhead.
   */
  virtual size_t getMaxDatagramPayload() const;

  /**
   * Provide a datagram buffer for writing, or signal defer/stop.
   *
   * Called from onJitProvideDatagram with the transport-specific context
   * pointer passed by picoquic/h3zero's JIT datagram callback.
   *
   * - length > 0: allocate and return a write buffer of exactly `length` bytes;
   *   `keepPolling` controls whether the transport polls again next packet.
   * - length == 0, keepPolling == true: defer — don't write, but poll again
   * next packet.
   * - length == 0, keepPolling == false: stop polling (queue drained).
   *
   * Returns a pointer to the write buffer when length > 0 and space is
   * available, nullptr otherwise.
   */
  virtual uint8_t*
  getDatagramBuffer(uint8_t* context, size_t length, bool keepPolling) = 0;

  /**
   * Called when session is closed (by us or peer).
   * Subclass can override for additional cleanup (e.g., clear picoquic
   * callback).
   */
  virtual void onSessionClosedImpl() {}

  // Shared helpers for subclasses

  /**
   * Process egress events from WtStreamManager (reset, stop-sending, close).
   * Called from EgressCallback::eventsAvailable().
   */
  void processEgressEvents();

  /**
   * JIT send path: dequeue data and provide to picoquic.
   * Subclass JIT callback should call this.
   * Returns true if a FIN was sent on this call.
   */
  bool
  onJitProvideData(uint64_t streamId, uint8_t* picoContext, size_t maxLength);

  /**
   * JIT datagram send path: dequeue next datagram and provide to
   * picoquic/h3zero. Handles defer-on-no-space (fixes stuck-datagram bug) and
   * IOBuf chain copy. Subclass JIT datagram callback should call this.
   */
  void onJitProvideDatagram(uint8_t* context, size_t maxLength);

  /**
   * Handle incoming stream data. Enqueues to WtStreamManager and
   * notifies handler of new peer streams.
   */
  void onStreamDataCommon(
      uint64_t streamId,
      uint8_t* bytes,
      size_t length,
      bool fin);

  /**
   * Handle peer RESET_STREAM.
   */
  void onStreamResetCommon(uint64_t streamId, uint64_t errorCode);

  /**
   * Handle peer STOP_SENDING.
   */
  void onStopSendingCommon(uint64_t streamId, uint64_t errorCode);

  /**
   * Handle peer-initiated session close.
   */
  void onSessionCloseCommon(uint32_t errorCode);

  /**
   * Handle incoming datagram.
   */
  void onReceiveDatagramCommon(uint8_t* bytes, size_t length);

  // Shared state accessible by subclasses
  picoquic_cnx_t* cnx_;
  folly::SocketAddress localAddr_;
  folly::SocketAddress peerAddr_;
  proxygen::WebTransportHandler* handler_{nullptr};
  bool isClient_;
  bool sessionClosed_{false};

  quic::HTTPPriorityQueue priorityQueue_;
  std::unique_ptr<proxygen::detail::WtStreamManager> streamManager_;

  // Queue for pending datagrams
  std::deque<std::unique_ptr<folly::IOBuf>> datagramQueue_;

  // Track streams that need handler notification
  folly::F14FastSet<uint64_t> pendingStreamNotifications_;

  std::function<void()> updateWakeTimeoutCallback_;
  PicoQuicStatsCallback* statsCallback_{nullptr};

 private:
  // WtStreamManager callbacks
  class EgressCallback
      : public proxygen::detail::WtStreamManager::EgressCallback {
   public:
    explicit EgressCallback(PicoWebTransportBase* parent) : parent_(parent) {}
    void eventsAvailable() noexcept override;

   private:
    PicoWebTransportBase* parent_;
  };

  class IngressCallback
      : public proxygen::detail::WtStreamManager::IngressCallback {
   public:
    explicit IngressCallback(PicoWebTransportBase* parent) : parent_(parent) {}
    void onNewPeerStream(uint64_t streamId) noexcept override;

   private:
    PicoWebTransportBase* parent_;
  };

  EgressCallback egressCallback_;
  IngressCallback ingressCallback_;
};

} // namespace moxygen
