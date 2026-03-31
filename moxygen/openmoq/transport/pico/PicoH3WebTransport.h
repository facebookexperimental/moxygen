/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <deque>
#include <folly/SocketAddress.h>
#include <functional>
#include <memory>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <proxygen/lib/http/webtransport/WtStreamManager.h>
#include <quic/priority/HTTPPriorityQueue.h>
#include <unordered_map>
#include <unordered_set>

// Forward declarations - avoid exposing picoquic/h3zero types
typedef struct st_picoquic_cnx_t picoquic_cnx_t;
typedef struct st_h3zero_callback_ctx_t h3zero_callback_ctx_t;
typedef struct st_h3zero_stream_ctx_t h3zero_stream_ctx_t;

namespace moxygen {

class MoQPicoServerBase;

/**
 * PicoH3WebTransport - WebTransport adapter for h3zero (HTTP/3) connections.
 *
 * This adapter bridges h3zero's WebTransport implementation to the
 * proxygen::WebTransport interface expected by MoQSession.
 *
 * Unlike PicoQuicWebTransport (which uses raw QUIC streams), this adapter:
 * - Receives WebTransport events via h3zero's picohttp_callback_* system
 * - Uses h3zero's WebTransport stream framing (control stream ID prefix)
 * - Handles HTTP/3-level flow control and settings
 *
 * Lifecycle:
 * 1. Browser sends HTTP/3 CONNECT to /moq endpoint
 * 2. h3zero invokes wtPathCallback with picohttp_callback_connect
 * 3. Server creates PicoH3WebTransport and sends 200 OK
 * 4. Subsequent events (streams, data, datagrams) route to this adapter
 */
class PicoH3WebTransport : public proxygen::WebTransport {
 public:
  PicoH3WebTransport(
      picoquic_cnx_t* cnx,
      h3zero_callback_ctx_t* h3Ctx,
      h3zero_stream_ctx_t* controlStreamCtx,
      const folly::SocketAddress& localAddr,
      const folly::SocketAddress& peerAddr);

  ~PicoH3WebTransport() override;

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

  // Stream creation
  folly::Expected<StreamWriteHandle*, ErrorCode> createUniStream() override;
  folly::Expected<BidiStreamHandle, ErrorCode> createBidiStream() override;

  // Stream credit management
  folly::SemiFuture<folly::Unit> awaitUniStreamCredit() override;
  folly::SemiFuture<folly::Unit> awaitBidiStreamCredit() override;

  // Stream operations by ID
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

  // Datagram support
  folly::Expected<folly::Unit, ErrorCode> sendDatagram(
      std::unique_ptr<folly::IOBuf> datagram) override;

  // Address accessors
  const folly::SocketAddress& getLocalAddress() const override;
  const folly::SocketAddress& getPeerAddress() const override;

  // Transport info and session management
  quic::TransportInfo getTransportInfo() const override;

  folly::Expected<folly::Unit, ErrorCode> closeSession(
      folly::Optional<uint32_t> error = folly::none) override;

  /**
   * Handle h3zero WebTransport callback event.
   * Called from the h3zero path callback (wtPathCallback).
   */
  int handleWtEvent(
      picoquic_cnx_t* cnx,
      uint8_t* bytes,
      size_t length,
      int wtEvent,
      h3zero_stream_ctx_t* streamCtx);

  /**
   * Get the control stream ID for this WebTransport session.
   */
  uint64_t getControlStreamId() const;

 private:
  // WtStreamManager callbacks
  class EgressCallback
      : public proxygen::detail::WtStreamManager::EgressCallback {
   public:
    explicit EgressCallback(PicoH3WebTransport* parent) : parent_(parent) {}
    void eventsAvailable() noexcept override;

   private:
    PicoH3WebTransport* parent_;
  };

  class IngressCallback
      : public proxygen::detail::WtStreamManager::IngressCallback {
   public:
    explicit IngressCallback(PicoH3WebTransport* parent) : parent_(parent) {}
    void onNewPeerStream(uint64_t streamId) noexcept override;

   private:
    PicoH3WebTransport* parent_;
  };

  // Handle incoming stream data
  void onStreamData(
      h3zero_stream_ctx_t* streamCtx,
      uint8_t* bytes,
      size_t length,
      bool fin);

  // Handle stream events
  void onStreamReset(uint64_t streamId, uint64_t errorCode);
  void onStopSending(uint64_t streamId, uint64_t errorCode);
  void onSessionClose(uint32_t errorCode, const char* errorMsg);

  // Handle incoming datagram
  void onReceiveDatagram(uint8_t* bytes, size_t length);

  // Process egress events from WtStreamManager
  void processEgressEvents();

  // Send data on a WebTransport stream via h3zero
  void sendStreamData(
      h3zero_stream_ctx_t* streamCtx,
      const uint8_t* data,
      size_t length,
      bool fin);

  picoquic_cnx_t* cnx_;
  h3zero_callback_ctx_t* h3Ctx_;
  h3zero_stream_ctx_t* controlStreamCtx_;

  folly::SocketAddress localAddr_;
  folly::SocketAddress peerAddr_;
  proxygen::WebTransportHandler* handler_{nullptr};

  bool sessionClosed_{false};

  EgressCallback egressCallback_;
  IngressCallback ingressCallback_;
  quic::HTTPPriorityQueue priorityQueue_;
  std::unique_ptr<proxygen::detail::WtStreamManager> streamManager_;

  // Map from QUIC stream ID to h3zero stream context
  std::unordered_map<uint64_t, h3zero_stream_ctx_t*> streamContexts_;

  // Queue for pending datagrams
  std::deque<std::unique_ptr<folly::IOBuf>> datagramQueue_;

  // Track streams that need handler notification
  std::unordered_set<uint64_t> pendingStreamNotifications_;

  // Callback to notify EventBase when wake time may have decreased
  std::function<void()> updateWakeTimeoutCallback_;
};

} // namespace moxygen
