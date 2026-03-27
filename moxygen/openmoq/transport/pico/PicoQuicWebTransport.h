/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
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
#include <unordered_set>

// Forward declaration — avoids including picoquic.h
typedef struct st_picoquic_cnx_t picoquic_cnx_t;

namespace moxygen {

/**
 * PicoQuicWebTransport - WebTransport implementation using picoquic
 *
 * This implementation uses picoquic's "just in time" (JIT) APIs:
 * - mark_active_stream: indicates a stream has data to send
 * - prepare_to_send callback: invoked when picoquic is ready for data
 * - mark_active_datagram: indicates a datagram is ready to send
 * - prepare_datagram callback: invoked when picoquic is ready for datagram
 */
class PicoQuicWebTransport : public proxygen::WebTransport {
public:
  PicoQuicWebTransport(picoquic_cnx_t *cnx,
                       const folly::SocketAddress &localAddr,
                       const folly::SocketAddress &peerAddr);

  ~PicoQuicWebTransport() override;

  /**
   * Set the handler for incoming WebTransport events.
   */
  void setHandler(proxygen::WebTransportHandler *handler) {
    handler_ = handler;
  }

  /**
   * Set a callback invoked when picoquic's wake time decreases (e.g. after
   * mark_active_stream). The callback should cancel and reschedule the wake
   * timer so sends are not delayed up to kMaxWakeDelayUs.
   */
  void setUpdateWakeTimeoutCallback(std::function<void()> cb) {
    updateWakeTimeoutCallback_ = std::move(cb);
  }

  /**
   * Fired after picoquic_callback_close. Allows the owner to stop shared I/O
   * infrastructure after the drain cycle completes.
   */
  void setOnConnectionClosedCallback(std::function<void()> cb) {
    onConnectionClosedCallback_ = std::move(cb);
  }


  // Stream creation
  folly::Expected<StreamWriteHandle *, ErrorCode> createUniStream() override;
  folly::Expected<BidiStreamHandle, ErrorCode> createBidiStream() override;

  // Stream credit management
  folly::SemiFuture<folly::Unit> awaitUniStreamCredit() override;
  folly::SemiFuture<folly::Unit> awaitBidiStreamCredit() override;

  // Stream operations by ID
  folly::Expected<folly::SemiFuture<StreamData>, ErrorCode>
  readStreamData(uint64_t id) override;

  folly::Expected<FCState, ErrorCode>
  writeStreamData(uint64_t id, std::unique_ptr<folly::IOBuf> data, bool fin,
                  ByteEventCallback *deliveryCallback) override;

  folly::Expected<folly::Unit, ErrorCode> resetStream(uint64_t streamId,
                                                      uint32_t error) override;

  folly::Expected<folly::Unit, ErrorCode>
  setPriority(uint64_t streamId,
              quic::PriorityQueue::Priority priority) override;

  folly::Expected<folly::Unit, ErrorCode>
  setPriorityQueue(std::unique_ptr<quic::PriorityQueue> queue) noexcept override;

  folly::Expected<folly::SemiFuture<uint64_t>, ErrorCode>
  awaitWritable(uint64_t streamId) override;

  folly::Expected<folly::Unit, ErrorCode> stopSending(uint64_t streamId,
                                                      uint32_t error) override;

  // Datagram support
  folly::Expected<folly::Unit, ErrorCode>
  sendDatagram(std::unique_ptr<folly::IOBuf> datagram) override;

  // Address accessors
  const folly::SocketAddress &getLocalAddress() const override;
  const folly::SocketAddress &getPeerAddress() const override;

  // Transport info and session management
  quic::TransportInfo getTransportInfo() const override;

  folly::Expected<folly::Unit, ErrorCode>
  closeSession(folly::Optional<uint32_t> error = folly::none) override;

  /**
   * Handle a picoquic callback event for this transport.
   * Called from picoquic's callback dispatch (e.g. MoQPicoQuicServer).
   * Parameters mirror picoquic_stream_data_cb_fn but event is int to avoid
   * exposing picoquic_call_back_event_t in the header.
   */
  int handlePicoEvent(picoquic_cnx_t *cnx, uint64_t stream_id, uint8_t *bytes,
                      size_t length, int fin_or_event, void *v_stream_ctx);

private:
  // WtStreamManager callbacks
  class EgressCallback
      : public proxygen::detail::WtStreamManager::EgressCallback {
  public:
    explicit EgressCallback(PicoQuicWebTransport *parent) : parent_(parent) {}
    void eventsAvailable() noexcept override;

  private:
    PicoQuicWebTransport *parent_;
  };

  class IngressCallback
      : public proxygen::detail::WtStreamManager::IngressCallback {
  public:
    explicit IngressCallback(PicoQuicWebTransport *parent) : parent_(parent) {}
    void onNewPeerStream(uint64_t streamId) noexcept override;

  private:
    PicoQuicWebTransport *parent_;
  };

  // Handle incoming stream data
  void onStreamData(uint64_t stream_id, uint8_t *bytes, size_t length,
                    bool fin);

  // Handle stream events from picoquic
  void onStreamReset(uint64_t stream_id, uint64_t error_code);
  void onStopSending(uint64_t stream_id, uint64_t error_code);
  void onConnectionClose(uint64_t error_code);

  // Process egress events from WtStreamManager
  void processEgressEvents();

  // Handle prepare_to_send callback from picoquic
  void onPrepareToSend(uint64_t streamId, uint8_t *buffer, size_t maxLength,
                       size_t &written, bool &fin);

  // Handle prepare_datagram callback from picoquic
  void onPrepareDatagram(uint8_t *context, size_t maxLength, size_t &written);

  // Handle received datagram from peer
  void onReceiveDatagram(uint8_t *bytes, size_t length);

  // Mark stream as active in picoquic (has data to send)
  void markStreamActive(uint64_t streamId);

  // Mark datagram as active in picoquic (has data to send)
  void markDatagramActive();

  // Clear picoquic callback to prevent use-after-free
  void clearPicoquicCallback();

  picoquic_cnx_t *cnx_;

  folly::SocketAddress localAddr_;
  folly::SocketAddress peerAddr_;
  proxygen::WebTransportHandler *handler_{nullptr};

  bool isClient_;
  bool sessionClosed_{false};

  EgressCallback egressCallback_;
  IngressCallback ingressCallback_;
  quic::HTTPPriorityQueue priorityQueue_;
  std::unique_ptr<proxygen::detail::WtStreamManager> streamManager_;

  // Queue for pending datagrams
  std::deque<std::unique_ptr<folly::IOBuf>> datagramQueue_;

  // Track streams that need handler notification
  std::unordered_set<uint64_t> pendingStreamNotifications_;

  std::function<void()> updateWakeTimeoutCallback_;
  std::function<void()> onConnectionClosedCallback_;
};

} // namespace moxygen
