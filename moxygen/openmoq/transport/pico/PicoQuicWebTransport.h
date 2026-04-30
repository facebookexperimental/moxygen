/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <functional>
#include "moxygen/openmoq/transport/pico/PicoWebTransportBase.h"

namespace moxygen {

/**
 * PicoQuicWebTransport - QUIC WebTransport implementation using picoquic
 *
 * This class extends PicoWebTransportBase and provides the QUIC
 * transport-specific implementations using picoquic's callback system.
 */
class PicoQuicWebTransport : public PicoWebTransportBase {
 public:
  PicoQuicWebTransport(
      picoquic_cnx_t* cnx,
      const folly::SocketAddress& localAddr,
      const folly::SocketAddress& peerAddr);

  ~PicoQuicWebTransport() override;

  /**
   * Fired after picoquic_callback_close. Allows the owner to stop shared I/O
   * infrastructure after the drain cycle completes.
   */
  void setOnConnectionClosedCallback(std::function<void()> cb) {
    onConnectionClosedCallback_ = std::move(cb);
  }

  /**
   * Handle a picoquic callback event for this transport.
   * Called from picoquic's callback dispatch (e.g. MoQPicoQuicServer).
   * Parameters mirror picoquic_stream_data_cb_fn but event is int to avoid
   * exposing picoquic_call_back_event_t in the header.
   */
  int handlePicoEvent(
      picoquic_cnx_t* cnx,
      uint64_t stream_id,
      uint8_t* bytes,
      size_t length,
      int fin_or_event,
      void* v_stream_ctx);

 protected:
  // PicoWebTransportBase pure virtual implementations
  folly::Expected<uint64_t, ErrorCode> createStreamImpl(bool bidi) override;
  void markStreamActiveImpl(uint64_t streamId) override;
  void markDatagramActiveImpl() override;
  void resetStreamImpl(uint64_t streamId, uint32_t error) override;
  void stopSendingImpl(uint64_t streamId, uint32_t error) override;
  void sendCloseImpl(uint32_t errorCode) override;
  void onSessionClosedImpl() override;
  uint8_t* getDatagramBuffer(uint8_t* context, size_t length, bool keepPolling)
      override;

 private:
  // Clear picoquic callback to prevent use-after-free
  void clearPicoquicCallback();

  std::function<void()> onConnectionClosedCallback_;
};

} // namespace moxygen
