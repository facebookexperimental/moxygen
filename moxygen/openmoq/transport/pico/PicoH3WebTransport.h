/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/container/F14Map.h>
#include "moxygen/openmoq/transport/pico/PicoWebTransportBase.h"

// Forward declarations - avoid exposing picoquic/h3zero types
typedef struct st_h3zero_callback_ctx_t h3zero_callback_ctx_t;
typedef struct st_h3zero_stream_ctx_t h3zero_stream_ctx_t;

namespace moxygen {

/**
 * PicoH3WebTransport - WebTransport adapter for h3zero (HTTP/3) connections.
 *
 * This adapter extends PicoWebTransportBase and provides the HTTP/3-specific
 * implementations using h3zero's callback system.
 *
 * Unlike PicoQuicWebTransport (which uses QUIC transport streams), this
 * adapter:
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
class PicoH3WebTransport : public PicoWebTransportBase {
 public:
  PicoH3WebTransport(
      picoquic_cnx_t* cnx,
      h3zero_callback_ctx_t* h3Ctx,
      h3zero_stream_ctx_t* controlStreamCtx,
      const folly::SocketAddress& localAddr,
      const folly::SocketAddress& peerAddr);

  ~PicoH3WebTransport() override;

  /**
   * Handle h3zero WebTransport callback event.
   * Called from the h3zero path callback (wtPathCallback).
   * Returns kDeleteCtx when the last stream has been freed and the session
   * context (PicoH3SessionContext) should be deleted by the caller.
   */
  int handleWtEvent(
      picoquic_cnx_t* cnx,
      uint8_t* bytes,
      size_t length,
      int wtEvent,
      h3zero_stream_ctx_t* streamCtx);

  // Sentinel return value from handleWtEvent: caller must delete sessionCtx.
  static constexpr int kDeleteCtx = 1;

  /**
   * Get the control stream ID for this WebTransport session.
   */
  uint64_t getControlStreamId() const;

 protected:
  // PicoWebTransportBase pure virtual implementations
  folly::Expected<uint64_t, ErrorCode> createStreamImpl(bool bidi) override;
  void markStreamActiveImpl(uint64_t streamId) override;
  void markDatagramActiveImpl() override;
  void resetStreamImpl(uint64_t streamId, uint32_t error) override;
  void stopSendingImpl(uint64_t streamId, uint32_t error) override;
  void sendCloseImpl(uint32_t errorCode) override;
  uint8_t* getDatagramBuffer(uint8_t* context, size_t length, bool keepPolling)
      override;
  size_t getMaxDatagramPayload() const override;

 private:
  // Handle incoming stream data (H3-specific: control stream capsule parsing)
  void onStreamData(
      h3zero_stream_ctx_t* streamCtx,
      uint8_t* bytes,
      size_t length,
      bool fin);

  // If both is_fin_received and is_fin_sent are set, call h3zero_delete_stream.
  // picohttp_callback_free will fire and erase the stream from streamContexts_.
  void maybeDeleteStream(h3zero_stream_ctx_t* streamCtx) noexcept;

  h3zero_callback_ctx_t* h3Ctx_;
  h3zero_stream_ctx_t* controlStreamCtx_;

  // Map from QUIC stream ID to h3zero stream context
  folly::F14FastMap<uint64_t, h3zero_stream_ctx_t*> streamContexts_;

  // Set true when picohttp_callback_deregister fires; once set and
  // streamContexts_ drains to empty on the last picohttp_callback_free,
  // handleWtEvent returns kDeleteCtx so the caller can delete sessionCtx.
  bool deregistered_{false};
};

} // namespace moxygen
