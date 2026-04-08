/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>
#include <string>

namespace moxygen {

/**
 * WebTransport configuration for picoquic server.
 */
struct PicoWebTransportConfig {
  bool enableWebTransport{false}; // Enable HTTP/3 WebTransport support
  bool enableQuicTransport{
      true}; // Enable QUIC for non-browser clients (default)
  std::string wtEndpoint{"/moq"}; // WebTransport CONNECT endpoint path
  uint32_t wtMaxSessions{100};    // Max concurrent WebTransport sessions
};

/**
 * QUIC transport parameter configuration for picoquic.
 *
 * Used by both server (MoQPicoServerBase) and client contexts to configure
 * flow control windows, stream limits, timeouts, and other transport
 * parameters applied to the picoquic_quic_t context.
 */
struct PicoTransportConfig {
  // Flow control (QUIC transport parameters)
  uint64_t maxData{67108864};       // connection FC window (bytes)
  uint64_t maxStreamData{16777216}; // per-stream FC window (bidi + uni)
  uint64_t maxUniStreams{8192};     // max concurrent unidirectional streams
  uint64_t maxBidiStreams{16};      // max concurrent bidirectional streams

  // Transport parameters
  uint32_t maxDatagramFrameSize{1280}; // max DATAGRAM frame size
  uint64_t idleTimeoutMs{30000};       // idle timeout (ms); handshake = /2 us
  uint32_t maxAckDelayUs{100000};      // max ACK delay (microseconds)
  uint32_t minAckDelayUs{1000};        // min ACK delay (microseconds)

  // Context-level defaults
  uint8_t defaultStreamPriority{2};   // default stream priority
  uint8_t defaultDatagramPriority{1}; // default datagram priority
  std::string ccAlgo{"bbr"};          // congestion control algorithm name
};

} // namespace moxygen
