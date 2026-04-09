/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstdint>

namespace moxygen {

/**
 * Callback interface for QUIC transport statistics from a picoquic server.
 *
 * Register an implementation via MoQPicoServerBase::setPicoQuicStatsCallback.
 * All calls happen on the server's EventBase thread — no synchronization
 * needed in implementations.
 */
class PicoQuicStatsCallback {
 public:
  virtual ~PicoQuicStatsCallback() = default;

  /**
   * Called when a new QUIC connection becomes ready (picoquic_callback_ready
   * on the QUIC transport path, or WebTransport CONNECT on the h3 path).
   */
  virtual void onConnectionCreated() = 0;

  /**
   * Called just before a connection context is destroyed
   * (picoquic_callback_close / application_close / stateless_reset).
   */
  virtual void onConnectionClosed() = 0;

  /**
   * Called when a new QUIC stream is created (first callback for a stream_id).
   * Fired for both locally-initiated and peer-initiated streams.
   */
  virtual void onStreamCreated() = 0;

  /**
   * Called when a stream is cleanly closed (FIN received).
   */
  virtual void onStreamClosed() = 0;

  /**
   * Called when a stream is reset (RESET_STREAM received from peer).
   */
  virtual void onStreamReset() = 0;

  /**
   * Incremental path quality update. Called each time
   * picoquic_callback_path_quality_changed fires for a connection.
   * All fields are deltas since the last call for that connection.
   */
  struct PathQualityDelta {
    uint64_t packetsSent{0};
    uint64_t packetsLost{0};
    uint64_t bytesSent{0};
    uint64_t bytesReceived{0};
    uint64_t timerLosses{0};    // losses detected by RTO timer
    uint64_t spuriousLosses{0}; // packets later acknowledged (false losses)
    bool cwndBlocked{false};    // bytes_in_transit >= cwin at this sample point
  };
  virtual void onPathQualityDelta(const PathQualityDelta& delta) = 0;
};

} // namespace moxygen
