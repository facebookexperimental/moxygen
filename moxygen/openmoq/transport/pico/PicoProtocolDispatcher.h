/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstring>
#include <string_view>

namespace moxygen {

/**
 * Protocol types supported by the picoquic MoQ server.
 */
enum class PicoProtocolType {
  Unknown,
  RawMoQ,       // Direct MoQ over QUIC (ALPN: moqt-16, moqt-15, moq-00)
  WebTransport  // MoQ over WebTransport over HTTP/3 (ALPN: h3)
};

/**
 * PicoProtocolDispatcher - Determines protocol type from ALPN.
 *
 * Used to route incoming connections to the appropriate handler:
 * - Raw MoQ connections use the existing PicoQuicWebTransport adapter
 * - WebTransport connections use h3zero's HTTP/3 + WebTransport stack
 */
class PicoProtocolDispatcher {
 public:
  /**
   * Determine protocol type from negotiated ALPN string.
   */
  static PicoProtocolType getProtocol(const char* alpn) {
    if (!alpn) {
      return PicoProtocolType::Unknown;
    }
    if (isH3(alpn)) {
      return PicoProtocolType::WebTransport;
    }
    if (isRawMoQ(alpn)) {
      return PicoProtocolType::RawMoQ;
    }
    return PicoProtocolType::Unknown;
  }

  /**
   * Check if ALPN indicates HTTP/3 (for WebTransport).
   */
  static bool isH3(const char* alpn) {
    return alpn && strcmp(alpn, "h3") == 0;
  }

  /**
   * Check if ALPN indicates raw MoQ over QUIC.
   */
  static bool isRawMoQ(const char* alpn) {
    if (!alpn) {
      return false;
    }
    // moqt-NN format (e.g., moqt-16, moqt-15)
    if (strncmp(alpn, "moqt-", 5) == 0) {
      return true;
    }
    // Legacy moq-00 format
    if (strcmp(alpn, "moq-00") == 0) {
      return true;
    }
    return false;
  }

  /**
   * Get human-readable name for protocol type.
   */
  static const char* protocolName(PicoProtocolType type) {
    switch (type) {
      case PicoProtocolType::RawMoQ:
        return "RawMoQ";
      case PicoProtocolType::WebTransport:
        return "WebTransport";
      default:
        return "Unknown";
    }
  }
};

}  // namespace moxygen
