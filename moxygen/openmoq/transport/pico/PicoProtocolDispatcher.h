/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <cstring>
#include <optional>

namespace moxygen {

/**
 * Protocol types supported by the picoquic MOQT server.
 */
enum class PicoProtocolType {
  Quic,          // MOQT over QUIC transport (ALPN: moqt-16, moqt-15, moq-00)
  WebTransportH3 // MOQT over WebTransport over HTTP/3 (ALPN: h3)
};

/**
 * PicoProtocolDispatcher - Determines protocol type from ALPN.
 *
 * Used to route incoming connections to the appropriate handler:
 * - QUIC transport connections use PicoQuicWebTransport
 * - WebTransport connections use h3zero's HTTP/3 + WebTransport stack
 */
class PicoProtocolDispatcher {
 public:
  /**
   * Determine protocol type from negotiated ALPN string.
   * alpnSelectCallback guarantees only known ALPNs are negotiated, so
   * an unrecognized value here indicates a bug.
   */
  static std::optional<PicoProtocolType> getProtocol(const char* alpn) {
    if (!alpn) {
      return std::nullopt;
    }
    if (isH3(alpn)) {
      return PicoProtocolType::WebTransportH3;
    }
    if (isQuic(alpn)) {
      return PicoProtocolType::Quic;
    }
    return std::nullopt;
  }

  /**
   * Check if ALPN indicates HTTP/3 (for WebTransport).
   */
  static bool isH3(const char* alpn) {
    return alpn && strcmp(alpn, "h3") == 0;
  }

  /**
   * Check if ALPN indicates QUIC transport (not HTTP/3).
   */
  static bool isQuic(const char* alpn) {
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
      case PicoProtocolType::Quic:
        return "Quic";
      case PicoProtocolType::WebTransportH3:
        return "WebTransportH3";
    }
    __builtin_unreachable();
  }
};

} // namespace moxygen
