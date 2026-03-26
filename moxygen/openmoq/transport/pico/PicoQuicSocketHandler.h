/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/AsyncUDPSocket.h>

// Forward declaration — avoids picoquic.h in this header
typedef struct st_picoquic_quic_t picoquic_quic_t;

namespace moxygen {

/**
 * PicoQuicSocketHandler
 *
 * Drives picoquic I/O from a folly::EventBase. Shared by both
 * MoQPicoQuicEventBaseServer and MoQPicoQuicEventBaseClient.
 *
 * Uses AsyncUDPSocket in notify-only mode so that recvmmsg is called
 * directly with a hand-crafted mmsghdr array that captures IP_PKTINFO
 * (local destination address) and TOS (ECN) — information that
 * AsyncUDPSocket's onDataAvailable callback does not expose.
 *
 * Outgoing packets are sent via ::sendmsg with IP_PKTINFO for per-packet
 * source address control and UDP_SEGMENT for GSO coalescing.
 *
 * The picoquic wake timer uses AsyncTimeout::scheduleTimeoutHighRes
 * driven by picoquic_get_next_wake_delay.
 */
class PicoQuicSocketHandler
    : public folly::AsyncUDPSocket::ReadCallback,
      public folly::AsyncTimeout,
      public folly::AsyncUDPSocket::ErrMessageCallback {
 public:
  PicoQuicSocketHandler(folly::EventBase* evb, picoquic_quic_t* quic);
  ~PicoQuicSocketHandler() override;

  /**
   * Bind the socket to addr, set socket options, and begin receiving.
   * Must be called from the EventBase thread.
   */
  void start(const folly::SocketAddress& addr);

  /**
   * Cancel the wake timer, pause reads, and unbind from the EventBase.
   * Must be called from the EventBase thread.
   */
  void stop();

  /**
   * Signal that the connection has closed; stop I/O after the current drain
   * cycle. Safe to call from within a picoquic callback. Only called by the
   * EVB client — the server never calls this (handler is shared).
   */
  void closeMaybeDeferred();

  /**
   * Called when picoquic's wake time may have decreased (e.g. after
   * mark_active_stream). Cancels the pending timer, drains any ready
   * packets, and reschedules the timer at the new wake delay.
   * Must be called from the EventBase thread.
   */
  void updateWakeTimeout();

  /**
   * Returns the local address the socket is bound to (after start()).
   */
  folly::SocketAddress boundAddress() const {
    return socket_.address();
  }

 private:
  void pauseRead();

  bool stopped_{false};
  bool pendingClose_{false};

  // AsyncUDPSocket::ReadCallback (notify-only)
  bool shouldOnlyNotify() override;
  void onNotifyDataAvailable(folly::AsyncUDPSocket& sock) noexcept override;
  void getReadBuffer(void** buf, size_t* len) noexcept override;
  void onDataAvailable(const folly::SocketAddress& client,
                       size_t len,
                       bool truncated,
                       OnDataAvailableParams params) noexcept override;
  void onReadError(const folly::AsyncSocketException& ex) noexcept override;
  void onReadClosed() noexcept override;

  // AsyncTimeout
  void timeoutExpired() noexcept override;

  // AsyncUDPSocket::ErrMessageCallback
  void errMessage(const cmsghdr& cmsg) noexcept override;
  void errMessageError(
      const folly::AsyncSocketException& ex) noexcept override;

  // I/O helpers
  void parseCmsgsAndDeliver(const struct mmsghdr& msg,
                            const uint8_t* pkt,
                            uint64_t currentTime);
  void drainOutgoing();
  void sendPacket(const uint8_t* data,
                  size_t length,
                  const struct sockaddr_storage& addrTo,
                  const struct sockaddr_storage& addrFrom,
                  int ifIndex,
                  size_t sendMsgSize);
  void rescheduleTimer();

  folly::AsyncUDPSocket socket_;
  picoquic_quic_t* quic_; // non-owning
  folly::EventBase* evb_; // non-owning
  int fd_{-1};
  bool gsoSupported_{false};
  uint16_t localPort_{0}; // actual bound port, for addrTo in parseCmsgsAndDeliver
};

} // namespace moxygen
