/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/openmoq/transport/pico/PicoQuicSocketHandler.h"
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <picoquic.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/ip_icmp.h>
#include <netinet/udp.h>
#include <sys/socket.h>
#ifdef __linux__
#include <linux/errqueue.h>
#endif

// IP_PKTINFO (Linux) provides dst addr + ifindex in one cmsg.
// On macOS/BSD, use IP_RECVDSTADDR for dst addr instead.
#ifndef IP_PKTINFO
#ifdef IP_RECVDSTADDR
#define MOXYGEN_USE_IP_RECVDSTADDR 1
#endif
#endif

// IPV6_RECVPKTINFO (POSIX RFC 3542) enables IPv6 pktinfo cmsgs.
// Fall back to IPV6_PKTINFO on older platforms that lack it.
#ifndef IPV6_RECVPKTINFO
#define IPV6_RECVPKTINFO IPV6_PKTINFO
#endif

namespace moxygen {

namespace {

constexpr int kRecvBatchSize = 16;
constexpr size_t kMaxPacketSize = 1500;
// cmsg buffer per message: space for IP_PKTINFO/IPV6_PKTINFO + IP_TOS.
constexpr size_t kCmsgBufSize =
    CMSG_SPACE(sizeof(struct in6_pktinfo)) + CMSG_SPACE(sizeof(uint8_t));
// Max wake delay passed to picoquic (200 ms in microseconds).
// This is does not impact loop latency, whenever the wake delay shrinks, we
// reschedule
constexpr int64_t kMaxWakeDelayUs = 200'000;

} // namespace

PicoQuicSocketHandler::PicoQuicSocketHandler(
    folly::EventBase* evb,
    picoquic_quic_t* quic)
    : folly::AsyncTimeout(evb), socket_(evb), quic_(quic), evb_(evb) {}

PicoQuicSocketHandler::~PicoQuicSocketHandler() {
  stop();
}

void PicoQuicSocketHandler::start(const folly::SocketAddress& addr) {
  XLOG(DBG1) << "PicoQuicSocketHandler::start called, addr=" << addr.describe();

  if (addr.getFamily() == AF_INET6) {
    // Enable dual-stack (accept both IPv4 and IPv6) by setting IPV6_V6ONLY=0
    // before bind. AsyncUDPSocket supports this via init() +
    // applyOptions(PRE_BIND).
    socket_.init(addr.getFamily());
    socket_.applyOptions(
        {{folly::SocketOptionKey{IPPROTO_IPV6, IPV6_V6ONLY}, 0}},
        folly::SocketOptionKey::ApplyPos::PRE_BIND);
    XLOG(DBG3) << "Dual-stack enabled (IPV6_V6ONLY=0)";
  }
  socket_.bind(addr);
  fd_ = socket_.getNetworkSocket().toFd();
  localPort_ = socket_.address().getPort();
  XLOG(DBG4) << "Socket bound, fd=" << fd_ << " localPort=" << localPort_;

  // Enable IP_PKTINFO / IPV6_RECVPKTINFO so recvmsg delivers the local
  // destination address. AsyncUDPSocket does not set these by default.
  int one = 1;
  if (addr.getFamily() == AF_INET6) {
    if (::setsockopt(fd_, IPPROTO_IPV6, IPV6_RECVPKTINFO, &one, sizeof(one)) <
        0) {
      XLOG(WARN) << "setsockopt IPV6_RECVPKTINFO failed: "
                 << folly::errnoStr(errno);
    }
  } else {
#ifdef MOXYGEN_USE_IP_RECVDSTADDR
    if (::setsockopt(fd_, IPPROTO_IP, IP_RECVDSTADDR, &one, sizeof(one)) < 0) {
      XLOG(WARN) << "setsockopt IP_RECVDSTADDR failed: "
                 << folly::errnoStr(errno);
    }
#else
    if (::setsockopt(fd_, IPPROTO_IP, IP_PKTINFO, &one, sizeof(one)) < 0) {
      XLOG(WARN) << "setsockopt IP_PKTINFO failed: " << folly::errnoStr(errno);
    }
#endif
  }

  // ECN receive (sets IP_RECVTOS + IPV6_RECVTCLASS).
  socket_.setRecvTos(true);

  // GRO if available.
  if (socket_.getGRO() >= 0) {
    socket_.setGRO(true);
  }

  // GSO availability.
  gsoSupported_ = (socket_.getGSO() >= 0);

  socket_.setErrMessageCallback(this);
  socket_.resumeRead(this);
  rescheduleTimer();

  XLOG(INFO) << "PicoQuicSocketHandler started on "
             << socket_.address().describe() << " gso=" << gsoSupported_;
}

void PicoQuicSocketHandler::stop() {
  if (stopped_) {
    return;
  }
  stopped_ = true;
  cancelTimeout();
  drainOutgoing();
  pauseRead();
}

void PicoQuicSocketHandler::closeMaybeDeferred() {
  pendingClose_ = true;
}

void PicoQuicSocketHandler::pauseRead() {
  if (socket_.isBound()) {
    socket_.pauseRead();
    socket_.setErrMessageCallback(nullptr);
  }
}

// ---------------------------------------------------------------------------
// AsyncUDPSocket::ReadCallback — notify-only
// ---------------------------------------------------------------------------

bool PicoQuicSocketHandler::shouldOnlyNotify() {
  return true;
}

void PicoQuicSocketHandler::onNotifyDataAvailable(
    folly::AsyncUDPSocket& sock) noexcept {
  struct mmsghdr msgs[kRecvBatchSize];
  struct iovec iovecs[kRecvBatchSize];
  uint8_t bufs[kRecvBatchSize][kMaxPacketSize];
  sockaddr_storage fromAddrs[kRecvBatchSize];
  char cmsgBufs[kRecvBatchSize][kCmsgBufSize];

  for (int i = 0; i < kRecvBatchSize; i++) {
    iovecs[i].iov_base = bufs[i];
    iovecs[i].iov_len = kMaxPacketSize;
    msgs[i].msg_hdr.msg_name = &fromAddrs[i];
    msgs[i].msg_hdr.msg_namelen = sizeof(fromAddrs[i]);
    msgs[i].msg_hdr.msg_iov = &iovecs[i];
    msgs[i].msg_hdr.msg_iovlen = 1;
    msgs[i].msg_hdr.msg_control = cmsgBufs[i];
    msgs[i].msg_hdr.msg_controllen = kCmsgBufSize;
    msgs[i].msg_hdr.msg_flags = 0;
    msgs[i].msg_len = 0;
  }

  bool anyReceived = false;
  int totalReceived = 0;
  for (;;) {
    int n = sock.recvmmsg(msgs, kRecvBatchSize, MSG_DONTWAIT, nullptr);
    if (n <= 0) {
      break;
    }
    anyReceived = true;
    totalReceived += n;

    uint64_t currentTime = picoquic_current_time();
    for (int i = 0; i < n; i++) {
      parseCmsgsAndDeliver(msgs[i], bufs[i], currentTime);
      msgs[i].msg_hdr.msg_namelen = sizeof(fromAddrs[i]);
      msgs[i].msg_hdr.msg_controllen = kCmsgBufSize;
      msgs[i].msg_hdr.msg_flags = 0;
      msgs[i].msg_len = 0;
    }
  }

  if (anyReceived) {
    XLOG(DBG5) << "onNotifyDataAvailable: received " << totalReceived
               << " packets, draining";
    drainOutgoing();
    if (pendingClose_) {
      stop();
    } else {
      rescheduleTimer();
    }
  }
}

void PicoQuicSocketHandler::getReadBuffer(
    void** /*buf*/,
    size_t* /*len*/) noexcept {
  XLOG(WARN)
      << "getReadBuffer called (should not happen with shouldOnlyNotify)";
}

void PicoQuicSocketHandler::onDataAvailable(
    const folly::SocketAddress& /*client*/,
    size_t /*len*/,
    bool /*truncated*/,
    OnDataAvailableParams /*params*/) noexcept {
  XLOG(WARN)
      << "onDataAvailable called (should not happen with shouldOnlyNotify)";
}

void PicoQuicSocketHandler::onReadError(
    const folly::AsyncSocketException& ex) noexcept {
  XLOG(ERR) << "UDP read error: " << ex.what();
}

void PicoQuicSocketHandler::onReadClosed() noexcept {
  XLOG(DBG1) << "UDP socket closed";
}

// ---------------------------------------------------------------------------
// AsyncTimeout — picoquic wake timer
// ---------------------------------------------------------------------------

void PicoQuicSocketHandler::timeoutExpired() noexcept {
  drainOutgoing();
  if (pendingClose_) {
    stop();
  } else {
    rescheduleTimer();
  }
}

// ---------------------------------------------------------------------------
// AsyncUDPSocket::ErrMessageCallback — ICMP errors
// ---------------------------------------------------------------------------

void PicoQuicSocketHandler::errMessage(const cmsghdr& cmsg) noexcept {
#ifdef __linux__
  if ((cmsg.cmsg_level == SOL_IP && cmsg.cmsg_type == IP_RECVERR) ||
      (cmsg.cmsg_level == SOL_IPV6 && cmsg.cmsg_type == IPV6_RECVERR)) {
    const auto* ee =
        reinterpret_cast<const struct sock_extended_err*>(CMSG_DATA(&cmsg));
    XLOG(WARN) << "ICMP error from peer: origin=" << (int)ee->ee_origin
               << " type=" << (int)ee->ee_type << " code=" << (int)ee->ee_code
               << " errno=" << (int)ee->ee_errno;
    // TODO: call picoquic_notify_destination_unreachable(cnx, ...) to let
    // picoquic fail the path immediately.  Requires a cnx lookup by peer
    // address; picoquic has no public API for that yet.
  }
#endif
}

void PicoQuicSocketHandler::errMessageError(
    const folly::AsyncSocketException& ex) noexcept {
  XLOG(WARN) << "Error reading error queue: " << ex.what();
}

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

void PicoQuicSocketHandler::parseCmsgsAndDeliver(
    const struct mmsghdr& msg,
    const uint8_t* pkt,
    uint64_t currentTime) {
  XLOG(DBG5) << "parseCmsgsAndDeliver: pktLen=" << msg.msg_len;
  sockaddr_storage addrTo{};
  int ifIndex = 0;
  unsigned char ecn = 0;

  for (auto* cmsg = CMSG_FIRSTHDR(&msg.msg_hdr); cmsg != nullptr;
       cmsg = CMSG_NXTHDR(const_cast<struct msghdr*>(&msg.msg_hdr), cmsg)) {
    if (cmsg->cmsg_level == IPPROTO_IP) {
#ifdef MOXYGEN_USE_IP_RECVDSTADDR
      if (cmsg->cmsg_type == IP_RECVDSTADDR) {
        auto* dst = reinterpret_cast<sockaddr_in*>(&addrTo);
        dst->sin_family = AF_INET;
        dst->sin_port = htons(localPort_);
        dst->sin_addr = *reinterpret_cast<struct in_addr*>(CMSG_DATA(cmsg));
      } else
#else
      if (cmsg->cmsg_type == IP_PKTINFO) {
        auto* pki = reinterpret_cast<struct in_pktinfo*>(CMSG_DATA(cmsg));
        auto* dst = reinterpret_cast<sockaddr_in*>(&addrTo);
        dst->sin_family = AF_INET;
        dst->sin_port = htons(localPort_);
        dst->sin_addr = pki->ipi_addr;
        ifIndex = static_cast<int>(pki->ipi_ifindex);
      } else
#endif
          if (cmsg->cmsg_type == IP_TOS || cmsg->cmsg_type == IP_RECVTOS) {
        ecn = *reinterpret_cast<unsigned char*>(CMSG_DATA(cmsg));
      }
    } else if (cmsg->cmsg_level == IPPROTO_IPV6) {
      if (cmsg->cmsg_type == IPV6_PKTINFO) {
        auto* pki6 = reinterpret_cast<struct in6_pktinfo*>(CMSG_DATA(cmsg));
        auto* dst = reinterpret_cast<sockaddr_in6*>(&addrTo);
        dst->sin6_family = AF_INET6;
        dst->sin6_port = htons(localPort_);
        dst->sin6_addr = pki6->ipi6_addr;
        ifIndex = static_cast<int>(pki6->ipi6_ifindex);
      } else if (cmsg->cmsg_type == IPV6_TCLASS) {
        ecn = *reinterpret_cast<unsigned char*>(CMSG_DATA(cmsg));
      }
    }
  }

  // Log address info for debugging dual-stack issues
  auto* fromAddr =
      reinterpret_cast<const sockaddr_storage*>(msg.msg_hdr.msg_name);
  XLOG(DBG6) << "parseCmsgsAndDeliver: from.family=" << fromAddr->ss_family
             << " to.family=" << addrTo.ss_family << " pktLen=" << msg.msg_len;

  picoquic_cnx_t* lastCnx = nullptr;
  int ret = picoquic_incoming_packet_ex(
      quic_,
      const_cast<uint8_t*>(pkt),
      static_cast<size_t>(msg.msg_len),
      reinterpret_cast<sockaddr*>(const_cast<sockaddr_storage*>(
          reinterpret_cast<const sockaddr_storage*>(msg.msg_hdr.msg_name))),
      reinterpret_cast<sockaddr*>(&addrTo),
      ifIndex,
      ecn,
      &lastCnx,
      currentTime);

  if (ret != 0) {
    XLOG(DBG4) << "picoquic_incoming_packet_ex returned " << ret;
  }
}

void PicoQuicSocketHandler::drainOutgoing() {
  static constexpr size_t kSendBufSize = kMaxPacketSize * 10;
  uint8_t sendBuf[kSendBufSize];

  uint64_t currentTime = picoquic_current_time();
  int packetsSent = 0;

  for (;;) {
    size_t sendLength = 0;
    size_t sendMsgSize = 0;
    sockaddr_storage addrTo{};
    sockaddr_storage addrFrom{};
    int ifIndex = 0;
    picoquic_connection_id_t logCid{};
    picoquic_cnx_t* lastCnx = nullptr;

    int ret = picoquic_prepare_next_packet_ex(
        quic_,
        currentTime,
        sendBuf,
        kSendBufSize,
        &sendLength,
        &addrTo,
        &addrFrom,
        &ifIndex,
        &logCid,
        &lastCnx,
        &sendMsgSize);

    if (ret != 0 || sendLength == 0) {
      XLOG(DBG5) << "drainOutgoing: done, sent=" << packetsSent;
      break;
    }

    ++packetsSent;

    sendPacket(sendBuf, sendLength, addrTo, addrFrom, ifIndex, sendMsgSize);
  }
}

void PicoQuicSocketHandler::sendPacket(
    const uint8_t* data,
    size_t length,
    const sockaddr_storage& addrTo,
    const sockaddr_storage& addrFrom,
    int ifIndex,
    size_t sendMsgSize) {
  char cmsgBuf
      [CMSG_SPACE(sizeof(struct in6_pktinfo)) + CMSG_SPACE(sizeof(uint16_t))];

  struct iovec iov;
  iov.iov_base = const_cast<uint8_t*>(data);
  iov.iov_len = length;

  struct msghdr msg{};
  msg.msg_name = const_cast<sockaddr_storage*>(&addrTo);
  msg.msg_namelen = (addrTo.ss_family == AF_INET6) ? sizeof(sockaddr_in6)
                                                   : sizeof(sockaddr_in);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = cmsgBuf;
  msg.msg_controllen = sizeof(cmsgBuf);

  struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
  size_t controlLen = 0;

  // Only set pktinfo if addrFrom is valid — matches picoquic sockloop behavior.
  if (addrFrom.ss_family == AF_INET6) {
    cmsg->cmsg_level = IPPROTO_IPV6;
    cmsg->cmsg_type = IPV6_PKTINFO;
    cmsg->cmsg_len = CMSG_LEN(sizeof(struct in6_pktinfo));
    auto* pki6 = reinterpret_cast<struct in6_pktinfo*>(CMSG_DATA(cmsg));
    pki6->ipi6_addr =
        reinterpret_cast<const sockaddr_in6*>(&addrFrom)->sin6_addr;
    pki6->ipi6_ifindex = static_cast<unsigned>(ifIndex);
    controlLen += CMSG_SPACE(sizeof(struct in6_pktinfo));
  } else if (addrFrom.ss_family == AF_INET) {
#ifdef MOXYGEN_USE_IP_RECVDSTADDR
    // macOS/BSD: use IP_SENDSRCADDR (struct in_addr, no ifindex).
    cmsg->cmsg_level = IPPROTO_IP;
    cmsg->cmsg_type = IP_SENDSRCADDR;
    cmsg->cmsg_len = CMSG_LEN(sizeof(struct in_addr));
    *reinterpret_cast<struct in_addr*>(CMSG_DATA(cmsg)) =
        reinterpret_cast<const sockaddr_in*>(&addrFrom)->sin_addr;
    controlLen += CMSG_SPACE(sizeof(struct in_addr));
#else
    cmsg->cmsg_level = IPPROTO_IP;
    cmsg->cmsg_type = IP_PKTINFO;
    cmsg->cmsg_len = CMSG_LEN(sizeof(struct in_pktinfo));
    auto* pki = reinterpret_cast<struct in_pktinfo*>(CMSG_DATA(cmsg));
    pki->ipi_spec_dst =
        reinterpret_cast<const sockaddr_in*>(&addrFrom)->sin_addr;
    pki->ipi_ifindex = static_cast<unsigned long>(ifIndex);
    controlLen += CMSG_SPACE(sizeof(struct in_pktinfo));
#endif
  }

#if defined(UDP_SEGMENT)
  if (gsoSupported_ && sendMsgSize > 0 && sendMsgSize < length) {
    cmsg = reinterpret_cast<struct cmsghdr*>(
        reinterpret_cast<char*>(cmsgBuf) + controlLen);
    cmsg->cmsg_level = SOL_UDP;
    cmsg->cmsg_type = UDP_SEGMENT;
    cmsg->cmsg_len = CMSG_LEN(sizeof(uint16_t));
    *reinterpret_cast<uint16_t*>(CMSG_DATA(cmsg)) =
        static_cast<uint16_t>(sendMsgSize);
    controlLen += CMSG_SPACE(sizeof(uint16_t));
  }
#endif

  msg.msg_controllen = controlLen;
  if (controlLen == 0) {
    msg.msg_control = nullptr;
  }

  ssize_t sent = ::sendmsg(fd_, &msg, 0);
  if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
    XLOG(WARN) << "sendmsg failed: " << folly::errnoStr(errno)
               << " addrFrom.family=" << addrFrom.ss_family;
  }
}

void PicoQuicSocketHandler::updateWakeTimeout() {
  // Called via WakeTimeGuard when picoquic's next wake time decreases (e.g.
  // after marking a stream or datagram active). Cancel the current timer and
  // reschedule: rescheduleTimer will call evb_->add() for delay<=0, which is
  // effectively immediate since the EVB passes 0 to epoll when tasks are
  // pending.
  cancelTimeout();
  rescheduleTimer();
}

void PicoQuicSocketHandler::rescheduleTimer() {
  uint64_t now = picoquic_current_time();
  int64_t rawDelayUs = picoquic_get_next_wake_delay(quic_, now, INT64_MAX);
  int64_t delayUs = std::min(rawDelayUs, kMaxWakeDelayUs);
  if (delayUs <= 0) {
    evb_->add([this] {
      drainOutgoing();
      rescheduleTimer();
    });
  } else {
    scheduleTimeoutHighRes(std::chrono::microseconds(delayUs));
  }
}

} // namespace moxygen
