/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQClient.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/util/QuicConnector.h>

namespace moxygen {
folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
MoQClient::connectQuic(
    std::chrono::milliseconds timeoutMs,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    const std::vector<std::string>& alpns,
    const quic::TransportSettings& transportSettings) {
  auto ts = transportSettings;
  // Priority schedule datagrams with streams rather than ahead of them, and
  // prefer new datagrams when the buffers are full.
  ts.datagramConfig.scheduleDatagramsWithStreams = true;
  ts.datagramConfig.recvDropOldDataFirst = true;
  ts.datagramConfig.sendDropOldDataFirst = true;
  auto quicClient = co_await QuicConnector::connectQuic(
      exec_->getTypedExecutor<MoQFollyExecutorImpl>()->getBackingEventBase(),
      folly::SocketAddress(
          url_.getHost(), url_.getPort(), true), // blocking DNS,
      timeoutMs,
      verifier,
      alpns,
      ts,
      pskCache_,
      url_.getHost(),
      earlyDataHandler_);

  co_return quicClient;
}

} // namespace moxygen
