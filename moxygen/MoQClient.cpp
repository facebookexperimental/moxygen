/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQClient.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <moxygen/util/QuicConnector.h>

namespace moxygen {
folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
MoQClient::connectQuic(
    folly::SocketAddress connectAddr,
    std::chrono::milliseconds timeoutMs,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    std::string alpn) {
  return QuicConnector::connectQuic(
      exec_->getTypedExecutor<MoQFollyExecutorImpl>()->getBackingEventBase(),
      folly::SocketAddress(
          url_.getHost(), url_.getPort(), true), // blocking DNS,
      timeoutMs,
      verifier,
      "moq-00");
}

} // namespace moxygen
