/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQQmuxClient.h>

#include <folly/coro/Task.h>
#include <folly/io/coro/Transport.h>
#include <folly/logging/xlog.h>
#include <proxygen/lib/transport/qmux/QmuxConnector.h>
#include <moxygen/QmuxUtils.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

namespace moxygen {

MoQQmuxClient::MoQQmuxClient(
    std::shared_ptr<MoQExecutor> exec,
    proxygen::URL url)
    : MoQClientBase(std::move(exec), std::move(url)) {}

MoQQmuxClient::~MoQQmuxClient() {
  if (qmuxSession_) {
    qmuxSession_->setHandler(nullptr);
  }
}

folly::coro::Task<void> MoQQmuxClient::setupMoQSession(
    std::chrono::milliseconds connectTimeout,
    std::chrono::milliseconds /*transactionTimeout*/,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler,
    const quic::TransportSettings& transportSettings,
    const std::vector<std::string>& /*alpns*/) {
  auto* evb =
      exec_->getTypedExecutor<MoQFollyExecutorImpl>()->getBackingEventBase();
  folly::SocketAddress addr(
      url_.getHost(), url_.getPort(), /*allowNameLookup=*/true);

  XLOG(DBG1) << "MoQQmuxClient: TCP connect to " << addr.describe();
  auto connectStart = std::chrono::steady_clock::now();

  // QMUX runs over a plain bytestream. Use folly::coro::Transport's TCP
  // factory; it gives us a TransportIf that QmuxConnector takes ownership of.
  auto transport = std::make_unique<folly::coro::Transport>(
      co_await folly::coro::Transport::newConnectedSocket(
          evb, addr, connectTimeout));

  // Calculate the remaining timeout budget.
  auto tcpConnectElapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - connectStart);
  auto qmuxConnectTimeout = connectTimeout > tcpConnectElapsed
      ? connectTimeout - tcpConnectElapsed
      : std::chrono::milliseconds(0);

  // Hand off to QmuxConnector, which writes our QX_TRANSPORT_PARAMETERS,
  // awaits the peer's, and returns a fully-formed QmuxSession ready to start.
  qmuxSession_ = co_await proxygen::qmux::QmuxConnector::connect(
      evb,
      proxygen::qmux::WtDir::Client,
      qmuxParamsFromTransportSettings(transportSettings),
      std::move(transport),
      qmuxConnectTimeout);
  qmuxSession_->setHandler(this);
  qmuxSession_->start(qmuxSession_);

  transportConnectTime_ = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - connectStart);

  auto handshakeStart = std::chrono::steady_clock::now();
  auto result = co_await folly::coro::co_awaitTry(completeSetupMoQSession(
      qmuxSession_.get(),
      url_.getPath(),
      std::move(publishHandler),
      std::move(subscribeHandler)));
  moqHandshakeTime_ = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - handshakeStart);

  if (result.hasException()) {
    co_yield folly::coro::co_error(result.exception());
  }
}

folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
MoQQmuxClient::connectQuic(
    folly::SocketAddress /*connectAddr*/,
    std::chrono::milliseconds /*timeoutMs*/,
    std::shared_ptr<fizz::CertificateVerifier> /*verifier*/,
    const std::vector<std::string>& /*alpns*/,
    const quic::TransportSettings& /*transportSettings*/) {
  LOG(FATAL) << "MoQQmuxClient does not use QUIC transport";
  co_return nullptr;
}

} // namespace moxygen
