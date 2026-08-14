/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQQmuxClient.h>

#include <folly/coro/CurrentExecutor.h>
#include <folly/logging/xlog.h>
#include <proxygen/lib/transport/qmux/QmuxConnector.h>
#include <moxygen/MoQFollyQmuxTransportFactory.h>
#include <moxygen/QmuxUtils.h>

#include <stdexcept>

namespace moxygen {

MoQQmuxClient::MoQQmuxClient(
    std::shared_ptr<MoQExecutor> exec,
    proxygen::URL url,
    std::shared_ptr<fizz::CertificateVerifier> verifier)
    : MoQQmuxClient(
          exec,
          std::move(url),
          makeFollyQmuxTransportFactory(exec, std::move(verifier))) {}

MoQQmuxClient::MoQQmuxClient(
    std::shared_ptr<MoQExecutor> exec,
    proxygen::URL url,
    std::shared_ptr<QmuxTransportFactory> transportFactory)
    : MoQClientBase(std::move(exec), std::move(url)),
      transportFactory_(std::move(transportFactory)) {
  if (!transportFactory_) {
    throw std::invalid_argument("MoQQmuxClient requires a transport factory");
  }
}

MoQQmuxClient::MoQQmuxClient(
    std::shared_ptr<MoQExecutor> exec,
    proxygen::URL url,
    SessionFactory sessionFactory,
    std::shared_ptr<fizz::CertificateVerifier> verifier)
    : MoQQmuxClient(
          exec,
          std::move(url),
          std::move(sessionFactory),
          makeFollyQmuxTransportFactory(exec, std::move(verifier))) {}

MoQQmuxClient::MoQQmuxClient(
    std::shared_ptr<MoQExecutor> exec,
    proxygen::URL url,
    SessionFactory sessionFactory,
    std::shared_ptr<QmuxTransportFactory> transportFactory)
    : MoQClientBase(std::move(exec), std::move(url), std::move(sessionFactory)),
      transportFactory_(std::move(transportFactory)) {
  if (!transportFactory_) {
    throw std::invalid_argument("MoQQmuxClient requires a transport factory");
  }
}

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
    const std::vector<std::string>& alpns) {
  auto executor = co_await folly::coro::co_current_executor;
  XLOG(DBG1) << "MoQQmuxClient: connect to "
             << url_.getHostAndPortOmitDefault();
  auto connectStart = std::chrono::steady_clock::now();

  auto connectResult = co_await folly::coro::co_withExecutor(
      executor,
      transportFactory_->createQmuxTransport(url_, connectTimeout, alpns));
  if (!connectResult.transport) {
    co_yield folly::coro::co_error(
        std::runtime_error("QMUX transport factory returned no transport"));
  }
  if (connectResult.negotiatedProtocol) {
    negotiatedProtocol_ = std::move(connectResult.negotiatedProtocol);
  }

  // Calculate the remaining timeout budget.
  auto elapsedSoFar = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - connectStart);
  auto qmuxConnectTimeout = connectTimeout > elapsedSoFar
      ? connectTimeout - elapsedSoFar
      : std::chrono::milliseconds(0);

  // Hand off to QmuxConnector, which writes our QX_TRANSPORT_PARAMETERS,
  // awaits the peer's, and returns a fully-formed QmuxSession ready to start.
  qmuxSession_ = co_await folly::coro::co_withExecutor(
      executor,
      proxygen::qmux::QmuxConnector::connect(
          exec_,
          proxygen::qmux::WtDir::Client,
          qmuxParamsFromTransportSettings(transportSettings),
          std::move(connectResult.transport),
          qmuxConnectTimeout));
  qmuxSession_->start(qmuxSession_);

  transportConnectTime_ = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - connectStart);

  completeSetupMoQSession(
      qmuxSession_.get(),
      url_.getPath(),
      std::move(publishHandler),
      std::move(subscribeHandler));
  qmuxSession_->setHandler(moqSession_.get());
  co_await folly::coro::co_withExecutor(executor, awaitSetupComplete());
}

folly::AsyncTransport* MoQQmuxClient::getUnderlyingTransport() const {
  return qmuxSession_ ? qmuxSession_->getUnderlyingTransport() : nullptr;
}

folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
MoQQmuxClient::connectQuic(
    std::chrono::milliseconds /*timeoutMs*/,
    std::shared_ptr<fizz::CertificateVerifier> /*verifier*/,
    const std::vector<std::string>& /*alpns*/,
    const quic::TransportSettings& /*transportSettings*/) {
  LOG(FATAL) << "MoQQmuxClient does not use QUIC transport";
  co_return nullptr;
}

} // namespace moxygen
