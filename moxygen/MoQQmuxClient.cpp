/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQQmuxClient.h>

#include <fizz/client/AsyncFizzClient.h>
#include <fizz/client/FizzClientContext.h>
#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Task.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/coro/Transport.h>
#include <folly/logging/xlog.h>
#include <proxygen/lib/transport/qmux/QmuxConnector.h>
#include <moxygen/QmuxUtils.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

namespace moxygen {

namespace {

// Bridges AsyncSocket::connect into a coroutine.
class TcpConnectCb : public folly::AsyncSocket::ConnectCallback {
 public:
  void connectSuccess() noexcept override {
    baton.post();
  }
  void connectErr(const folly::AsyncSocketException& ex) noexcept override {
    exception = ex;
    baton.post();
  }

  folly::coro::Baton baton;
  std::optional<folly::AsyncSocketException> exception;
};

// Bridges AsyncFizzClient's callback-based handshake into a coroutine.
class FizzHandshakeCb
    : public fizz::client::AsyncFizzClient::HandshakeCallback {
 public:
  void fizzHandshakeSuccess(
      fizz::client::AsyncFizzClient* /*transport*/) noexcept override {
    baton.post();
  }
  void fizzHandshakeError(
      fizz::client::AsyncFizzClient* /*transport*/,
      folly::exception_wrapper ex) noexcept override {
    exception = std::move(ex);
    baton.post();
  }

  folly::coro::Baton baton;
  folly::exception_wrapper exception;
};

folly::coro::Task<folly::AsyncSocket::UniquePtr> connectTcp(
    folly::EventBase* evb,
    const folly::SocketAddress& addr,
    std::chrono::milliseconds connectTimeout) {
  folly::AsyncSocket::UniquePtr asyncSocket(folly::AsyncSocket::newSocket(evb));
  TcpConnectCb tcpCb;
  asyncSocket->connect(&tcpCb, addr, static_cast<int>(connectTimeout.count()));
  co_await tcpCb.baton;
  if (tcpCb.exception) {
    co_yield folly::coro::co_error(*tcpCb.exception);
  }
  co_return std::move(asyncSocket);
}

folly::coro::Task<fizz::client::AsyncFizzClient::UniquePtr> fizzHandshake(
    folly::AsyncSocket::UniquePtr asyncSocket,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    std::string host,
    const std::vector<std::string>& alpns,
    std::chrono::milliseconds fizzTimeout) {
  auto fizzContext = std::make_shared<fizz::client::FizzClientContext>();
  fizzContext->setSupportedAlpns(alpns);
  fizz::client::AsyncFizzClient::UniquePtr fizzClient(
      new fizz::client::AsyncFizzClient(
          folly::AsyncTransportWrapper::UniquePtr(std::move(asyncSocket)),
          std::move(fizzContext)));

  FizzHandshakeCb fizzCb;
  fizzClient->connect(
      &fizzCb,
      std::move(verifier),
      /*sni=*/folly::Optional<std::string>(host),
      /*pskIdentity=*/host,
      /*echConfigs=*/folly::none,
      fizzTimeout);
  co_await fizzCb.baton;
  if (fizzCb.exception) {
    co_yield folly::coro::co_error(std::move(fizzCb.exception));
  }
  co_return std::move(fizzClient);
}

} // namespace

MoQQmuxClient::MoQQmuxClient(
    std::shared_ptr<MoQExecutor> exec,
    proxygen::URL url,
    std::shared_ptr<fizz::CertificateVerifier> verifier)
    : MoQClientBase(std::move(exec), std::move(url), std::move(verifier)) {}

MoQQmuxClient::MoQQmuxClient(
    std::shared_ptr<MoQExecutor> exec,
    proxygen::URL url,
    SessionFactory sessionFactory,
    std::shared_ptr<fizz::CertificateVerifier> verifier)
    : MoQClientBase(
          std::move(exec),
          std::move(url),
          std::move(sessionFactory),
          std::move(verifier)) {}

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
  auto* evb =
      exec_->getTypedExecutor<MoQFollyExecutorImpl>()->getBackingEventBase();
  folly::SocketAddress addr(
      url_.getHost(), url_.getPort(), /*allowNameLookup=*/true);

  XLOG(DBG1) << "MoQQmuxClient: TCP connect to " << addr.describe();
  auto connectStart = std::chrono::steady_clock::now();

  // QMUX runs over a plain bytestream. We always use Fizz for
  // confidentiality/authentication: TCP-connect, then TLS handshake via
  // AsyncFizzClient, then wrap the encrypted AsyncTransport in coro::Transport.
  // QmuxConnector is transport-agnostic and sees only the encrypted stream.

  auto asyncSocket = co_await folly::coro::co_withExecutor(
      executor, connectTcp(evb, addr, connectTimeout));
  auto tcpElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - connectStart);
  auto fizzTimeout = connectTimeout > tcpElapsed ? connectTimeout - tcpElapsed
                                                 : std::chrono::milliseconds(0);

  auto fizzClient = co_await folly::coro::co_withExecutor(
      executor,
      fizzHandshake(
          std::move(asyncSocket),
          verifier_,
          url_.getHost(),
          alpns,
          fizzTimeout));
  if (auto stdAlpn = fizzClient->getApplicationProtocol(); !stdAlpn.empty()) {
    negotiatedProtocol_ = std::move(stdAlpn);
  }

  auto transport = std::make_unique<folly::coro::Transport>(
      evb, folly::AsyncTransport::UniquePtr(std::move(fizzClient)));

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
          evb,
          proxygen::qmux::WtDir::Client,
          qmuxParamsFromTransportSettings(transportSettings),
          std::move(transport),
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
    folly::SocketAddress /*connectAddr*/,
    std::chrono::milliseconds /*timeoutMs*/,
    std::shared_ptr<fizz::CertificateVerifier> /*verifier*/,
    const std::vector<std::string>& /*alpns*/,
    const quic::TransportSettings& /*transportSettings*/) {
  LOG(FATAL) << "MoQQmuxClient does not use QUIC transport";
  co_return nullptr;
}

} // namespace moxygen
