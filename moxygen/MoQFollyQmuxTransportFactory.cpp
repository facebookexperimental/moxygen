/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQFollyQmuxTransportFactory.h>

#include <fizz/client/AsyncFizzClient.h>
#include <fizz/client/FizzClientContext.h>
#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/coro/Transport.h>
#include <folly/logging/xlog.h>
#include <proxygen/lib/transport/qmux/FollyQmuxTransport.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

#include <stdexcept>

namespace moxygen {
namespace {

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

class FollyQmuxTransportFactory final : public QmuxTransportFactory {
 public:
  FollyQmuxTransportFactory(
      std::shared_ptr<MoQExecutor> exec,
      std::shared_ptr<fizz::CertificateVerifier> verifier)
      : exec_(std::move(exec)), verifier_(std::move(verifier)) {}

  folly::coro::Task<QmuxTransportConnectResult> createQmuxTransport(
      const proxygen::URL& url,
      std::chrono::milliseconds connectTimeout,
      const std::vector<std::string>& alpns) override {
    auto* follyExecutor = exec_->getTypedExecutor<MoQFollyExecutorImpl>();
    if (!follyExecutor) {
      co_yield folly::coro::co_error(
          std::invalid_argument(
              "Folly QMUX transport requires MoQFollyExecutorImpl"));
    }
    auto* evb = follyExecutor->getBackingEventBase();
    folly::SocketAddress addr(
        url.getHost(), url.getPort(), /*allowNameLookup=*/true);

    XLOG(DBG1) << "MoQQmuxClient: TCP connect to " << addr.describe();
    auto connectStart = std::chrono::steady_clock::now();
    auto currentExecutor = co_await folly::coro::co_current_executor;
    auto asyncSocket = co_await folly::coro::co_withExecutor(
        currentExecutor, connectTcp(evb, addr, connectTimeout));
    auto tcpElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - connectStart);
    auto fizzTimeout = connectTimeout > tcpElapsed
        ? connectTimeout - tcpElapsed
        : std::chrono::milliseconds(0);

    auto fizzClient = co_await folly::coro::co_withExecutor(
        currentExecutor,
        fizzHandshake(
            std::move(asyncSocket),
            verifier_,
            url.getHost(),
            alpns,
            fizzTimeout));

    std::optional<std::string> negotiatedProtocol;
    if (auto alpn = fizzClient->getApplicationProtocol(); !alpn.empty()) {
      negotiatedProtocol = std::move(alpn);
    }
    auto transport = std::make_unique<proxygen::qmux::FollyQmuxTransport>(
        std::make_unique<folly::coro::Transport>(
            evb, folly::AsyncTransport::UniquePtr(std::move(fizzClient))));
    co_return QmuxTransportConnectResult{
        .transport = std::move(transport),
        .negotiatedProtocol = std::move(negotiatedProtocol)};
  }

 private:
  std::shared_ptr<MoQExecutor> exec_;
  std::shared_ptr<fizz::CertificateVerifier> verifier_;
};

} // namespace

std::shared_ptr<QmuxTransportFactory> makeFollyQmuxTransportFactory(
    std::shared_ptr<MoQExecutor> exec,
    std::shared_ptr<fizz::CertificateVerifier> verifier) {
  return std::make_shared<FollyQmuxTransportFactory>(
      std::move(exec), std::move(verifier));
}

} // namespace moxygen
