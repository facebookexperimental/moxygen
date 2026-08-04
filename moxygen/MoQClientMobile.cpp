/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQClientMobile.h>

#include <folly/OperationCancelled.h>
#include <folly/coro/Error.h>
#include <folly/coro/Promise.h>
#include <folly/coro/Timeout.h>

#include <stdexcept>

// libev and libevent both define these macros; libevent arrives via the folly
// and MoQClientMobile.h headers above. Undef them before the libev-backed
// headers below so ev.h can define them cleanly (see MoQTextClientMobile.cpp).
#undef EV_READ
#undef EV_WRITE
#undef EV_TIMEOUT
#undef EV_SIGNAL
#undef EVLOOP_NONBLOCK

#include <quic/common/udpsocket/LibevQuicAsyncUDPSocket.h>
#include <quic/fizz/client/handshake/FizzClientQuicHandshakeContext.h>
#include <moxygen/events/MoQLibevExecutorImpl.h>

namespace {

class QuicConnectCB : public quic::QuicSocket::ConnectionSetupCallback {
 public:
  QuicConnectCB(
      std::shared_ptr<quic::QuicClientTransport> quicClient,
      folly::CancellationToken cancellationToken)
      : quicClient_(std::move(quicClient)),
        cancellationToken_(std::move(cancellationToken)) {
    auto contract = folly::coro::makePromiseContract<folly::Unit>();
    promise_ = std::move(contract.first);
    future = std::move(contract.second);
  }

  folly::coro::Future<folly::Unit> future;
  folly::exception_wrapper quicException;

 private:
  void quicConnectErr(folly::exception_wrapper ex) noexcept {
    quicException = std::move(ex);
    promise_.setValue(folly::unit);
  }
  void onConnectionSetupError(quic::QuicError error) noexcept override {
    switch (error.code.type()) {
      case quic::QuicErrorCode::Type::ApplicationErrorCode:
        quicConnectErr(
            quic::QuicApplicationException(
                error.message, *error.code.asApplicationErrorCode()));
        break;
      case quic::QuicErrorCode::Type::LocalErrorCode:
        quicConnectErr(
            quic::QuicInternalException(
                error.message, *error.code.asLocalErrorCode()));
        break;
      case quic::QuicErrorCode::Type::TransportErrorCode:
        quicConnectErr(
            quic::QuicTransportException(
                error.message, *error.code.asTransportErrorCode()));
        break;
    }
  }
  void onReplaySafe() noexcept override {}
  void onTransportReady() noexcept override {
    if (cancellationToken_.isCancellationRequested()) {
      quicConnectErr(folly::OperationCancelled{});
      return;
    }
    promise_.setValue(folly::unit);
  }
  std::shared_ptr<quic::QuicClientTransport> quicClient_;
  folly::CancellationToken cancellationToken_;
  folly::coro::Promise<folly::Unit> promise_;
};

} // namespace

namespace moxygen {

MoQClientMobile::MoQClientMobile(
    std::shared_ptr<MoQLibevExecutorImpl> moqEvb,
    proxygen::URL url,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    bool useQuicWtSession,
    std::shared_ptr<MoQQuicAddressResolver> addressResolver)
    : MoQClientBase(
          moqEvb,
          std::move(url),
          std::move(verifier),
          useQuicWtSession),
      moqlibevEvb_(std::move(moqEvb)),
      addressResolver_(std::move(addressResolver)) {
  if (!addressResolver_) {
    throw std::invalid_argument("MoQClientMobile requires an address resolver");
  }
}

folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
MoQClientMobile::connectQuic(
    std::chrono::milliseconds timeoutMs,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    const std::vector<std::string>& alpns,
    const quic::TransportSettings& transportSettings) {
  const auto connectDeadline = std::chrono::steady_clock::now() + timeoutMs;
  // DNS and QUIC share the caller's total connection budget. A DNS lookup
  // that consumes it leaves no time for the handshake.
  auto resolveResult = co_await folly::coro::co_awaitTry(
      folly::coro::timeout(
          addressResolver_->resolveAddress(
              url_.getHost(), url_.getPort(), timeoutMs),
          timeoutMs));
  if (resolveResult.hasException<folly::OperationCancelled>()) {
    // Cancellation is a stopped completion, not a connection failure.
    co_yield folly::coro::co_stopped_may_throw;
  }
  if (resolveResult.hasException()) {
    co_yield folly::coro::co_error(std::move(resolveResult).exception());
  }
  auto connectAddr = std::move(resolveResult).value();

  if (std::chrono::steady_clock::now() >= connectDeadline) {
    co_yield folly::coro::co_error(
        quic::QuicInternalException(
            "Connection timed out during DNS resolution",
            quic::LocalErrorCode::CONNECT_FAILED));
  }

  auto sock = std::make_unique<quic::LibevQuicAsyncUDPSocket>(moqlibevEvb_);
  // Set UDP socket buffer sizes to 1 MB
  constexpr int kUdpBufferSize = 1024 * 1024; // 1 MB
  sock->setRcvBuf(kUdpBufferSize);
  sock->setSndBuf(kUdpBufferSize);
  auto fizzContext = std::make_shared<fizz::client::FizzClientContext>();
  fizzContext->setSupportedAlpns(alpns);
  auto quicClient = quic::QuicClientTransport::newClient(
      moqlibevEvb_,
      std::move(sock),
      quic::FizzClientQuicHandshakeContext::Builder()
          .setFizzClientContext(fizzContext)
          .setCertificateVerifier(std::move(verifier))
          .build(),
      /*connectionIdSize=*/0);
  quicClient->setTransportSettings(transportSettings);
  quicClient->addNewPeerAddress(std::move(connectAddr));
  quicClient->setSupportedVersions({quic::QuicVersion::QUIC_V1});
  quicClient->setHostname(url_.getHost());
  folly::CancellationToken cancellationToken =
      co_await folly::coro::co_current_cancellation_token;
  QuicConnectCB cb(quicClient, std::move(cancellationToken));
  quicClient->start(&cb, nullptr);
  const auto quicStartedAt = std::chrono::steady_clock::now();
  // Preserve a zero budget so the normal timeout path still detaches the
  // callback and closes the transport.
  const auto remainingTimeout = connectDeadline > quicStartedAt
      ? std::chrono::ceil<std::chrono::microseconds>(
            connectDeadline - quicStartedAt)
      : std::chrono::microseconds::zero();
  auto res = co_await co_awaitTry(
      folly::coro::timeout(std::move(cb.future), remainingTimeout));
  quicClient->setConnectionSetupCallback(nullptr);
  if (res.hasException()) {
    const bool cancelled = res.hasException<folly::OperationCancelled>();
    quic::ApplicationErrorCode err(0);
    auto errString = folly::exceptionStr(res.exception()).toStdString();
    quicClient->close(
        quic::QuicError(quic::QuicErrorCode(err), std::string(errString)));
    if (cancelled) {
      co_yield folly::coro::co_stopped_may_throw;
    }
    co_yield folly::coro::co_error(
        quic::QuicInternalException(
            std::move(errString), quic::LocalErrorCode::CONNECT_FAILED));
  }
  if (cb.quicException) {
    co_yield folly::coro::co_error(std::move(cb.quicException));
  }
  co_return quicClient;
}

} // namespace moxygen
