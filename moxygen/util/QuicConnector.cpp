/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include <moxygen/util/QuicConnector.h>

#include <folly/coro/Promise.h>
#include <folly/coro/Timeout.h>
#include <folly/futures/ThreadWheelTimekeeper.h>
#include <quic/client/QuicClientTransport.h>
#include <quic/common/events/FollyQuicEventBase.h>
#include <quic/common/udpsocket/FollyQuicAsyncUDPSocket.h>
#include <quic/fizz/client/handshake/FizzClientQuicHandshakeContext.h>

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
      quicConnectErr(
          quic::QuicTransportException(
              "Connection has been cancelled",
              quic::TransportErrorCode::INTERNAL_ERROR));
    }
    promise_.setValue(folly::unit);
  }
  std::shared_ptr<quic::QuicClientTransport> quicClient_;
  folly::CancellationToken cancellationToken_;
  folly::coro::Promise<folly::Unit> promise_;
};

} // namespace

namespace moxygen {

folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
QuicConnector::connectQuic(
    folly::EventBase* eventBase,
    folly::SocketAddress connectAddr,
    std::chrono::milliseconds timeoutMs,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    const std::vector<std::string>& alpns,
    const quic::TransportSettings& transportSettings) {
  auto qEvb = std::make_shared<quic::FollyQuicEventBase>(eventBase);
  auto sock = std::make_unique<quic::FollyQuicAsyncUDPSocket>(qEvb);
  // Set UDP socket buffer sizes to 1 MB
  constexpr int kUdpBufferSize = 1024 * 1024; // 1 MB
  sock->setRcvBuf(kUdpBufferSize);
  sock->setSndBuf(kUdpBufferSize);
  auto fizzContext = std::make_shared<fizz::client::FizzClientContext>();
  fizzContext->setSupportedAlpns(alpns);
  auto quicClient = quic::QuicClientTransport::newClient(
      std::move(qEvb),
      std::move(sock),
      quic::FizzClientQuicHandshakeContext::Builder()
          .setFizzClientContext(fizzContext)
          .setCertificateVerifier(std::move(verifier))
          .build(),
      /*connectionIdSize=*/0);
  // Make a copy of transportSettings and enable datagram support
  auto ts = transportSettings;
  ts.maxServerRecvPacketsPerLoop = 10;
  ts.datagramConfig.enabled = true;

  quicClient->setCongestionControllerFactory(
      std::make_shared<quic::DefaultCongestionControllerFactory>());
  quicClient->setTransportSettings(ts);
  quicClient->addNewPeerAddress(connectAddr);
  quicClient->setSupportedVersions({quic::QuicVersion::QUIC_V1});
  folly::CancellationToken cancellationToken =
      co_await folly::coro::co_current_cancellation_token;
  QuicConnectCB cb(quicClient, std::move(cancellationToken));
  quicClient->start(&cb, nullptr);
  folly::EventBaseThreadTimekeeper tk(*eventBase);
  auto res = co_await co_awaitTry(
      folly::coro::timeout(std::move(cb.future), timeoutMs, &tk));
  quicClient->setConnectionSetupCallback(nullptr);
  if (res.hasException()) {
    quic::ApplicationErrorCode err(0);
    auto errString = folly::exceptionStr(res.exception()).toStdString();
    quicClient->close(
        quic::QuicError(quic::QuicErrorCode(err), std::string(errString)));
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
