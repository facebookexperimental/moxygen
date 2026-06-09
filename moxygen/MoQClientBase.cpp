/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fizz/protocol/CertificateVerifier.h>
#include <folly/String.h>
#include <folly/coro/Error.h>
#include <quic/client/QuicClientTransport.h>
#include <quic/common/address/QuicSocketAddressBridge.h>
#include <moxygen/MoQClientBase.h>

#include <utility>

namespace moxygen {

folly::coro::Task<void> MoQClientBase::connectAndSendSetup(
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler,
    const quic::TransportSettings& transportSettings,
    const std::vector<std::string>& alpns) {
  std::vector<std::string> alpn =
      alpns.empty() ? getDefaultMoqtProtocols(false) : alpns;
  XLOG(DBG1) << "MoQClientBase: QUIC ALPNs: " << folly::join(", ", alpn);

  auto quicConnectStart = std::chrono::steady_clock::now();

  // Establish QUIC connection with multiple ALPN options
  auto quicClient = co_await connectQuic(
      folly::SocketAddress(
          url_.getHost(), url_.getPort(), true), // blocking DNS,
      connect_timeout,
      verifier_,
      alpn,
      transportSettings);

  transportConnectTime_ = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - quicConnectStart);

  if (logger_) {
    if (auto scid = quicClient->getClientConnectionId()) {
      logger_->setSrcCid(*scid);
    }
    if (auto dcid = quicClient->getServerConnectionId()) {
      logger_->setDcid(*dcid);
    }
    logger_->setLocalAddress(
        quic::toFollySocketAddress(quicClient->getLocalAddress()));
    logger_->setPeerAddress(
        quic::toFollySocketAddress(quicClient->getPeerAddress()));
  }

  // Detect negotiated ALPN before wrapping the socket
  auto stdAlpn = quicClient->getAppProtocol();
  if (stdAlpn) {
    negotiatedProtocol_ = *stdAlpn;
    XLOG(DBG1) << "Client: Negotiated ALPN: " << *negotiatedProtocol_;
  }

  // Make WebTransport object
  quicSocket_ = quicClient.get();
  quicWebTransport_ =
      std::make_shared<proxygen::QuicWebTransport>(std::move(quicClient));
  auto* wt = quicWebTransport_.get();

  completeSetupMoQSession(
      wt,
      url_.getPath(),
      std::move(publishHandler),
      std::move(subscribeHandler));
}

folly::coro::Task<Setup> MoQClientBase::awaitSetupComplete() {
  auto moqHandshakeStart = std::chrono::steady_clock::now();

  auto result =
      co_await folly::coro::co_awaitTry(moqSession_->awaitPeerSetup());

  moqHandshakeTime_ = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - moqHandshakeStart);

  if (result.hasException()) {
    co_yield folly::coro::co_error(result.exception());
  }

  if (quicSocket_) {
    auto transportInfo = quicSocket_->getTransportInfo();
    XLOG(DBG1) << "MoQ setup complete, usedZeroRtt="
               << transportInfo.usedZeroRtt;
  }

  // Update early data handler with server's params for future 0-RTT
  if (earlyDataHandler_ && result.hasValue()) {
    uint64_t serverMaxRequestID = 0;
    uint64_t serverMaxAuthTokenCacheSize = 0;
    for (const auto& param : result->params) {
      if (param.key == folly::to_underlying(SetupKey::MAX_REQUEST_ID)) {
        serverMaxRequestID = param.asUint64;
      } else if (
          param.key ==
          folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE)) {
        serverMaxAuthTokenCacheSize = param.asUint64;
      }
    }
    earlyDataHandler_->setCurrentParams(
        serverMaxRequestID, serverMaxAuthTokenCacheSize);
  }

  co_return std::move(*result);
}

folly::coro::Task<void> MoQClientBase::setupMoQSession(
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler,
    const quic::TransportSettings& transportSettings,
    const std::vector<std::string>& alpns) {
  co_await connectAndSendSetup(
      connect_timeout,
      transaction_timeout,
      std::move(publishHandler),
      std::move(subscribeHandler),
      transportSettings,
      alpns);
  co_await awaitSetupComplete();
}

void MoQClientBase::completeSetupMoQSession(
    proxygen::WebTransport* wt,
    const std::optional<std::string>& pathParam,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler) {
  //  Create MoQSession and Setup MoQSession parameters
  moqSession_ =
      createSession(folly::MaybeManagedPtr<proxygen::WebTransport>(wt));
  if (quicWebTransport_) {
    quicWebTransport_->setHandler(moqSession_.get());
  }
  moqSession_->setLogger(logger_);

  moqSession_->setPath(url_.getPath());
  moqSession_->setAuthority(url_.getHostAndPortOmitDefault());

  // Configure session based on negotiated ALPN
  // If there is no ALPN negotiation, the negotiation will be done in the
  // Setup messages.
  if (negotiatedProtocol_) {
    moqSession_->validateAndSetVersionFromAlpn(*negotiatedProtocol_);
  }

  moqSession_->setPublishHandler(std::move(publishHandler));
  moqSession_->setSubscribeHandler(std::move(subscribeHandler));
  moqSession_->setLogger(logger_);
  moqSession_->start();
  ClientSetup clientSetup = getClientSetup(pathParam);
  if (logger_) {
    logger_->logClientSetup(
        clientSetup,
        moqSession_->getNegotiatedVersion().value_or(kVersionDraft14));
  }
  moqSession_->sendSetup(clientSetup);
}

Setup MoQClientBase::getClientSetup(const std::optional<std::string>& path) {
  // Setup MoQSession parameters
  // TODO: maybe let the caller set max subscribes.  Any client that publishes
  // via relay needs to support subscribes.
  const uint32_t kDefaultMaxRequestID = 100;
  const uint32_t kMaxAuthTokenCacheSize = 1024;

  Setup clientSetup;
  clientSetup.params.insertParam(Parameter(
      folly::to_underlying(SetupKey::MAX_REQUEST_ID), kDefaultMaxRequestID));
  clientSetup.params.insertParam(Parameter(
      folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE),
      kMaxAuthTokenCacheSize));

  if (path) {
    clientSetup.params.insertParam(
        SetupParameter(folly::to_underlying(SetupKey::PATH), *path));
  }

  // Add AUTHORITY parameter for Direct QUIC with moqt:// scheme only
  if (path.has_value() && url_.getScheme() == "moqt") {
    // Extract authority from URI as per RFC 3986
    std::string authority = url_.getHost();
    if (url_.getPort() != 0 && url_.getPort() != 443) {
      authority += ":" + std::to_string(url_.getPort());
    }

    clientSetup.params.insertParam(
        SetupParameter(folly::to_underlying(SetupKey::AUTHORITY), authority));
  }

  return clientSetup;
}

void MoQClientBase::goaway(const Goaway& goaway) {
  XLOG(DBG1) << __func__;
  if (moqSession_) {
    moqSession_->goaway(goaway);
  }
}

void MoQClientBase::setLogger(const std::shared_ptr<MLogger>& logger) {
  logger_ = logger;
  if (moqSession_) {
    moqSession_->setLogger(logger_);
  }
}

MoQClientBase::SessionFactory MoQClientBase::defaultSessionFactory() {
  static SessionFactory factory =
      [](folly::MaybeManagedPtr<proxygen::WebTransport> wt,
         std::shared_ptr<MoQExecutor> exec) {
        return std::make_shared<MoQSession>(std::move(wt), std::move(exec));
      };
  return factory;
}

std::shared_ptr<MoQSession> MoQClientBase::createSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt) {
  return sessionFactory_(std::move(wt), exec_);
}

} // namespace moxygen
