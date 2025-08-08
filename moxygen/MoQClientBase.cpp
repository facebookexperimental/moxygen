/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <fizz/protocol/CertificateVerifier.h>
#include <quic/client/QuicClientTransport.h>
#include <moxygen/MoQClientBase.h>

// TODO: clean this up.
namespace proxygen {

// This is an insecure certificate verifier and is not meant to be
// used in production. Using it in production would mean that this will
// leave everyone insecure.
class InsecureVerifierDangerousDoNotUseInProduction
    : public fizz::CertificateVerifier {
 public:
  ~InsecureVerifierDangerousDoNotUseInProduction() override = default;

  std::shared_ptr<const folly::AsyncTransportCertificate> verify(
      const std::vector<std::shared_ptr<const fizz::PeerCert>>& certs)
      const override {
    return certs.front();
  }

  std::vector<fizz::Extension> getCertificateRequestExtensions()
      const override {
    return std::vector<fizz::Extension>();
  }
};
} // namespace proxygen

namespace moxygen {
folly::coro::Task<void> MoQClientBase::setupMoQSession(
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler,
    bool v11Plus) noexcept {
  proxygen::WebTransport* wt = nullptr;
  // Establish QUIC connection
  auto quicClient = co_await connectQuic(
      folly::SocketAddress(
          url_.getHost(), url_.getPort(), true), // blocking DNS,
      connect_timeout,
      std::make_shared<
          proxygen::InsecureVerifierDangerousDoNotUseInProduction>(),
      "moq-00");

  // Make WebTransport object
  quicWebTransport_ =
      std::make_shared<proxygen::QuicWebTransport>(std::move(quicClient));
  quicWebTransport_->setHandler(this);
  wt = quicWebTransport_.get();
  co_await completeSetupMoQSession(
      wt,
      url_.getPath(),
      std::move(publishHandler),
      std::move(subscribeHandler),
      v11Plus);
}

folly::coro::Task<ServerSetup> MoQClientBase::completeSetupMoQSession(
    proxygen::WebTransport* wt,
    const folly::Optional<std::string>& pathParam,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler,
    bool v11Plus) {
  //  Create MoQSession and Setup MoQSession parameters
  moqSession_ = std::make_shared<MoQSession>(wt, exec_);
  moqSession_->setPublishHandler(std::move(publishHandler));
  moqSession_->setSubscribeHandler(std::move(subscribeHandler));
  moqSession_->setLogger(logger_);
  moqSession_->start();
  ClientSetup clientSetup = getClientSetup(pathParam, v11Plus);
  if (logger_) {
    logger_->logClientSetup(clientSetup);
  }
  return moqSession_->setup(clientSetup);
}

ClientSetup MoQClientBase::getClientSetup(
    const folly::Optional<std::string>& path,
    bool v11Plus) {
  // Setup MoQSession parameters
  // TODO: maybe let the caller set max subscribes.  Any client that publishes
  // via relay needs to support subscribes.
  const uint32_t kDefaultMaxRequestID = 100;
  const uint32_t kMaxAuthTokenCacheSize = 1024;
  static const std::vector<uint64_t> pre11Versions = {
      kVersionDraft09, kVersionDraft10};
  static const std::vector<uint64_t> post11Versions = {kVersionDraft11};

  ClientSetup clientSetup{
      v11Plus ? post11Versions : pre11Versions,
      {{folly::to_underlying(SetupKey::MAX_REQUEST_ID),
        "",
        kDefaultMaxRequestID,
        {}},
       {folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE),
        "",
        kMaxAuthTokenCacheSize,
        {}}}};

  if (path) {
    clientSetup.params.emplace_back(
        SetupParameter({folly::to_underlying(SetupKey::PATH), *path, 0}));
  }

  return clientSetup;
}

void MoQClientBase::onSessionEnd(folly::Optional<uint32_t> err) {
  if (logger_) {
    logger_->outputLogsToFile();
  }
  if (moqSession_) {
    moqSession_->onSessionEnd(err);
    XLOG(DBG1) << "resetting moqSession_";
    moqSession_.reset();
  }
}

void MoQClientBase::onNewBidiStream(
    proxygen::WebTransport::BidiStreamHandle bidi) {
  XLOG(DBG1) << __func__;
  moqSession_->onNewBidiStream(std::move(bidi));
}

void MoQClientBase::onNewUniStream(
    proxygen::WebTransport::StreamReadHandle* stream) {
  XLOG(DBG1) << __func__;
  moqSession_->onNewUniStream(stream);
}

void MoQClientBase::onDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  moqSession_->onDatagram(std::move(datagram));
}

void MoQClientBase::goaway(const Goaway& goaway) {
  XLOG(DBG1) << __func__;

  moqSession_->goaway(goaway);
}

void MoQClientBase::setLogger(const std::shared_ptr<MLogger>& logger) {
  logger_ = logger;
}

} // namespace moxygen
