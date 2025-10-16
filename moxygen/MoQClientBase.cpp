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
/*static*/
bool MoQClientBase::shouldSendAuthorityParam(
    const std::vector<uint64_t>& supportedVersions) {
  for (const auto& version : supportedVersions) {
    if (getDraftMajorVersion(version) >= 14) {
      return true;
    }
  }
  return false;
}

folly::coro::Task<void> MoQClientBase::setupMoQSession(
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler,
    const quic::TransportSettings& transportSettings) noexcept {
  proxygen::WebTransport* wt = nullptr;
  // Establish QUIC connection
  auto quicClient = co_await connectQuic(
      folly::SocketAddress(
          url_.getHost(), url_.getPort(), true), // blocking DNS,
      connect_timeout,
      std::make_shared<
          proxygen::InsecureVerifierDangerousDoNotUseInProduction>(),
      "moq-00",
      transportSettings);

  // Make WebTransport object
  quicWebTransport_ =
      std::make_shared<proxygen::QuicWebTransport>(std::move(quicClient));
  quicWebTransport_->setHandler(this);
  wt = quicWebTransport_.get();
  co_await completeSetupMoQSession(
      wt,
      url_.getPath(),
      std::move(publishHandler),
      std::move(subscribeHandler));
}

folly::coro::Task<ServerSetup> MoQClientBase::completeSetupMoQSession(
    proxygen::WebTransport* wt,
    const folly::Optional<std::string>& pathParam,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler) {
  //  Create MoQSession and Setup MoQSession parameters
  moqSession_ =
      createSession(folly::MaybeManagedPtr<proxygen::WebTransport>(wt));
  moqSession_->setPublishHandler(std::move(publishHandler));
  moqSession_->setSubscribeHandler(std::move(subscribeHandler));
  moqSession_->setLogger(logger_);
  moqSession_->start();
  ClientSetup clientSetup = getClientSetup(pathParam);
  if (logger_) {
    logger_->logClientSetup(clientSetup);
  }
  return moqSession_->setup(clientSetup);
}

ClientSetup MoQClientBase::getClientSetup(
    const folly::Optional<std::string>& path) {
  // Setup MoQSession parameters
  // TODO: maybe let the caller set max subscribes.  Any client that publishes
  // via relay needs to support subscribes.
  const uint32_t kDefaultMaxRequestID = 100;
  const uint32_t kMaxAuthTokenCacheSize = 1024;
  static const std::vector<uint64_t> post11Versions = {
      kVersionDraft11, kVersionDraft12, kVersionDraft13, kVersionDraft14};

  ClientSetup clientSetup{
      post11Versions,
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
        SetupParameter({folly::to_underlying(SetupKey::PATH), *path, 0, {}}));
  }

  if (shouldSendAuthorityParam(clientSetup.supportedVersions)) {
    // Add AUTHORITY parameter for Direct QUIC with moqt:// scheme only
    if (path.has_value() && url_.getScheme() == "moqt") {
      // Extract authority from URI as per RFC 3986
      std::string authority = url_.getHost();
      if (url_.getPort() != 0 && url_.getPort() != 443) {
        authority += ":" + std::to_string(url_.getPort());
      }

      clientSetup.params.emplace_back(SetupParameter(
          {folly::to_underlying(SetupKey::AUTHORITY), authority, 0}));
    }
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
  if (!moqSession_) {
    XLOG(DBG1) << "onNewBidiStream after session reset; ignoring";
    return;
  }
  moqSession_->onNewBidiStream(std::move(bidi));
}

void MoQClientBase::onNewUniStream(
    proxygen::WebTransport::StreamReadHandle* stream) {
  XLOG(DBG1) << __func__;
  if (!moqSession_) {
    XLOG(DBG1) << "onNewUniStream after session reset; ignoring";
    return;
  }
  moqSession_->onNewUniStream(stream);
}

void MoQClientBase::onDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  if (!moqSession_) {
    XLOG(DBG1) << "onDatagram after session reset; ignoring";
    return;
  }
  moqSession_->onDatagram(std::move(datagram));
}

void MoQClientBase::goaway(const Goaway& goaway) {
  XLOG(DBG1) << __func__;
  if (!moqSession_) {
    return;
  }
  moqSession_->goaway(goaway);
}

void MoQClientBase::setLogger(const std::shared_ptr<MLogger>& logger) {
  logger_ = logger;
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
