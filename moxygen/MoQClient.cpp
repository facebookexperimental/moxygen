/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQClient.h>

#include <moxygen/util/QuicConnector.h>

#include <quic/api/QuicSocket.h>
#include <quic/client/QuicClientTransport.h>

#include <fizz/protocol/CertificateVerifier.h>

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
folly::coro::Task<void> MoQClient::setupMoQSession(
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler) noexcept {
  proxygen::WebTransport* wt = nullptr;
  // Establish QUIC connection
  auto quicClient = co_await QuicConnector::connectQuic(
      evb_,
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
      std::move(subscribeHandler));
}

folly::coro::Task<ServerSetup> MoQClient::completeSetupMoQSession(
    proxygen::WebTransport* wt,
    folly::Optional<std::string> pathParam,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler) {
  //  Create MoQSession and Setup MoQSession parameters
  moqSession_ = std::make_shared<MoQSession>(wt, evb_);
  moqSession_->setPublishHandler(std::move(publishHandler));
  moqSession_->setSubscribeHandler(std::move(subscribeHandler));
  moqSession_->start();
  return moqSession_->setup(getClientSetup(pathParam));
}

ClientSetup MoQClient::getClientSetup(
    const folly::Optional<std::string>& path) {
  // Setup MoQSession parameters
  // TODO: maybe let the caller set max subscribes.  Any client that publishes
  // via relay needs to support subscribes.
  const uint32_t kDefaultMaxSubscribeId = 100;
  ClientSetup clientSetup{
      {kVersionDraft08, kVersionDraft09, kVersionDraft10},
      {{folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
        "",
        kDefaultMaxSubscribeId}}};
  if (path) {
    clientSetup.params.emplace_back(
        SetupParameter({folly::to_underlying(SetupKey::PATH), *path, 0}));
  }
  return clientSetup;
}

void MoQClient::onSessionEnd(folly::Optional<uint32_t> err) {
  if (moqSession_) {
    moqSession_->onSessionEnd(err);
    XLOG(DBG1) << "resetting moqSession_";
    moqSession_.reset();
  }
}

void MoQClient::onNewBidiStream(proxygen::WebTransport::BidiStreamHandle bidi) {
  XLOG(DBG1) << __func__;
  moqSession_->onNewBidiStream(std::move(bidi));
}

void MoQClient::onNewUniStream(
    proxygen::WebTransport::StreamReadHandle* stream) {
  XLOG(DBG1) << __func__;
  moqSession_->onNewUniStream(stream);
}

void MoQClient::onDatagram(std::unique_ptr<folly::IOBuf> datagram) {
  moqSession_->onDatagram(std::move(datagram));
}

} // namespace moxygen
