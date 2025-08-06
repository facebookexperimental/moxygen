/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQServer.h"
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>

#include <utility>

using namespace quic::samples;
using namespace proxygen;

namespace moxygen {

MoQServer::MoQServer(
    uint16_t port,
    std::string cert,
    std::string key,
    std::string endpoint)
    : endpoint_(std::move(endpoint)) {
  params_.localAddress.emplace();
  params_.localAddress->setFromLocalPort(port);
  params_.serverThreads = 1;
  params_.certificateFilePath = std::move(cert);
  params_.keyFilePath = std::move(key);
  params_.txnTimeout = std::chrono::seconds(60);
  params_.supportedAlpns = {"h3", "moq-00"};
  auto factory = std::make_unique<HQServerTransportFactory>(
      params_, [this](HTTPMessage*) { return new Handler(*this); }, nullptr);
  factory->addAlpnHandler(
      {"moq-00"},
      [this](
          std::shared_ptr<quic::QuicSocket> quicSocket,
          wangle::ConnectionManager*) {
        createMoQQuicSession(std::move(quicSocket));
      });
  hqServer_ = std::make_unique<HQServer>(params_, std::move(factory));
  hqServer_->start();
}

void MoQServer::stop() {
  hqServer_->stop();
}

void MoQServer::createMoQQuicSession(
    std::shared_ptr<quic::QuicSocket> quicSocket) {
  auto qevb = quicSocket->getEventBase();
  auto ts = quicSocket->getTransportSettings();
  // TODO make this configurable, also have a shared pacing timer per thread.
  ts.defaultCongestionController = quic::CongestionControlType::Copa;
  ts.copaDeltaParam = 0.05;
  ts.pacingEnabled = true;
  ts.experimentalPacer = true;
  auto quicWebTransport =
      std::make_shared<proxygen::QuicWebTransport>(std::move(quicSocket));
  auto qWtPtr = quicWebTransport.get();
  std::shared_ptr<proxygen::WebTransport> wt(std::move(quicWebTransport));
  folly::EventBase* evb{nullptr};
  if (qevb) {
    evb = qevb->getTypedEventBase<quic::FollyQuicEventBase>()
              ->getBackingEventBase();
  }
  if (!moqEvb_) {
    moqEvb_ = std::make_unique<MoQFollyExecutorImpl>(evb);
  }
  auto moqSession = std::make_shared<MoQSession>(wt, *this, moqEvb_.get());
  qWtPtr->setHandler(moqSession.get());
  // the handleClientSession coro this session moqSession
  co_withExecutor(moqEvb_.get(), handleClientSession(std::move(moqSession)))
      .start();
}

folly::Try<ServerSetup> MoQServer::onClientSetup(
    ClientSetup setup,
    std::shared_ptr<MoQSession>) {
  XLOG(INFO) << "ClientSetup";
  uint64_t negotiatedVersion = 0;
  // Iterate over supported versions and set the highest version within the
  // range
  constexpr uint64_t kVersionMin = kVersionDraft08;
  constexpr uint64_t kVersionMax = kVersionDraft11;
  uint64_t highestVersion = 0;
  for (const auto& version : setup.supportedVersions) {
    if (version >= kVersionMin && version <= kVersionMax) {
      highestVersion = std::max(highestVersion, version);
    }
  }
  if (highestVersion == 0) {
    return folly::Try<ServerSetup>(std::runtime_error(
        "Client does not support versions in the range " +
        std::to_string(getDraftMajorVersion(kVersionMin)) + " to " +
        std::to_string(getDraftMajorVersion(kVersionMax))));
  }
  negotiatedVersion = highestVersion;

  // TODO: Make the default MAX_REQUEST_ID configurable and
  // take in the value from ClientSetup
  static constexpr size_t kDefaultMaxRequestID = 100;
  static constexpr size_t kMaxAuthTokenCacheSize = 1024;
  ServerSetup serverSetup = ServerSetup({
      negotiatedVersion,
      {{folly::to_underlying(SetupKey::MAX_REQUEST_ID),
        "",
        kDefaultMaxRequestID,
        {}},
       {folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE),
        "",
        kMaxAuthTokenCacheSize,
        {}}},
  });

  // Log Server Setup
  if (logger_) {
    logger_->logServerSetup(serverSetup);
  }

  return folly::Try<ServerSetup>(serverSetup);
}

folly::coro::Task<void> MoQServer::handleClientSession(
    std::shared_ptr<MoQSession> clientSession) {
  onNewSession(clientSession);
  clientSession->start();

  // The clientSession will cancel this token when the app calls close() or
  // the underlying transport invokes onSessionEnd
  folly::coro::Baton baton;
  folly::CancellationCallback cb(
      clientSession->getCancelToken(), [&baton] { baton.post(); });
  co_await baton;
  terminateClientSession(std::move(clientSession));
}

void MoQServer::Handler::onHeadersComplete(
    std::unique_ptr<HTTPMessage> req) noexcept {
  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);

  if (req->getPathAsStringPiece() != server_.getEndpoint()) {
    XLOG(INFO) << req->getPathAsStringPiece();
    req->dumpMessage(0);
    resp.setStatusCode(404);
    txn_->sendHeadersWithEOM(resp);
    return;
  }
  if (req->getMethod() != HTTPMethod::CONNECT || !req->getUpgradeProtocol() ||
      *req->getUpgradeProtocol() != std::string("webtransport")) {
    resp.setStatusCode(400);
    txn_->sendHeadersWithEOM(resp);
    return;
  }
  resp.setStatusCode(200);
  resp.getHeaders().add("sec-webtransport-http3-draft", "draft02");
  txn_->sendHeaders(resp);
  auto wt = txn_->getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Failed to get WebTransport";
    txn_->sendAbort();
    return;
  }
  if (!server_.moqEvb_) {
    auto evb = folly::EventBaseManager::get()->getEventBase();
    server_.moqEvb_ = std::make_unique<MoQFollyExecutorImpl>(evb);
  }
  clientSession_ =
      std::make_shared<MoQSession>(wt, server_, server_.moqEvb_.get());

  co_withExecutor(
      server_.moqEvb_.get(), server_.handleClientSession(clientSession_))
      .start();
}

void MoQServer::setLogger(std::shared_ptr<MLogger> logger) {
  logger_ = std::move(logger);
}

} // namespace moxygen
