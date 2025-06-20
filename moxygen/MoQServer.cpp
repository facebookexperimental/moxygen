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
  auto moqSession = std::make_shared<MoQSession>(wt, *this, evb);
  qWtPtr->setHandler(moqSession.get());
  // the handleClientSession coro this session moqSession
  handleClientSession(std::move(moqSession)).scheduleOn(evb).start();
}

folly::Try<ServerSetup> MoQServer::onClientSetup(ClientSetup setup) {
  XLOG(INFO) << "ClientSetup";
  uint64_t negotiatedVersion = 0;
  // Pick the highest available version of 8, 9, and 10
  folly::F14FastSet<uint64_t> negotiatedVersionSet(
      setup.supportedVersions.begin(), setup.supportedVersions.end());
  if (negotiatedVersionSet.contains(kVersionDraft10)) {
    negotiatedVersion = kVersionDraft10;
  } else if (negotiatedVersionSet.contains(kVersionDraft09)) {
    negotiatedVersion = kVersionDraft09;
  } else if (negotiatedVersionSet.contains(kVersionDraft08)) {
    negotiatedVersion = kVersionDraft08;
  } else {
    return folly::Try<ServerSetup>(
        std::runtime_error("Client does not support draft-09 or draft-10"));
  }

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
  auto evb = folly::EventBaseManager::get()->getEventBase();
  clientSession_ = std::make_shared<MoQSession>(wt, server_, evb);

  server_.handleClientSession(clientSession_).scheduleOn(evb).start();
}

void MoQServer::setLogger(std::shared_ptr<MLogger> logger) {
  logger_ = std::move(logger);
}

} // namespace moxygen
