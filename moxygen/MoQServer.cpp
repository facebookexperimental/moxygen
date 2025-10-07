/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQServer.h"
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include <proxygen/lib/http/session/HQSession.h>
#include <proxygen/lib/http/webtransport/HTTPWebTransport.h>
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

#include <utility>

using namespace quic::samples;
using namespace proxygen;

namespace moxygen {

MoQServer::MoQServer(
    uint16_t port,
    std::string cert,
    std::string key,
    std::string endpoint)
    : MoQServer(
          port,
          quic::samples::createFizzServerContext(
              {"h3", "moq-00"},
              fizz::server::ClientAuthMode::Optional,
              cert,
              key),
          std::move(endpoint)) {}

MoQServer::MoQServer(
    uint16_t port,
    std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
    std::string endpoint)
    : endpoint_(std::move(endpoint)) {
  params_.localAddress.emplace();
  params_.localAddress->setFromLocalPort(port);
  params_.serverThreads = 1;
  params_.txnTimeout = std::chrono::seconds(60);
  auto factory = std::make_unique<HQServerTransportFactory>(
      params_, [this](HTTPMessage*) { return new Handler(*this); }, nullptr);
  factory->addAlpnHandler(
      {"moq-00"},
      [this](
          std::shared_ptr<quic::QuicSocket> quicSocket,
          wangle::ConnectionManager*) {
        createMoQQuicSession(std::move(quicSocket));
      });
  hqServer_ =
      std::make_unique<HQServer>(params_, std::move(factory), fizzContext);
}

void MoQServer::start(std::vector<folly::EventBase*> evbs) {
  hqServer_->start(std::move(evbs));
}

void MoQServer::stop() {
  hqServer_->stop();
}

void MoQServer::rejectNewConnections(bool reject) {
  if (hqServer_) {
    hqServer_->rejectNewConnections(reject);
  }
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
  auto evb = qevb->getTypedEventBase<quic::FollyQuicEventBase>()
                 ->getBackingEventBase();
  auto moqSession = std::make_shared<MoQSession>(
      wt, *this, std::make_unique<MoQFollyExecutorImpl>(evb));
  qWtPtr->setHandler(moqSession.get());
  // the handleClientSession coro this session moqSession
  co_withExecutor(evb, handleClientSession(std::move(moqSession))).start();
}
bool MoQServer::isValidAuthorityFormat(const std::string& authority) {
  // TODO: Implement RFC 3986 authority format validation
  return !authority.empty();
}

bool MoQServer::isSupportedAuthority(const std::string& authority) {
  // TODO: Implement server-side authority support validation
  return true;
}

folly::Expected<folly::Unit, SessionCloseErrorCode>
MoQServer::validateAuthority(
    const ClientSetup& setup,
    uint64_t negotiatedVersion,
    std::shared_ptr<MoQSession>) {
  if (getDraftMajorVersion(negotiatedVersion) >= 14) {
    // Find and validate AUTHORITY
    auto authorityParam = std::find_if(
        setup.params.begin(), setup.params.end(), [](const SetupParameter& p) {
          return p.key == folly::to_underlying(SetupKey::AUTHORITY);
        });

    bool hasAuthority = (authorityParam != setup.params.end());

    if (hasAuthority) {
      std::string authority = authorityParam->asString;

      // MALFORMED_AUTHORITY validation
      if (!isValidAuthorityFormat(authority)) {
        XLOG(ERR) << "Malformed authority format: " << authority;
        return folly::makeUnexpected(
            SessionCloseErrorCode::MALFORMED_AUTHORITY);
      }

      // INVALID_AUTHORITY validation
      if (!isSupportedAuthority(authority)) {
        XLOG(ERR) << "Unsupported authority: " << authority;
        return folly::makeUnexpected(SessionCloseErrorCode::INVALID_AUTHORITY);
      }
    }
  }

  return folly::unit;
}

void MoQServer::setHostId(uint32_t hostId) {
  hqServer_->setHostId(hostId);
}

void MoQServer::setProcessId(quic::ProcessId processId) {
  hqServer_->setProcessId(processId);
}

void MoQServer::setConnectionIdVersion(quic::ConnectionIdVersion version) {
  hqServer_->setConnectionIdVersion(version);
}

void MoQServer::waitUntilInitialized() {
  hqServer_->waitUntilInitialized();
}

void MoQServer::allowBeingTakenOver(const folly::SocketAddress& addr) {
  hqServer_->allowBeingTakenOver(addr);
}

int MoQServer::getTakeoverHandlerSocketFD() const {
  return hqServer_->getTakeoverHandlerSocketFD();
}

std::vector<int> MoQServer::getAllListeningSocketFDs() const {
  return hqServer_->getAllListeningSocketFDs();
}

void MoQServer::setListeningFDs(const std::vector<int>& fds) {
  hqServer_->setListeningFDs(fds);
}

quic::ProcessId MoQServer::getProcessId() const {
  return hqServer_->getProcessId();
}

quic::TakeoverProtocolVersion MoQServer::getTakeoverProtocolVersion() const {
  return hqServer_->getTakeoverProtocolVersion();
}

void MoQServer::startPacketForwarding(const folly::SocketAddress& addr) {
  hqServer_->startPacketForwarding(addr);
}

void MoQServer::pauseRead() {
  hqServer_->pauseRead();
}

folly::Try<ServerSetup> MoQServer::onClientSetup(
    ClientSetup setup,
    std::shared_ptr<MoQSession>) {
  XLOG(INFO) << "ClientSetup";

  uint64_t negotiatedVersion = 0;
  // Iterate over supported versions and set the highest version within the
  // range
  constexpr uint64_t kVersionMin = kVersionDraft11;
  constexpr uint64_t kVersionMax = kVersionDraft14;
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
  std::vector<std::string> supportedProtocols{"moq-00"};
  if (auto wtAvailableProtocols =
          HTTPWebTransport::getWTAvailableProtocols(*req)) {
    if (auto wtProtocol = HTTPWebTransport::negotiateWTProtocol(
            wtAvailableProtocols.value(), supportedProtocols)) {
      HTTPWebTransport::setWTProtocol(resp, wtProtocol.value());
    } else {
      VLOG(4) << "Failed to negotiate WebTransport protocol";
      resp.setStatusCode(400);
    }
  }
  txn_->sendHeaders(resp);
  auto wt = txn_->getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Failed to get WebTransport";
    txn_->sendAbort();
    return;
  }
  auto evb = folly::EventBaseManager::get()->getEventBase();
  clientSession_ = std::make_shared<MoQSession>(
      wt, server_, std::make_unique<MoQFollyExecutorImpl>(evb));
  co_withExecutor(evb, server_.handleClientSession(clientSession_)).start();
}

void MoQServer::setLogger(std::shared_ptr<MLogger> logger) {
  logger_ = std::move(logger);
}

void MoQServer::setQuicStatsFactory(
    std::unique_ptr<quic::QuicTransportStatsCallbackFactory> factory) {
  if (hqServer_) {
    hqServer_->setStatsFactory(std::move(factory));
  }
}

} // namespace moxygen
