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
    std::string cert,
    std::string key,
    std::string endpoint,
    std::optional<quic::TransportSettings> transportSettings)
    : MoQServer(
          quic::samples::createFizzServerContext(
              []() {
                std::vector<std::string> alpns = {"h3"};
                auto moqt = getDefaultMoqtProtocols(false);
                alpns.insert(alpns.end(), moqt.begin(), moqt.end());
                return alpns;
              }(),
              fizz::server::ClientAuthMode::Optional,
              cert,
              key),
          std::move(endpoint),
          std::move(transportSettings)) {}

MoQServer::MoQServer(
    std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
    std::string endpoint,
    std::optional<quic::TransportSettings> transportSettings)
    : MoQServerBase(std::move(endpoint)), fizzContext_(std::move(fizzContext)) {
  params_.serverThreads = 1;
  params_.txnTimeout = std::chrono::seconds(60);
  if (transportSettings) {
    params_.transportSettings = *transportSettings;
  } else {
    // Sensible default values
    params_.transportSettings.defaultCongestionController =
        quic::CongestionControlType::Copa;
    params_.transportSettings.copaDeltaParam = 0.05;
    params_.transportSettings.pacingEnabled = true;
    params_.transportSettings.maxCwndInMss = quic::kLargeMaxCwndInMss;
    params_.transportSettings.batchingMode =
        quic::QuicBatchingMode::BATCHING_MODE_GSO;
    params_.transportSettings.maxBatchSize = 48;
    params_.transportSettings.dataPathType =
        quic::DataPathType::ContinuousMemory;
    params_.transportSettings.maxServerRecvPacketsPerLoop = 10;
    params_.transportSettings.writeConnectionDataPacketsLimit = 48;
    params_.transportSettings.advertisedInitialConnectionFlowControlWindow =
        1024 * 1024;
    params_.transportSettings
        .advertisedInitialBidiLocalStreamFlowControlWindow = 1024 * 1024;
    params_.transportSettings
        .advertisedInitialBidiRemoteStreamFlowControlWindow = 1024 * 1024;
    params_.transportSettings.advertisedInitialUniStreamFlowControlWindow =
        1024 * 1024;
  }

  // UDP socket buffer sizes
  constexpr size_t kUdpBufferSize = 1024 * 1024; // 1 MB
  params_.udpSendBufferSize = kUdpBufferSize;
  params_.udpRecvBufferSize = kUdpBufferSize;

  factory_ = std::make_unique<HQServerTransportFactory>(
      params_, [this](HTTPMessage*) { return new Handler(*this); }, nullptr);

  // Register all handlers
  registerAlpnHandler(getDefaultMoqtProtocols(true));

  hqServer_ =
      std::make_unique<HQServer>(params_, std::move(factory_), fizzContext_);
}

void MoQServer::registerAlpnHandler(const std::vector<std::string>& alpns) {
  if (!factory_) {
    XLOG(INFO) << "Cannot register ALPN handler: factory not initialized";
    return;
  }

  if (hqServer_) {
    XLOG(INFO) << "Cannot register ALPN handler: server already started";
    return;
  }

  factory_->addAlpnHandler(
      alpns,
      [this](
          std::shared_ptr<quic::QuicSocket> quicSocket,
          wangle::ConnectionManager*) {
        createMoQQuicSession(std::move(quicSocket));
      });
}

void MoQServer::start(
    const folly::SocketAddress& addr,
    std::vector<folly::EventBase*> evbs) {
  hqServer_->start(addr, std::move(evbs));
}

void MoQServer::stop() {
  hqServer_->stop();
}

void MoQServer::rejectNewConnections(std::function<bool()> rejectFn) {
  if (hqServer_) {
    hqServer_->rejectNewConnections(std::move(rejectFn));
  }
}

void MoQServer::createMoQQuicSession(
    std::shared_ptr<quic::QuicSocket> quicSocket) {
  // Detect negotiated ALPN before wrapping the socket
  auto stdAlpn = quicSocket->getAppProtocol();
  std::optional<std::string> alpn;
  if (stdAlpn) {
    alpn = *stdAlpn;
    XLOG(DBG1) << "Server: Negotiated ALPN: " << *alpn;
  }

  // Capture connection IDs and addresses before moving the socket
  auto clientCid = quicSocket->getClientConnectionId();
  auto serverCid = quicSocket->getServerConnectionId();
  auto peerAddress = quicSocket->getPeerAddress();
  auto localAddress = quicSocket->getLocalAddress();

  auto qevb = quicSocket->getEventBase();
  auto quicWebTransport =
      std::make_shared<proxygen::QuicWebTransport>(std::move(quicSocket));
  auto qWtPtr = quicWebTransport.get();
  std::shared_ptr<proxygen::WebTransport> wt(std::move(quicWebTransport));
  auto evb = qevb->getTypedEventBase<quic::FollyQuicEventBase>()
                 ->getBackingEventBase();
  auto moqSession = createSession(std::move(wt), getOrCreateExecutor(evb));
  if (mLoggerFactory_) {
    auto logger = createLogger();
    // Set QUIC connection IDs and addresses on the logger
    if (clientCid) {
      logger->setDcid(*clientCid);
    }
    if (serverCid) {
      logger->setSrcCid(*serverCid);
    }
    logger->setPeerAddress(peerAddress);
    logger->setLocalAddress(localAddress);
    moqSession->setLogger(logger);
  }

  // Configure session based on negotiated ALPN
  if (alpn) {
    moqSession->validateAndSetVersionFromAlpn(*alpn);
  }

  qWtPtr->setHandler(moqSession.get());
  // the handleClientSession coro this session moqSession
  co_withExecutor(evb, handleClientSession(std::move(moqSession))).start();
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

void MoQServer::setFizzContext(
    std::shared_ptr<const fizz::server::FizzServerContext> ctx) {
  hqServer_->setFizzContext(std::move(ctx));
}

void MoQServer::setFizzContext(
    folly::EventBase* evb,
    std::shared_ptr<const fizz::server::FizzServerContext> ctx) {
  hqServer_->setFizzContext(evb, std::move(ctx));
}

void MoQServer::Handler::onHeadersComplete(
    std::unique_ptr<HTTPMessage> req) noexcept {
  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);

  if (req->getPathAsStringPiece() != server_.getEndpoint()) {
    XLOG(DBG0) << req->getPathAsStringPiece();
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

  // Use default MoQT protocols for WebTransport negotiation
  std::vector<std::string> supportedProtocols = getDefaultMoqtProtocols(false);
  std::optional<std::string> negotiatedProtocol;
  if (auto wtAvailableProtocols =
          HTTPWebTransport::getWTAvailableProtocols(*req)) {
    if (auto wtProtocol = HTTPWebTransport::negotiateWTProtocol(
            wtAvailableProtocols.value(), supportedProtocols)) {
      HTTPWebTransport::setWTProtocol(resp, wtProtocol.value());
      negotiatedProtocol = wtProtocol.value();
      XLOG(DBG1) << "WebTransport: Negotiated protocol: " << *wtProtocol;
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
  clientSession_ = server_.createSession(
      folly::MaybeManagedPtr<proxygen::WebTransport>(wt),
      server_.getOrCreateExecutor(evb));
  if (server_.mLoggerFactory_) {
    auto logger = server_.createLogger();
    // Set QUIC connection IDs and addresses on the logger from the underlying
    // transaction
    auto& transport = txn_->getTransport();
    if (transport.getSessionType() ==
        proxygen::HTTPTransaction::Transport::Type::QUIC) {
      auto* hqSession =
          static_cast<proxygen::HQSession*>(transport.getHTTPSessionBase());
      if (auto* quicSocket = hqSession->getQuicSocket()) {
        if (auto clientCid = quicSocket->getClientConnectionId()) {
          logger->setDcid(*clientCid);
        }
        if (auto serverCid = quicSocket->getServerConnectionId()) {
          logger->setSrcCid(*serverCid);
        }
        logger->setPeerAddress(quicSocket->getPeerAddress());
        logger->setLocalAddress(quicSocket->getLocalAddress());
      }
    }
    clientSession_->setLogger(logger);
  }

  // Configure session based on negotiated WebTransport protocol
  if (negotiatedProtocol) {
    clientSession_->validateAndSetVersionFromAlpn(*negotiatedProtocol);
  }

  co_withExecutor(evb, server_.handleClientSession(clientSession_)).start();
}

void MoQServer::setQuicStatsFactory(
    std::unique_ptr<quic::QuicTransportStatsCallbackFactory> factory) {
  if (hqServer_) {
    hqServer_->setStatsFactory(std::move(factory));
  }
}

std::shared_ptr<MoQExecutor> MoQServer::getOrCreateExecutor(
    folly::EventBase* evb) {
  return executorLocal_.try_emplace_with(
      *evb, [evb] { return std::make_shared<MoQFollyExecutorImpl>(evb); });
}

} // namespace moxygen
