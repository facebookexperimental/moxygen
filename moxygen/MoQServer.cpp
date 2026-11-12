/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQServer.h"
#include <folly/ExceptionString.h>
#include <folly/Function.h>
#include <folly/Portability.h>
#include <folly/String.h>
#include <folly/logging/xlog.h>
#include <folly/net/NetOps.h>
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include <proxygen/lib/http/session/HQSession.h>
#include <proxygen/lib/http/webtransport/HTTPWebTransport.h>
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>
#include <proxygen/lib/http/webtransport/QuicWtSession.h>
#include <quic/common/address/QuicSocketAddressBridge.h>
#include <moxygen/MoQTypes.h>
#include <moxygen/MoQVersions.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

#include <utility>

using namespace quic::samples;
using namespace proxygen;

namespace moxygen {

namespace {
constexpr size_t kUdpBufferSize = 1024 * 1024; // 1 MB
} // namespace

quic::TransportSettings MoQServer::defaultTransportSettings() {
  quic::TransportSettings ts;
  ts.defaultCongestionController = quic::CongestionControlType::Copa;
  ts.copaDeltaParam = 0.05;
  ts.pacingEnabled = true;
  ts.maxCwndInMss = quic::kLargeMaxCwndInMss;
  ts.batchingMode = quic::QuicBatchingMode::BATCHING_MODE_GSO;
  ts.maxBatchSize = 48;
  ts.dataPathType = quic::DataPathType::ContinuousMemory;
  ts.maxServerRecvPacketsPerLoop = 10;
  ts.writeConnectionDataPacketsLimit = 48;
  ts.advertisedInitialConnectionFlowControlWindow = 1024 * 1024;
  ts.advertisedInitialBidiLocalStreamFlowControlWindow = 1024 * 1024;
  ts.advertisedInitialBidiRemoteStreamFlowControlWindow = 1024 * 1024;
  ts.advertisedInitialUniStreamFlowControlWindow = 1024 * 1024;
  // Priority schedule datagrams with streams rather than ahead of them,
  // buffer more than the default 75, and prefer new datagrams when full.
  ts.datagramConfig.enabled = true;
  ts.datagramConfig.scheduleDatagramsWithStreams = true;
  ts.datagramConfig.readBufSize = 1024;
  ts.datagramConfig.writeBufSize = 1024;
  ts.datagramConfig.recvDropOldDataFirst = true;
  ts.datagramConfig.sendDropOldDataFirst = true;
  return ts;
}

MoQServer::MoQServer(
    std::string cert,
    std::string key,
    std::string endpoint,
    std::optional<quic::TransportSettings> transportSettings,
    std::function<bool()> useQuicWtSession)
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
          Options{
              .transportSettings = std::move(transportSettings),
              .useQuicWtSession = std::move(useQuicWtSession)}) {}

MoQServer::MoQServer(
    std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
    std::string endpoint,
    Options options)
    : MoQServerBase(std::move(endpoint)),
      fizzContext_(std::move(fizzContext)),
      useQuicWtSession_(std::move(options.useQuicWtSession)),
      useHqWtSession_(std::move(options.useHqWtSession)) {
  params_.serverThreads = 1;
  params_.txnTimeout = options.txnTimeout;
  params_.transportSettings = options.transportSettings
      ? *options.transportSettings
      : defaultTransportSettings();

  // UDP socket buffer sizes
  params_.udpSendBufferSize = options.udpSendBufferBytes > 0
      ? options.udpSendBufferBytes
      : kUdpBufferSize;
  params_.udpRecvBufferSize = options.udpRecvBufferBytes > 0
      ? options.udpRecvBufferBytes
      : kUdpBufferSize;

  // Extract MoQT protocols from the fizz context (filter out "h3")
  std::vector<std::string> quicAlpns;
  for (const auto& alpn : fizzContext_->getSupportedAlpns()) {
    if (alpn != "h3") {
      quicAlpns.push_back(alpn);
      wtMoqtProtocols_.push_back(alpn);
    }
  }

  factory_ = std::make_unique<HQServerTransportFactory>(
      params_, [this](HTTPMessage*) { return new Handler(*this); }, nullptr);
  factory_->setQLoggerFactory(
      [this](quic::VantagePoint vp) { return makeQLogger(vp); });

  // Configure 0-RTT early data handler with server's default params
  earlyDataHandler_.setCurrentParams(
      kDefaultMaxRequestID, kDefaultMaxAuthTokenCacheSize);
  factory_->setDefaultEarlyDataHandler(&earlyDataHandler_);

  // Register ALPN handlers for direct QUIC MoQT connections
  XLOG(DBG1) << "MoQServer: Registering ALPN handlers: "
             << folly::join(", ", quicAlpns);
  registerAlpnHandler(quicAlpns);

  hqServer_ =
      std::make_unique<HQServer>(params_, std::move(factory_), fizzContext_);
}

void MoQServer::setAuthTokenCacheEnabled(bool enabled) {
  MoQServerBase::setAuthTokenCacheEnabled(enabled);
  earlyDataHandler_.setMaxAuthTokenCacheSize(
      enabled ? kDefaultMaxAuthTokenCacheSize : 0);
}

void MoQServer::registerAlpnHandler(const std::vector<std::string>& alpns) {
  if (!factory_) {
    XLOG(DBG1) << "Cannot register ALPN handler: factory not initialized";
    return;
  }

  if (hqServer_) {
    XLOG(DBG1) << "Cannot register ALPN handler: server already started";
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

  // Warn if the kernel capped our UDP send buffer below what we requested.
  // Linux doubles the value we set (to account for overhead), so getsockopt
  // returns 2x the effective buffer size; other platforms report as-is.
  const int kExpectedSndBuf =
      static_cast<int>((folly::kIsLinux ? 2 : 1) * params_.udpSendBufferSize);
  for (int fd : getAllListeningSocketFDs()) {
    int actual = 0;
    socklen_t len = sizeof(actual);
    if (folly::netops::getsockopt(
            folly::NetworkSocket(fd), SOL_SOCKET, SO_SNDBUF, &actual, &len) ==
        0) {
      if (actual < kExpectedSndBuf) {
        XLOG(WARN) << "UDP send buffer for fd " << fd << " is " << actual
                   << " bytes (expected " << kExpectedSndBuf << ")";
      }
    }
  }
}

void MoQServer::stop() {
  hqServer_->stop();
  hqServer_.reset();
}

void MoQServer::rejectNewConnections(
    std::function<bool(const quic::SocketAddress&)> rejectFn) {
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
  if (!alpn || !getVersionFromAlpn(*alpn)) {
    XLOG(ERR) << "Server: no supported MoQT version for ALPN "
              << alpn.value_or("<none>");
    quic::ApplicationErrorCode err(
        folly::to_underlying(
            SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED));
    quicSocket->close(
        quic::QuicError(
            quic::QuicErrorCode(err), std::string("unsupported MoQT ALPN")));
    return;
  }

  // Capture connection IDs and addresses before moving the socket
  auto clientCid = quicSocket->getClientConnectionId();
  auto serverCid = quicSocket->getServerConnectionId();
  auto peerAddress = quic::toFollySocketAddress(quicSocket->getPeerAddress());
  auto localAddress = quic::toFollySocketAddress(quicSocket->getLocalAddress());

  auto qevb = quicSocket->getEventBase();
  auto* socket = quicSocket.get();
  const bool useQuicWtSession = useQuicWtSession_ && useQuicWtSession_();
  std::shared_ptr<proxygen::WebTransport> wt;
  if (useQuicWtSession) {
    wt = std::make_shared<proxygen::QuicWtSession>(
        std::move(quicSocket), /*wtHandler=*/nullptr);
  } else {
    wt = std::make_shared<proxygen::QuicWebTransport>(std::move(quicSocket));
  }
  auto* wtPtr = wt.get();
  auto evb = qevb->getTypedEventBase<quic::FollyQuicEventBase>()
                 ->getBackingEventBase();
  auto moqSession = createSession(std::move(wt), getOrCreateExecutor(evb));
  if (auto logger = createLogger()) {
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

  moqSession->validateAndSetVersionFromAlpn(*alpn);

  if (useQuicWtSession) {
    static_cast<proxygen::QuicWtSession*>(wtPtr)->setHandler(moqSession.get());
  } else {
    static_cast<proxygen::QuicWebTransport*>(wtPtr)->setHandler(
        moqSession.get());
  }
  QuicTransportObservers observers(*socket);
  onSessionTransportReady(moqSession, observers);
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

namespace {
// HqWtSession owns this handler and the MoQ session owns the HqWtSession, so
// the handler holds the session weakly.
class WtSessionHandler final : public proxygen::WebTransportHandler {
 public:
  using Factory =
      folly::Function<std::shared_ptr<proxygen::WebTransportHandler>(
          std::shared_ptr<proxygen::WebTransport>)>;

  explicit WtSessionHandler(Factory factory) : factory_(std::move(factory)) {}

  void onWebTransportSession(
      std::shared_ptr<proxygen::WebTransport> wt) noexcept override {
    if (!wt) {
      return;
    }
    try {
      session_ = std::exchange(factory_, nullptr)(wt);
    } catch (...) {
      XLOG(ERR) << "Failed to start MoQ WebTransport session: "
                << folly::exceptionStr(std::current_exception());
      (void)wt->closeSession(proxygen::WebTransport::kInternalError);
    }
  }
  void onNewUniStream(
      proxygen::WebTransport::StreamReadHandle* handle) noexcept override {
    if (auto session = session_.lock()) {
      session->onNewUniStream(handle);
    }
  }
  void onNewBidiStream(
      proxygen::WebTransport::BidiStreamHandle handle) noexcept override {
    if (auto session = session_.lock()) {
      session->onNewBidiStream(handle);
    }
  }
  void onDatagrams(
      std::vector<std::unique_ptr<folly::IOBuf>> datagrams) noexcept override {
    if (auto session = session_.lock()) {
      session->onDatagrams(std::move(datagrams));
    }
  }
  void onSessionEnd(folly::Optional<uint32_t> error) noexcept override {
    auto session = session_.lock();
    session_.reset();
    if (session) {
      session->onSessionEnd(std::move(error));
    }
  }
  void onSessionDrain() noexcept override {
    if (auto session = session_.lock()) {
      session->onSessionDrain();
    }
  }

 private:
  Factory factory_;
  std::weak_ptr<proxygen::WebTransportHandler> session_;
};
} // namespace

void MoQServer::Handler::onHeadersComplete(
    std::unique_ptr<HTTPMessage> req) noexcept {
  HTTPMessage resp;
  resp.setHTTPVersion(1, 1);

  if (!server_.isAcceptedEndpoint(req->getPathAsStringPiece())) {
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

  // Use configured MoQT protocols for WebTransport negotiation
  const auto& supportedProtocols = server_.wtMoqtProtocols_;
  XLOG(DBG1) << "MoQServer WebTransport: supported protocols: "
             << folly::join(", ", supportedProtocols);
  std::optional<std::string> negotiatedProtocol;
  if (auto wtAvailableProtocols =
          HTTPWebTransport::getWTAvailableProtocols(*req)) {
    if (auto wtProtocol = HTTPWebTransport::negotiateWTProtocol(
            wtAvailableProtocols.value(), supportedProtocols)) {
      negotiatedProtocol = wtProtocol.value();
    }
  }
  // The WT protocol is the only source of the MoQT version, so refuse the
  // session rather than create one without a version.
  if (!negotiatedProtocol || !getVersionFromAlpn(*negotiatedProtocol)) {
    XLOG(DBG4) << "Failed to negotiate a MoQT WebTransport protocol";
    resp.setStatusCode(400);
    txn_->sendHeadersWithEOM(resp);
    return;
  }
  HTTPWebTransport::setWTProtocol(resp, *negotiatedProtocol);
  XLOG(DBG1) << "WebTransport: Negotiated protocol: " << *negotiatedProtocol;
  quic::QuicSocket* quicSocket = nullptr;
  auto& transport = txn_->getTransport();
  if (transport.getSessionType() ==
      proxygen::HTTPTransaction::Transport::Type::QUIC) {
    auto* hqSession =
        static_cast<proxygen::HQSession*>(transport.getHTTPSessionBase());
    quicSocket = hqSession->getQuicSocket();
  }
  std::string authority(req->getHeaders().getSingleOrEmpty(HTTP_HEADER_HOST));
  std::string path(req->getPathAsStringPiece());
  if (server_.useHqWtSession_ && server_.useHqWtSession_()) {
    // Detaches and deletes this handler, so return right after.
    txn_->sendWtHeaders(
        resp,
        std::make_unique<WtSessionHandler>(
            [&server = server_,
             // HqWtSession holds this socket too, so it outlives the handler.
             quicSocket,
             authority = std::move(authority),
             path = std::move(path),
             protocol = *negotiatedProtocol](
                std::shared_ptr<proxygen::WebTransport> wt) mutable {
              return server.startWebTransportSession(
                  folly::MaybeManagedPtr<proxygen::WebTransport>(wt),
                  quicSocket,
                  std::move(authority),
                  std::move(path),
                  protocol);
            }));
    return;
  }
  txn_->sendHeaders(resp);
  auto wt = txn_->getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Failed to get WebTransport";
    txn_->sendAbort();
    return;
  }
  clientSession_ = server_.startWebTransportSession(
      folly::MaybeManagedPtr<proxygen::WebTransport>(wt),
      quicSocket,
      std::move(authority),
      std::move(path),
      *negotiatedProtocol);
}

std::shared_ptr<MoQSession> MoQServer::startWebTransportSession(
    folly::MaybeManagedPtr<proxygen::WebTransport> wt,
    quic::QuicSocket* quicSocket,
    std::string authority,
    std::string path,
    const std::string& protocol) {
  auto evb = folly::EventBaseManager::get()->getEventBase();
  auto session = createSession(std::move(wt), getOrCreateExecutor(evb));
  session->setAuthority(std::move(authority));
  session->setPath(std::move(path));
  if (auto logger = createLogger()) {
    if (quicSocket) {
      if (auto clientCid = quicSocket->getClientConnectionId()) {
        logger->setDcid(*clientCid);
      }
      if (auto serverCid = quicSocket->getServerConnectionId()) {
        logger->setSrcCid(*serverCid);
      }
      logger->setPeerAddress(
          quic::toFollySocketAddress(quicSocket->getPeerAddress()));
      logger->setLocalAddress(
          quic::toFollySocketAddress(quicSocket->getLocalAddress()));
    }
    session->setLogger(logger);
  }

  session->validateAndSetVersionFromAlpn(protocol);

  if (quicSocket) {
    QuicTransportObservers observers(*quicSocket);
    onSessionTransportReady(session, observers);
  }
  co_withExecutor(evb, handleClientSession(session)).start();
  return session;
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
