/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQServer.h"
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>

using namespace quic::samples;
using namespace proxygen;

namespace moxygen {

MoQServer::MoQServer(
    uint16_t port,
    std::string cert,
    std::string key,
    std::string endpoint)
    : endpoint_(endpoint) {
  params_.localAddress.emplace();
  params_.localAddress->setFromLocalPort(port);
  params_.serverThreads = 1;
  params_.certificateFilePath = cert;
  params_.keyFilePath = key;
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

void MoQServer::createMoQQuicSession(
    std::shared_ptr<quic::QuicSocket> quicSocket) {
  auto qevb = quicSocket->getEventBase();
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

folly::Try<ServerSetup> MoQServer::onClientSetup(ClientSetup /*setup*/) {
  XLOG(INFO) << "ClientSetup";
  // TODO: Make the default MAX_SUBSCRIBE_ID configurable and
  // take in the value from ClientSetup
  static constexpr size_t kDefaultMaxSubscribeId = 100;
  return folly::Try<ServerSetup>(ServerSetup({
      kVersionDraftCurrent,
      {{folly::to_underlying(SetupKey::ROLE),
        "",
        folly::to_underlying(Role::PUB_AND_SUB)},
       {folly::to_underlying(SetupKey::MAX_SUBSCRIBE_ID),
        "",
        kDefaultMaxSubscribeId}},
  }));
}

// TODO: Implement message handling
void MoQServer::ControlVisitor::operator()(Announce announce) const {
  XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
  clientSession_->announceError(
      {announce.trackNamespace, 500, "not implemented"});
}

void MoQServer::ControlVisitor::operator()(Unannounce unannounce) const {
  XLOG(INFO) << "Unannounce ns=" << unannounce.trackNamespace;
}

void MoQServer::ControlVisitor::operator()(
    AnnounceCancel announceCancel) const {
  XLOG(INFO) << "AnnounceCancel ns=" << announceCancel.trackNamespace;
}

void MoQServer::ControlVisitor::operator()(Goaway goaway) const {
  XLOG(INFO) << "Goaway nsuri=" << goaway.newSessionUri;
}

folly::coro::Task<void> MoQServer::handleClientSession(
    std::shared_ptr<MoQSession> clientSession) {
  clientSession->start();
  co_await clientSession->clientSetupComplete();

  auto control = makeControlVisitor(clientSession);
  while (auto msg = co_await clientSession->controlMessages().next()) {
    boost::apply_visitor(*control, msg.value());
  }
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
} // namespace moxygen
