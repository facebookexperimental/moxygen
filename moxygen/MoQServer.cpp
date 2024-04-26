#include "moxygen/MoQServer.h"

using namespace quic::samples;
using namespace proxygen;

namespace moxygen {

MoQServer::MoQServer(
    uint16_t port,
    std::string cert,
    std::string key,
    std::string endpoint)
    : endpoint_(endpoint) {
  HQServerParams params;
  params.localAddress.emplace();
  params.localAddress->setFromLocalPort(port);
  params.serverThreads = 1;
  params.certificateFilePath = cert;
  params.keyFilePath = key;
  params.txnTimeout = std::chrono::seconds(60);
  hqServer_ = std::make_unique<HQServer>(
      params, [this](HTTPMessage*) { return new Handler(*this); });
  hqServer_->start();
}

void MoQServer::ControlVisitor::operator()(ClientSetup /*setup*/) const {
  XLOG(INFO) << "ClientSetup";
  clientSession_->setup(
      {kVersionDraftCurrent,
       {{folly::to_underlying(SetupKey::ROLE),
         "",
         folly::to_underlying(Role::BOTH)}}});
}

void MoQServer::ControlVisitor::operator()(ServerSetup) const {
  // error
  XLOG(ERR) << "Server received ServerSetup";
  clientSession_->close();
}

void MoQServer::ControlVisitor::operator()(Announce announce) const {
  XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
  clientSession_->announceError(
      {announce.trackNamespace, 500, "not implemented"});
}

void MoQServer::ControlVisitor::operator()(
    SubscribeRequest subscribeReq) const {
  XLOG(INFO) << "SubscribeRequest track="
             << subscribeReq.fullTrackName.trackNamespace
             << subscribeReq.fullTrackName.trackName
             << " id=" << subscribeReq.subscribeID;
  clientSession_->subscribeError(
      {subscribeReq.subscribeID, 500, "not implemented"});
}

void MoQServer::ControlVisitor::operator()(Unannounce unannounce) const {
  XLOG(INFO) << "Unannounce ns=" << unannounce.trackNamespace;
}

void MoQServer::ControlVisitor::operator()(SubscribeFin subscribeFin) const {
  XLOG(INFO) << "SubscribeFin id=" << subscribeFin.subscribeID;
}

void MoQServer::ControlVisitor::operator()(SubscribeRst subscribeRst) const {
  XLOG(INFO) << "SubscribeRst id=" << subscribeRst.subscribeID
             << " code=" << subscribeRst.errorCode
             << " reason=" << subscribeRst.reasonPhrase;
}

void MoQServer::ControlVisitor::operator()(Unsubscribe unsubscribe) const {
  XLOG(INFO) << "Unsubscribe id=" << unsubscribe.subscribeID;
}

void MoQServer::ControlVisitor::operator()(Goaway goaway) const {
  XLOG(INFO) << "Goaway nsuri=" << goaway.newSessionUri;
}

folly::coro::Task<void> MoQServer::handleClientSession(
    std::shared_ptr<MoQSession> clientSession) {
  clientSession->start();

  auto control = makeControlVisitor(clientSession);
  while (auto msg = co_await clientSession->controlMessages().next()) {
    boost::apply_visitor(*control, msg.value());
  }
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
  clientSession_ =
      std::make_shared<MoQSession>(MoQCodec::Direction::SERVER, wt, evb);

  server_.handleClientSession(clientSession_).scheduleOn(evb).start();
}
} // namespace moxygen
