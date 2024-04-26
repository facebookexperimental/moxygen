#include "moxygen/MoQClient.h"

#include <proxygen/httpserver/samples/hq/InsecureVerifierDangerousDoNotUseInProduction.h>
#include <proxygen/lib/http/HQConnector.h>

namespace moxygen {
folly::coro::Task<void> MoQClient::setupMoQSession(
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout,
    Role role) noexcept {
  // Establish an H3 connection
  class ConnectCallback : public proxygen::HQConnector::Callback {
   public:
    ~ConnectCallback() override = default;
    void connectSuccess(proxygen::HQUpstreamSession* session) override {
      XLOG(INFO) << __func__;
      sessionContract.first.setValue(session);
    }
    void connectError(const quic::QuicErrorCode& ex) override {
      XLOG(INFO) << __func__;
      sessionContract.first.setException(
          std::runtime_error(quic::toString(ex)));
    }

    std::pair<
        folly::coro::Promise<proxygen::HQUpstreamSession*>,
        folly::coro::Future<proxygen::HQUpstreamSession*>>
        sessionContract{
            folly::coro::makePromiseContract<proxygen::HQUpstreamSession*>()};
  };
  XLOG(INFO) << __func__;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(INFO) << "exit " << func; });
  ConnectCallback connectCb;
  proxygen::HQConnector hqConnector(&connectCb, transaction_timeout);
  quic::TransportSettings ts;
  // ts.idleTimeout = std::chrono::seconds(10);
  hqConnector.setTransportSettings(ts);
  auto fizzContext = std::make_shared<fizz::client::FizzClientContext>();
  fizzContext->setSupportedAlpns({"h3"});
  hqConnector.setH3Settings(
      {{proxygen::SettingsId::ENABLE_CONNECT_PROTOCOL, 1},
       {proxygen::SettingsId::_HQ_DATAGRAM_DRAFT_8, 1},
       {proxygen::SettingsId::ENABLE_WEBTRANSPORT, 1}});
  hqConnector.connect(
      evb_,
      folly::none,
      folly::SocketAddress(
          url_.getHost(), url_.getPort(), true), // blocking DNS,
      std::move(fizzContext),
      std::make_shared<
          proxygen::InsecureVerifierDangerousDoNotUseInProduction>(),
      connect_timeout,
      folly::emptySocketOptionMap,
      url_.getHost());
  auto session =
      co_await co_awaitTry(std::move(connectCb.sessionContract.second));
  if (session.hasException()) {
    XLOG(ERR) << session.exception().what();
    co_yield folly::coro::co_error(session.exception());
  }

  // Establish WebTransport session and create MoQSession
  auto txn = session.value()->newTransaction(&httpHandler_);
  proxygen::HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.getHeaders().set(proxygen::HTTP_HEADER_HOST, url_.getHost());
  req.getHeaders().add("Sec-Webtransport-Http3-Draft02", "1");
  req.setURL(url_.makeRelativeURL());
  req.setMethod(proxygen::HTTPMethod::CONNECT);
  req.setUpgradeProtocol("webtransport");
  txn->sendHeaders(req);
  auto moqSession =
      co_await co_awaitTry(std::move(httpHandler_.sessionContract.second));
  if (moqSession.hasException()) {
    XLOG(ERR) << moqSession.exception().what();
    co_yield folly::coro::co_error(moqSession.exception());
  }
  session.value()->drain();

  // Setup MoQSession parameters
  moqSession_ = std::move(moqSession.value());
  moqSession_->start();
  moqSession_->setup(ClientSetup(
      {{kVersionDraftCurrent},
       {{folly::to_underlying(SetupKey::ROLE), "", folly::to_underlying(role)},
        {folly::to_underlying(SetupKey::PATH), url_.getPath(), 0}}}));
  co_await moqSession_->setupComplete();
}

void MoQClient::HTTPHandler::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> resp) noexcept {
  if (resp->getStatusCode() != 200) {
    txn_->sendAbort();
    sessionContract.first.setException(std::runtime_error(
        fmt::format("Non-200 response: {0}", resp->getStatusCode())));
    return;
  }
  auto wt = txn_->getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Failed to get web transport, exiting";
    txn_->sendAbort();
    return;
  }
  sessionContract.first.setValue(std::make_shared<MoQSession>(
      MoQCodec::Direction::CLIENT, wt, client_.evb_));
}

void MoQClient::HTTPHandler::onError(
    const proxygen::HTTPException& ex) noexcept {
  XLOG(INFO) << __func__;
  if (!sessionContract.first.isFulfilled()) {
    sessionContract.first.setException(std::runtime_error(
        fmt::format("Error setting up WebTransport: {0}", ex.what())));
    return;
  }
  // the moq session has been torn down...
  XLOG(ERR) << ex.what();
  client_.onSessionEnd(ex);
}

void MoQClient::onSessionEnd(folly::Optional<proxygen::HTTPException>) {
  // TODO: cleanup?
  XLOG(INFO) << "resetting moqSession_";
  moqSession_.reset();
  CHECK(!moqSession_);
}

void MoQClient::onWebTransportBidiStream(
    proxygen::WebTransport::BidiStreamHandle bidi) {
  XLOG(INFO) << __func__;
  moqSession_->onNewBidiStream(std::move(bidi));
}

void MoQClient::onWebTransportUniStream(
    proxygen::WebTransport::StreamReadHandle* stream) {
  XLOG(INFO) << __func__;
  moqSession_->onNewUniStream(stream);
}

} // namespace moxygen
