/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQWebTransportClient.h>

#include <proxygen/lib/http/HQConnector.h>
#include <proxygen/lib/http/session/QuicProtocolInfo.h>
#include <proxygen/lib/http/webtransport/HTTPWebTransport.h>
#include <moxygen/MoQFramer.h>

namespace {

proxygen::HTTPMessage getWebTransportConnectRequest(
    const proxygen::URL& url,
    const std::vector<std::string>& alpns) {
  proxygen::HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setSecure(true);
  req.getHeaders().set(proxygen::HTTP_HEADER_HOST, url.getHost());
  req.getHeaders().add("Sec-Webtransport-Http3-Draft02", "1");
  req.setURL(url.makeRelativeURL());
  req.setMethod(proxygen::HTTPMethod::CONNECT);
  req.setUpgradeProtocol("webtransport");

  // Set available MoQT protocols for version negotiation
  proxygen::HTTPWebTransport::setWTAvailableProtocols(req, alpns);

  return req;
}

folly::coro::Task<proxygen::HQUpstreamSession*> connectH3WithWebtransport(
    moxygen::MoQFollyExecutorImpl* exec,
    const proxygen::URL& url,
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    const quic::TransportSettings& transportSettings) {
  // Establish an H3 connection
  class ConnectCallback : public proxygen::HQConnector::Callback {
   public:
    ~ConnectCallback() override = default;
    void connectSuccess(proxygen::HQUpstreamSession* session) override {
      XLOG(DBG1) << __func__;
      sessionContract.first.setValue(session);
    }
    void connectError(const quic::QuicErrorCode& ex) override {
      XLOG(DBG1) << __func__;
      sessionContract.first.setException(
          std::runtime_error(quic::toString(ex)));
    }

    std::pair<
        folly::coro::Promise<proxygen::HQUpstreamSession*>,
        folly::coro::Future<proxygen::HQUpstreamSession*>>
        sessionContract{
            folly::coro::makePromiseContract<proxygen::HQUpstreamSession*>()};
  };
  XLOG(DBG1) << __func__;
  auto g =
      folly::makeGuard([func = __func__] { XLOG(DBG1) << "exit " << func; });
  ConnectCallback connectCb;
  proxygen::HQConnector hqConnector(&connectCb, transaction_timeout);
  // Make a copy of transportSettings and enable datagram support
  auto ts = transportSettings;
  ts.datagramConfig.enabled = true;
  ts.maxServerRecvPacketsPerLoop = 10;
  hqConnector.setTransportSettings(ts);
  hqConnector.setSupportedQuicVersions({quic::QuicVersion::QUIC_V1});
  auto fizzContext = std::make_shared<fizz::client::FizzClientContext>();
  fizzContext->setSupportedAlpns({"h3"});
  hqConnector.setH3Settings(
      {{proxygen::SettingsId::ENABLE_CONNECT_PROTOCOL, 1},
       {proxygen::SettingsId::_HQ_DATAGRAM_DRAFT_8, 1},
       {proxygen::SettingsId::_HQ_DATAGRAM, 1},
       {proxygen::SettingsId::_HQ_DATAGRAM_RFC, 1},
       {proxygen::SettingsId::ENABLE_WEBTRANSPORT, 1}});
  hqConnector.connect(
      exec->getBackingEventBase(),
      folly::none,
      folly::SocketAddress(url.getHost(), url.getPort(), true), // blocking DNS,
      std::move(fizzContext),
      verifier,
      connect_timeout,
      folly::emptySocketOptionMap,
      url.getHost());
  auto session =
      co_await co_awaitTry(std::move(connectCb.sessionContract.second));
  if (session.hasException()) {
    XLOG(ERR) << session.exception().what();
    co_yield folly::coro::co_error(session.exception());
  }
  co_return session.value();
}
} // namespace

namespace moxygen {
folly::coro::Task<void> MoQWebTransportClient::setupMoQSession(
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout,
    std::shared_ptr<Publisher> publishHandler,
    std::shared_ptr<Subscriber> subscribeHandler,
    const quic::TransportSettings& transportSettings,
    const std::vector<std::string>& alpns) noexcept {
  proxygen::WebTransport* wt = nullptr;

  std::vector<std::string> protocolList =
      alpns.empty() ? getDefaultMoqtProtocols(false) : alpns;

  // Establish H3 connection
  auto session = co_await connectH3WithWebtransport(
      exec_->getTypedExecutor<MoQFollyExecutorImpl>(),
      url_,
      connect_timeout,
      transaction_timeout,
      verifier_,
      transportSettings);

  if (logger_) {
    auto quicInfo = session->getQuicInfo();
    if (quicInfo) {
      if (quicInfo->clientConnectionId) {
        logger_->setSrcCid(*quicInfo->clientConnectionId);
      }
      if (quicInfo->serverConnectionId) {
        logger_->setDcid(*quicInfo->serverConnectionId);
      }
    }
    logger_->setLocalAddress(session->getLocalAddress());
    logger_->setPeerAddress(session->getPeerAddress());
  }

  // Establish WebTransport session
  auto txn = session->newTransaction(&httpHandler_);
  txn->sendHeaders(getWebTransportConnectRequest(url_, protocolList));
  auto wtTry = co_await co_awaitTry(std::move(httpHandler_.wtContract.second));
  if (wtTry.hasException()) {
    XLOG(ERR) << wtTry.exception().what();
    co_yield folly::coro::co_error(wtTry.exception());
  }
  session->drain();
  wt = wtTry.value();

  co_await completeSetupMoQSession(
      wt, folly::none, std::move(publishHandler), std::move(subscribeHandler));
}

void MoQWebTransportClient::HTTPHandler::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> resp) noexcept {
  if (resp->getStatusCode() != 200) {
    txn_->sendAbort();
    wtContract.first.setException(
        std::runtime_error(
            fmt::format("Non-200 response: {0}", resp->getStatusCode())));
    return;
  }

  // Read negotiated MoQT protocol from WT-Protocol header
  auto protocolResult = proxygen::HTTPWebTransport::getWTProtocol(*resp);
  if (protocolResult.hasValue()) {
    client_.negotiatedProtocol_ = protocolResult.value();
    XLOG(DBG1) << "WebTransport client: Negotiated protocol: "
               << *client_.negotiatedProtocol_;
  } else {
    XLOG(WARN) << "WebTransport: No WT-Protocol header in response, "
               << "will use legacy version negotiation";
  }

  auto wt = txn_->getWebTransport();
  if (!wt) {
    XLOG(ERR) << "Failed to get web transport, exiting";
    txn_->sendAbort();
    return;
  }
  wtContract.first.setValue(wt);
}

void MoQWebTransportClient::HTTPHandler::onError(
    const proxygen::HTTPException& ex) noexcept {
  XLOG(DBG1) << __func__;
  if (!wtContract.first.isFulfilled()) {
    wtContract.first.setException(
        std::runtime_error(
            fmt::format(
                "Error setting up WebTransport: {0}",
                folly::exceptionStr(ex))));
    return;
  }
  // the moq session has been torn down...
  XLOG(ERR) << folly::exceptionStr(ex);
  client_.onSessionEnd(folly::none);
}

} // namespace moxygen
