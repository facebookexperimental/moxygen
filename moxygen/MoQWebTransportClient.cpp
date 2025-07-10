/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQWebTransportClient.h>

#include <proxygen/lib/http/HQConnector.h>

namespace {

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

proxygen::HTTPMessage getWebTransportConnectRequest(const proxygen::URL& url) {
  proxygen::HTTPMessage req;
  req.setHTTPVersion(1, 1);
  req.setSecure(true);
  req.getHeaders().set(proxygen::HTTP_HEADER_HOST, url.getHost());
  req.getHeaders().add("Sec-Webtransport-Http3-Draft02", "1");
  req.setURL(url.makeRelativeURL());
  req.setMethod(proxygen::HTTPMethod::CONNECT);
  req.setUpgradeProtocol("webtransport");
  return req;
}

folly::coro::Task<proxygen::HQUpstreamSession*> connectH3WithWebtransport(
    folly::EventBase* evb,
    const proxygen::URL& url,
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds transaction_timeout) {
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
  quic::TransportSettings ts;
  ts.datagramConfig.enabled = true;
  // ts.idleTimeout = std::chrono::seconds(10);
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
      evb,
      folly::none,
      folly::SocketAddress(url.getHost(), url.getPort(), true), // blocking DNS,
      std::move(fizzContext),
      std::make_shared<InsecureVerifierDangerousDoNotUseInProduction>(),
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
    bool v11Plus) noexcept {
  proxygen::WebTransport* wt = nullptr;
  // Establish H3 connection
  auto session = co_await connectH3WithWebtransport(
      evb_, url_, connect_timeout, transaction_timeout);

  // Establish WebTransport session
  auto txn = session->newTransaction(&httpHandler_);
  txn->sendHeaders(getWebTransportConnectRequest(url_));
  auto wtTry = co_await co_awaitTry(std::move(httpHandler_.wtContract.second));
  if (wtTry.hasException()) {
    XLOG(ERR) << wtTry.exception().what();
    co_yield folly::coro::co_error(wtTry.exception());
  }
  session->drain();
  wt = wtTry.value();

  co_await completeSetupMoQSession(
      wt,
      folly::none,
      std::move(publishHandler),
      std::move(subscribeHandler),
      v11Plus);
}

void MoQWebTransportClient::HTTPHandler::onHeadersComplete(
    std::unique_ptr<proxygen::HTTPMessage> resp) noexcept {
  if (resp->getStatusCode() != 200) {
    txn_->sendAbort();
    wtContract.first.setException(std::runtime_error(
        fmt::format("Non-200 response: {0}", resp->getStatusCode())));
    return;
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
    wtContract.first.setException(std::runtime_error(fmt::format(
        "Error setting up WebTransport: {0}", folly::exceptionStr(ex))));
    return;
  }
  // the moq session has been torn down...
  XLOG(ERR) << folly::exceptionStr(ex);
  client_.onSessionEnd(folly::none);
}

} // namespace moxygen
