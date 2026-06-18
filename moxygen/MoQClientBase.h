/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fizz/protocol/CertificateVerifier.h>
#include <folly/coro/Promise.h>
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>
#include <proxygen/lib/http/webtransport/QuicWtSession.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <proxygen/lib/utils/URL.h>
#include <quic/client/QuicClientTransport.h>
#include <quic/fizz/client/handshake/QuicPskCache.h>
#include <quic/state/TransportSettings.h>
#include <moxygen/MoQEarlyDataHandler.h>
#include <moxygen/MoQSession.h>
#include <moxygen/mlog/MLogger.h>
#include <functional>
#include <memory>

namespace moxygen {

const std::string kDefaultClientFilePath = "./mlog_client.txt";

class Subscriber;

class MoQClientBase {
 public:
  using SessionFactory = std::function<std::shared_ptr<MoQSession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>,
      std::shared_ptr<MoQExecutor>)>;

  MoQClientBase(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr,
      bool useQuicWtSession = false)
      : exec_(std::move(exec)),
        url_(std::move(url)),
        sessionFactory_(defaultSessionFactory()),
        verifier_(std::move(verifier)),
        useQuicWtSession_(useQuicWtSession) {}

  MoQClientBase(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      SessionFactory sessionFactory,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr,
      bool useQuicWtSession = false)
      : exec_(std::move(exec)),
        url_(std::move(url)),
        sessionFactory_(std::move(sessionFactory)),
        verifier_(std::move(verifier)),
        useQuicWtSession_(useQuicWtSession) {}

  virtual ~MoQClientBase() {
    if (moqSession_) {
      moqSession_->close(SessionCloseErrorCode::NO_ERROR);
      moqSession_.reset();
    }
    if (quicWebTransport_) {
      quicWebTransport_->setHandler(nullptr);
    }
    if (quicWtSession_) {
      quicWtSession_->setHandler(nullptr);
    }
  }

  std::shared_ptr<MoQExecutor> getEventBase() {
    return exec_;
  }

  std::chrono::milliseconds getTransportConnectTime() {
    return transportConnectTime_;
  }

  std::chrono::milliseconds getMoQHandshakeTime() {
    return moqHandshakeTime_;
  }

  std::shared_ptr<MoQSession> moqSession_;
  virtual folly::coro::Task<void> setupMoQSession(
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds transaction_timeout,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler,
      const quic::TransportSettings& transportSettings,
      const std::vector<std::string>& alpns = {});

  // Phase 1: Connect QUIC, create session, send CLIENT_SETUP.
  // After this, moqSession_ is available for sending early requests.
  folly::coro::Task<void> connectAndSendSetup(
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds transaction_timeout,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler,
      const quic::TransportSettings& transportSettings,
      const std::vector<std::string>& alpns = {});

  // Phase 2: Await SERVER_SETUP, update earlyDataHandler params.
  folly::coro::Task<Setup> awaitSetupComplete();

  void setLogger(const std::shared_ptr<MLogger>& logger);

  void setPskCache(std::shared_ptr<quic::QuicPskCache> pskCache) {
    pskCache_ = std::move(pskCache);
  }

  void setEarlyDataHandler(MoQEarlyDataHandler* handler) {
    earlyDataHandler_ = handler;
  }

  void goaway(const Goaway& goaway);
  std::shared_ptr<MLogger> logger_ = nullptr;

 protected:
  virtual folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
  connectQuic(
      folly::SocketAddress connectAddr,
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      const std::vector<std::string>& alpns,
      const quic::TransportSettings& transportSettings) = 0;

  virtual std::shared_ptr<MoQSession> createSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt);

  static SessionFactory defaultSessionFactory();

  void completeSetupMoQSession(
      proxygen::WebTransport* wt,
      const std::optional<std::string>& pathParam,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler);
  Setup getClientSetup(const std::optional<std::string>& path);

  std::shared_ptr<MoQExecutor> exec_;
  proxygen::URL url_;
  SessionFactory sessionFactory_;
  std::shared_ptr<proxygen::QuicWebTransport> quicWebTransport_;
  std::shared_ptr<proxygen::QuicWtSession> quicWtSession_;
  std::optional<std::string> negotiatedProtocol_;
  std::shared_ptr<fizz::CertificateVerifier> verifier_;
  std::shared_ptr<quic::QuicPskCache> pskCache_;
  MoQEarlyDataHandler* earlyDataHandler_{nullptr};
  bool useQuicWtSession_{false};
  std::chrono::milliseconds transportConnectTime_{0};
  std::chrono::milliseconds moqHandshakeTime_{0};

  const quic::QuicSocket* getQuicSocket() const {
    return quicSocket_.lock().get();
  }

 private:
  // temporary, remove weak_ptr when migration to QuicWtSession is complete
  std::weak_ptr<const quic::QuicClientTransport> quicSocket_;
};

} // namespace moxygen
