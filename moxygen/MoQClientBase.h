/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fizz/protocol/CertificateVerifier.h>
#include <folly/coro/Promise.h>
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>
#include <proxygen/lib/utils/URL.h>
#include <quic/client/QuicClientTransport.h>
#include <quic/state/TransportSettings.h>
#include <moxygen/MoQSession.h>
#include <moxygen/mlog/MLogger.h>
#include <functional>
#include <memory>

namespace moxygen {

const std::string kDefaultClientFilePath = "./mlog_client.txt";

class Subscriber;

class MoQClientBase : public proxygen::WebTransportHandler {
 public:
  using SessionFactory = std::function<std::shared_ptr<MoQSession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>,
      std::shared_ptr<MoQExecutor>)>;

  MoQClientBase(std::shared_ptr<MoQExecutor> exec, proxygen::URL url)
      : exec_(std::move(exec)),
        url_(std::move(url)),
        sessionFactory_(defaultSessionFactory()) {}

  MoQClientBase(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      SessionFactory sessionFactory)
      : exec_(std::move(exec)),
        url_(std::move(url)),
        sessionFactory_(std::move(sessionFactory)) {}

  std::shared_ptr<MoQExecutor> getEventBase() {
    return exec_;
  }

  std::shared_ptr<MoQSession> moqSession_;
  virtual folly::coro::Task<void> setupMoQSession(
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds transaction_timeout,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler,
      const quic::TransportSettings& transportSettings,
      const std::vector<std::string>& alpns = {}) noexcept;

  void setLogger(const std::shared_ptr<MLogger>& logger);

  void goaway(const Goaway& goaway);
  std::shared_ptr<MLogger> logger_ = nullptr;

 protected:
  static bool shouldSendAuthorityParam(
      const std::vector<uint64_t>& supportedVersions);
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

  folly::coro::Task<ServerSetup> completeSetupMoQSession(
      proxygen::WebTransport* wt,
      const folly::Optional<std::string>& pathParam,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler);
  ClientSetup getClientSetup(const folly::Optional<std::string>& path);

  void onSessionEnd(folly::Optional<uint32_t>) override;
  void onNewBidiStream(
      proxygen::WebTransport::BidiStreamHandle handle) override;
  void onNewUniStream(
      proxygen::WebTransport::StreamReadHandle* handle) override;
  void onDatagram(std::unique_ptr<folly::IOBuf>) override;

  std::shared_ptr<MoQExecutor> exec_;
  proxygen::URL url_;
  SessionFactory sessionFactory_;
  std::shared_ptr<proxygen::QuicWebTransport> quicWebTransport_;
  folly::Optional<std::string> negotiatedProtocol_;
};

} // namespace moxygen
