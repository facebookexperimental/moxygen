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
#include <moxygen/MoQSession.h>
#include <moxygen/mlog/MLogger.h>

namespace moxygen {

const std::string kDefaultClientFilePath = "./mlog_client.txt";

class Subscriber;

class MoQClientBase : public proxygen::WebTransportHandler {
 public:
  MoQClientBase(MoQExecutor* exec, proxygen::URL url)
      : exec_(exec), url_(std::move(url)) {}

  MoQExecutor* getEventBase() {
    return exec_;
  }

  std::shared_ptr<MoQSession> moqSession_;
  virtual folly::coro::Task<void> setupMoQSession(
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds transaction_timeout,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler,
      bool v11Plus = true) noexcept;

  void setLogger(const std::shared_ptr<MLogger>& logger);

  void goaway(const Goaway& goaway);
  std::shared_ptr<MLogger> logger_ = nullptr;

 protected:
  virtual folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
  connectQuic(
      folly::SocketAddress connectAddr,
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      std::string alpn) = 0;

  folly::coro::Task<ServerSetup> completeSetupMoQSession(
      proxygen::WebTransport* wt,
      const folly::Optional<std::string>& pathParam,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler,
      bool v11Plus);
  ClientSetup getClientSetup(
      const folly::Optional<std::string>& path,
      bool v11Plus);

  void onSessionEnd(folly::Optional<uint32_t>) override;
  void onNewBidiStream(
      proxygen::WebTransport::BidiStreamHandle handle) override;
  void onNewUniStream(
      proxygen::WebTransport::StreamReadHandle* handle) override;
  void onDatagram(std::unique_ptr<folly::IOBuf>) override;

  MoQExecutor* exec_{nullptr};
  proxygen::URL url_;
  std::shared_ptr<proxygen::QuicWebTransport> quicWebTransport_;
};

} // namespace moxygen
