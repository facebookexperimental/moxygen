/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQClient.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <memory>

#include <proxygen/lib/http/session/HTTPTransaction.h>

namespace moxygen {

class MoQWebTransportClient : public MoQClient {
 public:
  MoQWebTransportClient(
      MoQExecutor::KeepAlive exec,
      proxygen::URL url,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr)
      : MoQClient(std::move(exec), std::move(url), std::move(verifier)) {}

  MoQWebTransportClient(
      MoQExecutor::KeepAlive exec,
      proxygen::URL url,
      SessionFactory sessionFactory,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr)
      : MoQClient(
            std::move(exec),
            std::move(url),
            std::move(sessionFactory),
            std::move(verifier)) {}

  folly::coro::Task<void> setupMoQSession(
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds transaction_timeout,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler,
      const quic::TransportSettings& transportSettings,
      const std::vector<std::string>& alpns = {}) noexcept override;

  class HTTPHandler : public proxygen::HTTPTransactionHandler {
   public:
    explicit HTTPHandler(MoQWebTransportClient& client) : client_(client) {}
    ~HTTPHandler() override {
      if (txn_) {
        txn_->setHandler(nullptr);
      }
    }

    void setTransaction(proxygen::HTTPTransaction* txn) noexcept override {
      txn_ = txn;
    }
    void detachTransaction() noexcept override {
      txn_ = nullptr;
    }
    void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> resp) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf>) noexcept override {}
    void onTrailers(std::unique_ptr<proxygen::HTTPHeaders>) noexcept override {}
    void onUpgrade(proxygen::UpgradeProtocol) noexcept override {}
    void onEgressPaused() noexcept override {}
    void onEgressResumed() noexcept override {}

    void onEOM() noexcept override {
      client_.onSessionEnd(folly::none);
    }
    void onError(const proxygen::HTTPException& ex) noexcept override;
    void onWebTransportBidiStream(
        proxygen::HTTPCodec::StreamID,
        proxygen::WebTransport::BidiStreamHandle handle) noexcept override {
      client_.onNewBidiStream(std::move(handle));
    }
    void onWebTransportUniStream(
        proxygen::HTTPCodec::StreamID,
        proxygen::WebTransport::StreamReadHandle* handle) noexcept override {
      client_.onNewUniStream(handle);
    }
    void onDatagram(std::unique_ptr<folly::IOBuf> datagram) noexcept override {
      client_.onDatagram(std::move(datagram));
    }

    MoQWebTransportClient& client_;
    proxygen::HTTPTransaction* txn_{nullptr};
    std::pair<
        folly::coro::Promise<proxygen::WebTransport*>,
        folly::coro::Future<proxygen::WebTransport*>>
        wtContract{folly::coro::makePromiseContract<proxygen::WebTransport*>()};
  };

 private:
  HTTPHandler httpHandler_{*this};
};

inline std::unique_ptr<MoQClient>
makeMoQClient(MoQExecutor::KeepAlive exec, proxygen::URL url, bool useQuic) {
  return useQuic
      ? std::make_unique<MoQClient>(exec, std::move(url))
      : std::make_unique<MoQWebTransportClient>(exec, std::move(url));
}

} // namespace moxygen
