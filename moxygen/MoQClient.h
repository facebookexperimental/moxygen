/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQSession.h"

#include <folly/coro/Promise.h>
#include <proxygen/lib/http/session/HTTPTransaction.h>
#include <proxygen/lib/http/webtransport/QuicWebTransport.h>
#include <proxygen/lib/utils/URL.h>

namespace moxygen {

class MoQClient : public proxygen::WebTransportHandler {
 public:
  enum class TransportType { H3_WEBTRANSPORT, QUIC };
  MoQClient(
      folly::EventBase* evb,
      proxygen::URL url,
      TransportType ttype = TransportType::H3_WEBTRANSPORT)
      : evb_(evb), url_(std::move(url)), transportType_(ttype) {}

  folly::EventBase* getEventBase() {
    return evb_;
  }

  class HTTPHandler : public proxygen::HTTPTransactionHandler {
   public:
    explicit HTTPHandler(MoQClient& client) : client_(client) {}

    void setTransaction(proxygen::HTTPTransaction* txn) noexcept override {
      txn_ = txn;
    }
    void detachTransaction() noexcept override {}
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

    MoQClient& client_;
    proxygen::HTTPTransaction* txn_{nullptr};
    std::pair<
        folly::coro::Promise<proxygen::WebTransport*>,
        folly::coro::Future<proxygen::WebTransport*>>
        wtContract{folly::coro::makePromiseContract<proxygen::WebTransport*>()};
  };

  std::shared_ptr<MoQSession> moqSession_;
  folly::coro::Task<void> setupMoQSession(
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds transaction_timeout,
      Role role = Role::PUB_AND_SUB) noexcept;

 private:
  std::shared_ptr<MoQSession> setupMoQSessionImpl(
      proxygen::WebTransport* wt,
      folly::EventBase* evb,
      Role role,
      folly::Optional<std::string> path);

  void onSessionEnd(folly::Optional<uint32_t>) override;
  void onNewBidiStream(
      proxygen::WebTransport::BidiStreamHandle handle) override;
  void onNewUniStream(
      proxygen::WebTransport::StreamReadHandle* handle) override;
  void onDatagram(std::unique_ptr<folly::IOBuf>) override;

  folly::EventBase* evb_{nullptr};
  proxygen::URL url_;
  HTTPHandler httpHandler_{*this};
  TransportType transportType_;
  std::shared_ptr<proxygen::QuicWebTransport> quicWebTransport_;
};

} // namespace moxygen
