#pragma once

#include "moxygen/MoQSession.h"

#include <folly/experimental/coro/Promise.h>
#include <proxygen/lib/http/session/HTTPTransaction.h>
#include <proxygen/lib/utils/URL.h>

namespace moxygen {

class MoQClient {
 public:
  MoQClient(folly::EventBase* evb, proxygen::URL url)
      : evb_(evb), url_(std::move(url)) {}

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
      client_.onWebTransportBidiStream(std::move(handle));
    }
    void onWebTransportUniStream(
        proxygen::HTTPCodec::StreamID,
        proxygen::WebTransport::StreamReadHandle* handle) noexcept override {
      client_.onWebTransportUniStream(handle);
    }
    MoQClient& client_;
    proxygen::HTTPTransaction* txn_{nullptr};
    std::pair<
        folly::coro::Promise<std::shared_ptr<MoQSession>>,
        folly::coro::Future<std::shared_ptr<MoQSession>>>
        sessionContract{
            folly::coro::makePromiseContract<std::shared_ptr<MoQSession>>()};
  };

  std::shared_ptr<MoQSession> moqSession_;
  folly::coro::Task<void> setupMoQSession(
      std::chrono::milliseconds connect_timeout,
      std::chrono::milliseconds transaction_timeout,
      Role role = Role::BOTH) noexcept;

 private:
  void onSessionEnd(folly::Optional<proxygen::HTTPException> ex);
  void onWebTransportBidiStream(
      proxygen::WebTransport::BidiStreamHandle handle);
  void onWebTransportUniStream(
      proxygen::WebTransport::StreamReadHandle* handle);

  folly::EventBase* evb_{nullptr};
  proxygen::URL url_;
  HTTPHandler httpHandler_{*this};
};

} // namespace moxygen
