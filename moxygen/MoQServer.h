/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/httpserver/samples/hq/HQServer.h>

#include <folly/init/Init.h>
#include <folly/io/async/EventBaseManager.h>

#include "moxygen/MoQSession.h"

namespace moxygen {

class MoQServer {
 public:
  MoQServer(
      uint16_t port,
      std::string cert,
      std::string key,
      std::string endpoint);
  virtual ~MoQServer() = default;

  class ControlVisitor : public MoQSession::ControlVisitor {
   public:
    explicit ControlVisitor(std::shared_ptr<MoQSession> clientSession)
        : clientSession_(std::move(clientSession)) {}

    ~ControlVisitor() override = default;

    void operator()(ClientSetup setup) const override;
    void operator()(ServerSetup) const override;
    void operator()(Announce announce) const override;
    void operator()(SubscribeRequest subscribeReq) const override;
    void operator()(SubscribeUpdate subscribeUpdate) const override;
    void operator()(MaxSubscribeId maxSubscribeId) const override;
    void operator()(Fetch fetch) const override;
    void operator()(FetchCancel fetchCancel) const override;
    void operator()(FetchOk fetchOk) const override;
    void operator()(FetchError fetchError) const override;
    void operator()(Unannounce unannounce) const override;
    void operator()(AnnounceCancel announceCancel) const override;
    void operator()(SubscribeAnnounces subscribeAnnounces) const override;
    void operator()(UnsubscribeAnnounces unsubscribeAnnounces) const override;
    void operator()(SubscribeDone subscribeDone) const override;
    void operator()(Unsubscribe unsubscribe) const override;
    void operator()(TrackStatusRequest trackStatusRequest) const override;
    void operator()(TrackStatus trackStatus) const override;
    void operator()(Goaway goaway) const override;

   protected:
    std::shared_ptr<MoQSession> clientSession_;
  };

  virtual std::unique_ptr<ControlVisitor> makeControlVisitor(
      std::shared_ptr<MoQSession> clientSession) {
    return std::make_unique<ControlVisitor>(std::move(clientSession));
  }

  virtual folly::coro::Task<void> handleClientSession(
      std::shared_ptr<MoQSession> clientSession);

  virtual void terminateClientSession(std::shared_ptr<MoQSession> /*session*/) {
  }

  class Handler : public proxygen::HTTPTransactionHandler {
   public:
    explicit Handler(MoQServer& server) : server_(server) {}

    void setTransaction(proxygen::HTTPTransaction* txn) noexcept override {
      txn_ = txn;
    }
    void detachTransaction() noexcept override {
      txn_ = nullptr;
      delete this;
    }
    void onHeadersComplete(
        std::unique_ptr<proxygen::HTTPMessage> req) noexcept override;
    void onBody(std::unique_ptr<folly::IOBuf>) noexcept override {}
    void onTrailers(std::unique_ptr<proxygen::HTTPHeaders>) noexcept override {}
    void onEOM() noexcept override {
      XLOG(DBG1) << "Session terminated";
      server_.terminateClientSession(std::move(clientSession_));
      if (!txn_->isEgressEOMSeen()) {
        txn_->sendEOM();
      }
    }
    void onUpgrade(proxygen::UpgradeProtocol) noexcept override {}
    void onError(const proxygen::HTTPException& error) noexcept override {
      XLOG(ERR) << error.what();
      server_.terminateClientSession(std::move(clientSession_));
    }
    void onEgressPaused() noexcept override {}
    void onEgressResumed() noexcept override {}
    void onWebTransportBidiStream(
        proxygen::HTTPCodec::StreamID,
        proxygen::WebTransport::BidiStreamHandle handle) noexcept override {
      clientSession_->onNewBidiStream(handle);
    }
    void onWebTransportUniStream(
        proxygen::HTTPCodec::StreamID,
        proxygen::WebTransport::StreamReadHandle* handle) noexcept override {
      clientSession_->onNewUniStream(handle);
    }
    void onDatagram(std::unique_ptr<folly::IOBuf> datagram) noexcept override {
      clientSession_->onDatagram(std::move(datagram));
    }

   private:
    MoQServer& server_;
    proxygen::HTTPTransaction* txn_{nullptr};
    std::shared_ptr<MoQSession> clientSession_;
  };

  [[nodiscard]] const std::string& getEndpoint() const {
    return endpoint_;
  }

 private:
  void createMoQQuicSession(std::shared_ptr<quic::QuicSocket> quicSocket);

  quic::samples::HQServerParams params_;
  std::unique_ptr<quic::samples::HQServer> hqServer_;
  std::string endpoint_;
};
} // namespace moxygen
