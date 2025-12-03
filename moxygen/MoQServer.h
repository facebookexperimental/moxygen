/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <proxygen/httpserver/samples/hq/HQServer.h>
#include <moxygen/mlog/MLogger.h>

#include <folly/init/Init.h>
#include <folly/io/async/EventBaseManager.h>

#include <utility>

#include "moxygen/MoQSession.h"

namespace moxygen {

const std::string kDefaultFilePath =
    "ti/experimental/moxygen/moqtest/mlog_server.txt";

class MoQServer : public MoQSession::ServerSetupCallback {
 public:
  MoQServer(std::string cert, std::string key, std::string endpoint);

  MoQServer(
      std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
      std::string endpoint);

  void start(
      const folly::SocketAddress& addr,
      std::vector<folly::EventBase*> evbs = {});

  MoQServer(const MoQServer&) = delete;
  MoQServer(MoQServer&&) = delete;
  MoQServer& operator=(const MoQServer&) = delete;
  MoQServer& operator=(MoQServer&&) = delete;
  virtual ~MoQServer() = default;

  virtual void onNewSession(std::shared_ptr<MoQSession> clientSession) = 0;
  virtual void terminateClientSession(std::shared_ptr<MoQSession> /*session*/) {
  }

  std::vector<folly::EventBase*> getWorkerEvbs() const noexcept {
    return hqServer_->getWorkerEvbs();
  }

  void setLogger(std::shared_ptr<MLogger> logger);
  std::shared_ptr<MLogger> getLogger() const;

  // QUIC stats factory setter
  void setQuicStatsFactory(
      std::unique_ptr<quic::QuicTransportStatsCallbackFactory> factory);

  void stop();

  folly::Try<ServerSetup> onClientSetup(
      ClientSetup clientSetup,
      const std::shared_ptr<MoQSession>& session) override;

  folly::Expected<folly::Unit, SessionCloseErrorCode> validateAuthority(
      const ClientSetup& clientSetup,
      uint64_t negotiatedVersion,
      std::shared_ptr<MoQSession> session) override;

  // Takeover runtime wrapper methods - forward to underlying QuicServer
  // Takeover part 1: Methods called on the old instance.
  void allowBeingTakenOver(const folly::SocketAddress& addr);
  int getTakeoverHandlerSocketFD() const;
  std::vector<int> getAllListeningSocketFDs() const;

  // Takeover part 2: Methods called during the initialization of the new
  // process.
  void setListeningFDs(const std::vector<int>& fds);
  void setProcessId(quic::ProcessId pid);
  void setHostId(uint32_t hostId);
  void setConnectionIdVersion(quic::ConnectionIdVersion version);
  void waitUntilInitialized();

  // Takeover part 3: Methods called during the packet forwarding setup.
  quic::ProcessId getProcessId() const;
  quic::TakeoverProtocolVersion getTakeoverProtocolVersion() const;
  void startPacketForwarding(const folly::SocketAddress& addr);

  // Takeover part 4: Methods called on the old instance to wind down.
  void rejectNewConnections(std::function<bool()> rejectFn);
  void pauseRead();

  void setFizzContext(
      std::shared_ptr<const fizz::server::FizzServerContext> ctx);

  void setFizzContext(
      folly::EventBase* evb,
      std::shared_ptr<const fizz::server::FizzServerContext> ctx);

 protected:
  virtual std::shared_ptr<MoQSession> createSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      std::shared_ptr<MoQExecutor> executor);

  // Register ALPN handlers for direct QUIC connections (internal use)
  void registerAlpnHandler(const std::vector<std::string>& alpns);

 private:
  // AUTHORITY parameter validation methods
  // Not called for HTTP inside proxygen, we leave it to applications.
  bool isValidAuthorityFormat(const std::string& authority);
  bool isSupportedAuthority(const std::string& authority);

  folly::coro::Task<void> handleClientSession(
      std::shared_ptr<MoQSession> clientSession);

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
      XLOG(DBG1) << "WebTransport session terminated";
      onSessionEnd(folly::none);
      if (!txn_->isEgressEOMSeen()) {
        txn_->sendEOM();
      }
    }
    void onUpgrade(proxygen::UpgradeProtocol) noexcept override {}
    void onError(const proxygen::HTTPException& error) noexcept override {
      XLOG(ERR) << folly::exceptionStr(error);
      onSessionEnd(proxygen::WebTransport::kInternalError);
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
    void onSessionEnd(folly::Optional<uint32_t> err) {
      if (clientSession_) {
        clientSession_->onSessionEnd(std::move(err));
        clientSession_.reset();
      }
    }
    MoQServer& server_;
    proxygen::HTTPTransaction* txn_{nullptr};
    std::shared_ptr<MoQSession> clientSession_;
  };

  [[nodiscard]] const std::string& getEndpoint() const {
    return endpoint_;
  }

  void createMoQQuicSession(std::shared_ptr<quic::QuicSocket> quicSocket);

  std::string cert_;
  std::string key_;
  quic::samples::HQServerParams params_;
  std::shared_ptr<const fizz::server::FizzServerContext> fizzContext_;
  std::unique_ptr<quic::samples::HQServerTransportFactory> factory_;
  std::unique_ptr<quic::samples::HQServer> hqServer_;
  std::string endpoint_;
  std::shared_ptr<MLogger> logger_;
};
} // namespace moxygen
