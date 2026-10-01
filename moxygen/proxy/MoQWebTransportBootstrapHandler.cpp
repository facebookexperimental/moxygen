/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQWebTransportBootstrapHandler.h"

#include <folly/MaybeManagedPtr.h>

#include <stdexcept>
#include <utility>

namespace moxygen {
namespace {

Setup makeClientSetup() {
  Setup setup;
  setup.params.insertParam(Parameter(
      folly::to_underlying(SetupKey::MAX_REQUEST_ID), kDefaultMaxRequestID));
  setup.params.insertParam(Parameter(
      folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE),
      kDefaultMaxAuthTokenCacheSize));
  return setup;
}

class MoQWebTransportBootstrapHandler final
    : public proxygen::WebTransportHandler {
 public:
  MoQWebTransportBootstrapHandler(
      std::shared_ptr<MoQExecutor> executor,
      folly::coro::Promise<std::shared_ptr<MoQSession>> sessionPromise,
      std::shared_ptr<void> keepalive)
      : executor_(std::move(executor)),
        sessionPromise_(std::move(sessionPromise)),
        keepalive_(std::move(keepalive)) {}

  MoQWebTransportBootstrapHandler(const MoQWebTransportBootstrapHandler&) =
      delete;
  MoQWebTransportBootstrapHandler& operator=(
      const MoQWebTransportBootstrapHandler&) = delete;
  MoQWebTransportBootstrapHandler(MoQWebTransportBootstrapHandler&&) = delete;
  MoQWebTransportBootstrapHandler& operator=(
      MoQWebTransportBootstrapHandler&&) = delete;

  ~MoQWebTransportBootstrapHandler() override {
    failPendingSession();
  }

  void onNewUniStream(
      proxygen::WebTransport::StreamReadHandle* readHandle) noexcept override {
    if (auto session = session_.lock()) {
      session->onNewUniStream(readHandle);
    }
  }

  void onNewBidiStream(
      proxygen::WebTransport::BidiStreamHandle bidiHandle) noexcept override {
    if (auto session = session_.lock()) {
      session->onNewBidiStream(bidiHandle);
    }
  }

  void onDatagram(std::unique_ptr<folly::IOBuf> datagram) noexcept override {
    if (auto session = session_.lock()) {
      session->onDatagram(std::move(datagram));
    }
  }

  void onSessionEnd(folly::Optional<uint32_t> error) noexcept override {
    if (auto session = session_.lock()) {
      session->onSessionEnd(error);
    } else {
      failPendingSession();
    }
  }

  void onSessionDrain() noexcept override {
    if (auto session = session_.lock()) {
      session->onSessionDrain();
    }
  }

  void onWebTransportSession(
      std::shared_ptr<proxygen::WebTransport> webTransport) noexcept override {
    if (sessionPromise_.isFulfilled()) {
      return;
    }

    try {
      auto* webTransportPtr = webTransport.get();
      auto session = std::shared_ptr<MoQSession>(
          new MoQSession(
              folly::MaybeManagedPtr<proxygen::WebTransport>(webTransportPtr),
              executor_),
          [webTransport = std::move(webTransport),
           keepalive =
               std::move(keepalive_)](MoQSession* sessionToDelete) mutable {
            sessionToDelete->close(SessionCloseErrorCode::NO_ERROR);
            delete sessionToDelete;
            webTransport.reset();
            keepalive.reset();
          });
      session_ = session;
      sessionPromise_.setValue(std::move(session));
    } catch (...) {
      sessionPromise_.setException(std::current_exception());
    }
  }

 private:
  void failPendingSession() noexcept {
    if (!sessionPromise_.isFulfilled()) {
      sessionPromise_.setException(
          std::runtime_error(
              "WebTransport ended before creating an MoQ session"));
    }
  }

  std::shared_ptr<MoQExecutor> executor_;
  std::weak_ptr<MoQSession> session_;
  folly::coro::Promise<std::shared_ptr<MoQSession>> sessionPromise_;
  std::shared_ptr<void> keepalive_;
};

} // namespace

PendingMoQWebTransportSession makeMoQWebTransportSession(
    std::shared_ptr<MoQExecutor> executor,
    std::shared_ptr<void> keepalive) {
  if (!executor) {
    throw std::invalid_argument("executor must not be null");
  }

  auto [sessionPromise, sessionFuture] =
      folly::coro::makePromiseContract<std::shared_ptr<MoQSession>>();
  return {
      .handler = std::make_unique<MoQWebTransportBootstrapHandler>(
          std::move(executor), std::move(sessionPromise), std::move(keepalive)),
      .session = std::move(sessionFuture),
  };
}

folly::coro::Task<std::shared_ptr<MoQSession>> establishMoQWebTransportSession(
    std::shared_ptr<MoQSession> session,
    std::string authority,
    std::string path,
    std::optional<std::string> negotiatedProtocol) {
  session->setAuthority(std::move(authority));
  session->setPath(std::move(path));
  if (negotiatedProtocol) {
    session->validateAndSetVersionFromAlpn(*negotiatedProtocol);
  }
  session->start();

  const auto sendResult = session->sendSetup(makeClientSetup());
  if (sendResult.hasError()) {
    session->close(SessionCloseErrorCode::INTERNAL_ERROR);
    throw std::runtime_error("failed to send MoQ CLIENT_SETUP");
  }

  try {
    co_await session->awaitPeerSetup();
  } catch (...) {
    session->close(SessionCloseErrorCode::VERSION_NEGOTIATION_FAILED);
    throw;
  }
  co_return session;
}

} // namespace moxygen
