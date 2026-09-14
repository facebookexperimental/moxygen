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

class MoQWebTransportBootstrapHandler final
    : public proxygen::WebTransportHandler {
 public:
  MoQWebTransportBootstrapHandler(
      std::shared_ptr<MoQExecutor> executor,
      folly::coro::Promise<std::shared_ptr<MoQSession>> sessionPromise)
      : executor_(std::move(executor)),
        sessionPromise_(std::move(sessionPromise)) {}

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
          [webTransport =
               std::move(webTransport)](MoQSession* sessionToDelete) mutable {
            sessionToDelete->close(SessionCloseErrorCode::NO_ERROR);
            delete sessionToDelete;
            webTransport.reset();
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
};

} // namespace

PendingMoQWebTransportSession makeMoQWebTransportSession(
    std::shared_ptr<MoQExecutor> executor) {
  if (!executor) {
    throw std::invalid_argument("executor must not be null");
  }

  auto [sessionPromise, sessionFuture] =
      folly::coro::makePromiseContract<std::shared_ptr<MoQSession>>();
  return {
      .handler = std::make_unique<MoQWebTransportBootstrapHandler>(
          std::move(executor), std::move(sessionPromise)),
      .session = std::move(sessionFuture),
  };
}

} // namespace moxygen
