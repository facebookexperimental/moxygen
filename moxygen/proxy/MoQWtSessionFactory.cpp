/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQWtSessionFactory.h"

#include <folly/MaybeManagedPtr.h>
#include <proxygen/lib/http/webtransport/SharedWebTransportHandler.h>

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

} // namespace

MoQWtSessionFactory::MoQWtSessionFactory(
    std::shared_ptr<MoQExecutor> executor,
    std::shared_ptr<void> keepalive,
    StatsCallbacks statsCallbacks)
    : executor_(std::move(executor)),
      keepalive_(std::move(keepalive)),
      statsCallbacks_(std::move(statsCallbacks)) {
  if (!executor_) {
    throw std::invalid_argument("executor must not be null");
  }
}

proxygen::WebTransportHandler::Ptr MoQWtSessionFactory::makeHandler(
    std::string negotiatedProtocol,
    std::string authority,
    std::string path) {
  return std::make_unique<proxygen::SharedWebTransportHandler>(
      [this,
       negotiatedProtocol = std::move(negotiatedProtocol),
       authority = std::move(authority),
       path = std::move(path)](
          std::shared_ptr<proxygen::WebTransport> webTransport) mutable
          -> std::shared_ptr<proxygen::WebTransportHandler> {
        // The WebTransport owns this session through the handler until the
        // WebTransport ends. The handle given to the caller keeps the
        // WebTransport alive and closes the session when released.
        auto session = std::make_shared<MoQSession>(
            folly::MaybeManagedPtr<proxygen::WebTransport>(webTransport.get()),
            executor_);
        session->setPublisherStatsCallback(statsCallbacks_.publisher);
        session->setSubscriberStatsCallback(statsCallbacks_.subscriber);
        if (!session->validateAndSetVersionFromAlpn(negotiatedProtocol)) {
          return nullptr;
        }
        session->setAuthority(std::move(authority));
        session->setPath(std::move(path));
        auto handle = std::shared_ptr<MoQSession>(
            session.get(),
            [session,
             webTransport = std::move(webTransport),
             keepalive = std::move(keepalive_)](MoQSession* closing) mutable {
              closing->close(SessionCloseErrorCode::NO_ERROR);
              session.reset();
              webTransport.reset();
              keepalive.reset();
            });
        session->start();
        // CLIENT_SETUP must precede the server's SETUP, which can arrive as
        // soon as the session starts.
        if (session->sendSetup(makeClientSetup()).hasError()) {
          return nullptr;
        }
        session_ = std::move(handle);
        return session;
      });
}

folly::coro::Task<std::shared_ptr<MoQSession>> MoQWtSessionFactory::establish(
    std::optional<std::chrono::milliseconds> setupTimeout) {
  auto session = std::exchange(session_, nullptr);
  if (!session) {
    throw std::runtime_error("failed to start the MoQ session");
  }
  if (setupTimeout) {
    auto settings = session->getMoqSettings();
    settings.setupTimeout = *setupTimeout;
    session->setMoqSettings(settings);
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
