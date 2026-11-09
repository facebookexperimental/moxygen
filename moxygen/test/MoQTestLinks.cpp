/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/test/MoQTestLinks.h"

#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQVersions.h"

namespace moxygen::test {
namespace {

// Enough for 100 requests open at once on draft 16, which spends two IDs per
// request.
constexpr uint64_t kMaxRequestID = 256;

} // namespace

folly::coro::Task<MoQTestLink*> MoQTestLinks::connect(
    std::shared_ptr<Publisher> serverPublisher,
    std::shared_ptr<Subscriber> serverSubscriber) {
  auto link = std::make_unique<MoQTestLink>();
  std::tie(link->clientWt, link->serverWt) =
      proxygen::test::FakeSharedWebTransport::makeSharedWebTransport();
  // Ends its transport once released, as MoQWebTransportBootstrapHandler's
  // sessions do, unless the link went first.
  link->client = std::shared_ptr<MoQRelaySession>(
      new MoQRelaySession(
          folly::MaybeManagedPtr<proxygen::WebTransport>(link->clientWt.get()),
          executor_),
      [weakTransport = std::weak_ptr(link->clientWt),
       weakPeerTransport =
           std::weak_ptr(link->serverWt)](MoQRelaySession* session) {
        const bool closed = session->isClosed();
        delete session;
        auto transport = weakTransport.lock();
        auto peerTransport = weakPeerTransport.lock();
        if (!transport || !peerTransport) {
          return;
        }
        peerTransport->setPeerHandler(nullptr);
        if (!closed) {
          transport->closeSession(
              folly::to_underlying(SessionCloseErrorCode::NO_ERROR));
        }
      });
  link->serverWt->setPeerHandler(link->client.get());
  link->server = std::make_shared<MoQRelaySession>(
      folly::MaybeManagedPtr<proxygen::WebTransport>(link->serverWt.get()),
      *this,
      executor_);
  link->clientWt->setPeerHandler(link->server.get());
  if (auto alpn = getAlpnFromVersion(version_)) {
    link->client->validateAndSetVersionFromAlpn(*alpn);
    link->server->validateAndSetVersionFromAlpn(*alpn);
  }
  link->server->setPublishHandler(std::move(serverPublisher));
  if (serverSubscriber) {
    link->server->setSubscribeHandler(std::move(serverSubscriber));
  }
  link->client->start();
  link->server->start();
  if (useUniControlStreams(version_)) {
    Setup serverSetup;
    serverSetup.params.insertParam(
        SetupParameter{
            folly::to_underlying(SetupKey::MAX_REQUEST_ID), kMaxRequestID});
    link->server->sendSetup(std::move(serverSetup));
  }
  Setup clientSetup;
  clientSetup.params.insertParam(
      SetupParameter{folly::to_underlying(SetupKey::PATH), "/foo"});
  clientSetup.params.insertParam(
      SetupParameter{
          folly::to_underlying(SetupKey::MAX_REQUEST_ID), kMaxRequestID});
  clientSetup.params.insertParam(
      SetupParameter{
          folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE), 16});
  co_await link->client->setup(std::move(clientSetup));
  links_.push_back(std::move(link));
  co_return links_.back().get();
}

void MoQTestLinks::closeClients() {
  for (auto& link : links_) {
    if (link->client) {
      link->client->close(SessionCloseErrorCode::NO_ERROR);
    }
  }
}

void MoQTestLinks::clear() {
  links_.clear();
}

folly::Try<Setup> MoQTestLinks::onClientSetup(
    Setup,
    const std::shared_ptr<MoQSession>&) {
  Setup setup;
  setup.params.insertParam(
      SetupParameter{
          folly::to_underlying(SetupKey::MAX_REQUEST_ID), kMaxRequestID});
  if (!useBidiRequestStreams(version_)) {
    setup.params.insertParam(
        SetupParameter{
            folly::to_underlying(SetupKey::MAX_AUTH_TOKEN_CACHE_SIZE), 16});
  }
  return folly::Try<Setup>(std::move(setup));
}

} // namespace moxygen::test
