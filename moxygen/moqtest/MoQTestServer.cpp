/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moqtest/MoQTestServer.h"
#include <folly/coro/Task.h>
#include <folly/logging/xlog.h>
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include "moxygen/samples/util/Utils.h"
#include "moxygen/util/InsecureVerifierDangerousDoNotUseInProduction.h"

std::string kEndpointName = "/test";

namespace moxygen {

MoQTestServer::MoQTestServer(
    std::shared_ptr<MoQTestPublisher> publisher,
    const std::string& cert,
    const std::string& key,
    const std::string& versions)
    : MoQServer(
          quic::samples::createFizzServerContextWithInsecureDefault(
              [&versions]() {
                std::vector<std::string> alpns = {"h3"};
                auto moqt = getMoqtProtocols(versions, true);
                alpns.insert(alpns.end(), moqt.begin(), moqt.end());
                return alpns;
              }(),
              fizz::server::ClientAuthMode::None,
              cert,
              key),
          kEndpointName),
      publisher_(std::move(publisher)),
      versions_(versions) {
  XCHECK(publisher_) << "MoQTestServer requires a publisher";
}

void MoQTestServer::gracefulShutdown() {
  publisher_->cancelAll();
  if (relayClient_) {
    // Drains the session, so the relay sees a clean close, and stops us
    // reconnecting to it afterwards.
    relayClient_->stop();
  }
}

folly::coro::Task<std::unique_ptr<MoQClientBase>> MoQTestServer::connectToRelay(
    proxygen::URL url,
    samples::TransportType transportType,
    int32_t connectTimeout,
    int32_t transactionTimeout) {
  // Every argument that outlives a suspend point below is a named local.  gcc
  // 11 ICEs in build_special_member_call when it has to materialize a
  // temporary with a non-trivial destructor into the coroutine frame.
  const quic::TransportSettings transportSettings;
  const auto alpns = getMoqtProtocols(versions_, true);
  // The relay-session factory is required so the session supports
  // publishNamespace.
  auto moqClient = samples::makeRelayClientTransport(
      moqEvb_,
      std::move(url),
      MoQRelaySession::createRelaySessionFactory(),
      std::make_shared<test::InsecureVerifierDangerousDoNotUseInProduction>(),
      transportType);
  auto setupResult =
      co_await folly::coro::co_awaitTry(moqClient->setupMoQSession(
          std::chrono::milliseconds(connectTimeout),
          std::chrono::milliseconds(transactionTimeout),
          /*publishHandler=*/publisher_,
          /*subscribeHandler=*/nullptr,
          transportSettings,
          alpns));
  if (setupResult.hasException()) {
    // Nothing observes this coroutine, so an escaping exception would leave
    // the server silently detached from the relay.
    XLOG(ERR) << "Relay setup failed: " << setupResult.exception().what();
    co_return nullptr;
  }
  if (!moqClient->moqSession_) {
    XLOG(ERR) << "Failed to establish relay session";
    co_return nullptr;
  }

  // Pass session to onNewSession to treat it like any other client
  onNewSession(moqClient->moqSession_);
  co_return std::move(moqClient);
}

bool MoQTestServer::startRelayClient(
    folly::EventBase* workerEvb,
    const std::string& relayUrl,
    int32_t connectTimeout,
    int32_t transactionTimeout,
    samples::TransportType transportType) {
  proxygen::URL url(relayUrl);
  if (!url.isValid() || !url.hasHost()) {
    XLOG(ERR) << "Invalid relay url: " << relayUrl;
    return false;
  }

  // Create executor wrapping the caller-supplied EB.
  if (!moqEvb_) {
    moqEvb_ = std::make_shared<MoQFollyExecutorImpl>(workerEvb);
  }

  relayClient_ = std::make_unique<ReconnectingRelayClient>(
      [this, url, transportType, connectTimeout, transactionTimeout] {
        return connectToRelay(
            url, transportType, connectTimeout, transactionTimeout);
      });

  // Publishes 'moq-test-00' and then keeps the session alive with a periodic
  // ping, so an idle relay doesn't time us out.  Start async (schedule on evb,
  // don't block).
  std::vector<TrackNamespace> namespaces{TrackNamespace("moq-test-00", "/")};
  co_withExecutor(
      workerEvb, relayClient_->run(publisher_, std::move(namespaces)))
      .start();

  return true;
}

} // namespace moxygen
