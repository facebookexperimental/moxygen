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
    if (auto session = relayClient_->getSession()) {
      session->drain();
    }
  }
}

folly::coro::Task<void> MoQTestServer::doRelaySetup(
    int32_t connectTimeout,
    int32_t transactionTimeout) {
  // Every argument that outlives a suspend point below is a named local.  gcc
  // 11 ICEs in build_special_member_call when it has to materialize a
  // temporary with a non-trivial destructor into the coroutine frame.
  const quic::TransportSettings transportSettings;
  const auto alpns = getMoqtProtocols(versions_, true);
  auto setupResult = co_await folly::coro::co_awaitTry(relayClient_->setup(
      /*publisher=*/publisher_,
      /*subscriber=*/nullptr,
      std::chrono::milliseconds(connectTimeout),
      std::chrono::milliseconds(transactionTimeout),
      transportSettings,
      alpns));
  if (setupResult.hasException()) {
    // Nothing observes this coroutine, so an escaping exception would leave
    // the server silently detached from the relay.
    XLOG(ERR) << "Relay setup failed: " << setupResult.exception().what();
    co_return;
  }

  auto session = relayClient_->getSession();
  if (!session) {
    XLOG(ERR) << "Failed to establish relay session";
    co_return;
  }

  // Pass session to onNewSession to treat it like any other client
  onNewSession(session);

  // Publishes 'moq-test-00' and then keeps the session alive with a periodic
  // ping, so an idle relay doesn't time us out.
  std::vector<TrackNamespace> namespaces{TrackNamespace("moq-test-00", "/")};
  co_await relayClient_->run(publisher_, std::move(namespaces));
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

  // The relay-session factory is required so the session supports
  // publishNamespace.
  relayClient_ = std::make_unique<
      MoQRelayClient>(samples::makeRelayClientTransport(
      moqEvb_,
      std::move(url),
      MoQRelaySession::createRelaySessionFactory(),
      std::make_shared<test::InsecureVerifierDangerousDoNotUseInProduction>(),
      transportType));

  // Start async relay setup (schedule on evb, don't block)
  co_withExecutor(workerEvb, doRelaySetup(connectTimeout, transactionTimeout))
      .start();

  return true;
}

} // namespace moxygen
