/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/moqtest/MoQTestServer.h"
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
  publishNamespaceHandle_.reset();
  if (relaySession_) {
    relaySession_->drain();
  }
}

folly::coro::Task<void> MoQTestServer::doRelaySetup(
    const std::string& relayUrl,
    int32_t connectTimeout,
    int32_t transactionTimeout) {
  // Setup MoQ session on the client
  co_await relayClient_->setupMoQSession(
      std::chrono::milliseconds(connectTimeout),
      std::chrono::milliseconds(transactionTimeout),
      /*publishHandler=*/publisher_,
      /*subscribeHandler=*/nullptr,
      quic::TransportSettings(),
      getMoqtProtocols(versions_, true));

  // Get the session
  relaySession_ =
      std::dynamic_pointer_cast<MoQRelaySession>(relayClient_->moqSession_);
  if (!relaySession_) {
    XLOG(ERR) << "Failed to get MoQRelaySession";
    co_return;
  }

  // Send PUBLISH_NAMESPACE for the base namespace "moq-test-00"
  PublishNamespace publishNamespace;
  publishNamespace.trackNamespace = TrackNamespace("moq-test-00", "/");

  auto publishNamespaceResult =
      co_await relaySession_->publishNamespace(publishNamespace);
  if (publishNamespaceResult.hasError()) {
    XLOG(ERR) << "Failed to publishNamespace namespace: "
              << publishNamespaceResult.error().reasonPhrase;
    co_return;
  }

  // Store publishNamespace handle to keep it alive
  publishNamespaceHandle_ = publishNamespaceResult.value();

  XLOG(INFO) << "Successfully published namespace 'moq-test-00' to relay at "
             << relayUrl;

  // Pass session to onNewSession to treat it like any other client
  onNewSession(relaySession_);

  co_return;
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

  // Create client connection with MoQRelaySession factory.
  relayClient_ = samples::makeRelayClientTransport(
      moqEvb_,
      std::move(url),
      MoQRelaySession::createRelaySessionFactory(),
      std::make_shared<test::InsecureVerifierDangerousDoNotUseInProduction>(),
      transportType);

  // Start async relay setup (schedule on evb, don't block)
  co_withExecutor(
      workerEvb, doRelaySetup(relayUrl, connectTimeout, transactionTimeout))
      .start();

  return true;
}

} // namespace moxygen
