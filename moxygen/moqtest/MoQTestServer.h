/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <utility>

#include "moxygen/MoQQmuxServer.h"
#include "moxygen/MoQServer.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"
#include "moxygen/moqtest/MoQTestPublisher.h"
#include "moxygen/relay/MoQRelayClient.h"
#include "moxygen/samples/util/Utils.h"

namespace moxygen {

// QUIC/WebTransport listener for moq-test. Track data comes from the supplied
// MoQTestPublisher, which is shared with MoQTestQmuxServer when both listeners
// are enabled.
class MoQTestServer : public moxygen::MoQServer {
 public:
  explicit MoQTestServer(
      std::shared_ptr<MoQTestPublisher> publisher,
      const std::string& cert = "",
      const std::string& key = "",
      const std::string& versions = "");

  //  Override onNewSession to set publisher handler
  virtual void onNewSession(
      std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(publisher_);
    // Use server-level logger if set, otherwise try factory
    if (auto logger = createLogger()) {
      clientSession->setLogger(std::move(logger));
    }
  }

  // Drains the relay-client session (when --relay_url is set) so the relay
  // sees a clean close instead of an idle timeout. Must run on the worker
  // EventBase. Listening sessions are torn down by stop().
  void gracefulShutdown();

  // Relay client support. Workers come from an externally-supplied EventBase
  // (use the QUIC server's worker pool when QUIC is running, otherwise the
  // QMUX server's worker pool).
  bool startRelayClient(
      folly::EventBase* workerEvb,
      const std::string& relayUrl,
      int32_t connectTimeout,
      int32_t transactionTimeout,
      samples::TransportType transportType);

 private:
  folly::coro::Task<void> doRelaySetup(
      int32_t connectTimeout,
      int32_t transactionTimeout);

  std::shared_ptr<MoQTestPublisher> publisher_;

  // Upstream session to the relay (if using relay mode)
  std::string versions_;
  std::unique_ptr<MoQRelayClient> relayClient_;
  std::shared_ptr<MoQFollyExecutorImpl> moqEvb_;
};

// QMUX-on-TCP variant of MoQTestServer, sharing its MoQTestPublisher.
class MoQTestQmuxServer : public MoQQmuxServer {
 public:
  MoQTestQmuxServer(
      std::shared_ptr<MoQTestPublisher> publisher,
      std::string endpoint,
      std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
      Config config = {})
      : MoQQmuxServer(
            std::move(endpoint),
            std::move(fizzContext),
            std::move(config)),
        publisher_(std::move(publisher)) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(publisher_);
    if (auto logger = createLogger()) {
      clientSession->setLogger(logger);
    }
  }

 private:
  std::shared_ptr<MoQTestPublisher> publisher_;
};

} // namespace moxygen
