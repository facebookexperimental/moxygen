/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Task.h>
#include <proxygen/lib/utils/URL.h>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include "moxygen/MoQClient.h"
#include "moxygen/events/MoQFollyExecutorImpl.h"

namespace moxygen {

struct InteropTestResult {
  bool passed{false};
  std::string testName;
  std::chrono::milliseconds duration{0};
  std::string message;
  // Draft negotiated on the wire, not the one requested.
  std::optional<uint64_t> negotiatedVersion;
  // Set only if a later session in the same test negotiated a different draft,
  // which makes negotiatedVersion unrepresentative.
  std::optional<uint64_t> divergentNegotiatedVersion;
};

class MoQInteropClient {
 public:
  MoQInteropClient(
      folly::EventBase* evb,
      proxygen::URL url,
      bool useQuicTransport,
      bool tlsDisableVerify,
      std::chrono::milliseconds connectTimeout,
      std::chrono::milliseconds transactionTimeout,
      const std::string& versions = "");

  ~MoQInteropClient() = default;

  MoQInteropClient(const MoQInteropClient&) = delete;
  MoQInteropClient& operator=(const MoQInteropClient&) = delete;

  folly::coro::Task<InteropTestResult> testSetupOnly();

  folly::coro::Task<InteropTestResult> testAnnounceOnly();

  folly::coro::Task<InteropTestResult> testSubscribeError();

  folly::coro::Task<InteropTestResult> testAnnounceSubscribe();

  folly::coro::Task<InteropTestResult> testPublishNamespaceDone();

  folly::coro::Task<InteropTestResult> testSubscribeBeforeAnnounce();

 private:
  folly::coro::Task<std::unique_ptr<MoQClient>> createAndSetupSession();

  folly::coro::Task<std::unique_ptr<MoQClient>> createAndSetupRelaySession();

  folly::EventBase* evb_;
  proxygen::URL url_;
  bool useQuicTransport_;
  bool tlsDisableVerify_;
  std::chrono::milliseconds connectTimeout_;
  std::chrono::milliseconds transactionTimeout_;
  std::vector<std::string> alpns_;
  std::shared_ptr<MoQFollyExecutorImpl> moqExecutor_;
};

} // namespace moxygen
