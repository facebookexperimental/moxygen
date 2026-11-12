/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Task.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>

#include "moxygen/MoQSession.h"

namespace moxygen {

// Creates the MoQ session for a client WebTransport. Return makeHandler's
// result from the sendWtReq handler factory, then call establish once
// sendWtReq returns.
class MoQWtSessionFactory {
 public:
  struct StatsCallbacks {
    std::shared_ptr<MoQPublisherStatsCallback> publisher;
    std::shared_ptr<MoQSubscriberStatsCallback> subscriber;
  };

  explicit MoQWtSessionFactory(
      std::shared_ptr<MoQExecutor> executor,
      std::shared_ptr<void> keepalive = nullptr,
      StatsCallbacks statsCallbacks = {});

  // The handler creates and starts a session and sends CLIENT_SETUP when the
  // WebTransport starts. negotiatedProtocol must map to a supported MoQT
  // version.
  proxygen::WebTransportHandler::Ptr makeHandler(
      std::string negotiatedProtocol,
      std::string authority,
      std::string path);

  // Waits for the peer's SETUP, for at most setupTimeout if it is set and for
  // the session's setupTimeout otherwise. Throws std::runtime_error if the
  // WebTransport never started or CLIENT_SETUP could not be sent. Releasing the
  // returned session closes it.
  folly::coro::Task<std::shared_ptr<MoQSession>> establish(
      std::optional<std::chrono::milliseconds> setupTimeout = std::nullopt);

 private:
  std::shared_ptr<MoQExecutor> executor_;
  std::shared_ptr<void> keepalive_;
  StatsCallbacks statsCallbacks_;
  std::shared_ptr<MoQSession> session_;
};

} // namespace moxygen
