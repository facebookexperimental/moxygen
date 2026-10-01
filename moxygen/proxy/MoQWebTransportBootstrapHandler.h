/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Promise.h>
#include <folly/coro/Task.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>

#include <memory>
#include <optional>
#include <string>

#include "moxygen/MoQSession.h"

namespace moxygen {

struct PendingMoQWebTransportSession {
  proxygen::WebTransportHandler::Ptr handler;
  folly::coro::Future<std::shared_ptr<MoQSession>> session;
};

PendingMoQWebTransportSession makeMoQWebTransportSession(
    std::shared_ptr<MoQExecutor> executor,
    std::shared_ptr<void> keepalive = nullptr);

folly::coro::Task<std::shared_ptr<MoQSession>> establishMoQWebTransportSession(
    std::shared_ptr<MoQSession> session,
    std::string authority,
    std::string path,
    std::optional<std::string> negotiatedProtocol);

} // namespace moxygen
