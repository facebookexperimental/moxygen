/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Promise.h>
#include <proxygen/lib/http/webtransport/WebTransport.h>

#include <memory>

#include "moxygen/MoQSession.h"

namespace moxygen {

struct PendingMoQWebTransportSession {
  proxygen::WebTransportHandler::Ptr handler;
  folly::coro::Future<std::shared_ptr<MoQSession>> session;
};

PendingMoQWebTransportSession makeMoQWebTransportSession(
    std::shared_ptr<MoQExecutor> executor);

} // namespace moxygen
