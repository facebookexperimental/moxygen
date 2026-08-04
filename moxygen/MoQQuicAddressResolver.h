/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Task.h>
#include <quic/mvfst-config.h>

#include <chrono>
#include <cstdint>
#include <string>

namespace moxygen {

class MoQQuicAddressResolver {
 public:
  virtual ~MoQQuicAddressResolver() = default;

  // Implementations select one endpoint and must either return it as an
  // initialized, owning IPv4 or IPv6 address or complete with an exception.
  // They must honor timeout and make late backend callbacks safe after
  // cancellation.
  // The timeout lets a backend abort its in-flight query; callers also enforce
  // the deadline by cancelling the returned task, which must unblock promptly.
  virtual folly::coro::Task<quic::SocketAddress> resolveAddress(
      std::string host,
      uint16_t port,
      std::chrono::milliseconds timeout) = 0;
};

} // namespace moxygen
