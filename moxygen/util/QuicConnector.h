/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#pragma once

#include <fizz/protocol/CertificateVerifier.h>
#include <folly/SocketAddress.h>
#include <folly/coro/Task.h>
#include <folly/io/async/EventBase.h>

namespace quic {
class QuicClientTransport;
}

namespace moxygen {

class QuicConnector {
 public:
  static folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>>
  connectQuic(
      folly::EventBase* eventBase,
      folly::SocketAddress connectAddr,
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      std::string alpn = "moq-00");
};

} // namespace moxygen
