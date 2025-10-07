/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Task.h>
#include <proxygen/lib/utils/URL.h>
#include <moxygen/MoQClientBase.h>

namespace moxygen {

class MoQClient : public MoQClientBase {
 public:
  MoQClient(std::shared_ptr<MoQExecutor> exec, proxygen::URL url)
      : MoQClientBase(std::move(exec), std::move(url)) {}

  [[nodiscard]] quic::
      Expected<quic::QuicSocketLite::FlowControlState, quic::LocalErrorCode>
      getConnectionFlowControl() const {
    if (!quicWebTransport_) {
      return quic::make_unexpected(quic::LocalErrorCode::CONNECTION_CLOSED);
    }
    return quicWebTransport_->getConnectionFlowControl();
  }

  [[nodiscard]] folly::Optional<quic::TransportInfo> getTransportInfo() const {
    if (!quicWebTransport_) {
      return folly::none;
    }
    return quicWebTransport_->getTransportInfo();
  }

 protected:
  folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>> connectQuic(
      folly::SocketAddress connectAddr,
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      std::string alpn,
      const quic::TransportSettings& transportSettings) override;
};

} // namespace moxygen
