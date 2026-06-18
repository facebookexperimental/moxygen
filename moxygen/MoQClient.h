/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Task.h>
#include <proxygen/lib/utils/URL.h>
#include <moxygen/MoQClientBase.h>

namespace moxygen {

class MoQClient : public MoQClientBase {
 public:
  MoQClient(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr,
      bool useQuicWtSession = false)
      : MoQClientBase(
            std::move(exec),
            std::move(url),
            std::move(verifier),
            useQuicWtSession) {}

  [[nodiscard]] quic::
      Expected<quic::QuicSocketLite::FlowControlState, quic::LocalErrorCode>
      getConnectionFlowControl() const {
    if (const auto* quicSocket = getQuicSocket()) {
      return quicSocket->getConnectionFlowControl();
    }
    return quic::make_unexpected(quic::LocalErrorCode::CONNECTION_CLOSED);
  }

  [[nodiscard]] std::optional<quic::TransportInfo> getTransportInfo() const {
    if (const auto* quicSocket = getQuicSocket()) {
      return quicSocket->getTransportInfo();
    }
    return std::nullopt;
  }

  MoQClient(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      SessionFactory sessionFactory,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr,
      bool useQuicWtSession = false)
      : MoQClientBase(
            std::move(exec),
            std::move(url),
            std::move(sessionFactory),
            std::move(verifier),
            useQuicWtSession) {}

 protected:
  folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>> connectQuic(
      folly::SocketAddress connectAddr,
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      const std::vector<std::string>& alpns,
      const quic::TransportSettings& transportSettings) override;
};

} // namespace moxygen
