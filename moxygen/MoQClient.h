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
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr)
      : MoQClientBase(std::move(exec), std::move(url), std::move(verifier)) {}

  [[nodiscard]] quic::
      Expected<quic::QuicSocketLite::FlowControlState, quic::LocalErrorCode>
      getConnectionFlowControl() const {
    auto quicSocket = getActiveQuicSocket();
    if (!quicSocket) {
      return quic::make_unexpected(quic::LocalErrorCode::CONNECTION_CLOSED);
    }
    return quicSocket->getConnectionFlowControl();
  }

  [[nodiscard]] std::optional<quic::TransportInfo> getTransportInfo() const {
    auto quicSocket = getActiveQuicSocket();
    if (!quicSocket) {
      return std::nullopt;
    }
    return quicSocket->getTransportInfo();
  }

  MoQClient(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      SessionFactory sessionFactory,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr)
      : MoQClientBase(
            std::move(exec),
            std::move(url),
            std::move(sessionFactory),
            std::move(verifier)) {}

 protected:
  folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>> connectQuic(
      folly::SocketAddress connectAddr,
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      const std::vector<std::string>& alpns,
      const quic::TransportSettings& transportSettings) override;

 private:
  [[nodiscard]] std::shared_ptr<const quic::QuicSocket> getActiveQuicSocket()
      const {
    if (!quicWebTransport_ || !moqSession_ || moqSession_->isClosed()) {
      return nullptr;
    }
    return quicWebTransport_->getQuicSocket();
  }
};

} // namespace moxygen
