/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fizz/protocol/CertificateVerifier.h>
#include <proxygen/lib/transport/qmux/QmuxSession.h>
#include <moxygen/MoQClientBase.h>

namespace folly {
class AsyncTransport;
}

namespace moxygen {

// MoQClient that runs the MoQ session over a QMUX-on-TCP transport instead of
// QUIC.  The base WebTransport surface that MoQSession programs against is
// satisfied by QmuxSession (which is-a WebTransport via WtSessionBase), so no
// changes to MoQSession itself are required.
class MoQQmuxClient : public MoQClientBase {
 public:
  MoQQmuxClient(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr);

  // Overload that lets the caller install a custom session factory, e.g.
  // MoQRelaySession::createRelaySessionFactory() to enable publishNamespace /
  // subscribeNamespace on the resulting session. Mirrors the corresponding
  // overload on MoQClient.
  MoQQmuxClient(
      std::shared_ptr<MoQExecutor> exec,
      proxygen::URL url,
      SessionFactory sessionFactory,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr);

  ~MoQQmuxClient() override;

  folly::coro::Task<void> setupMoQSession(
      std::chrono::milliseconds connectTimeout,
      std::chrono::milliseconds transactionTimeout,
      std::shared_ptr<Publisher> publishHandler,
      std::shared_ptr<Subscriber> subscribeHandler,
      const quic::TransportSettings& transportSettings,
      const std::vector<std::string>& alpns = {}) override;

  // Returns the underlying byte-stream transport (Fizz-over-TCP) carrying the
  // QMUX session, or nullptr before setup completes / after teardown. Used to
  // surface TCP-level transport stats; the transport is owned by the session.
  folly::AsyncTransport* getUnderlyingTransport() const;

 protected:
  // QMUX has no QUIC connection. The base class uses connectQuic() only from
  // its default setupMoQSession(), which we override above; this implementation
  // exists solely to satisfy the pure-virtual contract.
  folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>> connectQuic(
      folly::SocketAddress connectAddr,
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      const std::vector<std::string>& alpns,
      const quic::TransportSettings& transportSettings) override;

 private:
  proxygen::qmux::QmuxSession::Ptr qmuxSession_;
};

} // namespace moxygen
