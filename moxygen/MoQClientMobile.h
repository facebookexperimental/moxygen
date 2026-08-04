/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Task.h>
#include <proxygen/lib/utils/URL.h>
#include <quic/state/TransportSettings.h>
#include <moxygen/MoQClientBase.h>
#include <moxygen/MoQQuicAddressResolver.h>

namespace moxygen {
class MoQLibevExecutorImpl;

class MoQClientMobile : public MoQClientBase {
 public:
  MoQClientMobile(
      std::shared_ptr<MoQLibevExecutorImpl> moqEvb,
      proxygen::URL url,
      std::shared_ptr<fizz::CertificateVerifier> verifier = nullptr,
      bool useQuicWtSession = true,
      std::shared_ptr<MoQQuicAddressResolver> addressResolver = nullptr);

 protected:
  folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>> connectQuic(
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      const std::vector<std::string>& alpns,
      const quic::TransportSettings& transportSettings) override;

 private:
  std::shared_ptr<MoQLibevExecutorImpl> moqlibevEvb_;
  std::shared_ptr<MoQQuicAddressResolver> addressResolver_;
};

} // namespace moxygen
