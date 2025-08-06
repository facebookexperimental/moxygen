/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/events/MoQLibevExecutorImpl.h>

#include <folly/coro/Task.h>
#include <proxygen/lib/utils/URL.h>
#include <moxygen/MoQClientBase.h>

namespace moxygen {

class MoQClientMobile : public MoQClientBase {
 public:
  MoQClientMobile(
      std::shared_ptr<MoQLibevExecutorImpl> moqEvb,
      proxygen::URL url)
      : MoQClientBase(moqEvb.get(), url), moqlibevEvb_(moqEvb) {}

 protected:
  folly::coro::Task<std::shared_ptr<quic::QuicClientTransport>> connectQuic(
      folly::SocketAddress connectAddr,
      std::chrono::milliseconds timeoutMs,
      std::shared_ptr<fizz::CertificateVerifier> verifier,
      std::string alpn) override;

 private:
  std::shared_ptr<MoQLibevExecutorImpl> moqlibevEvb_;
};

} // namespace moxygen
