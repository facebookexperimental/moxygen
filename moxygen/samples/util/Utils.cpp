/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/util/Utils.h>

#include <folly/lang/Assume.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQQmuxClient.h>
#include <moxygen/MoQRelaySession.h>
#include <moxygen/MoQWebTransportClient.h>

namespace moxygen::samples {

std::unique_ptr<MoQClientBase> makeRelayClientTransport(
    std::shared_ptr<MoQFollyExecutorImpl> executor,
    proxygen::URL url,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    TransportType transportType) {
  auto sessionFactory = MoQRelaySession::createRelaySessionFactory();
  switch (transportType) {
    case TransportType::QMUX:
      return std::make_unique<MoQQmuxClient>(
          std::move(executor),
          std::move(url),
          std::move(sessionFactory),
          std::move(verifier));
    case TransportType::QUIC:
      return std::make_unique<MoQClient>(
          std::move(executor),
          std::move(url),
          std::move(sessionFactory),
          std::move(verifier));
    case TransportType::WEB_TRANSPORT:
      return std::make_unique<MoQWebTransportClient>(
          std::move(executor),
          std::move(url),
          std::move(sessionFactory),
          std::move(verifier));
  }
  folly::assume_unreachable();
}

} // namespace moxygen::samples
