/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/util/Utils.h>

#include <folly/lang/Assume.h>
#include <folly/logging/xlog.h>
#include <folly/portability/GFlags.h>
#include <moxygen/MoQClient.h>
#include <moxygen/MoQQmuxClient.h>
#include <moxygen/MoQRelaySession.h>
#include <moxygen/MoQWebTransportClient.h>

namespace moxygen::samples {

std::optional<TransportType> parseTransportType(const std::string& s) {
  if (s == "quic") {
    return TransportType::QUIC;
  }
  if (s == "h3wt") {
    return TransportType::WEB_TRANSPORT;
  }
  if (s == "qmux") {
    return TransportType::QMUX;
  }
  return std::nullopt;
}

TransportType selectClientTransport(
    const char* transportFlagName,
    const char* quicTransportFlagName) {
  gflags::CommandLineFlagInfo transportInfo;
  gflags::CommandLineFlagInfo quicInfo;
  CHECK(gflags::GetCommandLineFlagInfo(transportFlagName, &transportInfo))
      << "--" << transportFlagName << " not registered";
  CHECK(gflags::GetCommandLineFlagInfo(quicTransportFlagName, &quicInfo))
      << "--" << quicTransportFlagName << " not registered";

  if (!quicInfo.is_default && !transportInfo.is_default) {
    XLOG(FATAL) << "--" << quicTransportFlagName << " (deprecated) and --"
                << transportFlagName << " are mutually exclusive";
  }

  if (!quicInfo.is_default) {
    bool useQuic = (quicInfo.current_value == "true");
    XLOG(WARNING) << "--" << quicTransportFlagName << " is deprecated; use --"
                  << transportFlagName << "=" << (useQuic ? "quic" : "h3wt");
    return useQuic ? TransportType::QUIC : TransportType::WEB_TRANSPORT;
  }

  auto parsed = parseTransportType(transportInfo.current_value);
  XLOG_IF(FATAL, !parsed) << "Invalid --" << transportFlagName << "="
                          << transportInfo.current_value
                          << " (must be one of: quic, h3wt, qmux)";
  return *parsed;
}

std::unique_ptr<MoQClientBase> makeRelayClientTransport(
    std::shared_ptr<MoQFollyExecutorImpl> executor,
    proxygen::URL url,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    TransportType transportType) {
  return makeRelayClientTransport(
      std::move(executor),
      std::move(url),
      MoQRelaySession::createRelaySessionFactory(),
      std::move(verifier),
      transportType);
}

std::unique_ptr<MoQClientBase> makeRelayClientTransport(
    std::shared_ptr<MoQFollyExecutorImpl> executor,
    proxygen::URL url,
    MoQClientBase::SessionFactory sessionFactory,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    TransportType transportType) {
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
