/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fizz/protocol/CertificateVerifier.h>
#include <proxygen/lib/utils/URL.h>
#include <moxygen/MoQClientBase.h>
#include <moxygen/events/MoQFollyExecutorImpl.h>

#include <memory>
#include <optional>
#include <string>

namespace moxygen::samples {

enum class TransportType {
  QUIC,
  QMUX,
  WEB_TRANSPORT,
};

// Parse the new --transport flag for sample clients. Accepts:
//   "quic"  -> raw QUIC
//   "h3wt"  -> HTTP/3 + WebTransport
//   "qmux"  -> QMUX-on-TCP
// Returns std::nullopt for any other value; callers should treat that as a
// fatal flag-parsing error.
std::optional<TransportType> parseTransportType(const std::string& s);

// Resolve client transport from the new --transport flag and the deprecated
// boolean --quic_transport flag. Caller passes gflags flag NAMES (not values);
// the helper reads `current_value` / `is_default` directly so it can detect
// whether --quic_transport was explicitly set. Fatals on conflict (both
// flags explicitly set) or on an invalid --transport value. Emits a
// deprecation XLOG(WARNING) when --quic_transport is used.
TransportType selectClientTransport(
    const char* transportFlagName,
    const char* quicTransportFlagName);

// Builds the transport for a MoQ sample client to use when connecting to a
// relay or peer.
std::unique_ptr<MoQClientBase> makeRelayClientTransport(
    std::shared_ptr<MoQFollyExecutorImpl> executor,
    proxygen::URL url,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    TransportType transportType);

// Same as above, but with a custom session factory (e.g.
// MoQRelaySession::createRelaySessionFactory() to enable publishNamespace /
// subscribeNamespace on the resulting session).
std::unique_ptr<MoQClientBase> makeRelayClientTransport(
    std::shared_ptr<MoQFollyExecutorImpl> executor,
    proxygen::URL url,
    MoQClientBase::SessionFactory sessionFactory,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    TransportType transportType);

} // namespace moxygen::samples
