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

namespace moxygen::samples {

enum class TransportType {
  QUIC,
  QMUX,
  WEB_TRANSPORT,
};

// Builds the transport for a MoQ sample client to use when connecting to a
// relay or peer.
std::unique_ptr<MoQClientBase> makeRelayClientTransport(
    std::shared_ptr<MoQFollyExecutorImpl> executor,
    proxygen::URL url,
    std::shared_ptr<fizz::CertificateVerifier> verifier,
    TransportType transportType);

} // namespace moxygen::samples
