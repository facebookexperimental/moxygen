/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fizz/protocol/CertificateVerifier.h>
#include <moxygen/MoQQmuxClient.h>

namespace moxygen {

std::shared_ptr<QmuxTransportFactory> makeFollyQmuxTransportFactory(
    std::shared_ptr<MoQExecutor> exec,
    std::shared_ptr<fizz::CertificateVerifier> verifier);

} // namespace moxygen
