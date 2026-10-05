/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <fizz/protocol/CertificateVerifier.h>

namespace moxygen::test {

// This is an insecure certificate verifier and is not meant to be
// used in production. Using it in production would mean that this will
// leave everyone insecure.
class InsecureVerifierDangerousDoNotUseInProduction
    : public fizz::InsecureCertificateVerifier {
 public:
  InsecureVerifierDangerousDoNotUseInProduction()
      : fizz::InsecureCertificateVerifier(fizz::VerificationContext::Client) {}
};

} // namespace moxygen::test
