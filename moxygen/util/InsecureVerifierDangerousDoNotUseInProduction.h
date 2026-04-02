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
    : public fizz::CertificateVerifier {
 public:
  ~InsecureVerifierDangerousDoNotUseInProduction() override = default;

  fizz::Status verify(
      std::shared_ptr<const fizz::Cert>& ret,
      fizz::Error& /* err */,
      const std::vector<std::shared_ptr<const fizz::PeerCert>>& certs)
      const override {
    ret = certs.front();
    return fizz::Status::Success;
  }

  std::vector<fizz::Extension> getCertificateRequestExtensions()
      const override {
    return std::vector<fizz::Extension>();
  }
};

} // namespace moxygen::test
