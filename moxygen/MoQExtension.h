/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <moxygen/MoQFramer.h>
#include <moxygen/MoQTypes.h>

#include <vector>

namespace moxygen {

// A protocol extension defined outside moxygen: its SETUP options, and the
// message parameters enabled once both SETUPs are known. Add it to a session
// (or a client/server base) before the session sends its SETUP.
// An extension added to a client/server base is shared by all its sessions,
// which may run on different threads.
class MoQExtension {
 public:
  virtual ~MoQExtension() = default;

  // Options added to this endpoint's SETUP. They replace any built-in options
  // with the same keys; one extension may return several with the same key.
  virtual std::vector<SetupParameter> getSetupOptions(
      uint64_t /*version*/) const {
    return {};
  }

  // The message parameters enabled by this extension, given both SETUPs. An
  // error closes the session with that code, after any onClientSetup.
  virtual folly::Expected<MessageParameterExtensions, SessionCloseErrorCode>
  negotiate(
      uint64_t /*version*/,
      const SetupParameters& /*localParams*/,
      const SetupParameters& /*peerParams*/) const {
    return MessageParameterExtensions{};
  }
};

} // namespace moxygen
