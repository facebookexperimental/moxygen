/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <folly/coro/Task.h>
#include <memory>
#include <string>

#include "moxygen/MoQTypes.h"

namespace moxygen {

class MoQSession;

struct MoQUpstreamProviderError {
  std::string message;
};

using MoQUpstreamSessionResult =
    folly::Expected<std::shared_ptr<MoQSession>, MoQUpstreamProviderError>;

class MoQUpstreamProvider {
 public:
  virtual ~MoQUpstreamProvider();

  virtual folly::coro::Task<MoQUpstreamSessionResult> getSession(
      const FullTrackName& fullTrackName,
      const TrackRequestParameters& params,
      bool hasFallbackProvider /* used for logging/stats */) = 0;
};

} // namespace moxygen
