/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQTypes.h>

#include <memory>

namespace moxygen::media_server {

class MoQBroadcast;

// Makes a MoQBroadcast for a namespace. The dispatcher depends only on this
// interface, so it stays free of backend/media concerns; the concrete
// implementation (see MoQBroadcastFactory) owns backend selection and resolver
// wiring.
class BroadcastFactory {
 public:
  virtual ~BroadcastFactory() = default;

  virtual std::shared_ptr<MoQBroadcast> makeBroadcast(
      const TrackNamespace& ns) = 0;
};

} // namespace moxygen::media_server
