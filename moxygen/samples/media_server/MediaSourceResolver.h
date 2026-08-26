/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQTypes.h>
#include <moxygen/samples/media_server/MoQMediaSource.h>

#include <folly/coro/Task.h>

#include <memory>
#include <optional>
#include <string>

namespace moxygen::media_server {

// Resolves a request to a backend on demand, selected by the namespace's first
// tuple field. The call is per-request and lazy: nothing is opened until a
// client actually asks, and it is scoped to exactly the one track asked for.
// The catalog is just another track (name == kCatalogTrackName), returned as a
// static single-object source, so it needs no separate entry point. Coroutine
// on purpose - a backend can perform async I/O; the file implementation
// completes inline.
class MediaSourceResolver {
 public:
  virtual ~MediaSourceResolver() = default;

  // A per-track segment feed for (ns, trackName), including the catalog track.
  // nullptr if the namespace / backend is unknown or the track does not exist.
  virtual folly::coro::Task<std::shared_ptr<SegmentSource>> openTrack(
      const TrackNamespace& ns,
      const std::string& trackName) = 0;
};

} // namespace moxygen::media_server
