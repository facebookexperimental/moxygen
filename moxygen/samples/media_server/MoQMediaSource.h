/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQTypes.h>

#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/Task.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace moxygen::media_server {

enum class TrackKind : uint8_t { Video, Audio, Subtitle, Data, Catalog };

// How the publish loop maps a track's objects onto MoQ streams.
enum class ForwardMode : uint8_t {
  SubgroupPerGroup, // one stream per group/GoP (video)
  StreamPerObject,  // one stream per object (audio)
  Datagram,         // one datagram per object
};

struct TrackSpec {
  std::string name;
  TrackKind kind{TrackKind::Data};
  ForwardMode mode{ForwardMode::SubgroupPerGroup};
  // MoQ publisher priority; lower is higher (audio should sort before video).
  uint8_t priority{kDefaultPriority};
  // Set when content is already available at open time, so a joining FETCH can
  // resolve before the publish loop produces another object.
  std::optional<AbsoluteLocation> initialLargest;
  // A static source is served through fetch() and has no publish loop.
  bool isStatic{false};
};

// One publishable unit. Move-only (owns payload). group/object are assigned by
// the source (content-derived for a file, passthrough for a live upstream).
struct MediaObject {
  uint64_t group{0};
  uint64_t object{0};
  Payload payload;
  Extensions extensions{noExtensions()};
  // Finish the subgroup after this object without ending the track.
  bool endOfGroup{false};
};

// The per-track segment feed for ONE track, opened on demand by that track's
// serving stack (never eagerly, never for tracks nobody subscribes to). It only
// produces media segments - init lives in the catalog, not here.
//
//  - objects(): the live object stream; the publish loop pulls it once and the
//    forwarder fans it out. Completing the generator ends the track.
//  - fetch():   catch-up / join-backfill over [start, end); backs a joining or
//    standalone FETCH. Sources without catch-up yield nothing.
class SegmentSource {
 public:
  virtual ~SegmentSource() = default;

  virtual const TrackSpec& spec() const = 0;

  virtual folly::coro::AsyncGenerator<MediaObject&&> objects() = 0;

  virtual folly::coro::AsyncGenerator<MediaObject&&> fetch(
      AbsoluteLocation start,
      AbsoluteLocation end) = 0;
};

} // namespace moxygen::media_server
