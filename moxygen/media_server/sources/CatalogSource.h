/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/media_server/MediaCatalog.h>
#include <moxygen/media_server/MoQMediaSource.h>

#include <folly/coro/Baton.h>
#include <folly/io/IOBuf.h>

#include <string>
#include <utility>

namespace moxygen::media_server {

// A static, single-object track: it serves one opaque document (the catalog) as
// the object at {0,0}. It has no live feed, so it flows through the exact same
// serving stack as a media track but behaves differently in two documented ways
// keyed off spec().initialLargest being set:
//   - the broadcast seeds the forwarder's largest to {0,0} so a joining FETCH
//     resolves immediately, and starts NO publish loop for it;
//   - the bytes are delivered on a (joining) FETCH via fetch().
// This is why the catalog needs no special-case path in MoQBroadcast anymore.
class CatalogSource : public SegmentSource {
 public:
  explicit CatalogSource(std::string doc) : doc_(std::move(doc)) {
    spec_.name = std::string(kCatalogTrackName);
    spec_.kind = TrackKind::Catalog;
    spec_.mode = ForwardMode::SubgroupPerGroup;
    spec_.priority = 0; // highest: discovery should win over media
    spec_.initialLargest = AbsoluteLocation{0, 0};
  }

  const TrackSpec& spec() const override {
    return spec_;
  }

  folly::coro::AsyncGenerator<MediaObject&&> objects() override {
    // Static track: nothing is produced live. The broadcast starts no publish
    // loop for a track with initialLargest set, so this is never pulled; if it
    // ever were, hold open until cancelled rather than signalling end-of-track.
    folly::coro::Baton never;
    co_await never;
    co_return;
  }

  folly::coro::AsyncGenerator<MediaObject&&> fetch(
      AbsoluteLocation /*start*/,
      AbsoluteLocation /*end*/) override {
    // Exactly one object - the catalog document. A joining FETCH for the
    // catalog always wants the current doc, so serve it regardless of the
    // (degenerate) requested range.
    co_yield MediaObject{
        .group = 0,
        .object = 0,
        .payload = folly::IOBuf::copyBuffer(doc_),
        .extensions = noExtensions()};
  }

 private:
  TrackSpec spec_;
  std::string doc_;
};

} // namespace moxygen::media_server
