/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/media_server/MoQMediaSource.h>

#include <chrono>
#include <memory>
#include <string>

namespace moxygen::media_server {

// A file-backed broadcast: a catalog-metadata JSON that lists tracks, each
// backed by its own fragmented-MP4 file (the per-track `sourceFile`, resolved
// next to the catalog). Everything is resolved on demand via openTrack(name):
//
//  - the catalog track (name == kCatalogTrackName): load the authored metadata
//    and inline each track's init segment (ftyp+moov, base64) into an MSF/CMSF
//    catalog document, wrapped in a static single-object CatalogSource.
//  - a media track: parse one track's fMP4 into a per-track SegmentSource (init
//    split off, each moof+mdat a fragment grouped by segmentStartPts). nullptr
//    if not listed.
//
// Nothing is parsed at construction; the object just remembers where to look.
class Fmp4MediaSource {
 public:
  Fmp4MediaSource(
      std::string catalogPath,
      std::chrono::milliseconds fragmentInterval,
      bool loop);

  std::shared_ptr<SegmentSource> openTrack(const std::string& trackName);

 private:
  // Assemble the served catalog document (authored metadata + inlined init
  // segments) for the catalog track; wrapped in a CatalogSource by openTrack().
  std::string catalog();

  std::string catalogPath_;
  std::chrono::milliseconds fragmentInterval_;
  bool loop_;
};

} // namespace moxygen::media_server
