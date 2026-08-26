/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/samples/media_server/MediaSourceResolver.h>
#include <moxygen/samples/media_server/sources/Fmp4MediaSource.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>

namespace moxygen::media_server {

// Resolves namespaces whose first tuple field is "file" from fragmented MP4
// media on disk. Other namespaces are not handled. A single configured catalog
// is used for every matching namespace.
class FileMediaSourceResolver : public MediaSourceResolver {
 public:
  FileMediaSourceResolver(
      std::string catalogPath,
      std::chrono::milliseconds fragmentInterval,
      bool loop);

  folly::coro::Task<std::shared_ptr<SegmentSource>> openTrack(
      const TrackNamespace& ns,
      const std::string& trackName) override;

 private:
  // True if `ns` selects the file backend (first tuple field == "file").
  static bool isFileNamespace(const TrackNamespace& ns);

  Fmp4MediaSource source_;
};

} // namespace moxygen::media_server
