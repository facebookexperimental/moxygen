/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/samples/media_server/MediaSourceResolver.h>
#include <moxygen/samples/media_server/sources/Fmp4MediaSource.h>

#include <folly/container/F14Set.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace moxygen::media_server {

// Resolver for the file-backed modes: "file" serves a static full catalog,
// "file_pr" also simulates media loss, and "file_abr" progressively advertises
// the authored video tracks. `fileAliases` are extra first fields served like
// "file". Other prefixes resolve to nothing.
class FileMediaSourceResolver : public MediaSourceResolver {
 public:
  FileMediaSourceResolver(
      std::string catalogPath,
      std::chrono::milliseconds fragmentInterval,
      std::chrono::milliseconds catalogUpdateInterval,
      bool loop,
      const std::vector<std::string>& fileAliases);

  folly::coro::Task<std::shared_ptr<SegmentSource>> openTrack(
      const TrackNamespace& ns,
      const std::string& trackName) override;

 private:
  // True if `ns` selects a file-backed mode.
  bool isFileNamespace(const TrackNamespace& ns) const;
  static bool isPartiallyReliableNamespace(const TrackNamespace& ns);
  static bool isAbrNamespace(const TrackNamespace& ns);

  Fmp4MediaSource source_;
  std::chrono::milliseconds catalogUpdateInterval_;
  folly::F14FastSet<std::string> fileAliases_;
};

} // namespace moxygen::media_server
