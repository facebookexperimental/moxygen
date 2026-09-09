/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/sources/FileMediaSourceResolver.h>

#include <folly/logging/xlog.h>

#include <utility>

namespace moxygen::media_server {

namespace {

constexpr uint32_t kFilePrDropPercent = 10;
constexpr uint64_t kFilePrDropSeed = 1;
static_assert(kFilePrDropPercent <= 100);

} // namespace

FileMediaSourceResolver::FileMediaSourceResolver(
    std::string catalogPath,
    std::chrono::milliseconds fragmentInterval,
    bool loop)
    : source_(std::move(catalogPath), fragmentInterval, loop) {}

bool FileMediaSourceResolver::isFileNamespace(const TrackNamespace& ns) {
  return !ns.trackNamespace.empty() &&
      (ns.trackNamespace.front() == "file" ||
       ns.trackNamespace.front() == "file_pr");
}

bool FileMediaSourceResolver::isPartiallyReliableNamespace(
    const TrackNamespace& ns) {
  return !ns.trackNamespace.empty() && ns.trackNamespace.front() == "file_pr";
}

folly::coro::Task<std::shared_ptr<SegmentSource>>
FileMediaSourceResolver::openTrack(
    const TrackNamespace& ns,
    const std::string& trackName) {
  if (!isFileNamespace(ns)) {
    XLOG(WARN) << "[FileResolver] openTrack: not a file-backend namespace";
    co_return nullptr;
  }
  const uint32_t dropPercent =
      isPartiallyReliableNamespace(ns) ? kFilePrDropPercent : 0;
  co_return source_.openTrack(trackName, dropPercent, kFilePrDropSeed);
}

} // namespace moxygen::media_server
