/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/MoQBroadcastFactory.h>

#include <moxygen/samples/media_server/sources/FileMediaSourceResolver.h>

#include <utility>

namespace moxygen::media_server {

MoQBroadcastFactory::MoQBroadcastFactory(
    std::string fileInput,
    std::chrono::milliseconds fragmentInterval,
    bool loop,
    folly::Executor* loopExecutor)
    : fileResolver_(
          std::make_shared<FileMediaSourceResolver>(
              std::move(fileInput),
              fragmentInterval,
              loop)),
      loopExecutor_(loopExecutor) {}

std::shared_ptr<MoQBroadcast> MoQBroadcastFactory::makeBroadcast(
    const TrackNamespace& ns) {
  return std::make_shared<MoQBroadcast>(ns, resolverFor(ns), loopExecutor_);
}

std::shared_ptr<MediaSourceResolver> MoQBroadcastFactory::resolverFor(
    const TrackNamespace& /*ns*/) {
  // Route by the namespace's backend prefix. Today only the file backend
  // exists, so every namespace maps to it (the file resolver rejects non-file
  // namespaces itself). Add backends here, e.g.:
  //   if (!ns.trackNamespace.empty() && ns.trackNamespace.front() == "oil") {
  //     return oilResolver_;
  //   }
  return fileResolver_;
}

} // namespace moxygen::media_server
