/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQTypes.h>
#include <moxygen/media_server/BroadcastFactory.h>
#include <moxygen/media_server/MediaSourceResolver.h>
#include <moxygen/media_server/MoQBroadcast.h>

#include <folly/Executor.h>

#include <chrono>
#include <memory>
#include <string>

namespace moxygen::media_server {

// The media BroadcastFactory: builds a MoQBroadcast for a namespace, selecting
// the backend by the namespace's prefix. This is the ONLY place backend/media
// wiring lives: it owns the per-backend MediaSourceResolver(s) (constructed
// here from config, never in main) and hands the right one to each broadcast it
// builds. Add a backend by constructing its resolver in the ctor and routing
// its prefix in resolverFor(); nothing in main or the dispatcher changes.
class MoQBroadcastFactory : public BroadcastFactory {
 public:
  MoQBroadcastFactory(
      std::string fileInput,
      std::chrono::milliseconds fragmentInterval,
      bool loop,
      folly::Executor* loopExecutor);

  std::shared_ptr<MoQBroadcast> makeBroadcast(
      const TrackNamespace& ns) override;

 private:
  // The resolver for `ns`, chosen by its backend prefix (ns.front()).
  std::shared_ptr<MediaSourceResolver> resolverFor(const TrackNamespace& ns);

  std::shared_ptr<MediaSourceResolver> fileResolver_;
  folly::Executor* loopExecutor_;
};

} // namespace moxygen::media_server
