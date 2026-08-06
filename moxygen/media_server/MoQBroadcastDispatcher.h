/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/Publisher.h>
#include <moxygen/media_server/BroadcastFactory.h>
#include <moxygen/media_server/MoQBroadcast.h>

#include <folly/Executor.h>
#include <folly/coro/Task.h>

#include <map>
#include <memory>
#include <string>

namespace moxygen::media_server {

// Namespace-agnostic MoQ publish handler. Holds no namespace, no content, and
// no backend knowledge of its own: every SUBSCRIBE/FETCH carries its
// FullTrackName, and the dispatcher routes it to a MoQBroadcast keyed by
// namespace, created on first touch (via an injected factory) and dropped once
// the broadcast reports it has no serving stacks left. All backend/media
// concerns (which store, the resolver, per-track segment sources) live inside
// the broadcast the factory builds; the dispatcher is pure registry +
// lifecycle. One shared instance serves every session.
class MoQBroadcastDispatcher
    : public Publisher,
      public std::enable_shared_from_this<MoQBroadcastDispatcher> {
 public:
  // The factory (see BroadcastFactory) is injected so the dispatcher stays free
  // of backend/media concerns; the concrete factory owns resolver/backend
  // selection. `main` wires it (see MoQMediaServerMain).
  MoQBroadcastDispatcher(
      std::shared_ptr<BroadcastFactory> factory,
      folly::Executor* loopExecutor)
      : factory_(std::move(factory)), loopExecutor_(loopExecutor) {}

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subReq,
      std::shared_ptr<TrackConsumer> consumer) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchCallback) override;

  void removeSubscriber(
      const std::shared_ptr<MoQSession>& session,
      const std::string& reason);

 private:
  // Find or create the broadcast for `ns` (created empty; nothing resolved
  // yet).
  std::shared_ptr<MoQBroadcast> getOrCreateBroadcast(const TrackNamespace& ns);

  // A broadcast reported empty (all stacks reaped, or it never resolved). Drop
  // it on the next turn if it is still empty (deferred: never destroy it from
  // inside its own teardown callback).
  void onBroadcastEmpty(const TrackNamespace& ns);
  void dropBroadcast(const TrackNamespace& ns);

  std::shared_ptr<BroadcastFactory> factory_;
  folly::Executor* loopExecutor_;
  // Active broadcasts, keyed by namespace (one entry per live stream).
  std::map<TrackNamespace, std::shared_ptr<MoQBroadcast>> broadcasts_;
};

} // namespace moxygen::media_server
