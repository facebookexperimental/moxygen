/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/Publisher.h>
#include <moxygen/relay/MoQForwarder.h>
#include <moxygen/samples/media_server/MediaSourceResolver.h>
#include <moxygen/samples/media_server/MoQMediaSource.h>

#include <folly/CancellationToken.h>
#include <folly/Executor.h>
#include <folly/Function.h>
#include <folly/container/F14Map.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Task.h>

#include <memory>
#include <optional>
#include <string>

namespace moxygen::media_server {

// One live stream (one namespace). Created empty by the dispatcher; it holds NO
// content knowledge until a client asks. Each track a client requests gets its
// own serving stack (segment source + fan-out forwarder + cancellable publish
// loop), built on demand and joined by later subscribers. The catalog is just
// another track: the resolver returns a static single-object source for
// kCatalogTrackName, so it flows through the same stack (seeded largest, no
// publish loop, bytes served via FETCH) with no special-case path here.
//
// Teardown is demand-driven, two ways, both via the track's MoQForwarder
// callbacks and always deferred onto the loop executor (never destroy a
// forwarder from inside its own callback):
//   - onEmpty (last subscriber left): reclaim the stack - cancel the loop,
//     close the source, drop the forwarder. The content still exists; a later
//     subscribe rebuilds it (re-resolve, join the live edge).
//   - onPublishDone (the source ended): the forwarder drains its subscribers,
//     then the stack is reclaimed. A later subscribe re-resolves and the
//     resolver decides whether it comes back.
// When the last serving stack is gone, the broadcast reports empty so the
// dispatcher can drop it from the registry.
//
// All methods run on the publisher's single event base.
class MoQBroadcast : public std::enable_shared_from_this<MoQBroadcast> {
 public:
  MoQBroadcast(
      TrackNamespace ns,
      std::shared_ptr<MediaSourceResolver> resolver,
      folly::Executor* loopExecutor)
      : ns_(std::move(ns)),
        resolver_(std::move(resolver)),
        loopExecutor_(loopExecutor) {}

  // Invoked (deferred onto the loop executor) when the broadcast has no serving
  // stacks left, so the dispatcher can drop it.
  void setOnEmpty(folly::Function<void()> cb) {
    onEmpty_ = std::move(cb);
  }

  bool empty() const {
    return mediaTracks_.empty();
  }

  folly::coro::Task<Publisher::SubscribeResult> subscribe(
      SubscribeRequest subReq,
      std::shared_ptr<TrackConsumer> consumer);

  folly::coro::Task<Publisher::FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> fetchCallback);

  void removeSubscriber(
      const std::shared_ptr<MoQSession>& session,
      const std::string& reason);

  // Forwarder lifecycle hooks (public so the per-forwarder Callback can reach
  // them). They only schedule work; the actual reap runs later on the executor.
  void onForwarderEmpty(const std::string& trackName);
  void onForwarderSourceEnded(const std::string& trackName);

 private:
  // A per-track serving stack: the track's segment feed, its fan-out forwarder,
  // its forwarder callback, a resolve-once coalescer, and the loop's cancel
  // handle. Built on the first request for that track (join-or-create).
  struct TrackStack {
    std::shared_ptr<SegmentSource> source;
    std::shared_ptr<MoQForwarder> forwarder;
    std::shared_ptr<MoQForwarder::Callback> callback;
    folly::CancellationSource loopCancel;
    folly::coro::SharedPromise<folly::Unit> ready;
    bool resolveStarted{false};
    bool loopStarted{false};
    bool ended{false};
  };

  folly::coro::Task<std::shared_ptr<TrackStack>> getOrCreateTrack(
      const std::string& name);

  void startLoop(
      const std::string& name,
      const std::shared_ptr<TrackStack>& stack);

  // Deferred reap of a track stack. No-op if the stack is gone or a subscriber
  // re-joined since the reap was scheduled.
  void reapTrack(const std::string& name);
  void maybeNotifyEmpty();

  // The forwarder for a track name; nullptr if absent.
  MoQForwarder* forwarderFor(const std::string& name);

  folly::coro::Task<void> serveMediaFetch(
      std::shared_ptr<FetchConsumer> consumer,
      std::shared_ptr<SegmentSource> source,
      SubscribeRange range);

  FullTrackName fullTrackName(const std::string& trackName) const {
    return FullTrackName{ns_, trackName};
  }

  TrackNamespace ns_;
  std::shared_ptr<MediaSourceResolver> resolver_;
  folly::Executor* loopExecutor_;
  folly::F14FastMap<std::string, std::shared_ptr<TrackStack>> mediaTracks_;

  folly::Function<void()> onEmpty_;
};

} // namespace moxygen::media_server
