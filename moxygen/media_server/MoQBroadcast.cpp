/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/media_server/MoQBroadcast.h>

#include <moxygen/MoQSession.h>
#include <moxygen/media_server/MediaCatalog.h>
#include <moxygen/media_server/PublishLoop.h>

#include <folly/CancellationToken.h>
#include <folly/coro/Task.h>
#include <folly/coro/WithCancellation.h>
#include <folly/logging/xlog.h>

#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace moxygen::media_server {

namespace {

// Handle for a (joining) FETCH. The objects are written asynchronously;
// fetchCancel() stops that writer.
class MediaFetchHandle : public Publisher::FetchHandle {
 public:
  explicit MediaFetchHandle(FetchOk ok)
      : Publisher::FetchHandle(std::move(ok)) {}

  void fetchCancel() override {
    cancelSource.requestCancellation();
  }

  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "request update not supported"});
  }

  folly::CancellationSource cancelSource;
};

// Per-forwarder lifecycle callback. Holds a weak ref to the broadcast so it
// never keeps it alive, and forwards the two lifecycle edges (last subscriber
// left / source ended) to the broadcast, which defers the actual reap.
class ForwarderCallback : public MoQForwarder::Callback {
 public:
  ForwarderCallback(
      std::weak_ptr<MoQBroadcast> broadcast,
      std::string trackName)
      : broadcast_(std::move(broadcast)), trackName_(std::move(trackName)) {}

  void onEmpty(MoQForwarder*) override {
    if (auto b = broadcast_.lock()) {
      b->onForwarderEmpty(trackName_);
    }
  }

  void onPublishDone(MoQForwarder*) override {
    if (auto b = broadcast_.lock()) {
      b->onForwarderSourceEnded(trackName_);
    }
  }

 private:
  std::weak_ptr<MoQBroadcast> broadcast_;
  std::string trackName_;
};

// The cancellable publish loop for one track, launched detached. This MUST be a
// named coroutine (not a capturing lambda): a temporary lambda's captures are
// destroyed at the end of the launching expression, dangling once the loop
// suspends (ASan stack-use-after-return). Coroutine parameters, by contrast,
// live in the coroutine frame. co_withCancellation lets a reap stop the loop;
// co_awaitTry swallows the resulting cancellation so a reclaimed loop unwinds
// quietly (no publishDone). source/forwarder are held by value for the loop's
// life.
folly::coro::Task<void> runTrackLoop(
    std::shared_ptr<SegmentSource> source,
    std::shared_ptr<MoQForwarder> forwarder,
    folly::CancellationToken token,
    std::string trackName) {
  auto tried = co_await folly::coro::co_awaitTry(
      folly::coro::co_withCancellation(
          token,
          runPublishLoop(
              std::move(source),
              std::move(forwarder), /*waitForSubscriber=*/
              false)));
  if (tried.hasException()) {
    XLOG(DBG1) << "[MoQBroadcast] track=" << trackName
               << " publish loop stopped (reclaimed)";
  }
}

} // namespace

folly::coro::Task<std::shared_ptr<MoQBroadcast::TrackStack>>
MoQBroadcast::getOrCreateTrack(const std::string& name) {
  auto it = mediaTracks_.find(name);
  if (it == mediaTracks_.end()) {
    auto stack = std::make_shared<TrackStack>();
    // Plain forwarder, group id = segmentStartPts. Do NOT pre-seed largest: an
    // unset largest means "nothing published yet", so a subscriber that joins
    // before the loop starts receives from the very first object.
    stack->forwarder = std::make_shared<MoQForwarder>(fullTrackName(name));
    stack->callback =
        std::make_shared<ForwarderCallback>(weak_from_this(), name);
    stack->forwarder->setCallback(stack->callback);
    it = mediaTracks_.emplace(name, std::move(stack)).first;
  }
  auto stack = it->second;
  if (!stack->resolveStarted) {
    stack->resolveStarted = true;
    try {
      stack->source = co_await resolver_->openTrack(ns_, name);
    } catch (...) {
      stack->ready.setException(std::runtime_error("track resolution failed"));
      if (auto found = mediaTracks_.find(name);
          found != mediaTracks_.end() && found->second == stack) {
        mediaTracks_.erase(found);
      }
      throw;
    }
    if (stack->source && stack->source->spec().initialLargest) {
      // A static track (the catalog): its object is already available, so seed
      // largest here - before any subscriber is added - so both its SubscribeOk
      // and a joining FETCH resolve against {0,0} immediately.
      stack->forwarder->setLargest(*stack->source->spec().initialLargest);
    }
    stack->ready.setValue(folly::Unit{});
  } else if (!stack->ready.isFulfilled()) {
    // A resolve is in flight; coalesce onto it.
    co_await stack->ready.getFuture();
  }
  if (!stack->source) {
    // Unknown track: drop the placeholder so a later request re-resolves, but
    // only if it is still the entry we created.
    if (auto found = mediaTracks_.find(name);
        found != mediaTracks_.end() && found->second == stack) {
      mediaTracks_.erase(found);
    }
    co_return nullptr;
  }
  co_return stack;
}

void MoQBroadcast::startLoop(
    const std::string& name,
    const std::shared_ptr<TrackStack>& stack) {
  stack->loopStarted = true;
  XLOG(INFO) << "[MoQBroadcast] starting publish loop track=" << name;
  // The broadcast stays alive while this loop runs because the stack is in
  // mediaTracks_ (so it is non-empty and the dispatcher won't drop it); the
  // loop itself only touches the source/forwarder it is handed.
  folly::coro::co_withExecutor(
      loopExecutor_,
      runTrackLoop(
          stack->source, stack->forwarder, stack->loopCancel.getToken(), name))
      .start();
}

MoQForwarder* MoQBroadcast::forwarderFor(const std::string& name) {
  auto it = mediaTracks_.find(name);
  return it != mediaTracks_.end() ? it->second->forwarder.get() : nullptr;
}

void MoQBroadcast::onForwarderEmpty(const std::string& trackName) {
  // Deferred: we are inside a forwarder operation; reaping now would destroy
  // the forwarder mid-callback. Reap on the next turn (and re-check for a
  // re-join).
  auto self = weak_from_this();
  const std::string& name = trackName;
  loopExecutor_->add([self, name]() {
    if (auto b = self.lock()) {
      b->reapTrack(name);
    }
  });
}

void MoQBroadcast::onForwarderSourceEnded(const std::string& trackName) {
  if (auto it = mediaTracks_.find(trackName); it != mediaTracks_.end()) {
    it->second->ended = true;
  }
  // When every LIVE track's source has ended, the broadcast is over: drain the
  // static tracks (the catalog) so their subscribers get publishDone too. A
  // static track (spec().initialLargest set) has no publish loop and never ends
  // on its own.
  bool anyLive = false;
  bool allLiveEnded = true;
  for (auto& [n, st] : mediaTracks_) {
    if (st->source && st->source->spec().initialLargest) {
      continue; // static (catalog): not a live track
    }
    anyLive = true;
    if (!st->ended) {
      allLiveEnded = false;
      break;
    }
  }
  if (anyLive && allLiveEnded) {
    for (auto& [n, st] : mediaTracks_) {
      if (!st->source || !st->source->spec().initialLargest || st->ended) {
        continue;
      }
      // Mark ended BEFORE publishDone: its onPublishDone re-enters this method,
      // and the guard above then skips it (no re-drain, no recursion).
      st->ended = true;
      auto res = st->forwarder->publishDone(
          PublishDone{
              RequestID{0},
              PublishDoneStatusCode::TRACK_ENDED,
              /*streamCount=*/0,
              "broadcast ended"});
      if (res.hasError()) {
        XLOG(ERR) << "[MoQBroadcast] catalog publishDone error: "
                  << res.error().what();
      } else {
        XLOG(INFO) << "[MoQBroadcast] broadcast over; sent catalog publishDone";
      }
    }
  }
  // Source ended with no subscribers to drain: onEmpty won't fire, so reap now.
  auto* fwd = forwarderFor(trackName);
  if (fwd && fwd->empty()) {
    onForwarderEmpty(trackName);
  }
}

void MoQBroadcast::reapTrack(const std::string& name) {
  auto it = mediaTracks_.find(name);
  if (it == mediaTracks_.end()) {
    return;
  }
  auto& stack = it->second;
  if (!stack->forwarder->empty()) {
    // A subscriber re-joined between onEmpty and this deferred reap; keep it.
    return;
  }
  XLOG(INFO) << "[MoQBroadcast] reaping track=" << name;
  stack->loopCancel.requestCancellation(); // stop the publish loop (if running)
  mediaTracks_.erase(it);
  maybeNotifyEmpty();
}

void MoQBroadcast::maybeNotifyEmpty() {
  if (empty() && onEmpty_) {
    XLOG(INFO) << "[MoQBroadcast] no serving stacks left; notifying dispatcher";
    onEmpty_();
  }
}

folly::coro::Task<Publisher::SubscribeResult> MoQBroadcast::subscribe(
    SubscribeRequest subReq,
    std::shared_ptr<TrackConsumer> consumer) {
  const auto& ftn = subReq.fullTrackName;
  auto stack = co_await getOrCreateTrack(ftn.trackName);
  if (!stack) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subReq.requestID,
            SubscribeErrorCode::DOES_NOT_EXIST,
            "unknown track"});
  }
  auto session = MoQSession::getRequestSession();
  auto sub = stack->forwarder->addSubscriber(
      std::move(session), subReq, std::move(consumer));
  if (!sub) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subReq.requestID,
            SubscribeErrorCode::DOES_NOT_EXIST,
            "track ended"});
  }
  XLOG(INFO) << "[MoQBroadcast] subscriber added track=" << ftn.trackName;
  // Live tracks produce objects through a publish loop; a static track (the
  // catalog) is served entirely via FETCH and never runs one.
  if (!stack->loopStarted && !stack->source->spec().initialLargest) {
    startLoop(ftn.trackName, stack);
  }
  co_return sub;
}

folly::coro::Task<Publisher::FetchResult> MoQBroadcast::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  const auto& ftn = fetch.fullTrackName;

  if (ftn.trackName != kCatalogTrackName) {
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID,
            FetchErrorCode::NOT_SUPPORTED,
            "media FETCH is not supported"});
  }

  auto session = MoQSession::getRequestSession();
  auto stack = co_await getOrCreateTrack(ftn.trackName);
  if (!stack) {
    co_return folly::makeUnexpected(
        FetchError{
            fetch.requestID, FetchErrorCode::DOES_NOT_EXIST, "unknown track"});
  }
  auto forwarder = stack->forwarder;
  auto source = stack->source;

  auto [standalone, joining] = fetchType(fetch);
  SubscribeRange range;
  if (joining) {
    auto res = forwarder->resolveJoiningFetch(session, *joining);
    if (res.hasError()) {
      co_return folly::makeUnexpected(res.error());
    }
    range = res.value();
  } else {
    range = SubscribeRange{standalone->start, standalone->end};
  }

  // Group ids are content-derived (segmentStartPts for media, {0,0} for the
  // catalog), not sequential, so a relative joining fetch's arithmetic start
  // lands between real groups. Widen the backfill to every buffered object (the
  // source ring for media, the single doc for the catalog).
  range.start = AbsoluteLocation{0, 0};

  auto handle = std::make_shared<MediaFetchHandle>(FetchOk{
      fetch.requestID,
      MoQSession::resolveGroupOrder(GroupOrder::OldestFirst, fetch.groupOrder),
      /*endOfTrack=*/0,
      /*endLocation=*/
      AbsoluteLocation{range.end.group > 0 ? range.end.group - 1 : 0, 0},
      /*extensions=*/{}});
  auto serve = serveMediaFetch(std::move(consumer), std::move(source), range);
  folly::coro::co_withExecutor(
      session->getExecutor(),
      folly::coro::co_withCancellation(
          handle->cancelSource.getToken(), std::move(serve)))
      .start();
  co_return handle;
}

folly::coro::Task<void> MoQBroadcast::serveMediaFetch(
    std::shared_ptr<FetchConsumer> consumer,
    std::shared_ptr<SegmentSource> source,
    SubscribeRange range) {
  std::vector<MediaObject> objs;
  auto gen = source->fetch(range.start, range.end);
  while (auto item = co_await gen.next()) {
    objs.push_back(std::move(*item));
  }
  if (objs.empty()) {
    XLOG(WARN) << "[MoQBroadcast] media FETCH empty range track="
               << source->spec().name;
    consumer->endOfFetch();
    co_return;
  }
  for (size_t i = 0; i < objs.size(); ++i) {
    const bool last = (i + 1 == objs.size());
    auto res = consumer->object(
        objs[i].group,
        /*subgroupID=*/0,
        objs[i].object,
        std::move(objs[i].payload),
        noExtensions(),
        /*finFetch=*/last);
    if (res.hasError()) {
      XLOG(ERR) << "[MoQBroadcast] media fetch object error: "
                << res.error().what();
      co_return;
    }
  }
  XLOG(INFO) << "[MoQBroadcast] served media FETCH track="
             << source->spec().name << " objects=" << objs.size();
}

void MoQBroadcast::removeSubscriber(
    const std::shared_ptr<MoQSession>& session,
    const std::string& reason) {
  // Drop the session from every forwarder (the catalog is just another track);
  // each forwarder that goes empty fires onEmpty -> deferred reap.
  for (auto& [name, stack] : mediaTracks_) {
    stack->forwarder->removeSubscriber(session, std::nullopt, reason);
  }
}

} // namespace moxygen::media_server
