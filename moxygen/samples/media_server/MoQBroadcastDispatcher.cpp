/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/MoQBroadcastDispatcher.h>

#include <folly/coro/Task.h>
#include <folly/logging/xlog.h>

#include <utility>

namespace moxygen::media_server {

std::shared_ptr<MoQBroadcast> MoQBroadcastDispatcher::getOrCreateBroadcast(
    const TrackNamespace& ns) {
  auto it = broadcasts_.find(ns);
  if (it == broadcasts_.end()) {
    auto broadcast = factory_->makeBroadcast(ns);
    auto weakSelf = weak_from_this();
    const auto& nsCopy = ns;
    broadcast->setOnEmpty([weakSelf, nsCopy]() {
      if (auto self = weakSelf.lock()) {
        self->onBroadcastEmpty(nsCopy);
      }
    });
    it = broadcasts_.emplace(ns, std::move(broadcast)).first;
    XLOG(INFO) << "[MoQBroadcastDispatcher] created broadcast";
  }
  return it->second;
}

void MoQBroadcastDispatcher::onBroadcastEmpty(const TrackNamespace& ns) {
  // Deferred: the broadcast may be calling us from its own teardown; dropping
  // it now would destroy it mid-call. Drop on the next turn if still empty.
  auto weakSelf = weak_from_this();
  const auto& nsCopy = ns;
  loopExecutor_->add([weakSelf, nsCopy]() {
    if (auto self = weakSelf.lock()) {
      self->dropBroadcast(nsCopy);
    }
  });
}

void MoQBroadcastDispatcher::dropBroadcast(const TrackNamespace& ns) {
  auto it = broadcasts_.find(ns);
  if (it != broadcasts_.end() && it->second->empty()) {
    XLOG(INFO) << "[MoQBroadcastDispatcher] dropping empty broadcast";
    broadcasts_.erase(it);
  }
}

folly::coro::Task<Publisher::SubscribeResult> MoQBroadcastDispatcher::subscribe(
    SubscribeRequest subReq,
    std::shared_ptr<TrackConsumer> consumer) {
  auto ns = subReq.fullTrackName.trackNamespace;
  XLOG(INFO) << "[MoQBroadcastDispatcher] SUBSCRIBE ftn="
             << subReq.fullTrackName << " requestID=" << subReq.requestID;
  auto broadcast = getOrCreateBroadcast(ns);
  auto result =
      co_await broadcast->subscribe(std::move(subReq), std::move(consumer));
  // Unknown namespace/track: the broadcast resolved nothing, so drop the empty
  // shell rather than leaking one per bogus request.
  if (broadcast->empty()) {
    onBroadcastEmpty(ns);
  }
  co_return result;
}

folly::coro::Task<Publisher::FetchResult> MoQBroadcastDispatcher::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> fetchCallback) {
  auto ns = fetch.fullTrackName.trackNamespace;
  XLOG(INFO) << "[MoQBroadcastDispatcher] FETCH ftn=" << fetch.fullTrackName
             << " requestID=" << fetch.requestID;
  auto broadcast = getOrCreateBroadcast(ns);
  auto result =
      co_await broadcast->fetch(std::move(fetch), std::move(fetchCallback));
  if (broadcast->empty()) {
    onBroadcastEmpty(ns);
  }
  co_return result;
}

void MoQBroadcastDispatcher::removeSubscriber(
    const std::shared_ptr<MoQSession>& session,
    const std::string& reason) {
  XLOG(INFO) << "[MoQBroadcastDispatcher] subscriber went away reason="
             << reason;
  // A session may hold subscriptions across several broadcasts; drop it from
  // each. Emptied forwarders fire onEmpty -> deferred reap -> broadcast drop.
  for (auto& [ns, broadcast] : broadcasts_) {
    broadcast->removeSubscriber(session, reason);
  }
}

} // namespace moxygen::media_server
