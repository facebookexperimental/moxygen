/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/proxy/MoQProxy.h"

#include <stdexcept>
#include <utility>

#include "moxygen/MoQSession.h"
#include "moxygen/proxy/MoQProxyTrack.h"

namespace moxygen {

std::shared_ptr<MoQProxy> MoQProxy::create(
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders) {
  return std::shared_ptr<MoQProxy>(new MoQProxy(std::move(upstreamProviders)));
}

MoQProxy::MoQProxy(
    std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders)
    : upstreamProviders_(std::move(upstreamProviders)) {
  if (upstreamProviders_.empty()) {
    throw std::invalid_argument("MoQProxy requires upstream providers");
  }
  for (const auto& provider : upstreamProviders_) {
    if (!provider) {
      throw std::invalid_argument(
          "MoQProxy requires non-null upstream providers");
    }
  }
}

MoQProxy::~MoQProxy() {
  close();
}

folly::coro::Task<Publisher::SubscribeResult> MoQProxy::subscribe(
    SubscribeRequest subscribeRequest,
    std::shared_ptr<TrackConsumer> consumer) {
  auto self = shared_from_this();
  if (closed_) {
    co_return folly::makeUnexpected(
        SubscribeError{
            subscribeRequest.requestID,
            SubscribeErrorCode::GOING_AWAY,
            "proxy is closed"});
  }

  auto downstreamSession = MoQSession::getRequestSession();
  auto track = getOrCreateTrack(subscribeRequest.fullTrackName);
  co_return co_await track->subscribe(
      std::move(subscribeRequest),
      std::move(consumer),
      std::move(downstreamSession));
}

std::shared_ptr<MoQProxyTrack> MoQProxy::getOrCreateTrack(
    const FullTrackName& fullTrackName) {
  auto it = tracks_.find(fullTrackName);
  if (it != tracks_.end()) {
    return it->second;
  }

  auto track = MoQProxyTrack::create(fullTrackName, upstreamProviders_);
  track->setCallback(shared_from_this());
  tracks_.emplace(fullTrackName, track);
  return track;
}

void MoQProxy::onNoSubscribers(MoQProxyTrack* track) {
  auto it = tracks_.find(track->fullTrackName());
  if (it != tracks_.end() && it->second.get() == track) {
    tracks_.erase(it);
  }
}

void MoQProxy::close() {
  if (closed_) {
    return;
  }
  closed_ = true;

  auto tracks = std::move(tracks_);
  for (auto& [_, track] : tracks) {
    track->setCallback({});
    track->close();
  }
}

} // namespace moxygen
