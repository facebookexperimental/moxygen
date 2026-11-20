/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/lane_manager/MoQLaneRouter.h"

#include <exception>
#include <optional>

namespace moxygen::lane_manager {

namespace {

std::optional<Role> roleOf(
    const FullTrackName& name,
    const std::optional<FullTrackName>& virtualVideoTrack) {
  if (virtualVideoTrack && name == *virtualVideoTrack) {
    return Role::Video;
  }
  if (name.trackName.ends_with("--abr-video")) {
    return Role::Video;
  }
  if (name.trackName.ends_with("--abr-audio")) {
    return Role::Audio;
  }
  return std::nullopt;
}

} // namespace

MoQLaneRouter::MoQLaneRouter(
    std::shared_ptr<Publisher> proxy,
    PolicyFactory policyFactory,
    std::chrono::milliseconds defaultMaxWait,
    LaneManagerAbrOptions abr,
    RoutingOptions routing)
    : proxy_(std::move(proxy)),
      policyFactory_(std::move(policyFactory)),
      defaultMaxWait_(defaultMaxWait),
      abr_(std::move(abr)),
      routing_(std::move(routing)) {}

folly::coro::Task<Publisher::SubscribeResult> MoQLaneRouter::subscribe(
    SubscribeRequest request,
    std::shared_ptr<TrackConsumer> consumer) {
  const auto role = roleOf(request.fullTrackName, routing_.virtualVideoTrack);
  if (!role) {
    if (routing_.originNamespace) {
      request.fullTrackName.trackNamespace =
          routing_.originNamespace(request.fullTrackName.trackNamespace);
    }
    co_return co_await proxy_->subscribe(
        std::move(request), std::move(consumer));
  }
  auto& weak = managers_[request.fullTrackName.trackNamespace];
  auto manager = weak.lock();
  if (!manager) {
    auto origin = routing_.originNamespace
        ? routing_.originNamespace(request.fullTrackName.trackNamespace)
        : request.fullTrackName.trackNamespace;
    manager = std::make_shared<MoQLaneManager>(
        proxy_, std::move(origin), defaultMaxWait_, abr_);
    weak = manager;
  }
  std::shared_ptr<SwitchingPolicy> policy;
  try {
    policy = routing_.policyFactory
        ? routing_.policyFactory(request.fullTrackName, *role)
        : policyFactory_(*role);
  } catch (const std::exception& error) {
    co_return folly::makeUnexpected(
        SubscribeError{
            request.requestID, RequestErrorCode::NOT_SUPPORTED, error.what()});
  }
  co_return co_await manager->subscribe(
      std::move(request), std::move(consumer), *role, std::move(policy));
}

folly::coro::Task<Publisher::FetchResult> MoQLaneRouter::fetch(
    Fetch fetch,
    std::shared_ptr<FetchConsumer> consumer) {
  const auto role = roleOf(fetch.fullTrackName, routing_.virtualVideoTrack);
  if (!role) {
    if (routing_.originNamespace) {
      fetch.fullTrackName.trackNamespace =
          routing_.originNamespace(fetch.fullTrackName.trackNamespace);
    }
    co_return co_await proxy_->fetch(std::move(fetch), std::move(consumer));
  }
  auto it = managers_.find(fetch.fullTrackName.trackNamespace);
  auto manager = it == managers_.end() ? nullptr : it->second.lock();
  if (!manager) {
    co_return folly::makeUnexpected(
        FetchError{fetch.requestID, FetchErrorCode::DOES_NOT_EXIST, ""});
  }
  co_return co_await manager->fetch(
      std::move(fetch), std::move(consumer), *role);
}

void MoQLaneRouter::onPolicyChanged(const FullTrackName& track) {
  const auto role = roleOf(track, routing_.virtualVideoTrack);
  auto found = managers_.find(track.trackNamespace);
  if (role && found != managers_.end()) {
    if (auto manager = found->second.lock()) {
      manager->onPolicyChanged(*role);
    }
  }
}

} // namespace moxygen::lane_manager
