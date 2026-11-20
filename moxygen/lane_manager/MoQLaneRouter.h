/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/Publisher.h"
#include "moxygen/lane_manager/MoQLaneManager.h"

#include <folly/container/F14Map.h>

#include <chrono>
#include <functional>
#include <memory>
#include <optional>

namespace moxygen::lane_manager {

// A session's publish handler: SUBSCRIBE and FETCH on <name>--abr-video or
// <name>--abr-audio go to the broadcast's MoQLaneManager, everything else to
// `proxy`. Other requests get Publisher's NOT_SUPPORTED, as MoQProxy gives.
class MoQLaneRouter : public Publisher {
 public:
  using PolicyFactory = std::function<std::shared_ptr<SwitchingPolicy>(Role)>;

  struct RoutingOptions {
    std::optional<FullTrackName> virtualVideoTrack;
    std::function<TrackNamespace(const TrackNamespace&)> originNamespace;
    std::function<std::shared_ptr<SwitchingPolicy>(const FullTrackName&, Role)>
        policyFactory;
  };

  MoQLaneRouter(
      std::shared_ptr<Publisher> proxy,
      PolicyFactory policyFactory,
      std::chrono::milliseconds defaultMaxWait,
      LaneManagerAbrOptions abr = {},
      RoutingOptions routing = {});

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> consumer) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override;

  void onPolicyChanged(const FullTrackName& track);

 private:
  std::shared_ptr<Publisher> proxy_;
  PolicyFactory policyFactory_;
  std::chrono::milliseconds defaultMaxWait_;
  LaneManagerAbrOptions abr_;
  RoutingOptions routing_;
  folly::F14FastMap<
      TrackNamespace,
      std::weak_ptr<MoQLaneManager>,
      TrackNamespace::hash>
      managers_;
};

} // namespace moxygen::lane_manager
