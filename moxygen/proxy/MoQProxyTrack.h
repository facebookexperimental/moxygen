/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/coro/Task.h>
#include <memory>
#include <optional>
#include <string>

#include "moxygen/Publisher.h"
#include "moxygen/proxy/MoQUpstreamProvider.h"

namespace moxygen {

class MoQForwarder;
class MoQSession;
class TrackConsumer;

class MoQProxyTrack : public std::enable_shared_from_this<MoQProxyTrack> {
 public:
  static std::shared_ptr<MoQProxyTrack> create(
      FullTrackName fullTrackName,
      std::shared_ptr<MoQUpstreamProvider> upstreamProvider);

  ~MoQProxyTrack();

  MoQProxyTrack(const MoQProxyTrack&) = delete;
  MoQProxyTrack& operator=(const MoQProxyTrack&) = delete;

  const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  folly::coro::Task<Publisher::SubscribeResult> subscribe(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer,
      std::shared_ptr<MoQSession> downstreamSession);

 private:
  class DownstreamSubscriptionHandle;

  MoQProxyTrack(
      FullTrackName fullTrackName,
      std::shared_ptr<MoQUpstreamProvider> upstreamProvider);

  Publisher::SubscribeResult addSubscriber(
      const SubscribeRequest& subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer,
      std::shared_ptr<MoQSession> downstreamSession);

  folly::coro::Task<std::optional<SubscribeError>> establishUpstream(
      const SubscribeRequest& subscribeRequest,
      const std::shared_ptr<MoQSession>& downstreamSession);

  FullTrackName fullTrackName_;
  std::shared_ptr<MoQUpstreamProvider> upstreamProvider_;
  std::shared_ptr<MoQForwarder> forwarder_;
  std::shared_ptr<MoQSession> upstreamSession_;
  std::shared_ptr<Publisher::SubscriptionHandle> upstreamHandle_;
  bool subscriptionStarted_{false};
};

} // namespace moxygen
