/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/container/F14Map.h>
#include <memory>
#include <vector>

#include "moxygen/Publisher.h"
#include "moxygen/proxy/MoQUpstreamProvider.h"

namespace moxygen {

class MoQProxyTrack;

class MoQProxy : public Publisher,
                 public std::enable_shared_from_this<MoQProxy> {
 public:
  static std::shared_ptr<MoQProxy> create(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders);

  ~MoQProxy() override;

  MoQProxy(const MoQProxy&) = delete;
  MoQProxy& operator=(const MoQProxy&) = delete;

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer) override;

 private:
  explicit MoQProxy(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders);

  std::shared_ptr<MoQProxyTrack> getOrCreateTrack(
      const FullTrackName& fullTrackName);

  std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders_;
  folly::F14FastMap<
      FullTrackName,
      std::shared_ptr<MoQProxyTrack>,
      FullTrackName::hash>
      tracks_;
};

} // namespace moxygen
