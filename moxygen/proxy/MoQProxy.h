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
#include "moxygen/Subscriber.h"
#include "moxygen/proxy/MoQProxyTrack.h"
#include "moxygen/proxy/MoQUpstreamProvider.h"

namespace moxygen {

class MoQCache;

class MoQProxy : public Publisher,
                 public Subscriber,
                 public std::enable_shared_from_this<MoQProxy>,
                 public MoQProxyTrack::Callback {
 public:
  static std::shared_ptr<MoQProxy> create(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders);

  ~MoQProxy() override;

  MoQProxy(const MoQProxy&) = delete;
  MoQProxy& operator=(const MoQProxy&) = delete;

  folly::coro::Task<SubscribeResult> subscribe(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer) override;

  folly::coro::Task<FetchResult> fetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer) override;

  Subscriber::PublishResult publish(
      PublishRequest publishRequest,
      std::shared_ptr<Publisher::SubscriptionHandle> handle = nullptr) override;

  void close();

 private:
  class LateBoundPublishConsumer;
  using PublishReplyTask =
      folly::coro::Task<folly::Expected<PublishOk, PublishError>>;

  explicit MoQProxy(
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders);

  static PublishReplyTask forwardPublish(
      std::shared_ptr<MoQProxy> self,
      PublishRequest publishRequest,
      std::shared_ptr<Publisher::SubscriptionHandle> handle,
      std::shared_ptr<LateBoundPublishConsumer> consumer);

  template <typename Result, typename Operation>
  folly::coro::Task<folly::Expected<Result, RequestError>> tryUpstreams(
      RequestID requestID,
      const FullTrackName& fullTrackName,
      const TrackRequestParameters& params,
      Operation operation);

  std::shared_ptr<MoQProxyTrack> getOrCreateTrack(
      const FullTrackName& fullTrackName);

  void onNoSubscribers(MoQProxyTrack* track) override;

  std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders_;
  std::shared_ptr<MoQCache> cache_;
  folly::F14FastMap<
      FullTrackName,
      std::shared_ptr<MoQProxyTrack>,
      FullTrackName::hash>
      tracks_;
  bool closed_{false};
};

} // namespace moxygen
