/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQFilters.h"
#include "moxygen/Publisher.h"

#include <folly/coro/Task.h>

#include <memory>

namespace moxygen::lane_manager {

class ManagedSubscription final
    : public TrackConsumerFilter,
      public std::enable_shared_from_this<ManagedSubscription> {
 public:
  using SubscribeResult = folly::Expected<SubscribeOk, SubscribeError>;

  ManagedSubscription(
      std::shared_ptr<Publisher> upstream,
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> downstreamLaneConsumer);
  ~ManagedSubscription() override;

  ManagedSubscription(const ManagedSubscription&) = delete;
  ManagedSubscription& operator=(const ManagedSubscription&) = delete;
  ManagedSubscription(ManagedSubscription&&) = delete;
  ManagedSubscription& operator=(ManagedSubscription&&) = delete;

  folly::coro::Task<SubscribeResult> subscribe();
  void unsubscribe();

  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override;

 private:
  std::shared_ptr<Publisher> upstream_;
  SubscribeRequest requestTemplate_;
  std::shared_ptr<Publisher::SubscriptionHandle> subscription_;
  bool stopped_{false};
};

} // namespace moxygen::lane_manager
