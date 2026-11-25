/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQConsumers.h"
#include "moxygen/Publisher.h"
#include "moxygen/events/MoQExecutor.h"

#include <folly/CancellationToken.h>
#include <folly/ExceptionWrapper.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Task.h>

#include <chrono>
#include <memory>
#include <optional>

namespace moxygen {
class MoQSession;
}

namespace moxygen::lane_manager {

class ManagedSubscription final
    : public TrackConsumer,
      public std::enable_shared_from_this<ManagedSubscription> {
 public:
  using SubscribeResult = folly::Expected<SubscribeOk, SubscribeError>;

  enum class State { Idle, Pending, Active, RetryWaiting, Ended, Stopped };

  ManagedSubscription(
      std::shared_ptr<Publisher> upstream,
      SubscribeRequest request,
      std::shared_ptr<TrackConsumer> downstreamLaneConsumer,
      MoQExecutor& executor);
  ~ManagedSubscription() override;

  ManagedSubscription(const ManagedSubscription&) = delete;
  ManagedSubscription& operator=(const ManagedSubscription&) = delete;
  ManagedSubscription(ManagedSubscription&&) = delete;
  ManagedSubscription& operator=(ManagedSubscription&&) = delete;

  folly::coro::Task<SubscribeResult> subscribe();
  folly::coro::Task<Publisher::FetchResult> subscribeWithJoiningFetch(
      RequestID downstreamRequestID,
      std::shared_ptr<FetchConsumer> consumer,
      FetchType fetchType);
  folly::coro::Task<Publisher::FetchResult> subscribeWithJoiningFetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer);
  void unsubscribe();

  State state() const {
    return state_;
  }

  bool ready() const {
    return state_ == State::Active;
  }

  bool ended() const {
    return state_ == State::Ended;
  }

  folly::Expected<folly::Unit, MoQPublishError> setTrackAlias(
      TrackAlias alias) override;
  folly::Expected<std::shared_ptr<SubgroupConsumer>, MoQPublishError>
  beginSubgroup(
      uint64_t groupID,
      uint64_t subgroupID,
      Priority priority,
      BeginSubgroupOptions options = {}) override;
  folly::Expected<folly::SemiFuture<folly::Unit>, MoQPublishError>
  awaitStreamCredit() override;
  folly::Expected<folly::Unit, MoQPublishError> objectStream(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override;
  folly::Expected<folly::Unit, MoQPublishError> datagram(
      const ObjectHeader& header,
      Payload payload,
      bool lastInGroup = false) override;
  folly::Expected<folly::Unit, MoQPublishError> publishDone(
      PublishDone pubDone) override;
  void setDeliveryCallback(std::shared_ptr<DeliveryCallback> callback) override;

 private:
  void setState(State state);
  void startAttempts(std::chrono::milliseconds delay);
  folly::coro::Task<Publisher::FetchResult> fetchWhenReady(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer,
      bool useSubscriptionGroupOrder);
  folly::coro::Task<Publisher::FetchResult> issueJoiningFetch(
      Fetch fetch,
      std::shared_ptr<FetchConsumer> consumer);
  static folly::coro::Task<void> runAttempts(
      std::shared_ptr<ManagedSubscription> self,
      std::chrono::milliseconds delay);

  std::shared_ptr<Publisher> upstream_;
  SubscribeRequest requestTemplate_;
  std::shared_ptr<TrackConsumer> downstreamLaneConsumer_;
  MoQExecutor& executor_;
  std::weak_ptr<MoQSession> requestSession_;
  std::shared_ptr<Publisher::SubscriptionHandle> subscription_;
  std::shared_ptr<folly::coro::SharedPromise<void>> stateChanged_;
  folly::CancellationSource stopSource_;
  std::optional<SubscribeError> lastError_;
  folly::exception_wrapper attemptException_;
  State state_{State::Idle};
  bool attemptInFlight_{false};
  bool wasActive_{false};
};

} // namespace moxygen::lane_manager
