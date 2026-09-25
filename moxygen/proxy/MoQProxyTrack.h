/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Expected.h>
#include <folly/coro/SharedPromise.h>
#include <folly/coro/Task.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "moxygen/Publisher.h"
#include "moxygen/proxy/MoQUpstreamProvider.h"

namespace moxygen {

class MoQForwarder;
class MoQSession;
class TrackConsumer;

class MoQProxyTrack : public std::enable_shared_from_this<MoQProxyTrack> {
 public:
  // The peer a subscription is served to. MoQProxyTrack never holds the
  // session, so it is handed the facts it needs.
  struct DownstreamPeer {
    SessionId sessionId;
    uint64_t version{0};
  };

  class Callback {
   public:
    virtual ~Callback() = default;
    virtual void onNoSubscribers(MoQProxyTrack* track) = 0;
  };

  static std::shared_ptr<MoQProxyTrack> create(
      FullTrackName fullTrackName,
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders);

  ~MoQProxyTrack();

  MoQProxyTrack(const MoQProxyTrack&) = delete;
  MoQProxyTrack& operator=(const MoQProxyTrack&) = delete;

  const FullTrackName& fullTrackName() const {
    return fullTrackName_;
  }

  void setCallback(std::weak_ptr<Callback> callback) {
    callback_ = std::move(callback);
  }

  folly::coro::Task<Publisher::SubscribeResult> subscribe(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer,
      DownstreamPeer downstream);

  void close();

 private:
  enum class State { IDLE, CONNECTING, READY, DRAINING, CLOSED };

  struct UpstreamEstablishmentFailure {
    SubscribeErrorCode errorCode;
    std::string reasonPhrase;
  };

  struct EstablishedUpstream {
    std::shared_ptr<MoQSession> session;
    std::shared_ptr<Publisher::SubscriptionHandle> handle;
  };

  using UpstreamEstablishmentResult =
      folly::Expected<EstablishedUpstream, SubscribeError>;

  class DownstreamSubscriptionHandle;
  class ForwarderCallback;

  MoQProxyTrack(
      FullTrackName fullTrackName,
      std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders);

  folly::coro::Task<Publisher::SubscribeResult> handleFirstSubscription(
      SubscribeRequest subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer,
      DownstreamPeer downstream);

  Publisher::SubscribeResult addSubscriber(
      const SubscribeRequest& subscribeRequest,
      std::shared_ptr<TrackConsumer> consumer,
      DownstreamPeer downstream);

  folly::coro::Task<std::optional<SubscribeError>> establishUpstream(
      const SubscribeRequest& subscribeRequest,
      DownstreamPeer downstream);

  folly::coro::Task<UpstreamEstablishmentResult> establishWithProvider(
      const std::shared_ptr<MoQUpstreamProvider>& upstreamProvider,
      const SubscribeRequest& subscribeRequest,
      DownstreamPeer downstream,
      bool hasFallbackProvider);

  SubscribeError makeSubscribeError(
      RequestID requestID,
      const UpstreamEstablishmentFailure& failure) const;

  void completeUpstreamEstablishment(
      std::optional<UpstreamEstablishmentFailure> failure);
  void onForwarderEmpty();
  void onUpstreamPublishDone();
  void notifyNoSubscribers();

  FullTrackName fullTrackName_;
  std::vector<std::shared_ptr<MoQUpstreamProvider>> upstreamProviders_;
  std::shared_ptr<MoQForwarder> forwarder_;
  std::shared_ptr<MoQSession> upstreamSession_;
  std::shared_ptr<Publisher::SubscriptionHandle> upstreamHandle_;
  std::weak_ptr<Callback> callback_;
  folly::coro::SharedPromise<folly::Unit> upstreamSubscriptionReadyPromise_;
  std::optional<UpstreamEstablishmentFailure> upstreamSubscriptionFailure_;
  State state_{State::IDLE};
  bool upstreamSubscriptionPromiseResolved_{false};
  bool noSubscribersNotificationSent_{false};
};

} // namespace moxygen
