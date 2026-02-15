/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/events/MoQFollyExecutorImpl.h>
#include <memory>
#include "moxygen/MoQClient.h"
#include "moxygen/relay/MoQRelayClient.h"

namespace moxygen {

class MoQChatClient : public Publisher,
                      public Publisher::SubscriptionHandle,
                      public Subscriber,
                      public std::enable_shared_from_this<MoQChatClient> {
 public:
  MoQChatClient(
      folly::EventBase* evb,
      proxygen::URL url,
      std::string chatID,
      std::string username,
      std::string device);

  folly::coro::Task<void> run() noexcept;

 private:
  folly::coro::Task<Publisher::SubscribeResult> subscribe(
      SubscribeRequest subscribeReq,
      std::shared_ptr<TrackConsumer> consumer) override;
  folly::coro::Task<RequestUpdateResult> requestUpdate(
      RequestUpdate reqUpdate) override {
    co_return folly::makeUnexpected(
        RequestError{
            reqUpdate.requestID,
            RequestErrorCode::NOT_SUPPORTED,
            "Request update not implemented"});
  }
  void unsubscribe() override;

  class PublishNamespaceHandle : public Subscriber::PublishNamespaceHandle {
   public:
    PublishNamespaceHandle(
        PublishNamespaceOk ok,
        std::shared_ptr<MoQChatClient> client,
        TrackNamespace trackNamespace)
        : Subscriber::PublishNamespaceHandle(std::move(ok)),
          client_(std::move(client)),
          trackNamespace_(std::move(trackNamespace)) {}

    void publishNamespaceDone() override {
      client_->publishNamespaceDone(trackNamespace_);
      client_.reset();
    }

   private:
    std::shared_ptr<MoQChatClient> client_;
    TrackNamespace trackNamespace_;
  };

  folly::coro::Task<Subscriber::PublishNamespaceResult> publishNamespace(
      PublishNamespace publishNamespace,
      std::shared_ptr<PublishNamespaceCallback>) override;
  void publishNamespaceDone(const TrackNamespace&);

  void publishLoop();
  folly::coro::Task<void> subscribeToUser(TrackNamespace trackNamespace);
  void publishDone(PublishDone pubDone);

  [[nodiscard]] std::vector<std::string> chatPrefix() const {
    return {"moq-chat", chatID_};
  }

  [[nodiscard]] bool isParticipantNamespace(const TrackNamespace& ns) const {
    return ns.startsWith(TrackNamespace(chatPrefix()));
  }

  [[nodiscard]] TrackNamespace participantTrackName(
      const std::string& username) const {
    auto prefix = chatPrefix();
    prefix.emplace_back(username);
    prefix.emplace_back(deviceId_);
    prefix.emplace_back(timestampString_);
    return TrackNamespace(std::move(prefix));
  }

  std::string chatID_;
  std::string username_;
  std::string deviceId_;
  std::string timestampString_;
  std::shared_ptr<MoQFollyExecutorImpl> executor_;
  MoQRelayClient moqClient_;
  std::optional<RequestID> chatRequestID_;
  std::optional<TrackAlias> chatTrackAlias_;
  std::shared_ptr<TrackConsumer> publisher_;
  uint64_t nextGroup_{0};
  struct UserTrack {
    std::string deviceId;
    std::chrono::seconds timestamp;
    RequestID requestID;
    std::shared_ptr<Publisher::SubscriptionHandle> subscription;

    static UserTrack
    make(std::string device, std::chrono::seconds ts, RequestID reqId) {
      UserTrack ut;
      ut.deviceId = std::move(device);
      ut.timestamp = ts;
      ut.requestID = reqId;
      return ut;
    }
  };
  std::map<std::string, std::vector<UserTrack>> subscriptions_;
  std::pair<folly::coro::Promise<ServerSetup>, folly::coro::Future<ServerSetup>>
      peerSetup_{folly::coro::makePromiseContract<ServerSetup>()};
  std::shared_ptr<Subscriber::PublishNamespaceHandle> publishNamespaceHandle_;
  std::shared_ptr<Publisher::SubscribeNamespaceHandle>
      subscribeNamespaceHandle_;
};

} // namespace moxygen
