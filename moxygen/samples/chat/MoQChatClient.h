/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQClient.h"

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
  void subscribeUpdate(SubscribeUpdate) override {}
  void unsubscribe() override;

  class AnnounceHandle : public Subscriber::AnnounceHandle {
   public:
    AnnounceHandle(AnnounceOk ok, std::shared_ptr<MoQChatClient> client)
        : Subscriber::AnnounceHandle(std::move(ok)),
          client_(std::move(client)) {}

    void unannounce() override {
      client_->unannounce(announceOk_->trackNamespace);
      client_.reset();
    }

   private:
    std::shared_ptr<MoQChatClient> client_;
  };

  folly::coro::Task<Subscriber::AnnounceResult> announce(
      Announce announce,
      std::shared_ptr<AnnounceCallback>) override;
  void unannounce(const TrackNamespace&);

  void publishLoop();
  folly::coro::Task<void> subscribeToUser(TrackNamespace trackNamespace);
  void subscribeDone(SubscribeDone subDone);

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
  MoQClient moqClient_;
  folly::Optional<RequestID> chatRequestID_;
  folly::Optional<TrackAlias> chatTrackAlias_;
  std::shared_ptr<TrackConsumer> publisher_;
  uint64_t nextGroup_{0};
  struct UserTrack {
    std::string deviceId;
    std::chrono::seconds timestamp;
    RequestID requestID;
    std::shared_ptr<Publisher::SubscriptionHandle> subscription;
  };
  std::map<std::string, std::vector<UserTrack>> subscriptions_;
  std::pair<folly::coro::Promise<ServerSetup>, folly::coro::Future<ServerSetup>>
      peerSetup_{folly::coro::makePromiseContract<ServerSetup>()};
  std::shared_ptr<Subscriber::AnnounceHandle> announceHandle_;
  std::shared_ptr<Publisher::SubscribeAnnouncesHandle> subscribeAnnounceHandle_;
};

} // namespace moxygen
