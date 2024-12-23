/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "moxygen/MoQClient.h"

namespace moxygen {

class MoQChatClient {
 public:
  MoQChatClient(
      folly::EventBase* evb,
      proxygen::URL url,
      std::string chatID,
      std::string username,
      std::string device);

  folly::coro::Task<void> run() noexcept;

 private:
  folly::coro::Task<void> controlReadLoop();
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
  folly::Optional<SubscribeID> chatSubscribeID_;
  folly::Optional<TrackAlias> chatTrackAlias_;
  std::shared_ptr<TrackConsumer> publisher_;
  uint64_t nextGroup_{0};
  struct UserTrack {
    std::string deviceId;
    std::chrono::seconds timestamp;
    SubscribeID subscribeId;
  };
  std::map<std::string, std::vector<UserTrack>> subscriptions_;
  std::pair<folly::coro::Promise<ServerSetup>, folly::coro::Future<ServerSetup>>
      peerSetup_{folly::coro::makePromiseContract<ServerSetup>()};
};

} // namespace moxygen
