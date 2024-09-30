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
      std::string username);

  folly::coro::Task<void> run() noexcept;

 private:
  folly::coro::Task<void> controlReadLoop();
  folly::coro::Task<void> readCatalogUpdates(
      std::shared_ptr<MoQSession::TrackHandle> catalogTrack);
  void publishLoop();
  folly::coro::Task<void> subscribeToUser(std::string username);

  [[nodiscard]] std::vector<std::string> chatPrefix() const {
    return {"moq-chat/", chatID_};
  }

  [[nodiscard]] FullTrackName catalogTrackName() const {
    return FullTrackName({TrackNamespace(chatPrefix()), "/catalog"});
  }

  [[nodiscard]] std::vector<std::string> participantPrefix() const {
    auto prefix = chatPrefix();
    prefix.emplace_back("/participant/");
    return prefix;
  }

  [[nodiscard]] bool isParticipantNamespace(const TrackNamespace& ns) const {
    return ns.startsWith(TrackNamespace(participantPrefix()));
  }

  [[nodiscard]] TrackNamespace participantTrackName(
      const std::string& username) const {
    auto prefix = participantPrefix();
    prefix.emplace_back(username);
    return TrackNamespace(std::move(prefix));
  }

  std::string chatID_;
  std::string username_;
  MoQClient moqClient_;
  folly::Optional<uint64_t> chatSubscribeID_;
  folly::Optional<uint64_t> chatTrackAlias_;
  uint64_t nextGroup_{0};
  std::set<std::string> subscriptions_;
  std::pair<folly::coro::Promise<ServerSetup>, folly::coro::Future<ServerSetup>>
      peerSetup_{folly::coro::makePromiseContract<ServerSetup>()};
};

} // namespace moxygen
