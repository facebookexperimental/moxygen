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

  [[nodiscard]] std::string chatPrefix() const {
    return folly::to<std::string>("moq-chat/", chatID_);
  }

  [[nodiscard]] FullTrackName catalogTrackName() const {
    return FullTrackName({chatPrefix(), "/catalog"});
  }

  [[nodiscard]] std::string participantPrefix() const {
    return folly::to<std::string>(chatPrefix(), "/participant/");
  }

  [[nodiscard]] bool isParticipantNamespace(const std::string& ns) const {
    return ns.starts_with(participantPrefix());
  }

  [[nodiscard]] std::string participantTrackName(
      const std::string& username) const {
    return folly::to<std::string>(participantPrefix(), username);
  }

  std::string chatID_;
  std::string username_;
  MoQClient moqClient_;
  folly::Optional<uint64_t> chatSubscribeID_;
  folly::Optional<uint64_t> chatTrackAlias_;
  std::set<std::string> subscriptions_;
  std::pair<folly::coro::Promise<ServerSetup>, folly::coro::Future<ServerSetup>>
      peerSetup_{folly::coro::makePromiseContract<ServerSetup>()};
};

} // namespace moxygen
