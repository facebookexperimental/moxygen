/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>

#include <moxygen/MoQServer.h>
#include <moxygen/MoQSession.h>
#include <moxygen/Publisher.h>
#include <moxygen/Subscriber.h>

namespace moxygen {

// Echo server wrapper that wires handler into new sessions
class MoQAudioEchoServer : public MoQServer {
 public:
  MoQAudioEchoServer(std::string cert, std::string key, std::string endpoint);

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override;
  void terminateClientSession(std::shared_ptr<MoQSession> session) override;

 private:
  // Echo handler implements publish+publish refactor: accept inbound
  // PUBLISH(ns/audio0) and republish back to the same session as ns/echo0.
  class EchoHandler : public Publisher,
                      public Subscriber,
                      public std::enable_shared_from_this<EchoHandler> {
   public:
    EchoHandler() = default;

    // Subscriber overrides
    // NEW: publish+publish echo path
    Subscriber::PublishResult publish(
        PublishRequest pub,
        std::shared_ptr<Publisher::SubscriptionHandle> handle) override;

    void goaway(Goaway) override {}

    void removeSession(const std::shared_ptr<MoQSession>& session);

   private:
    static constexpr const char* kUpstreamTrackName = "audio0";
    static constexpr const char* kEchoTrackName = "echo0";
  };

  std::shared_ptr<EchoHandler> handler_{std::make_shared<EchoHandler>()};
};

} // namespace moxygen
