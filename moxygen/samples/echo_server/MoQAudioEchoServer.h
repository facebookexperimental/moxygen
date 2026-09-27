/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <memory>

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

 private:
  // Echo handler implements publish+publish refactor: accept inbound
  // PUBLISH(ns/audio0) and republish back to the same session as ns/echo0.
  // One per session, so the peer to echo to is the one it is bound to.
  class EchoHandler : public Publisher,
                      public Subscriber,
                      public SessionScoped {
   public:
    // Subscriber overrides
    // NEW: publish+publish echo path
    Subscriber::PublishResult publish(
        PublishRequest pub,
        std::shared_ptr<Publisher::SubscriptionHandle> handle) override;

    void goaway(Goaway) override {}

   private:
    static constexpr const char* kUpstreamTrackName = "audio0";
    static constexpr const char* kEchoTrackName = "echo0";
  };
};

} // namespace moxygen
