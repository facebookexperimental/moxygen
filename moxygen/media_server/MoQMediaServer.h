/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQServer.h>
#include <moxygen/media_server/MoQBroadcastDispatcher.h>

#include <folly/logging/xlog.h>

#include <memory>
#include <string>
#include <utility>

namespace moxygen::media_server {

// A MoQ origin that accepts subscribers (WebTransport + raw QUIC) and serves
// tracks via a MoQBroadcastDispatcher. The caller registers tracks on the
// publisher and starts one runPublishLoop() per track (see the binary).
class MoQMediaServer : public MoQServer {
 public:
  MoQMediaServer(
      std::shared_ptr<const fizz::server::FizzServerContext> fizzContext,
      std::string endpoint,
      std::shared_ptr<MoQBroadcastDispatcher> publisher)
      : MoQServer(std::move(fizzContext), std::move(endpoint)),
        publisher_(std::move(publisher)) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    XLOG(INFO) << "[MoQMediaServer] onNewSession";
    clientSession->setPublishHandler(publisher_);
  }

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    XLOG(INFO) << "[MoQMediaServer] terminateClientSession";
    publisher_->removeSubscriber(session, "terminateClientSession");
  }

 private:
  std::shared_ptr<MoQBroadcastDispatcher> publisher_;
};

} // namespace moxygen::media_server
