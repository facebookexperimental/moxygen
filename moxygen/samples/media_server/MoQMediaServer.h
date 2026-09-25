/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <moxygen/MoQQmuxServer.h>
#include <moxygen/MoQServer.h>
#include <moxygen/samples/media_server/MoQBroadcastDispatcher.h>

#include <folly/logging/xlog.h>

#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace moxygen::media_server {

// A MoQ origin that accepts subscribers and serves tracks via a
// MoQBroadcastDispatcher. ServerBase picks the transport: MoQServer
// (WebTransport + raw QUIC) or MoQQmuxServer (QMUX-on-TCP). Listeners that
// share a dispatcher must share its worker EventBase, since the dispatcher and
// its forwarders are not thread-safe.
template <typename ServerBase>
class MoQMediaServerImpl : public ServerBase {
 public:
  template <typename... BaseArgs>
  explicit MoQMediaServerImpl(
      std::shared_ptr<MoQBroadcastDispatcher> publisher,
      BaseArgs&&... baseArgs)
      : ServerBase(std::forward<BaseArgs>(baseArgs)...),
        publisher_(std::move(publisher)) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    XLOG(INFO) << "[MoQMediaServer:" << kTransport
               << "] onNewSession sess=" << clientSession.get();
    clientSession->setPublishHandler(publisher_);
  }

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    XLOG(INFO) << "[MoQMediaServer:" << kTransport
               << "] terminateClientSession sess=" << session.get();
    publisher_->removeSubscriber(session, "terminateClientSession");
  }

 private:
  static_assert(
      std::is_same_v<ServerBase, MoQServer> ||
          std::is_same_v<ServerBase, MoQQmuxServer>,
      "kTransport only names MoQServer (quic) and MoQQmuxServer (qmux)");
  static constexpr std::string_view kTransport =
      std::is_same_v<ServerBase, MoQQmuxServer> ? "qmux" : "quic";

  std::shared_ptr<MoQBroadcastDispatcher> publisher_;
};

using MoQMediaServer = MoQMediaServerImpl<MoQServer>;
using MoQMediaQmuxServer = MoQMediaServerImpl<MoQQmuxServer>;

} // namespace moxygen::media_server
