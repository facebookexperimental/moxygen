/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/samples/echo_server/MoQAudioEchoServer.h"

#include <utility>

#include <folly/Optional.h>
#include <folly/container/F14Map.h>
#include <folly/logging/xlog.h>

#include <moxygen/MoQFramer.h>

namespace moxygen {

MoQAudioEchoServer::MoQAudioEchoServer(
    uint16_t port,
    std::string cert,
    std::string key,
    std::string endpoint)
    : MoQServer(port, std::move(cert), std::move(key), std::move(endpoint)) {}

void MoQAudioEchoServer::onNewSession(
    std::shared_ptr<MoQSession> clientSession) {
  clientSession->setPublishHandler(handler_);
  clientSession->setSubscribeHandler(handler_);
}

void MoQAudioEchoServer::terminateClientSession(
    std::shared_ptr<MoQSession> /*session*/) {
  // No-op: no legacy per-session state to clean up in publish+publish mode.
}

// ---- EchoHandler ----

Subscriber::PublishResult MoQAudioEchoServer::EchoHandler::publish(
    PublishRequest pub,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  // Only echo upstream audio track
  if (pub.fullTrackName.trackName != kUpstreamTrackName) {
    return folly::makeUnexpected(PublishError{
        pub.requestID, PublishErrorCode::NOT_SUPPORTED, "Unknown track"});
  }
  // Republish as echo0 within the same namespace back to current session
  pub.fullTrackName.trackName = kEchoTrackName;
  auto session = MoQSession::getRequestSession();
  return session->publish(std::move(pub), std::move(handle));
}

void MoQAudioEchoServer::EchoHandler::removeSession(
    const std::shared_ptr<MoQSession>& /*session*/) {
  // No-op: no per-session forwarding state to remove in publish+publish mode.
}

} // namespace moxygen
