/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/samples/echo_server/MoQAudioEchoServer.h"

#include <utility>

#include <optional>

namespace moxygen {

MoQAudioEchoServer::MoQAudioEchoServer(
    std::string cert,
    std::string key,
    std::string endpoint)
    : MoQServer(std::move(cert), std::move(key), std::move(endpoint)) {}

void MoQAudioEchoServer::onNewSession(
    std::shared_ptr<MoQSession> clientSession) {
  // Enable server timestamp stamping for echo sample
  MoQSettings settings;
  // TODO: Server timestamp stamping is app-layer; core no longer supports
  // stampServerTimestamps.
  clientSession->setMoqSettings(settings);

  auto handler = std::make_shared<EchoHandler>();
  clientSession->setPublishHandler(handler);
  clientSession->setSubscribeHandler(handler);
}

// ---- EchoHandler ----

Subscriber::PublishResult MoQAudioEchoServer::EchoHandler::publish(
    PublishRequest pub,
    std::shared_ptr<Publisher::SubscriptionHandle> handle) {
  // Only echo upstream audio track
  if (pub.fullTrackName.trackName != kUpstreamTrackName) {
    return folly::makeUnexpected(
        PublishError{
            pub.requestID, PublishErrorCode::NOT_SUPPORTED, "Unknown track"});
  }
  auto peer = getSession();
  if (!peer) {
    return folly::makeUnexpected(
        PublishError{
            pub.requestID,
            PublishErrorCode::INTERNAL_ERROR,
            "session is gone"});
  }
  // Republish as echo0 within the same namespace back to the peer
  pub.fullTrackName.trackName = kEchoTrackName;
  return peer->publish(std::move(pub), std::move(handle));
}

} // namespace moxygen
