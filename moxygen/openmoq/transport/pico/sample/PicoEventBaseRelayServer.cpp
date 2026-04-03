/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQRelaySession.h"
#include "moxygen/openmoq/transport/pico/MoQPicoQuicEventBaseServer.h"
#include "moxygen/relay/MoQRelay.h"

#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <gflags/gflags.h>

#include <csignal>

DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_string(endpoint, "/moq-relay", "End point");
DEFINE_int32(port, 9668, "Relay Server Port");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. \"14,16\"). Empty = all supported.");
DEFINE_int32(
    max_cached_tracks,
    100,
    "Maximum number of cached tracks (0 to disable caching)");
DEFINE_int32(
    max_cached_groups_per_track,
    3,
    "Maximum groups per track in cache");

namespace {
using namespace moxygen;

folly::EventBase* gEvb{nullptr};

void signalHandler(int /*sig*/) {
  if (gEvb) {
    gEvb->terminateLoopSoon();
  }
}

class PicoEventBaseRelayServer : public MoQPicoQuicEventBaseServer {
 public:
  PicoEventBaseRelayServer(
      const std::string& cert,
      const std::string& key,
      const std::string& endpoint,
      folly::Executor::KeepAlive<folly::EventBase> evb,
      const std::string& versions,
      std::shared_ptr<MoQRelay> relay)
      : MoQPicoQuicEventBaseServer(
            cert, key, endpoint, std::move(evb), versions),
        relay_(std::move(relay)) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(relay_);
    clientSession->setSubscribeHandler(relay_);
  }

 protected:
  std::shared_ptr<MoQSession> createSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      std::shared_ptr<MoQExecutor> executor) override {
    return std::make_shared<MoQRelaySession>(
        folly::MaybeManagedPtr<proxygen::WebTransport>(std::move(wt)),
        *this,
        std::move(executor));
  }

 private:
  std::shared_ptr<MoQRelay> relay_;
};
} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);

  // Declare relay and server before evb so they are destroyed after evb.
  // evb's destructor drains pending coroutine continuations (e.g.
  // handleClientSession resuming after session teardown), which access the
  // server via `this`. server must still be alive at that point.
  auto relay = std::make_shared<moxygen::MoQRelay>(
      FLAGS_max_cached_tracks, FLAGS_max_cached_groups_per_track);
  std::shared_ptr<PicoEventBaseRelayServer> server;

  folly::EventBase evb;

  server = std::make_shared<PicoEventBaseRelayServer>(
      FLAGS_cert,
      FLAGS_key,
      FLAGS_endpoint,
      folly::getKeepAliveToken(&evb),
      FLAGS_versions,
      relay);

  folly::SocketAddress addr("::", FLAGS_port);
  server->start(addr);

  gEvb = &evb;
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  evb.loop();

  gEvb = nullptr;
  server->stop();
  return 0;
}
