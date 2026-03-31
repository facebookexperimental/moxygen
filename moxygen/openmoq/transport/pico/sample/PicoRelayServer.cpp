/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQRelaySession.h"
#include "moxygen/openmoq/transport/pico/MoQPicoQuicServer.h"
#include "moxygen/relay/MoQRelay.h"

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include <atomic>
#include <condition_variable>
#include <csignal>
#include <mutex>

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

std::atomic<bool> gShutdown{false};
std::mutex gMutex;
std::condition_variable gCV;

void signalHandler(int /*sig*/) {
  gShutdown.store(true);
  gCV.notify_all();
}

class PicoRelayServer : public MoQPicoQuicServer {
 public:
  PicoRelayServer(
      const std::string& cert,
      const std::string& key,
      const std::string& endpoint,
      const std::string& versions,
      std::shared_ptr<MoQRelay> relay)
      : MoQPicoQuicServer(cert, key, endpoint, versions),
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

  auto relay = std::make_shared<moxygen::MoQRelay>(
      FLAGS_max_cached_tracks, FLAGS_max_cached_groups_per_track);

  auto server = std::make_shared<PicoRelayServer>(
      FLAGS_cert, FLAGS_key, FLAGS_endpoint, FLAGS_versions, relay);

  folly::SocketAddress addr("::", FLAGS_port);
  server->start(addr);

  // Block until SIGINT or SIGTERM
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  {
    std::unique_lock<std::mutex> lock(gMutex);
    gCV.wait(lock, [] { return gShutdown.load(); });
  }

  server->stop();
  return 0;
}
