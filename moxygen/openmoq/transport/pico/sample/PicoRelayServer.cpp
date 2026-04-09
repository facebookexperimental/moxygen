/*
 * Copyright (c) OpenMOQ contributors.
 * This source code is licensed under the Apache 2.0 license found in the
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
    "Comma-separated MOQT draft versions (e.g. \"14,16\"). Empty = all supported.");
DEFINE_int32(
    max_cached_tracks,
    100,
    "Maximum number of cached tracks (0 to disable caching)");
DEFINE_int32(
    max_cached_groups_per_track,
    3,
    "Maximum groups per track in cache");

// WebTransport configuration
DEFINE_bool(
    enable_webtransport,
    false,
    "Enable HTTP/3 WebTransport support for browser clients");
DEFINE_bool(
    enable_quic_transport,
    true,
    "Enable QUIC transport for non-browser clients");
DEFINE_string(wt_endpoint, "/moq", "WebTransport CONNECT endpoint path");
DEFINE_int32(wt_max_sessions, 100, "Maximum concurrent WebTransport sessions");

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
      PicoWebTransportConfig wtConfig,
      std::shared_ptr<MoQRelay> relay)
      : MoQPicoQuicServer(
            cert,
            key,
            endpoint,
            versions,
            {},
            std::move(wtConfig)),
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

  // Configure WebTransport
  PicoWebTransportConfig wtConfig;
  wtConfig.enableWebTransport = FLAGS_enable_webtransport;
  wtConfig.enableQuicTransport = FLAGS_enable_quic_transport;
  wtConfig.wtEndpoints = {FLAGS_wt_endpoint};
  wtConfig.wtMaxSessions = static_cast<uint32_t>(FLAGS_wt_max_sessions);

  auto server = std::make_shared<PicoRelayServer>(
      FLAGS_cert,
      FLAGS_key,
      FLAGS_endpoint,
      FLAGS_versions,
      std::move(wtConfig),
      relay);

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
