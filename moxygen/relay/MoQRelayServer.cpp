/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQQmuxServer.h"
#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQServer.h"
#include "moxygen/QmuxUtils.h"
#include "moxygen/relay/MoQRelay.h"

#include <folly/init/Init.h>
#include <folly/io/async/AsyncSignalHandler.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <signal.h>

using namespace proxygen;

DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_string(endpoint, "/moq-relay", "End point");
DEFINE_int32(port, 9668, "Relay Server Port");
DEFINE_bool(enable_cache, true, "Enable relay cache");
DEFINE_bool(
    insecure,
    false,
    "Use insecure verifier (skip certificate validation)");
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
DEFINE_bool(
    quic,
    true,
    "Listen on QUIC/WebTransport (UDP). May be combined with --qmux for "
    "dual-stack.");
DEFINE_bool(
    qmux,
    false,
    "Listen on QMUX-on-TCP (TLS via Fizz is mandatory). May be combined with "
    "--quic for dual-stack.");

namespace {
using namespace moxygen;

template <typename ServerBase>
class RelayServerImpl : public ServerBase {
 public:
  template <typename... BaseArgs>
  RelayServerImpl(std::shared_ptr<MoQRelay> relay, BaseArgs&&... baseArgs)
      : ServerBase(std::forward<BaseArgs>(baseArgs)...),
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

using MoQRelayQuicServer = RelayServerImpl<MoQServer>;
using MoQRelayQmuxServer = RelayServerImpl<MoQQmuxServer>;

} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  XLOG_IF(FATAL, !FLAGS_quic && !FLAGS_qmux)
      << "At least one of --quic or --qmux must be enabled";

  auto relay = std::make_shared<MoQRelay>(
      FLAGS_max_cached_tracks, FLAGS_max_cached_groups_per_track);

  folly::EventBase evb;
  std::shared_ptr<MoQRelayQuicServer> quicServer;
  std::shared_ptr<MoQRelayQmuxServer> qmuxServer;
  std::vector<std::shared_ptr<MoQServerBase>> servers;

  if (FLAGS_quic) {
    std::vector<std::string> alpns = {"h3"};
    auto moqt = getMoqtProtocols(FLAGS_versions, true);
    alpns.insert(alpns.end(), moqt.begin(), moqt.end());
    auto fizzContext = FLAGS_insecure
        ? quic::samples::createFizzServerContextWithInsecureDefault(
              alpns,
              fizz::server::ClientAuthMode::None,
              "" /* cert */,
              "" /* key */)
        : quic::samples::createFizzServerContext(
              alpns,
              fizz::server::ClientAuthMode::Optional,
              FLAGS_cert,
              FLAGS_key);
    quicServer = std::make_shared<MoQRelayQuicServer>(
        relay, std::move(fizzContext), FLAGS_endpoint);
    servers.push_back(quicServer);
  }

  if (FLAGS_qmux) {
    // QMUX runs straight on TCP+TLS — no WebTransport-over-HTTP/3, so the
    // ALPN list is just the MoQ versions (no "h3").
    auto qmuxAlpns = getMoqtProtocols(FLAGS_versions, true);
    auto fizzContext = FLAGS_insecure
        ? quic::samples::createFizzServerContextWithInsecureDefault(
              qmuxAlpns,
              fizz::server::ClientAuthMode::None,
              "" /* cert */,
              "" /* key */)
        : quic::samples::createFizzServerContext(
              qmuxAlpns,
              fizz::server::ClientAuthMode::Optional,
              FLAGS_cert,
              FLAGS_key);
    MoQRelayQmuxServer::Config config;
    config.selfTransportParams =
        qmuxParamsFromTransportSettings(quic::TransportSettings{});
    qmuxServer = std::make_shared<MoQRelayQmuxServer>(
        relay, FLAGS_endpoint, std::move(fizzContext), std::move(config));
    servers.push_back(qmuxServer);
  }

  // MoQRelay is single-threaded; share one worker EB across both stacks.
  folly::ScopedEventBaseThread worker("MoQRelayWorker");
  std::vector<folly::EventBase*> workerEvbs{worker.getEventBase()};
  folly::SocketAddress addr("::", FLAGS_port);
  if (quicServer) {
    quicServer->start(addr, workerEvbs);
  }
  if (qmuxServer) {
    qmuxServer->start(addr, workerEvbs);
  }

  struct SigHandler : public folly::AsyncSignalHandler {
    SigHandler(folly::EventBase* evb, std::function<void()> fn)
        : folly::AsyncSignalHandler(evb), fn_(std::move(fn)) {
      registerSignalHandler(SIGTERM);
      registerSignalHandler(SIGINT);
    }
    void signalReceived(int /*signum*/) noexcept override {
      unregisterSignalHandler(SIGTERM);
      unregisterSignalHandler(SIGINT);
      fn_();
    }
    std::function<void()> fn_;
  } sigHandler(&evb, [&] {
    XLOG(INFO) << "Caught signal, stopping relay server";
    for (auto& server : servers) {
      server->stop();
    }
    evb.terminateLoopSoon();
  });
  evb.loopForever();
  return 0;
}
