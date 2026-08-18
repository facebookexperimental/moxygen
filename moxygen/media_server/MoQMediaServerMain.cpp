/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/MoQVersions.h>
#include <moxygen/media_server/MoQBroadcastDispatcher.h>
#include <moxygen/media_server/MoQBroadcastFactory.h>
#include <moxygen/media_server/MoQMediaServer.h>
#include <moxygen/util/SignalHandler.h>

#include <proxygen/httpserver/samples/hq/FizzContext.h>

#include <folly/SocketAddress.h>
#include <folly/coro/Task.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/logging/xlog.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

DEFINE_int32(port, 9779, "Server port");
DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_bool(
    insecure,
    false,
    "Use insecure default certificate instead of --cert and --key");
DEFINE_string(
    input,
    "",
    "Catalog JSON for the file backend (required). Any namespace with first "
    "tuple field 'file' is served from it, e.g. file-<id>--video0.");
DEFINE_int32(fragment_interval_ms, 1000, "fMP4 per-fragment pacing (ms)");
DEFINE_bool(loop, false, "Loop the fMP4 source forever");

namespace {
using namespace moxygen;
using namespace moxygen::media_server;

std::vector<std::string> serverAlpns() {
  std::vector<std::string> alpns = {"h3"};
  auto moqt = getMoqtProtocols("", true);
  alpns.insert(alpns.end(), moqt.begin(), moqt.end());
  return alpns;
}
} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);

  XCHECK(!FLAGS_input.empty()) << "--input is required";

  folly::ScopedEventBaseThread worker("MoQMediaWorker");
  auto* workerEvb = worker.getEventBase();

  // The server boots content-agnostic: no namespace, no track table, no
  // resolver in sight. All backend/media wiring lives in the factory, which
  // selects a backend per namespace and builds each broadcast; the dispatcher
  // is a pure namespace registry. Publish loops run on the worker evb (same evb
  // as sessions and forwarders, which are not thread-safe).
  auto dispatcher = std::make_shared<MoQBroadcastDispatcher>(
      std::make_shared<MoQBroadcastFactory>(
          FLAGS_input,
          std::chrono::milliseconds(FLAGS_fragment_interval_ms),
          FLAGS_loop,
          workerEvb),
      workerEvb);

  const auto alpns = serverAlpns();
  auto fizzContext = FLAGS_insecure
      ? quic::samples::createFizzServerContextWithInsecureDefault(
            alpns,
            fizz::server::ClientAuthMode::None,
            "" /* cert */,
            "" /* key */)
      : quic::samples::createFizzServerContext(
            alpns, fizz::server::ClientAuthMode::None, FLAGS_cert, FLAGS_key);
  auto server =
      std::make_shared<MoQMediaServer>(fizzContext, "/moq-media", dispatcher);

  folly::SocketAddress addr("::", FLAGS_port);
  server->start(addr, {workerEvb});
  server->waitUntilInitialized();
  XLOG(INFO) << "[main] MoQMediaServer listening port=" << FLAGS_port
             << " (namespaces resolved by prefix; file backend input="
             << FLAGS_input << ")";

  folly::EventBase evb;
  moxygen::SignalHandler handler(&evb, [&evb](int sig) {
    XLOG(INFO) << "[main] received signal " << sig << ", shutting down";
    evb.terminateLoopSoon();
  });
  evb.loopForever();

  server->stop();
  XLOG(INFO) << "[main] stopped";
  return 0;
}
