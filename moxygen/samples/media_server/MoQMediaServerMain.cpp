/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <moxygen/samples/media_server/FilePrControlServer.h>
#include <moxygen/samples/media_server/MoQBroadcastDispatcher.h>
#include <moxygen/samples/media_server/MoQBroadcastFactory.h>
#include <moxygen/samples/media_server/MoQMediaListeners.h>
#include <moxygen/util/SignalHandler.h>

#include <folly/SocketAddress.h>
#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#include <folly/logging/xlog.h>

#include <chrono>
#include <memory>
#include <string>

DEFINE_int32(port, 9779, "Server port (UDP for QUIC, TCP for QMUX)");
DEFINE_bool(quic, true, "Listen on QUIC/WebTransport (UDP)");
DEFINE_bool(qmux, true, "Listen on QMUX-on-TCP (TLS via Fizz is mandatory)");
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
    "tuple field 'file', 'file_pr', or 'file_abr' is served from it.");
DEFINE_int32(fragment_interval_ms, 1000, "fMP4 playback window width (ms)");
DEFINE_int32(
    catalog_update_interval,
    10,
    "Seconds between file_abr catalog updates");
DEFINE_bool(loop, false, "Loop the fMP4 source forever");
DEFINE_int32(
    file_pr_control_port,
    60101,
    "Loopback HTTP port for file_pr fault controls; 0 disables the UI");

namespace {
using namespace moxygen;
using namespace moxygen::media_server;
} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);

  XCHECK(!FLAGS_input.empty()) << "--input is required";
  XCHECK(FLAGS_quic || FLAGS_qmux)
      << "At least one of --quic or --qmux must be enabled";
  XCHECK_GT(FLAGS_fragment_interval_ms, 0);
  XCHECK_GT(FLAGS_catalog_update_interval, 0);
  XCHECK_GE(FLAGS_file_pr_control_port, 0);
  XCHECK_LE(FLAGS_file_pr_control_port, 65535);

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
          std::chrono::seconds(FLAGS_catalog_update_interval),
          FLAGS_loop,
          workerEvb),
      *workerEvb);

  auto listeners = startMediaListeners(
      dispatcher,
      folly::SocketAddress("::", FLAGS_port),
      MediaListenerOptions{
          .quic = FLAGS_quic,
          .qmux = FLAGS_qmux,
          .cert = FLAGS_cert,
          .key = FLAGS_key,
          .insecure = FLAGS_insecure});
  XLOG(INFO) << "[main] MoQMediaServer listening port=" << FLAGS_port
             << " quic=" << FLAGS_quic << " qmux=" << FLAGS_qmux
             << " (namespaces resolved by prefix; file backend input="
             << FLAGS_input << ")";

  std::unique_ptr<FilePrControlServer> filePrControl;
  if (FLAGS_file_pr_control_port > 0) {
    filePrControl = std::make_unique<FilePrControlServer>(
        static_cast<uint16_t>(FLAGS_file_pr_control_port));
    XCHECK(filePrControl->start()) << "cannot start file_pr control server";
  }

  folly::EventBase evb;
  moxygen::SignalHandler handler(&evb, [&evb](int sig) {
    XLOG(INFO) << "[main] received signal " << sig << ", shutting down";
    evb.terminateLoopSoon();
  });
  evb.loopForever();

  filePrControl.reset();
  listeners.stop();
  XLOG(INFO) << "[main] stopped";
  return 0;
}
