/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the Apache 2.0 license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/io/async/ScopedEventBaseThread.h>
#include <proxygen/httpserver/samples/hq/FizzContext.h>
#include <iostream>
#include "moxygen/QmuxUtils.h"
#include "moxygen/mlog/FileMLogger.h"
#include "moxygen/mlog/FileMLoggerFactory.h"
#include "moxygen/moqtest/MoQTestServer.h"
#include "moxygen/moqtest/Utils.h"
#include "moxygen/samples/util/Utils.h"

namespace moxygen {

} // namespace moxygen

const std::string kDefaultServerFilePath = "./mlog_server.txt";

DEFINE_int32(port, 9999, "Port to listen on");
DEFINE_bool(log, false, "Log to mlog file");
DEFINE_string(mlog_path, kDefaultServerFilePath, "Path to mlog file.");
DEFINE_string(relay_url, "", "Relay server URL to connect to");
DEFINE_int32(relay_connect_timeout, 1000, "Relay connect timeout (ms)");
DEFINE_int32(
    relay_transaction_timeout,
    1200000,
    "Relay transaction timeout (s)");
DEFINE_string(
    transport,
    "h3wt",
    "Relay-client transport (used when --relay_url is set): 'quic' (raw QUIC), "
    "'h3wt' (HTTP/3 + WebTransport, default), 'qmux' (QMUX-on-TCP, TLS via "
    "Fizz mandatory).");
DEFINE_bool(
    quic_transport,
    false,
    "DEPRECATED: use --transport=quic (or --transport=h3wt) instead. "
    "Selects raw QUIC vs WebTransport for the relay client.");
DEFINE_string(cert, "", "Path to TLS certificate file");
DEFINE_string(key, "", "Path to TLS private key file");
DEFINE_string(
    versions,
    "",
    "Comma-separated MoQ draft versions (e.g. '14,16'). Empty = all supported.");
DEFINE_bool(
    include_timestamp_extension,
    false,
    "Stamp each object with a send-time millisecond timestamp extension "
    "(used by the perf client for latency measurement).");
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

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);
  XLOG_IF(FATAL, !FLAGS_quic && !FLAGS_qmux)
      << "At least one of --quic or --qmux must be enabled";

  // MoQTestServer owns the Publisher state (subscriptions, etc.) regardless
  // of whether its own QUIC listener is started. The QMUX server, when
  // enabled, points at it for publisher callbacks.
  auto server = std::make_shared<moxygen::MoQTestServer>(
      FLAGS_cert, FLAGS_key, FLAGS_versions);
  server->setIncludeTimestampExtension(FLAGS_include_timestamp_extension);

  std::shared_ptr<moxygen::MoQTestQmuxServer> qmuxServer;
  if (FLAGS_qmux) {
    auto qmuxAlpns = moxygen::getMoqtProtocols(FLAGS_versions, true);
    auto fizzContext =
        quic::samples::createFizzServerContextWithInsecureDefault(
            qmuxAlpns,
            fizz::server::ClientAuthMode::None,
            FLAGS_cert,
            FLAGS_key);
    moxygen::MoQTestQmuxServer::Config config;
    config.selfTransportParams =
        moxygen::qmuxParamsFromTransportSettings(quic::TransportSettings{});
    qmuxServer = std::make_shared<moxygen::MoQTestQmuxServer>(
        server, "/test", std::move(fizzContext), std::move(config));
  }

  // MoQTestServer publisher state is single-threaded; share one worker EB
  // across stacks (and reuse it for the relay client when --relay_url is set).
  folly::ScopedEventBaseThread worker("MoQTestWorker");
  std::vector<folly::EventBase*> workerEvbs{worker.getEventBase()};
  folly::SocketAddress addr("::", FLAGS_port);
  if (FLAGS_quic) {
    server->start(addr, workerEvbs);
  }
  if (qmuxServer) {
    qmuxServer->start(addr, workerEvbs);
  }

  // If relay URL provided, connect as relay client. Relay-client transport
  // comes from --transport (or the deprecated --quic_transport).
  if (!FLAGS_relay_url.empty()) {
    auto transportType =
        moxygen::samples::selectClientTransport("transport", "quic_transport");
    if (!server->startRelayClient(
            worker.getEventBase(),
            FLAGS_relay_url,
            FLAGS_relay_connect_timeout,
            FLAGS_relay_transaction_timeout,
            transportType)) {
      XLOG(ERR) << "Failed to start relay client";
      return 1;
    }
  }

  if (FLAGS_log) {
    auto factory = std::make_shared<moxygen::FileMLoggerFactory>(
        FLAGS_mlog_path, moxygen::VantagePoint::SERVER);
    server->setMLoggerFactory(factory);
    if (qmuxServer) {
      qmuxServer->setMLoggerFactory(factory);
    }
    std::cout << "Type Anything To Exit Server...";
    std::string line;
    std::getline(std::cin, line);
    return 0;
  } else {
    folly::EventBase evb;
    evb.loopForever();
  }
}
