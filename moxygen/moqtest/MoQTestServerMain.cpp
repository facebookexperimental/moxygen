/*
 *  Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 *  This source code is licensed under the MIT license found in the LICENSE
 *  file in the root directory of this source tree.
 *
 */

#include "moxygen/moqtest/MoQTestServer.h"

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
DEFINE_bool(
    quic_transport,
    false,
    "Use raw QUIC transport instead of WebTransport");
DEFINE_string(cert, "", "Path to TLS certificate file");
DEFINE_string(key, "", "Path to TLS private key file");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);

  auto server = std::make_shared<moxygen::MoQTestServer>(FLAGS_cert, FLAGS_key);

  folly::SocketAddress addr("::", FLAGS_port);
  server->start(addr);

  // If relay URL provided, connect as relay client
  if (!FLAGS_relay_url.empty() &&
      !server->startRelayClient(
          FLAGS_relay_url,
          FLAGS_relay_connect_timeout,
          FLAGS_relay_transaction_timeout,
          FLAGS_quic_transport)) {
    XLOG(ERR) << "Failed to start relay client";
    return 1;
  }

  if (FLAGS_log) {
    server->logger_ =
        std::make_shared<moxygen::MLogger>(moxygen::VantagePoint::SERVER);
    server->logger_->setPath(FLAGS_mlog_path);
    std::cout << "Type Anything To Exit Server...";
    std::string line;
    std::getline(std::cin, line);
    server->logger_->outputLogsToFile();
    return 0;
  } else {
    folly::EventBase evb;
    evb.loopForever();
  }
}
