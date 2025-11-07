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

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, false);
  folly::Init init(&argc, &argv);
  if (FLAGS_log) {
    auto server = std::make_shared<moxygen::MoQTestServer>();
    folly::SocketAddress addr("::", FLAGS_port);
    server->start(addr);
    server->logger_ =
        std::make_shared<moxygen::MLogger>(moxygen::VantagePoint::SERVER);
    server->logger_->setPath(FLAGS_mlog_path);
    std::cout << "Type Anything To Exit Server...";
    std::string line;
    std::getline(std::cin, line);
    server->logger_->outputLogsToFile();
    return 0;
  } else {
    auto server = std::make_shared<moxygen::MoQTestServer>();
    folly::SocketAddress addr("::", FLAGS_port);
    server->start(addr);
    folly::EventBase evb;
    evb.loopForever();
  }
}
