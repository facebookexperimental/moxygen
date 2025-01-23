/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQServer.h"
#include "moxygen/relay/MoQRelay.h"

#include <folly/init/Init.h>

using namespace proxygen;

DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_string(endpoint, "/moq-relay", "End point");
DEFINE_int32(port, 9668, "Relay Server Port");

namespace {
using namespace moxygen;

class MoQRelayServer : MoQServer {
 public:
  MoQRelayServer()
      : MoQServer(FLAGS_port, FLAGS_cert, FLAGS_key, FLAGS_endpoint) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(relay_);
    clientSession->setSubscribeHandler(relay_);
  }

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    relay_->removeSession(session);
  }

 private:
  std::shared_ptr<MoQRelay> relay_{std::make_shared<MoQRelay>()};
};
} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  MoQRelayServer moqRelayServer;
  folly::EventBase evb;
  evb.loopForever();
  return 0;
}
