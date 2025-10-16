/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "moxygen/MoQRelaySession.h"
#include "moxygen/MoQServer.h"
#include "moxygen/relay/MoQRelay.h"

#include <folly/init/Init.h>

using namespace proxygen;

DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_string(endpoint, "/moq-relay", "End point");
DEFINE_int32(port, 9668, "Relay Server Port");
DEFINE_bool(enable_cache, false, "Enable relay cache");

namespace {
using namespace moxygen;

class MoQRelayServer : public MoQServer {
 public:
  MoQRelayServer() : MoQServer(FLAGS_cert, FLAGS_key, FLAGS_endpoint) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(relay_);
    clientSession->setSubscribeHandler(relay_);
  }

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    relay_->removeSession(session);
  }

 protected:
  std::shared_ptr<MoQSession> createSession(
      std::shared_ptr<proxygen::WebTransport> wt,
      std::shared_ptr<MoQExecutor> executor) override {
    return std::make_shared<MoQRelaySession>(
        folly::MaybeManagedPtr<proxygen::WebTransport>(std::move(wt)),
        *this,
        std::move(executor));
  }

 private:
  std::shared_ptr<MoQRelay> relay_{
      std::make_shared<MoQRelay>(FLAGS_enable_cache)};
};
} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  MoQRelayServer moqRelayServer;
  folly::SocketAddress addr("::", FLAGS_port);
  moqRelayServer.start(addr);
  folly::EventBase evb;
  evb.loopForever();
  return 0;
}
