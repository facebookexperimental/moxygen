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
DEFINE_bool(
    insecure,
    false,
    "Use insecure verifier (skip certificate validation)");
DEFINE_bool(
    use_legacy_setup,
    false,
    "If true, use only moq-00 ALPN (legacy). If false, use latest draft ALPN with fallback to legacy");

namespace {
using namespace moxygen;

class MoQRelayServer : public MoQServer {
 public:
  // Used when the insecure flag is false
  MoQRelayServer(const std::string& cert, const std::string& key)
      : MoQServer(cert, key, FLAGS_endpoint) {}

  // Used when the insecure flag is true
  MoQRelayServer()
      : MoQServer(
            quic::samples::createFizzServerContextWithInsecureDefault(
                []() {
                  std::vector<std::string> alpns = {"h3"};
                  auto moqt = getDefaultMoqtProtocols(!FLAGS_use_legacy_setup);
                  alpns.insert(alpns.end(), moqt.begin(), moqt.end());
                  return alpns;
                }(),
                fizz::server::ClientAuthMode::None,
                "" /* cert */,
                "" /* key */),
            FLAGS_endpoint) {}

  void onNewSession(std::shared_ptr<MoQSession> clientSession) override {
    clientSession->setPublishHandler(relay_);
    clientSession->setSubscribeHandler(relay_);
  }

 protected:
  std::shared_ptr<MoQSession> createSession(
      folly::MaybeManagedPtr<proxygen::WebTransport> wt,
      MoQExecutor::KeepAlive executor) override {
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
  std::shared_ptr<MoQRelayServer> moqRelayServer = nullptr;
  if (FLAGS_insecure) {
    moqRelayServer = std::make_shared<MoQRelayServer>();
  } else {
    moqRelayServer = std::make_shared<MoQRelayServer>(FLAGS_cert, FLAGS_key);
  }
  folly::SocketAddress addr("::", FLAGS_port);
  moqRelayServer->start(addr);
  folly::EventBase evb;
  evb.loopForever();
  return 0;
}
