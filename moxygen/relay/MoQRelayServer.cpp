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

  class RelayControlVisitor : public MoQServer::ControlVisitor {
   public:
    RelayControlVisitor(
        MoQRelayServer& server,
        std::shared_ptr<MoQSession> clientSession)
        : MoQServer::ControlVisitor(std::move(clientSession)),
          server_(server) {}

    void operator()(Announce announce) const override {
      XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
      server_.relay_.onAnnounce(std::move(announce), clientSession_);
    }

    void operator()(Unannounce unannounce) const override {
      XLOG(INFO) << "Unannounce ns=" << unannounce.trackNamespace;
      server_.relay_.onUnannounce(std::move(unannounce), clientSession_);
    }

    void operator()(SubscribeAnnounces subscribeAnnounces) const override {
      XLOG(INFO) << "SubscribeAnnounces ns="
                 << subscribeAnnounces.trackNamespacePrefix;
      server_.relay_.onSubscribeAnnounces(
          std::move(subscribeAnnounces), clientSession_);
    }

    void operator()(UnsubscribeAnnounces unsubscribeAnnounces) const override {
      XLOG(INFO) << "UnsubscribeAnnounces ns="
                 << unsubscribeAnnounces.trackNamespacePrefix;
      server_.relay_.onUnsubscribeAnnounces(
          std::move(unsubscribeAnnounces), clientSession_);
    }

    void operator()(SubscribeRequest subscribeReq) const override {
      XLOG(INFO) << "SubscribeRequest track="
                 << subscribeReq.fullTrackName.trackNamespace << "/"
                 << subscribeReq.fullTrackName.trackName;
      server_.relay_.onSubscribe(std::move(subscribeReq), clientSession_)
          .scheduleOn(clientSession_->getEventBase())
          .start();
    }

    void operator()(Unsubscribe unsubscribe) const override {
      XLOG(INFO) << "Unsubscribe id=" << unsubscribe.subscribeID;
      server_.relay_.onUnsubscribe(std::move(unsubscribe), clientSession_);
    }

    void operator()(Goaway) const override {
      XLOG(INFO) << "Goaway";
    }

   private:
    MoQRelayServer& server_;
  };

  std::unique_ptr<ControlVisitor> makeControlVisitor(
      std::shared_ptr<MoQSession> clientSession) override {
    return std::make_unique<RelayControlVisitor>(
        *this, std::move(clientSession));
  }

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    relay_.removeSession(session);
  }

 private:
  MoQRelay relay_;
};
} // namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  MoQRelayServer moqRelayServer;
  folly::EventBase evb;
  evb.loopForever();
  return 0;
}
