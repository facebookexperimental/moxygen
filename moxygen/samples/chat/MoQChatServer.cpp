/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/container/F14Set.h>
#include "moxygen/MoQServer.h"
#include "moxygen/relay/MoQRelay.h"

using namespace quic::samples;
using namespace proxygen;

DEFINE_string(relay, "", "Use specified relay");
DEFINE_string(chat_id, "1000", "Chat ID");
DEFINE_string(cert, "", "Cert path");
DEFINE_string(key, "", "Key path");
DEFINE_int32(port, 9667, "MoQChat Server Port");

namespace {
using namespace moxygen;

class MoQChatServer : MoQServer {
 public:
  explicit MoQChatServer(std::string chatID)
      : MoQServer(FLAGS_port, FLAGS_cert, FLAGS_key, "/moq-chat"),
        chatID_(chatID) {
    relay_.setAllowedNamespacePrefix(participantPrefix());
  }

  class ChatControlVisitor : public MoQServer::ControlVisitor {
   public:
    ChatControlVisitor(
        MoQChatServer& server,
        std::shared_ptr<MoQSession> clientSession)
        : MoQServer::ControlVisitor(std::move(clientSession)),
          server_(server) {}

    void operator()(Announce announce) const override {
      XLOG(INFO) << "Announce ns=" << announce.trackNamespace;
      if (!FLAGS_relay.empty()) {
        clientSession_->announceError(
            {announce.trackNamespace, 403, "unexpected announce"});
      } else {
        server_.relay_.onAnnounce(std::move(announce), clientSession_);
      }
    }

    void operator()(SubscribeRequest subscribeReq) const override {
      XLOG(INFO) << "SubscribeRequest track="
                 << subscribeReq.fullTrackName.trackNamespace
                 << subscribeReq.fullTrackName.trackName;
      if (!(subscribeReq.fullTrackName == server_.catalogTrackName())) {
        if (!FLAGS_relay.empty()) {
          clientSession_->subscribeError(
              {subscribeReq.subscribeID, 403, "unexpected subscribe"});
        } else {
          server_.relay_.onSubscribe(subscribeReq, clientSession_)
              .scheduleOn(clientSession_->getEventBase())
              .start();
        }
        return;
      }
      server_.onSubscribe(clientSession_, std::move(subscribeReq));
    }

    void operator()(Goaway) const override {
      XLOG(INFO) << "Goaway";
      // remove this session from server_.subscribers_
    }

   private:
    MoQChatServer& server_;
  };

  std::unique_ptr<ControlVisitor> makeControlVisitor(
      std::shared_ptr<MoQSession> clientSession) override {
    return std::make_unique<ChatControlVisitor>(
        *this, std::move(clientSession));
  }

  void onSubscribe(
      std::shared_ptr<MoQSession> clientSession,
      SubscribeRequest subReq) {
    std::string username;
    for (const auto& p : subReq.params) {
      if (p.key == folly::to_underlying(TrackRequestParamKey::AUTHORIZATION)) {
        username = p.value;
        break;
      }
    }
    if (username.empty()) {
      clientSession->subscribeError(
          {subReq.subscribeID, 403, "authorization(username) required"});
      return;
    }
    auto subIt = subscribers_.find(username);
    if (subIt != subscribers_.end()) {
      // is it the same session?  if so, reply with the same track id
      // is it a different session?  if so, subscribe error?
      XLOG(INFO) << username << " rejoined the chat";
    } else {
      XLOG(INFO) << username << " joined the chat";
    }
    subscribers_[username] = std::make_pair(clientSession, subReq.subscribeID);
    folly::Optional<GroupAndObject> current;
    if (catGroup_ > 0) {
      current = GroupAndObject({catGroup_ - 1, 0});
    }
    clientSession->subscribeOk(
        {subReq.subscribeID, std::chrono::milliseconds(0), std::move(current)});
    publishCatalog();
  }

  void publishCatalog() {
    std::vector<std::string> subs;
    for (auto& sub : subscribers_) {
      subs.push_back(sub.first);
    }
    auto catalogString = fmt::format("version=1\n{0}", folly::join("\n", subs));
    auto catalogBuf = folly::IOBuf::copyBuffer(catalogString);
    auto catGroup = catGroup_++;
    for (auto& sub : subscribers_) {
      sub.second.first->publish(
          {sub.second.second,
           0 /* WRONG */,
           catGroup,
           0,
           0,
           ForwardPreference::Object,
           ObjectStatus::NORMAL,
           folly::none},
          0,
          catalogBuf->clone(),
          true);
    }
  }

  void terminateClientSession(std::shared_ptr<MoQSession> session) override {
    if (!session) {
      XLOG(ERR) << "what";
      return;
    }
    relay_.removeSession(session);
    bool catalogChange = false;
    for (auto it = subscribers_.begin(); it != subscribers_.end(); it++) {
      if (it->second.first.get() == session.get()) {
        XLOG(INFO) << it->first << " left the chat";
        subscribers_.erase(it);
        catalogChange = true;
      }
    }
    if (catalogChange) {
      publishCatalog();
    }
    session->close();
  }

 private:
  std::string chatID_;
  folly::
      F14FastMap<std::string, std::pair<std::shared_ptr<MoQSession>, uint64_t>>
          subscribers_;
  uint64_t catGroup_{0};
  MoQRelay relay_;

  [[nodiscard]] std::string chatPrefix() const {
    return fmt::format("moq-chat/{0}", chatID_);
  }

  [[nodiscard]] FullTrackName catalogTrackName() const {
    return FullTrackName({chatPrefix(), "/catalog"});
  }

  [[nodiscard]] std::string participantPrefix() const {
    return fmt::format("{0}/participant/", chatPrefix());
  }
};
} // namespace
int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv, true);
  MoQChatServer moqChatServer(FLAGS_chat_id);
  folly::EventBase evb;
  evb.loopForever();
  return 0;
}
